(ns parsley-jepsen.nemesis
  "Jepsen's combined nemesis (network partitions, broker kill and restart, SIGSTOP pauses,
  clock skew) plus the Parsley-specific faults. Every Parsley fault op carries, in its
  value, the refusal reasons it justifies, which the checker reads: a refusal no earlier
  fault justifies is an Operational 1/6 violation. A fault that must produce a refusal also
  carries :expect, the process and the reason, and the checker reports one that never came.

  A refusal is terminal by design, so a refusal-class fault is scheduled at most once per
  process per run: `plan` gives each requested fault a process no other fault has."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [util :as util]]
            [jepsen.nemesis.combined :as nc]
            [parsley-jepsen [client :as client]
                            [db :as db]
                            [workload :refer [after]]])
  (:import [java.util.concurrent ExecutionException TimeUnit]
           [org.apache.kafka.clients.admin Admin NewPartitions OffsetSpec RecordsToDelete]
           [org.apache.kafka.clients.consumer OffsetAndMetadata]
           [org.apache.kafka.clients.producer KafkaProducer RecordMetadata]
           [org.apache.kafka.common IsolationLevel TopicPartition]))

(def instance-faults #{:instance-kill :instance-pause :wipe})

(def refusal-targets
  "The refusal-class faults in the README's order of expected yield, each with the
  processes it can be aimed at, most preferred first, and the topic it acts on there.
  :names is the received topic a manufactured hold's stamp names. A recreated topic is
  never one external producers write, or their stamps would name a dead incarnation."
  [[:truncate [["splitter" {:topic "src"}] ["joiner" {:topic "a"}]]]
   [:delete-topic [["selfer" {:topic "self" :names "d"}]]]
   [:recreate-topic [["cycler" {:topic "c"}] ["selfer" {:topic "d"}] ["joiner" {:topic "loop"}]]]
   [:delete-changelog [["splitter" {}] ["cycler" {}] ["joiner" {}] ["selfer" {}]]]
   [:add-partitions [["joiner" {:topic "a"}] ["selfer" {:topic "d"}]]]
   [:restart-dropping [["joiner" {:topic "b" :names "a"}] ["selfer" {:topic "self" :names "d"}]]]
   [:corrupt [["splitter" {:topic "src"}] ["joiner" {:topic "a"}]]]])

(def refusal-faults (set (map first refusal-targets)))

(def op-f
  "The :f a fault's op carries where it is not the fault's own name: Jepsen's file
  corruption nemesis already answers to :truncate."
  {:truncate :truncate-records})

(defn plan
  "Gives each requested refusal-class fault a process no other fault in the run has:
  [[fault {:process p :topic t ...}] ...] in the order they will be injected. A fault left
  without a process is dropped with a warning."
  [faults]
  (loop [remaining (filter (comp (set faults) first) refusal-targets)
         taken #{}
         planned []]
    (if-let [[fault candidates] (first remaining)]
      (if-let [[process target] (first (remove (comp taken first) candidates))]
        (recur (rest remaining) (conj taken process) (conj planned [fault (assoc target :process process)]))
        (do (warn "No process is left for the refusal-class fault" fault "- it is not scheduled in this run")
            (recur (rest remaining) taken planned)))
      planned)))

;; ---- helpers ----

(defn- with-admin [test f]
  (with-open [admin (db/admin test)]
    (f admin)))

(defn- get! [future]
  (.get ^org.apache.kafka.common.KafkaFuture future 30 TimeUnit/SECONDS))

(defn- kill-all! [test]
  (c/on-nodes test (fn [t n] (db/kill-harness! t n))))

(defn- stop-all!
  "Stops every instance gracefully, so their groups are left rather than held until the
  session timeout."
  [test]
  (c/on-nodes test (fn [t n] (db/stop-harness! t n))))

(defn- start-all! [test]
  (c/on-nodes test (fn [t n] (db/start-harness! t n))))

(defn- nodes-of [test value]
  (if (= :all (:nodes value)) (:nodes test) [(:node value)]))

(defn- with-views
  "Calls f with one admin client per broker, so a fault can ask every broker's view. A
  broker cut off by a partition keeps answering from a stale cache, and a fault that acts
  on one broker's answer can do what no fault meant to: a committed position read high
  from a coordinator that missed a reset, and records deleted up to it, leave the group
  below the log start."
  [test f]
  (let [views (client/make-views test)]
    (try (f views)
         (finally (doseq [^Admin view views] (.close view (java.time.Duration/ofSeconds 5)))))))

(defn- view-each
  "f over every view that answers, or nil when none does."
  [views f]
  (seq (keep (fn [^Admin view] (try (f view) (catch Exception _ nil))) views)))

(defn- committed
  "The group's committed position on tp, the lowest of every broker's view: a coordinator
  that missed a reset answers high, and a fault that deletes or rewinds must not act on
  that."
  [views process ^TopicPartition tp]
  (some->> (view-each views #(get (client/committed % (client/group-id process)) tp))
           (remove nil?) seq (apply min)))

(defn- stable-end
  "The partition's last stable offset, the highest of every broker's view."
  [views ^TopicPartition tp]
  (some->> (view-each views #(get (client/list-offsets % [tp] (OffsetSpec/latest) IsolationLevel/READ_COMMITTED) tp))
           (remove nil?) seq (apply max)))

(defn- log-start
  "The partition's log start, the highest of every broker's view: a deposed leader answers
  low, and a rewind to below the true log start is a fault no row of the table injects."
  [views ^TopicPartition tp]
  (some->> (view-each views #(get (client/list-offsets % [tp] (OffsetSpec/earliest) IsolationLevel/READ_COMMITTED) tp))
           (remove nil?) seq (apply max)))

(defn- trace-high-watermarks
  "The trace's high watermarks. Read while every instance is down, they are a boundary: a
  trace record at or past them belongs to a step taken after the restart."
  [test ^Admin admin]
  (client/trace-ends admin (:partitions test) IsolationLevel/READ_UNCOMMITTED))

(defn- await-group-empty!
  "Waits until the group has no members, which altering its offsets requires."
  [^Admin admin group]
  (util/await-fn
   (fn []
     (let [description ^org.apache.kafka.clients.admin.ConsumerGroupDescription
           (get (get! (.all (.describeConsumerGroups admin [group]))) group)]
       (when (seq (.members description))
         (info "Group" group "is" (str (.groupState description)) "with" (count (.members description)) "members")
         (throw (ex-info "group still has members" {:group group})))))
   {:retry-interval 2000 :log-interval 10000 :timeout 120000
    :log-message (str "Waiting for group " group " to empty")}))

(defn- await-topic-gone! [^Admin admin topic]
  (util/await-fn
   (fn []
     (when (contains? (get! (.names (.listTopics admin))) topic)
       (throw (ex-info "topic still listed" {:topic topic}))))
   {:retry-interval 1000 :log-interval 10000 :timeout 120000
    :log-message (str "Waiting for topic " topic " to be deleted")}))

(defn- delete-topic!
  "Deletes the topic and waits until it is gone. The request is retried, since under a
  network partition it can time out after the deletion has begun, and a topic already
  gone is fine."
  [^Admin admin ^String topic]
  (util/await-fn
   (fn []
     (try (get! (.all (.deleteTopics admin ^java.util.Collection (list topic))))
          (catch ExecutionException e
            (when-not (instance? org.apache.kafka.common.errors.UnknownTopicOrPartitionException (.getCause e))
              (throw e)))))
   {:retry-interval 2000 :log-interval 10000 :timeout 180000
    :log-message (str "Deleting topic " topic)})
  (await-topic-gone! admin topic))

(defn- recreate!
  "Creates the topic again and waits until its name resolves to an incarnation other than
  `old`, retrying the creation, which can time out under a partition too."
  [test ^Admin admin topic old]
  (util/await-fn
   (fn []
     (db/create-topics! test [topic])
     (let [new (get (client/describe-ids admin [topic]) topic)]
       (when (or (nil? new) (= new old))
         (throw (ex-info "topic not yet recreated" {:topic topic})))
       new))
   {:retry-interval 2000 :log-interval 10000 :timeout 180000
    :log-message (str "Recreating topic " topic)}))

(defn- hold!
  "Manufactures a held message: a record on `topic`'s partition whose stamp names a
  position far past the log end of `names` there, so the receiving task reads it and holds
  it. Waits until the task has committed reading past it. Returns {:held bool :held-at o}."
  [test ^Admin admin {:keys [process topic names partition]}]
  (let [ids (client/topic-ids test admin)
        named (+ 1000000 (client/log-end admin (TopicPartition. names (int partition))))
        uid (str "held-" topic "-" partition)
        tp (TopicPartition. topic (int partition))
        ;; A partition can stall a send for a while; the record must get through.
        md (with-open [producer ^KafkaProducer (client/make-producer test {"delivery.timeout.ms" "150000"
                                                                            "request.timeout.ms" "30000"
                                                                            "max.block.ms" "60000"})]
             ^RecordMetadata (.get (.send producer (client/record {:topic topic :partition partition :key uid :uid uid
                                                                   :stamp {[(get ids names) partition] named}}))
                                   180 TimeUnit/SECONDS))
        offset (.offset ^RecordMetadata md)
        held (try (util/await-fn
                   (fn []
                     (when-not (some-> (committed admin process tp) (> offset))
                       (throw (ex-info "not yet read" {})))
                     true)
                   {:retry-interval 1000 :log-interval 10000 :timeout 60000
                    :log-message (str "Waiting for " process " to read the held record at " tp "@" offset)})
                  (catch Exception _ false))]
    {:held held :held-at offset :held-uid uid :named [(get ids names) partition named]}))

;; ---- the faults ----

(defn- truncate!
  "Delete records past a lagging task's committed position. Every instance is killed so the
  task lags behind what external producers keep sending, the records just past its
  committed position are deleted, and the restart's first fetch finds them gone."
  [test {:keys [process topic partition]}]
  (let [tp (TopicPartition. topic (int partition))]
    (kill-all! test)
    (try
      (with-views test
        (fn [views]
          (let [position (or (committed views process tp) 0)
                end (try (util/await-fn
                          (fn [] (let [end (stable-end views tp)]
                                   (when (< end (+ position 2)) (throw (ex-info "no lag yet" {})))
                                   end))
                          {:retry-interval 1000 :log-interval 10000 :timeout 45000
                           :log-message (str "Waiting for " tp " to grow past " process "'s committed position")})
                         (catch Exception _ (stable-end views tp)))
                to (min end (+ position 1 (rand-int 3)))]
            (when (> to position)
              (with-admin test (fn [^Admin admin] (get! (.all (.deleteRecords admin {tp (RecordsToDelete/beforeOffset to)}))))))
            (cond-> {:committed position :to to :end end :channel [topic partition]}
              (> to position) (assoc :expect {:process process :refusal :POSITIONS_DISCARDED_UNREAD})))))
      (finally (start-all! test)))))

(defn- discard-held-copy!
  "Retention discards a held message's copy: delete records up to a task's committed
  position, which removes copies of messages it holds but nothing it has not read. No
  refusal: the hold delivers from the ordering changelog in order."
  [test {:keys [process topic partition]}]
  (let [tp (TopicPartition. topic (int partition))]
    (with-views test
      (fn [views]
        (let [position (or (committed views process tp) 0)]
          (when (pos? position)
            (with-admin test (fn [^Admin admin] (get! (.all (.deleteRecords admin {tp (RecordsToDelete/beforeOffset position)}))))))
          {:to position :channel [topic partition]})))))

(defn- delete-topic!!
  "Delete a received topic while a message is held from it."
  [test {:keys [process topic] :as v}]
  (with-admin test
    (fn [^Admin admin]
      (let [id (get (client/topic-ids test admin) topic)
            held (hold! test admin v)]
        (swap! (:gone-topics test) conj topic)
        (delete-topic! admin topic)
        (cond-> (assoc held :channel [id 0])
          (:held held) (assoc :expect {:process process :refusal :CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES}))))))

(defn- recreate-topic!
  "Delete and recreate a received topic under the same name while every instance is down:
  the next start finds a different topic id."
  [test {:keys [process topic]}]
  (kill-all! test)
  (try
    (with-admin test
      (fn [^Admin admin]
        (let [old (get (client/topic-ids test admin) topic)
              ends (trace-high-watermarks test admin)]
          (delete-topic! admin topic)
          (let [new (recreate! test admin topic old)]
            {:old old :new new :reinitialised-ends ends
             :expect {:process process :refusal :CHANNEL_IDENTITY_CHANGED}}))))
    (finally (start-all! test))))

(defn- reset-offsets!
  "Reset the group's offsets backwards while every instance is down: the re-fed records
  are dropped as already delivered, and no refusal is justified. The instances are stopped
  gracefully, since altering a group's offsets needs it empty, and a killed member holds
  its place for the session timeout."
  [test {:keys [process topic partition back]}]
  (let [tp (TopicPartition. topic (int partition))
        group (client/group-id process)]
    (stop-all! test)
    (try
      (with-views test
        (fn [views]
          (with-admin test (fn [^Admin admin] (await-group-empty! admin group)))
          (let [position (or (committed views process tp) 0)
                target (max (log-start views tp) (- position back))]
            (when (< target position)
              ;; An alter whose request times out can still be applied later, after the next
              ;; fault has read the old position and acted on it; so the alter is confirmed by
              ;; reading the position back, and repeated until it is.
              (util/await-fn
               (fn []
                 (with-admin test (fn [^Admin admin]
                                    (try (get! (.all (.alterConsumerGroupOffsets admin group {tp (OffsetAndMetadata. target)})))
                                         (catch Exception e (info "alter not confirmed:" (.getMessage e))))))
                 (when (not= target (committed views process tp))
                   (throw (ex-info "offsets not yet altered" {:group group :target target}))))
               {:retry-interval 2000 :log-interval 10000 :timeout 120000
                :log-message (str "Altering " group "'s offsets on " tp " to " target)}))
            {:from position :to target :channel [topic partition]})))
      (finally (start-all! test)))))

(defn- delete-changelog!
  "Delete the ordering changelog and every instance's local state, keeping the group's
  offsets: the causal past is gone while the positions say it should exist."
  [test {:keys [process]}]
  (let [changelog (str (client/group-id process) "-__parsley.ordering-changelog")]
    (kill-all! test)
    (try
      (with-admin test (fn [^Admin admin] (delete-topic! admin changelog)))
      (c/on-nodes test (fn [t n] (db/wipe-harness-state! t n)))
      {:changelog changelog :expect {:process process :refusal :ORDERING_STATE_LOST}}
      (finally (start-all! test)))))

(defn- add-partitions!
  "Add a partition to a received topic while every instance is down."
  [test {:keys [process topic]}]
  (kill-all! test)
  (try
    (with-admin test
      (fn [^Admin admin]
        (get! (.all (.createPartitions admin {topic (NewPartitions/increaseTo (int (inc (:partitions test))))})))
        {:partitions (inc (:partitions test)) :expect {:process process :refusal :TASK_WIDTH_CHANGED}}))
    (finally (start-all! test))))

(defn- restart-dropping!
  "Restart every instance with a declaration dropping a topic a message is held from."
  [test {:keys [process topic] :as v}]
  (let [held (with-admin test (fn [^Admin admin] (hold! test admin v)))]
    (kill-all! test)
    (try
      (let [ends (with-admin test (fn [^Admin admin] (trace-high-watermarks test admin)))]
        (swap! (:dropped-topics test) conj topic)
        (cond-> (assoc held :trace-ends ends :dropped true)
          (:held held) (assoc :expect {:process process :refusal :CHANNEL_REMOVED_WITH_HELD_MESSAGES})))
      (finally (start-all! test)))))

(def justifies
  "The refusal reasons each fault justifies once injected."
  {:truncate-records #{:POSITIONS_DISCARDED_UNREAD}
   :delete-topic #{:CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES :CHANNEL_IDENTITY_CHANGED}
   :recreate-topic #{:CHANNEL_IDENTITY_CHANGED :CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES}
   :delete-changelog #{:ORDERING_STATE_LOST}
   :add-partitions #{:TASK_WIDTH_CHANGED}
   :restart-dropping #{:CHANNEL_REMOVED_WITH_HELD_MESSAGES}})

(defn parsley-nemesis
  "The Parsley-specific faults. Each :f names a fault; :value carries its target and,
  once applied, the refusals it justifies."
  []
  (reify
    nemesis/Reflection
    (fs [_] #{:kill-instance :restart-instance :pause-instance :resume-instance :wipe-restart-instance
              :restart-dropping :truncate-records :discard-held-copy :delete-topic :recreate-topic
              :reset-offsets :delete-changelog :add-partitions})

    nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (let [v (:value op)
            f (:f op)
            ;; A fault that fails part-way may still have been applied, so it justifies
            ;; what it would have justified.
            result (try
                     (case f
                       :kill-instance
                       (do (c/on-nodes test (nodes-of test v) (fn [t n] (db/kill-harness! t n))) {})

                       :restart-instance
                       (do (c/on-nodes test (nodes-of test v) (fn [t n] (db/start-harness! t n))) {})

                       ;; A pause past the transaction timeout: the resumed instance is a zombie
                       ;; the host must fence (SPEC Host obligation 6); no refusal is justified.
                       :pause-instance
                       (do (c/on-nodes test (nodes-of test v) (fn [t n] (db/pause-harness! t n))) {})

                       :resume-instance
                       (do (c/on-nodes test (nodes-of test v) (fn [t n] (db/resume-harness! t n))) {})

                       :wipe-restart-instance
                       (do (c/on-nodes test (nodes-of test v)
                                       (fn [t n] (db/kill-harness! t n) (db/wipe-harness-state! t n) (db/start-harness! t n)))
                           {})

                       :truncate-records (truncate! test v)
                       :discard-held-copy (discard-held-copy! test v)
                       :delete-topic (delete-topic!! test v)
                       :recreate-topic (recreate-topic! test v)
                       :reset-offsets (reset-offsets! test v)
                       :delete-changelog (delete-changelog! test v)
                       :add-partitions (add-partitions! test v)
                       :restart-dropping (restart-dropping! test v))
                     (catch Exception e
                       (warn e "Fault" f "failed part-way")
                       {:error (str (.getName (class e)) ": " (.getMessage e))}))]
        (assoc op :type :info :value (merge v result {:justifies (get justifies f #{})}))))

    (teardown! [this test])))

(defn- instance-generator
  "Instance faults: a kill is followed by a restart of whatever is down, a pause by a
  resume, so each lasts about one interval, which is past the transaction timeout."
  [faults]
  (let [node (fn [test] (rand-nth (:nodes test)))
        all {:nodes :all}]
    (gen/mix
     (remove nil?
             [(when (:instance-kill faults)
                (gen/flip-flop (fn [test _] {:type :info :f :kill-instance :value {:node (node test)}})
                               (gen/repeat {:type :info :f :restart-instance :value all})))
              (when (:instance-pause faults)
                (gen/flip-flop (fn [test _] {:type :info :f :pause-instance :value {:node (node test)}})
                               (gen/repeat {:type :info :f :resume-instance :value all})))
              (when (:wipe faults)
                (fn [test _] {:type :info :f :wipe-restart-instance :value {:node (node test)}}))]))))

(defn- topic-generator
  "Faults on the logs that justify no refusal, on a random received partition."
  [faults]
  (let [target (fn [test]
                 (let [[process received] (rand-nth (seq client/processes))
                       received (remove @(:gone-topics test) received)]
                   (when (seq received)
                     {:process process :topic (rand-nth received) :partition (rand-int (:partitions test))})))]
    (gen/mix
     (remove nil?
             [(when (:discard-held-copy faults)
                (fn [test _] (when-let [t (target test)] {:type :info :f :discard-held-copy :value t})))
              (when (:reset-offsets faults)
                (fn [test _] (when-let [t (target test)]
                               {:type :info :f :reset-offsets :value (assoc t :back (inc (rand-int 5)))})))]))))

(defn- refusal-generator
  "The planned refusal-class faults, one per interval after a first interval of plain
  running. :corrupt is a client's send, which the workload schedules."
  [opts planned]
  (let [ops (for [[fault target] planned
                  :when (not= :corrupt fault)]
              {:type :info :f (get op-f fault fault) :value (assoc target :partition (rand-int (:partitions opts)))})]
    (when (seq ops)
      (after (:nemesis-interval opts 60) (gen/delay (:nemesis-interval opts 60) ops)))))

(defn parsley-package
  "A nemesis package in jepsen.nemesis.combined's shape, for the Parsley faults."
  [opts planned]
  (let [faults (:nemesis opts)
        interval (:nemesis-interval opts 60)
        instance (instance-generator faults)
        topic (topic-generator faults)
        refusal (refusal-generator opts planned)
        ;; Every fault waits out one interval first: stagger can emit its first op at once,
        ;; before an instance has finished starting.
        generators (remove nil? [(after interval (gen/stagger interval instance))
                                 (after interval (gen/stagger interval topic))
                                 refusal])]
    {:nemesis (parsley-nemesis)
     :generator (when (seq generators) (apply gen/any generators))
     :final-generator [{:type :info :f :resume-instance :value {:nodes :all}}
                       {:type :info :f :restart-instance :value {:nodes :all}}]
     :perf #{{:name "instance-kill" :start #{:kill-instance} :stop #{:restart-instance} :color "#E9A4A0"}
             {:name "instance-pause" :start #{:pause-instance} :stop #{:resume-instance} :color "#A0B1E9"}
             {:name "parsley-fault" :fs (-> (set (map #(get op-f % %) refusal-faults))
                                           (disj :corrupt)
                                           (conj :wipe-restart-instance :discard-held-copy :reset-offsets))
              :color "#C5A0E9"}}}))

(defn package
  "The combined nemesis: Jepsen's partition, kill, pause and clock faults over the brokers,
  and the Parsley faults over the instances and the topics. `planned` is `plan`'s answer."
  [opts planned]
  (let [faults (:nemesis opts)
        combined (nc/nemesis-package {:db (:db opts)
                                      :interval (:nemesis-interval opts 60)
                                      :faults (set (filter #{:partition :kill :pause :clock} faults))
                                      :partition {:targets [:one :majority :majorities-ring]}
                                      :kill {:targets [:one]}
                                      :pause {:targets [:one]}
                                      :clock {:targets [:one]}})]
    ;; Jepsen's own faults wait out the first interval too; stagger would fire one at once.
    (nc/compose-packages [(update combined :generator #(after (:nemesis-interval opts 60) %))
                          (parsley-package opts planned)])))
