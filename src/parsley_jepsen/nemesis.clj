(ns parsley-jepsen.nemesis
  "Jepsen's combined nemesis (network partitions, broker kill and restart, SIGSTOP pauses,
  clock skew) plus the Parsley-specific faults. Every Parsley fault op carries, in its
  value, the refusal reasons it justifies, which the checker reads: a refusal no earlier
  fault justifies is an Operational 1/6 violation.

  UNVERIFIED: written without a cluster or a Clojars-reachable build."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [generator :as gen]
                    [nemesis :as nemesis]
                    [util :as util]]
            [jepsen.nemesis.combined :as nc]
            [parsley-jepsen [client :as client]
                            [db :as db]])
  (:import [java.util.concurrent TimeUnit]
           [org.apache.kafka.clients.admin Admin NewPartitions NewTopic OffsetSpec RecordsToDelete]
           [org.apache.kafka.clients.consumer OffsetAndMetadata]
           [org.apache.kafka.common TopicPartition]))

(def topics client/topics)

(defn- with-admin [test f]
  (with-open [admin (client/admin test)]
    (f admin)))

(defn- log-end [^Admin admin ^TopicPartition tp]
  (-> (.get (.all (.listOffsets admin {tp (OffsetSpec/latest)})) 30 TimeUnit/SECONDS) (get tp) .offset))

(defn- committed [^Admin admin group ^TopicPartition tp]
  (get (client/committed admin group) tp))

(defn- on [node f]
  (c/on-nodes {:nodes [node]} (fn [test node] (f node))))

(defn parsley-nemesis
  "The Parsley-specific faults. Each :f names a fault; :value carries its target and the
  refusals it justifies once applied."
  []
  (reify
    nemesis/Reflection
    (fs [_] #{:kill-instance :restart-instance :pause-instance :resume-instance :wipe-restart-instance
              :restart-dropping :truncate :discard-held-copy :delete-topic :recreate-topic
              :reset-offsets :delete-changelog :add-partitions})

    nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (let [{:keys [node] :as v} (:value op)
            done (fn [extra justifies]
                   (assoc op :type :info :value (merge v extra {:justifies justifies})))]
        (case (:f op)
          :kill-instance
          (do (c/on-nodes test [node] (fn [t n] (db/kill-harness! t n)))
              (done {} #{}))

          :restart-instance
          (do (c/on-nodes test [node] (fn [t n] (db/start-harness! t n)))
              (done {} #{}))

          ;; A pause past the transaction timeout: the resumed instance is a zombie the host
          ;; must fence (SPEC Host obligation 6); no refusal is justified.
          :pause-instance
          (do (c/on-nodes test [node] (fn [t n] (db/pause-harness! t n)))
              (done {} #{}))

          :resume-instance
          (do (c/on-nodes test [node] (fn [t n] (db/resume-harness! t n)))
              (done {} #{}))

          :wipe-restart-instance
          (do (c/on-nodes test [node] (fn [t n] (db/kill-harness! t n) (db/wipe-harness-state! t n) (db/start-harness! t n)))
              (done {} #{}))

          ;; Restart every instance with a declaration dropping a topic: a channel still holding
          ;; messages refuses CHANNEL_REMOVED_WITH_HELD_MESSAGES; an empty one settles.
          :restart-dropping
          (do (c/on-nodes test (fn [t n] (db/kill-harness! t n) (db/start-harness! t n [(:topic v)])))
              (done {} #{:CHANNEL_REMOVED_WITH_HELD_MESSAGES}))

          ;; Delete records past a lagging task's committed position: the fetch at that position
          ;; refuses POSITIONS_DISCARDED_UNREAD, and nothing delivers past the gap.
          :truncate
          (let [{:keys [topic partition process]} v
                tp (TopicPartition. topic partition)
                result (with-admin test
                         (fn [^Admin admin]
                           (let [position (or (committed admin (str db/app-prefix "-" process) tp) 0)
                                 end (log-end admin tp)
                                 to (min end (+ position 1 (rand-int 3)))]
                             (.get (.all (.deleteRecords admin {tp (RecordsToDelete/beforeOffset to)})) 30 TimeUnit/SECONDS)
                             {:committed position :to to :channel [topic partition]})))]
            (done result #{:POSITIONS_DISCARDED_UNREAD}))

          ;; Retention discards a held message's copy: delete records up to a task's committed
          ;; position, which removes copies of messages it holds but nothing it has not read.
          ;; No refusal: the hold delivers from the ordering changelog in order.
          :discard-held-copy
          (let [{:keys [topic partition process]} v
                tp (TopicPartition. topic partition)
                result (with-admin test
                         (fn [^Admin admin]
                           (let [position (or (committed admin (str db/app-prefix "-" process) tp) 0)]
                             (.get (.all (.deleteRecords admin {tp (RecordsToDelete/beforeOffset position)})) 30 TimeUnit/SECONDS)
                             {:to position :channel [topic partition]})))]
            (done result #{}))

          :delete-topic
          (do (with-admin test (fn [^Admin admin] (.get (.all (.deleteTopics admin [(:topic v)])) 30 TimeUnit/SECONDS)))
              (done {} #{:CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES :CHANNEL_IDENTITY_CHANGED}))

          ;; Delete and recreate under the same name while every instance is down: the next start
          ;; refuses CHANNEL_IDENTITY_CHANGED.
          :recreate-topic
          (let [n (count (:nodes test))
                old (with-admin test (fn [^Admin admin] (get (client/topic-ids admin) (:topic v))))]
            (c/on-nodes test (fn [t n] (db/kill-harness! t n)))
            (with-admin test
              (fn [^Admin admin]
                (.get (.all (.deleteTopics admin [(:topic v)])) 30 TimeUnit/SECONDS)
                (util/await-fn (fn [] (.get (.all (.createTopics admin [(NewTopic. ^String (:topic v) (int (:partitions test)) (short (min 3 n)))])) 30 TimeUnit/SECONDS))
                               {:retry-interval 1000 :log-interval 10000 :log-message "recreating"})))
            (let [new (with-admin test (fn [^Admin admin] (get (client/topic-ids admin) (:topic v))))]
              (c/on-nodes test (fn [t n] (db/start-harness! t n)))
              (done {:old old :new new :reinitialised-ends nil} #{:CHANNEL_IDENTITY_CHANGED :CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES})))

          ;; Reset the group's offsets backwards while every instance is down: re-fed records are
          ;; dropped as duplicates, nothing delivers twice, and no refusal is justified.
          :reset-offsets
          (let [{:keys [process topic partition back]} v
                tp (TopicPartition. topic partition)
                group (str db/app-prefix "-" process)]
            (c/on-nodes test (fn [t n] (db/kill-harness! t n)))
            (let [result (with-admin test
                           (fn [^Admin admin]
                             (let [position (or (committed admin group tp) 0)
                                   target (max 0 (- position back))]
                               (.get (.all (.alterConsumerGroupOffsets admin group {tp (OffsetAndMetadata. target)})) 30 TimeUnit/SECONDS)
                               {:from position :to target :channel [topic partition]})))]
              (c/on-nodes test (fn [t n] (db/start-harness! t n)))
              (done result #{})))

          ;; Delete the ordering changelog, keeping the group's offsets: ORDERING_STATE_LOST.
          :delete-changelog
          (let [changelog (str db/app-prefix "-" (:process v) "-__parsley.ordering-changelog")]
            (c/on-nodes test (fn [t n] (db/kill-harness! t n)))
            (with-admin test (fn [^Admin admin] (.get (.all (.deleteTopics admin [changelog])) 30 TimeUnit/SECONDS)))
            (c/on-nodes test (fn [t n] (db/wipe-harness-state! t n) (db/start-harness! t n)))
            (done {:changelog changelog} #{:ORDERING_STATE_LOST}))

          ;; Add partitions to the widest received topic, then restart: TASK_WIDTH_CHANGED.
          :add-partitions
          (do (c/on-nodes test (fn [t n] (db/kill-harness! t n)))
              (with-admin test (fn [^Admin admin]
                                 (.get (.all (.createPartitions admin {(:topic v) (NewPartitions/increaseTo (int (inc (:partitions test))))})) 30 TimeUnit/SECONDS)))
              (c/on-nodes test (fn [t n] (db/start-harness! t n)))
              (done {} #{:TASK_WIDTH_CHANGED})))))

    (teardown! [this test])))

(defn parsley-generator
  "Instance faults every interval; a refusal-class fault at most once per process and only
  when asked for, since a refusal is terminal by design."
  [opts]
  (let [faults (:nemesis opts)
        interval (:nemesis-interval opts 60)
        node (fn [test] (rand-nth (:nodes test)))
        instance (gen/mix (remove nil?
                                  [(when (:instance-kill faults)
                                     (fn [test _] {:type :info :f :kill-instance :value {:node (node test)}}))
                                   (when (:instance-kill faults)
                                     (fn [test _] {:type :info :f :restart-instance :value {:node (node test)}}))
                                   (when (:instance-pause faults)
                                     (gen/cycle [(fn [test _] {:type :info :f :pause-instance :value {:node (node test)}})
                                                 (gen/sleep 120)
                                                 (fn [test _] {:type :info :f :resume-instance :value {:node (node test)}})]))
                                   (when (:wipe faults)
                                     (fn [test _] {:type :info :f :wipe-restart-instance :value {:node (node test)}}))]))
        once (fn [f value] (when (faults f) (gen/once-per-test? nil)))]
    (gen/stagger interval instance)))

(defn parsley-package
  "A nemesis package in jepsen.nemesis.combined's shape, for the Parsley faults."
  [opts]
  {:nemesis (parsley-nemesis)
   :generator (parsley-generator opts)
   :final-generator (fn [test _] (map (fn [node] {:type :info :f :restart-instance :value {:node node}}) (:nodes test)))
   :perf #{{:name "instance" :fs #{:kill-instance :restart-instance :pause-instance :resume-instance} :color "#E9A4A0"}}})

(defn package
  "The combined nemesis: Jepsen's partition, kill, pause and clock faults over the brokers,
  and the Parsley faults over the instances and the topics."
  [opts]
  (let [faults (:nemesis opts)
        combined (nc/nemesis-package {:db (:db opts)
                                      :interval (:nemesis-interval opts 60)
                                      :faults (set (filter #{:partition :kill :pause :clock} faults))
                                      :partition {:targets [:one :majority :majorities-ring]}
                                      :kill {:targets [:one]}
                                      :pause {:targets [:one]}
                                      :clock {:targets [:one]}})]
    (nc/compose-packages [combined (parsley-package opts)])))
