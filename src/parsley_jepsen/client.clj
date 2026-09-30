(ns parsley-jepsen.client
  "Producer, consumer and admin clients behind the operations: external sends to the source
  topics, status polls of each node, read-position observations bracketed by the trace ends,
  the quiescence wait and the final dump.

  Operations and what an :ok result carries:
    {:f :send   :value {:topic t :key k :uid u :stamp {[topic-id p] pos} :malformed bool
                        :out-of-contract {:topic t :partition p} :understamp bool}}
      -> :value gains :partition and :offset; an :out-of-contract send gains the :stamp it
         made and the :position it named
    {:f :status :value node}   -> :value {:node n :healthy b :processes {name {...}}}
    {:f :reads}                -> :value [reads...] as JepsenExport spells them
    {:f :quiesce}              -> :value {:lag [...]} once every running process has no lag
    {:f :dump}                 -> :value {:file path :topic-ids {name id} :records n :trace n};
                                  the export map (records, trace, topics) is written to the
                                  store directory, and the ids are those the run began with

  A send that fails is :info, since the record may have been written; an observation that
  fails is :fail, since nothing happened.

  The test map carries two atoms. :acked holds every acknowledged send, as
  {:uid u :topic t :topic-id id :partition p :offset o :causes {...}}, which the workload
  stamps from. :topic-ids holds each topic's id as it was when the run began: a task's
  committed positions are attributed to the incarnation it attached to, and no process in
  this test ever attaches to a recreated one (it refuses, or is redeclared without it)."
  (:require [clojure.edn :as edn]
            [clojure.java.shell :as sh]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]
                    [store :as store]]
            [parsley-jepsen.db :as db])
  (:import [java.net HttpURLConnection URL]
           [java.util Properties]
           [java.util.concurrent ExecutionException TimeUnit]
           [org.apache.kafka.clients.admin Admin AdminClientConfig ListOffsetsOptions OffsetSpec]
           [org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerRecord RecordMetadata]
           [org.apache.kafka.common IsolationLevel TopicPartition]
           [org.apache.kafka.common.errors GroupIdNotFoundException]
           [org.apache.kafka.common.header.internals RecordHeader]
           [org.apache.kafka.common.serialization StringSerializer]))

(def trace-topic db/trace-topic)
(def topics db/topics)
(def processes {"splitter" ["src"] "joiner" ["a" "b" "loop"] "cycler" ["c"] "selfer" ["d" "self"]})

(defn make-producer ^KafkaProducer [test]
  (let [props (doto (Properties.)
                (.put ProducerConfig/BOOTSTRAP_SERVERS_CONFIG (db/bootstrap-servers test))
                (.put ProducerConfig/ACKS_CONFIG "all")
                (.put ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG "true")
                (.put ProducerConfig/REQUEST_TIMEOUT_MS_CONFIG "10000")
                (.put ProducerConfig/DELIVERY_TIMEOUT_MS_CONFIG "30000")
                (.put ProducerConfig/MAX_BLOCK_MS_CONFIG "10000"))]
    (KafkaProducer. props (StringSerializer.) (StringSerializer.))))

(def make-admin db/admin)

(defn make-views
  "One admin client per broker, bootstrapped from that broker alone, so an observation
  can ask every broker's view. Short timeouts: a broker cut off by a partition answers
  slowly or not at all, and its view is then simply missing."
  [test]
  (mapv (fn [node]
          (Admin/create ^java.util.Map
                        {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG (str node ":9092")
                         AdminClientConfig/REQUEST_TIMEOUT_MS_CONFIG "4000"
                         AdminClientConfig/DEFAULT_API_TIMEOUT_MS_CONFIG "6000"}))
        (:nodes test)))

;; ---- the causes header, encoded by the frozen grammar (wire-format.md) ----

(defn- write-varint [^java.io.ByteArrayOutputStream out value]
  (loop [v (long value)]
    (if (zero? (bit-and v (bit-not 0x7F)))
      (.write out (int v))
      (do (.write out (int (bit-or (bit-and v 0x7F) 0x80)))
          (recur (unsigned-bit-shift-right v 7))))))

(defn- uuid-longs [^String id]
  (let [u (java.util.UUID/fromString id)]
    [(.getMostSignificantBits u) (.getLeastSignificantBits u)]))

(defn- compare-ids
  "Topic ids ascending as unsigned 128-bit numbers."
  [a b]
  (let [[am al] (uuid-longs a)
        [bm bl] (uuid-longs b)
        c (Long/compareUnsigned am bm)]
    (if (zero? c) (Long/compareUnsigned al bl) c)))

(defn encode-causes
  "Encodes {[topic-id partition] position} as the parsley.causes value: topics ascending
  unsigned, partitions ascending, minimal varints, big-endian positions."
  ^bytes [causes]
  (let [groups (sort-by key compare-ids (group-by (comp first key) causes))
        out (java.io.ByteArrayOutputStream.)]
    (.write out 1)
    (write-varint out (count groups))
    (doseq [[id pairs] groups]
      (let [[m l] (uuid-longs id)
            buf (java.nio.ByteBuffer/allocate 16)]
        (.putLong buf m) (.putLong buf l)
        (.write out (.array buf) 0 16)
        (write-varint out (count pairs))
        (doseq [[[_ partition] position] (sort-by (comp second key) pairs)]
          (write-varint out partition)
          (let [pb (java.nio.ByteBuffer/allocate 8)]
            (.putLong pb (long position))
            (.write out (.array pb) 0 8)))))
    (.toByteArray out)))

(defn record
  "The producer record for a send: an under-stamped send carries no header however much
  its sender knows, which is the lie the calibration run must catch."
  ^ProducerRecord [{:keys [topic partition key uid stamp malformed understamp]}]
  (let [r (ProducerRecord. ^String topic (when partition (Integer/valueOf (int partition))) key uid)]
    (cond
      malformed (.add (.headers r) (RecordHeader. "parsley.causes" (byte-array [99 1 2 3])))
      understamp nil
      (seq stamp) (.add (.headers r) (RecordHeader. "parsley.causes" (encode-causes stamp))))
    r))

;; ---- observations ----

(defn- uuid-str [^org.apache.kafka.common.Uuid id]
  (str (java.util.UUID. (.getMostSignificantBits id) (.getLeastSignificantBits id))))

(defn describe-ids
  "The id of every named topic that exists, by name."
  [^Admin admin names]
  (into {} (for [[name future] (.topicNameValues (.describeTopics admin ^java.util.Collection names))
                 :let [description (try (.get ^org.apache.kafka.common.KafkaFuture future 30 TimeUnit/SECONDS)
                                        (catch ExecutionException e
                                          (when-not (instance? org.apache.kafka.common.errors.UnknownTopicOrPartitionException (.getCause e))
                                            (throw e))))]
                 :when description]
             [name (uuid-str (.topicId ^org.apache.kafka.clients.admin.TopicDescription description))])))

(defn topic-ids
  "Each topic's id as it was when the run began, read once and kept in the test map."
  [test ^Admin admin]
  (let [ids (:topic-ids test)]
    (or @ids
        (locking ids
          (or @ids (reset! ids (describe-ids admin topics)))))))

(defn list-offsets
  "The latest or earliest offset of each partition under `isolation`."
  [^Admin admin partitions ^OffsetSpec spec ^IsolationLevel isolation]
  (let [query (into {} (map (fn [tp] [tp spec]) partitions))
        result (.get (.all (.listOffsets admin query (ListOffsetsOptions. isolation))) 30 TimeUnit/SECONDS)]
    (into {} (map (fn [[tp info]] [tp (.offset ^org.apache.kafka.clients.admin.ListOffsetsResult$ListOffsetsResultInfo info)]) result))))

(defn log-end
  "The first unassigned offset of a partition: its high watermark."
  [^Admin admin ^TopicPartition tp]
  (get (list-offsets admin [tp] (OffsetSpec/latest) IsolationLevel/READ_UNCOMMITTED) tp))

(defn trace-ends
  "The end of every trace partition, keyed by partition as text: the last stable offset under
  READ_COMMITTED, the high watermark under READ_UNCOMMITTED."
  [^Admin admin n isolation]
  (into {} (map (fn [[^TopicPartition tp off]] [(str (.partition tp)) off])
                (list-offsets admin (map #(TopicPartition. trace-topic (int %)) (range n)) (OffsetSpec/latest) isolation))))

(defn committed
  "The group's committed positions by TopicPartition, or {} when the group does not exist."
  [^Admin admin ^String group]
  (try
    (into {} (for [[tp om] (.get (.partitionsToOffsetAndMetadata (.listConsumerGroupOffsets admin group)) 30 TimeUnit/SECONDS)
                   :when om]
               [tp (.offset ^org.apache.kafka.clients.consumer.OffsetAndMetadata om)]))
    (catch ExecutionException e
      (if (instance? GroupIdNotFoundException (.getCause e)) {} (throw e)))))

(defn group-id [process]
  (str db/app-prefix "-" process))

(defn- freshest
  "The freshest of several brokers' views of values that only grow: their maximum per key.
  A broker cut off by a partition keeps answering from a stale cache, as a deposed group
  coordinator or partition leader, and an observation taken from it alone would place a
  step's receipts before positions that were committed after it. Every broker is asked and
  a view that fails is left out."
  [f views]
  (apply merge-with max {} (keep (fn [^Admin view] (try (f view) (catch Exception _ nil))) views)))

(defn observe-reads
  "One observation per task: the last stable offsets of the trace before the group offsets,
  the high watermarks after, as JepsenExport records them, each the freshest of every
  broker's view. The order is what makes both bounds on receipt sound; do not collapse it
  to one read."
  [views test ids]
  (let [n (:partitions test)
        lo (freshest #(trace-ends % n IsolationLevel/READ_COMMITTED) views)
        by-process (into {} (for [p (keys processes)] [p (freshest #(committed % (group-id p)) views)]))
        hi (freshest #(trace-ends % n IsolationLevel/READ_UNCOMMITTED) views)]
    (vec (for [[process offsets] by-process
               [task next-read] (->> offsets
                                     (group-by (fn [[^TopicPartition tp _]] (.partition tp)))
                                     (map (fn [[task pairs]]
                                            [task (into {} (for [[^TopicPartition tp off] pairs
                                                                 :let [id (get ids (.topic tp))]
                                                                 :when id]
                                                             [[id (.partition tp)] off]))]))
                                     (sort-by first))]
           {:process process :task task :ends-lo lo :ends-hi hi :next-read next-read :exec-start {}}))))

(defn lag
  "Lines naming every received partition a running process has not committed reading to
  the end of. Topics in `gone` no longer exist and are left out."
  [^Admin admin test refused gone]
  (vec (for [[process received] processes
             :when (not (contains? refused process))
             :let [partitions (for [topic received :when (not (contains? gone topic))
                                    p (range (:partitions test))]
                                (TopicPartition. topic (int p)))
                   ends (list-offsets admin partitions (OffsetSpec/latest) IsolationLevel/READ_COMMITTED)
                   positions (committed admin (group-id process))]
             tp partitions
             :let [end (get ends tp 0) position (get positions tp)]
             :when (if (nil? position) (pos? end) (< position end))]
         (str process " " tp " committed " position " of " end))))

(defn status
  "The harness's status EDN from one node."
  [node]
  (let [connection ^HttpURLConnection (.openConnection (URL. (str "http://" node ":" db/status-port "/status")))]
    (.setConnectTimeout connection 5000)
    (.setReadTimeout connection 5000)
    (try
      (with-open [in (.getInputStream connection)]
        (edn/read-string (slurp in)))
      (finally (.disconnect connection)))))

(defn refused-processes [test]
  (set (for [node (:nodes test)
             [process s] (try (:processes (status node)) (catch Exception _ nil))
             :when (:refusal s)]
         process)))

(defn dump
  "The final dump through the harness jar on the control node: every topic from earliest
  under read_committed, headers included, and the trace. Written to the store directory."
  [test ^Admin admin]
  (let [ids (topic-ids test admin)
        out (store/path! test "dump.edn")
        result (sh/sh "java" "-jar" (:harness-jar test) "export"
                      "--bootstrap" (db/bootstrap-servers test)
                      "--out" (.getPath out))]
    (when-not (zero? (:exit result))
      (throw (ex-info "export failed" result)))
    (let [export (edn/read-string (slurp out))]
      {:file (.getPath out) :topic-ids ids :records (count (:records export)) :trace (count (:trace export))})))

;; ---- the client ----

(defn- send!
  "Sends one record and records the acknowledgement. An out-of-contract send first reads
  the named partition's log end and stamps that offset, which no record occupies yet."
  [test ^KafkaProducer producer ^Admin admin op]
  (let [ids (topic-ids test admin)
        v (:value op)
        v (if-let [{:keys [topic partition]} (:out-of-contract v)]
            (let [end (log-end admin (TopicPartition. topic (int partition)))]
              (assoc v
                     :stamp {[(get ids topic) partition] end}
                     :out-of-contract {:topic topic :partition partition :position end}))
            v)
        md ^RecordMetadata (.get (.send producer (record v)) 30 TimeUnit/SECONDS)
        v (assoc v :partition (.partition md) :offset (.offset md))]
    (swap! (:acked test) conj {:uid (:uid v) :topic (:topic v) :topic-id (get ids (:topic v))
                               :partition (:partition v) :offset (:offset v)
                               :causes (if (or (:malformed v) (:understamp v)) {} (:stamp v {}))})
    (assoc op :type :ok :value v)))

(defn- quiesce
  "Waits for zero lag on every received partition of every process that has not refused,
  with the trace ends unchanged across three polls two seconds apart."
  [test ^Admin admin op]
  (let [deadline (+ (System/nanoTime) (long (* (:quiesce-seconds test 600) 1e9)))]
    (loop [stable 0 previous nil]
      (let [lagging (lag admin test (refused-processes test) @(:gone-topics test))
            ends (trace-ends admin (:partitions test) IsolationLevel/READ_COMMITTED)
            stable (if (and (empty? lagging) (= ends previous)) (inc stable) 0)]
        (cond
          (>= stable 3) (assoc op :type :ok :value {:lag lagging :trace-ends ends})
          (> (System/nanoTime) deadline) (assoc op :type :fail :value {:lag lagging :trace-ends ends})
          :else (do (Thread/sleep 2000) (recur stable ends)))))))

(defn- error [^Throwable e]
  (str (.getName (class e)) ": " (.getMessage e)))

(defrecord Client [producer admin views]
  client/Client
  (open! [this test node]
    (assoc this :producer (make-producer test) :admin (make-admin test) :views (make-views test)))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      :send (try (send! test producer admin op)
                 (catch Exception e
                   (assoc op :type :info :error (error e))))
      (try
        (case (:f op)
          :status (assoc op :type :ok :value (assoc (status (:value op)) :node (:value op)))
          :reads (assoc op :type :ok :value (observe-reads views test (topic-ids test admin)))
          :quiesce (quiesce test admin op)
          :dump (assoc op :type :ok :value (dump test admin)))
        (catch Exception e
          (assoc op :type :fail :error (error e))))))

  (teardown! [this test])

  (close! [this test]
    (.close ^KafkaProducer producer (java.time.Duration/ofSeconds 5))
    (.close ^Admin admin (java.time.Duration/ofSeconds 5))
    (doseq [^Admin view views]
      (.close view (java.time.Duration/ofSeconds 5)))))

(defn client [opts]
  (map->Client {}))
