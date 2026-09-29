(ns parsley-jepsen.client
  "Producer, consumer and admin clients behind the operations: external sends to the source
  topics, status polls of each node, read-position observations bracketed by the trace ends,
  the quiescence wait and the final dump.

  Operations and what an :ok result carries:
    {:f :send   :value {:topic t :key k :uid u :stamp {[topic-id p] pos} :malformed bool}}
      -> :value gains :partition and :offset, and :stamped-from when a stamp named a record
    {:f :status :value node}   -> :value {:node n :healthy b :processes {name {...}}}
    {:f :reads}                -> :value [reads...] as JepsenExport spells them
    {:f :quiesce}              -> :value {:lag [...]} once every running process has no lag
    {:f :dump}                 -> :value the export map (records, trace, topics) from the harness jar

  UNVERIFIED: written without a Clojars-reachable build; the Java side it mirrors
  (JepsenClusterExport) is tested against the embedded broker."
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.java.shell :as sh]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]
                    [util :as util]]
            [parsley-jepsen.db :as db])
  (:import [java.util Properties]
           [java.util.concurrent TimeUnit]
           [org.apache.kafka.clients.admin Admin AdminClientConfig ListOffsetsOptions OffsetSpec]
           [org.apache.kafka.clients.producer KafkaProducer ProducerConfig ProducerRecord]
           [org.apache.kafka.common IsolationLevel TopicPartition]
           [org.apache.kafka.common.header.internals RecordHeader]
           [org.apache.kafka.common.serialization StringSerializer]))

(def trace-topic "trace")
(def topics ["src" "a" "b" "c" "d" "loop" "self" "trace"])
(def processes {"splitter" ["src"] "joiner" ["a" "b" "loop"] "cycler" ["c"] "selfer" ["d" "self"]})

(defn producer [test]
  (let [props (doto (Properties.)
                (.put ProducerConfig/BOOTSTRAP_SERVERS_CONFIG (db/bootstrap-servers test))
                (.put ProducerConfig/ACKS_CONFIG "all")
                (.put ProducerConfig/ENABLE_IDEMPOTENCE_CONFIG "true")
                (.put ProducerConfig/DELIVERY_TIMEOUT_MS_CONFIG "30000")
                (.put ProducerConfig/MAX_BLOCK_MS_CONFIG "10000"))]
    (KafkaProducer. props (StringSerializer.) (StringSerializer.))))

(defn admin [test]
  (Admin/create {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG (db/bootstrap-servers test)
                 AdminClientConfig/REQUEST_TIMEOUT_MS_CONFIG "10000"
                 AdminClientConfig/DEFAULT_API_TIMEOUT_MS_CONFIG "15000"}))

;; ---- the causes header, encoded by the frozen grammar (wire-format.md) ----

(defn- write-varint [^java.io.ByteArrayOutputStream out value]
  (loop [v value]
    (if (zero? (bit-and v (bit-not 0x7F)))
      (.write out (int v))
      (do (.write out (int (bit-or (bit-and v 0x7F) 0x80)))
          (recur (unsigned-bit-shift-right v 7))))))

(defn- uuid-longs [^String id]
  (let [u (java.util.UUID/fromString id)]
    [(.getMostSignificantBits u) (.getLeastSignificantBits u)]))

(defn encode-causes
  "Encodes {[topic-id partition] position} as the parsley.causes value: topics ascending
  unsigned, partitions ascending, minimal varints, big-endian positions."
  ^bytes [causes]
  (let [groups (->> causes
                    (group-by (comp first key))
                    (sort-by (fn [[id _]] (let [[m l] (uuid-longs id)] [(Long/compareUnsigned m 0) m l]))
                             (fn [[_ am al] [_ bm bl]]
                               (let [c (Long/compareUnsigned am bm)] (if (zero? c) (Long/compareUnsigned al bl) c)))))
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

(defn- record [{:keys [topic partition key uid stamp malformed]}]
  (let [r (ProducerRecord. ^String topic ^Integer (some-> partition int) ^String key ^String uid)]
    (cond
      malformed (.add (.headers r) (RecordHeader. "parsley.causes" (byte-array [99 1 2 3])))
      stamp (.add (.headers r) (RecordHeader. "parsley.causes" (encode-causes stamp))))
    r))

;; ---- observations ----

(defn topic-ids [^Admin admin]
  (into {} (for [[name desc] (.get (.allTopicNames (.describeTopics admin topics)) 30 TimeUnit/SECONDS)]
             [name (let [id (.topicId desc)] (str (java.util.UUID. (.getMostSignificantBits id) (.getLeastSignificantBits id))))])))

(defn- list-offsets [^Admin admin partitions spec isolation]
  (let [query (into {} (map (fn [tp] [tp spec]) partitions))
        result (.get (.all (.listOffsets admin query (ListOffsetsOptions. isolation))) 30 TimeUnit/SECONDS)]
    (into {} (map (fn [[tp info]] [tp (.offset info)]) result))))

(defn trace-ends
  "The end of every trace partition, keyed by partition as text: the last stable offset under
  READ_COMMITTED, the high watermark under READ_UNCOMMITTED."
  [^Admin admin n isolation]
  (into {} (map (fn [[tp off]] [(str (.partition tp)) off])
                (list-offsets admin (map #(TopicPartition. trace-topic %) (range n)) (OffsetSpec/latest) isolation))))

(defn committed
  "The group's committed positions by TopicPartition, or {} when the group does not exist."
  [^Admin admin group]
  (try
    (into {} (for [[tp om] (.get (.partitionsToOffsetAndMetadata (.listConsumerGroupOffsets admin group)) 30 TimeUnit/SECONDS)
                   :when om]
               [tp (.offset om)]))
    (catch java.util.concurrent.ExecutionException e
      (if (instance? org.apache.kafka.common.errors.GroupIdNotFoundException (.getCause e)) {} (throw e)))))

(defn observe-reads
  "One observation per task: the last stable offsets of the trace before the group offsets,
  the high watermarks after, as JepsenExport records them."
  [^Admin admin test ids]
  (let [n (:partitions test)
        lo (trace-ends admin n IsolationLevel/READ_COMMITTED)
        by-process (into {} (for [p (keys processes)] [p (committed admin (str db/app-prefix "-" p))]))
        hi (trace-ends admin n IsolationLevel/READ_UNCOMMITTED)]
    (vec (for [[process offsets] by-process
               [task next-read] (->> offsets
                                     (group-by (fn [[^TopicPartition tp _]] (.partition tp)))
                                     (map (fn [[task pairs]]
                                            [task (into {} (for [[^TopicPartition tp off] pairs
                                                                 :let [id (get ids (.topic tp))]
                                                                 :when id]
                                                             [[id (.partition tp)] off]))])))]
           {:process process :task task :ends-lo lo :ends-hi hi :next-read next-read :exec-start {}}))))

(defn lag
  "Lines naming every received partition a running process has not committed reading to the end of."
  [^Admin admin test refused]
  (vec (for [[process received] processes
             :when (not (contains? refused process))
             :let [partitions (for [topic received p (range (:partitions test))] (TopicPartition. topic p))
                   ends (list-offsets admin partitions (OffsetSpec/latest) IsolationLevel/READ_COMMITTED)
                   positions (committed admin (str db/app-prefix "-" process))]
             tp partitions
             :let [end (get ends tp 0) position (get positions tp)]
             :when (if (nil? position) (pos? end) (< position end))]
         (str process " " tp " committed " position " of " end))))

(defn status
  "The harness's status EDN from one node."
  [node]
  (edn/read-string (slurp (str "http://" node ":" db/status-port "/status"))))

(defn refused-processes [test]
  (set (for [node (:nodes test)
             [process s] (try (:processes (status node)) (catch Exception _ nil))
             :when (:refusal s)]
         process)))

(defn dump
  "The final dump through the harness jar on the control node: every topic from earliest
  under read_committed, headers included, and the trace."
  [test]
  (let [out (java.io.File/createTempFile "parsley-dump" ".edn")
        result (sh/sh "java" "-jar" (:harness-jar test) "export"
                      "--bootstrap" (db/bootstrap-servers test)
                      "--out" (.getPath out))]
    (when-not (zero? (:exit result))
      (throw (ex-info "export failed" result)))
    (edn/read-string (slurp out))))

;; ---- the client ----

(defrecord Client [producer admin ids]
  client/Client
  (open! [this test node]
    (let [admin (admin test)]
      (assoc this :producer (producer test) :admin admin :ids (atom nil))))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      :send (let [r (record (:value op))
                  md (.get (.send ^KafkaProducer producer r) 30 TimeUnit/SECONDS)]
              (assoc op :type :ok :value (assoc (:value op) :partition (.partition md) :offset (.offset md))))
      :status (assoc op :type :ok :value (assoc (status (:value op)) :node (:value op)))
      :reads (let [ids (or @ids (reset! ids (topic-ids admin)))]
               (assoc op :type :ok :value (observe-reads admin test ids)))
      :quiesce (let [deadline (+ (System/nanoTime) (* 600 1e9))]
                 (loop [stable 0 previous nil]
                   (let [lagging (lag admin test (refused-processes test))
                         ends (trace-ends admin (:partitions test) IsolationLevel/READ_COMMITTED)
                         stable (if (and (empty? lagging) (= ends previous)) (inc stable) 0)]
                     (cond
                       (>= stable 3) (assoc op :type :ok :value {:lag lagging})
                       (> (System/nanoTime) deadline) (assoc op :type :fail :value {:lag lagging})
                       :else (do (Thread/sleep 2000) (recur stable ends))))))
      :dump (assoc op :type :ok :value (dump test))))

  (teardown! [this test])

  (close! [this test]
    (.close ^KafkaProducer producer)
    (.close ^Admin admin)))

(defn client [opts]
  (map->Client {}))
