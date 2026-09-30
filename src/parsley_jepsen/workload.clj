(ns parsley-jepsen.workload
  "The generator: producer and status operations.

  Producer clients send to the source topics: unstamped records, which Safety 6 makes
  immediately deliverable; stamped records whose header names a record the client has
  already observed committed, with that record's own causes merged in as the simulator's
  external producers do; and occasionally an out-of-contract stamp naming a channel's
  log-end offset, or a malformed header. Status clients poll each node's status and the
  read positions into the history, so refusals are timestamped against faults and receipt
  is bracketed by the trace ends. core.clj builds the final phase.

  The calibration generator manufactures a live inversion: a producer that stamps less
  than it knows, which the checker must flag."
  (:require [jepsen.generator :as gen]))

(defrecord After [deadline gen]
  gen/Generator
  (op [_ test ctx]
    (when-let [[op gen'] (gen/op gen test ctx)]
      (if (= :pending op)
        [:pending (After. deadline gen')]
        [(if (< (:time op) deadline) (assoc op :time deadline) op) (After. deadline gen')])))
  (update [_ test ctx event]
    (After. deadline (gen/update gen test ctx event))))

(defn after
  "A generator whose first op from `gen` is scheduled no earlier than `seconds` into the
  test's clock, however many threads are free: a sleep op only holds the thread that
  performs it, so a sequence of a sleep and an op runs the op at once on another thread.
  The deadline is on the clock, not from the first ask, since a generator that is asked and
  not chosen keeps its old state."
  [seconds gen]
  (when gen
    (After. (long (* seconds 1e9)) gen)))

(defn acked
  "The atom in the test map where the client records every acknowledged send, as
  {:uid u :topic t :topic-id id :partition p :offset o :causes {...}}; the generator stamps
  from it."
  [test]
  (:acked test))

(defn- gone? [test topic]
  (contains? @(:gone-topics test) topic))

(defn unstamped-send [counter]
  (fn [test _]
    (when-not (gone? test "src")
      (let [uid (str "x" (swap! counter inc))]
        {:f :send :value {:topic "src" :key uid :uid uid}}))))

(defn stamped-send
  "A record on b naming a record the client observed committed on a, placed on the same
  partition so the joiner task for it must deliver the cause first; the observed record's
  own causes are merged so the stamp expresses every cause (wire-format.md)."
  [counter]
  (fn [test _]
    (when-not (or (gone? test "a") (gone? test "b"))
      (let [observed (->> @(acked test) (filter #(= "a" (:topic %))) seq)
            observed (when observed (rand-nth observed))]
        (if (nil? observed)
          (let [uid (str "ea" (swap! counter inc))]
            {:f :send :value {:topic "a" :key uid :uid uid}})
          (let [uid (str "sb" (swap! counter inc))
                causes (merge (:causes observed) {[(:topic-id observed) (:partition observed)] (:offset observed)})]
            {:f :send :value {:topic "b" :partition (:partition observed) :key uid :uid uid
                              :stamp causes
                              :stamped-from [(:topic-id observed) (:partition observed) (:offset observed)]}}))))))

(defn external-a-send
  "An unstamped record on a, for later stamped sends to name."
  [counter]
  (fn [test _]
    (when-not (gone? test "a")
      (let [uid (str "ea" (swap! counter inc))]
        {:f :send :value {:topic "a" :key uid :uid uid}}))))

(defn out-of-contract-send
  "A stamp naming a's log-end offset on a random partition: held until the channel's next
  record settles it, never refused (wire-format constraint 8). The client looks the offset
  up when it sends."
  [counter]
  (fn [test _]
    (when-not (or (gone? test "a") (gone? test "b"))
      (let [uid (str "oc" (swap! counter inc))
            partition (rand-int (:partitions test))]
        {:f :send :value {:topic "b" :partition partition :key uid :uid uid
                          :out-of-contract {:topic "a" :partition partition}}}))))

(defn malformed-send
  "A malformed parsley.causes header on `topic`: the receiving process refuses
  UNDECODABLE_METADATA, which is terminal, so a run schedules it at most once, and only
  when asked."
  [topic]
  {:f :send :value {:topic topic :key "garbage" :uid "garbage" :malformed true}})

(defn status-op [test _]
  {:f :status :value (rand-nth (:nodes test))})

(defn reads-op [_ _]
  {:f :reads})

(defn- weighted [& weights-and-gens]
  (mapcat (fn [[weight gen]] (repeat weight gen)) (partition 2 weights-and-gens)))

(defn inversion
  "The calibration sequence, run while nothing else sends. H goes to a's partition 0 with
  an out-of-contract stamp naming b's log end there, so the joiner holds it. E then goes to
  that position of b carrying no header, though its producer had observed H and the
  history records that it had. The joiner delivers E at once, which settles H's stamp, and
  H after it: an effect before its cause, which no honest stamper produces."
  []
  (gen/phases
   {:f :send :value {:topic "a" :partition 0 :key "inv-h" :uid "inv-h"
                     :out-of-contract {:topic "b" :partition 0}}}
   (gen/once
    (fn [test _]
      (when-let [h (first (filter #(= "inv-h" (:uid %)) @(acked test)))]
        {:f :send :value {:topic "b" :partition 0 :key "inv-e" :uid "inv-e" :understamp true
                          :stamped-from [(:topic-id h) (:partition h) (:offset h)]}})))
   (gen/sleep 5)))

(defn generator
  "The main phase: sends at the requested rate with a status and a reads observation every
  second, on client threads. With :calibrate :inversion the manufactured inversion runs
  first; with :corrupt among the faults one malformed record is sent to the topic
  `corrupt-topic` names, one nemesis interval in."
  [opts corrupt-topic]
  (let [counter (atom 0)
        sends (gen/mix (weighted 6 (unstamped-send counter)
                                 2 (stamped-send counter)
                                 1 (external-a-send counter)
                                 (if (:out-of-contract opts) 1 0) (out-of-contract-send counter)))
        sends (gen/stagger (/ 1 (:rate opts 5)) sends)
        observations (gen/stagger 1/2 (gen/mix [status-op reads-op]))
        corrupt (when corrupt-topic
                  (after (:nemesis-interval opts 60) (malformed-send corrupt-topic)))
        main (gen/any sends observations corrupt)]
    (if (= :inversion (:calibrate opts))
      (gen/phases (inversion) main)
      main)))
