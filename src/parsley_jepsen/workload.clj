(ns parsley-jepsen.workload
  "The generator: producer and status operations, and the final phase.

  Producer clients send to the source topics: unstamped records, which Safety 6 makes
  immediately deliverable; stamped records whose header names a record the client has
  already observed committed, with that record's own causes merged in as the simulator's
  external producers do; and occasionally an out-of-contract stamp naming a channel's
  log-end offset, or a malformed header. Status clients poll each node's status and the
  read positions into the history, so refusals are timestamped against faults and receipt
  is bracketed by the trace ends. The final phase heals, waits for zero lag, then dumps.

  UNVERIFIED: written without a Clojars-reachable build."
  (:require [clojure.string :as str]
            [jepsen [generator :as gen]]
            [parsley-jepsen.client :as client]))

(def source-topics ["src" "a" "b"])

(defn acked
  "The atom in the test map where the client records every acknowledged send, as
  {:uid u :topic t :partition p :offset o :causes {...}}; the generator stamps from it."
  [test]
  (:acked test))

(defn unstamped-send [counter]
  (fn [test _]
    (let [uid (str "x" (swap! counter inc))]
      {:f :send :value {:topic "src" :key uid :uid uid}})))

(defn stamped-send
  "A record on b naming a record the client observed committed on a, placed on the same
  partition so the joiner task for it must deliver the cause first; the observed record's
  own causes are merged so the stamp expresses every cause (wire-format.md)."
  [counter]
  (fn [test _]
    (let [observed (->> @(acked test) (filter #(= "a" (:topic %))) shuffle first)]
      (if (nil? observed)
        (let [uid (str "ea" (swap! counter inc))]
          {:f :send :value {:topic "a" :key uid :uid uid}})
        (let [uid (str "sb" (swap! counter inc))
              causes (merge (:causes observed) {[(:topic-id observed) (:partition observed)] (:offset observed)})]
          {:f :send :value {:topic "b" :partition (:partition observed) :key uid :uid uid
                            :stamp causes :stamped-from [(:topic-id observed) (:partition observed) (:offset observed)]}})))))

(defn out-of-contract-send
  "A stamp naming a's log-end offset on a random partition: held until the channel's next
  record settles it, never refused (wire-format constraint 8)."
  [counter]
  (fn [test _]
    (let [uid (str "oc" (swap! counter inc))
          partition (rand-int (:partitions test))]
      {:f :send :value {:topic "b" :partition partition :key uid :uid uid
                        :out-of-contract {:topic "a" :partition partition}}})))

(defn malformed-send
  "A malformed parsley.causes header: the receiving process refuses UNDECODABLE_METADATA,
  which is terminal, so a run schedules it at most once, and only when asked."
  [counter]
  (fn [test _]
    (let [uid (str "g" (swap! counter inc))]
      {:f :send :value {:topic "src" :key uid :uid uid :malformed true}})))

(defn status-op [test _]
  {:f :status :value (rand-nth (:nodes test))})

(def reads-op {:f :reads})

(defn generator
  "The main phase: sends at the requested rate with a status and a reads observation every
  second, on client threads."
  [opts]
  (let [counter (atom 0)
        sends (gen/mix [(unstamped-send counter)
                        (unstamped-send counter)
                        (stamped-send counter)
                        (when (:out-of-contract opts) (out-of-contract-send counter))])
        sends (gen/stagger (/ 1 (:rate opts 5)) (remove nil? sends))
        observations (gen/stagger 1 (gen/mix [status-op reads-op]))]
    (gen/mix [sends observations])))

(defn final-generator
  "After the nemesis has healed: wait for zero lag on every received partition of every
  process that has not refused, take a last observation, then dump every topic."
  []
  (gen/phases
   (gen/clients {:f :quiesce})
   (gen/clients {:f :reads})
   (gen/clients (map (fn [node] {:f :status :value node}) (:nodes {}))) ; filled by core with the nodes
   (gen/clients {:f :dump})))
