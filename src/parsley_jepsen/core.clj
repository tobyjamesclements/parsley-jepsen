(ns parsley-jepsen.core
  "Command line and test map. `lein run test --nodes-file nodes.txt --kafka-version 4.3.1
  --time-limit 3600 --nemesis partition,pause,kill,truncate --nemesis-interval 60`.

  The checker here is the Jepsen adapter: it assembles the export the pure checker judges
  from the history (the final dump, the read observations, the status polls and the
  nemesis ops as faults) and hands it to parsley-jepsen.checker.

  UNVERIFIED: written without a Clojars-reachable build; the pure checker it calls is
  calibrated by its tests."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os.debian :as debian]
            [parsley-jepsen [checker :as pchecker]
                            [client :as client]
                            [db :as db]
                            [nemesis :as pnemesis]
                            [workload :as workload]]))

(def cli-opts
  [[nil "--kafka-version VERSION" "Apache Kafka version to install." :default "4.3.1"]
   [nil "--harness-jar PATH" "The Parsley Jepsen harness jar (./mvnw -Pjepsen-harness -DskipTests package in parsley)."
    :default "parsley-jepsen-harness.jar"]
   [nil "--partitions N" "Partitions per topic, and so tasks per process." :default 3 :parse-fn parse-long]
   [nil "--rate HZ" "Sends per second." :default 5 :parse-fn parse-double]
   [nil "--nemesis FAULTS" "Comma-separated: partition,kill,pause,clock,instance-kill,instance-pause,wipe."
    :default #{:partition :instance-kill}
    :parse-fn (fn [s] (set (map keyword (str/split s #","))))]
   [nil "--nemesis-interval SECONDS" "Seconds between nemesis operations; rebalances and transaction timeouts run to tens of seconds."
    :default 60 :parse-fn parse-long]
   [nil "--out-of-contract" "Send occasional stamps naming a log-end offset." :default true]
   [nil "--unclean-leader-election" "Turn unclean leader election on: a separately labelled run." :default false]])

;; ---- the history-to-export adapter ----

(defn- nearest-ends
  "The trace ends the last read observation before `index` saw, or {}."
  [reads index]
  (or (->> reads (filter #(< (:index %) index)) last :ends-hi) {}))

(defn export-from-history
  "Assembles a JepsenExport from the history: the dump's records, trace and topics; every
  :reads result with its history index; every :status result per process; every nemesis
  op and every malformed send as a fault with the refusals it justifies."
  [test history]
  (let [ok? #(= :ok (:type %))
        dump (->> history (filter #(and (ok? %) (= :dump (:f %)))) last :value)
        reads (vec (for [op history :when (and (ok? op) (= :reads (:f op)))
                         r (:value op)]
                     (assoc r :index (:index op) :time (:time op))))
        statuses (vec (for [op history :when (and (ok? op) (= :status (:f op)))
                            [process s] (:processes (:value op))]
                        {:index (:index op) :time (:time op) :process process
                         :lifecycle (:lifecycle s) :refusal (:refusal s) :detail (:detail s)
                         :trace-ends (nearest-ends reads (:index op))}))
        faults (vec (concat
                     (for [op history :when (and (= :nemesis (:process op)) (= :info (:type op)) (:justifies (:value op)))]
                       {:index (:index op) :time (:time op) :kind (:f op)
                        :justifies (vec (:justifies (:value op)))
                        :trace-ends (nearest-ends reads (:index op))
                        :details (dissoc (:value op) :justifies)})
                     (for [op history :when (and (ok? op) (= :send (:f op)) (:malformed (:value op)))]
                       {:index (:index op) :time (:time op) :kind :corrupt
                        :justifies [:UNDECODABLE_METADATA]
                        :trace-ends (nearest-ends reads (:index op))
                        :details {:uid (:uid (:value op))}})))
        stamped-from (into {} (for [op history :when (and (ok? op) (= :send (:f op)) (:stamped-from (:value op)))]
                                [(:uid (:value op)) (:stamped-from (:value op))]))
        ids (into {} (map (fn [t] [(:name t) (:id t)]) (:topics dump)))
        tasks (vec (for [[process received] client/processes p (range (:partitions test))]
                     {:process process :task p :receives (vec (for [topic received] [(get ids topic) p]))}))
        starts (vec (for [{:keys [process task receives]} tasks]
                      {:process process :task task :positions (into {} (map (fn [ch] [ch 0]) receives))}))]
    (-> dump
        (update :records (fn [records] (mapv (fn [r] (if-let [from (get stamped-from (:uid r))] (assoc r :stamped-from from) r)) records)))
        (assoc :reads reads :statuses statuses :faults faults :tasks tasks :start-positions starts))))

(defn parsley-checker
  "Judges the run with parsley-jepsen.checker over the export assembled from the history."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (if-not (some #(and (= :ok (:type %)) (= :dump (:f %))) history)
        {:valid? :unknown :error "no final dump in the history"}
        (let [export (export-from-history test history)
              {:keys [valid? violations]} (pchecker/check export)]
          {:valid? valid?
           :violation-count (count violations)
           :violations (take 200 violations)
           :records (count (:records export))
           :trace (count (:trace export))})))))

(defn parsley-test
  "A test map from CLI options."
  [opts]
  (let [nemesis (pnemesis/package (assoc opts :db (db/db opts)))
        final (gen/phases
               (gen/clients {:f :quiesce})
               (gen/clients {:f :reads})
               (gen/clients (gen/each-thread (map (fn [node] {:f :status :value node}) (:nodes opts))))
               (gen/clients {:f :dump}))]
    (merge tests/noop-test
           opts
           {:name (str "parsley-" (:kafka-version opts) (when (:unclean-leader-election opts) "-unclean"))
            :os debian/os
            :db (db/db opts)
            :cluster-id (db/cluster-id)
            :acked (atom [])
            :client (client/client opts)
            :nemesis (:nemesis nemesis)
            :generator (gen/phases
                        (->> (workload/generator opts)
                             (gen/nemesis (:generator nemesis))
                             (gen/time-limit (:time-limit opts)))
                        (gen/nemesis (:final-generator nemesis))
                        (gen/sleep 30)
                        final)
            :checker (checker/compose {:perf (checker/perf {:nemeses (:perf nemesis)})
                                       :timeline (timeline/html)
                                       :stats (checker/stats)
                                       :parsley (parsley-checker)})})))

(defn -main
  "Handles command line arguments: `lein run test ...` and `lein run serve`."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn parsley-test :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))
