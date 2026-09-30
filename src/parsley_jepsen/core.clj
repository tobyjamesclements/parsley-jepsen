(ns parsley-jepsen.core
  "Command line and test map. `lein run test --nodes-file nodes.txt --kafka-version 4.3.1
  --time-limit 3600 --nemesis partition,instance-pause,instance-kill,truncate
  --nemesis-interval 60 --harness-jar parsley-jepsen-harness.jar`.

  The checker here is the Jepsen adapter: it assembles the export the pure checker judges
  from the history (the final dump, the read observations, the status polls and the
  nemesis ops as faults), writes it beside the run as export.edn, and hands it to
  parsley-jepsen.checker and to Parsley's own Oracle replay in the harness jar."
  (:require [clojure.edn :as edn]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [store :as store]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os.debian :as debian]
            [parsley-jepsen [checker :as pchecker]
                            [client :as client]
                            [db :as db]
                            [nemesis :as pnemesis]
                            [workload :as workload]]))

(def all-faults
  #{:partition :kill :pause :clock
    :instance-kill :instance-pause :wipe
    :discard-held-copy :reset-offsets
    :truncate :delete-topic :recreate-topic :delete-changelog :add-partitions :restart-dropping :corrupt})

(defn- parse-faults [s]
  (let [faults (disj (set (map keyword (remove str/blank? (str/split s #",")))) :none)]
    (when-let [unknown (seq (remove all-faults faults))]
      (throw (IllegalArgumentException. (str "unknown faults: " (str/join ", " (map name unknown))))))
    faults))

(def cli-opts
  [[nil "--kafka-version VERSION" "Apache Kafka version to install." :default "4.3.1"]
   [nil "--harness-jar PATH" "The Parsley Jepsen harness jar (./mvnw -Pjepsen-harness -DskipTests package in parsley)."
    :default "parsley-jepsen-harness.jar"]
   [nil "--partitions N" "Partitions per topic, and so tasks per process." :default 3 :parse-fn parse-long]
   [nil "--rate HZ" "Sends per second." :default 5 :parse-fn parse-double]
   [nil "--nemesis FAULTS" (str "Comma-separated, or none: " (str/join "," (sort (map name all-faults))) ".")
    :default #{:partition :instance-kill}
    :parse-fn parse-faults]
   [nil "--nemesis-interval SECONDS" "Seconds between nemesis operations; rebalances and transaction timeouts run to tens of seconds."
    :default 60 :parse-fn parse-long]
   [nil "--[no-]out-of-contract" "Send occasional stamps naming a log-end offset." :default true]
   [nil "--calibrate KIND" "inversion: an external producer stamps less than it knows, which the checker must flag."
    :parse-fn keyword :validate [#{:inversion} "must be inversion"]]
   [nil "--quiesce-seconds SECONDS" "How long the final phase waits for zero lag." :default 600 :parse-fn parse-long]
   [nil "--[no-]java-oracle" "Cross-check the export with Parsley's Oracle replay in the harness jar." :default true]
   [nil "--java-oracle-heap SIZE" "The replay's heap; it holds every record's causes as a set, so it grows with the square of a run."
    :default "3g"]
   [nil "--kafka-heap SIZE" "Each broker's heap." :default "1g"]
   [nil "--harness-heap SIZE" "Each harness instance's heap." :default "1g"]
   [nil "--unclean-leader-election" "Turn unclean leader election on: a separately labelled run." :default false]])

;; ---- the history-to-export adapter ----

(defn- nearest-ends
  "The trace ends the last read observation before `index` saw, or {}."
  [reads index]
  (or (->> reads (filter #(< (:index %) index)) last :ends-hi) {}))

(defn- nemesis-faults
  "Every nemesis op as [invocation completion]: a fault began when it was invoked, and its
  completion says what it did and what it justifies."
  [history]
  (loop [ops (filter #(= :nemesis (:process %)) history)
         pending {}
         pairs []]
    (if-let [op (first ops)]
      (if-let [invocation (get pending (:f op))]
        (recur (rest ops) (dissoc pending (:f op)) (conj pairs [invocation op]))
        (recur (rest ops) (assoc pending (:f op) op) pairs))
      pairs)))

(defn- task-receives [ids partitions process dropped]
  (vec (for [p (range partitions)]
         {:process process :task p
          :receives (vec (for [topic (get client/processes process) :when (not (contains? dropped topic))]
                           [(get ids topic) p]))})))

(defn export-from-history
  "Assembles a JepsenExport from the dump and the history: the dump's records, trace and
  topics, with every incarnation the run deleted added as a dead topic; every :reads result
  with its history index; every :status result per process; every nemesis op, malformed
  send and out-of-contract stamp as a fault with the refusals it justifies. A topic
  deletion is a `kill`, a recreation a `recreate`, and a restart under a narrower
  declaration a `declared` fault per task, which is how the checker spells them. Read
  observations answered from a stale cache (parsley-jepsen.checker/stale-reads) go under
  :stale-reads, where no checker reads them."
  [test history dump]
  (let [ok? #(= :ok (:type %))
        invoke? #(= :invoke (:type %))
        partitions (:partitions test)
        ids (->> history (filter #(and (ok? %) (= :dump (:f %)))) last :value :topic-ids)
        live (set (map :id (:topics dump)))
        dead (for [[name id] ids :when (not (contains? live id))]
               {:id id :name name :partitions partitions :alive false :log-start {}})
        reads (vec (for [op history :when (and (ok? op) (= :reads (:f op)))
                         r (:value op)]
                     (assoc r :index (:index op) :time (:time op))))
        statuses (vec (for [op history :when (and (ok? op) (= :status (:f op)))
                            [process s] (:processes (:value op))]
                        {:index (:index op) :time (:time op) :process process
                         :lifecycle (:lifecycle s) :refusal (:refusal s) :detail (:detail s)
                         :trace-ends (nearest-ends reads (:index op))}))
        injected (loop [pairs (nemesis-faults history) dropped #{} faults []]
                   (if-let [[invocation completion] (first pairs)]
                     (let [v (:value completion)
                           details (if (map? v) (dissoc v :justifies) {:value (pr-str v)})
                           details (if (and (= :reset-offsets (:f completion)) (:to v))
                                     (assoc details :rewound {[(get ids (:topic v)) (:partition v)] (:to v)})
                                     details)
                           fault {:index (:index invocation) :time (:time invocation)
                                  :kind (case (:f completion)
                                          :delete-topic :kill
                                          :recreate-topic :recreate
                                          (:f completion))
                                  :justifies (vec (when (map? v) (:justifies v)))
                                  :trace-ends (or (:trace-ends details) (nearest-ends reads (:index invocation)))
                                  :details details}]
                       (if (= :restart-dropping (:f completion))
                         (let [dropped (conj dropped (:topic v))]
                           (recur (rest pairs) dropped
                                  (into (conj faults fault)
                                        (for [task (task-receives ids partitions (:process v) dropped)]
                                          (assoc fault :kind :declared :justifies [] :details task)))))
                         (recur (rest pairs) dropped (conj faults fault))))
                     faults))
        corrupt (for [op history :when (and (invoke? op) (= :send (:f op)) (:malformed (:value op)))]
                  {:index (:index op) :time (:time op) :kind :corrupt
                   :justifies [:UNDECODABLE_METADATA]
                   :trace-ends (nearest-ends reads (:index op))
                   :details {:uid (:uid (:value op)) :topic (:topic (:value op))
                             :expect {:process (first (for [[process received] client/processes
                                                            :when (some #{(:topic (:value op))} received)]
                                                        process))
                                      :refusal :UNDECODABLE_METADATA}}})
        out-of-contract (for [op history :when (and (ok? op) (= :send (:f op)) (:position (:out-of-contract (:value op))))]
                          {:index (:index op) :time (:time op) :kind :out-of-contract-stamp
                           :justifies []
                           :trace-ends (nearest-ends reads (:index op))
                           :details (assoc (:out-of-contract (:value op)) :uid (:uid (:value op)))})
        stamped-from (into {} (for [op history :when (and (invoke? op) (= :send (:f op)) (:stamped-from (:value op)))]
                                [(:uid (:value op)) (:stamped-from (:value op))]))
        tasks (vec (mapcat #(task-receives ids partitions % #{}) (keys client/processes)))
        starts (vec (for [{:keys [process task receives]} tasks]
                      {:process process :task task :positions (into {} (map (fn [ch] [ch 0]) receives))}))
        faults (vec (sort-by :index (concat injected corrupt out-of-contract)))
        ;; An observation answered from a stale cache is set aside before either checker
        ;; sees it, so both judge the same observations.
        {fresh :fresh stale :stale} (pchecker/stale-reads :cluster (map #(assoc % :task-name (pchecker/task-name (:process %) (:task %))) reads) faults)]
    (-> dump
        (update :topics #(vec (concat % dead)))
        (update :records (fn [records] (mapv (fn [r] (if-let [from (get stamped-from (:uid r))] (assoc r :stamped-from from) r)) records)))
        (assoc :reads (mapv #(dissoc % :task-name) fresh)
               :stale-reads (mapv #(dissoc % :task-name) stale)
               :statuses statuses
               :faults faults
               :tasks tasks
               :start-positions starts))))

(defn missing-refusals
  "Every fault that must produce a refusal and did not: no status after it shows its
  process refused for its reason. A finding, since the README's table says the fault ends
  in that refusal."
  [export]
  (vec (for [fault (:faults export)
             :let [{:keys [process refusal]} (get-in fault [:details :expect])]
             :when process
             :when (not-any? (fn [s] (and (= process (:process s)) (= refusal (:refusal s)) (> (:index s) (:index fault))))
                             (:statuses export))]
         (str "Expected refusal missing: " (name (:kind fault)) " at index " (:index fault) " should stop "
              process " with " (name refusal) ", and no later status shows it"))))

(defn- java-oracle
  "Parsley's own Oracle replay over the same export, through the harness jar. :clean? is
  :unknown when the replay could not finish, which its memory decides: it keeps every
  record's causes as a set."
  [test path]
  (if-not (:java-oracle test true)
    {:clean? :unknown :error "skipped"}
    (let [{:keys [exit out err]} (sh/sh "java" (str "-Xmx" (:java-oracle-heap test "3g")) "-jar" (:harness-jar test) "check" "--in" path)
          lines (vec (remove str/blank? (str/split-lines out)))]
      (if (contains? #{0 1} exit)
        {:clean? (zero? exit) :violation-count (if (zero? exit) 0 (dec (count lines))) :violations (vec (take 50 (butlast lines)))}
        {:clean? :unknown :error (str/trim (str/join "\n" (take-last 5 (str/split-lines (str out err)))))}))))

(defn parsley-checker
  "Judges the run with parsley-jepsen.checker over the export assembled from the history,
  and cross-checks it with Parsley's Oracle replay: a violation from either fails the run,
  and a replay that could not finish is reported but decides nothing."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [dumped (->> history (filter #(and (= :ok (:type %)) (= :dump (:f %)))) last :value)]
        (if-not dumped
          {:valid? :unknown :error "no final dump in the history"}
          (let [export (export-from-history test history (edn/read-string (slurp (:file dumped))))
                path (.getPath (store/path! test "export.edn"))
                _ (spit path (pr-str export))
                {:keys [valid? violations]} (pchecker/check export)
                missing (missing-refusals export)
                oracle (java-oracle test path)
                quiesced? (boolean (some #(and (= :ok (:type %)) (= :quiesce (:f %))) history))]
            {:valid? (and valid? (empty? missing) (not (false? (:clean? oracle))))
             :quiesced? quiesced?
             :violation-count (count violations)
             :violations (vec (take 200 violations))
             :stale-reads (count (:stale-reads export))
             :missing-refusals missing
             :java-oracle oracle
             :refusals (vec (distinct (for [s (:statuses export) :when (:refusal s)] [(:process s) (:refusal s)])))
             :faults (frequencies (map :kind (:faults export)))
             :records (count (:records export))
             :trace (count (:trace export))
             :reads (count (:reads export))}))))))

(defn parsley-test
  "A test map from CLI options."
  [opts]
  (let [db (db/db opts)
        planned (pnemesis/plan (:nemesis opts))
        corrupt-topic (some (fn [[fault target]] (when (= :corrupt fault) (:topic target))) planned)
        nemesis (pnemesis/package (assoc opts :db db) planned)
        final (gen/phases
               (gen/clients {:f :quiesce})
               (gen/clients {:f :reads})
               (gen/clients (map (fn [node] {:f :status :value node}) (:nodes opts)))
               (gen/clients {:f :dump}))]
    (merge tests/noop-test
           opts
           {:name (str "parsley-" (:kafka-version opts) (when (:unclean-leader-election opts) "-unclean"))
            :os debian/os
            :db db
            :acked (atom [])
            :topic-ids (atom nil)
            :gone-topics (atom #{})
            :dropped-topics (atom #{})
            :nonserializable-keys [:acked :topic-ids :gone-topics :dropped-topics]
            :plan planned
            :client (client/client opts)
            :nemesis (:nemesis nemesis)
            :generator (gen/phases
                        (->> (workload/generator opts corrupt-topic)
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
