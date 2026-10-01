(ns parsley-jepsen.checker-test
  "Calibration: a checker is worth nothing until it has caught something. Every simulator
  export under test/resources/exports (written by `JepsenHarness export-simulator`) is
  judged; the honest runs must pass and every sabotage mode must be flagged. The real
  cluster run the harness's own integration test exported must pass too."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [parsley-jepsen.checker :as checker]))

(def exports-dir "test/resources/exports")

(def index (edn/read-string (slurp (io/file exports-dir "index.edn"))))

(def runs (filter :file index))

(deftest the-calibration-set-covers-every-mode
  (is (seq (filter :expect-clean runs)) "honest runs are present")
  (doseq [mode [:IGNORE_CAUSES :NO_FIFO :REDELIVER_REFEEDS :UNDECODABLE_AS_ABSENT :SKIP_RECEIPT_MERGE
                :DROP_HELD :IGNORE_REMOVED_CHANNELS :SILENT_DROP :OVEREXPRESS :IGNORE_RECREATION
                :DELIVER_PAST_DEAD_HOLDS]]
    (is (some #(= mode (:mode %)) runs) (str "a run under " mode " is present")))
  (is (some #(= :RESET_PAST_LOG_START (:host-fault %)) runs) "a run under the host fault is present"))

(deftest honest-runs-pass
  (doseq [{:keys [file]} (filter :expect-clean runs)]
    (testing file
      (let [{:keys [valid? violations]} (checker/check-file (str exports-dir "/" file))]
        (is valid? (str/join "\n" violations))))))

(deftest every-sabotaged-run-is-caught
  (doseq [{:keys [file mode host-fault]} (remove :expect-clean runs)]
    (testing (str file " (" (or host-fault mode) ")")
      (let [{:keys [valid? violations]} (checker/check-file (str exports-dir "/" file))]
        (is (not valid?) (str "the checker must flag " file))
        (is (seq violations))))))

(deftest the-dead-holds-inversion-is-a-causal-order-violation
  (let [{:keys [violations]} (checker/check-file (str exports-dir "/DELIVER_PAST_DEAD_HOLDS-scenario.edn"))]
    (is (some #(str/starts-with? % "Safety 1") violations) (str/join "\n" violations))))

(deftest a-recreation-falls-on-the-tasks-attached-to-the-dead-incarnation
  (let [old "11111111-1111-1111-1111-111111111111"
        new "22222222-2222-2222-2222-222222222222"
        record (fn [id offset] {:topic id :partition 0 :offset offset :key (str "k" offset) :value "v" :uid (str id "@" offset) :causes nil})
        entry (fn [process to id offset] {:process process :task 0 :tp "n1" :to to :channel [id 0] :offset offset
                                          :uid (str id "@" offset) :causes nil :effects []})
        export {:format 1 :source :cluster
                :processes {"cycler" {:receives ["c"] :sends []} "cycler#2" {:receives ["c"] :sends []}}
                :topics [{:id old :name "c" :partitions 1 :alive false :log-start {0 0} :log-end {0 2}}
                         {:id new :name "c" :partitions 1 :alive true :log-start {0 0} :log-end {0 1}}]
                :tasks [{:process "cycler" :task 0 :receives [[old 0]]}
                        {:process "cycler#2" :task 0 :receives [[new 0]]}]
                :start-positions [{:process "cycler" :task 0 :positions {[old 0] 0}}
                                  {:process "cycler#2" :task 0 :positions {[new 0] 0}}]
                :records [(record old 0) (record old 1) (record new 0)]
                :trace [(entry "cycler" 0 old 0) (entry "cycler" 2 old 1) (entry "cycler#2" 3 new 0)]
                :reads [] :stale-reads [] :statuses []
                :faults [{:index 5 :kind :recreate :justifies [:CHANNEL_IDENTITY_CHANGED] :trace-ends {"n1" 1}
                          :details {:process "cycler" :topic "c" :old old :new new :reinitialised-ends {"n1" 1}}}]}
        assumption-2 (filter #(str/starts-with? % "Assumption 2") (:violations (checker/check export)))]
    (testing "the task that received the dead incarnation and went on delivering is flagged"
      (is (= 1 (count assumption-2)))
      (is (str/starts-with? (first assumption-2) "Assumption 2: cycler-0 delivered")))
    (testing "the lifetime an operator's reset attached to the new incarnation is not"
      (is (not-any? #(str/includes? % "cycler#2") assumption-2)))))

(deftest a-cluster-run-passes
  (let [file (io/file exports-dir "cluster-honest.edn")]
    (when (.exists file)
      (let [{:keys [valid? violations]} (checker/check-file (str file))]
        (is valid? (str/join "\n" violations))))))
