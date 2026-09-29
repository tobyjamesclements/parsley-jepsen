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

(deftest a-cluster-run-passes
  (let [file (io/file exports-dir "cluster-honest.edn")]
    (when (.exists file)
      (let [{:keys [valid? violations]} (checker/check-file (str file))]
        (is valid? (str/join "\n" violations))))))
