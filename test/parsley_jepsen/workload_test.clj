(ns parsley-jepsen.workload-test
  "The time-gated generator that delays a fault or a malformed send past the first
  interval: a sleep op holds only the thread that performs it, so this gate is by time."
  (:require [clojure.test :refer [deftest is testing]]
            [jepsen.generator :as gen]
            [jepsen.generator.context :as context]
            [parsley-jepsen.workload :as workload]))

(defn- at [ctx seconds]
  (assoc ctx :time (long (* seconds 1e9))))

(deftest after-holds-its-first-op-until-the-time-has-passed
  (let [ctx (context/context {:concurrency 2})
        g (workload/after 60 {:f :send :value {:malformed true}})]
    (testing "asked at once, the op is scheduled for sixty seconds on, whichever thread is free"
      (let [[op _] (gen/op g {} (at ctx 0))]
        (is (= (long 60e9) (:time op)))
        (is (= :send (:f op)))))
    (testing "the deadline is on the clock, so a generator asked and not chosen keeps it"
      (let [[op _] (gen/op g {} (at ctx 30))]
        (is (= (long 60e9) (:time op)))))
    (testing "an op that is already later is left as it is"
      (let [[op _] (gen/op (workload/after 10 {:f :x}) {} (at ctx 50))]
        (is (= (long 50e9) (:time op)))))
    (testing "exhaustion passes through"
      (is (nil? (gen/op (workload/after 10 nil) {} (at ctx 0))))
      (is (nil? (workload/after 10 nil))))))
