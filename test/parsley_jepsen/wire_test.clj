(ns parsley-jepsen.wire-test
  "The decoder against Parsley's own codec vectors, exported by the harness
  (`JepsenHarness codec-vectors`) into test/resources/codec-vectors.edn: every encoding
  the codec produces decodes to the frontier it encoded, and every catalogued malformation
  is refused. A few spellings from the page itself are pinned besides."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [parsley-jepsen.wire :as wire]))

(def vectors (edn/read-string (slurp (io/file "test/resources/codec-vectors.edn"))))

(defn- expected-causes [causes]
  (into {} (map (fn [[[topic partition] position]] [[topic partition] position]) causes)))

(deftest header-key-is-the-one-the-codec-uses
  (is (= (:header-key vectors) wire/header-key)))

(deftest every-valid-vector-decodes-to-its-frontier
  (doseq [{:keys [label bytes causes]} (:valid vectors)]
    (testing label
      (is (= {:causes (expected-causes causes)} (wire/decode-hex bytes))))))

(deftest every-catalogued-malformation-is-refused
  (doseq [{:keys [family label bytes]} (:malformed vectors)]
    (testing (str family ": " label)
      (let [result (wire/decode-hex bytes)]
        (is (contains? result :undecodable) (pr-str result))))))

(deftest spellings-from-the-page
  (testing "an empty frontier is the version byte and a zero topic count"
    (is (= {:causes {}} (wire/decode-hex "0100"))))
  (testing "partition 300 spells AC 02, lowest bits first"
    (is (= {:causes {["00000000-0000-0001-0000-000000000001" 300] 7}}
           (wire/decode-hex (str "01" "01" "0000000000000001" "0000000000000001" "01" "ac02" "0000000000000007"))))
    ;; the same header cut after the topic id is truncated, never a smaller frontier
    (is (contains? (wire/decode-hex "010100000000000000010000000000000001") :undecodable)))
  (testing "85 80 80 80 10 is not another spelling of 05"
    (is (contains? (wire/decode-hex "01858080801000000000000000010000000000000001010000000000000000000001")
                   :undecodable)))
  (testing "a header present with a null value is undecodable, never absent"
    (is (contains? (wire/decode nil) :undecodable)))
  (testing "a header present with an empty value is undecodable"
    (is (contains? (wire/decode (byte-array 0)) :undecodable))))
