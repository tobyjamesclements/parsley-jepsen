(ns parsley-jepsen.client-test
  "The producer client's encoder against Parsley's own codec vectors: every frontier the
  codec encodes, the client encodes to the same bytes, so an external stamp is exactly what
  a Parsley sender would have written."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [parsley-jepsen.client :as client]
            [parsley-jepsen.wire :as wire]))

(def vectors (edn/read-string (slurp (io/file "test/resources/codec-vectors.edn"))))

(defn- hex [^bytes bytes]
  (apply str (map #(format "%02x" (bit-and % 0xFF)) bytes)))

(deftest every-valid-vector-encodes-to-its-bytes
  (doseq [{:keys [label bytes causes]} (:valid vectors)]
    (testing label
      (is (= bytes (hex (client/encode-causes causes)))))))

(deftest an-encoded-stamp-decodes-to-what-it-named
  (let [causes {["f1020304-0506-0708-090a-0b0c0d0e0f10" 0] 9
                ["01020304-0506-0708-090a-0b0c0d0e0f10" 300] 1000000000000
                ["01020304-0506-0708-090a-0b0c0d0e0f10" 2] 0}]
    (is (= {:causes causes} (wire/decode (client/encode-causes causes))))))

(deftest an-under-stamped-record-carries-no-header
  (let [stamp {["01020304-0506-0708-090a-0b0c0d0e0f10" 0] 4}
        headers #(seq (.headers (client/record (merge {:topic "b" :partition 0 :key "k" :uid "k"} %))))]
    (is (nil? (headers {})))
    (is (= 1 (count (headers {:stamp stamp}))))
    (is (nil? (headers {:stamp stamp :understamp true})) "the lie the calibration run must catch")
    (is (= [99 1 2 3] (vec (.value (first (headers {:malformed true}))))))))
