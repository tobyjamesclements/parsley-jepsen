(defproject parsley-jepsen "0.1.0-SNAPSHOT"
  :description "A Jepsen test for Parsley: causal delivery order for Kafka Streams processors."
  :url "https://github.com/tobyjamesclements/parsley-jepsen"
  :license {:name "MIT License"
            :url "https://github.com/tobyjamesclements/parsley/blob/main/LICENSE"}
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [jepsen "0.3.9"]
                 [org.apache.kafka/kafka-clients "4.3.1"]]
  :main parsley-jepsen.core
  :jvm-opts ["-Xmx4g" "-server"]
  :test-selectors {:default (complement :cluster)
                   :cluster :cluster}
  :repl-options {:init-ns parsley-jepsen.core})
