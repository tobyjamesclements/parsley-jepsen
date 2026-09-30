(defproject parsley-jepsen "0.1.0-SNAPSHOT"
  :description "A Jepsen test for Parsley: causal delivery order for Kafka Streams processors."
  :url "https://github.com/tobyjamesclements/parsley-jepsen"
  :license {:name "MIT License"
            :url "https://github.com/tobyjamesclements/parsley/blob/main/LICENSE"}
  :dependencies [[org.clojure/clojure "1.12.6"]
                 [jepsen "0.3.9"]
                 ;; Jepsen logs through logback 1.5, which needs slf4j 2; kafka-clients asks for 1.7.
                 [org.slf4j/slf4j-api "2.0.16"]
                 [org.apache.kafka/kafka-clients "4.3.1" :exclusions [org.slf4j/slf4j-api]]]
  :main parsley-jepsen.core
  :jvm-opts ["-Xmx4g" "-server"]
  :test-selectors {:default (complement :cluster)
                   :cluster :cluster}
  :repl-options {:init-ns parsley-jepsen.core})
