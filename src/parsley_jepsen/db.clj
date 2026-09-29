(ns parsley-jepsen.db
  "Apache Kafka in KRaft mode on Debian nodes, one combined controller-plus-broker per node,
  and one instance of the Parsley harness app per node under the same application-id
  prefix. Install, configure, format, start, stop, kill, pause, logs.

  UNVERIFIED: written without a cluster or a Clojars-reachable build; exercise on Jepsen's
  docker nodes before trusting it (README, build order)."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [db :as db]
                    [util :as util :refer [meh]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]))

(def kafka-dir "/opt/kafka")
(def data-dir "/opt/kafka/data")
(def kafka-log "/opt/kafka/logs/server.log")
(def kafka-pid "/opt/kafka/kafka.pid")
(def kafka-props (str kafka-dir "/config/kraft.properties"))

(def harness-dir "/opt/parsley")
(def harness-jar (str harness-dir "/parsley-jepsen-harness.jar"))
(def harness-state (str harness-dir "/state"))
(def harness-log "/var/log/parsley-harness.log")
(def harness-pid (str harness-dir "/harness.pid"))
(def status-port 8080)
(def app-prefix "jepsen")

(defn node-id
  "Node ids start at 1: the index of the node in the test's node list plus one."
  [test node]
  (inc (.indexOf ^java.util.List (vec (:nodes test)) node)))

(defn bootstrap-servers [test]
  (str/join "," (map #(str % ":9092") (:nodes test))))

(defn quorum-voters [test]
  (str/join "," (map (fn [node] (str (node-id test node) "@" node ":9093")) (:nodes test))))

(defn tarball-url [version]
  (str "https://archive.apache.org/dist/kafka/" version "/kafka_2.13-" version ".tgz"))

(defn install-java!
  "Temurin 21, which both Kafka 4 and the harness (Java 21) accept. Adoptium's apt repository
  serves every Debian release the docker nodes use."
  []
  (c/su
   (debian/install [:wget :gnupg :apt-transport-https :ca-certificates])
   (c/exec :bash :-c "wget -qO- https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /usr/share/keyrings/adoptium.gpg")
   (c/exec :bash :-c "echo \"deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main\" > /etc/apt/sources.list.d/adoptium.list")
   (debian/update!)
   (debian/install [:temurin-21-jdk])))

(defn install-kafka! [test version]
  (c/su
   (cu/install-archive! (tarball-url version) kafka-dir)
   (c/exec :mkdir :-p data-dir (str kafka-dir "/logs"))))

(defn install-harness! [test]
  (c/su
   (c/exec :mkdir :-p harness-dir harness-state)
   (c/upload (:harness-jar test) harness-jar)))

(defn server-properties
  "A combined controller and broker. Replication and in-sync minimums are set so a
  committed write survives a broker; unclean leader election stays off unless the test
  says otherwise, and that run is labelled separately."
  [test node]
  (let [n (count (:nodes test))
        rf (min 3 n)
        min-isr (max 1 (dec rf))]
    (->> {"process.roles" "broker,controller"
          "node.id" (node-id test node)
          "controller.quorum.voters" (quorum-voters test)
          "listeners" "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
          "advertised.listeners" (str "PLAINTEXT://" node ":9092")
          "inter.broker.listener.name" "PLAINTEXT"
          "controller.listener.names" "CONTROLLER"
          "listener.security.protocol.map" "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
          "log.dirs" data-dir
          "num.partitions" (:partitions test)
          "default.replication.factor" rf
          "min.insync.replicas" min-isr
          "offsets.topic.replication.factor" rf
          "transaction.state.log.replication.factor" rf
          "transaction.state.log.min.isr" min-isr
          "auto.create.topics.enable" "false"
          "unclean.leader.election.enable" (str (boolean (:unclean-leader-election test)))
          "log.retention.check.interval.ms" 1000
          "group.initial.rebalance.delay.ms" 0
          "transaction.max.timeout.ms" 900000}
         (map (fn [[k v]] (str k "=" v)))
         (str/join "\n"))))

(defn configure! [test node]
  (c/su (cu/write-file! (server-properties test node) kafka-props)))

(defn format-storage!
  "Formats the log directory for the cluster id every node shares."
  [test node]
  (c/su
   (c/exec (str kafka-dir "/bin/kafka-storage.sh") :format
           :-t (:cluster-id test) :-c kafka-props :--ignore-formatted)))

(defn start-kafka! [test node]
  (c/su
   (cu/start-daemon! {:logfile kafka-log
                      :pidfile kafka-pid
                      :chdir kafka-dir
                      :env {:KAFKA_HEAP_OPTS "-Xmx1g -Xms1g"}}
                     (str kafka-dir "/bin/kafka-server-start.sh")
                     kafka-props)))

(defn stop-kafka! [test node]
  (c/su (cu/stop-daemon! kafka-pid)))

(defn kill-kafka! [test node]
  (c/su (cu/grepkill! :kill "kafka.Kafka")))

(defn harness-args
  "The harness's run arguments on this node; `drop-topics` names topics left out of the
  declaration, the shape of the CHANNEL_REMOVED_WITH_HELD_MESSAGES fault."
  [test node drop-topics]
  (concat ["run"
           "--bootstrap" (bootstrap-servers test)
           "--prefix" app-prefix
           "--state-dir" harness-state
           "--status-port" status-port
           "--log-file" harness-log]
          (mapcat (fn [topic] ["--drop-topic" topic]) drop-topics)))

(defn start-harness!
  ([test node] (start-harness! test node []))
  ([test node drop-topics]
   (c/su
    (apply cu/start-daemon! {:logfile "/dev/null"
                             :pidfile harness-pid
                             :chdir harness-dir}
           "java" "-Xmx1g" "-jar" harness-jar
           (harness-args test node drop-topics)))))

(defn stop-harness! [test node]
  (c/su (cu/stop-daemon! harness-pid)))

(defn kill-harness! [test node]
  (c/su (cu/grepkill! :kill "parsley-jepsen-harness")))

(defn wipe-harness-state!
  "Removes the instance's local state directory; the ordering state must rebuild from its
  changelog at the next start."
  [test node]
  (c/su (c/exec :rm :-rf harness-state)
        (c/exec :mkdir :-p harness-state)))

(defn harness-pid-number [] (meh (c/exec :cat harness-pid)))

(defn pause-harness!
  "SIGSTOP: the instance keeps its transactions open and must be fenced when resumed past
  the transaction timeout (SPEC Host obligation 6)."
  [test node]
  (c/su (c/exec :kill :-STOP (harness-pid-number))))

(defn resume-harness! [test node]
  (c/su (c/exec :kill :-CONT (harness-pid-number))))

(defn create-topics!
  "Creates every topic of the topology once, from the first node, through the harness jar."
  [test]
  (let [n (count (:nodes test))]
    (c/su
     (c/exec "java" "-jar" harness-jar "create-topics"
             "--bootstrap" (bootstrap-servers test)
             "--partitions" (:partitions test)
             "--replication" (min 3 n)
             "--min-isr" (max 1 (dec (min 3 n)))
             "--unclean-election" (str (boolean (:unclean-leader-election test)))))))

(defn await-brokers!
  "Waits until every broker answers a metadata request."
  [test]
  (util/await-fn
   (fn [] (c/exec "java" "-jar" harness-jar "export"
                  "--bootstrap" (bootstrap-servers test) "--out" "/dev/null"))
   {:retry-interval 2000 :log-interval 10000 :log-message "waiting for the brokers"}))

(defrecord Kafka [version topics-created]
  db/DB
  (setup! [_ test node]
    (install-java!)
    (install-kafka! test version)
    (install-harness! test)
    (configure! test node)
    (format-storage! test node)
    (start-kafka! test node)
    (Thread/sleep 5000)
    ;; Topics once, after every broker is up; the harness waits for them.
    (locking topics-created
      (when-not @topics-created
        (Thread/sleep 15000)
        (create-topics! test)
        (reset! topics-created true)))
    (start-harness! test node))

  (teardown! [_ test node]
    (kill-harness! test node)
    (kill-kafka! test node)
    (c/su (c/exec :rm :-rf data-dir harness-state harness-log kafka-log)))

  db/LogFiles
  (log-files [_ test node]
    [kafka-log harness-log])

  db/Process
  (start! [_ test node] (start-kafka! test node))
  (kill! [_ test node] (kill-kafka! test node))

  db/Pause
  (pause! [_ test node] (c/su (cu/grepkill! :stop "kafka.Kafka")))
  (resume! [_ test node] (c/su (cu/grepkill! :cont "kafka.Kafka"))))

(defn db
  "The Kafka DB for the test map. The cluster id is drawn once per test."
  [opts]
  (Kafka. (:kafka-version opts) (atom false)))

(defn cluster-id
  "A base64 cluster id of the shape kafka-storage.sh random-uuid prints."
  []
  (-> (java.util.UUID/randomUUID)
      str
      (.getBytes "UTF-8")
      (->> (.encodeToString (java.util.Base64/getUrlEncoder)))
      (subs 0 22)))
