(ns parsley-jepsen.db
  "Apache Kafka in KRaft mode on Debian nodes, one combined controller-plus-broker per node,
  and one instance of the Parsley harness app per node under the same application-id
  prefix. Install, configure, format, start, stop, kill, pause, logs.

  Setup runs on every node at once and meets at three barriers: every broker starts only
  once every node is installed and formatted, since a broker that cannot register with the
  controller quorum within a minute exits; once every broker process has been started, the
  primary node waits for every broker to register and creates the topics; once the topics
  exist, every node starts its harness instance."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+]])
  (:import [java.util.concurrent ExecutionException TimeUnit]
           [org.apache.kafka.clients.admin Admin AdminClientConfig NewTopic]
           [org.apache.kafka.common.errors TopicExistsException]))

(def kafka-dir "/opt/kafka")
(def data-dir "/opt/kafka/data")
(def kafka-log "/opt/kafka/logs/server.log")
(def kafka-pid "/opt/kafka/kafka.pid")
(def kafka-props (str kafka-dir "/config/kraft.properties"))

(def harness-dir "/opt/parsley")
(def harness-jar (str harness-dir "/parsley-jepsen-harness.jar"))
(def harness-state (str harness-dir "/state"))
(def harness-log "/var/log/parsley-harness.log")
(def harness-stdout "/var/log/parsley-harness.out")
(def harness-pid (str harness-dir "/harness.pid"))
(def status-port 8080)
(def app-prefix "jepsen")

(def trace-topic "trace")
(def topics ["src" "a" "b" "c" "d" "loop" "self" "trace"])

(def setup-barrier-seconds
  "How long a node waits at a setup barrier for the others. The first setup on fresh nodes
  installs a JDK and downloads Kafka before the first barrier, so this is generous."
  900)

(defn node-id
  "Node ids start at 1: the index of the node in the test's node list plus one."
  [test node]
  (inc (.indexOf ^java.util.List (vec (:nodes test)) node)))

(defn bootstrap-servers [test]
  (str/join "," (map #(str % ":9092") (:nodes test))))

(defn quorum-voters [test]
  (str/join "," (map (fn [node] (str (node-id test node) "@" node ":9093")) (:nodes test))))

(defn replication-factor [test]
  (min 3 (count (:nodes test))))

(defn min-isr [test]
  (max 1 (dec (replication-factor test))))

(defn tarball-url [version]
  (str "https://archive.apache.org/dist/kafka/" version "/kafka_2.13-" version ".tgz"))

(defn admin
  "An admin client on the control node, bootstrapped from every broker."
  ^Admin [test]
  (Admin/create ^java.util.Map
                {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG (bootstrap-servers test)
                 AdminClientConfig/REQUEST_TIMEOUT_MS_CONFIG "10000"
                 AdminClientConfig/DEFAULT_API_TIMEOUT_MS_CONFIG "15000"}))

;; ---- processes by pattern ----

(defn signal-matching!
  "Signals every process whose command line matches `pattern`, a regex whose first
  character is bracketed so the shell that runs pkill does not match itself. Returns
  whether any process matched."
  [signal pattern]
  (try+ (c/exec :pkill (str "-" (name signal)) :-f pattern)
        true
        (catch [:type :jepsen.control/nonzero-exit :exit 1] _ false)))

(def kafka-pattern "[k]afka\\.Kafka")
(def harness-pattern "[p]arsley-jepsen-harness\\.jar run")

;; ---- install ----

(defn install-java!
  "Temurin 21, which both Kafka 4 and the harness (Java 21) accept. Adoptium's apt repository
  serves every Debian release the docker nodes use."
  []
  (c/su
   (when-not (cu/exists? "/usr/bin/java")
     (debian/install [:wget :gnupg :apt-transport-https :ca-certificates])
     (c/exec :bash :-c "wget -qO- https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor --yes -o /usr/share/keyrings/adoptium.gpg")
     (c/exec :bash :-c "echo \"deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main\" > /etc/apt/sources.list.d/adoptium.list")
     (debian/update!)
     (debian/install [:temurin-21-jdk]))))

(defn install-kafka! [test version]
  (c/su
   (when-not (cu/exists? (str kafka-dir "/libs/kafka-clients-" version ".jar"))
     (cu/install-archive! (tarball-url version) kafka-dir))
   (c/exec :mkdir :-p data-dir (str kafka-dir "/logs"))))

(defn- sha256 [^java.io.File file]
  (let [digest (java.security.MessageDigest/getInstance "SHA-256")
        buffer (byte-array 65536)]
    (with-open [in (java.io.FileInputStream. file)]
      (loop []
        (let [n (.read in buffer)]
          (when (pos? n)
            (.update digest buffer 0 n)
            (recur)))))
    (apply str (map #(format "%02x" %) (.digest digest)))))

(defn install-harness!
  "Uploads the harness jar unless the node already holds the same bytes."
  [test jar-digest]
  (c/su
   (c/exec :mkdir :-p harness-dir harness-state)
   (let [remote (when (cu/exists? harness-jar)
                  (first (str/split (c/exec :sha256sum harness-jar) #"\s+")))]
     (when-not (= remote @jar-digest)
       (c/upload (:harness-jar test) harness-jar)))))

;; ---- the broker ----

(defn server-properties
  "A combined controller and broker. Replication and in-sync minimums are set so a
  committed write survives a broker; unclean leader election stays off unless the test
  says otherwise, and that run is labelled separately."
  [test node]
  (let [rf (replication-factor test)
        min-isr (min-isr test)]
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
          "delete.topic.enable" "true"
          "unclean.leader.election.enable" (str (boolean (:unclean-leader-election test)))
          "log.retention.check.interval.ms" 1000
          "group.initial.rebalance.delay.ms" 0
          "transaction.max.timeout.ms" 900000}
         (map (fn [[k v]] (str k "=" v)))
         (str/join "\n"))))

(defn configure! [test node]
  (c/su (cu/write-file! (server-properties test node) kafka-props)))

(defn cluster-id!
  "The cluster id every node formats its storage with: `kafka-storage.sh random-uuid`, run
  once on whichever node asks first."
  [id]
  (locking id
    (or @id
        (reset! id (str/trim (c/su (c/exec (str kafka-dir "/bin/kafka-storage.sh") :random-uuid)))))))

(defn format-storage!
  "Formats the log directory for the cluster id every node shares."
  [test node id]
  (c/su
   (c/exec (str kafka-dir "/bin/kafka-storage.sh") :format
           :-t id :-c kafka-props :--ignore-formatted)))

(defn start-kafka! [test node]
  (c/su
   (cu/start-daemon! {:logfile kafka-log
                      :pidfile kafka-pid
                      :chdir kafka-dir
                      :match-executable? false
                      :env {:KAFKA_HEAP_OPTS (str "-Xmx" (:kafka-heap test "1g") " -Xms" (:kafka-heap test "1g"))
                            :LOG_DIR (str kafka-dir "/logs")}}
                     (str kafka-dir "/bin/kafka-server-start.sh")
                     kafka-props)))

(defn kill-kafka! [test node]
  (c/su (signal-matching! :KILL kafka-pattern)
        (c/exec :rm :-f kafka-pid)))

;; ---- the harness instance ----

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
  "Starts the instance on this node, declaring everything but the topics the run has
  dropped so far (the test map's :dropped-topics). The harness truncates its log file when
  it starts, so what an earlier instance logged is kept in the .history file first."
  [test node]
  (c/su
   (when (cu/exists? harness-log)
     (c/exec :bash :-c (str "cat " harness-log " >> " harness-log ".history && rm -f " harness-log)))
   (apply cu/start-daemon! {:logfile harness-stdout
                            :pidfile harness-pid
                            :chdir harness-dir
                            :match-executable? false}
          "/usr/bin/java" (str "-Xmx" (:harness-heap test "1g")) "-jar" harness-jar
          (harness-args test node (some-> (:dropped-topics test) deref sort)))))

(defn kill-harness! [test node]
  (c/su (signal-matching! :KILL harness-pattern)
        (c/exec :rm :-f harness-pid)))

(defn stop-harness!
  "Stops the instance gracefully: SIGTERM runs the harness's shutdown hook, which closes
  every process and leaves its groups, and a kill follows if it has not exited in time."
  [test node]
  (c/su (when (signal-matching! :TERM harness-pattern)
          (try (util/await-fn (fn [] (when (signal-matching! :CONT harness-pattern)
                                       (throw (ex-info "still running" {}))))
                              {:retry-interval 1000 :log-interval 10000 :timeout 60000
                               :log-message "Waiting for the harness to exit"})
               (catch Exception _ (warn "The harness on" node "did not exit on SIGTERM; killing it"))))
        (kill-harness! test node)))

(defn wipe-harness-state!
  "Removes the instance's local state directory; the ordering state must rebuild from its
  changelog at the next start."
  [test node]
  (c/su (c/exec :rm :-rf harness-state)
        (c/exec :mkdir :-p harness-state)))

(defn pause-harness!
  "SIGSTOP: the instance keeps its transactions open and must be fenced when resumed past
  the transaction timeout (SPEC Host obligation 6)."
  [test node]
  (c/su (signal-matching! :STOP harness-pattern)))

(defn resume-harness! [test node]
  (c/su (signal-matching! :CONT harness-pattern)))

(defn await-harness!
  "Waits until the instance on this node serves its status."
  [test node]
  (cu/await-tcp-port status-port {:retry-interval 1000 :log-interval 10000 :timeout 120000}))

;; ---- the cluster, from the control node ----

(defn await-brokers!
  "Waits until every broker has registered with the controller quorum."
  [test]
  (let [n (count (:nodes test))]
    (util/await-fn
     (fn []
       (with-open [admin (admin test)]
         (let [brokers (count (.get (.nodes (.describeCluster admin)) 10 TimeUnit/SECONDS))]
           (when (< brokers n)
             (throw (ex-info "brokers still registering" {:registered brokers :expected n}))))))
     {:retry-interval 2000 :log-interval 10000 :timeout 300000
      :log-message "Waiting for every broker to register"})))

(defn create-topics!
  "Creates every topic of the topology, then waits until every partition of each has a
  leader and a full in-sync set. A topic that already exists is left as it is, so a retry
  after a partial failure converges."
  ([test] (create-topics! test topics))
  ([test topics]
   (let [rf (short (replication-factor test))
         configs {"min.insync.replicas" (str (min-isr test))
                  "unclean.leader.election.enable" (str (boolean (:unclean-leader-election test)))}]
     (with-open [admin (admin test)]
       (doseq [topic topics]
         (try
           (-> (.createTopics admin [(.configs (NewTopic. ^String topic (int (:partitions test)) rf) configs)])
               .all
               (.get 60 TimeUnit/SECONDS))
           (catch ExecutionException e
             (when-not (instance? TopicExistsException (.getCause e))
               (throw e)))))
       (util/await-fn
        (fn []
          (doseq [[topic description] (.get (.allTopicNames (.describeTopics admin ^java.util.Collection topics)) 10 TimeUnit/SECONDS)
                  partition (.partitions description)]
            (when (or (nil? (.leader partition)) (< (count (.isr partition)) rf))
              (throw (ex-info "topic still settling" {:topic topic :partition (.partition partition)})))))
        {:retry-interval 1000 :log-interval 10000 :timeout 120000
         :log-message "Waiting for every topic partition to have a leader and a full in-sync set"})))))

(defrecord Kafka [version cluster-id jar-digest]
  db/DB
  (setup! [_ test node]
    (install-java!)
    (install-kafka! test version)
    (install-harness! test jar-digest)
    (configure! test node)
    (format-storage! test node (cluster-id! cluster-id))
    ;; A broker that cannot register with the controller quorum within a minute exits, so
    ;; every broker starts only once every node is installed and formatted.
    (jepsen/synchronize test setup-barrier-seconds)
    (start-kafka! test node)
    ;; Every broker process is started: the primary waits for them all and creates the topics once.
    (jepsen/synchronize test setup-barrier-seconds)
    (when (= node (jepsen/primary test))
      (await-brokers! test)
      (create-topics! test))
    ;; The topics exist: the harness refuses to start without them.
    (jepsen/synchronize test setup-barrier-seconds)
    (start-harness! test node)
    (await-harness! test node))

  (teardown! [_ test node]
    (kill-harness! test node)
    (kill-kafka! test node)
    (c/su (c/exec :rm :-rf data-dir harness-state harness-log (str harness-log ".history") harness-stdout (str kafka-dir "/logs"))))

  db/LogFiles
  (log-files [_ test node]
    [kafka-log harness-log (str harness-log ".history") harness-stdout])

  db/Kill
  (start! [_ test node] (start-kafka! test node))
  (kill! [_ test node] (kill-kafka! test node))

  db/Pause
  (pause! [_ test node] (c/su (signal-matching! :STOP kafka-pattern)))
  (resume! [_ test node] (c/su (signal-matching! :CONT kafka-pattern))))

(defn db
  "The Kafka DB for the test map. The cluster id is drawn once per test, on a node, by
  kafka-storage.sh; the harness jar's digest is computed once."
  [opts]
  (Kafka. (:kafka-version opts) (atom nil) (delay (sha256 (java.io.File. ^String (:harness-jar opts))))))
