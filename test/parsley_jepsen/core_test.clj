(ns parsley-jepsen.core-test
  "The adapter from a Jepsen history to the export the pure checker judges, and the plan
  that keeps refusal-class faults to one per process, over constructed histories: no
  cluster is involved."
  (:require [clojure.test :refer [deftest is testing]]
            [parsley-jepsen.checker :as checker]
            [parsley-jepsen.core :as core]
            [parsley-jepsen.nemesis :as nemesis]))

(def ids {"src" "00000000-0000-0000-0000-000000000001" "a" "00000000-0000-0000-0000-00000000000a"
          "b" "00000000-0000-0000-0000-00000000000b" "c" "00000000-0000-0000-0000-00000000000c"
          "d" "00000000-0000-0000-0000-00000000000d" "loop" "00000000-0000-0000-0000-0000000000f0"
          "self" "00000000-0000-0000-0000-0000000000f1" "trace" "00000000-0000-0000-0000-0000000000f2"})

(def new-c "00000000-0000-0000-0000-0000000000cc")

(def dump
  "What the harness's export leaves after `self` was deleted and `c` recreated."
  {:format 1 :source :cluster :trace-topic "trace"
   :processes {"cycler" {:receives ["c"] :sends ["d" "loop" "trace"]}}
   :topics (vec (for [[name id] (assoc (dissoc ids "self") "c" new-c)]
                  {:id id :name name :partitions 2 :alive true :log-start {0 0 1 0}}))
   :records [{:topic (ids "b") :partition 0 :offset 0 :key "sb1" :value "sb1" :uid "sb1" :causes nil}]
   :trace []})

(defn- indexed [ops]
  (vec (map-indexed (fn [i op] (assoc op :index i :time (* i 1000))) ops)))

(def history
  (indexed
   [{:type :invoke :process 0 :f :send :value {:topic "b" :uid "sb1" :stamped-from [(ids "a") 0 7]}}
    {:type :info :process 0 :f :send :value {:topic "b" :uid "sb1" :stamped-from [(ids "a") 0 7]}}
    {:type :invoke :process 1 :f :reads}
    {:type :ok :process 1 :f :reads
     :value [{:process "cycler" :task 0 :ends-lo {"0" 3} :ends-hi {"0" 4} :next-read {[(ids "c") 0] 5} :exec-start {}}]}
    {:type :info :process :nemesis :f :start-partition :value :majority}
    {:type :info :process :nemesis :f :start-partition :value [:isolated {"n1" #{"n2"}}]}
    {:type :info :process :nemesis :f :delete-topic :value {:process "selfer" :topic "self" :partition 0}}
    {:type :info :process :nemesis :f :delete-topic
     :value {:process "selfer" :topic "self" :partition 0 :channel [(ids "self") 0] :held true
             :expect {:process "selfer" :refusal :CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES}
             :justifies #{:CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES :CHANNEL_IDENTITY_CHANGED}}}
    {:type :info :process :nemesis :f :recreate-topic :value {:process "cycler" :topic "c"}}
    {:type :invoke :process 2 :f :status :value "n1"}
    {:type :ok :process 2 :f :status
     :value {:node "n1" :healthy false
             :processes {"selfer" {:lifecycle :STOPPED :refusal :CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES :detail "gone"}
                         "cycler" {:lifecycle :RUNNING :refusal nil :detail nil}}}}
    {:type :info :process :nemesis :f :recreate-topic
     :value {:process "cycler" :topic "c" :old (ids "c") :new new-c :reinitialised-ends {"0" 9 "1" 2}
             :expect {:process "cycler" :refusal :CHANNEL_IDENTITY_CHANGED}
             :justifies #{:CHANNEL_IDENTITY_CHANGED}}}
    {:type :info :process :nemesis :f :restart-dropping :value {:process "joiner" :topic "b" :partition 1}}
    {:type :info :process :nemesis :f :restart-dropping
     :value {:process "joiner" :topic "b" :partition 1 :held true :trace-ends {"0" 11 "1" 3}
             :justifies #{:CHANNEL_REMOVED_WITH_HELD_MESSAGES}}}
    {:type :invoke :process 3 :f :send :value {:topic "src" :uid "garbage" :malformed true}}
    {:type :invoke :process 1 :f :reads}
    {:type :ok :process 1 :f :reads
     :value [{:process "cycler" :task 0 :ends-lo {"0" 12} :ends-hi {"0" 12} :next-read {[(ids "c") 0] 4} :exec-start {}}]}
    {:type :invoke :process 1 :f :dump}
    {:type :ok :process 1 :f :dump :value {:file "dump.edn" :topic-ids ids :records 1 :trace 0}}]))

(def export (core/export-from-history {:partitions 2} history dump))

(defn- fault [kind]
  (first (filter #(= kind (:kind %)) (:faults export))))

(deftest dead-incarnations-join-the-topics
  (let [by-id (into {} (map (juxt :id identity) (:topics export)))]
    (is (false? (:alive (by-id (ids "self")))) "a deleted topic is dead")
    (is (false? (:alive (by-id (ids "c")))) "so is the old incarnation of a recreated one")
    (is (true? (:alive (by-id new-c))))
    (is (= "c" (:name (by-id (ids "c"))) (:name (by-id new-c))))))

(deftest faults-are-spelled-as-the-checker-reads-them
  (testing "a fault begins at its invocation"
    (is (= 6 (:index (fault :kill))))
    (is (= 8 (:index (fault :recreate)))))
  (testing "a deletion is a kill naming the channel's topic"
    (is (= [(ids "self") 0] (get-in (fault :kill) [:details :channel])))
    (is (= #{:CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES :CHANNEL_IDENTITY_CHANGED} (set (:justifies (fault :kill))))))
  (testing "a recreation names both incarnations and where its receivers re-initialised"
    (is (= [(ids "c") new-c {"0" 9 "1" 2}]
           ((juxt :old :new :reinitialised-ends) (:details (fault :recreate))))))
  (testing "a narrower declaration is declared per task, at the trace ends read while every instance was down"
    (let [declared (filter #(= :declared (:kind %)) (:faults export))]
      (is (= [0 1] (map (comp :task :details) declared)))
      (is (= [[(ids "a") 1] [(ids "loop") 1]] (get-in (second declared) [:details :receives])))
      (is (every? #(= {"0" 11 "1" 3} (:trace-ends %)) declared))
      (is (every? #(empty? (:justifies %)) declared))))
  (testing "Jepsen's own faults justify nothing"
    (is (= [] (:justifies (fault :start-partition)))))
  (testing "a malformed send is a fault from its invocation, whether or not it was acknowledged"
    (is (= [:UNDECODABLE_METADATA] (:justifies (fault :corrupt))))
    (is (= "splitter" (get-in (fault :corrupt) [:details :expect :process])))))

(deftest observations-carry-their-history-index
  (is (= [3] (map :index (:reads export))))
  (is (= [16] (map :index (:stale-reads export)))
      "an observation reporting a position below an earlier one was answered from a stale cache")
  (is (= #{["selfer" :CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES] ["cycler" nil]}
         (set (map (juxt :process :refusal) (:statuses export)))))
  (is (= {"0" 4} (:trace-ends (first (:statuses export)))) "a status is placed by the last read before it"))

(deftest an-indeterminate-stamped-send-still-names-its-cause
  (is (= [(ids "a") 0 7] (:stamped-from (first (:records export))))))

(deftest tasks-receive-the-incarnations-the-run-began-with
  (is (= 8 (count (:tasks export))))
  (is (= [[(ids "c") 1]] (:receives (first (filter #(and (= "cycler" (:process %)) (= 1 (:task %))) (:tasks export)))))))

(deftest a-missing-expected-refusal-is-reported
  (let [missing (core/missing-refusals export)]
    (is (= 2 (count missing)) (pr-str missing))
    (is (some #(re-find #"recreate .* cycler with CHANNEL_IDENTITY_CHANGED" %) missing))
    (is (some #(re-find #"corrupt .* splitter with UNDECODABLE_METADATA" %) missing))
    (is (not-any? #(re-find #"selfer" %) missing) "the refusal that did follow its fault is not reported")))

(deftest a-refusal-class-fault-takes-a-process-no-other-has
  (testing "the README's order, one process each"
    (is (= [[:truncate "splitter"] [:delete-topic "selfer"] [:recreate-topic "cycler"] [:delete-changelog "joiner"]]
           (map (fn [[fault target]] [fault (:process target)])
                (nemesis/plan #{:truncate :delete-topic :recreate-topic :delete-changelog :partition})))))
  (testing "a fault with no process left is dropped rather than doubled up"
    (let [planned (nemesis/plan #{:truncate :delete-topic :recreate-topic :delete-changelog :add-partitions :restart-dropping :corrupt})]
      (is (= 4 (count planned)))
      (is (apply distinct? (map (comp :process second) planned)))))
  (testing "faults that justify no refusal are not planned"
    (is (empty? (nemesis/plan #{:partition :instance-kill :reset-offsets})))))

(deftest a-rewind-fault-lowers-what-later-observations-must-reach
  (let [reads [{:index 1 :task-name "joiner-0" :next-read {["a" 0] 10} :ends-hi {"0" 5}}
               {:index 3 :task-name "joiner-0" :next-read {["a" 0] 7} :ends-hi {"0" 6}}
               {:index 5 :task-name "joiner-0" :next-read {["a" 0] 8} :ends-hi {"0" 4}}
               {:index 6 :task-name "joiner-1" :next-read {["a" 1] 2} :ends-hi {"0" 7}}]
        faults [{:index 2 :kind :reset-offsets :details {:process "joiner" :partition 0 :rewound {["a" 0] 7}}}]]
    (testing "the observation after the rewind is fresh, and one whose trace ends fall is stale"
      (is (= [1 3 6] (map :index (:fresh (checker/stale-reads :cluster reads faults)))))
      (is (= [5] (map :index (:stale (checker/stale-reads :cluster reads faults))))))
    (testing "without the rewind fault the lower observation is stale"
      (is (= [1 6] (map :index (:fresh (checker/stale-reads :cluster reads []))))))
    (testing "a simulator's observations are exact and never set aside"
      (is (= 4 (count (:fresh (checker/stale-reads :simulator reads []))))))))
