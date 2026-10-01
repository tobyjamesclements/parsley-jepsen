(ns parsley-jepsen.checker
  "The oracle over the trace and the topic dumps. Pure: it takes the export the harness
  writes (`JepsenHarness export`, or the simulator's calibration exports) and judges it,
  never trusting the engine.

  Happened-before comes from the trace and the declaration, not from headers. A record sent
  by task T in the step that delivered D is caused by D, by every earlier delivery at T, by
  the record an external client had observed before a stamped send, by the causes of every
  record T is known to have received before the step, and transitively by their causes.
  That is a sound subset of the true causes: a receipt is known only from a read observation
  bracketed by the trace ends, so what no observation covers is missing from it, and the
  expression check catches what that misses, since every send must express every cause its
  sender had delivered or seen expressed.

  Judgements, cited as in Parsley's SPEC.md: Safety 1 at delivery time and over delivered
  pairs, Safety 2, Safety 3, Safety 7, Safety 8, Structural 12, 14 and 15, over-expression,
  Liveness 1 at quiescence with the spec's exemptions, Host obligations 3 and 6 through the
  effects each step declared, Assumption 2, and Operational 1 and 6: every refusal follows an
  injected fault that justifies it.

  A causal past is a frontier, the greatest position per channel, standing for every
  position up to it; under FIFO delivery (Safety 3, judged on its own) that is the past
  exactly, and it is what lets a long trace be judged. Receipt spans and what received
  records name are kept per observation prefix and per channel prefix, found by binary
  search.

  Ordering never relies on a clock. Every trace entry carries its trace partition `tp` and
  offset `to`; every observation carries the trace ends it saw. A read observation records
  the ends twice: `ends-lo` was read before the group's positions, `ends-hi` after, so its
  positions bound receipt from below for any step at or past `ends-hi` and from above for
  any step below `ends-lo`."
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [parsley-jepsen.wire :as wire]))

;; ---- reading ----

(defn read-export
  "Reads an export file written by the harness or the simulator."
  [path]
  (edn/read-string (slurp (io/file path))))

(defn task-name [process task]
  (str process "-" task))

(defn- channel-str [[id partition]]
  (str id "-" partition))

(defn- pos-str [[id partition offset]]
  (str id "-" partition "@" offset))

;; ---- the model ----

(defn- decoded
  "The decoded causes of a header value spelled in the export: a map, or nil when the
  header is absent (no causes), or :undecodable."
  [causes]
  (cond
    (nil? causes) {}
    (= :null-value causes) :undecodable
    :else (let [r (wire/decode-hex causes)]
            (if (:undecodable r) :undecodable (:causes r)))))

(defn stale-reads
  "Splits the read observations into the fresh and the stale. Committed positions and trace
  ends only grow, so an observation that reports a position or a trace end below what an
  earlier one reported was answered from a stale cache, as a group coordinator or a
  partition leader cut off by a network partition keeps answering, and it would place a
  step's receipts before positions committed after it. A `reset-offsets` fault, whose
  details name what it `rewound`, lowers what a task's later observations must reach. Only
  a cluster's observations are judged so: the simulated host rewinds a process's positions
  as a fault of its own, and its observations are exact."
  [source reads faults]
  (if (not= :cluster source)
    {:fresh reads :stale []}
  (let [rewinds (for [f faults :when (get-in f [:details :rewound])]
                  {:index (:index f) :rewound (get-in f [:details :rewound])
                   :task-name (task-name (get-in f [:details :process]) (get-in f [:details :partition]))})
        events (sort-by :index (concat (map #(assoc % :event :read) reads) (map #(assoc % :event :rewind) rewinds)))]
    (loop [events events floors {} ends {} fresh [] stale []]
      (if-let [e (first events)]
        (case (:event e)
          :rewind (recur (rest events)
                         (update floors (:task-name e) #(merge-with min % (:rewound e)))
                         ends fresh stale)
          :read (let [floor (get floors (:task-name e) {})
                      below? (or (some (fn [[channel position]] (< position (get floor channel -1))) (:next-read e))
                                 (some (fn [[tp end]] (< end (get ends tp -1))) (:ends-hi e)))]
                  (if below?
                    (recur (rest events) floors ends fresh (conj stale e))
                    (recur (rest events)
                           (assoc floors (:task-name e) (merge-with max floor (:next-read e)))
                           (merge-with max ends (:ends-hi e))
                           (conj fresh e) stale))))
        {:fresh (map #(dissoc % :event) fresh) :stale (map #(dissoc % :event) stale)})))))

(defn- build
  "Indexes the export once: topics, records, trace, observations, declarations."
  [export]
  (let [topics (into {} (map (fn [t] [(:id t) t]) (:topics export)))
        by-name (group-by :name (:topics export))
        records (map (fn [r] (assoc r :channel [(:topic r) (:partition r)]
                                    :pos [(:topic r) (:partition r) (:offset r)]
                                    :meta (decoded (:causes r))))
                     (:records export))
        records-by-channel (reduce (fn [m r] (update m (:channel r) (fnil assoc (sorted-map)) (:offset r) r))
                                   {} records)
        records-by-pos (into {} (map (fn [r] [(:pos r) r]) records))
        ;; Per channel, in offset order, what the records up to each one could have let a
        ;; receiver see expressed: the offset itself and every position its metadata names.
        ;; A receipt span that starts at the channel's first record is answered from here
        ;; by one lookup instead of a walk of the span.
        named-by-channel (into {} (for [[channel by-offset] records-by-channel]
                                    [channel {:offsets (vec (keys by-offset))
                                              :bounds (vec (rest (reductions (fn [bound r]
                                                                              (let [bound (assoc bound channel (:offset r))]
                                                                                (if (map? (:meta r)) (merge-with max bound (:meta r)) bound)))
                                                                            {} (vals by-offset))))}]))
        records-by-uid (group-by :uid records)
        trace (->> (:trace export)
                   (map (fn [e] (assoc e :task-name (task-name (:process e) (:task e))
                                       :pos [(first (:channel e)) (second (:channel e)) (:offset e)]
                                       :meta (decoded (:causes e)))))
                   (sort-by (juxt :tp :to)))
        trace-by-task (reduce (fn [m e] (update m (:task-name e) (fnil conj []) e)) {} trace)
        trace-by-task (into {} (map (fn [[t es]] [t (vec (map-indexed #(assoc %2 :step %1) es))]) trace-by-task))
        producer-by-effect (reduce (fn [m e] (reduce (fn [m [_ uid]] (if (contains? m uid) m (assoc m uid e)))
                                                     m (:effects e)))
                                   {} (mapcat val trace-by-task))
        faults (sort-by :index (:faults export))
        {fresh-reads :fresh stale :stale} (stale-reads (:source export)
                                                       (map #(assoc % :task-name (task-name (:process %) (:task %))) (:reads export))
                                                       faults)
        reads-by-task (->> fresh-reads
                           (sort-by :index)
                           (group-by :task-name))
        declarations-by-task (->> faults
                                  (filter #(= :declared (:kind %)))
                                  (group-by #(task-name (get-in % [:details :process])
                                                        (or (get-in % [:details :task]) 0))))
        starts-by-task (reduce (fn [m s] (update m (task-name (:process s) (:task s))
                                                 (fn [known] (merge (:positions s) known))))
                               {} (:start-positions export))
        task-info (into {} (map (fn [t] [(task-name (:process t) (:task t)) t]) (:tasks export)))
        created-at (reduce (fn [m f] (if-let [new (get-in f [:details :new])]
                                       (if (contains? m new) m (assoc m new (:index f))) m))
                           {} (filter #(= :recreate (:kind %)) faults))
        dead-at (reduce (fn [m f]
                          (case (:kind f)
                            :recreate (if-let [old (get-in f [:details :old])]
                                        (if (contains? m old) m (assoc m old (:index f))) m)
                            :kill (if-let [ch (get-in f [:details :channel])]
                                    (if (contains? m (first ch)) m (assoc m (first ch) (:index f))) m)
                            m))
                        {} faults)
        first-refusal (reduce (fn [m s] (if (and (:refusal s) (not (contains? m (:process s))))
                                          (assoc m (:process s) s) m))
                              {} (sort-by :index (:statuses export)))
        ;; The last assigned position counts the offsets transaction markers and aborted records
        ;; took, which the export's log ends record and no committed record occupies: a frontier
        ;; names such a position once an out-of-contract stamp was settled by the channel moving
        ;; past it. Records alone stand in where an export predates the log ends.
        last-assigned (reduce (fn [m r] (update m (:channel r) (fnil max -1) (:offset r)))
                              (into {} (for [t (:topics export) [p end] (:log-end t) :when (pos? end)] [[(:id t) p] (dec end)]))
                              records)
        undecodable (into #{} (map :pos (filter #(= :undecodable (:meta %)) records)))]
    {:export export
     :topics topics
     :by-name by-name
     :records-by-channel records-by-channel
     :records-by-pos records-by-pos
     :named-by-channel named-by-channel
     :records-by-uid records-by-uid
     :trace-by-task trace-by-task
     :producer-by-effect producer-by-effect
     :reads-by-task reads-by-task
     :stale-reads (vec stale)
     :faults faults
     :declarations-by-task declarations-by-task
     :starts-by-task starts-by-task
     :task-info task-info
     :created-at created-at
     :dead-at dead-at
     :first-refusal first-refusal
     :last-assigned last-assigned
     :undecodable undecodable
     :memo (atom {:causes {} :past {} :merged {}})}))

;; ---- topics ----

(defn- dead? [m [id _]]
  (let [t (get-in m [:topics id])]
    (or (nil? t) (not (:alive t)))))

(defn- alive-at? [m id index]
  (let [created (get-in m [:created-at id])
        died (get-in m [:dead-at id])]
    (and (or (nil? created) (< created index))
         (or (nil? died) (> died index)))))

(defn- recreated-at?
  "Whether, at history index `index`, the channel's topic is dead while its name resolves to
  a live other id."
  [m [id _] index]
  (let [died (get-in m [:dead-at id])
        t (get-in m [:topics id])]
    (boolean
     (and died (<= died index) t
          (some (fn [other] (and (not= (:id other) id) (alive-at? m (:id other) index)))
                (get-in m [:by-name (:name t)]))))))

(defn- log-start [m [id partition]]
  (get-in m [:topics id :log-start partition] 0))

(defn- topic-name [m id]
  (get-in m [:topics id :name] id))

(defn- nothing-discarded? [m name]
  (every? (fn [t] (and (:alive t) (every? zero? (vals (:log-start t)))))
          (get-in m [:by-name name])))

(defn- record-at [m pos]
  (get-in m [:records-by-pos pos]))

;; ---- received sets over time ----

(defn- process-of [task] (subs task 0 (str/last-index-of task "-")))
(defn- partition-of [task] (Long/parseLong (subs task (inc (str/last-index-of task "-")))))

(defn- declared-initially [m task]
  (if-let [info (get-in m [:task-info task])]
    (set (:receives info))
    (let [decl (get-in m [:export :processes (process-of task)])
          p (partition-of task)]
      (set (for [topic (:receives decl)
                 t (get-in m [:by-name topic])
                 :when (< p (:partitions t))]
             [(:id t) p])))))

(defn- declared-by [declaration]
  (set (get-in declaration [:details :receives])))

(defn- received-at
  "The received set in force when `entry` was delivered: the last declaration before it."
  [m task entry]
  (reduce (fn [received d]
            (let [end (get-in d [:trace-ends (:tp entry)])]
              (if (and end (<= end (:to entry))) (declared-by d) received)))
          (declared-initially m task)
          (get-in m [:declarations-by-task task])))

(defn- received-at-index [m task index]
  (reduce (fn [received d] (if (< (:index d) index) (declared-by d) received))
          (declared-initially m task)
          (get-in m [:declarations-by-task task])))

(defn- received-finally [m task]
  (if-let [ds (seq (get-in m [:declarations-by-task task]))]
    (declared-by (last ds))
    (declared-initially m task)))

(defn- received-ever [m task]
  (reduce into (declared-initially m task) (map declared-by (get-in m [:declarations-by-task task]))))

;; ---- receipt, from the read observations ----

(defn- count-at-or-below
  "How many of the ascending `offsets` are at or below `offset`."
  [offsets offset]
  (loop [lo 0 hi (count offsets)]
    (if (< lo hi)
      (let [mid (quot (+ lo hi) 2)]
        (if (<= (nth offsets mid) offset) (recur (inc mid) hi) (recur lo mid)))
      lo)))

(defn- merged-spans
  "The union of spans as disjoint spans in ascending order. Every observation of a task
  contributes a span, most of them nested in the next, so walking the records of each
  span in turn walks the same records once per observation; walking the union walks them
  once."
  [spans]
  (reduce (fn [merged [from to]]
            (let [[last-from last-to] (peek merged)]
              (if (and last-to (<= from last-to))
                (conj (pop merged) [last-from (max last-to to)])
                (conj merged [from to]))))
          []
          (sort spans)))


(defn- execution-start [m task reads channel]
  (or (get-in reads [:exec-start channel])
      (get-in m [:starts-by-task task channel])
      0))

(defn- read-span
  "The span [from next) of positions on `channel` one observation shows the task received,
  or nil."
  [m task reads channel]
  (let [next (get-in reads [:next-read channel])
        from (execution-start m task reads channel)]
    (when (and next (> next from)) [from next])))

;; The observations of a task are kept in index order, and the trace ends they carry only
;; grow along it (a cluster's stale observations are set aside first, and the simulator's
;; trace ends are counts), so the observations taken before a step are a prefix of them.
;; Each prefix's receipt spans are merged once and then found by one binary search, which
;; keeps a task's replay linear in its observations rather than quadratic.

(defn- ends-vector
  "Each observation's end for trace partition `tp`, in index order; an observation that
  carries no end for it had seen nothing of it, which is 0."
  [m task key tp]
  (let [memo (:memo m)]
    (or (get-in @memo [:ends task key tp])
        (let [v (mapv #(get-in % [key tp] 0) (get-in m [:reads-by-task task]))
              v (if (= v (vec (sort v))) v ::unordered)]
          (swap! memo assoc-in [:ends task key tp] v)
          v))))

(defn- prefix-spans
  "For each k, the merged spans of the first k observations' receipts on `channel`."
  [m task channel]
  (let [memo (:memo m)]
    (or (get-in @memo [:prefix-spans task channel])
        (let [spans (map #(read-span m task % channel) (get-in m [:reads-by-task task]))
              v (vec (reductions (fn [merged span] (if span (merged-spans (conj merged span)) merged)) [] spans))]
          (swap! memo assoc-in [:prefix-spans task channel] v)
          v))))

(defn- received-spans-before
  "Spans [from to) of positions on `channel` the task is known to have received before
  `entry`: for every observation taken before the step, from where its execution began
  reading the channel to the position it committed. Merged."
  [m task channel entry]
  (let [ends (ends-vector m task :ends-hi (:tp entry))]
    (if (= ::unordered ends)
      (merged-spans (for [reads (get-in m [:reads-by-task task])
                          :let [end (get-in reads [:ends-hi (:tp entry)])
                                span (read-span m task reads channel)]
                          :when (and end (<= end (:to entry)) span)]
                      span))
      (nth (prefix-spans m task channel) (count-at-or-below ends (:to entry))))))

(defn- received-spans-ever [m task channel]
  (peek (prefix-spans m task channel)))

(defn- received-spans-up-to
  "Spans of positions on `channel` the task could have received before `entry`: the spans
  of every observation before the step and the span of the first observation after it,
  whose positions cover the step's own receipts; everything when no observation follows.
  Over-approximates receipt, and survives a rewind."
  [m task channel entry]
  (let [before (received-spans-before m task channel entry)
        reads (get-in m [:reads-by-task task])
        ends (ends-vector m task :ends-lo (:tp entry))
        after (if (= ::unordered ends)
                (->> reads
                     (filter (fn [reads] (when-let [end (get-in reads [:ends-lo (:tp entry)])] (> end (:to entry)))))
                     first)
                (let [k (count-at-or-below ends (:to entry))]
                  (when (< k (count reads)) (nth reads k))))]
    (if (nil? after)
      (conj (vec before) [0 Long/MAX_VALUE])
      (if-let [span (read-span m task after channel)]
        (conj (vec before) span)
        (vec before)))))

(defn- in-span? [[from to] position]
  (and (>= position from) (< position to)))

;; ---- ground truth: true causes, as frontiers ----

;; A causal past is a frontier: for each channel, the greatest position in it, standing
;; for every position on that channel up to it. The set of positions it stands for is the
;; true past only where a task delivers each channel in position order (Safety 3), which
;; is judged on its own: under FIFO a position is delivered exactly when every record
;; before it on its channel is, so "every cause delivered" is "the greatest cause on each
;; channel delivered", and "every cause expressed" is "the greatest expressed". A frontier
;; is a map of a few channels where a set was a copy of the run so far, which is what lets
;; an hour's trace be judged.

(defn- frontier-merge
  ([] {})
  ([a b] (merge-with max a b)))

(declare past-after)

(defn- true-causes
  "The true causes of the record at `pos`, as a frontier: the record an external stamper
  observed and its causes, or the producing task's past once its step delivered."
  [m pos uid-hint]
  (let [memo (:memo m)]
    (if-let [known (get-in @memo [:causes pos])]
      known
      (let [rec (record-at m pos)
            uid (or (:uid rec) uid-hint)
            causes (cond
                     (:stamped-from rec)
                     (let [[id partition offset :as from] (:stamped-from rec)]
                       (if (record-at m from)
                         (frontier-merge (true-causes m from nil) {[id partition] offset})
                         {}))
                     :else
                     (if-let [producer (and uid (get-in m [:producer-by-effect uid]))]
                       (past-after m producer)
                       {}))]
        (swap! memo assoc-in [:causes pos] causes)
        causes))))

(defn- past-after
  "The producing task's causal past once its step `entry` had delivered: every earlier
  delivery and its causes, the causes of every record known received before the step, and
  the delivery itself with its causes. One frontier per step."
  [m entry]
  (let [task (:task-name entry)
        step (:step entry)
        memo (:memo m)
        entries (get-in m [:trace-by-task task])]
    (loop []
      (let [snapshots (get-in @memo [:past task] [])]
        (if (< step (count snapshots))
          (nth snapshots step)
          (let [next (count snapshots)
                delivered (nth entries next)
                previous (if (zero? next) {} (peek snapshots))
                merged (get-in @memo [:merged task] {})
                [received-causes merged]
                (reduce (fn [[acc merged] channel]
                          (let [records (get-in m [:records-by-channel channel])
                                done (get merged channel #{})]
                            (if (nil? records)
                              [acc merged]
                              (let [fresh (for [[from to] (merged-spans (received-spans-before m task channel delivered))
                                                [offset r] (subseq records >= from < to)
                                                :when (not (contains? done offset))]
                                            r)
                                    acc (reduce (fn [acc r] (frontier-merge acc (true-causes m (:pos r) (:uid r)))) acc fresh)]
                                [acc (assoc merged channel (into done (map :offset fresh)))]))))
                        [previous merged]
                        (received-ever m task))
                [id partition offset] (:pos delivered)
                past (-> received-causes
                         (frontier-merge {[id partition] offset})
                         (frontier-merge (true-causes m (:pos delivered) (:uid delivered))))]
            (swap! memo (fn [state] (-> state
                                        (assoc-in [:past task] (conj (get-in state [:past task] []) past))
                                        (assoc-in [:merged task] merged))))
            (recur)))))))

;; ---- the expression bound ----

(defn- expression-bound
  "Everything the task could have seen expressed by the time of `entry`: what it had
  delivered, every position it could have received on a channel it ever received, and every
  position the records there name. Nil when records the task may have received are gone
  from the export, since what they named is gone with them: retention discarded them and
  the export does not hold them (the simulator's keeps every record its retention
  discarded), or their topic was deleted and nothing of it could be dumped."
  [m task entry delivered-positions]
  (reduce (fn [bound channel]
            (let [records (get-in m [:records-by-channel channel])
                  spans (merged-spans (received-spans-up-to m task channel entry))
                  log-start (log-start m channel)]
              (if (or (and (dead? m channel) (nil? records) (seq spans))
                      (and (pos? log-start) (seq spans) (< (ffirst spans) log-start)
                           (empty? (when records (subseq records < log-start)))))
                (reduced nil)
                (reduce (fn [bound [from to]]
                          (let [{:keys [offsets bounds]} (get-in m [:named-by-channel channel])]
                            (if (and offsets (<= from (first offsets)))
                              (let [n (count-at-or-below offsets (dec to))]
                                (if (pos? n) (merge-with max bound (nth bounds (dec n))) bound))
                              (reduce (fn [bound [_ r]]
                                        (let [bound (update bound channel (fnil max -1) (:offset r))]
                                          (if (map? (:meta r))
                                            (reduce (fn [bound [named position]] (update bound named (fnil max -1) position))
                                                    bound (:meta r))
                                            bound)))
                                      bound
                                      (when records (subseq records >= from < to))))))
                        bound
                        spans))))
          (reduce (fn [bound [id partition offset]] (update bound [id partition] (fnil max -1) offset)) {} delivered-positions)
          (received-ever m task)))

;; ---- what a task owes ----

(defn- owed
  "Records the task owes a delivery of: on every channel it ever received, those in a span
  some observation shows it received and committed; with no read observations at all,
  every record at or above the start position on its final channels."
  [m task]
  (let [observed? (seq (get-in m [:reads-by-task task]))]
    (into {}
          (for [channel (if observed? (received-ever m task) (received-finally m task))
                :let [records (get-in m [:records-by-channel channel])]
                :when records]
            [channel
             (if observed?
               (vals (reduce (fn [acc [from to]] (into acc (subseq records >= from < to)))
                             (sorted-map) (received-spans-ever m task channel)))
               (vals (subseq records >= (get-in m [:starts-by-task task channel] 0))))]))))

;; ---- the replay ----

(defn- settled-now
  "The channels of the delivered message's causes settled by evidence at this moment: a
  channel the task does not receive, one whose cause lies below the start position, or a
  dead channel the task is not known to have received the cause from. A cause the task had
  received is never settled by its channel's death (Safety 9)."
  [m task entry received causes max-delivered]
  (let [start (get-in m [:starts-by-task task] {})]
    (set (for [[channel offset] causes
               :when (or (not (contains? received channel))
                         (< offset (get start channel 0))
                         (and (dead? m channel)
                              (not (or (some-> (get max-delivered channel) (> offset))
                                       (some #(in-span? % offset) (received-spans-before m task channel entry))))))]
           channel))))

(defn- check-expression
  "The expression checks on one send: Structural 14 and 12, over-expression, Structural 15.
  `past` is the sender's causal past as a frontier and `excused` the channels in it that no
  longer exist. With `upper` nil, what the sender could have seen expressed is unknown, and
  the two checks that need it are not made."
  [m sent-pos meta past upper excused]
  (if (= :undecodable meta)
    [(str "Trace: the frontier expressed by " (pos-str sent-pos) " is undecodable")]
    (let [[own-id own-partition own-offset] sent-pos
          own-channel [own-id own-partition]]
      (concat
       (for [[channel position] meta
             :when (and (= channel own-channel) (>= position own-offset))]
         (str "Structural 14: " (pos-str sent-pos) " expresses dependency on own channel at or above itself: " position))
       ;; Structural 12 lets a process express a position it learned from the metadata of a
       ;; message it received, assigned or not: an out-of-contract stamp naming the log end
       ;; is held, and its position travels in the holder's frontier meanwhile.
       (when upper
         (for [[channel position] meta
               :let [last (get-in m [:last-assigned channel])]
               :when (and last (> position last) (> position (get upper channel -1)))]
           (str "Structural 12: " (pos-str sent-pos) " expresses position " (channel-str channel) "@" position
                " which was unassigned at send time (last assigned: " last ")")))
       (when upper
         (for [[channel position] meta
               :let [bound (get upper channel)]
               :when (or (nil? bound) (> position bound))]
           (str "Over-expression: " (pos-str sent-pos) " expresses " (channel-str channel) "@" position
                " above anything its sender had delivered or seen expressed at send time (bound: " bound ")")))
       (for [[channel offset] past
             :when (not (contains? excused channel))
             :let [expressed (get meta channel)]
             :when (or (nil? expressed) (< expressed offset))]
         (str "Structural 15: " (pos-str sent-pos) " fails to express cause " (pos-str (conj channel offset))
              " (expressed: " expressed ")"))))))

(defn- trace-pos [m entry]
  (let [trace-topic (get-in m [:export :trace-topic])
        live (first (filter :alive (get-in m [:by-name trace-topic])))
        partition (try (Long/parseLong (:tp entry)) (catch Exception _ nil))]
    (if (and live partition)
      [(:id live) partition (:to entry)]
      [(str "trace:" (:task-name entry)) 0 (:to entry)])))

(defn- replay-task
  "Replays one task's deliveries in trace order. Returns {:violations [...] :delivered [pos...]}."
  [m task]
  (let [start (get-in m [:starts-by-task task] {})
        fed (owed m task)
        fed-set (set (map :pos (mapcat val fed)))]
    (loop [entries (get-in m [:trace-by-task task])
           delivered-set #{}
           delivered []
           engine-past {}
           max-delivered {}
           violations []]
      (if (empty? entries)
        {:violations violations :delivered delivered :fed fed :fed-set fed-set
         :delivered-set delivered-set}
        (let [entry (first entries)
              pos (:pos entry)
              channel (:channel entry)
              rec (record-at m pos)
              received (received-at m task entry)
              causes (true-causes m pos (:uid entry))
              settled (settled-now m task entry received causes max-delivered)
              v (cond-> []
                  (and rec (not= (:uid rec) (:uid entry)))
                  (conj (str "Trace: " task " reports delivering " (:uid entry) " at " (pos-str pos)
                             " but the committed record there carries " (:uid rec)))
                  (and (nil? rec) (not (dead? m channel)) (<= (log-start m channel) (:offset entry)))
                  (conj (str "Trace: " task " reports delivering " (pos-str pos) " (" (:uid entry)
                             ") but no committed record exists there"))
                  (contains? (:undecodable m) pos)
                  (conj (str "Safety 7: " task " delivered " (:uid entry) " (" (pos-str pos)
                             ") whose causal metadata is present and undecodable"))
                  (not (contains? received channel))
                  (conj (str "Declaration: " task " delivered " (:uid entry) " (" (pos-str pos)
                             ") from a channel it does not receive")))
              ;; Everything delivered lies within the engine's past, so a cause not within it was
              ;; not delivered either.
              v (into v (for [[channel offset] causes
                              :when (not (contains? settled channel))
                              :let [bound (get engine-past channel Long/MIN_VALUE)]
                              :when (> offset bound)]
                          (str "Safety 1 (delivery-time): " task " delivered " (:uid entry) " (" (pos-str pos)
                               ") while its cause " (pos-str (conj channel offset))
                               " was neither delivered, nor settled by evidence, nor within the delivered past")))
              delivered-set (conj delivered-set pos)
              delivered (conj delivered pos)
              meta (if (map? (:meta rec)) (:meta rec) {})
              engine-past (reduce (fn [p [c position]] (update p c (fnil max Long/MIN_VALUE) position))
                                  (update engine-past channel (fnil max Long/MIN_VALUE) (:offset entry))
                                  meta)
              max-delivered (update max-delivered channel (fnil max -1) (:offset entry))
              upper (expression-bound m task entry delivered)
              past (past-after m entry)
              excused (set (filter #(dead? m %) (keys past)))
              v (into v (check-expression m (trace-pos m entry) (:meta entry) past upper excused))
              v (reduce (fn [v [topic uid]]
                          (let [matches (filter #(= (topic-name m (:topic %)) topic) (get-in m [:records-by-uid uid]))]
                            (cond
                              (empty? matches)
                              (if (nothing-discarded? m topic)
                                (conj v (str "Host obligation 3: " task " step " (:tp entry) "@" (:to entry)
                                             " committed a send of " uid " to " topic
                                             " but no committed record carries it"))
                                v)
                              :else
                              (let [v (if (> (count matches) 1)
                                        (conj v (str "Host obligation 6: " uid " is committed " (count matches)
                                                     " times on " topic
                                                     "; a superseded execution's step was committed too"))
                                        v)]
                                (reduce (fn [v r] (into v (check-expression m (:pos r) (:meta r) past upper excused)))
                                        v matches)))))
                        v (:effects entry))]
          (recur (rest entries) delivered-set delivered engine-past max-delivered (into violations v)))))))

;; ---- final judgements ----

(defn- final-order-checks
  "Safety 2, Safety 3 and Safety 1 over the task's committed deliveries."
  [m task delivered]
  (let [first-index (reduce (fn [idx [i pos]] (if (contains? idx pos) idx (assoc idx pos i)))
                            {} (map-indexed vector delivered))
        dupes (for [[i pos] (map-indexed vector delivered)
                    :let [previous (get first-index pos)]
                    :when (not= previous i)]
                (str "Safety 2: " task " delivered " (pos-str pos) " twice (indexes " previous " and " i ")"))
        fifo (:violations
              (reduce (fn [{:keys [last] :as acc} [id partition offset :as pos]]
                        (let [channel [id partition]
                              previous (get last channel)]
                          (-> acc
                              (assoc-in [:last channel] offset)
                              (cond-> (and previous (<= offset previous))
                                (update :violations conj
                                        (str "Safety 3: " task " delivered " (pos-str pos) " after position "
                                             previous " of the same channel"))))))
                      {:last {} :violations []} delivered))
        ;; Per channel, positions in order with the latest first-delivery index among those
        ;; up to each: a frontier's cause on that channel was delivered after the effect
        ;; exactly when that index is past the effect's.
        latest-by-channel (into {} (for [[channel positions] (group-by (fn [[[id partition _] _]] [id partition]) first-index)]
                                     [channel (let [sorted (sort-by (fn [[[_ _ offset] _]] offset) positions)]
                                                {:offsets (mapv (fn [[[_ _ offset] _]] offset) sorted)
                                                 :latest (vec (rest (reductions (fn [latest [_ i]] (max latest i)) -1 sorted)))})]))
        latest-up-to (fn [channel offset]
                       (when-let [{:keys [offsets latest]} (get latest-by-channel channel)]
                         (let [n (count-at-or-below offsets offset)]
                           (when (pos? n)
                             (let [i (nth latest (dec n))]
                               ;; The position delivered at that index is only looked up for a violation.
                               [(delay (first (filter #(= i (get first-index (conj channel %))) (take n offsets)))) i])))))
        pairs (for [[i effect] (map-indexed vector delivered)
                    [channel offset] (true-causes m effect nil)
                    :let [[cause-offset cause-index] (latest-up-to channel offset)]
                    :when (and cause-index (> cause-index i))]
                (str "Safety 1: " task " delivered effect " (pos-str effect) " (index " i
                     ") before its cause " (pos-str (conj channel @cause-offset)) " (index " cause-index ")"))]
    (concat dupes fifo pairs)))

(defn- held-by-unsettled-stamp? [m rec received start exempt owed-by-channel]
  (let [meta (:meta rec)]
    (and (map? meta)
         (some (fn [[channel position]]
                 (and (contains? received channel)
                      (not (dead? m channel))
                      (>= position (get start channel 0))
                      (let [last (get-in m [:last-assigned channel])]
                        (or (nil? last) (< last position)
                            (some (fn [r] (and (<= (:offset r) position) (contains? exempt (:pos r))))
                                  (get owed-by-channel channel))))))
               meta))))

(defn- exemptions
  "Records not owed at quiescence: behind an undecodable header on their channel, and
  behind a stamp naming a position no later record on that channel settles, transitively."
  [m task received start]
  (let [owed-by-channel (into {} (for [channel received
                                       :let [records (get-in m [:records-by-channel channel])]
                                       :when records]
                                   [channel (vals (subseq records >= (get start channel 0)))]))
        initial (into #{} (for [[_ records] owed-by-channel
                                r (:exempt (reduce (fn [{:keys [behind] :as acc} r]
                                                     (let [behind (or behind (contains? (:undecodable m) (:pos r)))]
                                                       (cond-> (assoc acc :behind behind)
                                                         behind (update :exempt conj r))))
                                                   {:behind false :exempt []} records))]
                            (:pos r)))]
    (loop [exempt initial]
      (let [next (reduce (fn [exempt [_ records]]
                           (:exempt (reduce (fn [{:keys [behind exempt]} r]
                                              (if (contains? exempt (:pos r))
                                                {:behind true :exempt exempt}
                                                (if (or behind (held-by-unsettled-stamp? m r received start exempt owed-by-channel))
                                                  {:behind true :exempt (conj exempt (:pos r))}
                                                  {:behind false :exempt exempt})))
                                            {:behind false :exempt exempt} records)))
                         exempt owed-by-channel)]
        (if (= next exempt) exempt (recur next))))))

(defn- liveness [m task {:keys [fed delivered-set]}]
  (let [received (received-finally m task)
        start (get-in m [:starts-by-task task] {})
        exempt (exemptions m task received start)]
    (for [[_ records] fed
          r records
          :when (not (contains? delivered-set (:pos r)))
          :when (not (contains? exempt (:pos r)))]
      (str "Liveness 1: " task " never delivered " (:uid r) " (" (pos-str (:pos r))
           "), which it received on a channel it declares at or above its start position"))))

(defn- safety-8 [m task {:keys [delivered]}]
  (let [start (get-in m [:starts-by-task task] {})
        last-read (:next-read (last (get-in m [:reads-by-task task])) {})
        max-delivered (reduce (fn [acc [id partition offset]] (update acc [id partition] (fnil max -1) offset)) {} delivered)]
    (for [channel (received-finally m task)
          :when (not (dead? m channel))
          :let [covered (max (get start channel 0)
                             (inc (get max-delivered channel -1))
                             (get last-read channel 0))
                ls (log-start m channel)]
          :when (> ls covered)]
      (str "Safety 8: " task " sailed past discarded positions on " (channel-str channel)
           " (earliest retained " ls " > covered position " covered ") without failing closed"))))

(defn- duplicates-across-tasks [m]
  (let [seen (atom {})]
    (doall
     (for [[task entries] (:trace-by-task m)
           entry entries
           :let [process (process-of task)
                 key [process (:pos entry)]
                 previous (get @seen key)]
           :when (do (when-not previous (swap! seen assoc key task))
                     (and previous (not= previous task)))]
       (str "Safety 2: " process " delivered " (pos-str (:pos entry)) " at two tasks, " previous " and " task)))))

(defn- refusals-justified [m]
  (for [[process status] (:first-refusal m)
        :when (not-any? (fn [f] (and (some #{(:refusal status)} (:justifies f)) (< (:index f) (:index status))))
                        (:faults m))]
    (str "Operational 1/6: " process " refused " (:refusal status) " (\"" (:detail status)
         "\") though no injected fault before it justifies that reason")))

(defn- orphans [m]
  (for [r (mapcat val (:records-by-channel m))
        :let [uid (:uid r)]
        :when (and uid (str/includes? uid ">") (not (contains? (:producer-by-effect m) uid)))]
    (str "Host obligation 3: " (topic-name m (first (:channel r))) "@" (second (:channel r)) "@" (:offset r)
         " carries " uid ", which no committed step's trace claims to have sent")))

(defn- recreations
  "SPEC Assumption 2. In the simulator every receiver re-initialises at the recreation, so
  a step committed afterwards while the dead incarnation is still received is a step on the
  wrong log. On a cluster the host re-creates the task only later, so the judgement starts
  at the re-initialisation the fault records, if it records one."
  [m]
  (if (= :simulator (get-in m [:export :source]))
    (let [flagged (atom #{})]
      (doall
       (for [reads (sort-by :index (:reads (:export m)))
             :let [task (task-name (:process reads) (:task reads))]
             channel (received-at-index m task (:index reads))
             :let [key [task (first channel)]]
             :when (and (not (contains? @flagged key)) (recreated-at? m channel (:index reads)))]
         (do (swap! flagged conj key)
             (str "Assumption 2: " task " committed a step while its received topic " (topic-name m (first channel))
                  " (" (channel-str channel) ") had been deleted and recreated under the same name, without failing closed")))))
    (for [f (:faults m)
          :when (= :recreate (:kind f))
          :let [ends (get-in f [:details :reinitialised-ends])
                topic (get-in f [:details :topic])]
          :when (and ends topic)
          [task entries] (:trace-by-task m)
          :let [decl (get-in m [:export :processes (process-of task)])]
          :when (some #{topic} (:receives decl))
          :let [after (first (filter (fn [e] (let [end (get ends (:tp e))] (and end (>= (:to e) end)))) entries))]
          :when after]
      (str "Assumption 2: " task " delivered " (:uid after) " after its received topic " topic
           " was deleted and recreated under the same name, without failing closed"))))

(defn- all-tasks [m]
  (let [declared (if (seq (:task-info m))
                   (keys (:task-info m))
                   (for [[name decl] (get-in m [:export :processes])
                         :let [width (reduce max 0 (for [topic (:receives decl)
                                                         t (get-in m [:by-name topic])]
                                                     (:partitions t)))]
                         p (range width)]
                     (task-name name p)))]
    (distinct (concat declared (keys (:trace-by-task m))))))

(defn check
  "Judges one export. Returns {:valid? boolean :violations [strings] :stale-reads n}, the
  last being the observations set aside as answered from a stale cache."
  [export]
  (let [m (build export)
        tasks (all-tasks m)
        replays (into {} (map (fn [task] [task (replay-task m task)]) tasks))
        refused? (fn [task] (contains? (:first-refusal m) (process-of task)))
        violations (concat
                    (mapcat (comp :violations val) replays)
                    (mapcat (fn [[task replay]] (final-order-checks m task (:delivered replay))) replays)
                    (mapcat (fn [[task replay]] (when-not (refused? task) (liveness m task replay))) replays)
                    (mapcat (fn [[task replay]] (when-not (refused? task) (safety-8 m task replay))) replays)
                    (duplicates-across-tasks m)
                    (refusals-justified m)
                    (orphans m)
                    (recreations m))
        violations (vec violations)]
    {:valid? (empty? violations)
     :violations violations
     :stale-reads (count (:stale-reads m))}))

(defn check-file
  "Judges the export at `path`."
  [path]
  (check (read-export path)))
