(ns parsley-jepsen.wire
  "A second implementation of the frozen `parsley.causes` grammar, written from Parsley's
  `docs/wire-format.md` alone, which doubles as a check that the page stands alone.

  `decode` takes the header value as a byte array (or nil, for a header present with a null
  value) and returns either `{:causes {[topic-id partition] position ...}}`, where a topic
  id is the Kafka topic ID's sixteen bytes rendered as a hyphenated UUID, or
  `{:undecodable reason}`. Constraints 1 to 7 of the page are decidable from the bytes and
  each refuses; constraint 8, that a position is the offset of a committed record, binds the
  writer and is judged by the checker against the dumps, not here."
  (:import [java.nio ByteBuffer BufferUnderflowException]))

(def header-key
  "The one record header carrying causal metadata; the `parsley.` prefix is reserved."
  "parsley.causes")

(def ^:private version-byte 1)

(defn- fail [reason]
  (throw (ex-info reason {:undecodable reason})))

(defn- read-varint
  "One minimal unsigned base-128 varint: seven payload bits per byte, lowest bits first,
  the high bit set on every byte but the last. The values a varint may spell are exactly
  0 to 2^31 - 1: a terminal zero byte after the first, a fifth byte carrying anything beyond
  its low three bits, and a sixth byte are each undecodable."
  [^ByteBuffer buffer field]
  (loop [value 0 shift 0]
    (let [b (bit-and (.get buffer) 0xFF)]
      (when (and (= shift 28) (not= 0 (bit-and b 0xF8)))
        (fail (str field " varint exceeds the non-negative int range")))
      (let [value (bit-or value (bit-shift-left (bit-and b 0x7F) shift))]
        (if (zero? (bit-and b 0x80))
          (do (when (and (pos? shift) (zero? (bit-and b 0x7F)))
                (fail (str "non-minimal varint in " field)))
              value)
          (recur value (+ shift 7)))))))

(defn- unsigned-compare
  "Unsigned lexicographic order of two sixteen-byte topic ids, each as [msb lsb]."
  [[amsb alsb] [bmsb blsb]]
  (let [c (Long/compareUnsigned amsb bmsb)]
    (if (zero? c) (Long/compareUnsigned alsb blsb) c)))

(defn topic-id-string
  "The sixteen bytes of a topic id, given as two big-endian longs, as a hyphenated UUID."
  [msb lsb]
  (str (java.util.UUID. msb lsb)))

(defn- read-group [^ByteBuffer buffer previous-topic group-index]
  (let [msb (.getLong buffer)
        lsb (.getLong buffer)]
    (when (and (zero? msb) (zero? lsb))
      (fail (str "zero topic id at group " group-index "; the substrate never assigns it to a channel")))
    (when (and previous-topic (<= (unsigned-compare [msb lsb] previous-topic) 0))
      (fail (str "topics not strictly ascending at " (topic-id-string msb lsb))))
    (let [partition-count (read-varint buffer "partition count")
          topic (topic-id-string msb lsb)]
      (when (zero? partition-count)
        (fail (str "topic " topic " names zero partitions")))
      (loop [i 0 previous-partition -1 pairs []]
        (if (= i partition-count)
          [[msb lsb] pairs]
          (let [partition (read-varint buffer "partition")]
            (when (<= partition previous-partition)
              (fail (str "partitions not strictly ascending at " topic "-" partition)))
            (let [position (.getLong buffer)]
              (when (neg? position)
                (fail (str "negative position " position " on " topic "-" partition)))
              (when (= position Long/MAX_VALUE)
                (fail (str "position " position " on " topic "-" partition
                           " is beyond any position a channel can assign")))
              (recur (inc i) partition (conj pairs [[topic partition] position])))))))))

(defn decode
  "Decodes one header value. Returns {:causes {[topic-id partition] position}} or
  {:undecodable reason}. A nil or empty value is present and fails the grammar."
  [^bytes value]
  (try
    (when (nil? value)
      (fail "causes header present with null value"))
    (let [buffer (ByteBuffer/wrap value)
          version (bit-and (.get buffer) 0xFF)]
      (when (not= version version-byte)
        (fail (str "unknown causes format version " version)))
      (let [topic-count (read-varint buffer "topic count")]
        (loop [group 0 previous nil causes {}]
          (if (= group topic-count)
            (do (when (pos? (.remaining buffer))
                  (fail (str (.remaining buffer) " trailing bytes after " topic-count " topic groups")))
                {:causes causes})
            (let [[topic pairs] (read-group buffer previous group)]
              (recur (inc group) topic (into causes pairs)))))))
    (catch BufferUnderflowException _
      {:undecodable "truncated causes header"})
    (catch clojure.lang.ExceptionInfo e
      (if-let [reason (:undecodable (ex-data e))]
        {:undecodable reason}
        (throw e)))))

(defn hex->bytes
  "Bytes from a hex string, as the export spells header values."
  ^bytes [^String hex]
  (let [n (quot (count hex) 2)
        out (byte-array n)]
    (dotimes [i n]
      (aset-byte out i (unchecked-byte (Integer/parseInt (subs hex (* 2 i) (+ 2 (* 2 i))) 16))))
    out))

(defn decode-hex
  "Decodes a header value spelled in hex, or nil for a header present with a null value."
  [hex]
  (decode (when hex (hex->bytes hex))))
