# parsley-jepsen

A [Jepsen](https://jepsen.io) test for [Parsley](https://github.com/tobyjamesclements/parsley).
Parsley provides causal delivery order for Kafka Streams processors: if message A is a cause of
message B, every process that delivers both delivers A first, across restarts and for the whole
lifetime of a process. Where the guarantee cannot be upheld, a process stops rather than weaken
it.

The plan below is the contract for what gets built, and the code follows it. Criteria are cited
as in Parsley's `SPEC.md`: "Safety 1", "Structural 15", "Host obligation 6". The first four
items of the build order are in place and verified; [Build order](#build-order) says what each
verification covered and what it did not.

## Why a Jepsen test

Parsley's own suite has three layers: unit tests over the pure protocol, a deterministic
simulator driving real engines under a simulated host that honours the spec's Host obligations,
checked against a happened-before oracle kept outside the engine, and integration tests against
an embedded single-node KRaft broker. Sabotage modes prove the oracle catches each violation
class.

None of that exercises the Host obligations and Assumptions the spec places on Kafka Streams and
Kafka, which the implementation cannot enforce and can only detect breached. This test runs the
real thing: a replicated KRaft cluster, several Streams instances, partitions, killed and paused
processes, clock skew, real retention timing, for hours, observed by a checker that never trusts
the engine.

Questions only this test answers:

- Does Streams' transactional fencing meet Host obligation 6 for an instance paused past its
  transaction timeout and then resumed?
- Does a partition between an instance and some brokers ever feed a channel out of order (Host
  obligation 1), or surface an out-of-range fetch the runtime misclassifies?
- Does the bootstrap's generation-fenced pre-commit of start positions survive a group
  coordinator failover mid-join?
- Does the identity check's three-answer corroboration hold when brokers' metadata views lag
  under a partition, which a single embedded broker hides?

## What is checked

Every property is judged from ground truth reconstructed outside the engine, as Parsley's
`Oracle` does.

| Property | Check |
|---|---|
| Safety 1 | Within each task's delivery sequence, over the whole run, every cause delivered by that task precedes its effect |
| Safety 2 | No message is delivered twice by a task, across restarts and migrations between instances |
| Safety 3 | Positions on each channel strictly increase in each task's delivery sequence |
| Structural 12, 14, 15 | Every send's causal metadata covers every cause its sender had delivered or seen expressed, never names an unassigned position, and never names its own channel at or above its own position |
| Liveness 1 | At quiescence, every committed record on a received partition at or above the process's start position was delivered, with the spec's exemptions: a process that refused for a ledgered reason, and records held behind an undecodable header or an out-of-contract stamp |
| Host obligations 3, 5, 6 | Each delivery the topology says produces a send yields exactly one committed downstream record, under crashes mid-step and fenced zombies |
| Operational 1, 6 | Every refusal in the status history follows an injected fault of the matching kind; network partitions, broker kills, pauses and clock skew justify none |

## The system under test

**Substrate.** Apache Kafka, KRaft, and nothing else. Two broker versions: the 3.7.0 floor
Parsley declares, and the 4.3 line its clients build against. Topics are replicated with an
in-sync minimum and have several partitions, so cross-partition causality exists and every
process has more than one task. Unclean leader election stays off; a run with it on loses
committed data at the broker, which is Kafka's failure rather than Parsley's, and is labelled
separately.

**Nodes.** Debian nodes, each running one combined controller-plus-broker and one instance of
the harness app under the same application-id prefix, so each declared process's tasks spread
over the instances and migrate when an instance dies.

**The harness app.** A Java main that lives in Parsley's test tree, in the same package, so it
can reach the codec and the simulator. It builds processes from a topology description with
fan-out, fan-in, a cycle and a self-channel. Every handler forwards deterministically and also
sends one trace record per delivery to a declared trace topic that no process receives. The
trace send is an effect of the step, so it commits atomically with the delivery: an aborted
step leaves no trace record, which is exactly Safety 2's "has not occurred". A trace record
names the process, the task, the delivered channel's topic id, partition and offset, and the
message's uid. Because it is a Parsley send, its own `parsley.causes` header is the task's
expressed frontier at that step. Keyed by task, each task's trace lands on one partition in
step order. The app serves `Parsley.status()` over a local port and logs to a file the test
collects.

As built (`JepsenTopology`, `JepsenHarness` in Parsley's test tree, branch
`claude/new-session-7c5301`): topics `src`, `a`, `b`, `c`, `d`, `loop`, `self` and `trace`, all
of one width; `splitter` (src → a, b), `joiner` (a, b, loop → c), `cycler` (c → d, and loop
half the time), `selfer` (d, self → self two times in three), each forward bounded at six hops.
Every value is its own uid and each forward appends `>process>topic`, so a record is matched
to the delivery that produced it by its uid alone. The trace value is EDN naming the delivery
and the effects the step sent, so a checker never needs the forwarding rule. The jar is built
by `./mvnw -Pjepsen-harness -DskipTests package` in Parsley and has these commands:

```
java -jar parsley-jepsen-harness.jar run --bootstrap n1:9092 --prefix jepsen \
    --state-dir /var/lib/parsley --status-port 8080 --log-file /var/log/parsley.log \
    [--drop-topic loop]
java -jar parsley-jepsen-harness.jar create-topics --bootstrap n1:9092 --partitions 3 \
    --replication 3 --min-isr 2
java -jar parsley-jepsen-harness.jar export --bootstrap n1:9092 --out run.edn
java -jar parsley-jepsen-harness.jar check --in run.edn
java -jar parsley-jepsen-harness.jar export-simulator --out test/resources/exports --seeds 120
java -jar parsley-jepsen-harness.jar codec-vectors --out test/resources/codec-vectors.edn
```

`run` keeps serving status after a process stops, since a refusal is terminal by design and
the status clients must read it. `export` writes the export the checker judges; `check` replays
it through Parsley's own `Oracle`.

## Workload

- **Producer clients** send to source topics: unstamped records, which Safety 6 makes
  immediately deliverable; stamped records whose header names a record the client has already
  observed committed, as the simulator's external producers do; and occasionally a malformed
  header, or an out-of-contract stamp naming the log-end offset. Every value embeds a uid so an
  indeterminate send can be matched to the record it may have produced.
- **Status clients** poll each node's status into the history, so refusals are timestamped
  against faults.
- **The final phase** heals the cluster, restarts killed instances, waits for zero lag on every
  received partition of every process that has not refused, then dumps every topic from
  earliest under `read_committed`, headers included, and the trace. The dumps enter the history
  as final-read operations.

## Nemeses

Jepsen's combined nemesis provides network partitions, broker kill and restart, SIGSTOP pauses
and clock skew. Instance-level faults on the harness app: kill and restart, pause past the
transaction timeout and resume so the zombie must be fenced, wipe the state directory before a
restart, scale instances up and down. Then the Parsley-specific faults, each with the outcome
the checker asserts:

| Fault | Expected outcome |
|---|---|
| Delete records past a lagging task's committed position | `POSITIONS_DISCARDED_UNREAD`; nothing delivered past the gap |
| Retention discards a held message's copy on its topic | No refusal; delivered from the ordering changelog in order |
| Delete a received topic while messages are held from it | `CHANNEL_DELETED_WITH_UNDELIVERED_MESSAGES` |
| Delete and recreate a received topic while the process is down | `CHANNEL_IDENTITY_CHANGED` at the next start |
| Reset the group's offsets backwards while the process is down | Re-fed records dropped; no duplicate delivery |
| Delete the ordering changelog, keeping the group's offsets | `ORDERING_STATE_LOST` |
| Add partitions to the widest received topic, then restart | `TASK_WIDTH_CHANGED` |
| Restart with a declaration dropping a topic that holds messages | `CHANNEL_REMOVED_WITH_HELD_MESSAGES` |
| Malformed `parsley.causes` header from an external producer | `UNDECODABLE_METADATA`; nothing delivered past it |
| Stamp naming the log-end offset | A hold until the channel's next record; no refusal |
| Partition, broker kill, pause, clock skew | No refusal; only a pause |

A refusal is terminal by design, so a run schedules at most one refusal-class fault per
process, or performs the runbook's reset afterwards. Rebalances and transaction timeouts run
to tens of seconds, so the nemesis interval must let the cluster recover between faults or
nothing ever delivers.

## The checker

- **Happened-before** comes from the trace and the topology, not from headers. A record sent by
  task T in the step that delivered D is caused by D, by every earlier delivery at T, by the
  records a client had observed before a stamped send, and transitively by their causes. This
  is a sound subset of the true causes. Causes known only through receipt of held messages are
  caught by the expression check instead.
- **Receipt** is known from the read observations, never guessed. A status client reads the
  trace's last stable offsets, then every group's committed positions, then the trace's high
  watermarks. A trace record below the first bracket had committed before the positions were
  read, so its step's receipts lie within them; a trace record at or past the second had not
  been acknowledged, so its step had not committed and everything the positions cover was
  received before it. That gives a lower bound on receipt, which extends the causal past with
  the causes of received messages, and an upper bound, which bounds what a send may express.
  Both brackets hold with several tasks sharing a trace partition and with a fenced zombie's
  open transaction, which a last-stable-offset bracket on the far side would not.
- **The export** is one EDN map: the declaration, topic identities with log starts and
  liveness, every task's received channels, every committed record with its raw
  `parsley.causes` header, the trace, the read observations, the status history, the faults
  with the refusals each justifies, and start positions. The simulator and a cluster produce
  the same shape (`JepsenExport` in Parsley).
- **Decoding** uses a second implementation of the frozen grammar in Parsley's
  `docs/wire-format.md`, written in Clojure from that page alone, which doubles as a check
  that the page stands alone. Its test vectors come from Parsley's `CausesCodecTest`.
- **Calibration.** A checker is worth nothing until it has caught something. Simulator runs
  under each of Parsley's `Sabotage` modes are exported into the trace format, and the checker
  must flag every mode. A live inversion, manufactured by an external producer that stamps
  less than it knows, must be caught on the cluster.

  Done for the export-based replay in Parsley (`JepsenExportOracleCalibrationTest`) and for
  `checker.clj` (`checker_test.clj`) over `test/resources/exports`: six honest seeds and the
  honest dead-holds scenario pass, three seeds per sabotage mode and the host fault are
  flagged, the constructed `DELIVER_PAST_DEAD_HOLDS` inversion is flagged as Safety 1, and a
  real two-instance run against the embedded broker (`cluster-honest.edn`) passes. Over 120
  seeds per mode, the export replay catches most of what the simulator's own oracle catches;
  `index.edn` records the figures. The gap is receipt the observations do not cover, which the
  simulator's oracle sees exactly.

  Done on the cluster (`--calibrate inversion`): while nothing else sends, H goes to `a`'s
  partition 0 with an out-of-contract stamp naming `b`'s log end there, so the joiner holds
  it; E then goes to that position of `b` with no header, though its producer had observed H
  and the history records that it had. The joiner delivers E at once, which settles H's
  stamp, and H after it. Both checkers flag the run: Safety 1 at delivery time and over the
  delivered pair, and Structural 15 on every downstream send that could not express the cause
  the stamper hid.

## Out of scope

- A topic recreated between two polls, and a stamper that lies about its causes, are outside
  the guarantee by Assumptions 17 and 13. The generator keeps them out, or the checker would
  report inversions the spec permits.
- Confluent Server, Confluent Cloud and other Kafka-protocol brokers. The spec names Apache
  Kafka, and a failure on it is attributable and reproducible upstream.

## Layout

```
project.clj
src/parsley_jepsen/
  core.clj       command line and test map, and the history-to-export adapter
  db.clj         Apache Kafka KRaft: install, configure, start, stop, kill, pause, logs
  client.clj     producer, consumer and admin clients behind the operations
  workload.clj   generator: producer and status operations, the final phase
  nemesis.clj    the combined nemesis plus the Parsley-specific faults
  wire.clj       the parsley.causes decoder
  checker.clj    the oracle over the trace and the topic dumps
test/parsley_jepsen/
  wire_test.clj      the codec vectors
  checker_test.clj   simulator exports under each sabotage mode, and a cluster run
test/resources/
  codec-vectors.edn  exported from Parsley's CausesCodecTest and CausesMalformationVectors
  exports/           the calibration set, with index.edn naming each file's expected outcome
```

The harness app is built from Parsley's test tree as a jar the DB adapter installs on each
node alongside the broker. On Parsley's side the pieces are `JepsenTopology`,
`JepsenHarness`, `JepsenExport` and `JepsenEdn`, `JepsenClusterExport`,
`JepsenExportOracle` (the replay through `Oracle`), `JepsenSimulatorExport` (an observer on
`Scenario` and `SimProcess`), `JepsenCodecVectors`, and the tests
`JepsenHarnessIntegrationTest` and `JepsenExportOracleCalibrationTest`.

## Build order

- [x] The harness app and the trace, run locally against Parsley's embedded broker, with the
      Java `Oracle` as the first checker over the trace. Two instances, external unstamped and
      stamped records, an out-of-contract stamp, an instance killed with its state wiped and
      restarted: the export replays clean, and negative controls (an inverted pair, an erased
      delivery, a twice-committed effect) do not. A malformed header stops the receiving
      process and the replay accepts the justified refusal.
- [x] The Clojure decoder, against the codec vectors.
- [x] The checker, and its calibration against simulator exports.
- [x] The KRaft DB adapter, developed on Jepsen's docker nodes (jepsen-io/jepsen v0.3.9,
      `docker/`, three Debian bookworm nodes on one 8 GB Docker VM, brokers and instances at
      512 MB heaps). Verified: Kafka 4.3.1, `--time-limit 120 --nemesis none` ends with the
      `:parsley` checker `:valid? true` over 7318 trace entries and as many records, quiesced,
      and Parsley's own Oracle replay clean over the same export; `--calibrate inversion` is
      flagged by both. Not verified: Kafka 3.7.0, more than three nodes, and runs longer than
      two minutes. The checker's replay is still quadratic in a task's steps (22 seconds for
      this run, 280 before the receipt spans were merged), which a long run will feel.
- [ ] Nemeses in order of expected yield: instance pause and kill, partitions during commit,
      retention and record deletion, topic delete and recreate, offset reset, changelog
      deletion, clock skew.
- [ ] Long mixed runs on both broker versions. A separate, labelled unclean-election run.

## Reference code

- `jepsen.tests.kafka`, in the Jepsen library: the queue workload and checker written for the
  Redpanda analysis and reused for Bufstream. It tests the broker's own semantics through send,
  poll and transaction operations. Running it separately on the same cluster says whether an
  anomaly is the broker's before Parsley is blamed.
- `jepsen.redpanda.db.kafka`, in the Redpanda test repository: an adapter that installs Apache
  Kafka on Debian nodes. It targets Kafka 3.0 on ZooKeeper, so it is a template for the
  adapter's shape, not something to run.

## Running

The decoder and checker tests run with `lein test`. Without Leiningen, plain Clojure from
Maven Central runs them from the project root:

```
java -cp "lib/*:src:test" clojure.main -e \
  "(require 'clojure.test 'parsley-jepsen.wire-test 'parsley-jepsen.checker-test)
   (clojure.test/run-tests 'parsley-jepsen.wire-test 'parsley-jepsen.checker-test)"
```

The cluster test follows Jepsen convention, with the harness jar built from Parsley beside
it. On Jepsen's docker nodes (jepsen-io/jepsen at v0.3.9, whose `docker/` directory later
releases dropped), `docker/docker-compose.parsley.yml` mounts this project and the jar into
the control node:

```
cd jepsen/docker
PARSLEY_JEPSEN_ROOT=/path/to/parsley-jepsen PARSLEY_TARGET=/path/to/parsley/target \
  COMPOSE="-f /path/to/parsley-jepsen/docker/docker-compose.parsley.yml" bin/up --dev --daemon -n 3
docker exec -it jepsen-control bash
cd /parsley-jepsen
lein run test --nodes-file ~/nodes --kafka-version 4.3.1 --time-limit 120 --nemesis none \
    --harness-jar /parsley-target/parsley-0.4.0-SNAPSHOT-jepsen-harness.jar \
    --kafka-heap 512m --harness-heap 512m
lein run test --nodes-file ~/nodes --kafka-version 4.3.1 --time-limit 3600 \
    --nemesis partition,instance-pause,instance-kill,truncate --nemesis-interval 60 ...
```

`--nemesis` takes `none` or any of `partition`, `kill`, `pause`, `clock` (Jepsen's, over the
brokers), `instance-kill`, `instance-pause`, `wipe`, `discard-held-copy`, `reset-offsets`,
and the refusal-class faults `truncate`, `delete-topic`, `recreate-topic`, `delete-changelog`,
`add-partitions`, `restart-dropping`, `corrupt`, of which a run schedules at most one per
process. Every run writes `dump.edn` (the harness export) and `export.edn` (what the checker
judged) beside its history under `store/`, and the `:parsley` result carries the Clojure
checker's violations, Parsley's Oracle verdict over the same file, and any expected refusal
that never came. The control node on an arm64 machine needs the control image's JDK URL
changed from `linux-x64` to `linux-aarch64`. The first run on fresh nodes downloads Kafka
from archive.apache.org, which is slow; later runs use the nodes' cache.
