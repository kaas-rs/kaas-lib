---
name: live-cluster
description: Run kaas-lib against the real Kafka clusters in this Kubernetes cluster — Strimzi (namespace strimzi) as the main target, kaas (namespace kaas) as the experimental one. Use when asked to test, probe, smoke-test or validate the library against a real broker, to compare the two brokers, to reproduce a protocol bug outside a container fixture, or to clean up after a live run.
user-invocable: true
allowed-tools:
  - Bash
  - Read
  - Write
  - Edit
---

# live-cluster — test kaas-lib against a real broker

The container acceptance suite in `crates/*/tests/` owns everything that needs a
broker configured a particular way, killed mid-request, or fed a damaged log
segment. It cannot tell you what happens against a cluster that is **shared,
long-lived, and not yours**, running a Kafka build you did not choose, holding
data written by clients you did not write. That is what this is for.

It is not a nice-to-have. The first live run against Strimzi found six protocol
bugs that every unit test passed straight over — see *What live runs catch* at
the bottom, because the same shapes will recur.

## Preconditions

- `kubectl` on PATH with access to this cluster (already true on this pod).
- Pods here reach service DNS directly, so **no port-forward is needed**.
  `kafka-cluster-kafka-bootstrap.strimzi.svc.cluster.local:9092` is dialable
  from a `cargo run`.
- Build once: `cargo build -p livetest`.

## The two clusters

| target | namespace | what it is | use it for |
|---|---|---|---|
| `strimzi` | `strimzi` | Apache Kafka 4.2 via the Strimzi operator, 3 combined broker/controller nodes, real workloads (a 15M-record benchmark topic, a Kafka Streams app with a compacted changelog) | **the main target.** Correctness, the read path against real data, everything by default |
| `kaas` | `kaas` | the `kaas` broker, 3 replicas | **experimental only.** Conformance diffing and early feedback; never the source of truth for whether kaas-lib is correct |

When a run fails against `kaas` and passes against `strimzi`, the default
conclusion is that `kaas` is incomplete, not that kaas-lib is broken. Check the
version table before assuming otherwise — `kaas` advertises about half the api
keys Strimzi does.

Listeners: `strimzi` has `plain` (9092, no auth) and `tls` (9093, server-auth
TLS). `kaas` has `plain` (9092), `authed` (9095) and `tls` (9093).

## Running it

Resolve the target into the environment, then run a command:

```sh
eval "$(.claude/skills/live-cluster/resolve-target.sh strimzi)"
cargo run -q -p livetest -- probe
```

`resolve-target.sh <strimzi|kaas> [plain|tls|authed]` reads the Kafka CR status
or the Service and prints `export` lines. For Strimzi's TLS listener it also
extracts the cluster CA into a temp file and points `KAAS_TEST_CA_FILE` at it.

It never resolves credentials. A run that needs SASL sets
`KAAS_TEST_SASL_MECHANISM` / `_USERNAME` / `_PASSWORD` itself, so an
unauthenticated run cannot silently pick up someone's credentials from a secret
it happened to be able to read.

### The four commands

```sh
cargo run -q -p livetest -- probe          # read-only inventory, diffable
cargo run -q -p livetest -- smoke          # admin round trip; creates and cleans up
cargo run -q -p livetest -- read [opts]    # scan and tail real topics
cargo run -q -p livetest -- sweep          # delete anything a crashed run left
```

`read` takes `--topic <name>` (repeatable), `--expect <n>`, `--limit <n>`
(default 20000) and `--max-topics <n>` (default 5, used when no `--topic` is
given). Every command exits non-zero on failure and writes its diffable report
to stdout, notes to stderr — so `cargo run -q -p livetest -- probe > out.txt`
captures exactly the report.

### Conformance diff — the reason this exists

CLAUDE.md's argument for keeping this codec independent of the `kaas` broker's
is that the two then form a conformance harness. This is that harness:

```sh
eval "$(.claude/skills/live-cluster/resolve-target.sh strimzi)"
cargo run -q -p livetest -- probe > /tmp/strimzi.txt
eval "$(.claude/skills/live-cluster/resolve-target.sh kaas)"
cargo run -q -p livetest -- probe > /tmp/kaas.txt
diff -u /tmp/strimzi.txt /tmp/kaas.txt
```

The report is sorted `key = value` lines, so `diff` is the whole analysis tool.
Useful slices:

```sh
# api keys Strimzi offers and kaas does not
comm -23 <(grep -oE '^api\.[0-9]+\.[A-Za-z]+' /tmp/strimzi.txt | sort -u) \
         <(grep -oE '^api\.[0-9]+\.[A-Za-z]+' /tmp/kaas.txt | sort -u)

# everything except the version table
diff -u <(grep -vE '^api\.[0-9]' /tmp/strimzi.txt) <(grep -vE '^api\.[0-9]' /tmp/kaas.txt)
```

## Safety — read this before running `smoke` or `sweep`

These clusters are shared and hold other people's benchmarks and data.

- Everything created is prefixed `kaaslib-live-` (override with
  `KAAS_TEST_PREFIX`) and unique per run.
- `smoke` cleans up **including on its error path**. If the process is killed,
  run `sweep`.
- `sweep` deletes only names matching the prefix. That filter is a separate,
  unit-tested function (`crates/livetest/src/target.rs::owns`) because it is
  the most destructive decision in the crate. It refuses `orders`,
  `__consumer_offsets`, and even `kaaslib-liveness-probe`.
- `probe` and `read` mutate nothing. Set `KAAS_TEST_READ_ONLY=1` to have the
  connection layer refuse every mutating api key before it reaches the socket —
  worth doing anyway, since it exercises M8's gate against a real broker.

Never point `smoke` at a cluster whose topic list you have not looked at first.

## Interpreting a report

Facts worth reading before anything else:

| key | meaning |
|---|---|
| `api.broker_ahead_count` | api keys where the broker outruns our schemas. Expected non-zero against Strimzi (8) — `kafka-protocol` 0.17 ships Kafka 4.0 schemas. Zero against `kaas` is normal, it is an older protocol surface. |
| `api.unnameable_count` | keys this build cannot name at all. `2` on Strimzi: 88 and 89, `StreamsGroupHeartbeat` and `StreamsGroupDescribe`. Exactly the gap CLAUDE.md predicts. |
| `api.typed.*` | the version actually **sent**, which is narrower than `*.ours` wherever a request and response have different schema ranges. |
| `groups.described.unrecognized` | groups listed but not describable. Must never be an error — that is M7's whole point. |
| `groups.described.failed` | should be `0`. Anything else, read `groups.described.first_failure`. |
| `smoke.*.settle_ms` | how long a write took to become visible on an arbitrary broker. Real on a 3-broker cluster (~500ms for a config change) and structurally invisible to a single-broker fixture. |
| `read.*.malformed` | must be `0` on healthy topics. Non-zero is either real corruption or a decoder bug — either way, worth stopping for. |

## What live runs catch

A single-broker container fixture is a different machine from a shared
three-broker cluster, and these are the classes it cannot show you. All six were
found by the first live run.

1. **Version-shaped requests.** The codec *rejects* a field set outside its own
   version range rather than ignoring it, so "set both the old field and the new
   one to cover the range" is an encode failure, not a compatibility trick.
   `FindCoordinator` (`key` 0-3 vs `coordinator_keys` 4+), `DeleteTopics`
   (`topic_names` 0-5 vs `topics` 6+), `OffsetFetch` (`group_id`/`topics` 1-7 vs
   `groups` 8+). Ask `negotiated_for::<R>()` and build one shape.
2. **`ApiKey::valid_versions()` is not the range a request can be encoded at.**
   It is derived per api key and reports the wider range where request and
   response schemas differ — `OffsetFetch` response reaches v10, the request
   stops at v9. Negotiation must clamp to the types' own `VERSIONS`.
3. **`Fetch` v13+ identifies topics by uuid, not by name.** Sending only the
   name leaves the id nil and the broker answers `UNKNOWN_TOPIC_ID` for every
   partition — which reads like a missing topic. Note this is invisible on an
   *empty* topic, because the scan planner never fetches: only a topic with
   records shows it.
4. **Nullable fields default to `Some(empty)`, not `None`.** The same trap as
   `allow_auto_topic_creation: true`, in a second place:
   `CreatePartitionsTopic::assignments` defaults to an empty list, which the
   broker reads as "zero assignments for N new partitions" and rejects.
5. **Read-after-write is not immediate.** A topic created on the controller is
   not yet visible to the broker that answers the next describe. `smoke` polls
   with a bounded `settle` helper and reports the delay rather than sleeping.
6. **Real data exercises decoders that fixtures do not.** Strimzi's Kafka
   Streams topics include a compacted changelog and a repartition topic written
   by the Java client; `kperf-bench` is 15M records over 16 partitions.

When you fix something a live run found, add a unit test for it — the live run
is the discovery mechanism, not the regression net, because CI has no cluster.
Examples: `negotiation_clamps_to_the_request_type_not_the_api_key` in
`crates/kafka-conn/src/conn.rs`,
`growing_a_topic_lets_the_broker_place_the_new_replicas` in
`crates/kafka-admin/src/topics.rs`.

## Layout

```
.claude/skills/live-cluster/
  SKILL.md            this file
  resolve-target.sh   Kubernetes → environment. All cluster knowledge lives here.
crates/livetest/
  src/target.rs       environment → connection config; the ownership filter
  src/report.rs       the sorted, diffable report
  src/probe.rs        read-only inventory + version table
  src/smoke.rs        admin round trip, with settle handling and cleanup
  src/read.rs         scan and tail real topics
  src/sweep.rs        delete leftovers, prefix-filtered
```

`crates/livetest` knows nothing about Kubernetes on purpose: the same binary
works against a port-forward, a laptop broker, or a cluster in another account.
Adding a cluster means editing `resolve-target.sh`, not the Rust.

## Extending

- **A new assertion about real data** → `src/read.rs`.
- **A new admin operation** → `src/smoke.rs`, and make sure the resource it
  creates is prefixed and swept.
- **A new fact to diff between brokers** → `src/probe.rs`. Record absence as a
  key with `-` (use `set_opt`) rather than omitting the line, or a diff cannot
  tell "not supported" from "not probed".
- **A new cluster** → `resolve-target.sh`.

## Troubleshooting

| symptom | cause |
|---|---|
| `KAAS_TEST_BOOTSTRAP is not set` | the `eval "$(...resolve-target.sh ...)"` step was skipped |
| `A field is set that is not available on the selected protocol version` | class 1 above — our encoder, not the broker |
| `specified version not supported by this message type` | class 2 above |
| `UNKNOWN_TOPIC_ID` on a topic that exists | class 3 above |
| `UNKNOWN_TOPIC_OR_PARTITION` right after a create | class 5 above; use the `settle` helper |
| TLS name mismatch | connecting by IP; set `KAAS_TEST_TLS_SERVER_NAME` to the advertised name |
| a `kaaslib-live-*` topic lingering | a run was killed; `sweep` |
