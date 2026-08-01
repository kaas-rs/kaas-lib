# livetest

A binary that points kaas-lib at a **real** Kafka cluster — shared,
long-lived, not ours, running a build we did not choose, holding data
produced by clients we did not write.

`publish = false`.

Not a replacement for the [container acceptance suite](testkit.md), which
owns the cases needing a broker configured a particular way, killed
mid-request, or fed a damaged log segment. This is the other half: the cases
that only appear when the cluster is somebody else's.

**Module map**

| File | Lines | What |
|---|---|---|
| `probe.rs` | 480 | read-only inventory and the negotiated version table |
| `read.rs` | 326 | scan and tail real topics, asserting the decoder |
| `target.rs` | 298 | resolve addresses, TLS and SASL from the environment |
| `smoke.rs` | 267 | admin round trip: create, describe, alter, verify, delete |
| `report.rs` | 182 | the sorted, diffable report format |
| `sweep.rs` | 107 | delete anything left behind |

## Four commands

```sh
livetest probe    # read-only inventory + version table. Touches nothing.
livetest smoke    # admin round trip, prefixed resources only
livetest read     # scan and tail, asserting the decoder against real data
livetest sweep    # delete anything this tool left behind
```

## `probe` is a conformance check in disguise

Its output is a **sorted, diffable report**. Run it against two clusters and
diff the results, and you have a typed parity check — which is exactly the
[conformance-harness](../compat/non-goals.md) idea made concrete. Point it at
`kaas` and at Apache Kafka and the diff is the answer.

Notes go to stderr and facts go to stdout, so `livetest probe > out.txt`
captures exactly the diffable body and nothing else.

## The report comes first, even on failure

```rust,no_run
# struct Outcome; 
fn emit(outcome: Outcome) {
    // notes to stderr, facts to stdout, then the pass/fail result
}
```

A partial report says how far the run got and what the cluster looked like on
the way, which is the entire diagnostic value. A failure that discards what
it learned before failing is a stack trace, and a stack trace from a
protocol mismatch against a cluster you cannot attach a debugger to is worth
very little.

`read` ranks topics by record count, so a run against a production-shaped
cluster reads the topics most likely to exercise the decoder rather than
whichever five sort first alphabetically.

## Everything is namespaced and swept

Every resource `livetest` creates carries a prefix (`kaaslib-live` by
default), and `sweep` **refuses to touch a name without it**. Running a tool
that creates topics against a shared cluster is only acceptable if cleanup is
mechanical and cannot over-reach.

`KAAS_TEST_READ_ONLY=1` turns on
[the read-only gate](../architecture/read-only-gate.md) and the target then
refuses any operation that would need to create something, with a clear error
naming the environment variable rather than a permission failure from the
broker.

## Configuration

| Variable | Meaning |
|---|---|
| `KAAS_TEST_BOOTSTRAP` | **required** — comma-separated `host:port` |
| `KAAS_TEST_LABEL` | report label, defaults to the first hostname |
| `KAAS_TEST_PREFIX` | prefix for created resources (default `kaaslib-live`) |
| `KAAS_TEST_READ_ONLY` | `1` to refuse every mutating api key |
| `KAAS_TEST_CA_PEM` / `KAAS_TEST_CA_FILE` | PEM bundle to trust |
| `KAAS_TEST_TLS_SERVER_NAME` | name to verify the broker certificate against |
| `KAAS_TEST_SASL_MECHANISM` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `KAAS_TEST_SASL_USERNAME` / `KAAS_TEST_SASL_PASSWORD` | credentials |

The `live-cluster` skill resolves all of these from Kubernetes — see
[Testing against a real cluster](../guide/live-cluster.md).

**Start reading at** `target.rs`, which is where a cluster stops being an
environment variable and becomes a configured `Cluster`.
