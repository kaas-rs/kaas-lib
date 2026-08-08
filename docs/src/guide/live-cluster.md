# Testing against a real cluster

The [container acceptance suite](../compat/verification.md) owns everything
needing a broker configured a particular way, killed mid-request, or fed a
damaged log segment. It cannot tell you what happens against a cluster that
is **shared, long-lived and not yours**, running a Kafka build you did not
choose, holding data written by clients you did not write.

That is what [`livetest`](../code-tour/livetest.md) is for, and it is not a
nice-to-have: the first live run against Strimzi found six protocol bugs that
every unit test passed straight over.

## The commands

```sh
cargo build -p livetest

livetest probe    # read-only inventory + negotiated version table. Touches nothing.
livetest smoke    # admin round trip: create, describe, alter, verify, delete
livetest read     # scan and tail real topics, asserting the decoder
livetest sweep    # delete anything this tool left behind
```

`read` takes options:

```sh
livetest read --topic orders --expect 15000
livetest read --max-topics 5 --limit 20000
```

## Configuration

Everything comes from the environment:

| Variable | Meaning |
|---|---|
| `KAAS_TEST_BOOTSTRAP` | **required** — comma-separated `host:port` |
| `KAAS_TEST_LABEL` | report label, defaults to the first hostname |
| `KAAS_TEST_PREFIX` | prefix for created resources (default `kaaslib-live`) |
| `KAAS_TEST_READ_ONLY` | `1` to refuse every mutating api key |
| `KAAS_TEST_CA_PEM` / `KAAS_TEST_CA_FILE` | PEM bundle to trust |
| `KAAS_TEST_TLS_SERVER_NAME` | name to verify the broker certificate against |
| `KAAS_TEST_SASL_MECHANISM` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER` |
| `KAAS_TEST_SASL_USERNAME` / `KAAS_TEST_SASL_PASSWORD` | credentials |
| `KAAS_TEST_OAUTH_TOKEN` | `OAUTHBEARER`: a token you already have |
| `KAAS_TEST_OAUTH_TOKEN_ENDPOINT` | `OAUTHBEARER`: fetch one instead, via `client_credentials` |
| `KAAS_TEST_OAUTH_CLIENT_ID` / `KAAS_TEST_OAUTH_CLIENT_SECRET` | credentials for that fetch |
| `KAAS_TEST_OAUTH_SCOPE` / `KAAS_TEST_OAUTH_AUDIENCE` | whichever your issuer wants |

## The `live-cluster` skill

This repository ships a `live-cluster` skill that resolves all of the above
from Kubernetes, so you do not assemble them by hand:

```sh
eval "$(.claude/skills/live-cluster/resolve-target.sh strimzi)"
cargo run -q -p livetest -- probe
```

`resolve-target.sh <strimzi|kaas> [plain|tls|authed]` reads the Kafka CR
status or the Service and prints `export` lines. For a TLS listener it also
extracts the cluster CA into a temp file and points `KAAS_TEST_CA_FILE` at
it. It never resolves credentials.

### The two targets

| Target | What it is | Use it for |
|---|---|---|
| `strimzi` | Apache Kafka via the Strimzi operator, 3 combined broker/controller nodes, real workloads | **the main target** — correctness, the read path against real data |
| `kaas` | the `kaas` broker, 3 replicas | **experimental only** — conformance diffing and early feedback |

**When a run fails against `kaas` and passes against `strimzi`, the default
conclusion is that `kaas` is incomplete, not that kaas-lib is broken.** Check
the version table first: `kaas` advertises roughly half the api keys Strimzi
does. See [Non-goals](../compat/non-goals.md) for why the two projects share
no code — this diff is exactly the check that separation exists to enable.

## `probe` is a conformance diff

Its output is sorted and diffable, with notes on stderr and facts on stdout:

```sh
eval "$(.claude/skills/live-cluster/resolve-target.sh strimzi)"; livetest probe > strimzi.txt
eval "$(.claude/skills/live-cluster/resolve-target.sh kaas)";    livetest probe > kaas.txt
diff strimzi.txt kaas.txt
```

That diff is the parity check. `livetest probe > out.txt` captures exactly
the body and nothing else, which is why the stream split exists.

## Safety on a shared cluster

**Everything created is prefixed**, and `sweep` refuses to touch a name
without the prefix. Running a tool that creates topics against a cluster
other people depend on is only acceptable if cleanup is mechanical and cannot
over-reach.

For a cluster you must not modify at all:

```sh
export KAAS_TEST_READ_ONLY=1
livetest probe
```

That turns on [the read-only gate](../architecture/read-only-gate.md) —
mutating api keys are refused before a socket is opened — and the target
additionally refuses any command that would need to create something, with an
error naming the environment variable rather than a permission failure from
the broker.

Always run `sweep` after a `smoke` run, including a failed one.

## Reading a failed run

The report is printed **before** the pass/fail result, even when the run
failed. A partial report says how far it got and what the cluster looked like
on the way, which is the entire diagnostic value — a protocol mismatch
against a cluster you cannot attach a debugger to is not something a stack
trace will explain.

`read` ranks topics by record count, so a run against a production-shaped
cluster exercises the topics most likely to stress the decoder rather than
whichever five sort first alphabetically. A compression bug, a header
encoding bug or a topic-id bug shows up in the topic with fifteen million
records, not in the empty one.
