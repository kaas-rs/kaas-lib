# kafka-meta

The layer that knows what a cluster looks like. Everything above sends
through `Cluster`, which resolves the right broker, retries on the errors
that mean "your view is stale", and keeps an immutable snapshot readers can
take without blocking.

**Module map**

| File | Lines | What |
|---|---|---|
| `cluster.rs` | 556 | `Cluster`, `ClusterConfig`, send-with-routing-and-retry |
| `snapshot.rs` | 367 | `MetadataSnapshot`, `BrokerInfo`, `TopicInfo`, `PartitionInfo`, `TopicId` |
| `pool.rs` | 335 | `BrokerPool`, `Endpoint`, lazy connect, backoff, bootstrap re-resolution |
| `routing.rs` | 209 | the routing table |
| `retry.rs` | 130 | `RetryPolicy` — capped, jittered |

**The two tables.** `routing.rs` and the error taxonomy are first-class
artifacts, each in one file, because both encode knowledge that otherwise
scatters into individual call sites and then quietly diverges. The error
table lives one crate down in `kafka-conn` and is re-exported here, so the
two sit together at the layer that acts on them.

**`routing.rs` is 209 lines and worth reading in full.** Six classes, not the
four the summary suggests — `Routing::Specific` splits into `Caller` (a
broker the caller names, for `DescribeLogDirs`) and `PartitionLeader` (a
broker the snapshot names, for `Fetch`). Same routing class, completely
different resolution.

Its wildcard arm is `Routing::Any`, and that is safe in a way the read-only
gate's wildcard is not: mis-routing costs a redirect, mis-classifying a
mutating API costs the property.

**`snapshot.rs` and `ArcSwap`.** The snapshot is immutable and swapped
wholesale; reads never block and never wait on a refresh. It carries its own
fetch timestamp (`age()`) because a UI rendering "as of 4 seconds ago" is
honest and one rendering stale data as live is not.

`TopicId` wraps `uuid::Uuid` rather than exposing it, so the `Fetch` v13+
topic-id path does not leak a codec-adjacent type upward.

**The one-word regression with a destructive blast radius.** Every
`MetadataRequest` sets `allow_auto_topic_creation: false` explicitly. The
schema default is `true` and the crate honours it, so following "Default plus
builders" literally produces a UI that creates a topic every time someone
typos a name into a search box. A unit test asserts it.

**`pool.rs` and the Kubernetes case.** `Endpoint` is an enum — `Node(i32)`
for a broker known by id, `Bootstrap(String)` for the addresses we were
given — because when every known broker goes unreachable the pool must fall
back to bootstrap and re-resolve. A cluster rolling every broker onto new
addresses is a normal Kubernetes event, and a pool that only remembers
metadata addresses never recovers from one.

Connecting happens under a **per-endpoint** async mutex, not a global one, so
a slow handshake to a dead broker does not stall healthy ones and twenty
callers for the same broker open one socket.

**Where the boundary sits**: this crate builds and sends `kafka-protocol`
requests but returns owned types. `Cluster::send` is the seam every layer
above uses; nothing above it opens a socket or picks a broker.

**Start reading at** `routing.rs` — it is the shortest complete statement of
what this crate is for — then `cluster.rs`'s send path.

Related chapters:
[Metadata, routing and the pool](../architecture/metadata-routing.md),
[The error taxonomy](../architecture/errors.md).
