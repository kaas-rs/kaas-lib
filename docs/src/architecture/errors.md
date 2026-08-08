# The error taxonomy

Two types, in two files, answering two different questions.

| Type | Question | Lives in |
|---|---|---|
| `ErrorCode` | *what did the broker say?* | `crates/kafka-conn/src/error_code.rs` |
| `Error` | *what happened?* | `crates/kafka-conn/src/error.rs` |

Both are defined in `kafka-conn` and re-exported upward. That placement is
forced: every crate in the workspace, including the connection layer itself,
has to classify a broker's answer, and a workspace with two error types would
push a `From` conversion into every call site in it.

## `Error` — distinguishable at the type level

A UI renders these differently, and collapsing them into one string throws
that away. A transport error means "the cluster is unreachable"; an
authorization error means "ask your admin"; a decode failure means "this is
our bug".

| Variant | Means |
|---|---|
| `Transport` | the socket failed or never opened |
| `ConnectionClosed` | the connection died; every in-flight request resolves to this rather than hanging |
| `Timeout` | the caller's deadline passed |
| `Authentication` | credentials rejected, or the handshake could not agree |
| `Authorization` | authenticated, but not permitted |
| `Broker` | the broker answered with an error code |
| `Decode` | a response did not parse — **this one means we are wrong** |
| `ReadOnly` | a read-only client refused a mutating key before touching the network |
| `UnsupportedApi` | no version of this API is speakable by both ends |
| `Unsupported` | the caller asked for something this build cannot express |
| `InvalidRequest` | malformed before it went out |
| `TokenEndpoint` | an OAuth token endpoint was unreachable or refused to issue a token |

`Decode` is worth calling out separately. Every other variant describes
something about the cluster or the caller; `Decode` describes a bug in this
library or a schema that has drifted, and it should be reported rather than
retried.

`TokenEndpoint` is the same argument applied to a *third* system. "Your
identity provider rejected our client secret" and "the broker rejected the
token it issued" are different problems with different owners, so the first is
not an `Authentication` failure — nothing has been said to a broker yet. It
carries the endpoint, the issuer's own `error_description`, and the HTTP status
when there was a response at all: `None` means unreachable, which is retriable,
where a `401` is not.

## `ErrorCode` — derived, not transcribed

The table is *derived* from `kafka_protocol::ResponseError`, and that is the
load-bearing part rather than an implementation detail.

- **`retriable()` delegates to the crate's own `is_retriable()`**, which
  encodes what the protocol says rather than what we remember it saying.
- **`from_response_error` matches `ResponseError` exhaustively.**
  `ResponseError` is a plain enum, so when an upstream bump adds a code, that
  match stops compiling. A new error code becomes a build failure to triage
  rather than a silent hole in the classification.

Hand-transcribing a 100+ entry table would be correct exactly once.

## Three independent axes

`kafka-protocol` models one of these. The other two are ours, and they are
exhaustive matches over our own enum for the same compile-failure reason.

| Axis | Owner | Question |
|---|---|---|
| `retriable()` | upstream | will trying again plausibly help? |
| `needs_metadata_refresh()` | ours | is the caller's view of leadership stale? |
| `needs_coordinator_refresh()` | ours | is the cached group/txn coordinator wrong? |

They are genuinely independent — a code can be retriable without needing any
refresh (`REQUEST_TIMED_OUT`), need a metadata refresh
(`NOT_LEADER_OR_FOLLOWER`), or need a coordinator refresh (`NOT_COORDINATOR`).
Modelling them as one enum would force a false choice on the codes that need
two. [Metadata, routing and the pool](metadata-routing.md) is what acts on
them.

## `Unknown(i16)` is not optional

`kafka-protocol` 0.17 knows error codes through Kafka **4.1**. The acceptance
suite runs against **4.3.1**. A broker can and will return a code with no
name here, and that is the expected case rather than a corruption signal.

`ErrorCode::Unknown(i16)` round-trips and renders. It never panics, and it
never collapses into a generic failure that discards the number — the number
is the only thing anyone can search for.

The unit test is table-driven over every code plus one that no Kafka release
defines (30000), asserting it lands in `Unknown(30000)` and still renders.

## Where classification happens

At the point the response is decoded, not at the call site. A response
carrying an error code becomes `Error::Broker { code, message }` — or
`Error::Authorization` where the code is an authorization failure, since that
distinction is what a UI needs and recovering it later means matching on the
code again.

Per-item results keep their own errors: `describe_topics` over 500 names
returns 500 entries, each independently `Ok` or `Err`, and one
`UNKNOWN_TOPIC_OR_PARTITION` does not become the result of the call. See
[the third invariant](../introduction.md).
