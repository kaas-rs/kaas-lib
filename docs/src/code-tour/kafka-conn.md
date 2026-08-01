# kafka-conn

Framing, correlation, version negotiation, TLS, SASL and the connection
actor — everything between a TCP socket and a typed Kafka request.

**Module map**

| File | Lines | What |
|---|---|---|
| `error_code.rs` | 1,318 | the broker error-code table, derived from `ResponseError` |
| `conn.rs` | 1,004 | the connection actor |
| `api_key.rs` | 647 | `ApiKey`, header versions, `is_mutating` |
| `rpc.rs` | 464 | the `Rpc` trait — request/response pairing and version ranges |
| `scram.rs` | 457 | SCRAM-SHA-256/512, RFC 5802 |
| `sasl.rs` | 294 | mechanisms, the exchange, KIP-368 re-auth |
| `versions.rs` | 253 | `ApiVersions`, `VersionRange`, `our_range` |
| `tls.rs` | 239 | rustls config: roots, client certs, SNI |
| `codec.rs` | 239 | length-delimited framing, header versions |
| `error.rs` | 234 | the `Error` enum |
| `stats.rs` | 137 | per-connection byte and request counters |
| `config.rs` | 132 | `ConnectionConfig` |
| `transport.rs` | 104 | plaintext or TLS, behind one type |

**What this crate owns that everything else borrows**: `ApiKey` and
`ErrorCode`, the two protocol vocabularies that would otherwise leak
everywhere. Both carry an `Unknown` variant, because the codec ships Kafka
4.0 schemas and the brokers we target are newer. They live here rather than
in `kafka-meta` because every crate — including this one — has to classify a
broker's answer, and a workspace with two error types pushes a `From`
conversion into every call site.

**The one deliberate exception to rule 1**: `Connection::send` is generic
over `kafka_protocol::protocol::Request`. This crate *is* the wire boundary,
and a parallel request trait here would convert protocol types into protocol
types for no gain. Everything above is held to the rule without exception.
See [The domain boundary](../architecture/domain-boundary.md).

**The re-export**: `kafka_conn::protocol` carries the codec's `Decodable`,
`Encodable`, `Message`, `Request`, `StrBytes`, `HeaderVersion`, plus
`compression`, `indexmap`, `messages` and `records`. Crates above reach the
codec through here so the version is pinned in one manifest. Re-exporting is
not licence to expose it in a signature.

**The subtle files**:

- `codec.rs` — two header traps, both producing off-by-a-few-bytes failures.
  The response header version is not the request's api version, and
  `ApiVersions` responses always use response header v0 even on a flexible
  connection. Both go through helpers rather than being computed.
- `versions.rs` — `negotiate` vs `negotiate_with`. The first reads
  `ApiKey::valid_versions()`, which is right for a report and wrong for
  encoding, because a request and its response can have different ranges.
  `OffsetFetch` is the live example.
- `api_key.rs` — `is_mutating` is an allowlist of read-only keys with
  `_ => true`. The direction is the whole security property.
- `scram.rs` — real SASLprep via `stringprep`, and a constant-time server
  signature check.

**Where the boundary sits**: this crate knows nothing about clusters. Give it
an address and it gives you request/response against that one broker.
Leadership, coordinators, retries and pooling are all
[`kafka-meta`](kafka-meta.md)'s problem.

**Start reading at** `conn.rs`'s module docs, then `versions.rs` end to end —
it is short, and it explains why the rest of the workspace is shaped the way
it is.

Related chapters: [The connection actor](../architecture/connection.md),
[Version negotiation](../architecture/version-negotiation.md),
[TLS, SASL and re-authentication](../architecture/security.md),
[The error taxonomy](../architecture/errors.md),
[The read-only gate](../architecture/read-only-gate.md).
