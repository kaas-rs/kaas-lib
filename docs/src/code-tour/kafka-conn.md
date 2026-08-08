# kafka-conn

Framing, correlation, version negotiation, TLS, SASL and the connection
actor — everything between a TCP socket and a typed Kafka request.

**Module map**

| File | Lines | What |
|---|---|---|
| `error_code.rs` | 1,340 | the broker error-code table, derived from `ResponseError` |
| `conn.rs` | 1,131 | the connection actor |
| `api_key.rs` | 733 | `ApiKey`, header versions, `is_mutating` |
| `oidc.rs` | 703 | KIP-768 `client_credentials` token fetch — `oidc` feature only |
| `sasl.rs` | 576 | mechanisms, the exchange, KIP-368 re-auth |
| `scram.rs` | 501 | SCRAM-SHA-256/512, RFC 5802 |
| `rpc.rs` | 488 | the `Rpc` trait — request/response pairing and version ranges |
| `oauth.rs` | 395 | SASL/OAUTHBEARER, RFC 7628, and the `TokenProvider` trait |
| `error.rs` | 324 | the `Error` enum |
| `versions.rs` | 253 | `ApiVersions`, `VersionRange`, `our_range` |
| `tls.rs` | 248 | rustls config: roots, client certs, SNI |
| `codec.rs` | 239 | length-delimited framing, header versions |
| `config.rs` | 139 | `ConnectionConfig` |
| `stats.rs` | 137 | per-connection byte and request counters |
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
- `oauth.rs` — the `%x01` separators are the message format, and a *rejected*
  token needs a second round trip the happy path does not. Both are asserted
  on exact bytes, because KAFKA-7182 is what a client and a broker agreeing
  with each other and with nobody else looks like.
- `oidc.rs` — the only file behind a cargo feature. It is where the HTTP client
  lives, which is the whole reason the feature exists.

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
