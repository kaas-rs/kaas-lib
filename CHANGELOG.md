# Changelog

The six published crates release in lockstep at one version, so this file
covers all of them at once. Entries start at 0.9.0 — for anything earlier, the
git history is the record, and it is a better one than notes written after the
fact would be.

What belongs here: anything that changes what existing code *does*. A new API
is discoverable from the docs; a changed meaning is not, and that is what a
reader of this file is looking for.

## 0.9.0

The security audit's findings and the retry-mechanism review, in one release.
Three changes alter existing behaviour — read those first.

### Behavioural

- **`ProducerConfig::delivery_timeout` is now a real client-side deadline**
  (#21). It used to be relayed only as the Produce request's broker-side
  `timeout_ms`, so nothing bounded a record's total lifetime. The clock now
  starts at `send` and every wait counts against it — buffer permit, linger,
  leader election, retry backoff. Two consequences in opposite directions: a
  producer that previously gave up when the retry attempt budget expired now
  keeps trying until the deadline, and a producer parked behind a full buffer
  or a failing `InitProducerId` now fails its records instead of waiting for
  ever. The request's `timeout_ms` becomes the remaining budget, capped by the
  configured value. An expired attempt keeps that attempt's error, so an
  ambiguous send stays ambiguous.

- **Coordinator and leader re-asks now follow `RetryPolicy`** (#22). Four
  crates each had their own flat, jitterless retry constants; they now share
  one driver paced by the cluster's configured policy and budgeted by
  `coordinator_timeout`. In practice transactions and `ListOffsets` survive
  elections that the old budgets (about 6s and about 1s respectively) gave up
  on.

- **SCRAM refuses a broker-chosen iteration count outside 4096..=1_000_000**
  (#29). Below RFC 7677's floor is a downgrade attack; above the ceiling is a
  denial of service, since the peer prices our PBKDF2. A broker asking for
  either now fails authentication where it previously ran.

### Security

- SCRAM key derivation moved off the executor onto `spawn_blocking`, so the
  handshake future stays cancel-safe and a large work factor cannot pin a
  worker thread (#29).
- A compressed batch's claimed record count is bounded by what it actually
  decompressed to, closing a path where a small crafted batch drove a ~2 GB
  reservation before decoding (#30).
- The coordinator cache and the pool's broker-address map are bounded; both
  were insert-only and keyed by strings an attacker could choose (#31).
- Broker- and user-authored strings are escaped before they reach `tracing`,
  so ANSI sequences in an `error_message` or an advertised hostname cannot
  reach an operator's terminal (#32).
- PLAIN refuses NUL in either credential (RFC 4616 field injection), the
  OIDC connector is https-only unless plaintext was opted into, a cached
  bearer token no longer renders in `Debug`, IdP error text is bounded, and a
  SASLprep failure on a password no longer names the offending character
  (#34).

### Added

- `ConsumerConfig::retry` — retry pacing for the consumer's own re-ask loops,
  inheriting the cluster's policy when unset (#24).
- Admin calls re-ask the retriable slice of per-item responses instead of
  handing a transient error back as a final per-item result (#23). A code that
  is not retriable for a named resource but does call for a metadata refresh —
  `UNKNOWN_TOPIC_OR_PARTITION` on a partition created seconds ago — buys
  exactly one refresh and re-ask, because a refresh is new information. A name
  that genuinely does not exist costs one extra round trip and never spins.
- `NegotiatedConsumer` downgrades to the classic protocol when a broker
  advertises KIP-848 and then refuses the first heartbeat — the case no
  `ApiVersions` probe can see (#28).
- `cargo-deny` runs weekly and on dependency changes over all three
  lockfiles (#33).
- mTLS reaches the acceptance suite: `BrokerConfig::with_client_auth` plus a
  generated clients CA and client certificate (#27), and OAUTHBEARER gets its
  librdkafka cross-check (#26).

### Fixed

- IPv6 broker addresses connect and verify. `[::1]:9092` was mis-parsed for
  the TLS name check, and an IPv6 host was rendered unbracketed, so a listener
  advertising one was unreachable (#34).
- `rustls-pemfile` (unmaintained, RUSTSEC-2025-0134) replaced by the
  `rustls-pki-types` PEM API, which was already in the tree (#34).
