# kafka-conn

**Kafka 4.x wire connection.** Part of [kaas-lib](https://kaas-rs.github.io/kaas-lib).

Framing, correlation, per-key version negotiation, TLS and SASL — everything
between a TCP socket and a typed Kafka request.

One socket, two tasks, one correlation map. Decoding happens on the *calling*
task, so a response that fails to parse fails one request instead of every
request sharing the connection. Dropping a `send` future is always safe: the
caller never touches the socket, so there is no half-read response to leave
behind.

Owns `ApiKey` and `ErrorCode` — the two protocol vocabularies every layer
above needs — each with an `Unknown` variant, because the codec ships Kafka
4.0 schemas and the brokers we target are newer.

## Documentation

- [The connection actor](https://kaas-rs.github.io/kaas-lib/architecture/connection.html)
- [Version negotiation](https://kaas-rs.github.io/kaas-lib/architecture/version-negotiation.html)
- [TLS, SASL and re-authentication](https://kaas-rs.github.io/kaas-lib/architecture/security.html)

Full book: <https://kaas-rs.github.io/kaas-lib/>

## Licence

Apache-2.0
