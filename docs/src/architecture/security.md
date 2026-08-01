# TLS, SASL and re-authentication

## TLS

`tokio-rustls` with the **`ring`** provider rather than the default
`aws_lc_rs`. That is a build-time decision, not a cryptographic one:
`aws-lc-sys` needs cmake and a C toolchain, `ring` builds with `cc` alone,
and CI on a minimal runner image has already lost time to exactly that.

`TlsConfig` covers system roots, a custom CA, client certificates for mTLS,
and an SNI override for the case where the address you dial is not the name
on the certificate — which is routine behind a Kubernetes service or a load
balancer.

## SASL mechanisms

`PLAIN`, `SCRAM-SHA-256` and `SCRAM-SHA-512`, negotiated via `SaslHandshake`
followed by `SaslAuthenticate`.

`PLAIN` sends a recoverable password over the wire and the code knows it —
`SaslMechanism::sends_cleartext_password` exists so the connection layer can
reason about the combination of mechanism and transport rather than leaving
it to the caller to notice.

## SASLprep is not `trim()`

> A password containing a non-ASCII space authenticates against a Java client
> and fails against ours if this is skipped.

SCRAM hashes the password, so both ends must normalise it *identically*
before hashing. RFC 4013 (SASLprep, a stringprep profile) maps non-ASCII
spaces — U+00A0, the U+2000 block — to U+0020, strips soft hyphens, and
applies a set of prohibited-character and bidirectional rules.

Java clients normalise. A hand-rolled `trim().to_lowercase()` does not, and
the failure mode is a password that works in every test you wrote and fails
for one user whose password manager inserted a non-breaking space. The error
says `authentication failed` and nothing about why.

This is why the `stringprep` crate is a dependency rather than a few lines of
inline character handling.

## The server signature is verified in constant time

SCRAM is mutual authentication: the server's final message carries a
signature that proves it knows the stored key. Verifying it with `==` leaks a
timing oracle on a value an attacker is actively trying to forge, so the
comparison goes through `subtle::ConstantTimeEq`.

Kafka does not do channel binding, so the gs2 header is always `n,,`.

## KIP-368 re-authentication

**This is the one people skip, and the symptom looks like a network fault.**

`SaslAuthenticate`'s response carries a session lifetime. On any cluster
where `connections.max.reauth.ms` is set — Confluent Cloud sets it — the
broker **kills the connection** when that expires unless the client re-issues
`SaslAuthenticate` on the live socket first.

A UI backend holds connections for hours. Without re-authentication you get
periodic unexplained disconnects, spread across brokers, that read as
flakiness in the network or the cluster and not as an auth problem at all.

The mechanism this needs is slightly awkward, and it is why the SASL exchange
is written against a `SaslTransport` trait rather than directly against a
socket. The same exchange has to run in two very different places:

1. On a **bare framed stream**, during connect, before the connection actor
   has started.
2. On a **live, fully multiplexed connection**, hours later, interleaved with
   other in-flight requests.

Both must behave identically. Abstracting the transport is what makes that
true by construction rather than by keeping two code paths in agreement.

```mermaid
sequenceDiagram
    participant C as Connection
    participant B as broker

    Note over C,B: connect — bare framed stream
    C->>B: ApiVersions
    B-->>C: version table
    C->>B: SaslHandshake(SCRAM-SHA-512)
    B-->>C: ok
    C->>B: SaslAuthenticate(client-first)
    B-->>C: server-first
    C->>B: SaslAuthenticate(client-final)
    B-->>C: server-final + session_lifetime_ms
    Note over C: actor starts; normal traffic flows

    Note over C,B: hours later — live multiplexed connection
    C->>B: SaslAuthenticate(client-first)
    B-->>C: server-first
    C->>B: SaslAuthenticate(client-final)
    B-->>C: server-final + new lifetime
    Note over C: connection survives; callers never notice
```

## The gate lets auth through

`SaslHandshake` and `SaslAuthenticate` are classified **non-mutating** by
[the read-only gate](read-only-gate.md), which looks wrong at first glance
since they plainly change state. They change *connection* state, not cluster
state, and gating them would leave a read-only client unable to authenticate
at all — which is to say, unable to read.

## Verification

The acceptance test boots brokers configured for `SASL_PLAINTEXT`/`PLAIN` and
`SASL_SSL`/`SCRAM-SHA-512`, asserts both authenticate, and asserts a wrong
password yields `Error::Authentication` rather than a timeout. A third case
runs a broker with `connections.max.reauth.ms` set to roughly 10 seconds and
asserts a connection survives past twice that window while still serving
requests — the only way to prove KIP-368 works is to let a session expire.
