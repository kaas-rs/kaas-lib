# kafka-consume

**Kafka 4.x consume path.** Part of [kaas-lib](https://kaas-rs.github.io/kaas-lib).

Incremental fetch sessions, a streaming fetcher, and three tiers of group
membership.

```rust,no_run
use kafka_consume::{ConsumerConfig, GroupConsumer};

let mut consumer =
    GroupConsumer::subscribe(cluster, ConsumerConfig::new(), "billing", ["orders"]).await?;

while running {
    // `poll` heartbeats, reconciles any new assignment, and then reads.
    for record in consumer.poll().await? {
        println!("{}-{} @{}", record.topic, record.partition, record.offset);
    }
}

consumer.leave().await?;
```

`poll` is also what keeps the membership alive, so a member that stops
polling is a member the coordinator evicts. An empty result is a normal
answer — a consumer at the log end is caught up, not broken.

`Consumer` reads an **explicitly assigned** set of partitions. Nothing
rebalances it, nothing heartbeats, and no broker knows it exists as a member
of anything — which is what you want to pin a reader to a partition. It can
still borrow a group's *offset storage*: set `ConsumerConfig::group_id` and
`commit` writes under it using the non-member sentinel. Borrowing the storage
is not joining the group.

```rust,no_run
use kafka_consume::{Consumer, ConsumerConfig, Position};

let mut consumer = Consumer::new(cluster, ConsumerConfig::new().group_id("reporting"));
consumer.assign([("orders".to_owned(), 0)], Position::Earliest).await?;

for record in consumer.poll().await? {
    // …
}
```

`GroupConsumer` joins a KIP-848 group, where the client generates its own
member id and the **broker** computes the assignment. `ClassicConsumer` speaks
the classic protocol, where the assignment is computed client-side by an
assignor — `range`, `round-robin` or `cooperative-sticky`, advertised in order
of preference. Note one hard protocol constraint there: every classic member
needs its own `Cluster`, because `JoinGroup` blocks on the coordinator and two
members sharing a connection deadlock.

Both take an `on_rebalance` listener whose `on_revoke` runs while the member
still owns the partitions and *before* the auto-commit, which is the only
moment a caller can flush its own per-partition state safely.

## Not `kafka-read`

`kafka-read::scan` is bounded and reports progress, because a UI is drawing a
page. A consumer runs until told to stop, and its interesting operations —
`seek`, `pause`, `resume` — are about changing its mind mid-stream, which a
bounded scan never does. They share the fetcher's shape and the tolerant
decoder, and neither is a special case of the other.

📖 **[Consuming records](https://kaas-rs.github.io/kaas-lib/guide/consuming.html)**
— the guide, including offsets, rebalance listeners and every config default.

Part of a workspace whose crates release in lockstep; pull it at the same
version as `kafka-conn`, `kafka-meta`, `kafka-admin`, `kafka-read` and
`kafka-produce`.

Full book: <https://kaas-rs.github.io/kaas-lib/>

## Licence

Apache-2.0
