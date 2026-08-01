# kafka-admin

The admin RPCs — 31 of the [37 api keys](../compat/api-matrix.md) this
library sends. Pure translation: build a request from owned types, send it
through `Cluster`, convert the response back.

**Module map**

| File | Lines | What |
|---|---|---|
| `groups.rs` | 1,100 | the four group kinds, offsets, offset reset |
| `security.rs` | 956 | ACLs, quotas, SCRAM credentials |
| `topics.rs` | 575 | create, delete, describe, `DeleteRecords` |
| `types.rs` | 524 | the owned vocabulary — `PerItem`, configs, offsets, log dirs |
| `transactions.rs` | 306 | list/describe transactions, describe producers |
| `partitions.rs` | 296 | reassignments, leader election |
| `configs.rs` | 252 | describe and incrementally alter |
| `offsets.rs` | 235 | `ListOffsets` and the six sentinels |
| `cluster_info.rs` | 235 | `DescribeCluster`, log dirs, topic sizes |
| `admin.rs` | 85 | the `Admin` handle |

## `PerItem` is the whole API shape

Every call naming several resources returns `PerItem<Id, T>` —
`Vec<(Id, Result<T, Error>)>`, never `Result<Vec<T>, Error>`.

```rust,no_run
# use kafka_admin::Admin;
# async fn example(admin: &Admin) -> kafka_admin::Result<()> {
for (name, result) in admin.describe_topics(["orders", "shipments"]).await? {
    match result {
        Ok(topic) => println!("{name}: {} partitions", topic.partitions.len()),
        Err(error) => println!("{name}: {error}"),
    }
}
# Ok(())
# }
```

Describing 500 topics while 3 are mid-deletion returns 497 descriptions and
3 errors. The alternative makes a UI unusable on exactly the clusters that
need one. `errs` and `oks` in `types.rs` are the helpers for splitting a
`PerItem` when a caller genuinely wants one side.

Note that the outer `Result` still exists and still means something: it is
the *transport* failing, not any individual item.

## `groups.rs` is the biggest file for a reason

Four group kinds, three describable, all with different response shapes. See
[The four group kinds](../compat/group-kinds.md) — this file is where that
chapter lives in code, including the `Unrecognized` variant that keeps a
streams group from taking down a group list.

It also holds the offset-reset path, where the classic protocol wants
`generation_id = -1` and KIP-848 wants `member_epoch = -1`, and where a reset
against a non-`EMPTY` group is refused with a clear error rather than
accepted as a commit a live member will immediately overwrite.

## `topics.rs` and the pagination fallback

`describe_topics` prefers `DescribeTopicPartitions` — how the 4.x Java
AdminClient describes topics, and it paginates. Unfiltered `Metadata` returns
the whole cluster in one response, which on a 10k-topic cluster is a
multi-megabyte payload on every refresh.

The fallback to `Metadata` has to handle two different causes arriving at the
same place: the broker is too old to offer the API, or
[our schemas are too old](../compat/upstream-gap.md) to encode it. Hence
`Admin::supports` and the `Error::UnsupportedApi` match.

## `offsets.rs` and the six sentinels

Surfaced distinctly, not collapsed. Five are reachable; `-6`
(`EARLIEST_PENDING_UPLOAD_TIMESTAMP`, KIP-1023) needs `ListOffsets` v11 and
the codec caps at v10 — the domain enum documents the gap rather than
omitting it silently.

## The private version helpers

`admin.rs` carries four `pub(crate)` helpers worth knowing about before
adding a method:

- `supports(ApiKey)` — does this cluster offer the key at all
- `negotiated_version(ApiKey)` — for reporting
- `negotiated_for::<R>()` — **for encoding**, because a request's shape can
  change with its version and the codec rejects a field set outside its own
  range rather than ignoring it. "Set both the old and the new field" is an
  encode failure, not a compatibility trick.
- `request_timeout_ms()` — the pool's timeout in the milliseconds admin RPCs
  want

## Where the boundary sits

No sockets, no broker selection, no retry — all of that is
[`kafka-meta`](kafka-meta.md)'s. This crate is request construction and
response translation, and the volume of it is the price of
[rule 1](../architecture/domain-boundary.md).

**Start reading at** `types.rs` for the vocabulary, then `topics.rs` for the
simplest complete round trip, then `groups.rs` when you need the hard case.
