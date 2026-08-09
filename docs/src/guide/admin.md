# Admin operations

Every call naming several resources returns `PerItem<Id, T>` —
`Vec<(Id, Result<T, Error>)>`. Handle the items; the outer `Result` is the
transport failing, not any individual resource.

```rust,no_run
use kafka_admin::{Admin, ClusterConfig};

# async fn example() -> kafka_admin::Result<()> {
let admin = Admin::connect(["localhost:9092"], ClusterConfig::default()).await?;
# Ok(())
# }
```

## Topics

```rust,no_run
use kafka_admin::{Admin, NewTopic};

# async fn example(admin: &Admin) -> kafka_admin::Result<()> {
// Create. NewTopic::new(name, partitions, replication_factor)
for (name, result) in admin.create_topics([NewTopic::new("orders", 6, 3)]).await? {
    match result {
        Ok(created) => println!("{name}: {} partitions", created.partitions),
        Err(error) => println!("{name}: {error}"),
    }
}

// List, describe, delete.
let names = admin.list_topics().await?;
let described = admin.describe_topics(["orders"]).await?;
let deleted = admin.delete_topics(["scratch"]).await?;

// Grow a topic. Partitions can only ever increase.
admin.create_partitions([("orders".to_owned(), 12)]).await?;
# Ok(())
# }
```

`describe_topics` prefers `DescribeTopicPartitions` and paginates, falling
back to `Metadata` when the broker or
[our codec](../compat/upstream-gap.md) cannot offer it. On a 10k-topic
cluster that difference is a multi-megabyte payload per refresh.

**`validate_topics` exists** — use it to check a creation would succeed
without performing it, which is what a UI's "check" button should call.

## Configs

```rust,no_run
use kafka_admin::{Admin, ConfigChange, ConfigResource};

# async fn example(admin: &Admin) -> kafka_admin::Result<()> {
let configs = admin.describe_configs([ConfigResource::topic("orders")]).await?;

admin
    .alter_configs([(
        ConfigResource::topic("orders"),
        vec![ConfigChange::set("retention.ms", "604800000")],
    )])
    .await?;
# Ok(())
# }
```

This is `IncrementalAlterConfigs` underneath, and that matters: the legacy
`AlterConfigs` **silently resets every config you did not mention**.
kaas-lib does not expose the legacy API at all, so this class of accident is
not reachable from here.

`describe_configs_documented` additionally returns the broker's own
documentation strings, which is what a config editor wants for tooltips.

## Offsets

```rust,no_run
use kafka_admin::{Admin, OffsetSpec};

# async fn example(admin: &Admin) -> kafka_admin::Result<()> {
let latest = admin
    .list_offsets([("orders".to_owned(), 0)], OffsetSpec::Latest)
    .await?;
let range = admin.topic_offset_range("orders").await?;

// Or a different spec per partition.
let mixed = admin
    .list_offsets_with([("orders".to_owned(), 0, OffsetSpec::Earliest)])
    .await?;
# Ok(())
# }
```

**Six sentinels, five reachable:**

| `OffsetSpec` | Wire | Meaning |
|---|---|---|
| `Latest` | `-1` | the high watermark |
| `Earliest` | `-2` | the first offset still retained |
| `MaxTimestamp` | `-3` | offset of the record with the largest timestamp — *not* `Latest` when producers write out of order |
| `EarliestLocalTimestamp` | `-4` | earliest offset on the broker's local disk; on a tiered topic, far ahead of `Earliest` |
| `LatestTieredTimestamp` | `-5` | the latest offset that has been tiered |
| — | `-6` | `EARLIEST_PENDING_UPLOAD_TIMESTAMP` — **unreachable**, needs `ListOffsets` v11 |

On a tiered cluster, `Earliest` and `EarliestLocalTimestamp` differ by
exactly the data that has been offloaded to remote storage — which is usually
most of it. Treating them as interchangeable is how a UI reports wrong
retention.

## Groups

```rust,no_run
use kafka_admin::{Admin, GroupDescription};

# async fn example(admin: &Admin) -> kafka_admin::Result<()> {
for listing in admin.list_groups().await? {
    // listing carries the group type — classic, consumer, share, or something else
}

for (id, result) in admin.describe_groups(["analytics"]).await? {
    match result {
        Ok(GroupDescription::Classic { members, .. }) => { /* generation-based */ }
        Ok(GroupDescription::Consumer { group_epoch, .. }) => { /* KIP-848 */ }
        Ok(GroupDescription::Share { .. }) => { /* KIP-932 */ }
        Ok(GroupDescription::Unrecognized { group_type, .. }) => {
            // A streams group, most likely. Render it; do not fail.
        }
        Err(error) => println!("{id}: {error}"),
    }
}

// None means "every partition the group has committed for".
let committed = admin.fetch_offsets("analytics", None).await?;
# Ok(())
# }
```

**Handle `Unrecognized`.** Streams groups list on any 4.1+ broker running
Kafka Streams and cannot be described by this build — see
[The four group kinds](../compat/group-kinds.md). A UI that treats it as an
error hard-fails on most real clusters.

Resetting a group's offsets works through `reset_offsets` / `delete_offsets`,
and it refuses when the group is not `EMPTY` rather than letting the broker
accept a commit that a live member will immediately overwrite. A silent no-op
in an admin tool is worse than a refusal.

## Security

```rust,no_run
use kafka_admin::{Admin, AclFilter, QuotaFilter};

# async fn example(admin: &Admin) -> kafka_admin::Result<()> {
// Both filters default to matching everything.
let acls = admin.describe_acls(&AclFilter::default()).await?;
let quotas = admin.describe_client_quotas(&QuotaFilter::default()).await?;
let scram = admin.describe_scram_credentials(["alice"]).await?;
# Ok(())
# }
```

ACLs, client quotas and SCRAM credentials, describe and alter. `create_acls`
and `delete_acls` are per-item like everything else.

### Delegation tokens

```rust,no_run
use kafka_admin::{Admin, NewDelegationToken, Principal};
use kafka_conn::{SaslConfig, ScramHash};

# async fn example(admin: &Admin) -> kafka_admin::Result<()> {
let token = admin
    .create_delegation_token(&NewDelegationToken::new().with_renewer(Principal::user("worker")))
    .await?;

// The token is a SCRAM credential: the id is the username, the base64 HMAC
// is the password, and `tokenauth=true` is what sends the broker to its token
// cache instead of the user store.
let sasl = SaslConfig::delegation_token(ScramHash::Sha256, &token.token_id, token.password());
# Ok(())
# }
```

A [KIP-48](../compat/kip-index.md) token lets something else authenticate *as*
you without holding your password — a fleet of workers, a Connect cluster, a
batch job fanned out over a hundred containers. Three broker rules decide
whether any of this works, and each arrives as a bare error code:

- The cluster needs a `delegation.token.secret.key`, identical on every
  broker. Without one, every call is `DELEGATION_TOKEN_AUTH_DISABLED`.
- Tokens cannot be created over an unauthenticated channel, or by a principal
  that authenticated *with* a token — that is
  `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`, and it is what stops a leaked token
  renewing itself into a permanent identity.
- Only the owner and the renewers named at creation may renew or expire one,
  and renewal is capped by the token's own `max_timestamp_ms`.

`hmac` is a live credential. `Debug` redacts it here; anything else that logs
a token should do the same.

## Cluster and storage

```rust,no_run
# use kafka_admin::Admin;
# async fn example(admin: &Admin) -> kafka_admin::Result<()> {
let cluster = admin.describe_cluster().await?;
let dirs = admin.describe_all_log_dirs().await?;
let sizes = admin.topic_sizes().await?;
let one = admin.topic_size("orders").await?;
# Ok(())
# }
```

`topic_sizes` joins `DescribeLogDirs` against `Metadata` for per-topic size.
**It does not double-count replicas** — an RF=3 topic reports its single-replica
size, not three times it, and there is an acceptance test asserting exactly
that because getting it wrong produces a plausible-looking number.

`topic_size` is the same join for one topic: metadata for that topic rather
than the cluster, and a `DescribeLogDirs` naming its partitions rather than
asking each broker for everything it holds. The fan-out itself stays — a log
directory belongs to a broker — but nothing else about the call is
cluster-sized. It errors on a topic that does not exist rather than reporting
it as empty.

Both return the same `TopicSize`, and it carries the detail the fan-out
already collected rather than only the totals:

```rust,no_run
# use kafka_admin::Admin;
# async fn example(admin: &Admin) -> kafka_admin::Result<()> {
let size = admin.topic_size("orders").await?;

// Which partition is the big one *on disk*, rather than by record count.
let biggest = size
    .partitions
    .iter()
    .max_by_key(|partition| partition.replicated_bytes);

// Which broker holds the big copy of it, and how far behind that copy is.
for replica in &size.replicas {
    println!(
        "p{} on {} in {}: {} bytes, lag {}{}{}",
        replica.partition,
        replica.node_id,
        replica.log_dir,
        replica.size_bytes,
        replica.offset_lag,
        if replica.is_leader { " (leader)" } else { "" },
        if replica.is_future { " (moving)" } else { "" },
    );
}
# let _ = biggest;
# Ok(())
# }
```

Two things worth knowing about `replicas`. Its length is the log-dir **entry**
count — `DescribeLogDirs` does not report segment files at all, so a "segment
count" taken from it is a replica count under another name. And future
replicas appear in it, flagged, so a directory move in flight is visible;
they stay out of every total, because counting them makes a topic appear to
grow during a reassignment and shrink again afterwards.

`TopicSize`, `PartitionSize` and `ReplicaSize` are `#[non_exhaustive]`: this
call collects more than any one caller wants, and the field set has grown once
already.

## Partitions and transactions

```rust,no_run
# use kafka_admin::Admin;
# async fn example(admin: &Admin) -> kafka_admin::Result<()> {
let ongoing = admin.list_partition_reassignments().await?;
let in_progress = admin.reassignments_in_progress().await?;

let txns = admin.list_transactions().await?;
let producers = admin
    .describe_producers([("orders".to_owned(), 0)])
    .await?;
# Ok(())
# }
```

Transactions and producers are **describe-only** — this library observes
transaction state without starting one. See
[Non-goals](../compat/non-goals.md).

## Errors worth matching on

```rust,no_run
# use kafka_conn::Error;
# fn example(error: Error) {
match error {
    Error::ReadOnly { api_key } => { /* this client refuses mutations */ }
    Error::UnsupportedApi { api_key, broker, ours } => { /* which side is the ceiling? */ }
    Error::Authorization { code, detail } => { /* ask your admin */ }
    Error::Decode { .. } => { /* this is our bug — report it */ }
    _ => {}
}
# }
```

See [The error taxonomy](../architecture/errors.md).
