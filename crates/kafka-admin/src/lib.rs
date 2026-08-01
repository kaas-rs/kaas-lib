//! Admin RPCs for a Kafka cluster UI.
//!
//! Everything here obeys rule 4: a call that names several resources returns
//! [`PerItem`], one answer per resource. Describing five hundred topics where
//! three are mid-deletion returns 497 descriptions and three errors, because
//! the alternative makes a UI unusable on precisely the clusters that need one.
//!
//! ```no_run
//! # async fn example() -> kafka_admin::Result<()> {
//! use kafka_admin::{Admin, NewTopic, ConfigResource, ConfigChange};
//! use kafka_meta::ClusterConfig;
//!
//! let admin = Admin::connect(["localhost:9092"], ClusterConfig::default()).await?;
//!
//! for (name, result) in admin.create_topics([NewTopic::new("orders", 6, 3)]).await? {
//!     match result {
//!         Ok(created) => println!("{name}: {} partitions", created.partitions),
//!         Err(error) => println!("{name}: {error}"),
//!     }
//! }
//!
//! admin
//!     .alter_configs([(
//!         ConfigResource::topic("orders"),
//!         vec![ConfigChange::set("retention.ms", "604800000")],
//!     )])
//!     .await?;
//! # Ok(())
//! # }
//! ```

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

mod admin;
mod cluster_info;
mod configs;
mod groups;
mod offsets;
mod partitions;
mod security;
pub(crate) mod topics;
mod transactions;
pub mod types;

pub use admin::Admin;
pub use groups::{
    ClassicGroupMember, CommittedOffset, ConsumerGroupMember, GroupDescription, GroupListing,
    GroupState, OffsetReset, ShareGroupMember,
};
pub use offsets::IsolationLevel;
pub use partitions::{ElectionType, OngoingReassignment, PartitionReassignment};
pub use security::{
    AclBinding, AclFilter, AclOperation, AclPermission, AclResourceType, PatternType, QuotaEntity,
    QuotaFilter, QuotaOp, ScramCredentialInfo, ScramMechanism, ScramUpsert,
};
pub use transactions::{ProducerState, TransactionDescription, TransactionListing};
pub use types::{
    AlterOp, ClusterBroker, ClusterDescription, ConfigChange, ConfigEntry, ConfigResource,
    ConfigResourceType, ConfigSource, CreatedTopic, ListedOffset, LogDir, LogDirReplica, NewTopic,
    OffsetSpec, PerItem, TopicSize, errs, oks,
};

pub use kafka_conn::{ApiKey, Error, ErrorCode, Result};
pub use kafka_meta::{Cluster, ClusterConfig, TopicInfo};
