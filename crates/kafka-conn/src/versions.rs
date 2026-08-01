//! Per-connection API version negotiation.
//!
//! Never hardcode a version. The broker advertises `(min, max)` per api key in
//! its `ApiVersions` response; we intersect that with what this build's codec
//! can encode and take the highest version in the overlap.
//!
//! Which side binds is not symmetric here. `kafka-protocol` 0.17 ships Kafka
//! 4.0 schemas and the acceptance suite runs against a 4.3.1 broker, so *our*
//! max is the ceiling more often than the broker's — which is also why the
//! table keeps api keys it cannot name at all.

use std::collections::BTreeMap;

use crate::api_key::ApiKey;
use crate::error::{Error, Result};

/// What one end supports for one api key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VersionRange {
    /// Lowest supported version, inclusive.
    pub min: i16,
    /// Highest supported version, inclusive.
    pub max: i16,
}

impl VersionRange {
    /// Build a range.
    pub const fn new(min: i16, max: i16) -> Self {
        Self { min, max }
    }

    /// Whether the range contains no versions.
    pub const fn is_empty(&self) -> bool {
        self.min > self.max
    }

    /// The overlap with another range, possibly empty.
    pub const fn intersect(&self, other: &VersionRange) -> VersionRange {
        VersionRange {
            min: if self.min > other.min {
                self.min
            } else {
                other.min
            },
            max: if self.max < other.max {
                self.max
            } else {
                other.max
            },
        }
    }
}

impl From<VersionRange> for (i16, i16) {
    fn from(r: VersionRange) -> Self {
        (r.min, r.max)
    }
}

/// One row of a broker's version table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerApiVersion {
    /// The api key, possibly [`ApiKey::Unknown`].
    pub api_key: ApiKey,
    /// What the broker supports.
    pub broker: VersionRange,
    /// What this build can encode, or `None` for a key with no schema here.
    pub ours: Option<VersionRange>,
}

impl BrokerApiVersion {
    /// The version we would use, or `None` when there is no overlap.
    pub fn negotiated(&self) -> Option<i16> {
        let ours = self.ours?;
        let overlap = self.broker.intersect(&ours);
        if overlap.is_empty() {
            None
        } else {
            Some(overlap.max)
        }
    }

    /// Whether the broker offers a version newer than anything we can encode.
    ///
    /// The normal case against a broker newer than the crate's schemas, and
    /// the thing M1's acceptance test asserts on to prove the clamp is ours.
    pub fn broker_ahead(&self) -> bool {
        self.ours.is_some_and(|ours| self.broker.max > ours.max)
    }
}

/// A broker's advertised API versions, intersected with our own.
#[derive(Debug, Clone, Default)]
pub struct ApiVersions {
    /// Keyed by wire code so unnamed keys survive.
    entries: BTreeMap<i16, BrokerApiVersion>,
}

impl ApiVersions {
    /// Build a table from `(api_key_code, min, max)` triples.
    pub fn from_triples(triples: impl IntoIterator<Item = (i16, i16, i16)>) -> Self {
        let entries = triples
            .into_iter()
            .map(|(code, min, max)| {
                let api_key = ApiKey::from_code(code);
                (
                    code,
                    BrokerApiVersion {
                        api_key,
                        broker: VersionRange::new(min, max),
                        ours: our_range(api_key),
                    },
                )
            })
            .collect();
        Self { entries }
    }

    /// Every row, in wire-code order.
    pub fn entries(&self) -> impl Iterator<Item = &BrokerApiVersion> {
        self.entries.values()
    }

    /// How many api keys the broker advertised.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the table is empty — a broker that answered nothing useful.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The row for one key.
    pub fn get(&self, api_key: ApiKey) -> Option<&BrokerApiVersion> {
        self.entries.get(&api_key.code())
    }

    /// Whether the broker advertised this key at all.
    pub fn supports(&self, api_key: ApiKey) -> bool {
        self.get(api_key).is_some_and(|e| e.negotiated().is_some())
    }

    /// The version to send, or a typed error naming both ranges.
    pub fn negotiate(&self, api_key: ApiKey) -> Result<i16> {
        let entry = self.entries.get(&api_key.code());
        match entry.and_then(BrokerApiVersion::negotiated) {
            Some(version) => Ok(version),
            None => Err(Error::UnsupportedApi {
                api_key,
                broker: entry.map(|e| e.broker.into()),
                ours: our_range(api_key).map(Into::into),
            }),
        }
    }
}

/// What this build's codec can encode for an api key.
///
/// `None` means `kafka-protocol` has no schema for the key — `StreamsGroupDescribe`
/// on a 4.1+ broker, for instance. That is a real gap, and the point of
/// returning `None` rather than a guess is that it stays visible.
pub fn our_range(api_key: ApiKey) -> Option<VersionRange> {
    let upstream = kafka_protocol::messages::ApiKey::try_from(api_key.code()).ok()?;
    let range = upstream.valid_versions();
    Some(VersionRange::new(range.min, range.max))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn we_clamp_to_our_own_ceiling() {
        // A broker offering Metadata 0..99 must not make us send 99.
        let ours = our_range(ApiKey::Metadata).expect("metadata has a schema");
        let table = ApiVersions::from_triples([(ApiKey::Metadata.code(), 0, 99)]);
        assert_eq!(table.negotiate(ApiKey::Metadata).ok(), Some(ours.max));
        let row = table.get(ApiKey::Metadata).expect("row");
        assert!(row.broker_ahead());
    }

    #[test]
    fn we_clamp_to_the_broker_ceiling_when_it_is_lower() {
        let table = ApiVersions::from_triples([(ApiKey::Metadata.code(), 0, 2)]);
        assert_eq!(table.negotiate(ApiKey::Metadata).ok(), Some(2));
        let row = table.get(ApiKey::Metadata).expect("row");
        assert!(!row.broker_ahead());
    }

    #[test]
    fn disjoint_ranges_are_an_error_not_a_guess() {
        let ours = our_range(ApiKey::Metadata).expect("metadata has a schema");
        let table =
            ApiVersions::from_triples([(ApiKey::Metadata.code(), ours.max + 1, ours.max + 5)]);
        let err = table.negotiate(ApiKey::Metadata).unwrap_err();
        assert!(matches!(err, Error::UnsupportedApi { .. }), "{err:?}");
    }

    #[test]
    fn a_key_the_broker_never_mentioned_is_an_error() {
        let table = ApiVersions::from_triples([]);
        let err = table.negotiate(ApiKey::Metadata).unwrap_err();
        match err {
            Error::UnsupportedApi { broker, ours, .. } => {
                assert!(broker.is_none());
                assert!(ours.is_some());
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn keys_with_no_schema_here_survive_in_the_table() {
        // 89 is unassigned in kafka-protocol 0.17; a 4.1+ broker advertises
        // StreamsGroupDescribe there. It must still show up in the table.
        let table = ApiVersions::from_triples([(89, 0, 1)]);
        let row = table.get(ApiKey::from_code(89)).expect("row survives");
        assert_eq!(row.api_key, ApiKey::Unknown(89));
        assert!(row.ours.is_none());
        assert!(row.negotiated().is_none());
        assert!(!row.broker_ahead());
        assert!(!table.supports(ApiKey::from_code(89)));
    }
}
