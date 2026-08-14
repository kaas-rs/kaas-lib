//! Everything a cluster will tell you about one principal.
//!
//! The question this answers is "how does user X authenticate here", and the
//! honest answer has a hard edge: **SCRAM is the only credential store Kafka
//! itself owns.** A `PLAIN` user lives in a JAAS file on the broker's disk or
//! behind a custom callback handler, an `OAUTHBEARER` identity belongs to an
//! identity provider, a Kerberos one to a KDC, and a mutual-TLS one to a
//! certificate authority. None of those four leaves a record any api key can
//! read, and `DescribeConfigs` returns the JAAS entries redacted — sensitive
//! configs arrive with no value at all — so a principal that authenticates by
//! any of them describes as *empty* here rather than as absent.
//!
//! That distinction is the whole reason
//! [`is_unrecorded`](PrincipalDescription::is_unrecorded) is named the way it
//! is. It says the cluster holds no record of this principal. It does not say
//! the principal cannot log in.
//!
//! What is readable, and what each source actually proves:
//!
//! * `DescribeUserScramCredentials` — the mechanisms and iteration counts of
//!   stored SCRAM credentials. Positive proof of how the principal can log in,
//!   and the only source here that is.
//! * `DescribeDelegationToken` — tokens owned by the principal. Each one is a
//!   live SCRAM credential that authenticates *as* it, which is the half of
//!   "who can be X" that an audit of credentials alone misses.
//! * `DescribeAcls` — what the principal is authorized for. Not how it
//!   authenticates, but reliably the next question.
//! * `DescribeClientQuotas` — the quotas that apply to it.
//!
//! Every one of the four is non-mutating, so this works unchanged on a
//! [read-only client](crate::Admin::connect_read_only).

use kafka_conn::{ErrorCode, Result};

use crate::Admin;
use crate::security::{
    AclBinding, AclFilter, QuotaAssignment, QuotaFilter, ScramCredentialInfo, ScramMechanism,
};
use crate::tokens::{DelegationToken, Principal};
use crate::types::PerItem;

/// The entity type quotas key a user by.
const USER_ENTITY: &str = "user";

/// What a cluster records about one principal.
///
/// Every source is a separate [`Result`] for the same reason rule 4 exists: a
/// caller who may read ACLs but not SCRAM credentials must get the ACLs. One
/// unreadable source failing the whole describe would make this useless on
/// precisely the clusters — locked down, partly delegated — where asking who
/// somebody is matters most.
///
/// So an error in a field means *that source* did not answer, and the
/// distinction between `Ok(vec![])` and `Err(_)` carries real information:
/// "the cluster has no SCRAM credential for this principal" versus "you may
/// not look".
#[derive(Debug)]
pub struct PrincipalDescription {
    /// The principal that was asked about.
    pub principal: Principal,
    /// Stored SCRAM credentials, one per mechanism. Empty when the principal
    /// has none — which is the normal answer for a mutual-TLS, Kerberos,
    /// `PLAIN` or `OAUTHBEARER` identity, all of which are stored elsewhere.
    pub scram: Result<Vec<ScramCredentialInfo>>,
    /// Delegation tokens owned by the principal, without their credentials.
    ///
    /// `Err` with `DELEGATION_TOKEN_AUTH_DISABLED` means the cluster has no
    /// token master key, so it stores no tokens for anybody.
    pub tokens: Result<Vec<PrincipalToken>>,
    /// ACLs naming this principal exactly.
    ///
    /// **Exactly** is load-bearing: `DescribeAcls` matches the principal
    /// string, so a binding on `User:*` that also grants this principal does
    /// not appear here. An empty list is not "has no permissions" — query
    /// [`describe_acls`](Admin::describe_acls) for `User:*` as well before
    /// telling anyone that.
    ///
    /// `Err` with `SECURITY_DISABLED` means the cluster runs no authorizer, so
    /// it holds no ACLs for anybody.
    pub acls: Result<Vec<AclBinding>>,
    /// Quotas whose entity includes `user=<name>`, with the entity the broker
    /// reported them under — a `user` + `client-id` quota is a different
    /// entity from a `user` one and applies in different circumstances.
    pub quotas: Result<Vec<QuotaAssignment>>,
}

/// One delegation token owned by a principal, **without its HMAC**.
///
/// The HMAC is the credential — base64 it and it is a SCRAM password — so an
/// audit view that carries one is a single careless log line away from leaking
/// it. `DescribeDelegationToken` does return it for tokens the caller may see,
/// and [`Admin::describe_delegation_tokens`] hands back the full
/// [`DelegationToken`] when the credential is what you actually want. This
/// type is for the question "who can act as this principal", which the id and
/// the expiry answer on their own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrincipalToken {
    /// The token id, which is also the SCRAM username it authenticates with.
    pub token_id: String,
    /// Who asked for it. The same as the owner unless a KIP-373 request named
    /// somebody else — and when it differs, it names the principal that chose
    /// to create an identity for this one.
    pub requester: Principal,
    /// When the token was issued, in epoch milliseconds.
    pub issue_timestamp_ms: i64,
    /// When it expires unless renewed, in epoch milliseconds.
    pub expiry_timestamp_ms: i64,
    /// The furthest it can ever be renewed to, in epoch milliseconds.
    pub max_timestamp_ms: i64,
    /// Who else may renew it.
    pub renewers: Vec<Principal>,
}

impl From<DelegationToken> for PrincipalToken {
    /// Drops the HMAC, deliberately. See the type docs.
    fn from(token: DelegationToken) -> Self {
        Self {
            token_id: token.token_id,
            requester: token.requester,
            issue_timestamp_ms: token.issue_timestamp_ms,
            expiry_timestamp_ms: token.expiry_timestamp_ms,
            max_timestamp_ms: token.max_timestamp_ms,
            renewers: token.renewers,
        }
    }
}

impl PrincipalDescription {
    /// The SCRAM mechanisms this principal can authenticate with.
    ///
    /// Empty when there are none *and* when the credentials could not be read;
    /// check [`scram`](Self::scram) directly to tell those apart.
    pub fn scram_mechanisms(&self) -> Vec<ScramMechanism> {
        match &self.scram {
            Ok(infos) => infos.iter().map(|info| info.mechanism).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Whether the cluster holds a credential that authenticates as this
    /// principal — a SCRAM entry or a delegation token.
    ///
    /// False for a mutual-TLS or `OAUTHBEARER` principal that is perfectly able
    /// to connect, because the credential proving it is held somewhere the
    /// protocol cannot reach.
    pub fn has_stored_credentials(&self) -> bool {
        matches!(&self.scram, Ok(infos) if !infos.is_empty())
            || matches!(&self.tokens, Ok(tokens) if !tokens.is_empty())
    }

    /// Whether every source reported nothing for this principal.
    ///
    /// "Reported nothing" folds in the two errors that are cluster-wide
    /// statements of absence rather than per-principal failures:
    /// `SECURITY_DISABLED` from a cluster running no authorizer, and
    /// `DELEGATION_TOKEN_AUTH_DISABLED` from one with no token master key.
    /// Both mean the cluster stores none of that kind of thing *for anyone*,
    /// which is an empty answer and not a refused one. Anything else — an
    /// authorization failure, a timeout, an api key the broker does not serve
    /// — leaves the answer unknown, and this returns false.
    ///
    /// True does not mean the principal cannot connect. It means the cluster
    /// is not where the answer lives; see the module docs.
    pub fn is_unrecorded(&self) -> bool {
        reports_nothing(&self.scram)
            && reports_nothing(&self.tokens)
            && reports_nothing(&self.acls)
            && reports_nothing(&self.quotas)
    }
}

/// Whether a source answered "nothing here", counting the two feature-disabled
/// codes as the cluster-wide empties they are.
fn reports_nothing<T>(source: &Result<Vec<T>>) -> bool {
    match source {
        Ok(items) => items.is_empty(),
        Err(error) => matches!(
            error.code(),
            Some(ErrorCode::SecurityDisabled | ErrorCode::DelegationTokenAuthDisabled)
        ),
    }
}

impl Admin {
    /// Describe what the cluster records about one principal.
    ///
    /// Four non-mutating RPCs, each reported separately — see
    /// [`PrincipalDescription`] for why, and the [module docs](self) for the
    /// much more important question of what the cluster cannot tell you at
    /// all.
    ///
    /// The outer `Result` is reserved for not reaching the cluster, so that an
    /// unreachable broker is one error rather than the same error copied into
    /// four fields. Once a connection exists, every failure is a field.
    ///
    /// ```no_run
    /// # async fn example() -> kafka_admin::Result<()> {
    /// use kafka_admin::{Admin, Principal};
    /// use kafka_meta::ClusterConfig;
    ///
    /// let admin = Admin::connect(["localhost:9092"], ClusterConfig::default()).await?;
    /// let described = admin.describe_principal(&Principal::user("alice")).await?;
    ///
    /// if described.is_unrecorded() {
    ///     // Not proof of absence: mutual TLS, Kerberos, PLAIN and
    ///     // OAUTHBEARER identities are all stored outside the cluster.
    ///     println!("the cluster records nothing about alice");
    /// } else {
    ///     println!("SCRAM: {:?}", described.scram_mechanisms());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn describe_principal(&self, principal: &Principal) -> Result<PrincipalDescription> {
        // Cheap: the pool hands back a connection it already holds, and opens
        // one only if it has none. Either way it is the "can we talk to this
        // cluster at all" question, asked once.
        self.cluster().pool().any().await?;

        let scram = self.principal_scram(principal).await;
        let tokens = self.principal_tokens(principal).await;
        let acls = self.principal_acls(principal).await;
        let quotas = self.principal_quotas(principal).await;

        Ok(PrincipalDescription {
            principal: principal.clone(),
            scram,
            tokens,
            acls,
            quotas,
        })
    }

    /// Stored SCRAM credentials for a principal.
    async fn principal_scram(&self, principal: &Principal) -> Result<Vec<ScramCredentialInfo>> {
        // SCRAM credentials are keyed by an unqualified username and only a
        // `User` principal has one. Asking about any other type is a question
        // the credential store cannot be asked, and its answer is the empty
        // set rather than an error.
        if principal.principal_type != "User" {
            return Ok(Vec::new());
        }

        // One user, sent as a one-element list. Never `None` here even though
        // this asks about exactly one user: `None` is "describe everyone",
        // which on a big cluster is a much larger answer, and which
        // `describe_scram_credentials` has its own NPE trap around.
        let described = self
            .describe_scram_credentials(Some(vec![principal.name.clone()]))
            .await?;
        scram_for(&principal.name, described)
    }

    /// Delegation tokens owned by a principal, stripped of their HMACs.
    async fn principal_tokens(&self, principal: &Principal) -> Result<Vec<PrincipalToken>> {
        let owners = [principal.clone()];
        Ok(self
            .describe_delegation_tokens(Some(&owners))
            .await?
            .into_iter()
            .map(PrincipalToken::from)
            .collect())
    }

    /// ACLs naming a principal exactly.
    async fn principal_acls(&self, principal: &Principal) -> Result<Vec<AclBinding>> {
        // `Principal`'s `Display` is the ACL spelling — `User:alice`, and for
        // a certificate principal the whole subject DN, commas and all.
        self.describe_acls(&AclFilter {
            principal: Some(principal.to_string()),
            ..AclFilter::default()
        })
        .await
    }

    /// Quotas that apply to a principal.
    async fn principal_quotas(&self, principal: &Principal) -> Result<Vec<QuotaAssignment>> {
        // Not strict: a `user` + `client-id` quota applies to this user too,
        // and a caller asking what constrains a principal wants it listed.
        self.describe_client_quotas(&QuotaFilter {
            components: vec![(USER_ENTITY.to_owned(), Some(principal.name.clone()))],
            strict: false,
        })
        .await
    }
}

/// Pick one user's credentials out of a per-user result set.
///
/// `RESOURCE_NOT_FOUND` is the broker's way of saying this user has no stored
/// credential. That is an answer, not a failure, and reporting it as an error
/// would make "no SCRAM credentials" indistinguishable from "you may not look"
/// — the one distinction this whole type exists to preserve.
fn scram_for(
    user: &str,
    described: PerItem<String, Vec<ScramCredentialInfo>>,
) -> Result<Vec<ScramCredentialInfo>> {
    let outcome = described
        .into_iter()
        .find(|(name, _)| name.as_str() == user)
        .map(|(_, outcome)| outcome);

    match outcome {
        Some(Err(error)) if error.code() == Some(ErrorCode::ResourceNotFound) => Ok(Vec::new()),
        // A broker answering a one-user describe without mentioning that user
        // is saying the same thing in a different way; Apache Kafka sends the
        // code, and a reimplementation may simply omit the entry.
        None => Ok(Vec::new()),
        Some(outcome) => outcome,
    }
}

#[cfg(test)]
mod tests {
    use kafka_conn::Error;

    use super::*;

    fn credential(mechanism: ScramMechanism) -> ScramCredentialInfo {
        ScramCredentialInfo {
            mechanism,
            iterations: 4096,
        }
    }

    fn described(
        scram: Result<Vec<ScramCredentialInfo>>,
        tokens: Result<Vec<PrincipalToken>>,
        acls: Result<Vec<AclBinding>>,
    ) -> PrincipalDescription {
        PrincipalDescription {
            principal: Principal::user("alice"),
            scram,
            tokens,
            acls,
            quotas: Ok(Vec::new()),
        }
    }

    #[test]
    fn a_user_with_no_stored_credential_is_not_an_error() {
        let response = vec![(
            "alice".to_owned(),
            Err(Error::from_code(ErrorCode::ResourceNotFound, None)),
        )];
        assert_eq!(scram_for("alice", response).unwrap(), Vec::new());
    }

    #[test]
    fn a_user_the_broker_did_not_mention_has_no_credential_either() {
        assert_eq!(scram_for("alice", Vec::new()).unwrap(), Vec::new());
    }

    /// The distinction the per-source `Result` exists for: an unreadable
    /// credential store must not read as an empty one.
    #[test]
    fn an_unreadable_credential_store_stays_an_error() {
        let response = vec![(
            "alice".to_owned(),
            Err(Error::from_code(
                ErrorCode::ClusterAuthorizationFailed,
                None,
            )),
        )];
        let error = scram_for("alice", response).unwrap_err();
        assert_eq!(error.code(), Some(ErrorCode::ClusterAuthorizationFailed));
    }

    #[test]
    fn credentials_are_returned_for_the_user_that_was_asked_about() {
        let response = vec![
            (
                "bob".to_owned(),
                Ok(vec![credential(ScramMechanism::Sha256)]),
            ),
            (
                "alice".to_owned(),
                Ok(vec![credential(ScramMechanism::Sha512)]),
            ),
        ];
        let found = scram_for("alice", response).unwrap();
        assert_eq!(found, vec![credential(ScramMechanism::Sha512)]);
    }

    #[test]
    fn scram_mechanisms_reads_the_stored_credentials() {
        let description = described(
            Ok(vec![
                credential(ScramMechanism::Sha256),
                credential(ScramMechanism::Sha512),
            ]),
            Ok(Vec::new()),
            Ok(Vec::new()),
        );
        assert_eq!(
            description.scram_mechanisms(),
            vec![ScramMechanism::Sha256, ScramMechanism::Sha512]
        );
        assert!(description.has_stored_credentials());
        assert!(!description.is_unrecorded());
    }

    /// A cluster with no authorizer and no token master key holds no ACLs and
    /// no tokens *for anybody*. Both codes are statements of absence, so a
    /// principal it records nothing else about is unrecorded — otherwise the
    /// helper would answer false on every plain broker and mean nothing.
    #[test]
    fn feature_disabled_codes_count_as_empty() {
        let description = described(
            Ok(Vec::new()),
            Err(Error::from_code(
                ErrorCode::DelegationTokenAuthDisabled,
                None,
            )),
            Err(Error::from_code(ErrorCode::SecurityDisabled, None)),
        );
        assert!(description.is_unrecorded());
        assert!(!description.has_stored_credentials());
    }

    /// An authorization failure is not an empty answer, and must not be
    /// rendered as "the cluster knows nothing about this user".
    #[test]
    fn a_refused_source_leaves_the_answer_unknown() {
        let description = described(
            Err(Error::from_code(
                ErrorCode::ClusterAuthorizationFailed,
                None,
            )),
            Ok(Vec::new()),
            Ok(Vec::new()),
        );
        assert!(!description.is_unrecorded());
    }

    /// A token is a credential that authenticates *as* the principal, so a
    /// principal with no password but a live token is not credential-less.
    #[test]
    fn a_delegation_token_counts_as_a_credential() {
        let token = PrincipalToken {
            token_id: "token-1".to_owned(),
            requester: Principal::user("alice"),
            issue_timestamp_ms: 0,
            expiry_timestamp_ms: 1,
            max_timestamp_ms: 2,
            renewers: Vec::new(),
        };
        let description = described(Ok(Vec::new()), Ok(vec![token]), Ok(Vec::new()));
        assert!(description.has_stored_credentials());
        assert!(!description.is_unrecorded());
    }
}
