//! ACLs, client quotas and SCRAM credentials.

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::alter_client_quotas_request::{
    EntityData as AlterEntity, EntryData as AlterEntry, OpData,
};
use kafka_conn::protocol::messages::alter_user_scram_credentials_request::{
    ScramCredentialDeletion, ScramCredentialUpsertion,
};
use kafka_conn::protocol::messages::create_acls_request::AclCreation;
use kafka_conn::protocol::messages::delete_acls_request::DeleteAclsFilter;
use kafka_conn::protocol::messages::describe_client_quotas_request::ComponentData;
use kafka_conn::protocol::messages::describe_user_scram_credentials_request::UserName;
use kafka_conn::protocol::messages::{
    AlterClientQuotasRequest, AlterUserScramCredentialsRequest, CreateAclsRequest,
    DeleteAclsRequest, DescribeAclsRequest, DescribeClientQuotasRequest,
    DescribeUserScramCredentialsRequest,
};
use kafka_conn::{Error, ErrorCode, Result};

use crate::Admin;
use crate::types::PerItem;

/// What an ACL applies to.
///
/// **Not [`crate::ConfigResourceType`].** The two numberings disagree: `4` is
/// `CLUSTER` here and `BROKER` there. Keeping them apart is the only thing
/// stopping a topic ACL from being written as a cluster ACL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AclResourceType {
    /// Match any resource type — filters only.
    Any,
    /// A topic.
    Topic,
    /// A consumer or share group.
    Group,
    /// The cluster itself.
    Cluster,
    /// A transactional id.
    TransactionalId,
    /// A delegation token.
    DelegationToken,
    /// A user, for SCRAM credential management.
    User,
}

impl AclResourceType {
    /// The wire value.
    pub const fn code(self) -> i8 {
        match self {
            AclResourceType::Any => 1,
            AclResourceType::Topic => 2,
            AclResourceType::Group => 3,
            AclResourceType::Cluster => 4,
            AclResourceType::TransactionalId => 5,
            AclResourceType::DelegationToken => 6,
            AclResourceType::User => 7,
        }
    }

    /// From a wire value.
    pub const fn from_code(code: i8) -> Option<Self> {
        match code {
            1 => Some(AclResourceType::Any),
            2 => Some(AclResourceType::Topic),
            3 => Some(AclResourceType::Group),
            4 => Some(AclResourceType::Cluster),
            5 => Some(AclResourceType::TransactionalId),
            6 => Some(AclResourceType::DelegationToken),
            7 => Some(AclResourceType::User),
            _ => None,
        }
    }
}

/// How a resource name is matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PatternType {
    /// Match any pattern type — filters only.
    Any,
    /// Match literal *and* prefixed bindings that would apply — filters only.
    Match,
    /// The name, exactly. `*` means every resource of that type.
    Literal,
    /// Every resource whose name starts with this.
    Prefixed,
}

impl PatternType {
    /// The wire value.
    pub const fn code(self) -> i8 {
        match self {
            PatternType::Any => 1,
            PatternType::Match => 2,
            PatternType::Literal => 3,
            PatternType::Prefixed => 4,
        }
    }

    /// From a wire value.
    pub const fn from_code(code: i8) -> Option<Self> {
        match code {
            1 => Some(PatternType::Any),
            2 => Some(PatternType::Match),
            3 => Some(PatternType::Literal),
            4 => Some(PatternType::Prefixed),
            _ => None,
        }
    }
}

/// What an ACL permits or forbids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AclOperation {
    /// Match any operation — filters only.
    Any,
    /// Every operation.
    All,
    /// Read.
    Read,
    /// Write.
    Write,
    /// Create.
    Create,
    /// Delete.
    Delete,
    /// Alter.
    Alter,
    /// Describe.
    Describe,
    /// Inter-broker actions.
    ClusterAction,
    /// Describe configs.
    DescribeConfigs,
    /// Alter configs.
    AlterConfigs,
    /// Idempotent write.
    IdempotentWrite,
    /// Create delegation tokens.
    CreateTokens,
    /// Describe delegation tokens.
    DescribeTokens,
    /// An operation this build does not name.
    Unknown(i8),
}

impl AclOperation {
    /// The wire value.
    pub const fn code(self) -> i8 {
        match self {
            AclOperation::Any => 1,
            AclOperation::All => 2,
            AclOperation::Read => 3,
            AclOperation::Write => 4,
            AclOperation::Create => 5,
            AclOperation::Delete => 6,
            AclOperation::Alter => 7,
            AclOperation::Describe => 8,
            AclOperation::ClusterAction => 9,
            AclOperation::DescribeConfigs => 10,
            AclOperation::AlterConfigs => 11,
            AclOperation::IdempotentWrite => 12,
            AclOperation::CreateTokens => 13,
            AclOperation::DescribeTokens => 14,
            AclOperation::Unknown(code) => code,
        }
    }

    /// From a wire value.
    pub const fn from_code(code: i8) -> Self {
        match code {
            1 => AclOperation::Any,
            2 => AclOperation::All,
            3 => AclOperation::Read,
            4 => AclOperation::Write,
            5 => AclOperation::Create,
            6 => AclOperation::Delete,
            7 => AclOperation::Alter,
            8 => AclOperation::Describe,
            9 => AclOperation::ClusterAction,
            10 => AclOperation::DescribeConfigs,
            11 => AclOperation::AlterConfigs,
            12 => AclOperation::IdempotentWrite,
            13 => AclOperation::CreateTokens,
            14 => AclOperation::DescribeTokens,
            other => AclOperation::Unknown(other),
        }
    }
}

/// Allow or deny.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AclPermission {
    /// Match either — filters only.
    Any,
    /// Deny. Denies beat allows in Kafka's evaluation order.
    Deny,
    /// Allow.
    Allow,
}

impl AclPermission {
    /// The wire value.
    pub const fn code(self) -> i8 {
        match self {
            AclPermission::Any => 1,
            AclPermission::Deny => 2,
            AclPermission::Allow => 3,
        }
    }

    /// From a wire value.
    pub const fn from_code(code: i8) -> Option<Self> {
        match code {
            1 => Some(AclPermission::Any),
            2 => Some(AclPermission::Deny),
            3 => Some(AclPermission::Allow),
            _ => None,
        }
    }
}

/// A complete ACL binding.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AclBinding {
    /// What kind of resource.
    pub resource_type: AclResourceType,
    /// The resource name, or `*`.
    pub resource_name: String,
    /// How the name is matched.
    pub pattern_type: PatternType,
    /// The principal, as `User:name`.
    pub principal: String,
    /// The host, or `*`.
    pub host: String,
    /// The operation.
    pub operation: AclOperation,
    /// Allow or deny.
    pub permission: AclPermission,
}

impl AclBinding {
    /// Allow a principal an operation on a literally-named resource.
    pub fn allow(
        resource_type: AclResourceType,
        resource_name: impl Into<String>,
        principal: impl Into<String>,
        operation: AclOperation,
    ) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.into(),
            pattern_type: PatternType::Literal,
            principal: principal.into(),
            host: "*".to_owned(),
            operation,
            permission: AclPermission::Allow,
        }
    }
}

/// A filter for describing or deleting ACLs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclFilter {
    /// Resource type, or [`AclResourceType::Any`].
    pub resource_type: AclResourceType,
    /// Resource name, or `None` for any.
    pub resource_name: Option<String>,
    /// Pattern type, or [`PatternType::Any`].
    pub pattern_type: PatternType,
    /// Principal, or `None` for any.
    pub principal: Option<String>,
    /// Host, or `None` for any.
    pub host: Option<String>,
    /// Operation, or [`AclOperation::Any`].
    pub operation: AclOperation,
    /// Permission, or [`AclPermission::Any`].
    pub permission: AclPermission,
}

impl Default for AclFilter {
    /// Matches every ACL.
    fn default() -> Self {
        Self {
            resource_type: AclResourceType::Any,
            resource_name: None,
            pattern_type: PatternType::Any,
            principal: None,
            host: None,
            operation: AclOperation::Any,
            permission: AclPermission::Any,
        }
    }
}

impl AclFilter {
    /// Match one exact binding.
    pub fn exact(binding: &AclBinding) -> Self {
        Self {
            resource_type: binding.resource_type,
            resource_name: Some(binding.resource_name.clone()),
            pattern_type: binding.pattern_type,
            principal: Some(binding.principal.clone()),
            host: Some(binding.host.clone()),
            operation: binding.operation,
            permission: binding.permission,
        }
    }
}

/// A quota entity: a map from entity type (`user`, `client-id`, `ip`) to name.
///
/// A `None` name means "the default for this entity type", which is a distinct
/// thing from a user literally named `<default>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaEntity {
    /// Entity components, in the order the broker reported them.
    pub components: Vec<(String, Option<String>)>,
}

/// A quota key and its value, or `None` to remove it.
pub type QuotaOp = (String, Option<f64>);

/// A filter for describing quotas.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuotaFilter {
    /// Components to match: entity type, and optionally an exact name.
    ///
    /// An entry with `None` matches the *default* entity for that type.
    pub components: Vec<(String, Option<String>)>,
    /// Whether the entity must contain exactly these component types.
    pub strict: bool,
}

/// A SCRAM mechanism, as the credential apis number them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramMechanism {
    /// SCRAM-SHA-256.
    Sha256,
    /// SCRAM-SHA-512.
    Sha512,
}

impl ScramMechanism {
    /// The wire value.
    pub const fn code(self) -> i8 {
        match self {
            ScramMechanism::Sha256 => 1,
            ScramMechanism::Sha512 => 2,
        }
    }

    /// From a wire value.
    pub const fn from_code(code: i8) -> Option<Self> {
        match code {
            1 => Some(ScramMechanism::Sha256),
            2 => Some(ScramMechanism::Sha512),
            _ => None,
        }
    }
}

/// A stored SCRAM credential, as the broker describes it.
///
/// No password: the broker stores a salted hash and cannot return one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScramCredentialInfo {
    /// The mechanism.
    pub mechanism: ScramMechanism,
    /// The iteration count.
    pub iterations: i32,
}

/// A SCRAM credential to write.
#[derive(Debug, Clone)]
pub struct ScramUpsert {
    /// The user.
    pub user: String,
    /// The mechanism.
    pub mechanism: ScramMechanism,
    /// Iteration count. Kafka's own minimum is 4096.
    pub iterations: i32,
    /// The password, which never leaves this process.
    ///
    /// The wire carries a salt and a *salted* password; the plaintext is hashed
    /// client-side. That is why this api exists at all rather than being a
    /// config setting.
    pub password: String,
}

impl ScramUpsert {
    /// A credential with Kafka's minimum iteration count.
    pub fn new(
        user: impl Into<String>,
        mechanism: ScramMechanism,
        password: impl Into<String>,
    ) -> Self {
        Self {
            user: user.into(),
            mechanism,
            iterations: 4096,
            password: password.into(),
        }
    }
}

impl Admin {
    /// Describe ACLs matching a filter.
    pub async fn describe_acls(&self, filter: &AclFilter) -> Result<Vec<AclBinding>> {
        let request = DescribeAclsRequest::default()
            .with_resource_type_filter(filter.resource_type.code())
            .with_resource_name_filter(
                filter
                    .resource_name
                    .as_ref()
                    .map(|n| StrBytes::from_string(n.clone())),
            )
            .with_pattern_type_filter(filter.pattern_type.code())
            .with_principal_filter(
                filter
                    .principal
                    .as_ref()
                    .map(|p| StrBytes::from_string(p.clone())),
            )
            .with_host_filter(
                filter
                    .host
                    .as_ref()
                    .map(|h| StrBytes::from_string(h.clone())),
            )
            .with_operation(filter.operation.code())
            .with_permission_type(filter.permission.code());

        let response = self.cluster().send_any(request).await?;
        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::from_code(
                code,
                response.error_message.map(|m| m.to_string()),
            ));
        }

        Ok(response
            .resources
            .into_iter()
            .flat_map(|resource| {
                let resource_type = AclResourceType::from_code(resource.resource_type)
                    .unwrap_or(AclResourceType::Any);
                let pattern_type =
                    PatternType::from_code(resource.pattern_type).unwrap_or(PatternType::Any);
                let name = resource.resource_name.to_string();
                resource.acls.into_iter().map(move |acl| AclBinding {
                    resource_type,
                    resource_name: name.clone(),
                    pattern_type,
                    principal: acl.principal.to_string(),
                    host: acl.host.to_string(),
                    operation: AclOperation::from_code(acl.operation),
                    permission: AclPermission::from_code(acl.permission_type)
                        .unwrap_or(AclPermission::Any),
                })
            })
            .collect())
    }

    /// Create ACLs.
    pub async fn create_acls(
        &self,
        bindings: impl IntoIterator<Item = AclBinding>,
    ) -> Result<PerItem<AclBinding, ()>> {
        let bindings: Vec<AclBinding> = bindings.into_iter().collect();
        if bindings.is_empty() {
            return Ok(Vec::new());
        }

        let request = CreateAclsRequest::default().with_creations(
            bindings
                .iter()
                .map(|binding| {
                    AclCreation::default()
                        .with_resource_type(binding.resource_type.code())
                        .with_resource_name(StrBytes::from_string(binding.resource_name.clone()))
                        .with_resource_pattern_type(binding.pattern_type.code())
                        .with_principal(StrBytes::from_string(binding.principal.clone()))
                        .with_host(StrBytes::from_string(binding.host.clone()))
                        .with_operation(binding.operation.code())
                        .with_permission_type(binding.permission.code())
                })
                .collect(),
        );

        let response = self.cluster().send_any(request).await?;
        // The response is positional — results come back in request order with
        // no key — so zipping is the only correlation available.
        Ok(bindings
            .into_iter()
            .zip(response.results)
            .map(|(binding, result)| {
                let outcome = match ErrorCode::from_code(result.error_code) {
                    Some(code) => Err(Error::from_code(
                        code,
                        result.error_message.map(|m| m.to_string()),
                    )),
                    None => Ok(()),
                };
                (binding, outcome)
            })
            .collect())
    }

    /// Delete ACLs matching filters, returning what each filter removed.
    pub async fn delete_acls(
        &self,
        filters: impl IntoIterator<Item = AclFilter>,
    ) -> Result<PerItem<AclFilter, Vec<AclBinding>>> {
        let filters: Vec<AclFilter> = filters.into_iter().collect();
        if filters.is_empty() {
            return Ok(Vec::new());
        }

        let request = DeleteAclsRequest::default().with_filters(
            filters
                .iter()
                .map(|filter| {
                    DeleteAclsFilter::default()
                        .with_resource_type_filter(filter.resource_type.code())
                        .with_resource_name_filter(
                            filter
                                .resource_name
                                .as_ref()
                                .map(|n| StrBytes::from_string(n.clone())),
                        )
                        .with_pattern_type_filter(filter.pattern_type.code())
                        .with_principal_filter(
                            filter
                                .principal
                                .as_ref()
                                .map(|p| StrBytes::from_string(p.clone())),
                        )
                        .with_host_filter(
                            filter
                                .host
                                .as_ref()
                                .map(|h| StrBytes::from_string(h.clone())),
                        )
                        .with_operation(filter.operation.code())
                        .with_permission_type(filter.permission.code())
                })
                .collect(),
        );

        let response = self.cluster().send_any(request).await?;
        Ok(filters
            .into_iter()
            .zip(response.filter_results)
            .map(|(filter, result)| {
                let outcome = match ErrorCode::from_code(result.error_code) {
                    Some(code) => Err(Error::from_code(
                        code,
                        result.error_message.map(|m| m.to_string()),
                    )),
                    None => Ok(result
                        .matching_acls
                        .into_iter()
                        .map(|acl| AclBinding {
                            resource_type: AclResourceType::from_code(acl.resource_type)
                                .unwrap_or(AclResourceType::Any),
                            resource_name: acl.resource_name.to_string(),
                            pattern_type: PatternType::from_code(acl.pattern_type)
                                .unwrap_or(PatternType::Any),
                            principal: acl.principal.to_string(),
                            host: acl.host.to_string(),
                            operation: AclOperation::from_code(acl.operation),
                            permission: AclPermission::from_code(acl.permission_type)
                                .unwrap_or(AclPermission::Any),
                        })
                        .collect()),
                };
                (filter, outcome)
            })
            .collect())
    }

    /// Describe client quotas.
    pub async fn describe_client_quotas(
        &self,
        filter: &QuotaFilter,
    ) -> Result<Vec<(QuotaEntity, Vec<(String, f64)>)>> {
        let request = DescribeClientQuotasRequest::default()
            .with_strict(filter.strict)
            .with_components(
                filter
                    .components
                    .iter()
                    .map(|(entity_type, name)| {
                        // match_type: 0 exact, 1 default, 2 any.
                        let (match_type, value) = match name {
                            Some(name) => (0, Some(StrBytes::from_string(name.clone()))),
                            None => (2, None),
                        };
                        ComponentData::default()
                            .with_entity_type(StrBytes::from_string(entity_type.clone()))
                            .with_match_type(match_type)
                            .with_match(value)
                    })
                    .collect(),
            );

        let response = self.cluster().send_any(request).await?;
        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::from_code(
                code,
                response.error_message.map(|m| m.to_string()),
            ));
        }

        Ok(response
            .entries
            .unwrap_or_default()
            .into_iter()
            .map(|entry| {
                let entity = QuotaEntity {
                    components: entry
                        .entity
                        .into_iter()
                        .map(|e| {
                            (
                                e.entity_type.to_string(),
                                e.entity_name.map(|n| n.to_string()),
                            )
                        })
                        .collect(),
                };
                let values = entry
                    .values
                    .into_iter()
                    .map(|value| (value.key.to_string(), value.value))
                    .collect();
                (entity, values)
            })
            .collect())
    }

    /// Set or remove client quotas.
    ///
    /// A `None` value removes the quota key; `Some` sets it.
    pub async fn alter_client_quotas(
        &self,
        changes: impl IntoIterator<Item = (QuotaEntity, Vec<QuotaOp>)>,
    ) -> Result<PerItem<QuotaEntity, ()>> {
        let changes: Vec<(QuotaEntity, Vec<QuotaOp>)> = changes.into_iter().collect();
        if changes.is_empty() {
            return Ok(Vec::new());
        }

        let request = AlterClientQuotasRequest::default()
            .with_validate_only(false)
            .with_entries(
                changes
                    .iter()
                    .map(|(entity, ops)| {
                        AlterEntry::default()
                            .with_entity(
                                entity
                                    .components
                                    .iter()
                                    .map(|(entity_type, name)| {
                                        AlterEntity::default()
                                            .with_entity_type(StrBytes::from_string(
                                                entity_type.clone(),
                                            ))
                                            .with_entity_name(
                                                name.as_ref()
                                                    .map(|n| StrBytes::from_string(n.clone())),
                                            )
                                    })
                                    .collect(),
                            )
                            .with_ops(
                                ops.iter()
                                    .map(|(key, value)| {
                                        OpData::default()
                                            .with_key(StrBytes::from_string(key.clone()))
                                            .with_value(value.unwrap_or_default())
                                            .with_remove(value.is_none())
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            );

        let response = self.cluster().send_any(request).await?;
        Ok(response
            .entries
            .into_iter()
            .map(|entry| {
                let entity = QuotaEntity {
                    components: entry
                        .entity
                        .into_iter()
                        .map(|e| {
                            (
                                e.entity_type.to_string(),
                                e.entity_name.map(|n| n.to_string()),
                            )
                        })
                        .collect(),
                };
                let outcome = match ErrorCode::from_code(entry.error_code) {
                    Some(code) => Err(Error::from_code(
                        code,
                        entry.error_message.map(|m| m.to_string()),
                    )),
                    None => Ok(()),
                };
                (entity, outcome)
            })
            .collect())
    }

    /// Describe stored SCRAM credentials.
    ///
    /// `None` describes every user.
    pub async fn describe_scram_credentials(
        &self,
        users: Option<Vec<String>>,
    ) -> Result<PerItem<String, Vec<ScramCredentialInfo>>> {
        let request =
            DescribeUserScramCredentialsRequest::default().with_users(users.map(|users| {
                users
                    .into_iter()
                    .map(|user| UserName::default().with_name(StrBytes::from_string(user)))
                    .collect()
            }));

        let response = self.cluster().send_any(request).await?;
        if let Some(code) = ErrorCode::from_code(response.error_code) {
            return Err(Error::from_code(
                code,
                response.error_message.map(|m| m.to_string()),
            ));
        }

        Ok(response
            .results
            .into_iter()
            .map(|result| {
                let user = result.user.to_string();
                let outcome = match ErrorCode::from_code(result.error_code) {
                    Some(code) => Err(Error::from_code(
                        code,
                        result.error_message.map(|m| m.to_string()),
                    )),
                    None => Ok(result
                        .credential_infos
                        .into_iter()
                        .filter_map(|info| {
                            ScramMechanism::from_code(info.mechanism).map(|mechanism| {
                                ScramCredentialInfo {
                                    mechanism,
                                    iterations: info.iterations,
                                }
                            })
                        })
                        .collect()),
                };
                (user, outcome)
            })
            .collect())
    }

    /// Create or replace SCRAM credentials.
    ///
    /// The password is salted and hashed here; the plaintext never reaches the
    /// broker. That also means the iteration count and salt are ours to choose,
    /// and getting the hashing wrong produces a credential that stores fine and
    /// fails every subsequent login.
    pub async fn upsert_scram_credentials(
        &self,
        upserts: impl IntoIterator<Item = ScramUpsert>,
    ) -> Result<PerItem<String, ()>> {
        let upserts: Vec<ScramUpsert> = upserts.into_iter().collect();
        if upserts.is_empty() {
            return Ok(Vec::new());
        }

        let mut upsertions = Vec::with_capacity(upserts.len());
        for upsert in &upserts {
            if upsert.iterations < 4096 {
                return Err(Error::InvalidRequest(format!(
                    "SCRAM iteration count {} is below Kafka's minimum of 4096",
                    upsert.iterations
                )));
            }
            let salt = kafka_conn::random_salt();
            let iterations = u32::try_from(upsert.iterations)
                .map_err(|_| Error::InvalidRequest("negative iteration count".to_owned()))?;
            let salted = kafka_conn::salted_password(
                match upsert.mechanism {
                    ScramMechanism::Sha256 => kafka_conn::ScramHash::Sha256,
                    ScramMechanism::Sha512 => kafka_conn::ScramHash::Sha512,
                },
                &upsert.password,
                &salt,
                iterations,
            )?;

            upsertions.push(
                ScramCredentialUpsertion::default()
                    .with_name(StrBytes::from_string(upsert.user.clone()))
                    .with_mechanism(upsert.mechanism.code())
                    .with_iterations(upsert.iterations)
                    .with_salt(bytes::Bytes::from(salt))
                    .with_salted_password(bytes::Bytes::from(salted)),
            );
        }

        let request = AlterUserScramCredentialsRequest::default().with_upsertions(upsertions);
        let response = self.cluster().send_any(request).await?;
        Ok(scram_results(response))
    }

    /// Delete SCRAM credentials.
    pub async fn delete_scram_credentials(
        &self,
        deletions: impl IntoIterator<Item = (String, ScramMechanism)>,
    ) -> Result<PerItem<String, ()>> {
        let deletions: Vec<(String, ScramMechanism)> = deletions.into_iter().collect();
        if deletions.is_empty() {
            return Ok(Vec::new());
        }

        let request = AlterUserScramCredentialsRequest::default().with_deletions(
            deletions
                .iter()
                .map(|(user, mechanism)| {
                    ScramCredentialDeletion::default()
                        .with_name(StrBytes::from_string(user.clone()))
                        .with_mechanism(mechanism.code())
                })
                .collect(),
        );
        let response = self.cluster().send_any(request).await?;
        Ok(scram_results(response))
    }
}

fn scram_results(
    response: kafka_conn::protocol::messages::AlterUserScramCredentialsResponse,
) -> PerItem<String, ()> {
    response
        .results
        .into_iter()
        .map(|result| {
            let user = result.user.to_string();
            let outcome = match ErrorCode::from_code(result.error_code) {
                Some(code) => Err(Error::from_code(
                    code,
                    result.error_message.map(|m| m.to_string()),
                )),
                None => Ok(()),
            };
            (user, outcome)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acl_and_config_resource_numberings_do_not_agree_and_must_not_be_shared() {
        // 4 is CLUSTER for an ACL and BROKER for a config. Two enums, on
        // purpose.
        assert_eq!(AclResourceType::Cluster.code(), 4);
        assert_eq!(crate::types::ConfigResourceType::Broker.code(), 4);
        assert_eq!(AclResourceType::Topic.code(), 2);
        assert_eq!(crate::types::ConfigResourceType::Topic.code(), 2);
        // Only TOPIC happens to coincide, which is exactly what makes sharing
        // one enum look like it works.
    }

    #[test]
    fn acl_wire_values_round_trip() {
        for resource_type in [
            AclResourceType::Any,
            AclResourceType::Topic,
            AclResourceType::Group,
            AclResourceType::Cluster,
            AclResourceType::TransactionalId,
            AclResourceType::DelegationToken,
            AclResourceType::User,
        ] {
            assert_eq!(
                AclResourceType::from_code(resource_type.code()),
                Some(resource_type)
            );
        }
        for pattern in [
            PatternType::Any,
            PatternType::Match,
            PatternType::Literal,
            PatternType::Prefixed,
        ] {
            assert_eq!(PatternType::from_code(pattern.code()), Some(pattern));
        }
        for permission in [
            AclPermission::Any,
            AclPermission::Deny,
            AclPermission::Allow,
        ] {
            assert_eq!(
                AclPermission::from_code(permission.code()),
                Some(permission)
            );
        }
        for code in 1..=14i8 {
            assert_eq!(AclOperation::from_code(code).code(), code);
        }
        // A future operation survives rather than collapsing into Any.
        assert_eq!(AclOperation::from_code(99), AclOperation::Unknown(99));
        assert_eq!(AclOperation::Unknown(99).code(), 99);
    }

    #[test]
    fn the_default_filter_matches_everything() {
        let filter = AclFilter::default();
        assert_eq!(filter.resource_type, AclResourceType::Any);
        assert_eq!(filter.operation, AclOperation::Any);
        assert_eq!(filter.permission, AclPermission::Any);
        assert!(filter.resource_name.is_none());
    }

    #[test]
    fn an_exact_filter_pins_every_field_of_its_binding() {
        let binding = AclBinding::allow(
            AclResourceType::Topic,
            "orders",
            "User:alice",
            AclOperation::Read,
        );
        let filter = AclFilter::exact(&binding);
        assert_eq!(filter.resource_name.as_deref(), Some("orders"));
        assert_eq!(filter.principal.as_deref(), Some("User:alice"));
        assert_eq!(filter.pattern_type, PatternType::Literal);
        assert_eq!(filter.permission, AclPermission::Allow);
    }

    #[test]
    fn scram_mechanisms_round_trip() {
        assert_eq!(ScramMechanism::from_code(1), Some(ScramMechanism::Sha256));
        assert_eq!(ScramMechanism::from_code(2), Some(ScramMechanism::Sha512));
        assert_eq!(ScramMechanism::from_code(0), None);
    }
}
