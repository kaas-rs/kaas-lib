//! Configuration: describe and incrementally alter.
//!
//! `IncrementalAlterConfigs`, never `AlterConfigs`. The older api replaces a
//! resource's entire dynamic configuration with what you send, so changing one
//! key means reading everything, editing it, and writing it back — and losing
//! whatever another operator changed in between. There is no case where that is
//! the behaviour a UI wants.

use kafka_conn::protocol::StrBytes;
use kafka_conn::protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_conn::protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig,
};
use kafka_conn::protocol::messages::{DescribeConfigsRequest, IncrementalAlterConfigsRequest};
use kafka_conn::{Error, ErrorCode, Result};

use crate::Admin;
use crate::types::{
    ConfigChange, ConfigEntry, ConfigResource, ConfigResourceType, ConfigSource, PerItem,
};

impl Admin {
    /// Describe configurations.
    pub async fn describe_configs(
        &self,
        resources: impl IntoIterator<Item = ConfigResource>,
    ) -> Result<PerItem<ConfigResource, Vec<ConfigEntry>>> {
        self.describe_configs_inner(resources, false).await
    }

    /// Describe configurations, including the broker's own documentation.
    ///
    /// Separate because documentation roughly triples the response size and a
    /// list view does not need it.
    pub async fn describe_configs_documented(
        &self,
        resources: impl IntoIterator<Item = ConfigResource>,
    ) -> Result<PerItem<ConfigResource, Vec<ConfigEntry>>> {
        self.describe_configs_inner(resources, true).await
    }

    async fn describe_configs_inner(
        &self,
        resources: impl IntoIterator<Item = ConfigResource>,
        documentation: bool,
    ) -> Result<PerItem<ConfigResource, Vec<ConfigEntry>>> {
        let resources: Vec<ConfigResource> = resources.into_iter().collect();
        if resources.is_empty() {
            return Ok(Vec::new());
        }

        let request = DescribeConfigsRequest::default()
            .with_include_synonyms(false)
            .with_include_documentation(documentation)
            .with_resources(
                resources
                    .iter()
                    .map(|resource| {
                        DescribeConfigsResource::default()
                            .with_resource_type(resource.resource_type.code())
                            .with_resource_name(StrBytes::from_string(resource.name.clone()))
                            .with_configuration_keys(None)
                    })
                    .collect(),
            );

        // A broker config describes *that broker*, so asking any broker about
        // node 3's configuration answers about node 3 only if the request is
        // routed there. Kafka forwards it, but routing it directly is one less
        // hop and one less way to get a stale answer.
        let response = match single_broker_target(&resources) {
            Some(node_id) => self.cluster().send_to_node(node_id, request).await?,
            None => self.cluster().send_any(request).await?,
        };

        Ok(response
            .results
            .into_iter()
            .map(|result| {
                let resource = ConfigResource {
                    resource_type: ConfigResourceType::from_code(result.resource_type)
                        .unwrap_or(ConfigResourceType::Topic),
                    name: result.resource_name.to_string(),
                };
                let outcome = match ErrorCode::from_code(result.error_code) {
                    Some(code) => Err(Error::from_code(
                        code,
                        result.error_message.map(|m| m.to_string()),
                    )),
                    None => Ok(result
                        .configs
                        .into_iter()
                        .map(|config| ConfigEntry {
                            name: config.name.to_string(),
                            value: config.value.map(|v| v.to_string()),
                            source: ConfigSource::from_code(config.config_source),
                            is_sensitive: config.is_sensitive,
                            read_only: config.read_only,
                            documentation: config.documentation.map(|d| d.to_string()),
                        })
                        .collect()),
                };
                (resource, outcome)
            })
            .collect())
    }

    /// Alter configurations incrementally.
    pub async fn alter_configs(
        &self,
        changes: impl IntoIterator<Item = (ConfigResource, Vec<ConfigChange>)>,
    ) -> Result<PerItem<ConfigResource, ()>> {
        self.alter_configs_inner(changes, false).await
    }

    /// Check what `alter_configs` would do without doing it.
    pub async fn validate_config_changes(
        &self,
        changes: impl IntoIterator<Item = (ConfigResource, Vec<ConfigChange>)>,
    ) -> Result<PerItem<ConfigResource, ()>> {
        self.alter_configs_inner(changes, true).await
    }

    async fn alter_configs_inner(
        &self,
        changes: impl IntoIterator<Item = (ConfigResource, Vec<ConfigChange>)>,
        validate_only: bool,
    ) -> Result<PerItem<ConfigResource, ()>> {
        let changes: Vec<(ConfigResource, Vec<ConfigChange>)> = changes.into_iter().collect();
        if changes.is_empty() {
            return Ok(Vec::new());
        }

        let request = IncrementalAlterConfigsRequest::default()
            .with_validate_only(validate_only)
            .with_resources(
                changes
                    .iter()
                    .map(|(resource, edits)| {
                        AlterConfigsResource::default()
                            .with_resource_type(resource.resource_type.code())
                            .with_resource_name(StrBytes::from_string(resource.name.clone()))
                            .with_configs(
                                edits
                                    .iter()
                                    .map(|edit| {
                                        AlterableConfig::default()
                                            .with_name(StrBytes::from_string(edit.name.clone()))
                                            .with_config_operation(edit.op.code())
                                            .with_value(
                                                edit.value
                                                    .as_ref()
                                                    .map(|v| StrBytes::from_string(v.clone())),
                                            )
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            );

        let resources: Vec<ConfigResource> = changes
            .iter()
            .map(|(resource, _)| resource.clone())
            .collect();
        let response = match single_broker_target(&resources) {
            Some(node_id) => self.cluster().send_to_node(node_id, request).await?,
            None => self.cluster().send_any(request).await?,
        };

        Ok(response
            .responses
            .into_iter()
            .map(|result| {
                let resource = ConfigResource {
                    resource_type: ConfigResourceType::from_code(result.resource_type)
                        .unwrap_or(ConfigResourceType::Topic),
                    name: result.resource_name.to_string(),
                };
                let outcome = match ErrorCode::from_code(result.error_code) {
                    Some(code) => Err(Error::from_code(
                        code,
                        result.error_message.map(|m| m.to_string()),
                    )),
                    None => Ok(()),
                };
                (resource, outcome)
            })
            .collect())
    }
}

/// If every resource is the same broker, the request belongs on that broker.
fn single_broker_target(resources: &[ConfigResource]) -> Option<i32> {
    let mut target = None;
    for resource in resources {
        if !matches!(
            resource.resource_type,
            ConfigResourceType::Broker | ConfigResourceType::BrokerLogger
        ) {
            return None;
        }
        let node_id: i32 = resource.name.parse().ok()?;
        match target {
            None => target = Some(node_id),
            Some(existing) if existing == node_id => {}
            Some(_) => return None,
        }
    }
    target
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_configs_route_to_that_broker() {
        assert_eq!(single_broker_target(&[ConfigResource::broker(3)]), Some(3));
        assert_eq!(
            single_broker_target(&[ConfigResource::broker(3), ConfigResource::broker(3)]),
            Some(3)
        );
    }

    #[test]
    fn a_mixed_or_multi_broker_batch_goes_anywhere() {
        assert_eq!(
            single_broker_target(&[ConfigResource::broker(3), ConfigResource::broker(4)]),
            None
        );
        assert_eq!(
            single_broker_target(&[ConfigResource::broker(3), ConfigResource::topic("orders")]),
            None
        );
        assert_eq!(
            single_broker_target(&[ConfigResource::topic("orders")]),
            None
        );
    }

    #[test]
    fn a_cluster_wide_broker_default_is_not_one_broker() {
        // The empty name means "the dynamic default for all brokers"; it does
        // not parse as a node id and must not be routed to one.
        let resource = ConfigResource {
            resource_type: ConfigResourceType::Broker,
            name: String::new(),
        };
        assert_eq!(single_broker_target(&[resource]), None);
    }
}
