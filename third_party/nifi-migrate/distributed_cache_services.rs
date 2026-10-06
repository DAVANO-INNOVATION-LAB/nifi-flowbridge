// SPDX-FileCopyrightText: 2025 Stackable GmbH
// SPDX-License-Identifier: Apache-2.0

use super::MigrationRule;
use serde_json::Value;

/// Migration rule for Distributed Cache controller services.
///
/// Migrates all Distributed Cache services by removing "Distributed" from their class names.
/// This includes both client and server services for Map and Set caches.
///
/// Migrations:
/// - `DistributedMapCacheClientService` → `MapCacheClientService`
/// - `DistributedSetCacheClientService` → `SetCacheClientService`
/// - `DistributedMapCacheServer` → `MapCacheServer`
/// - `DistributedSetCacheServer` → `SetCacheServer`
///
/// All services remain in the same bundle: `nifi-distributed-cache-services-nar`.
///
/// Reference: <https://issues.apache.org/jira/browse/NIFI-13596>
pub struct DistributedCacheServicesMigration;

const CACHE_SERVICE_MIGRATIONS: [(&str, &str); 4] = [
    (
        "org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService",
        "org.apache.nifi.distributed.cache.client.MapCacheClientService",
    ),
    (
        "org.apache.nifi.distributed.cache.client.DistributedSetCacheClientService",
        "org.apache.nifi.distributed.cache.client.SetCacheClientService",
    ),
    (
        "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        "org.apache.nifi.distributed.cache.server.map.MapCacheServer",
    ),
    (
        "org.apache.nifi.distributed.cache.server.set.DistributedSetCacheServer",
        "org.apache.nifi.distributed.cache.server.set.SetCacheServer",
    ),
];

impl MigrationRule for DistributedCacheServicesMigration {
    fn applies(&self, component: &Value) -> bool {
        if let Some(type_str) = component.get("type").and_then(|t| t.as_str()) {
            CACHE_SERVICE_MIGRATIONS
                .iter()
                .any(|(old_type, _)| type_str == *old_type)
        } else {
            false
        }
    }

    fn apply(&self, component: &mut Value) -> bool {
        let mut changed = false;

        if let Some(type_field) = component.get_mut("type")
            && let Some(current_type) = type_field.as_str()
        {
            for (old_type, new_type) in CACHE_SERVICE_MIGRATIONS {
                if current_type == old_type {
                    *type_field = Value::String(new_type.to_string());
                    changed = true;
                    break;
                }
            }
        }

        changed
    }

    fn description(&self) -> String {
        "Migrate Distributed Cache services (remove 'Distributed' prefix)".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn test_map_cache_client_migration() {
        let mut service = json!({
            "identifier": "test-service-123",
            "name": "MapCacheClientService",
            "type": "org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService",
            "bundle": {
                "group": "org.apache.nifi",
                "artifact": "nifi-distributed-cache-services-nar",
                "version": "1.27.0"
            }
        });

        let rule = DistributedCacheServicesMigration;
        assert!(rule.applies(&service));
        assert!(rule.apply(&mut service));

        assert_eq!(
            service.get("type").and_then(|v| v.as_str()),
            Some("org.apache.nifi.distributed.cache.client.MapCacheClientService")
        );
    }

    #[test]
    fn test_set_cache_client_migration() {
        let mut service = json!({
            "identifier": "test-service-456",
            "name": "SetCacheClientService",
            "type": "org.apache.nifi.distributed.cache.client.DistributedSetCacheClientService",
            "bundle": {
                "group": "org.apache.nifi",
                "artifact": "nifi-distributed-cache-services-nar",
                "version": "1.27.0"
            }
        });

        let rule = DistributedCacheServicesMigration;
        assert!(rule.applies(&service));
        assert!(rule.apply(&mut service));

        assert_eq!(
            service.get("type").and_then(|v| v.as_str()),
            Some("org.apache.nifi.distributed.cache.client.SetCacheClientService")
        );
    }

    #[test]
    fn test_map_cache_server_migration() {
        let mut service = json!({
            "identifier": "test-server-123",
            "name": "MapCacheServer",
            "type": "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
            "bundle": {
                "group": "org.apache.nifi",
                "artifact": "nifi-distributed-cache-services-nar",
                "version": "1.27.0"
            }
        });

        let rule = DistributedCacheServicesMigration;
        assert!(rule.applies(&service));
        assert!(rule.apply(&mut service));

        assert_eq!(
            service.get("type").and_then(|v| v.as_str()),
            Some("org.apache.nifi.distributed.cache.server.map.MapCacheServer")
        );
    }

    #[test]
    fn test_set_cache_server_migration() {
        let mut service = json!({
            "identifier": "test-server-456",
            "name": "SetCacheServer",
            "type": "org.apache.nifi.distributed.cache.server.set.DistributedSetCacheServer",
            "bundle": {
                "group": "org.apache.nifi",
                "artifact": "nifi-distributed-cache-services-nar",
                "version": "1.27.0"
            }
        });

        let rule = DistributedCacheServicesMigration;
        assert!(rule.applies(&service));
        assert!(rule.apply(&mut service));

        assert_eq!(
            service.get("type").and_then(|v| v.as_str()),
            Some("org.apache.nifi.distributed.cache.server.set.SetCacheServer")
        );
    }

    #[test]
    fn test_does_not_apply_to_other_services() {
        let service = json!({
            "identifier": "other-service",
            "name": "SomeOtherService",
            "type": "org.apache.nifi.other.SomeService",
            "bundle": {
                "artifact": "nifi-standard-nar"
            }
        });

        let rule = DistributedCacheServicesMigration;
        assert!(!rule.applies(&service));
    }

    #[test]
    fn test_with_full_controller_service_structure() {
        let mut service = json!({
            "identifier": "d9a5b110-2bb0-3b18-866b-6660d12f0bc1",
            "instanceIdentifier": "cfe54b26-0199-1000-ffff-ffffd8c8433f",
            "name": "MapCacheClientService",
            "comments": "",
            "type": "org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService",
            "bundle": {
                "group": "org.apache.nifi",
                "artifact": "nifi-distributed-cache-services-nar",
                "version": "1.27.0"
            },
            "properties": {
                "SSL Context Service": null,
                "Server Port": "4557",
                "Server Hostname": "localhost",
                "Communications Timeout": "30 secs"
            },
            "propertyDescriptors": {
                "SSL Context Service": {
                    "name": "SSL Context Service",
                    "displayName": "SSL Context Service",
                    "identifiesControllerService": true,
                    "sensitive": false,
                    "dynamic": false
                }
            },
            "controllerServiceApis": [
                {
                    "type": "org.apache.nifi.distributed.cache.client.AtomicDistributedMapCacheClient",
                    "bundle": {
                        "group": "org.apache.nifi",
                        "artifact": "nifi-standard-services-api-nar",
                        "version": "1.27.0"
                    }
                }
            ],
            "scheduledState": "DISABLED",
            "bulletinLevel": "WARN",
            "componentType": "CONTROLLER_SERVICE"
        });

        let rule = DistributedCacheServicesMigration;
        assert!(rule.applies(&service));
        assert!(rule.apply(&mut service));

        // Verify the type was changed
        assert_eq!(
            service.get("type").and_then(|v| v.as_str()),
            Some("org.apache.nifi.distributed.cache.client.MapCacheClientService")
        );

        // Verify other fields remain unchanged
        assert_eq!(
            service.get("identifier").and_then(|v| v.as_str()),
            Some("d9a5b110-2bb0-3b18-866b-6660d12f0bc1")
        );
        assert_eq!(
            service.get("componentType").and_then(|v| v.as_str()),
            Some("CONTROLLER_SERVICE")
        );
    }
}
