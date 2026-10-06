// SPDX-FileCopyrightText: 2025 Stackable GmbH
// SPDX-License-Identifier: Apache-2.0

use super::MigrationRule;
use serde_json::Value;

/// Migration rule for JoltTransformJSON processor.
///
/// Migrates `org.apache.nifi.processors.standard.JoltTransformJSON` to
/// `org.apache.nifi.processors.jolt.JoltTransformJSON` and updates the
/// bundle from `nifi-standard-nar` to `nifi-jolt-nar`.
///
/// Also migrates property names:
/// - `jolt-spec` → `Jolt Specification`
/// - `jolt-transform` → `Jolt Transform`
/// - `pretty_print` → `Pretty Print`
/// - `jolt-custom-class` → `Custom Transformation Class Name`
/// - `jolt-custom-modules` → `Custom Module Directory`
///
/// Reference: <https://issues.apache.org/jira/browse/NIFI-12554>
pub struct JoltTransformJsonMigration;

const PROPERTY_MIGRATIONS: [(&str, &str); 5] = [
    ("jolt-spec", "Jolt Specification"),
    ("jolt-transform", "Jolt Transform"),
    ("pretty_print", "Pretty Print"),
    ("jolt-custom-class", "Custom Transformation Class Name"),
    ("jolt-custom-modules", "Custom Module Directory"),
];

impl MigrationRule for JoltTransformJsonMigration {
    fn applies(&self, processor: &Value) -> bool {
        processor
            .get("type")
            .and_then(|t| t.as_str())
            .map(|t| t == "org.apache.nifi.processors.standard.JoltTransformJSON")
            .unwrap_or(false)
    }

    fn apply(&self, processor: &mut Value) -> bool {
        let mut changed = false;

        // Update the type field
        if let Some(type_field) = processor.get_mut("type")
            && type_field.as_str() == Some("org.apache.nifi.processors.standard.JoltTransformJSON")
        {
            *type_field =
                Value::String("org.apache.nifi.processors.jolt.JoltTransformJSON".to_owned());
            changed = true;
        }

        // Update the bundle artifact field
        if let Some(bundle) = processor.get_mut("bundle")
            && let Some(artifact) = bundle.get_mut("artifact")
            && artifact.as_str() == Some("nifi-standard-nar")
        {
            *artifact = Value::String("nifi-jolt-nar".to_owned());
            changed = true;
        }

        // Migrate properties: rename old property keys to new ones
        if let Some(properties) = processor
            .get_mut("properties")
            .and_then(|p| p.as_object_mut())
        {
            for (old_name, new_name) in PROPERTY_MIGRATIONS {
                if let Some(value) = properties.remove(old_name) {
                    properties.insert(new_name.to_owned(), value);
                    changed = true;
                }
            }
        }

        // Migrate propertyDescriptors: rename old property descriptor keys to new ones
        if let Some(descriptors) = processor
            .get_mut("propertyDescriptors")
            .and_then(|p| p.as_object_mut())
        {
            for (old_name, new_name) in PROPERTY_MIGRATIONS {
                if let Some(mut descriptor) = descriptors.remove(old_name) {
                    // Update the name and displayName fields within the descriptor
                    if let Some(descriptor_obj) = descriptor.as_object_mut() {
                        descriptor_obj
                            .insert("name".to_owned(), Value::String(new_name.to_owned()));
                        descriptor_obj
                            .insert("displayName".to_owned(), Value::String(new_name.to_owned()));
                    }
                    descriptors.insert(new_name.to_owned(), descriptor);
                    changed = true;
                }
            }
        }

        changed
    }

    fn description(&self) -> String {
        "Migrate JoltTransformJSON from standard to jolt bundle".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn test_jolt_transform_migration() {
        let mut processor = json!({
            "identifier": "test-id-123",
            "name": "JoltTransform",
            "type": "org.apache.nifi.processors.standard.JoltTransformJSON",
            "bundle": {
                "artifact": "nifi-standard-nar",
                "group": "org.apache.nifi",
                "version": "1.25.0"
            }
        });

        let rule = JoltTransformJsonMigration;
        assert!(rule.applies(&processor));
        assert!(rule.apply(&mut processor));

        assert_eq!(
            processor.get("type").and_then(|v| v.as_str()),
            Some("org.apache.nifi.processors.jolt.JoltTransformJSON")
        );
        assert_eq!(
            processor
                .get("bundle")
                .and_then(|b| b.get("artifact"))
                .and_then(|v| v.as_str()),
            Some("nifi-jolt-nar")
        );
    }

    #[test]
    fn test_full_real_world_processor() {
        let mut processor = json!({
            "identifier": "347aaa3b-2b3a-30c5-932d-58a109f9478f",
            "instanceIdentifier": "e70d7d94-e92a-3472-b1c7-338e291af5e9",
            "name": "JoltTransformJSON",
            "comments": "",
            "position": {
                "x": 3368.0,
                "y": 200.0
            },
            "type": "org.apache.nifi.processors.standard.JoltTransformJSON",
            "bundle": {
                "group": "org.apache.nifi",
                "artifact": "nifi-standard-nar",
                "version": "1.18.0"
            },
            "properties": {},
            "propertyDescriptors": {
                "jolt-spec": {
                    "name": "jolt-spec",
                    "displayName": "jolt-spec",
                    "identifiesControllerService": false,
                    "sensitive": true,
                    "dynamic": false
                },
                "jolt-transform": {
                    "name": "jolt-transform",
                    "displayName": "jolt-transform",
                    "identifiesControllerService": false,
                    "sensitive": true,
                    "dynamic": false
                },
                "pretty_print": {
                    "name": "pretty_print",
                    "displayName": "pretty_print",
                    "identifiesControllerService": false,
                    "sensitive": true,
                    "dynamic": false
                },
                "Transform Cache Size": {
                    "name": "Transform Cache Size",
                    "displayName": "Transform Cache Size",
                    "identifiesControllerService": false,
                    "sensitive": true,
                    "dynamic": false
                }
            },
            "style": {},
            "schedulingPeriod": "0 sec",
            "schedulingStrategy": "TIMER_DRIVEN",
            "executionNode": "ALL",
            "penaltyDuration": "30 sec",
            "yieldDuration": "1 sec",
            "bulletinLevel": "WARN",
            "runDurationMillis": 0,
            "concurrentlySchedulableTaskCount": 1,
            "autoTerminatedRelationships": ["failure"],
            "scheduledState": "ENABLED",
            "retryCount": 10,
            "retriedRelationships": [],
            "backoffMechanism": "PENALIZE_FLOWFILE",
            "maxBackoffPeriod": "10 mins",
            "componentType": "PROCESSOR",
            "groupIdentifier": "5e61ca9d-43f8-3176-b706-2404009bcc5b"
        });

        let rule = JoltTransformJsonMigration;

        // Verify it applies to this processor
        assert!(
            rule.applies(&processor),
            "Rule should apply to JoltTransformJSON processor"
        );

        // Apply the migration
        assert!(rule.apply(&mut processor), "Migration should make changes");

        // Verify the type was changed
        assert_eq!(
            processor.get("type").and_then(|v| v.as_str()),
            Some("org.apache.nifi.processors.jolt.JoltTransformJSON"),
            "Processor type should be updated"
        );

        // Verify the bundle artifact was changed
        assert_eq!(
            processor
                .get("bundle")
                .and_then(|b| b.get("artifact"))
                .and_then(|v| v.as_str()),
            Some("nifi-jolt-nar"),
            "Bundle artifact should be updated to nifi-jolt-nar"
        );

        // Verify other bundle fields remain unchanged
        assert_eq!(
            processor
                .get("bundle")
                .and_then(|b| b.get("group"))
                .and_then(|v| v.as_str()),
            Some("org.apache.nifi"),
            "Bundle group should remain unchanged"
        );
        assert_eq!(
            processor
                .get("bundle")
                .and_then(|b| b.get("version"))
                .and_then(|v| v.as_str()),
            Some("1.18.0"),
            "Bundle version should remain unchanged"
        );

        // Verify other fields remain unchanged
        assert_eq!(
            processor.get("identifier").and_then(|v| v.as_str()),
            Some("347aaa3b-2b3a-30c5-932d-58a109f9478f"),
            "Identifier should remain unchanged"
        );
        assert_eq!(
            processor.get("name").and_then(|v| v.as_str()),
            Some("JoltTransformJSON"),
            "Name should remain unchanged"
        );
    }

    #[test]
    fn test_property_migrations() {
        let mut processor = json!({
            "identifier": "test-id-123",
            "name": "JoltTransform",
            "type": "org.apache.nifi.processors.standard.JoltTransformJSON",
            "bundle": {
                "artifact": "nifi-standard-nar",
                "group": "org.apache.nifi",
                "version": "1.27.0"
            },
            "properties": {
                "jolt-spec": "[{\"operation\": \"shift\"}]",
                "jolt-transform": "jolt-transform-chain",
                "pretty_print": "false",
                "jolt-custom-class": "com.example.CustomTransform",
                "jolt-custom-modules": "/path/to/modules",
                "Other Property": "should remain"
            },
            "propertyDescriptors": {
                "jolt-spec": {
                    "name": "jolt-spec",
                    "displayName": "jolt-spec",
                    "identifiesControllerService": false,
                    "sensitive": false,
                    "dynamic": false
                },
                "jolt-transform": {
                    "name": "jolt-transform",
                    "displayName": "jolt-transform",
                    "identifiesControllerService": false,
                    "sensitive": false,
                    "dynamic": false
                },
                "pretty_print": {
                    "name": "pretty_print",
                    "displayName": "pretty_print",
                    "identifiesControllerService": false,
                    "sensitive": false,
                    "dynamic": false
                }
            }
        });

        let rule = JoltTransformJsonMigration;
        assert!(rule.applies(&processor));
        assert!(rule.apply(&mut processor));

        // Verify properties were migrated
        let properties = processor.get("properties").unwrap();
        assert_eq!(
            properties
                .get("Jolt Specification")
                .and_then(|v| v.as_str()),
            Some("[{\"operation\": \"shift\"}]"),
            "jolt-spec should be migrated to Jolt Specification"
        );
        assert_eq!(
            properties.get("Jolt Transform").and_then(|v| v.as_str()),
            Some("jolt-transform-chain"),
            "jolt-transform should be migrated to Jolt Transform"
        );
        assert_eq!(
            properties.get("Pretty Print").and_then(|v| v.as_str()),
            Some("false"),
            "pretty_print should be migrated to Pretty Print"
        );
        assert_eq!(
            properties
                .get("Custom Transformation Class Name")
                .and_then(|v| v.as_str()),
            Some("com.example.CustomTransform"),
            "jolt-custom-class should be migrated to Custom Transformation Class Name"
        );
        assert_eq!(
            properties
                .get("Custom Module Directory")
                .and_then(|v| v.as_str()),
            Some("/path/to/modules"),
            "jolt-custom-modules should be migrated to Custom Module Directory"
        );

        // Verify old property names are removed
        assert!(
            properties.get("jolt-spec").is_none(),
            "Old property name jolt-spec should be removed"
        );
        assert!(
            properties.get("jolt-transform").is_none(),
            "Old property name jolt-transform should be removed"
        );
        assert!(
            properties.get("pretty_print").is_none(),
            "Old property name pretty_print should be removed"
        );

        // Verify other properties remain unchanged
        assert_eq!(
            properties.get("Other Property").and_then(|v| v.as_str()),
            Some("should remain"),
            "Other properties should remain unchanged"
        );

        // Verify propertyDescriptors were migrated
        let descriptors = processor.get("propertyDescriptors").unwrap();
        let jolt_spec_descriptor = descriptors.get("Jolt Specification").unwrap();
        assert_eq!(
            jolt_spec_descriptor.get("name").and_then(|v| v.as_str()),
            Some("Jolt Specification"),
            "Descriptor name should be updated"
        );
        assert_eq!(
            jolt_spec_descriptor
                .get("displayName")
                .and_then(|v| v.as_str()),
            Some("Jolt Specification"),
            "Descriptor displayName should be updated"
        );

        // Verify old descriptor keys are removed
        assert!(
            descriptors.get("jolt-spec").is_none(),
            "Old descriptor key jolt-spec should be removed"
        );
        assert!(
            descriptors.get("jolt-transform").is_none(),
            "Old descriptor key jolt-transform should be removed"
        );
        assert!(
            descriptors.get("pretty_print").is_none(),
            "Old descriptor key pretty_print should be removed"
        );
    }

    #[test]
    fn test_migration_with_missing_properties() {
        let mut processor = json!({
            "identifier": "test-id-123",
            "name": "JoltTransform",
            "type": "org.apache.nifi.processors.standard.JoltTransformJSON",
            "bundle": {
                "artifact": "nifi-standard-nar",
                "group": "org.apache.nifi",
                "version": "1.27.0"
            },
            "properties": {
                "jolt-spec": "[{\"operation\": \"shift\"}]"
            }
        });

        let rule = JoltTransformJsonMigration;
        assert!(rule.applies(&processor));
        assert!(rule.apply(&mut processor));

        // Verify the property that exists was migrated
        let properties = processor.get("properties").unwrap();
        assert_eq!(
            properties
                .get("Jolt Specification")
                .and_then(|v| v.as_str()),
            Some("[{\"operation\": \"shift\"}]"),
            "jolt-spec should be migrated"
        );

        // Verify old property name is removed
        assert!(
            properties.get("jolt-spec").is_none(),
            "Old property name should be removed"
        );
    }
}
