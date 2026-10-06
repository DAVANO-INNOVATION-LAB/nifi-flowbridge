// SPDX-FileCopyrightText: 2025 Stackable GmbH
// SPDX-License-Identifier: Apache-2.0

use super::MigrationRule;
use serde_json::Value;

/// Migration rule for JoltTransformRecord processor.
///
/// Migrates `org.apache.nifi.processors.jolt.record.JoltTransformRecord` to
/// `org.apache.nifi.processors.jolt.JoltTransformRecord` and updates the
/// bundle from `nifi-jolt-record-nar` to `nifi-jolt-nar`.
///
/// Also migrates property names:
/// - `jolt-record-transform` → `Jolt Transform`
/// - `jolt-record-spec` → `Jolt Specification`
/// - `jolt-record-custom-class` → `Custom Transformation Class Name`
/// - `jolt-record-custom-modules` → `Custom Module Directory`
/// - `jolt-record-transform-cache-size` → `Transform Cache Size`
///
/// Reference: <https://issues.apache.org/jira/browse/NIFI-12554>
pub struct JoltTransformRecordMigration;

const PROPERTY_MIGRATIONS: [(&str, &str); 5] = [
    ("jolt-record-transform", "Jolt Transform"),
    ("jolt-record-spec", "Jolt Specification"),
    (
        "jolt-record-custom-class",
        "Custom Transformation Class Name",
    ),
    ("jolt-record-custom-modules", "Custom Module Directory"),
    ("jolt-record-transform-cache-size", "Transform Cache Size"),
];

impl MigrationRule for JoltTransformRecordMigration {
    fn applies(&self, processor: &Value) -> bool {
        processor
            .get("type")
            .and_then(|t| t.as_str())
            .map(|t| t == "org.apache.nifi.processors.jolt.record.JoltTransformRecord")
            .unwrap_or(false)
    }

    fn apply(&self, processor: &mut Value) -> bool {
        let mut changed = false;

        // Update the type field
        if let Some(type_field) = processor.get_mut("type")
            && type_field.as_str()
                == Some("org.apache.nifi.processors.jolt.record.JoltTransformRecord")
        {
            *type_field =
                Value::String("org.apache.nifi.processors.jolt.JoltTransformRecord".to_owned());
            changed = true;
        }

        // Update the bundle artifact field
        if let Some(bundle) = processor.get_mut("bundle")
            && let Some(artifact) = bundle.get_mut("artifact")
            && artifact.as_str() == Some("nifi-jolt-record-nar")
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
        "Migrate JoltTransformRecord from jolt-record to jolt bundle".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn test_jolt_transform_record_migration() {
        let mut processor = json!({
            "identifier": "test-id-456",
            "name": "JoltTransformRecord",
            "type": "org.apache.nifi.processors.jolt.record.JoltTransformRecord",
            "bundle": {
                "artifact": "nifi-jolt-record-nar",
                "group": "org.apache.nifi",
                "version": "1.27.0"
            }
        });

        let rule = JoltTransformRecordMigration;
        assert!(rule.applies(&processor));
        assert!(rule.apply(&mut processor));

        assert_eq!(
            processor.get("type").and_then(|v| v.as_str()),
            Some("org.apache.nifi.processors.jolt.JoltTransformRecord")
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
    fn test_does_not_apply_to_other_processors() {
        let processor = json!({
            "identifier": "other-proc",
            "name": "SomeOtherProcessor",
            "type": "org.apache.nifi.processors.standard.LogAttribute",
            "bundle": {
                "artifact": "nifi-standard-nar"
            }
        });

        let rule = JoltTransformRecordMigration;
        assert!(!rule.applies(&processor));
    }

    #[test]
    fn test_property_migrations() {
        let mut processor = json!({
            "identifier": "test-id-789",
            "name": "JoltRecord",
            "type": "org.apache.nifi.processors.jolt.record.JoltTransformRecord",
            "bundle": {
                "artifact": "nifi-jolt-record-nar",
                "group": "org.apache.nifi",
                "version": "1.27.0"
            },
            "properties": {
                "jolt-record-transform": "jolt-transform-chain",
                "jolt-record-spec": "[{\"operation\": \"shift\"}]",
                "jolt-record-custom-class": "com.example.CustomTransform",
                "jolt-record-custom-modules": "/path/to/modules",
                "jolt-record-transform-cache-size": "10",
                "jolt-record-record-reader": "reader-service-id",
                "jolt-record-record-writer": "writer-service-id"
            },
            "propertyDescriptors": {
                "jolt-record-transform": {
                    "name": "jolt-record-transform",
                    "displayName": "Jolt Transformation DSL",
                    "identifiesControllerService": false,
                    "sensitive": false,
                    "dynamic": false
                },
                "jolt-record-spec": {
                    "name": "jolt-record-spec",
                    "displayName": "Jolt Specification",
                    "identifiesControllerService": false,
                    "sensitive": false,
                    "dynamic": false
                }
            }
        });

        let rule = JoltTransformRecordMigration;
        assert!(rule.applies(&processor));
        assert!(rule.apply(&mut processor));

        // Verify properties were migrated
        let properties = processor.get("properties").unwrap();
        assert_eq!(
            properties.get("Jolt Transform").and_then(|v| v.as_str()),
            Some("jolt-transform-chain"),
            "jolt-record-transform should be migrated to Jolt Transform"
        );
        assert_eq!(
            properties
                .get("Jolt Specification")
                .and_then(|v| v.as_str()),
            Some("[{\"operation\": \"shift\"}]"),
            "jolt-record-spec should be migrated to Jolt Specification"
        );
        assert_eq!(
            properties
                .get("Custom Transformation Class Name")
                .and_then(|v| v.as_str()),
            Some("com.example.CustomTransform"),
            "jolt-record-custom-class should be migrated"
        );
        assert_eq!(
            properties
                .get("Custom Module Directory")
                .and_then(|v| v.as_str()),
            Some("/path/to/modules"),
            "jolt-record-custom-modules should be migrated"
        );
        assert_eq!(
            properties
                .get("Transform Cache Size")
                .and_then(|v| v.as_str()),
            Some("10"),
            "jolt-record-transform-cache-size should be migrated"
        );

        // Verify old property names are removed
        assert!(
            properties.get("jolt-record-transform").is_none(),
            "Old property name should be removed"
        );
        assert!(
            properties.get("jolt-record-spec").is_none(),
            "Old property name should be removed"
        );

        // Verify properties that should NOT be migrated remain unchanged
        assert_eq!(
            properties
                .get("jolt-record-record-reader")
                .and_then(|v| v.as_str()),
            Some("reader-service-id"),
            "jolt-record-record-reader should remain unchanged"
        );
        assert_eq!(
            properties
                .get("jolt-record-record-writer")
                .and_then(|v| v.as_str()),
            Some("writer-service-id"),
            "jolt-record-record-writer should remain unchanged"
        );

        // Verify propertyDescriptors were migrated
        let descriptors = processor.get("propertyDescriptors").unwrap();
        let transform_descriptor = descriptors.get("Jolt Transform").unwrap();
        assert_eq!(
            transform_descriptor.get("name").and_then(|v| v.as_str()),
            Some("Jolt Transform"),
            "Descriptor name should be updated"
        );
        assert_eq!(
            transform_descriptor
                .get("displayName")
                .and_then(|v| v.as_str()),
            Some("Jolt Transform"),
            "Descriptor displayName should be updated"
        );

        // Verify old descriptor keys are removed
        assert!(
            descriptors.get("jolt-record-transform").is_none(),
            "Old descriptor key should be removed"
        );
        assert!(
            descriptors.get("jolt-record-spec").is_none(),
            "Old descriptor key should be removed"
        );
    }
}
