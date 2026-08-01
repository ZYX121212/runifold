use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! define_id {
    ($name:ident, $docs:literal) => {
        #[doc = $docs]
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates a time-ordered `UUIDv7` identifier.
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Creates an identifier from an existing UUID.
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            /// Returns the underlying UUID.
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self)
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self::from_uuid(value)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.as_uuid()
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

define_id!(RunId, "A globally unique run identifier.");
define_id!(EventId, "A globally unique event identifier.");
define_id!(EffectId, "A globally unique effect identifier.");
define_id!(CapabilityId, "A globally unique capability identifier.");
define_id!(InvocationId, "A globally unique invocation identifier.");
define_id!(CheckpointId, "A globally unique checkpoint identifier.");

#[cfg(test)]
mod tests {
    use super::{CapabilityId, RunId};

    #[test]
    fn generated_ids_are_distinct_and_v7() {
        let first = RunId::new();
        let second = RunId::new();

        assert_ne!(first, second);
        assert_eq!(first.as_uuid().get_version_num(), 7);
    }

    #[test]
    fn stable_ids_round_trip_through_configuration_strings() {
        let configured = "018f6f7e-6f1d-7f2a-9c40-7f4f8f0a3d21";

        let parsed: CapabilityId = configured.parse().expect("configured UUID is valid");

        assert_eq!(parsed.to_string(), configured);
    }
}
