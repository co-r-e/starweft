//! ULID-based typed identifiers for all Starweft domain entities.
//!
//! Each ID type is a newtype wrapper around a prefixed ULID string
//! (e.g. `"actor_01J..."`, `"task_01J..."`), providing type-safe
//! identifiers that are sortable, serializable, and human-readable.

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

/// Errors that can occur when constructing an identifier.
#[derive(Debug, Error)]
pub enum IdError {
    /// The provided identifier string was empty or blank.
    #[error("identifier must not be empty")]
    Empty,
}

macro_rules! define_id {
    ($(#[$meta:meta])* $name:ident, $prefix:literal) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Creates an ID from an existing string value.
            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(IdError::Empty);
                }
                Ok(Self(value))
            }

            /// Generates a new unique ID with the type-specific prefix.
            #[must_use]
            pub fn generate() -> Self {
                Self(format!("{}_{}", $prefix, Ulid::new()))
            }

            /// Returns the underlying string representation.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }
    };
}

define_id!(
    /// Unique identifier for an actor (agent or human principal).
    ActorId, "actor"
);
define_id!(
    /// Unique identifier for a stored artifact.
    ArtifactId, "art"
);
define_id!(
    /// Unique identifier for an evaluation certificate.
    EvalCertId, "eval"
);
define_id!(
    /// Unique identifier for a cryptographic keypair.
    KeyId, "key"
);
define_id!(
    /// Unique identifier for a protocol message.
    MessageId, "msg"
);
define_id!(
    /// Unique identifier for a network node.
    NodeId, "node"
);
define_id!(
    /// Unique identifier for a project.
    ProjectId, "proj"
);
define_id!(
    /// Unique identifier for a state snapshot.
    SnapshotId, "snap"
);
define_id!(
    /// Unique identifier for a stop order.
    StopId, "stop"
);
define_id!(
    /// Unique identifier for a delegated task.
    TaskId, "task"
);
define_id!(
    /// Unique identifier for a vision intent.
    VisionId, "vis"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_use_prefix() {
        let actor_id = ActorId::generate();
        assert!(actor_id.as_str().starts_with("actor_"));
    }
}
