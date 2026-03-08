use std::fmt::{Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use ulid::Ulid;

#[derive(Debug, Error)]
pub enum IdError {
    #[error("identifier must not be empty")]
    Empty,
}

macro_rules! define_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(IdError::Empty);
                }
                Ok(Self(value))
            }

            #[must_use]
            pub fn generate() -> Self {
                Self(format!("{}_{}", $prefix, Ulid::new()))
            }

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

define_id!(ActorId, "actor");
define_id!(ArtifactId, "art");
define_id!(EvalCertId, "eval");
define_id!(KeyId, "key");
define_id!(MessageId, "msg");
define_id!(NodeId, "node");
define_id!(ProjectId, "proj");
define_id!(SnapshotId, "snap");
define_id!(StopId, "stop");
define_id!(TaskId, "task");
define_id!(VisionId, "vis");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_use_prefix() {
        let actor_id = ActorId::generate();
        assert!(actor_id.as_str().starts_with("actor_"));
    }
}
