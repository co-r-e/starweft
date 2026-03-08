use serde::{Deserialize, Serialize};
use starweft_id::ProjectId;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectSnapshotCache {
    pub project_id: ProjectId,
    pub snapshot_json: String,
}
