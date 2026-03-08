use serde::{Deserialize, Serialize};
use starweft_protocol::StopOrder;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StopTransition {
    pub accepted: bool,
    pub order: StopOrder,
}
