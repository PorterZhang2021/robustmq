use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub cluster_uid: String,
    pub cluster_name: String,
    pub cluster_type: String,
    pub nodes: Vec<u64>,
    pub create_time: u128,
}