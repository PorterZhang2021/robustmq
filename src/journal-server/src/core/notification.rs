// Copyright 2023 RobustMQ Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use metadata_struct::journal::segment::JournalSegment;
use metadata_struct::journal::segment_meta::JournalSegmentMetadata;
use metadata_struct::journal::shard::JournalShard;
use metadata_struct::placement::node::BrokerNode;
use protocol::journal_server::journal_inner::{
    JournalUpdateCacheActionType, JournalUpdateCacheResourceType,
};
use tracing::{error, info};

use super::cache::CacheManager;
use crate::segment::manager::{create_local_segment, SegmentFileManager};

pub async fn parse_notification(
    cache_manager: &Arc<CacheManager>,
    segment_file_manager: &Arc<SegmentFileManager>,
    action_type: JournalUpdateCacheActionType,
    resource_type: JournalUpdateCacheResourceType,
    data: &str,
) {
    match resource_type {
        JournalUpdateCacheResourceType::JournalNode => parse_node(cache_manager, action_type, data),
        JournalUpdateCacheResourceType::Shard => parse_shard(cache_manager, action_type, data),
        JournalUpdateCacheResourceType::Segment => {
            parse_segment(cache_manager, segment_file_manager, action_type, data).await
        }
        JournalUpdateCacheResourceType::SegmentMeta => {
            parse_segment_meta(cache_manager, action_type, data).await
        }
    }
}

fn parse_node(
    cache_manager: &Arc<CacheManager>,
    action_type: JournalUpdateCacheActionType,
    data: &str,
) {
    match action_type {
        JournalUpdateCacheActionType::Set => match serde_json::from_str::<BrokerNode>(data) {
            Ok(node) => {
                info!("Update the cache, Set node, node: {:?}", node);
                cache_manager.add_node(node);
            }
            Err(e) => {
                error!(
                    "Set node information failed to parse with error message :{},body:{}",
                    e, data,
                );
            }
        },

        JournalUpdateCacheActionType::Delete => match serde_json::from_str::<BrokerNode>(data) {
            Ok(node) => {
                info!("Update the cache, remove node, node id: {}", node.node_id);
                cache_manager.delete_node(node.node_id);
            }
            Err(e) => {
                error!(
                    "Remove node information failed to parse with error message :{},body:{}",
                    e, data,
                );
            }
        },
    }
}

fn parse_shard(
    cache_manager: &Arc<CacheManager>,
    action_type: JournalUpdateCacheActionType,
    data: &str,
) {
    match action_type {
        JournalUpdateCacheActionType::Set => match serde_json::from_str::<JournalShard>(data) {
            Ok(shard) => {
                info!("Update the cache, set shard, shard name: {:?}", shard);
                cache_manager.set_shard(shard);
            }
            Err(e) => {
                error!(
                    "set shard information failed to parse with error message :{},body:{}",
                    e, data,
                );
            }
        },

        _ => {
            error!(
                "UpdateCache updates Shard information, only supports Set operations, not {:?}",
                action_type
            );
        }
    }
}

async fn parse_segment(
    cache_manager: &Arc<CacheManager>,
    segment_file_manager: &Arc<SegmentFileManager>,
    action_type: JournalUpdateCacheActionType,
    data: &str,
) {
    match action_type {
        JournalUpdateCacheActionType::Set => match serde_json::from_str::<JournalSegment>(data) {
            Ok(segment) => {
                info!("Segment cache update, action: set, segment:{:?}", segment);

                if let Err(e) =
                    create_local_segment(cache_manager, segment_file_manager, &segment).await
                {
                    error!("Error creating local Segment file, error message: {}", e);
                }
            }
            Err(e) => {
                error!(
                    "Set segment information failed to parse with error message :{},body:{}",
                    e, data,
                );
            }
        },
        _ => {
            error!(
                "UpdateCache updates Segment information, only supports Set operations, not {:?}",
                action_type
            );
        }
    }
}

async fn parse_segment_meta(
    cache_manager: &Arc<CacheManager>,
    action_type: JournalUpdateCacheActionType,
    data: &str,
) {
    match action_type {
        JournalUpdateCacheActionType::Set => {
            match serde_json::from_str::<JournalSegmentMetadata>(data) {
                Ok(segment_meta) => {
                    info!(
                        "Update the cache, set segment meta, segment meta:{:?}",
                        segment_meta
                    );

                    cache_manager.set_segment_meta(segment_meta);
                }
                Err(e) => {
                    error!(
                        "Set segment meta information failed to parse with error message :{},body:{}",
                        e, data,
                    );
                }
            }
        }
        _ => {
            error!(
                "UpdateCache updates SegmentMeta information, only supports Set operations, not {:?}",
                action_type
            );
        }
    }
}
