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
use std::time::Duration;

use dashmap::DashMap;
use grpc_clients::journal::inner::call::journal_inner_update_cache;
use grpc_clients::pool::ClientPool;
use log::{debug, error, info};
use metadata_struct::journal::segment::JournalSegment;
use metadata_struct::journal::shard::JournalShard;
use metadata_struct::placement::node::BrokerNode;
use protocol::journal_server::journal_inner::{
    JournalUpdateCacheActionType, JournalUpdateCacheResourceType, UpdateJournalCacheRequest,
};
use tokio::select;
use tokio::sync::broadcast::{self, Sender};
use tokio::time::sleep;

use crate::core::cache::PlacementCacheManager;
use crate::core::error::PlacementCenterError;

#[derive(Clone)]
pub struct JournalInnerCallMessage {
    action_type: JournalUpdateCacheActionType,
    resource_type: JournalUpdateCacheResourceType,
    cluster_name: String,
    data: String,
}

#[derive(Clone)]
pub struct JournalInnerCallNodeSender {
    sender: Sender<JournalInnerCallMessage>,
    addr: String,
}

pub struct JournalInnerCallManager {
    pub node_sender: DashMap<String, JournalInnerCallNodeSender>,
    pub node_sender_thread: DashMap<String, Sender<bool>>,
    pub placement_cache_manager: Arc<PlacementCacheManager>,
}

impl JournalInnerCallManager {
    pub fn new(placement_cache_manager: Arc<PlacementCacheManager>) -> Self {
        let node_sender = DashMap::with_capacity(2);
        let node_sender_thread = DashMap::with_capacity(2);
        JournalInnerCallManager {
            node_sender,
            node_sender_thread,
            placement_cache_manager,
        }
    }

    fn node_key(&self, cluster: &str, node_addr: &str) -> String {
        format!("{}_{}", cluster, node_addr)
    }
}

pub async fn call_thread_manager(
    call_manager: &Arc<JournalInnerCallManager>,
    client_pool: &Arc<ClientPool>,
) {
    loop {
        // start thread
        for (key, node_sender) in call_manager.node_sender.clone() {
            if !call_manager.node_sender_thread.contains_key(&key) {
                let (stop_send, _) = broadcast::channel(2);
                start_call_thread(
                    key.clone(),
                    node_sender.addr,
                    call_manager.clone(),
                    client_pool.clone(),
                    stop_send.clone(),
                )
                .await;
                call_manager.node_sender_thread.insert(key, stop_send);
            }
        }

        // gc thread
        for (key, sx) in call_manager.node_sender_thread.clone() {
            if !call_manager.node_sender.contains_key(&key) {
                match sx.send(true) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("{}", e);
                    }
                }
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}

pub async fn update_cache_by_add_journal_node(
    cluster_name: &str,
    call_manager: &Arc<JournalInnerCallManager>,
    client_pool: &Arc<ClientPool>,
    node: BrokerNode,
) -> Result<(), PlacementCenterError> {
    let data = serde_json::to_string(&node)?;
    let mesage = JournalInnerCallMessage {
        action_type: JournalUpdateCacheActionType::Set,
        resource_type: JournalUpdateCacheResourceType::JournalNode,
        cluster_name: cluster_name.to_string(),
        data,
    };
    add_call_message(call_manager, cluster_name, client_pool, mesage).await?;
    Ok(())
}

pub async fn update_cache_by_delete_journal_node(
    cluster_name: &str,
    call_manager: &Arc<JournalInnerCallManager>,
    client_pool: &Arc<ClientPool>,
    node: BrokerNode,
) -> Result<(), PlacementCenterError> {
    let data = serde_json::to_string(&node)?;
    let mesage = JournalInnerCallMessage {
        action_type: JournalUpdateCacheActionType::Delete,
        resource_type: JournalUpdateCacheResourceType::JournalNode,
        cluster_name: cluster_name.to_string(),
        data,
    };
    add_call_message(call_manager, cluster_name, client_pool, mesage).await?;
    Ok(())
}

pub async fn update_cache_by_set_shard(
    cluster_name: &str,
    call_manager: &Arc<JournalInnerCallManager>,
    client_pool: &Arc<ClientPool>,
    shard_info: JournalShard,
) -> Result<(), PlacementCenterError> {
    let data = serde_json::to_string(&shard_info)?;
    let mesage = JournalInnerCallMessage {
        action_type: JournalUpdateCacheActionType::Set,
        resource_type: JournalUpdateCacheResourceType::Shard,
        cluster_name: cluster_name.to_string(),
        data,
    };
    add_call_message(call_manager, cluster_name, client_pool, mesage).await?;
    Ok(())
}

pub async fn update_cache_by_set_segment(
    cluster_name: &str,
    call_manager: &Arc<JournalInnerCallManager>,
    client_pool: &Arc<ClientPool>,
    segment_info: JournalSegment,
) -> Result<(), PlacementCenterError> {
    let data = serde_json::to_string(&segment_info)?;
    let mesage = JournalInnerCallMessage {
        action_type: JournalUpdateCacheActionType::Set,
        resource_type: JournalUpdateCacheResourceType::Segment,
        cluster_name: cluster_name.to_string(),
        data,
    };
    add_call_message(call_manager, cluster_name, client_pool, mesage).await?;
    Ok(())
}

async fn start_call_thread(
    key: String,
    addr: String,
    call_manager: Arc<JournalInnerCallManager>,
    client_pool: Arc<ClientPool>,
    stop_send: broadcast::Sender<bool>,
) {
    tokio::spawn(async move {
        let mut raw_stop_rx = stop_send.subscribe();
        if let Some(node_send) = call_manager.node_sender.get(&key) {
            let mut data_recv = node_send.sender.subscribe();
            info!("Thread starts successfully, Inner communication between Placement Center and Journal Engine node [{}].",addr);
            loop {
                select! {
                    val = raw_stop_rx.recv() =>{
                        if let Ok(flag) = val {
                            if flag {
                                info!("Thread stops successfully, Inner communication between Placement Center and Journal Engine node [{}].",addr);
                                break;
                            }
                        }
                    },
                    val = data_recv.recv()=>{
                        if let Ok(data) = val{
                            call_journal_update_cache(client_pool.clone(), addr.clone(), data).await;
                        }
                    }
                }
            }
        }
    });
}

async fn call_journal_update_cache(
    client_pool: Arc<ClientPool>,
    addr: String,
    data: JournalInnerCallMessage,
) {
    let request = UpdateJournalCacheRequest {
        cluster_name: data.cluster_name.to_string(),
        action_type: data.action_type.into(),
        resource_type: data.resource_type.into(),
        data: data.data.clone(),
    };
    match journal_inner_update_cache(client_pool.clone(), vec![addr], request).await {
        Ok(resp) => {
            debug!("Calling Journal Engine returns information:{:?}", resp);
        }
        Err(e) => {
            error!("Calling Journal Engine to update cache failed,{}", e);
        }
    };
}

async fn add_call_message(
    call_manager: &Arc<JournalInnerCallManager>,
    cluster_name: &str,
    client_pool: &Arc<ClientPool>,
    message: JournalInnerCallMessage,
) -> Result<(), PlacementCenterError> {
    for addr in call_manager
        .placement_cache_manager
        .get_broker_node_addr_by_cluster(cluster_name)
    {
        let key = call_manager.node_key(cluster_name, &addr);
        if let Some(node_sender) = call_manager.node_sender.get(&key) {
            match node_sender.sender.send(message.clone()) {
                Ok(_) => {}
                Err(e) => {
                    error!("v1{}", e);
                }
            }
        } else {
            // add sender
            let (sx, _) = broadcast::channel::<JournalInnerCallMessage>(1000);
            call_manager.node_sender.insert(
                key.clone(),
                JournalInnerCallNodeSender {
                    sender: sx.clone(),
                    addr: addr.clone(),
                },
            );

            // start thread
            let (stop_send, _) = broadcast::channel(2);
            start_call_thread(
                key.clone(),
                addr,
                call_manager.clone(),
                client_pool.clone(),
                stop_send.clone(),
            )
            .await;
            call_manager.node_sender_thread.insert(key, stop_send);

            // Wait 2s for the "broadcast rx" thread to start, otherwise the send message will report a "channel closed" error
            sleep(Duration::from_secs(2)).await;

            // send message
            match sx.send(message.clone()) {
                Ok(_) => {}
                Err(e) => {
                    error!("v2{}", e);
                }
            }
        }
    }
    Ok(())
}
