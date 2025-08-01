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

use super::common::loop_commit_offset;
use super::common::Subscriber;
use super::manager::SubscribeManager;
use super::push::{build_publish_message, send_publish_packet_to_client};
use crate::common::types::ResultMqttBrokerError;
use crate::handler::cache::CacheManager;
use crate::handler::error::MqttBrokerError;
use crate::server::common::connection_manager::ConnectionManager;
use crate::storage::message::MessageStorage;
use crate::subscribe::common::is_ignore_push_error;
use crate::subscribe::manager::SubPushThreadData;
use crate::subscribe::push::{build_pub_qos, build_sub_ids};
use common_base::tools::now_second;
use metadata_struct::adapter::record::Record;
use protocol::mqtt::common::QoS;
use std::sync::Arc;
use std::time::Duration;
use storage_adapter::storage::ArcStorageAdapter;
use tokio::select;
use tokio::sync::broadcast::{self};
use tokio::time::sleep;
use tracing::warn;
use tracing::{error, info};

pub struct ExclusivePush {
    cache_manager: Arc<CacheManager>,
    subscribe_manager: Arc<SubscribeManager>,
    connection_manager: Arc<ConnectionManager>,
    message_storage: ArcStorageAdapter,
}

impl ExclusivePush {
    pub fn new(
        message_storage: ArcStorageAdapter,
        cache_manager: Arc<CacheManager>,
        subscribe_manager: Arc<SubscribeManager>,
        connection_manager: Arc<ConnectionManager>,
    ) -> Self {
        ExclusivePush {
            message_storage,
            cache_manager,
            subscribe_manager,
            connection_manager,
        }
    }

    pub async fn start(&self) {
        loop {
            self.start_push_thread().await;
            self.try_thread_gc().await;
            sleep(Duration::from_secs(1)).await;
        }
    }

    async fn try_thread_gc(&self) {
        // Periodically verify that a push task is running, but the subscribe task has stopped
        // If so, stop the process and clean up the data
        for (exclusive_key, sx) in self.subscribe_manager.exclusive_push_thread.clone() {
            if !self
                .subscribe_manager
                .exclusive_push
                .contains_key(&exclusive_key)
            {
                if let Err(e) = sx.sender.send(true) {
                    error!(
                        "exclusive push thread gc failed, exclusive_key: {:?}, error: {:?}",
                        exclusive_key, e
                    );
                }
                self.subscribe_manager
                    .exclusive_push_thread
                    .remove(&exclusive_key);
            }
        }
    }

    // Handles exclusive subscription push tasks
    // Exclusively subscribed messages are pushed directly to the consuming client
    async fn start_push_thread(&self) {
        for (exclusive_key, subscriber) in self.subscribe_manager.exclusive_push.clone() {
            if self
                .subscribe_manager
                .exclusive_push_thread
                .contains_key(&exclusive_key)
            {
                continue;
            }

            let (sub_thread_stop_sx, mut sub_thread_stop_rx) = broadcast::channel(1);

            let message_storage = MessageStorage::new(self.message_storage.clone());
            let cache_manager = self.cache_manager.clone();
            let connection_manager = self.connection_manager.clone();
            let subscribe_manager = self.subscribe_manager.clone();

            // Subscribe to the data push thread
            self.subscribe_manager.exclusive_push_thread.insert(
                exclusive_key.clone(),
                SubPushThreadData {
                    push_success_record_num: 0,
                    push_error_record_num: 0,
                    last_push_time: 0,
                    last_run_time: 0,
                    create_time: now_second(),
                    sender: sub_thread_stop_sx.clone(),
                },
            );

            tokio::spawn(async move {
                info!("Exclusive push thread for client_id [{}], sub_path: [{}], topic_id [{}] was started successfully",
                        subscriber.client_id, subscriber.sub_path, subscriber.topic_id);

                let group_id = build_group_name(&subscriber);
                let qos = build_pub_qos(&cache_manager, &subscriber);
                let sub_ids = build_sub_ids(&subscriber);

                let mut offset = match message_storage.get_group_offset(&group_id).await {
                    Ok(offset) => offset,
                    Err(e) => {
                        error!("{}", e);
                        subscribe_manager
                            .exclusive_push_thread
                            .remove(&exclusive_key);
                        return;
                    }
                };

                loop {
                    select! {
                        val = sub_thread_stop_rx.recv() =>{
                            if let Ok(flag) = val {
                                if flag {
                                    info!(
                                        "Exclusive Push thread for client_id [{}], sub_path: [{}], topic_id [{}] was stopped successfully",
                                        subscriber.client_id,
                                        subscriber.sub_path,
                                        subscriber.topic_id
                                    );

                                    subscribe_manager.exclusive_push_thread.remove(&exclusive_key);
                                    break;
                                }
                            }
                        },
                        val = pub_message(
                                &subscribe_manager,
                                &connection_manager,
                                &message_storage,
                                &cache_manager,
                                &subscriber,
                                &group_id,
                                &qos,
                                &sub_ids,
                                offset,
                                &exclusive_key,
                                &sub_thread_stop_sx
                            ) => {
                                match val{
                                    Ok(offset_op) => {
                                        if let Some(off) = offset_op{
                                            offset = off + 1;
                                        } else {
                                            sleep(Duration::from_millis(100)).await;
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "Push message to client failed, failure message: {}, topic:{}, group:{}",
                                            e.to_string(),
                                            subscriber.topic_id.clone(),
                                            group_id.clone()
                                        );
                                        sleep(Duration::from_millis(100)).await;
                                    }
                                }
                            }
                    }
                }
            });
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn pub_message(
    subscribe_manager: &Arc<SubscribeManager>,
    connection_manager: &Arc<ConnectionManager>,
    message_storage: &MessageStorage,
    cache_manager: &Arc<CacheManager>,
    subscriber: &Subscriber,
    group_id: &str,
    qos: &QoS,
    sub_ids: &[usize],
    offset: u64,
    exclusive_key: &str,
    sub_thread_stop_sx: &broadcast::Sender<bool>,
) -> Result<Option<u64>, MqttBrokerError> {
    let record_num = 5;
    let results = message_storage
        .read_topic_message(&subscriber.topic_id, offset, record_num)
        .await?;

    let push_fn = async |record: &Record| -> ResultMqttBrokerError {
        let record_offset = if let Some(offset) = record.offset {
            offset
        } else {
            return Ok(());
        };

        // build publish params
        let sub_pub_param = if let Some(params) = build_publish_message(
            cache_manager,
            connection_manager,
            &subscriber.client_id,
            record.to_owned(),
            group_id,
            qos,
            subscriber,
            sub_ids,
        )
        .await?
        {
            params
        } else {
            return Ok(());
        };

        // publish data to client
        send_publish_packet_to_client(
            connection_manager,
            cache_manager,
            &sub_pub_param,
            qos,
            sub_thread_stop_sx,
        )
        .await?;

        // commit offset
        loop_commit_offset(
            message_storage,
            &subscriber.topic_id,
            group_id,
            record_offset,
        )
        .await?;

        Ok(())
    };

    let mut success_num = 0;
    let mut error_num = 0;
    for record in results.iter() {
        match push_fn(record).await {
            Ok(_) => {
                success_num += 1;
            }
            Err(e) => {
                error_num += 1;
                if !is_ignore_push_error(&e) {
                    warn!(
                        "Exclusive push fail, offset [{:?}], error message:{},",
                        record.offset, e
                    );
                }
            }
        }
    }

    subscribe_manager.update_exclusive_push_thread_info(
        exclusive_key,
        success_num as u64,
        error_num as u64,
    );

    if results.is_empty() {
        return Ok(None);
    }

    Ok(Some(results.last().unwrap().offset.unwrap()))
}

fn build_group_name(subscriber: &Subscriber) -> String {
    format!(
        "system_sub_{}_{}_{}",
        subscriber.client_id, subscriber.sub_path, subscriber.topic_id
    )
}

#[cfg(test)]
mod test {}
