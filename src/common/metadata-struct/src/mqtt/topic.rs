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

use common_base::tools::now_second;
use protocol::broker_mqtt::broker_mqtt_admin::MqttTopicRaw;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Serialize, Deserialize, Debug, PartialEq)]
pub struct MQTTTopic {
    pub topic_id: String,
    pub cluster_name: String,
    pub topic_name: String,
    pub retain_message: Option<Vec<u8>>,
    pub retain_message_expired_at: Option<u64>,
    pub create_time: u64,
}

impl MQTTTopic {
    pub fn new(topic_id: String, cluster_name: String, topic_name: String) -> Self {
        MQTTTopic {
            topic_id,
            cluster_name,
            topic_name,
            retain_message: None,
            retain_message_expired_at: None,
            create_time: now_second(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(&self).unwrap()
    }
}

impl From<MQTTTopic> for MqttTopicRaw {
    fn from(topic: MQTTTopic) -> Self {
        Self {
            topic_id: topic.topic_id,
            cluster_name: topic.cluster_name,
            topic_name: topic.topic_name,
            is_contain_retain_message: topic.retain_message.is_some(),
        }
    }
}
