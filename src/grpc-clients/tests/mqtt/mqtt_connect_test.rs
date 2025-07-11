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

#[cfg(test)]
mod tests {
    use crate::common::get_mqtt_broker_addr;
    use common_base::tools::now_second;
    use grpc_clients::mqtt::admin::call::{
        mqtt_broker_cluster_overview_metrics, mqtt_broker_list_connection,
    };
    use grpc_clients::pool::ClientPool;
    use protocol::broker_mqtt::broker_mqtt_admin::{
        ClusterOverviewMetricsRequest, ListConnectionRequest,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn test_list_connection() {
        let client_pool: Arc<ClientPool> = Arc::new(ClientPool::new(3));
        let addrs = vec![get_mqtt_broker_addr()];

        match mqtt_broker_list_connection(&client_pool, &addrs, ListConnectionRequest {}).await {
            Ok(data) => {
                println!("{data:?}");
            }

            Err(e) => {
                eprintln!("Failed to list connections: {e:?}");
                std::process::exit(1);
            }
        };
    }

    #[tokio::test]
    async fn test_cluster_overview_metrics() {
        let client_pool: Arc<ClientPool> = Arc::new(ClientPool::new(3));
        let addrs = vec![get_mqtt_broker_addr()];

        match mqtt_broker_cluster_overview_metrics(
            &client_pool,
            &addrs,
            ClusterOverviewMetricsRequest {
                start_time: now_second() - 360,
                end_time: now_second() + 120,
            },
        )
        .await
        {
            Ok(data) => {
                println!("connection_num:{}", data.connection_num);
                println!("topic_num: {}", data.topic_num);
                println!("subscribe_num: {}", data.subscribe_num);
                println!("message_in_num: {}", data.message_in_num);
                println!("message_out_num: {}", data.message_out_num);
                println!("message_drop_num: {}", data.message_drop_num);
            }

            Err(e) => {
                eprintln!("Failed to list connections: {e:?}");
                std::process::exit(1);
            }
        };
    }
}
