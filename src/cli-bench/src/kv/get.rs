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

use std::{sync::Arc, time::Duration};

use clap::Parser;
use dashmap::DashMap;
use futures::future;
use tokio::time::Instant;

use crate::{
    error::BenchMarkError,
    kv::utils::{
        do_placement_get, do_placement_set, gen_data, print_latency_table, print_throughput_table,
        print_total_statistics, sort_lantencies_by_client_id,
    },
    BenchMark,
};
use grpc_clients::pool::ClientPool;

#[derive(Debug, Clone, Parser)]
pub struct KvGetBenchArgs {
    /// The number of concurrent clients to simulate
    #[clap(long, default_value = "100")]
    pub num_clients: usize,

    /// The number of worker threads to run the benchmark
    #[clap(long, default_value = "4")]
    pub worker_threads: usize,

    /// The number of keys to get
    #[clap(long, default_value = "500")]
    pub num_keys: usize,

    /// The size of each value in bytes
    #[clap(long, default_value = "64")]
    pub value_size: usize,

    /// The address of the placement center
    #[clap(long, default_value = "127.0.0.1:1228", value_delimiter = ',')]
    pub pc_addrs: Vec<String>,
}

#[axum::async_trait]
impl BenchMark for KvGetBenchArgs {
    fn validate(&self) -> Result<(), BenchMarkError> {
        Ok(())
    }

    async fn do_bench(&self) -> Result<(), BenchMarkError> {
        self.validate()?;

        let KvGetBenchArgs {
            num_clients,
            worker_threads,
            num_keys,
            value_size,
            pc_addrs,
        } = self.clone();

        println!(
            "Starting KV Get Benchmark with {} clients, {} worker threads, {} keys, value size: {}, placement center addresses: {:?}",
            num_clients, worker_threads, num_keys, value_size, pc_addrs
        );

        let client_pool = Arc::new(ClientPool::new(0)); // unlimited connections
        let mut set_handles = Vec::with_capacity(num_clients);

        for client_id in 0..num_clients {
            let client_pool_shared = client_pool.clone();
            let pc_addrs_clone = pc_addrs.clone();
            let set_handle = tokio::spawn(async move {
                let result: Result<(), BenchMarkError> = async {
                    for key_id in 0..num_keys {
                        // generate random key and value
                        let key = (client_id * num_keys + key_id).to_string();

                        let value = gen_data(value_size);

                        do_placement_set(
                            &client_pool_shared,
                            &pc_addrs_clone
                                .iter()
                                .map(String::as_str)
                                .collect::<Vec<_>>(),
                            key,
                            value,
                        )
                        .await?;
                    }
                    Ok(())
                }
                .await;
                result
            });

            set_handles.push(set_handle);
        }

        future::join_all(set_handles).await;

        let mut get_handles = Vec::with_capacity(num_clients);

        // we start the timer after all kv pairs are set
        let latencies = Arc::new(DashMap::with_capacity(num_clients));

        let total_key_sizes = Arc::new(DashMap::with_capacity(num_clients));

        for client_id in 0..num_clients {
            latencies.insert(client_id, Vec::new());
            total_key_sizes.insert(client_id, 0);
        }

        let total_now = Instant::now();

        for client_id in 0..num_clients {
            let client_pool_shared = client_pool.clone();
            let latencies_shared = latencies.clone();
            let total_key_sizes_shared = total_key_sizes.clone();
            let pc_addrs_clone = pc_addrs.clone();
            let get_handle = tokio::spawn(async move {
                let result: Result<(), BenchMarkError> = async {
                    let mut total_key_size = 0;
                    for key_id in 0..num_keys {
                        // generate random key and value
                        let key = (client_id * num_keys + key_id).to_string();

                        total_key_size += key.len();

                        let now = Instant::now();

                        // get the value back
                        do_placement_get::<String>(
                            &client_pool_shared,
                            &pc_addrs_clone
                                .iter()
                                .map(String::as_str)
                                .collect::<Vec<_>>(),
                            key,
                        )
                        .await?;

                        latencies_shared
                            .entry(client_id)
                            .and_modify(|e: &mut Vec<Duration>| {
                                e.push(now.elapsed());
                            });
                    }

                    total_key_sizes_shared
                        .entry(client_id)
                        .and_modify(|v| *v += total_key_size);

                    Ok(())
                }
                .await;
                result
            });

            get_handles.push(get_handle);
        }

        future::join_all(get_handles).await;

        // print statistics across all clients
        print_total_statistics(total_now.elapsed(), "get", num_clients, num_keys);

        // Sort latencies by client ID
        let latencies_sorted = sort_lantencies_by_client_id(&latencies);

        // Print the results in a table format
        print_latency_table(&latencies_sorted);
        print_throughput_table(
            &latencies_sorted,
            num_keys,
            value_size,
            total_key_sizes.as_ref(),
        );

        Ok(())
    }
}
