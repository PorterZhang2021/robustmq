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

use metadata_struct::mqtt::bridge::connector::FailureHandlingStrategy;
use std::time::Duration;
use tokio::time::sleep;

pub async fn failure_message_process(strategy: FailureHandlingStrategy, retry_times: u32) -> bool {
    match strategy {
        FailureHandlingStrategy::Discard => true,
        FailureHandlingStrategy::DiscardAfterRetry(strategy) => {
            if retry_times < strategy.retry_total_times {
                sleep(Duration::from_millis(strategy.wait_time_ms)).await;
                return false;
            }
            true
        }
        FailureHandlingStrategy::DeadMessageQueue(_strategy) => {
            // todo
            true
        }
    }
}
