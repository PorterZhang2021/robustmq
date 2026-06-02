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

use std::net::{TcpStream, UdpSocket};

/// Check whether a local TCP port is currently listening.
pub fn is_local_port_listening(port: u32) -> bool {
    TcpStream::connect(format!("127.0.0.1:{port}")).is_ok()
}

/// Check whether a local UDP port is currently bound (e.g. a QUIC listener).
///
/// UDP is connectionless, so — unlike TCP — we cannot detect a listener by
/// connecting (a UDP "connect" only sets the default peer and always
/// succeeds). Instead we try to bind the port ourselves: if the bind fails
/// the address is already in use, which we treat as "listening". A plain
/// bind without `SO_REUSEADDR`/`SO_REUSEPORT` still conflicts even when the
/// existing listener set those options, so the check stays reliable. If the
/// bind succeeds nothing is using the port, so we drop the socket and report
/// not listening.
pub fn is_local_udp_port_listening(port: u32) -> bool {
    UdpSocket::bind(format!("0.0.0.0:{port}")).is_err()
}
