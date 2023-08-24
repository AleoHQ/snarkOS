// Copyright (C) 2019-2023 Aleo Systems Inc.
// This file is part of the snarkOS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    io::{self, ErrorKind::*},
    net::{IpAddr, Ipv4Addr, SocketAddr},
};

#[cfg(doc)]
use crate::protocols::{self, Handshake, Reading, Writing};

/// The Tcp's configuration. See the source of [`Config::default`] for the defaults.
#[derive(Debug, Clone)]
pub struct Config {
    /// A user-friendly identifier of the Tcp. It is visible in the logs, where it allows Tcp instances to be
    /// distinguished more easily if multiple are run at the same time.
    ///
    /// note: If set to `None`, Tcp will automatically be assigned a sequential, zero-based numeric identifier.
    pub name: Option<String>,
    /// The IP address the Tcp's connection listener should bind to.
    ///
    /// note: If set to `None`, the Tcp will not listen for inbound connections at all.
    pub listener_ip: Option<IpAddr>,
    /// The desired listening port of the Tcp. If [`Config::allow_random_port`] is set to `true`, the Tcp
    /// will attempt to bind its listener to a different port if the desired one is not available.
    ///
    /// note: [`Config::listener_ip`] must not be `None` in order for it to have any effect.
    pub desired_listening_port: Option<u16>,
    /// Allow listening on a different port if [`Config::desired_listening_port`] is unavailable.
    ///
    /// note: [`Config::listener_ip`] must not be `None` in order for it to have any effect.
    pub allow_random_port: bool,
    /// The list of IO errors considered fatal and causing the connection to be dropped.
    ///
    /// note: Tcp needs to implement the [`Reading`] and/or [`Writing`] protocol in order for it to have any effect.
    pub fatal_io_errors: Vec<io::ErrorKind>,
    /// The maximum number of active connections Tcp can maintain at any given time.
    ///
    /// note: This number can very briefly be breached by 1 in case of inbound connection attempts. It can never be
    /// breached by outbound connection attempts, though.
    pub max_connections: u16,
    /// The maximum time (in milliseconds) allowed to establish a raw (before the [`Handshake`] protocol) TCP connection.
    pub connection_timeout_ms: u16,
}

impl Config {
    /// Initializes a new Tcp configuration with a listener address,
    /// a maximum number of connections, and the default values.
    pub fn new(listener_address: SocketAddr, max_connections: u16) -> Self {
        Self {
            listener_ip: Some(listener_address.ip()),
            desired_listening_port: Some(listener_address.port()),
            max_connections,
            ..Default::default()
        }
    }
}

impl Default for Config {
    /// Initializes a new Tcp configuration with the default values.
    fn default() -> Self {
        #[cfg(feature = "test")]
        fn default_ip() -> Option<IpAddr> {
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
        }

        #[cfg(not(feature = "test"))]
        fn default_ip() -> Option<IpAddr> {
            Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
        }

        Self {
            name: None,
            listener_ip: default_ip(),
            desired_listening_port: None,
            allow_random_port: false,
            fatal_io_errors: vec![ConnectionReset, ConnectionAborted, BrokenPipe, InvalidData, UnexpectedEof],
            max_connections: 100,
            connection_timeout_ms: 1_000,
        }
    }
}
