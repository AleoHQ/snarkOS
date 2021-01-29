// Copyright (C) 2019-2021 Aleo Systems Inc.
// This file is part of the snarkOS library.

// The snarkOS library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// The snarkOS library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with the snarkOS library. If not, see <https://www.gnu.org/licenses/>.

use crate::{peers::PeerInfo, NetworkError};
use snarkos_metrics::Metrics;
use snarkos_storage::Ledger;
use snarkvm_models::{algorithms::LoadableMerkleParameters, objects::Transaction};

use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
};

///
/// A data structure for storing the history of all peers with this node server.
///
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PeerBook {
    /// The map of the addresses currently being handshaken with.
    connecting_peers: HashSet<SocketAddr>,
    /// The map of connected peers to their metadata.
    connected_peers: HashMap<SocketAddr, PeerInfo>,
    /// The map of disconnected peers to their metadata.
    disconnected_peers: HashMap<SocketAddr, PeerInfo>,
}

impl PeerBook {
    // TODO (howardwu): Implement manual serializers and deserializers to prevent forward breakage
    //  when the PeerBook or PeerInfo struct fields change.
    ///
    /// Returns an instance of `PeerBook` from the given storage object.
    ///
    /// This function fetches a serialized peer book from the given storage object,
    /// and attempts to deserialize it as an instance of `PeerBook`.
    ///
    /// If the peer book does not exist in storage or fails to deserialize properly,
    /// returns a `NetworkError`.
    ///
    #[inline]
    pub fn load<T: Transaction, P: LoadableMerkleParameters>(storage: &Ledger<T, P>) -> Result<Self, NetworkError> {
        // Fetch the peer book from storage.
        match storage.get_peer_book() {
            // Attempt to deserialize it as a peer book.
            Ok(serialized_peer_book) => Ok(bincode::deserialize(&serialized_peer_book)?),
            _ => Err(NetworkError::PeerBookFailedToLoad),
        }
    }

    ///
    /// Returns `true` if a given address is a connecting peer in the `PeerBook`.
    ///
    #[inline]
    pub fn is_connecting(&self, address: SocketAddr) -> bool {
        self.connecting_peers.contains(&address)
    }

    ///
    /// Returns `true` if a given address is a connected peer in the `PeerBook`.
    ///
    #[inline]
    pub fn is_connected(&self, address: SocketAddr) -> bool {
        self.connected_peers.contains_key(&address)
    }

    ///
    /// Returns `true` if a given address is a disconnected peer in the `PeerBook`.
    ///
    #[inline]
    pub fn is_disconnected(&self, address: SocketAddr) -> bool {
        self.disconnected_peers.contains_key(&address)
    }

    ///
    /// Returns the number of connecting peers.
    ///
    #[inline]
    pub fn number_of_connecting_peers(&self) -> u16 {
        self.connecting_peers.len() as u16
    }

    ///
    /// Returns the number of connected peers.
    ///
    #[inline]
    pub fn number_of_connected_peers(&self) -> u16 {
        self.connected_peers.len() as u16
    }

    ///
    /// Returns the number of disconnected peers.
    ///
    #[inline]
    pub fn number_of_disconnected_peers(&self) -> u16 {
        self.disconnected_peers.len() as u16
    }

    ///
    /// Returns a reference to the connecting peers in this peer book.
    ///
    #[inline]
    pub fn connecting_peers(&self) -> &HashSet<SocketAddr> {
        &self.connecting_peers
    }

    ///
    /// Returns a reference to the connected peers in this peer book.
    ///
    #[inline]
    pub fn connected_peers(&self) -> &HashMap<SocketAddr, PeerInfo> {
        &self.connected_peers
    }

    ///
    /// Returns a reference to the disconnected peers in this peer book.
    ///
    #[inline]
    pub fn disconnected_peers(&self) -> &HashMap<SocketAddr, PeerInfo> {
        &self.disconnected_peers
    }

    ///
    /// Marks the given address as "connecting".
    ///
    pub fn set_connecting(&mut self, address: SocketAddr) -> Result<(), NetworkError> {
        if self.is_connected(address) {
            return Err(NetworkError::PeerAlreadyConnected);
        }
        self.connecting_peers.insert(address);

        Ok(())
    }

    ///
    /// Adds the given address to the connected peers in the `PeerBook`.
    ///
    pub fn set_connected(&mut self, address: SocketAddr, listener: Option<SocketAddr>) -> Result<(), NetworkError> {
        // If listener.is_some(), then it's different than the address; otherwise it's just the address param.
        let listener = if let Some(addr) = listener { addr } else { address };

        // Remove the address from the connecting peers, if it exists.
        let mut peer_info = match self.disconnected_peers.remove(&listener) {
            // Case 1 - A previously known peer.
            Some(peer_info) => peer_info,
            // Case 2 - A peer that was previously not known.
            None => PeerInfo::new(listener),
        };

        // Remove the peer's address from the list of connecting peers.
        self.connecting_peers.remove(&address);

        // Update the peer info to connected.
        peer_info.set_connected()?;

        // Add the address into the connected peers.
        let success = self.connected_peers.insert(listener, peer_info).is_none();
        // On success, increment the connected peer count.
        connected_peers_inc!(success);

        Ok(())
    }

    ///
    /// Removes the given address from the connecting and connected peers in this `PeerBook`,
    /// and adds the given address to the disconnected peers in this `PeerBook`.
    ///
    pub fn set_disconnected(&mut self, address: SocketAddr) -> Result<(), NetworkError> {
        // Case 1 - The given address is a connecting peer, attempt to disconnect.
        if self.connecting_peers.remove(&address) {
            return Ok(());
        }

        // Case 2 - The given address is a connected peer, attempt to disconnect.
        if let Some(mut peer_info) = self.connected_peers.remove(&address) {
            // Update the peer info to disconnected.
            peer_info.set_disconnected()?;

            // Add the address into the disconnected peers.
            let success = self.disconnected_peers.insert(address, peer_info).is_none();
            // On success, decrement the connected peer count.
            connected_peers_dec!(success);

            return Ok(());
        }

        // Case 3 - The given address is not a connected peer.
        // Check if the peer is a known disconnected peer, and attempt to
        // add them to the disconnected peers if they are undiscovered.
        // Check if the peer is a known disconnected peer.
        if !self.disconnected_peers.contains_key(&address) {
            // If not, add the address into the disconnected peers.
            trace!("Adding an undiscovered peer to the peer book - {}", address);
            self.add_peer(address);
        }

        Ok(())
    }

    ///
    /// Adds the given address to the disconnected peers in this `PeerBook`.
    ///
    pub fn add_peer(&mut self, address: SocketAddr) {
        if self.is_connected(address) || self.is_disconnected(address) || self.is_connecting(address) {
            return;
        }

        // Add the given address to the map of disconnected peers.
        self.disconnected_peers
            .entry(address)
            .or_insert_with(|| PeerInfo::new(address));

        debug!("Added {} to the peer book", address);
    }

    ///
    /// Returns a reference to the peer info of the given address, if it exists.
    ///
    pub fn get_peer(&mut self, address: SocketAddr) -> Result<&PeerInfo, NetworkError> {
        // Check if the address is a connected peer.
        if self.is_connected(address) {
            // Fetch the peer info of the connected peer.
            return self
                .connected_peers
                .get(&address)
                .ok_or(NetworkError::PeerBookMissingPeer);
        }

        // Check if the address is a known disconnected peer.
        if self.is_disconnected(address) {
            // Fetch the peer info of the disconnected peer.
            return self
                .disconnected_peers
                .get(&address)
                .ok_or(NetworkError::PeerBookMissingPeer);
        }

        error!("Missing {} in the peer book", address);
        Err(NetworkError::PeerBookMissingPeer)
    }

    ///
    /// Removes the given address from this `PeerBook`.
    ///
    /// This function should only be used in the case that the peer
    /// should be forgotten about permanently.
    ///
    pub fn remove_peer(&mut self, address: &SocketAddr) {
        // Remove the given address from the connecting peers, if it exists.
        self.connecting_peers.remove(address);

        // Remove the given address from the connected peers, if it exists.
        if self.connected_peers.remove(address).is_some() {
            // Decrement the connected_peer metric as the peer was not yet disconnected.
            connected_peers_dec!()
        }

        // Remove the address from the disconnected peers, if it exists.
        self.disconnected_peers.remove(address);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_set_connecting_from_never_connected() {
        let mut peer_book = PeerBook::default();
        let remote_address = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 4031));

        peer_book.add_peer(remote_address);
        assert_eq!(false, peer_book.is_connecting(remote_address));
        assert_eq!(false, peer_book.is_connected(remote_address));
        assert_eq!(true, peer_book.is_disconnected(remote_address));

        peer_book.set_connecting(remote_address).unwrap();
        assert_eq!(true, peer_book.is_connecting(remote_address));
        assert_eq!(false, peer_book.is_connected(remote_address));
        assert_eq!(true, peer_book.is_disconnected(remote_address));
    }

    #[test]
    fn test_set_connected_from_connecting() {
        let mut peer_book = PeerBook::default();
        let remote_address = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 4031));

        peer_book.set_connecting(remote_address).unwrap();
        assert_eq!(true, peer_book.is_connecting(remote_address));
        assert_eq!(false, peer_book.is_connected(remote_address));
        assert_eq!(false, peer_book.is_disconnected(remote_address));

        peer_book.set_connected(remote_address, None).unwrap();
        assert_eq!(false, peer_book.is_connecting(remote_address));
        assert_eq!(true, peer_book.is_connected(remote_address));
        assert_eq!(false, peer_book.is_disconnected(remote_address));
    }

    #[test]
    fn test_set_disconnected_from_connecting() {
        let mut peer_book = PeerBook::default();
        let remote_address = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 4031));

        peer_book.add_peer(remote_address);

        peer_book.set_connecting(remote_address).unwrap();
        assert_eq!(true, peer_book.is_connecting(remote_address));
        assert_eq!(false, peer_book.is_connected(remote_address));
        assert_eq!(true, peer_book.is_disconnected(remote_address));

        peer_book.set_disconnected(remote_address).unwrap();
        assert_eq!(false, peer_book.is_connecting(remote_address));
        assert_eq!(false, peer_book.is_connected(remote_address));
        assert_eq!(true, peer_book.is_disconnected(remote_address));
    }

    #[test]
    fn test_set_disconnected_from_connected() {
        let mut peer_book = PeerBook::default();
        let remote_address = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 4031));

        peer_book.set_connecting(remote_address).unwrap();
        assert_eq!(true, peer_book.is_connecting(remote_address));
        assert_eq!(false, peer_book.is_connected(remote_address));
        assert_eq!(false, peer_book.is_disconnected(remote_address));

        peer_book.set_connected(remote_address, None).unwrap();
        assert_eq!(false, peer_book.is_connecting(remote_address));
        assert_eq!(true, peer_book.is_connected(remote_address));
        assert_eq!(false, peer_book.is_disconnected(remote_address));

        peer_book.set_disconnected(remote_address).unwrap();
        assert_eq!(false, peer_book.is_connecting(remote_address));
        assert_eq!(false, peer_book.is_connected(remote_address));
        assert_eq!(true, peer_book.is_disconnected(remote_address));
    }

    #[test]
    fn test_set_connected_from_disconnected() {
        let mut peer_book = PeerBook::default();
        let remote_address = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 4031));

        peer_book.set_connecting(remote_address).unwrap();
        peer_book.set_connected(remote_address, None).unwrap();
        peer_book.set_disconnected(remote_address).unwrap();
        assert_eq!(false, peer_book.is_connecting(remote_address));
        assert_eq!(false, peer_book.is_connected(remote_address));
        assert_eq!(true, peer_book.is_disconnected(remote_address));

        assert!(peer_book.set_connected(remote_address, None).is_ok());

        assert_eq!(false, peer_book.is_connecting(remote_address));
        assert_eq!(true, peer_book.is_connected(remote_address));
    }
}
