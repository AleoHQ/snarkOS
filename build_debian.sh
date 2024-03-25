#!/bin/bash
echo "================================================"
echo " Attention - Building snarkOS from source code."
echo " This will request root permissions with sudo."
echo "================================================"

# Install Debian dependencies

sudo apt-get update
sudo apt-get install -y \
	build-essential \
	curl \
	gcc \
	libssl-dev \
	make \
	pkg-config \
        sudo \
	tmux \
	xz-utils \
	ufw

# Install Rust

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env

# Install snarkOS
# cargo clean
cargo install --locked --path .

echo "=================================================="
echo " Attention - Please ensure ports 4133 and 3033"
echo "             are enabled on your local network."
echo ""
echo " Cloud Providers - Enable ports 4133 and 3033"
echo "                   in your network firewall"
echo ""
echo " Home Users - Enable port forwarding or NAT rules"
echo "              for 4133 and 3033 on your router."
echo "=================================================="

