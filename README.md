<p align="center">
    <img alt="snarkOS" width="1412" src="https://cdn.aleo.org/snarkos/banner.png">
</p>

<p align="center">
    <a href="https://circleci.com/gh/AleoHQ/snarkOS"><img src="https://circleci.com/gh/AleoHQ/snarkOS.svg?style=svg"></a>
    <a href="https://codecov.io/gh/AleoHQ/snarkOS"><img src="https://codecov.io/gh/AleoHQ/snarkOS/branch/master/graph/badge.svg?token=cck8tS9HpO"/></a>
    <a href="https://discord.gg/aleo"><img src="https://img.shields.io/discord/700454073459015690?logo=discord"/></a>
    <a href="https://twitter.com/AleoHQ"><img src="https://img.shields.io/twitter/follow/AleoHQ?style=social"/></a>
    <a href="https://GitHub.com/AleoHQ/snarkOS"><img src="https://img.shields.io/badge/contributors-59-ee8449"/></a>
</p>

## <a name='TableofContents'></a>Table of Contents

* [1. Overview](#1-overview)
* [2. Build Guide](#2-build-guide)
  * [2.1 Requirements](#21-requirements)
  * [2.2 Installation](#22-installation)
* [3. Run an Aleo Node](#3-run-an-aleo-node)
  * [3a. Run an Aleo Client](#3a-run-an-aleo-client)
  * [3b. Run an Aleo Prover](#3b-run-an-aleo-prover)
* [4. FAQs](#4-faqs)
* [5. Command Line Interface](#5-command-line-interface)
* [6. Development Guide](#6-development-guide)
  * [6.1 Quick Start](#61-quick-start)
  * [6.2 Operations](#62-operations)
* [7. Contributors](#7-contributors)
* [8. License](#8-license)

[comment]: <> (* [4. JSON-RPC Interface]&#40;#4-json-rpc-interface&#41;)
[comment]: <> (* [5. Additional Information]&#40;#5-additional-information&#41;)

## 1. Overview

__snarkOS__ is a decentralized operating system for zero-knowledge applications.
This code forms the backbone of [Aleo](https://aleo.org/) network,
which verifies transactions and stores the encrypted state applications in a publicly-verifiable manner.

## 2. Build Guide

### 2.1 Requirements

The following are **minimum** requirements to run an Aleo node:
 - **OS**: 64-bit architectures only, latest up-to-date for security
    - Clients: Ubuntu 22.04 (LTS), macOS Ventura or later, Windows 11 or later
    - Provers: Ubuntu 22.04 (LTS), macOS Ventura or later
    - Validators: Ubuntu 22.04 (LTS)
 - **CPU**: 64-bit architectures only
    - Clients: 16-cores
    - Provers: 32-cores (64-cores preferred)
    - Validators: 32-cores (64-cores preferred)
 - **RAM**: DDR4 or better
    - Clients: 16GB of memory
    - Provers: 32GB of memory (64GB or larger preferred)
    - Validators: 64GB of memory (128GB or larger preferred)
 - **Storage**: PCIe Gen 3 x4, PCIe Gen 4 x2 NVME SSD, or better
    - Clients: 64GB of disk space
    - Provers: 128GB of disk space
    - Validators: 2TB of disk space (4TB or larger preferred)
 - **Network**: Symmetric, commercial, always-on
    - Clients: 100Mbps of upload **and** download bandwidth
    - Provers: 250Mbps of upload **and** download bandwidth
    - Validators: 500Mbps of upload **and** download bandwidth
- **GPU**:
    - Clients: Not required at this time
    - Provers: CUDA-enabled GPU (optional)
    - Validators: Not required at this time

Please note that in order to run an Aleo Prover that is **competitive**, the machine will need more than these requirements.

### 2.2 Installation

Before beginning, please ensure your machine has `Rust v1.66+` installed. Instructions to [install Rust can be found here.](https://www.rust-lang.org/tools/install)

Start by cloning this GitHub repository:
```
git clone https://github.com/AleoHQ/snarkOS.git --depth 1
```

Next, move into the `snarkOS` directory:
```
cd snarkOS
```

Lastly, install `snarkOS`:
```
cargo install --locked --path .
```

Please ensure ports `4133/tcp` and `3033/tcp` are open on your router and OS firewall.

**[For Ubuntu users]** A helper script to install dependencies is available. From the `snarkOS` directory, run:
```
./build_ubuntu.sh
```


**[For Debian users]** 

Firstly, install the most recent versions of LLVM and Clang.
Download the official install script from here (https://apt.llvm.org/llvm.sh) and then run:
```
./llvm.sh
```

A helper script to install dependencies is available. From the `snarkOS` directory, run:
```
./build_debian.sh
```

## 3. Run an Aleo Node

## 3a. Run an Aleo Client

Start by following the instructions in the [Build Guide](#2-build-guide).

Next, to start a client node, from the `snarkOS` directory, run:
```
./run-client.sh
```

## 3b. Run an Aleo Prover

Start by following the instructions in the [Build Guide](#2-build-guide).

Next, generate an Aleo account address:
```
snarkos account new
```
This will output a new Aleo account in the terminal.

**Please remember to save the account private key and view key.** The following is an example output:
```
 Attention - Remember to store this account private key and view key.

  Private Key  APrivateKey1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx  <-- Save Me And Use In The Next Step
     View Key  AViewKey1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx  <-- Save Me
      Address  aleo1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx  <-- Save Me
```

Next, to start a proving node, from the `snarkOS` directory, run:
```
./run-prover.sh
```
When prompted, enter your Aleo private key:
```
Enter the Aleo Prover account private key:
APrivateKey1xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

## 4. FAQs

### 1. My node is unable to compile.

- Ensure your machine has `Rust v1.66+` installed. Instructions to [install Rust can be found here.](https://www.rust-lang.org/tools/install)
- If large errors appear during compilation, try running `cargo clean`.
- Ensure `snarkOS` is started using `./run-client.sh` or `./run-prover.sh`.

### 2. My node is unable to connect to peers on the network.

- Ensure ports `4133/tcp` and `3033/tcp` are open on your router and OS firewall.
- Ensure `snarkOS` is started using `./run-client.sh` or `./run-prover.sh`.

### 3. I can't generate a new address ### 

- Before running the command above (`snarkos account new`) try `source ~/.bashrc`
- Also double-check the spelling of `snarkos`. Note the directory is `/snarkOS`, and the command is `snarkos`

### 4. How do I use the CLI to sign and verify a message?

1. Generate an account with `snarkos account new` if you haven't already
2. Sign a message with your private key using `snarkos account sign --raw -m "Message" --private-key-file=<PRIVATE_KEY_FILE>`
3. Verify your signature with `snarkos account verify --raw -m "Message" -s sign1SignatureHere -a aleo1YourAccountAddress`

Note, using the `--raw` flag with the command will sign plaintext messages as bytes rather than [Aleo](https://developer.aleo.org/aleo/language#data-types-and-values) values such as `1u8` or `100field`.


## 5. Command Line Interface

To run a node with custom settings, refer to the options and flags available in the `snarkOS` CLI.

The full list of CLI flags and options can be viewed with `snarkos --help`:
```
snarkOS 
The Aleo Team <hello@aleo.org>

USAGE:
    snarkos [OPTIONS] <SUBCOMMAND>

OPTIONS:
    -h, --help                     Print help information
    -v, --verbosity <VERBOSITY>    Specify the verbosity [options: 0, 1, 2, 3] [default: 2]

SUBCOMMANDS:
    account    Commands to manage Aleo accounts
    clean      Cleans the snarkOS node storage
    help       Print this message or the help of the given subcommand(s)
    start      Starts the snarkOS node
    update     Update snarkOS
```

The following are the options for the `snarkos start` command:
```
USAGE:
    snarkos start [OPTIONS]

OPTIONS:
        --network <NETWORK_ID>                  Specify the network ID of this node [default: 3]
        
        --validator                             Specify this node as a validator
        --prover                                Specify this node as a prover
        --client                                Specify this node as a client
        
        --private-key <PRIVATE_KEY>             Specify the node's account private key
        --private-key-file <PRIVATE_KEY_FILE>   Specify the path to a file containing the node's account private key
        
        --node <IP:PORT>                        Specify the IP address and port for the node server [default: 0.0.0.0:4133]
        --connect <IP:PORT>                     Specify the IP address and port of a peer to connect to
 
        --rest <REST>                           Specify the IP address and port for the REST server [default: 0.0.0.0:3033]
        --norest                                If the flag is set, the node will not initialize the REST server
        
        --nodisplay                             If the flag is set, the node will not render the display
        --verbosity <VERBOSITY_LEVEL>           Specify the verbosity of the node [options: 0, 1, 2, 3] [default: 2]
        --logfile <PATH>                        Specify the path to the file where logs will be stored [default: /tmp/snarkos.log]
        
        --dev <NODE_ID>                         Enables development mode, specify a unique ID for this node
```

## 6. Development Guide

### 6.1 Quick Start

In the first terminal, start the first validator by running:
```
cargo run --release -- start --nodisplay --dev 0 --validator
```
In the second terminal, start the second validator by running:
```
cargo run --release -- start --nodisplay --dev 1 --validator
```
In the third terminal, start the third validator by running:
```
cargo run --release -- start --nodisplay --dev 2 --validator
```
In the fourth terminal, start the fourth validator by running:
```
cargo run --release -- start --nodisplay --dev 3 --validator
```

From here, this procedure can be used to further start-up provers and clients.

### 6.2 Operations

It is important to initialize the nodes starting from `0` and incrementing by `1` for each new node.

The following is a list of options to initialize a node (replace `<NODE_ID>` with a number starting from `0`):
```
cargo run --release -- start --nodisplay --dev <NODE_ID> --validator
cargo run --release -- start --nodisplay --dev <NODE_ID> --prover
cargo run --release -- start --nodisplay --dev <NODE_ID> --client
cargo run --release -- start --nodisplay --dev <NODE_ID>
```

When no node type is specified, the node will default to `--client`.

### 6.3 Local Devnet

#### 6.3.1 Install `tmux`

To run a local devnet with the script, start by installing `tmux`.

<details><summary>macOS</summary>

To install `tmux` on macOS, you can use the `Homebrew` package manager.
If you haven't installed `Homebrew` yet, you can find instructions at [their website](https://brew.sh/).
```bash
# Once Homebrew is installed, run:
brew install tmux
```

</details>

<details><summary>Ubuntu</summary>

On Ubuntu and other Debian-based systems, you can use the `apt` package manager:
```bash
sudo apt update
sudo apt install tmux
```

</details>

<details><summary>Windows</summary>

There are a couple of ways to use `tmux` on Windows:

### Using Windows Subsystem for Linux (WSL)

1. First, install [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install).
2. Once WSL is set up and you have a Linux distribution installed (e.g., Ubuntu), open your WSL terminal and install `tmux` as you would on a native Linux system:
```bash
sudo apt update
sudo apt install tmux
```

</details>

#### 6.3.2 Start a Local Devnet

To start a local devnet, run:
```
./devnet.sh
```
Follow the instructions in the terminal to start the devnet.

#### 6.3.3 View a Local Devnet

#### Switch Nodes (forward)

To toggle to the next node in a local devnet, run:
```
Ctrl+b n
```

#### Switch Nodes (backwards)

To toggle to the previous node in a local devnet, run:
```
Ctrl+b p
```

#### Select a Node (choose-tree)

To select a node in a local devnet, run:
```
Ctrl+b w
```

#### Select a Node (manually)

To select a node manually in a local devnet, run:
```
Ctrl+b :select-window -t {NODE_ID}
```

#### 6.3.4 Stop a Local Devnet

To stop a local devnet, run:
```
Ctrl+b :kill-session
```
Then, press `Enter`.

### Clean Up

To clean up the node storage, run:
```
cargo run --release -- clean --dev <NODE_ID>
```

## 7. Contributors
Thank you for helping make snarkOS better!  
[🧐 What do the emojis mean?](https://allcontributors.org/docs/en/emoji-key)

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/howardwu"><img src="https://avatars.githubusercontent.com/u/9260812?v=4?s=100" width="100px;" alt="Howard Wu"/><br /><sub><b>Howard Wu</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=howardwu" title="Code">💻</a> <a href="#maintenance-howardwu" title="Maintenance">🚧</a> <a href="#ideas-howardwu" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/AleoHQ/snarkOS/pulls?q=is%3Apr+reviewed-by%3Ahowardwu" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/raychu86"><img src="https://avatars.githubusercontent.com/u/14917648?v=4?s=100" width="100px;" alt="Raymond Chu"/><br /><sub><b>Raymond Chu</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=raychu86" title="Code">💻</a> <a href="#maintenance-raychu86" title="Maintenance">🚧</a> <a href="#ideas-raychu86" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/AleoHQ/snarkOS/pulls?q=is%3Apr+reviewed-by%3Araychu86" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ljedrz"><img src="https://avatars.githubusercontent.com/u/3750347?v=4?s=100" width="100px;" alt="ljedrz"/><br /><sub><b>ljedrz</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=ljedrz" title="Code">💻</a> <a href="#maintenance-ljedrz" title="Maintenance">🚧</a> <a href="#ideas-ljedrz" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/AleoHQ/snarkOS/pulls?q=is%3Apr+reviewed-by%3Aljedrz" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/niklaslong"><img src="https://avatars.githubusercontent.com/u/13221615?v=4?s=100" width="100px;" alt="Niklas Long"/><br /><sub><b>Niklas Long</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=niklaslong" title="Code">💻</a> <a href="#maintenance-niklaslong" title="Maintenance">🚧</a> <a href="#ideas-niklaslong" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/AleoHQ/snarkOS/pulls?q=is%3Apr+reviewed-by%3Aniklaslong" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/collinc97"><img src="https://avatars.githubusercontent.com/u/16715212?v=4?s=100" width="100px;" alt="Collin Chin"/><br /><sub><b>Collin Chin</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=collinc97" title="Code">💻</a> <a href="https://github.com/AleoHQ/snarkOS/commits?author=collinc97" title="Documentation">📖</a> <a href="https://github.com/AleoHQ/snarkOS/pulls?q=is%3Apr+reviewed-by%3Acollinc97" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/iamalwaysuncomfortable"><img src="https://avatars.githubusercontent.com/u/26438809?v=4?s=100" width="100px;" alt="Mike Turner"/><br /><sub><b>Mike Turner</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=iamalwaysuncomfortable" title="Code">💻</a> <a href="https://github.com/AleoHQ/snarkOS/commits?author=iamalwaysuncomfortable" title="Documentation">📖</a> <a href="https://github.com/AleoHQ/snarkOS/pulls?q=is%3Apr+reviewed-by%3Aiamalwaysuncomfortable" title="Reviewed Pull Requests">👀</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://gakonst.com/"><img src="https://avatars.githubusercontent.com/u/17802178?v=4?s=100" width="100px;" alt="Georgios Konstantopoulos"/><br /><sub><b>Georgios Konstantopoulos</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=gakonst" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/kobigurk"><img src="https://avatars.githubusercontent.com/u/3520024?v=4?s=100" width="100px;" alt="Kobi Gurkan"/><br /><sub><b>Kobi Gurkan</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=kobigurk" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/vvp"><img src="https://avatars.githubusercontent.com/u/700877?v=4?s=100" width="100px;" alt="Vesa-Ville"/><br /><sub><b>Vesa-Ville</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=vvp" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/jules"><img src="https://avatars.githubusercontent.com/u/30194392?v=4?s=100" width="100px;" alt="jules"/><br /><sub><b>jules</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=jules" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/daniilr"><img src="https://avatars.githubusercontent.com/u/1212355?v=4?s=100" width="100px;" alt="Daniil"/><br /><sub><b>Daniil</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=daniilr" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/akattis"><img src="https://avatars.githubusercontent.com/u/4978114?v=4?s=100" width="100px;" alt="akattis"/><br /><sub><b>akattis</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=akattis" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/wcannon"><img src="https://avatars.githubusercontent.com/u/910589?v=4?s=100" width="100px;" alt="William Cannon"/><br /><sub><b>William Cannon</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=wcannon" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/wcannon-aleo"><img src="https://avatars.githubusercontent.com/u/93155840?v=4?s=100" width="100px;" alt="wcannon-aleo"/><br /><sub><b>wcannon-aleo</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=wcannon-aleo" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/sadroeck"><img src="https://avatars.githubusercontent.com/u/31270289?v=4?s=100" width="100px;" alt="Sam De Roeck"/><br /><sub><b>Sam De Roeck</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=sadroeck" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/soft2dev"><img src="https://avatars.githubusercontent.com/u/35427355?v=4?s=100" width="100px;" alt="soft2dev"/><br /><sub><b>soft2dev</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=soft2dev" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/amousa11"><img src="https://avatars.githubusercontent.com/u/12452142?v=4?s=100" width="100px;" alt="Ali Mousa"/><br /><sub><b>Ali Mousa</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=amousa11" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://pyk.sh/"><img src="https://avatars.githubusercontent.com/u/2213646?v=4?s=100" width="100px;" alt="pyk"/><br /><sub><b>pyk</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=pyk" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/whalelephant"><img src="https://avatars.githubusercontent.com/u/18553484?v=4?s=100" width="100px;" alt="Belsy"/><br /><sub><b>Belsy</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=whalelephant" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/apruden2008"><img src="https://avatars.githubusercontent.com/u/39969542?v=4?s=100" width="100px;" alt="apruden2008"/><br /><sub><b>apruden2008</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=apruden2008" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://stackoverflow.com/story/fabianoprestes"><img src="https://avatars.githubusercontent.com/u/976612?v=4?s=100" width="100px;" alt="Fabiano Prestes"/><br /><sub><b>Fabiano Prestes</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=zosorock" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/HarukaMa"><img src="https://avatars.githubusercontent.com/u/861659?v=4?s=100" width="100px;" alt="Haruka"/><br /><sub><b>Haruka</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=HarukaMa" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/e4m7he6g"><img src="https://avatars.githubusercontent.com/u/95574065?v=4?s=100" width="100px;" alt="e4m7he6g"/><br /><sub><b>e4m7he6g</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=e4m7he6g" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/w4ll3"><img src="https://avatars.githubusercontent.com/u/8595904?v=4?s=100" width="100px;" alt="Gregório Granado Magalhães"/><br /><sub><b>Gregório Granado Magalhães</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=w4ll3" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://stake.nodes.guru/"><img src="https://avatars.githubusercontent.com/u/44749897?v=4?s=100" width="100px;" alt="Evgeny Garanin"/><br /><sub><b>Evgeny Garanin</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=evgeny-garanin" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/macro-ss"><img src="https://avatars.githubusercontent.com/u/59944291?v=4?s=100" width="100px;" alt="Macro Hoober"/><br /><sub><b>Macro Hoober</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=macro-ss" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/code-pangolin"><img src="https://avatars.githubusercontent.com/u/89436546?v=4?s=100" width="100px;" alt="code-pangolin"/><br /><sub><b>code-pangolin</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=code-pangolin" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/kaola526"><img src="https://avatars.githubusercontent.com/u/88829586?v=4?s=100" width="100px;" alt="kaola526"/><br /><sub><b>kaola526</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=kaola526" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/clarenous"><img src="https://avatars.githubusercontent.com/u/18611530?v=4?s=100" width="100px;" alt="clarenous"/><br /><sub><b>clarenous</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=clarenous" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/unordered-set"><img src="https://avatars.githubusercontent.com/u/78592281?v=4?s=100" width="100px;" alt="Kostyan"/><br /><sub><b>Kostyan</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=unordered-set" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/austinabell"><img src="https://avatars.githubusercontent.com/u/24993711?v=4?s=100" width="100px;" alt="Austin Abell"/><br /><sub><b>Austin Abell</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=austinabell" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/yelhousni"><img src="https://avatars.githubusercontent.com/u/16170090?v=4?s=100" width="100px;" alt="Youssef El Housni"/><br /><sub><b>Youssef El Housni</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=yelhousni" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ghostant-1017"><img src="https://avatars.githubusercontent.com/u/53888545?v=4?s=100" width="100px;" alt="ghostant-1017"/><br /><sub><b>ghostant-1017</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=ghostant-1017" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://pencil.li/"><img src="https://avatars.githubusercontent.com/u/5947268?v=4?s=100" width="100px;" alt="Miguel Gargallo"/><br /><sub><b>Miguel Gargallo</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=miguelgargallo" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/wang384670111"><img src="https://avatars.githubusercontent.com/u/78151109?v=4?s=100" width="100px;" alt="Chines Wang"/><br /><sub><b>Chines Wang</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=wang384670111" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ayushgw"><img src="https://avatars.githubusercontent.com/u/14152340?v=4?s=100" width="100px;" alt="Ayush Goswami"/><br /><sub><b>Ayush Goswami</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=ayushgw" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/timsmith1337"><img src="https://avatars.githubusercontent.com/u/77958700?v=4?s=100" width="100px;" alt="Tim - o2Stake"/><br /><sub><b>Tim - o2Stake</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=timsmith1337" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/liusen-adalab"><img src="https://avatars.githubusercontent.com/u/74092505?v=4?s=100" width="100px;" alt="liu-sen"/><br /><sub><b>liu-sen</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=liusen-adalab" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Pa1amar"><img src="https://avatars.githubusercontent.com/u/20955327?v=4?s=100" width="100px;" alt="Palamar"/><br /><sub><b>Palamar</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=Pa1amar" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/swift-mx"><img src="https://avatars.githubusercontent.com/u/80231732?v=4?s=100" width="100px;" alt="swift-mx"/><br /><sub><b>swift-mx</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=swift-mx" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/dtynn"><img src="https://avatars.githubusercontent.com/u/1426666?v=4?s=100" width="100px;" alt="Caesar Wang"/><br /><sub><b>Caesar Wang</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=dtynn" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/paulip1792"><img src="https://avatars.githubusercontent.com/u/52645166?v=4?s=100" width="100px;" alt="Paul IP"/><br /><sub><b>Paul IP</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=paulip1792" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://philipglazman.com/"><img src="https://avatars.githubusercontent.com/u/8378656?v=4?s=100" width="100px;" alt="Philip Glazman"/><br /><sub><b>Philip Glazman</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=philipglazman" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Avadon"><img src="https://avatars.githubusercontent.com/u/404177?v=4?s=100" width="100px;" alt="Ruslan Nigmatulin"/><br /><sub><b>Ruslan Nigmatulin</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=Avadon" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.garillot.net/"><img src="https://avatars.githubusercontent.com/u/4142?v=4?s=100" width="100px;" alt="François Garillot"/><br /><sub><b>François Garillot</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=huitseeker" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/aolcr"><img src="https://avatars.githubusercontent.com/u/67066732?v=4?s=100" width="100px;" alt="aolcr"/><br /><sub><b>aolcr</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=aolcr" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/zvolin"><img src="https://avatars.githubusercontent.com/u/34972409?v=4?s=100" width="100px;" alt="Maciej Zwoliński"/><br /><sub><b>Maciej Zwoliński</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=zvolin" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.linkedin.com/in/ignacio-avecilla-39386a191/"><img src="https://avatars.githubusercontent.com/u/63374472?v=4?s=100" width="100px;" alt="Nacho Avecilla"/><br /><sub><b>Nacho Avecilla</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=IAvecilla" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Protryon"><img src="https://avatars.githubusercontent.com/u/8600837?v=4?s=100" width="100px;" alt="Max Bruce"/><br /><sub><b>Max Bruce</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=Protryon" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/whalelephant"><img src="https://avatars.githubusercontent.com/u/18553484?v=4?s=100" width="100px;" alt="whalelephant"/><br /><sub><b>Belsy</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=whalelephant" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/tranhoaison"><img src="https://avatars.githubusercontent.com/u/31094102?v=4?s=100" width="100px;" alt="tranhoaison"/><br /><sub><b>Santala</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=tranhoaison" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/web3deadline"><img src="https://avatars.githubusercontent.com/u/89900222?v=4?s=100" width="100px;" alt="web3deadline"/><br /><sub><b>deadline</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=web3deadline" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/CedricYanYuhui"><img src="https://avatars.githubusercontent.com/u/136431832?v=4?s=100" width="100px;" alt="CedricYanYuhui"/><br /><sub><b>CedricYanYuhui</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=CedricYanYuhui" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/craigjson"><img src="https://avatars.githubusercontent.com/u/16459396?v=4?s=100" width="100px;" alt="craigjson"/><br /><sub><b>Craig Johnson</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=craigjson" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/vbar"><img src="https://avatars.githubusercontent.com/u/108574?v=4?s=100" width="100px;" alt="vbar"/><br /><sub><b>Vaclav Barta</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=vbar" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/features/security"><img src="https://avatars.githubusercontent.com/u/27347476?v=4?s=100" width="100px;" alt="Dependabot"/><br /><sub><b>Dependabot</b></sub></a><br /><a href="https://github.com/AleoHQ/snarkOS/commits?author=dependabot" title="Code">💻</a></td>
    </tr>
  </tbody>
  <tfoot>
    <tr>
      <td align="center" size="13px" colspan="7">
        <img src="https://raw.githubusercontent.com/all-contributors/all-contributors-cli/1b8533af435da9854653492b1327a23a4dbd0a10/assets/logo-small.svg">
          <a href="https://all-contributors.js.org/docs/en/bot/usage">Add your contributions</a>
        </img>
      </td>
    </tr>
  </tfoot>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind are welcome!

## 8. License

We welcome all contributions to `snarkOS`. Please refer to the [license](#7-license) for the terms of contributions.

[![License: GPL v3](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE.md)
