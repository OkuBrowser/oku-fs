# `oku-fs`

A distributed file system for use with the Oku browser.

## Build Instructions

### Prerequisites

To build, please install:
* A copy of the [Rust toolchain](https://www.rust-lang.org/tools/install)
    * It is recommended that you install Rust using [`rustup.rs`](https://rustup.rs/), though many Linux distributions also package the Rust toolchain as well.
* [`libfuse`](https://github.com/libfuse/libfuse/)
    * This is required if you intend on building with the `fuse` feature, but is otherwise optional.
    * It is recommended that you obtain this development package from your distribution.

### Commands

After pre-requisites are installed, you may run:
* `cargo build` for debug builds.
* `cargo build --release` for release builds.
* `cargo install --path .` to install.
* Note: If intending on building or installing an executable rather than a library, please specify the intended features by appending `--features="<features separated by commas>"` to the build command.

### Features
* `cli` - A command-line interface for performing file system operations.
* `relay` - A relay server to enable hole punching (see the 'Technical Design' section below).
* `fuse` - Enables mounting the file system via [FUSE](https://en.wikipedia.org/wiki/Filesystem_in_Userspace).
* Note: If neither the `cli` or `relay` features are enabled, this software will be installed as a development library.

## Technical Design

Files and directories are stored in replicas implemented as Iroh documents, allowing them to be shared publicly over the mainline DHT or directly between Oku file system nodes.

An Oku file system node consists of three parts:
- A running [Iroh](https://www.iroh.computer) node.
- Authorship credentials, consisting of a public and private key pair.
- Replicas, which are Iroh documents containing the file system data.
    - Replicas have a private key that is used for writing, and a public key that is used for reading and sharing (referred to as a 'namespace ID' or 'document ID').
    - Replicas have entries comprising a key and a value, where the key is a path and the value is a blob of data.
    - Paths in a replica can be accessed via a prefix string, so only file paths are stored in the replica; directories are inferred from the paths.

Content discovery occurs over [the mainline DHT](https://en.wikipedia.org/wiki/Mainline_DHT). Content discovery happens as follows:
1. A node has a document ID. It queries the DHT on UDP port `4938` using this document ID.
    - Any nodes claiming to have the document with this ID have announced themselves in the DHT prior.
2. The node receives a list of nodes that claim to have the document.
3. On TCP port `4938`, the node asks the nodes in the list for the document's swarm ticket.
    - Port forwarding is necessary to both (1) announce content on the DHT and (2) respond with document tickets when behind NAT.
4. The node uses the swarm ticket to connect to the document swarm and download the document.

### NAT

Nodes behind NAT (eg, devices on a home network using IPv4) are unable to listen for incoming connections. This means address and content announcements on the DHT will be meaningless; external nodes will be unable to initiate connections to a local address. Consequently, the node will be unable to serve external requests for content (ie, perform ticket exchanges), as no external nodes will be able to reach it.

To solve this, a relay node is used. These relay nodes:
- Are port-forwarded to route traffic to-and-from the local network.
- Perform DHT announcements on behalf of the connected nodes behind NAT.
- Facilitate ticket exchanges between the appropriate connected nodes and external nodes.

To enable this functionality, relay nodes maintain a mapping of replicas to a list of nodes behind NAT.\
When an external node requests a replica:
1. Said external node connects to the relay node
2. The relay node finds all connected nodes with that replica and asks each of them for tickets
3. The relay node constructs a read-only ticket with all of the node addresses in the tickets each connected node provided
4. The relay node sends the constructed ticket to the external node

Note: When replicas are requested from a DHT, an estimated content size is provided. The relay node provides the largest estimate from its connected nodes.

#### Hole punching

It may seem unclear how a ticket allows an external node to connect to a node behind NAT if the relay is only facilitating ticket exchange.
The relay node described above only performs hole-punching for content discovery; N0, Inc maintains a [network of Iroh nodes and relay servers](https://iroh.network/), with the [Iroh relay servers](https://docs.rs/iroh-net/latest/iroh_net/relay/server/struct.Server.html) facilitating hole punching during document syncing.

The Iroh network's relay servers are currently free. When this is no longer the case, Oku relay nodes will be modified to also function as Iroh relay nodes, requiring Let's Encrypt TLS certificates.