# `oku-fs`

A distributed file system for use with the Oku browser.

## Technical Design

Files and directories are stored in replicas implemented as Iroh documents, allowing them to be shared publicly over the mainline DHT or directly between Oku file system nodes.

An Oku file system node consists of three parts:
- A running Iroh node.
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

To enable this functionality, relay nodes maintain a list of which replicas are held by which nodes behind NAT.
When an external node requests a replica, said external node connects to the relay node, and the relay node finds the appropriate connected node and begins acting as a middleman during the ticket exchange.