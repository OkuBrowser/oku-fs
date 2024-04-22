# `oku-fs`

An implementation of the file system used in the Oku browser.

Files and directories are stored in replicas implemented as Iroh documents, allowing them to be shared publicly over the mainline DHT or directly between Oku file system nodes.

An Oku file system node consists of three parts:
- A running Iroh node.
- Authorship credentials, consisting of a public and private key pair.
- Replicas, which are Iroh documents containing the file system data.
    - Replicas have a private key that is used for writing, and a public key that is used for reading and sharing (referred to as a 'namespace ID' or 'document ID').
    - Replicas have entries comprising a key and a value, where the key is a path and the value is a blob of data.
    - Paths in a replica can be accessed via a prefix string, so only file paths are stored in the replica; directories are inferred from the paths.

Content discovery occurs over [the mainline DHT](https://en.wikipedia.org/wiki/Mainline_DHT). Content discovery happens as follows:
1. A node has a document ID. It queries the DHT using this document ID.
    - Any nodes claiming to have the document with this ID have announced themselves in the DHT prior.
2. The node receives a list of nodes that claim to have the document.
3. The node asks the nodes in the list for the document's swarm ticket. It stops when it receives a valid swarm ticket.
4. The node uses the swarm ticket to connect to the document swarm and download the document.

As a reminder, 'documents' are in fact entire replicas, with multiple files and directories. Fetching individual files or directories has not yet been implemented.