# `okufs`

An implementation of the file system used in the Oku browser.

Files and directories are stored as Iroh documents, allowing them to be shared publicly over the mainline DHT or directly between Oku file system nodes.

An Oku file system node consists of three parts:
- A running Iroh node.
- Authorship credentials, consisting of a public and private key pair.
- A mapping of file paths to Iroh document keys.