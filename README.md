# Bitcoin Reader

Bitcoin reader finds peers in the Bitcoin P2P network and listens to them. It always collects all
headers with valid proof of work. If a tx processor is provided then all txs are run through it as
they are seen on the network. If a block manager is provided then all blocks are fed through it.
