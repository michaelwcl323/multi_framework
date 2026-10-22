# Parallel PBFT instances

Each committee member leads one instance; IDs follow sorted committee key order.
`primary.rs` starts every instance on every node, with separate Core tasks and
channels. Its receiver routes messages by instance ID. The local transaction
ingress feeds only the proposer for the local node's own instance.

`messages.rs` signs instance-aware positions and validates each proposal against
its instance's fixed leader. `pbft.rs` owns one instance's slots, early votes,
quorums, and contiguous cleanup watermark. It rejects messages for another
instance before changing state. Epoch and view remain fixed at zero.

`core.rs` executes one instance's actions: it stores complete proposals before
preparing and local votes before broadcasting, counts local votes once, and
persists decisions with the full protocol position in the storage key. Proposer
feedback and transport retry cleanup belong to that instance only. `transport.rs`
multiplexes all local instances over one outgoing connection per remote node.
These writes do not implement crash recovery.

`consensus` verifies commit proofs and maintains separate delivery buffers for
each instance. Output is ordered within an instance, with no cross-instance total
order or waiting barrier. See the root README for scope and validation commands.
