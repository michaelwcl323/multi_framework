# Direct batch DAG proposals

The primary owns transaction ingress, batching, proposal dissemination, voting,
parent synchronization, and certificate production in one process.

`Batch` contains `author`, `round`, full `transactions`, `parents`, `id`, and
`signature`. The ID commits to transaction counts, transaction lengths and
bytes, and the DAG metadata. `Vote` signs that proposal; `Certificate` contains
the complete `Batch` plus quorum signatures. There is no separate `Header` or
payload-digest map.

The proposer waits for the prior-round parent quorum, then seals a batch when
transaction bytes reach `batch_size` or `max_batch_delay` expires. It broadcasts
the full proposal immediately without any preliminary data-availability ACK
quorum. Receivers verify the author/signature and parents, store the full batch,
and then vote. The certificate drives the existing Tusk ordering code.

The batch waiter synchronizes missing parent certificates, which also contain
their complete transaction payloads. The garbage collector receives committed
rounds from Tusk; there are no worker cleanup messages or worker data ports.
