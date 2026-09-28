# Sprint 04: Distributed Transactions & Parallel Commits (2PC, Interceptors, Intents)

## 1. Executive Summary & Vision
- **Objective**: Reverse engineer CockroachDB's distributed transaction protocol. When a transaction spans multiple Ranges located on distinct physical nodes across geographically separated locations, CockroachDB executes distributed atomicity and serializability using an optimized Two-Phase Commit (2PC) protocol featuring **Parallel Commits**, inline **Write Intents**, and a layered **Transaction Interceptor Pipeline**. This reduces multi-region write latency from 2 Raft round-trips down to a single Raft round-trip.
- **Architectural Leads**:
  - **Winston (System Architect)**: Formal verification of Parallel Commit staging invariants, transaction record state transitions (`PENDING`, `STAGING`, `COMMITTED`, `ABORTED`), and asynchronous intent resolution.
  - **Amelia (Senior Software Engineer)**: Tracing the interceptor stack in `pkg/kv/kvclient/kvcoord`, specifically `txn_coord_sender.go`, `txn_interceptor_committer.go`, and the Concurrency Manager in `pkg/kv/kvserver/concurrency`.

---

## 2. Distributed Transaction Architecture & Interceptor Stack

```mermaid
graph TB
    subgraph SG_Coord ["Client Gateway / Coordinator Node"]
        SQL["SQL Execution Engine"] --> TCS["pkg/kv/kvclient/kvcoord/txn_coord_sender.go<br/>TxnCoordSender"]
        
        subgraph SG_Pipe ["Interceptor Pipeline Stack"]
            TCS --> I_Seq["txn_interceptor_seq_num_allocator.go<br/>Allocates monotonic transaction sequence numbers"]
            I_Seq --> I_Pipe["txn_interceptor_pipeliner.go<br/>Pipelines in-flight writes without waiting for acks"]
            I_Pipe --> I_Buf["txn_interceptor_write_buffer.go<br/>Buffers writes in memory until flush or commit"]
            I_Buf --> I_Span["txn_interceptor_span_refresher.go<br/>Refreshes read spans to prevent transaction aborts"]
            I_Span --> I_Com["txn_interceptor_committer.go<br/>Coordinates 1-RTT Parallel Commit & Staging"]
        end
        
        I_Com --> DS["pkg/kv/kvclient/kvcoord/dist_sender.go<br/>DistSender: Dispatches RPCs to target ranges"]
    end

    subgraph SG_NodeA ["Remote Node A (Range 1: Intent Range)"]
        DS --> |"BatchRequest (Write Intent)"| LH1["Leaseholder Range 1"]
        LH1 --> CM1["Concurrency Manager<br/>Latches + Lock Table"]
        CM1 --> Raft1["Raft Group 1: Write Intent Stored"]
    end

    subgraph SG_NodeB ["Remote Node B (Range 2: Transaction Record Range)"]
        DS --> |"EndTxnRequest (STAGING)"| LH2["Leaseholder Range 2"]
        LH2 --> CM2["Concurrency Manager"]
        CM2 --> Raft2["Raft Group 2: Txn Record Staged"]
    end
```

### Key Source Code Anchors
1. **Transaction Coordinator**: [`pkg/kv/kvclient/kvcoord/txn_coord_sender.go`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/kv/kvclient/kvcoord/txn_coord_sender.go)
   - Coordinates transaction state, heartbeats transaction records to keep them alive, and executes abort/commit lifecycles.
2. **Parallel Commit Interceptor**: [`pkg/kv/kvclient/kvcoord/txn_interceptor_committer.go`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/kv/kvclient/kvcoord/txn_interceptor_committer.go)
   - Injects the list of all written spans into the `EndTxn` request, moving the transaction directly into the `STAGING` state.
3. **Write Pipelining Interceptor**: [`pkg/kv/kvclient/kvcoord/txn_interceptor_pipeliner.go`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/kv/kvclient/kvcoord/txn_interceptor_pipeliner.go)
   - Allows multiple statements within a transaction to execute asynchronously without stalling for Raft consensus between statements.
4. **Concurrency Manager**: [`pkg/kv/kvserver/concurrency/concurrency_manager.go`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/kv/kvserver/concurrency/concurrency_manager.go)
   - Unifies in-memory short-term latches with lock-table entries, managing conflicts between concurrent transactions across distributed nodes.

---

## 3. Parallel Commit Protocol Execution (1-RTT Commit)

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Coord as TxnCoordSender (Local Node)
    participant Range1 as Range 1 Leaseholder (Write Key X)
    participant Range2 as Range 2 Leaseholder (Txn Record & Key Y)

    Client->>Coord: BEGIN TRANSACTION
    Client->>Coord: UPDATE table_x SET status = 'consulting'
    Note over Coord: Pipelined Write: Sends intent without waiting
    Coord->>Range1: Put(Key=X, Intent=True) [Raft In Flight]

    Client->>Coord: UPDATE table_y SET patient_count = patient_count + 1
    Coord->>Range2: Put(Key=Y, Intent=True) [Raft In Flight]

    Client->>Coord: COMMIT
    Note over Coord: PARALLEL COMMIT OPTIMIZATION:<br/>Sends EndTxn(Commit=true, Status=STAGING, InFlightWrites=[X, Y])
    
    par Parallel Raft Quorums across WAN
        Range1->>Range1: Replicate Intent X to Quorum
        Range2->>Range2: Replicate Intent Y & Txn Record(STAGING) to Quorum
    end

    Range1-->>Coord: Intent X Replicated
    Range2-->>Coord: Staging Record Replicated
    
    Note over Coord: STAGING SUCCESS = COMMITTED!<br/>All in-flight intents and staging record are durable!
    Coord-->>Client: Transaction Committed (Success in 1 RTT!)

    rect rgb(241, 245, 249)
    Note over Coord,Range2: ASYNCHRONOUS PHASE (Decoupled from client latency)
    Coord->>Range2: Mark Txn Record COMMITTED
    par Asynchronous Intent Resolution
        Coord->>Range1: ResolveIntent(Key=X, Committed)
        Coord->>Range2: ResolveIntent(Key=Y, Committed)
    end
    end
```

---

## 4. Latency Analysis: Traditional 2PC vs. CockroachDB Parallel Commit

| Phase | Traditional Distributed 2PC | CockroachDB Parallel Commit | Latency Reduction |
| :--- | :--- | :--- | :--- |
| **Statement Execution** | Stalls on every statement until Raft quorum responds. | Pipelined asynchronously (`txn_interceptor_pipeliner.go`). | Overlaps statement round-trips. |
| **Prepare / Staging Phase** | 1 Raft RTT to write intents on all participant ranges. | In-flight intents and `STAGING` transaction record replicated in parallel. | Concurrent execution. |
| **Commit Phase** | 1 Raft RTT to update transaction record to `COMMITTED`. | **0 RTT to client**; Staged status proves commit atomically. | **Eliminates 1 entire WAN Raft RTT!** |
| **Total Client Latency** | $\mathbf{2 \times \text{WAN Raft RTT}}$ | $\mathbf{1 \times \text{WAN Raft RTT}}$ | **$50\%$ latency reduction across regions.** |

---

## 5. THKMesh Engineering Relevance & Translation

1. **Multi-Kiosk Cross-Range Atomicity**: When a patient checks in at Kiosk A, their appointment record in Table 1 and clinic queue slot in Table 2 must be updated atomically. Parallel Commits allow this distributed update across disparate regional nodes without high tail latency.
2. **Intent Resolution & Crash Recovery**: If a kiosk or edge node loses power during a consultation, lingering write intents are safely discovered by subsequent transactions. The transaction record's `STAGING` state deterministically resolves whether the write succeeded without human intervention.
3. **Decoupled Asynchronous Cleanup**: Edge mesh applications do not wait for background lock cleanup, maximizing user interface responsiveness for doctors and patients.

---

## 6. Sprint Epics & Story Breakdown

### Epic 1: Interceptor Pipeline Decompilation
- **Story 1.1**: Map execution ordering of all interceptors in [`pkg/kv/kvclient/kvcoord/txn_coord_sender.go`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/kv/kvclient/kvcoord/txn_coord_sender.go).
- **Story 1.2**: Trace `txn_interceptor_committer.go` to analyze how `InFlightWrites` are tracked and validated.

### Epic 2: Parallel Commit State Machine & Recovery
- **Story 2.1**: Document the state machine transition from `PENDING` $\to$ `STAGING` $\to$ `COMMITTED`.
- **Story 2.2**: Inspect recovery rules when a coordinator crashes while a transaction is in `STAGING` state.
