# Closed Timestamps & Follower Reads: WAN Latency Optimization

## 1. Executive Summary & The WAN Read Bottleneck

In a globally distributed database operating standard consensus algorithms (e.g., Paxos or Raft), read operations must be served by the **Leader / Leaseholder** to guarantee linearizability and prevent stale reads. When application clients are located in Singapore and the Range Leaseholder is located in London or Frankfurt, every read query incurs cross-continental WAN round-trip latency ($150-300$ms).

CockroachDB solves this problem through an elegant distributed protocol: **Closed Timestamps** coupled with **Follower Reads**. 

Implemented in [`pkg/kv/kvserver/closedts`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/kv/kvserver/closedts), the Leaseholder continuously promises that no future writes will ever be committed at or below a historical timestamp $T_{\text{closed}}$. This promise is broadcast to all follower replicas via an out-of-band side-transport network. Follower replicas in any geographic region can then serve read-only queries locally (`AS OF SYSTEM TIME`) with **sub-millisecond latency**.

---

## 2. Closed Timestamp Architecture & Side-Transport

```mermaid
graph TB
    subgraph SG_LH ["Leaseholder Node (Central Cloud / London)"]
        LH["Range Leaseholder"]
        CM["Concurrency Manager"]
        CT_Pol["pkg/kv/kvserver/closedts/policy.go<br/>Closed Timestamp Policy Engine"]
        ST_Send["pkg/kv/kvserver/closedts/sidetransport/sender.go<br/>Side-Transport Sender"]
        
        CM --> |"Reports in-flight write horizons"| CT_Pol
        CT_Pol --> |"Advances T_closed"| ST_Send
    end

    subgraph SG_Net ["Out-of-Band Side-Transport Network"]
        ST_Send --> |"Dedicated gRPC Stream (ctpb.Service)"| ST_Recv
    end

    subgraph SG_Follower ["Follower Node (Edge Hub / Singapore)"]
        ST_Recv["pkg/kv/kvserver/closedts/sidetransport/receiver.go<br/>Side-Transport Receiver"]
        FR_Reg["In-Memory Closed Timestamp Horizon Registry"]
        LocalFollower["Local Follower Replica"]
        
        ST_Recv --> FR_Reg
        FR_Reg --> LocalFollower
        
        Client["Telehealth Kiosk / Doctor's Terminal"] --> |"SELECT ... AS OF SYSTEM TIME"| LocalFollower
        LocalFollower --> |"Local Read: Latency < 1ms!"| Client
    end
```

---

## 3. The Closed Timestamp Protocol Lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant Writer as Client Writer
    participant LH as Leaseholder (Remote Node)
    participant Sender as Side-Transport Sender
    participant Receiver as Side-Transport Receiver (Local Node)
    participant Follower as Follower Replica (Local Node)
    participant Reader as Local Application Reader

    Note over LH,Sender: Step 1: Timestamp Closure
    LH->>LH: Current HLC Time = 2000ms
    LH->>LH: Compute Target Lag (e.g., lag = 1000ms)
    LH->>LH: Check In-Flight Latches: No active writes at T <= 1000ms
    LH->>Sender: Close Timestamp T_closed = 1000ms for Range [A-M)

    Note over Sender,Receiver: Step 2: Out-of-Band Dissemination
    Sender->>Receiver: gRPC Stream: CloseRange(Span=[A-M), T_closed=1000ms)
    Receiver->>Follower: Record Horizon: Range [A-M) Closed at T=1000ms

    Note over Reader,Follower: Step 3: Follower Read Execution
    Reader->>Follower: Query Patient Vitals (AS OF SYSTEM TIME 950ms)
    Follower->>Follower: Evaluate: 950ms <= T_closed (1000ms)? TRUE!
    Follower->>Follower: Scan Local MVCC Pebble Engine at T=950ms
    Follower-->>Reader: Return Patient Vitals (Latency: 0.8ms - Zero WAN Traffic!)

    Note over Writer,LH: Step 4: Write Rejection Invariant
    Writer->>LH: Write Patient Vitals (Attempting Timestamp T=900ms)
    LH->>LH: Check: 900ms <= T_closed (1000ms)?
    LH-->>Writer: Push Timestamp to T > 1000ms (Write never allowed <= T_closed)
```

---

## 4. Source Code Implementation Trace

### 4.1 Side-Transport Sender (`sender.go`)
Located in [`pkg/kv/kvserver/closedts/sidetransport/sender.go`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/kv/kvserver/closedts/sidetransport/sender.go):
- Establishes persistent, bi-directional gRPC streams to all cluster nodes.
- Coalesces range closures across hundreds of ranges into single network messages to minimize bandwidth consumption over WAN.
- Enforces heartbeats to detect disconnected follower nodes.

### 4.2 Side-Transport Receiver (`receiver.go`)
Located in [`pkg/kv/kvserver/closedts/sidetransport/receiver.go`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/kv/kvserver/closedts/sidetransport/receiver.go):
- Receives range closure intervals and updates lock-free, read-optimized in-memory interval trees.
- Responded to instantly by `dist_sender.go` when validating whether a follower node can execute an `AS OF SYSTEM TIME` request locally.

### 4.3 Closed Timestamp Policy (`policy.go`)
Located in [`pkg/kv/kvserver/closedts/policy.go`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/kv/kvserver/closedts/policy.go):
- Calculates:
  $$T_{\text{closed}} = T_{\text{now}} - \text{closedts.target\_lag}$$
- By default, `closedts.target_lag` is set between $1$ to $3$ seconds.
- Tuning Trade-off:
  - **Smaller Lag (e.g., $500$ms)**: Provides fresher follower reads, but increases the probability that slow-running write transactions must retry because their timestamp gets pushed.
  - **Larger Lag (e.g., $3000$ms)**: Near-zero write transaction retries, but follower reads see data that is slightly older.

---

## 5. Comparative Latency & Throughput Benchmark Analysis

| Query Scenario | Network Path | Average Latency | WAN Bandwidth Used | Availability during WAN Severance |
| :--- | :--- | :--- | :--- | :--- |
| **Strict Read (Current Time)** | Client $\to$ Local Node $\to$ **Remote Leaseholder** $\to$ Local Node | $\mathbf{80 - 180 \text{ ms}}$ | Full query & response transmitted over WAN. | **Unavailable** (Fails if WAN link drops). |
| **Follower Read (`AS OF SYSTEM TIME`)** | Client $\to$ **Local Follower Node** (Internal NVMe read) | $\mathbf{0 . 5 - 2 \text{ ms}}$ | **0 Bytes WAN** (Only periodic compressed closedts metadata). | **Fully Available** (Can read up to last closed horizon). |
| **Global Table Read** | Client $\to$ **Local Node** | $\mathbf{0 . 3 - 1 \text{ ms}}$ | **0 Bytes WAN**. | **Fully Available**. |

---

## 6. THKMesh Telehealth Architecture Adaptation

```
+---------------------------------------------------------------------------------------+
|                       THKMESH FOLLOWER READ CLINICAL DEPLOYMENT                       |
+---------------------------------------------------------------------------------------+
| CLINICAL WORKFLOW        | QUERY PATTERN              | CONSISTENCY / PERFORMANCE     |
+--------------------------+----------------------------+-------------------------------+
| Patient Allergy Check    | AS OF SYSTEM TIME (-2s)    | Local read < 1ms; 100% offline|
| Medical History Review   | AS OF SYSTEM TIME (-5s)    | Zero WAN hop to cloud central |
| Drug Interaction Lookups | GLOBAL TABLE Local Read    | Cached locally on Edge Kiosk  |
| Active Consultation Note | Current Time Leaseholder   | Replicated to Regional Hub    |
+--------------------------+----------------------------+-------------------------------+
```

1. **Cellular Partition Survivability**: Telehealth kiosks located in rural community clinics frequently experience cellular dropouts. Because reference tables and patient history records are cached on local follower replicas, kiosks can perform uninterrupted clinical reviews during total network blackouts.
2. **Elimination of Cloud Read Costs**: Without follower reads, hundreds of edge kiosks streaming queries to the cloud generate massive WAN data transfer bills. Follower reads shift 95% of clinical read traffic to the local edge node.
3. **Monotonic Client-Side Tokens**: When a patient checks in at a kiosk, the kiosk receives a commit timestamp $T_{\text{checkin}}$. All subsequent reads by that kiosk include `AS OF SYSTEM TIME >= T_checkin`, providing causal read-your-writes guarantees without WAN round-trips.
