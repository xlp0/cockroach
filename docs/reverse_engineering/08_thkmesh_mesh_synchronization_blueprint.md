# THKMesh Architecture Blueprint: Multi-Location & Multi-Database Synchronization

## 1. Executive Summary & Problem Formulation

The **GovTech Telehealth Kiosk Mesh (THKMesh)** is a mission-critical public health infrastructure connecting hundreds of unattended community telehealth kiosks, regional polyclinics, acute-care hospitals, and national health cloud systems.

### Operational Challenges of THKMesh
1. **Flaky Network Connectivity**: Kiosks operate over public 4G/5G cellular connections subject to frequent signal degradation, packet loss, and extended offline blackouts.
2. **Local Operational Autonomy**: A citizen presenting for medical triage, biometric scanning, or tele-consultation must never be turned away due to cloud network outages. Kiosks must register patients, measure vitals, and issue prescriptions in 100% offline mode.
3. **Medical Data Integrity & Zero Data Loss**: Clinical information cannot tolerate silent overwrites, split-brain race conditions, or dropped sensor records.
4. **Data Sovereignty & Strict Privacy**: Patient identifiers and consultation recordings must reside within authorized regional security perimeters, synchronizing only compliant records upstream.

By extracting and synthesizing the reverse-engineered architectural patterns from CockroachDB (Multi-Raft, HLC, Closed Timestamps, PCR, and LDR), this blueprint provides the comprehensive engineering design for THKMesh.

---

## 2. End-to-End Three-Tier Topology Architecture

```mermaid
graph TB
    subgraph SG_Tier1 ["Tier 1: Edge Telehealth Kiosks (Community & Polyclinics)"]
        Kiosk1["Kiosk 101: Ang Mo Kio<br/>Embedded Local DB<br/>HLC Clock Generator<br/>Outbox Sync Worker"]
        Kiosk2["Kiosk 102: Bedok<br/>Embedded Local DB<br/>HLC Clock Generator<br/>Outbox Sync Worker"]
        Kiosk3["Kiosk 103: Jurong<br/>Embedded Local DB<br/>HLC Clock Generator<br/>Outbox Sync Worker"]
    end

    subgraph SG_Tier2 ["Tier 2: Regional Hospital Hubs (Active-Active Clusters)"]
        subgraph SG_HubNorth ["Regional Hub North (Khoo Teck Puat Hospital)"]
            HubN_C["CockroachDB Multi-Node Cluster<br/>REGIONAL BY ROW Locality"]
            HubN_LDR["LDR Processor & LWW Resolver"]
            HubN_DLQ["Clinical Review DLQ"]
        end
        
        subgraph SG_HubWest ["Regional Hub West (Ng Teng Fong Hospital)"]
            HubW_C["CockroachDB Multi-Node Cluster<br/>REGIONAL BY ROW Locality"]
            HubW_LDR["LDR Processor & LWW Resolver"]
            HubW_DLQ["Clinical Review DLQ"]
        end
    end

    subgraph SG_Tier3 ["Tier 3: National Central Health Cloud (NEHR / MOH Data Center)"]
        Cloud_Primary["Primary CockroachDB Cluster<br/>Multi-Region Enterprise Fabric"]
        Cloud_Standby["Standby Disaster Recovery Cluster<br/>Physical Cluster Replication - PCR"]
        Cloud_CDC["CDC Streaming Engine"]
        NationalEHR[("National Electronic Health Record - NEHR")]
    end

    Kiosk1 -.-> |"Intermittent 4G Cellular<br/>Delta Sync via Resolved Tokens"| HubN_LDR
    Kiosk2 -.-> |"Intermittent 4G Cellular<br/>Delta Sync via Resolved Tokens"| HubN_LDR
    Kiosk3 -.-> |"Intermittent 5G Cellular<br/>Delta Sync via Resolved Tokens"| HubW_LDR

    HubN_LDR <==> |"Bi-Directional Active-Active LDR<br/>pgwire Stream / Txn Scheduler"| HubW_LDR
    
    HubN_C --> |"Upstream Changefeed"| Cloud_Primary
    HubW_C --> |"Upstream Changefeed"| Cloud_Primary
    
    Cloud_Primary ==> |"Physical Cluster Replication (PCR)<br/>ReplicatedTime RPO < 1s"| Cloud_Standby
    Cloud_Primary --> Cloud_CDC
    Cloud_CDC --> NationalEHR
```

---

## 3. Adaptation of CockroachDB Core Principles to THKMesh

| CockroachDB Pattern | Subsystem Origin | THKMesh Edge/Hub Realization | Value Delivered |
| :--- | :--- | :--- | :--- |
| **Hybrid Logical Clocks (HLC)** | [`pkg/util/hlc/hlc.go`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/util/hlc/hlc.go) | Kiosk clients stamp all sensor reads and triage events with a local HLC token $\langle \text{wall}, \text{logical} \rangle$. | Guarantees absolute causal ordering across distributed clinics even if kiosk clocks drift by minutes. |
| **Closed Timestamps & Follower Reads** | [`pkg/kv/kvserver/closedts`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/kv/kvserver/closedts) | Edge kiosks maintain local follower replicas of reference catalogs (drugs, staff, diagnostic codes). | **100% offline read availability**; drug allergy lookups execute in $<1$ms without WAN reliance. |
| **Logical Replication & LWW** | [`pkg/crosscluster/logical`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/crosscluster/logical) | Hubs synchronize patient records using conditional SQL updates against MVCC origin timestamps. | Automatic, mathematically deterministic resolution of concurrent doctor/patient profile edits. |
| **Transaction Dependency Scheduler** | [`txnscheduler/scheduler.go`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/crosscluster/logical/txnscheduler/scheduler.go) | Regional hubs maintain an in-memory lock table when applying batches from reconnecting kiosks. | Ingests offline batches concurrently across worker threads while preserving causal transaction chains. |
| **Resolved Span Frontiers** | [`changefeedccl/resolvedspan`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/ccl/changefeedccl/resolvedspan/) | Kiosks track a persistent sync token ($T_{\text{resolved}}$) representing the last acknowledged event. | Zero duplicate event transmissions; minimal cellular bandwidth consumption. |
| **Physical Cluster Replication (PCR)** | [`pkg/crosscluster/physical`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/crosscluster/physical) | National cloud primary mirrors to disaster recovery standby via byte-level SSTable stream ingestion. | Near-zero RPO ($<1$s) and instantaneous cutover RTO ($<5$s) for national healthcare availability. |
| **Dead Letter Queue (DLQ)** | [`dead_letter_queue.go`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/crosscluster/logical/dead_letter_queue.go) | Unresolvable clinical conflicts are isolated in `system.thkmesh_dlq` for clinician triage. | Zero silent data loss; complete clinical governance and audit trail. |

---

## 4. Edge-to-Hub Offline Resumption Lifecycle

```mermaid
sequenceDiagram
    autonumber
    actor Patient
    participant Kiosk as Telehealth Kiosk (Local SQLite)
    participant Sync as Kiosk Sync Worker
    participant Hub as Regional Hospital Hub (CockroachDB)
    participant Cloud as National Health Cloud

    Note over Kiosk,Sync: Network Link DOWN (Cellular Outage)
    Patient->>Kiosk: Tap NRIC / Identity Card
    Kiosk->>Kiosk: Capture Vitals: BP=125/82, HR=74, SpO2=98%
    Kiosk->>Kiosk: Generate HLC Timestamp T_event = 171000.1
    Kiosk->>Kiosk: Write to Local DB & Append to Local Outbox
    Note over Kiosk: Patient finishes triage successfully!

    Note over Sync,Hub: Network Link RESTORED (Cellular Connected)
    Sync->>Hub: Connect: KioskID=101, LastAckToken=170500.0
    Hub-->>Sync: Connection Accepted, Ready for Delta Stream
    
    Sync->>Hub: Push Batch: Checkin T=170999.0, Vitals T=171000.1
    
    Note over Hub: Hub applies CockroachDB LDR Pipeline:
    Hub->>Hub: 1. Build Txn Dependency DAG (Checkin -> Vitals)
    Hub->>Hub: 2. Evaluate Conditional LWW: crdb_origin_ts is older than incoming
    Hub->>Hub: 3. Commit Transactions to Regional Storage
    
    Hub-->>Sync: Ack Frontier Token T_resolved = 171000.1
    Sync->>Kiosk: Advance Local Frontier, Purge Acknowledged Outbox Rows
    
    Note over Hub,Cloud: Asynchronous Cloud Aggregation
    Hub->>Cloud: Stream Verified Encounter via RangeFeed CDC
```

---

## 5. Medical Conflict Resolution Matrix

When concurrent writes occur across edge kiosks and hospital terminals, THKMesh applies strict domain-specific conflict rules:

| Clinical Data Entity | Conflict Scenario | Resolution Policy | Technical Implementation |
| :--- | :--- | :--- | :--- |
| **Patient Demographics** (Address, Phone, Emergency Contact) | Kiosk updates phone number while hospital clerk updates home address. | **Last-Write-Wins (LWW)** based on originating HLC timestamp. | Conditional update: `WHERE crdb_origin_ts < $origin_ts` ([`lww_row_processor.go`](file:///Users/bkoo/Documents/Development/GovTech/THKMesh/cockroach/pkg/crosscluster/logical/lww_row_processor.go)). |
| **Clinical Consultation Notes** | Doctor in hospital edits notes while specialist adds telemetry observation. | **Additive Merge** (Never overwrite). | Appends independent consultation revision entries with UUIDs; UI displays revision history. |
| **Drug Prescriptions** | Doctor cancels prescription in hospital while patient attempts refill at kiosk. | **Safety Override + DLQ**. | Cancellation takes absolute priority; kiosk refill mutation is diverted to `thkmesh_dlq` with urgent nurse alert. |
| **Sensor Telemetry** (ECG, Vitals, Blood Glucose) | Pure time-series append. | **Idempotent Upsert**. | Primary key is `(patient_id, sensor_id, hlc_timestamp)`. Replicas ingest deduplicated points. |

---

## 6. Edge Kiosk Database Schema Specification

All THKMesh edge tables must incorporate CockroachDB-inspired synchronization metadata columns:

```sql
CREATE TABLE thk_patient_vitals (
    patient_id          UUID NOT NULL,
    recorded_at         TIMESTAMPTZ NOT NULL,
    systolic            INT NOT NULL,
    diastolic           INT NOT NULL,
    pulse_rate          INT NOT NULL,
    spo2_percent        NUMERIC(4, 1),
    temperature_c       NUMERIC(4, 2),
    
    -- CockroachDB Distributed Synchronization Metadata
    crdb_origin_ts      DECIMAL(20, 10) NOT NULL,   -- HLC Timestamp: <physical_epoch_ms>.<logical_counter>
    crdb_origin_node    VARCHAR(64) NOT NULL,       -- Originating Kiosk or Hub Identifier
    sync_status         VARCHAR(20) DEFAULT 'PENDING', -- 'PENDING', 'SYNCED', 'CONFLICT'
    
    PRIMARY KEY (patient_id, recorded_at)
);

CREATE INDEX idx_vitals_sync ON thk_patient_vitals (sync_status, crdb_origin_ts);
```

---

## 7. Implementation Roadmap & Milestones

```mermaid
gantt
    title THKMesh Synchronization Implementation Roadmap
    dateFormat  YYYY-MM-DD
    section Phase 1: Core Edge Engine
    HLC Library & SQLite Outbox Module         :p1_1, 2026-10-01, 30d
    Closed Timestamp Caching for Kiosks       :p1_2, after p1_1, 20d
    
    section Phase 2: Regional Hub Pipeline
    CockroachDB Multi-Region Cluster Deploy    :p2_1, 2026-11-01, 25d
    LDR Ingestion Worker & LWW Processor      :p2_2, after p2_1, 30d
    Txn Scheduler Lock Table Integration       :p2_3, after p2_2, 20d
    
    section Phase 3: National Cloud & Resilience
    PCR Disaster Recovery Standby Cluster      :p3_1, 2026-12-15, 30d
    Dead Letter Queue (DLQ) Triage Portal      :p3_2, after p3_1, 20d
    End-to-End Cellular Partition Chaos Test   :p3_3, after p3_2, 15d
```

### Milestone Deliverables:
- **Milestone A (Edge Resilience)**: Standalone kiosk demonstrator capable of 7-day disconnected offline triage with zero data corruption upon cellular reconnect.
- **Milestone B (Hub Multi-Master)**: Active-active cross-clinic synchronization with verified sub-second LWW conflict resolution.
- **Milestone C (National Disaster Recovery)**: Disaster cutover validation proving $RPO < 1$s and $RTO < 5$s across regional health clusters.
