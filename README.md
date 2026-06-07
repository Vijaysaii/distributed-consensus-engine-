# distributed-consensus-engine

**Assignment 1 — Fundamentals of Distributed Systems**
**IIT Jodhpur | G25AI1016 | Vijay Sai Krishna Devabhakthuni**

---

## What This Project Does

This is a distributed, append-only transaction ledger running across 5 nodes. The system supports two modes:

- **Mode A** — Crash fault tolerance using Basic Paxos + Leader Election
- **Mode B** — Byzantine fault tolerance using PBFT with RSA-PSS signatures

---

## Folder Structure

```
distributed-consensus-engine/
├── src/
│   ├── node.py          # Main node daemon (Leader Election + Paxos + PBFT)
│   ├── adversary.py     # Byzantine adversary node
│   ├── client.py        # Submits concurrent transactions
│   └── crypto_utils.py  # RSA key generation, signing, verification
├── tests/
│   └── chaos_test.sh    # Injects latency, partitions, and crashes
├── Dockerfile
├── docker-compose.yml   # 5 nodes + adversary + client + toxiproxy
├── requirements.txt
└── README.md
```

---

## How to Run

### Requirements
- Docker Desktop installed and running
- Docker Compose v2

### Start the cluster (Mode A — Paxos)

```bash
docker compose up --build
```

### Check all containers are running

```bash
docker compose ps
```

### View logs from all nodes

```bash
docker compose logs node0 node1 node2
```

### Run the chaos test

```bash
bash tests/chaos_test.sh
```

### Switch to Mode B (PBFT)

Edit `docker-compose.yml` and change `MODE=A` to `MODE=B` for all node services, then restart:

```bash
docker compose down
docker compose up --build
```

---

## How It Works

### Leader Election
Node 0 starts as leader. Every 1 second the leader sends a HEARTBEAT to all peers. If a node gets no heartbeat for 3 seconds it starts a new election, increments the term, and broadcasts NEW_LEADER. Other nodes accept the new leader if the term is higher than what they have seen.

### Paxos (Mode A)
Standard 4-phase Basic Paxos: PREPARE → PROMISE → ACCEPT → ACCEPTED. Transactions are written to `/tmp/ledger_nodeX.log` only after majority consensus is reached.

### PBFT (Mode B)
3-phase PBFT: PRE_PREPARE → PREPARE → COMMIT. Every message is signed with RSA-PSS (SHA-256). Nodes verify every signature before counting a vote. The adversary node's forged messages are rejected because its signatures fail verification.

### Adversary Node
Performs equivocation (sending different values to different peers) and message suppression (randomly dropping commits). Both attacks fail because honest nodes need 3 matching signed votes.

---

## Dependencies

```
cryptography==41.0.5
aiohttp==3.9.1
```
