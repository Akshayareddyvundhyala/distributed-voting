# Distributed Voting System
*A Fault-Tolerant Distributed Election Platform (Distributed Computing academic project)*

## Problem
A centralized voting server is a **single point of failure**: if it crashes, nobody can vote and votes may be lost.

## Solution
Run **multiple independent voting nodes**. Every node keeps its own copy of all votes, votes are **replicated** to the other nodes, failed nodes are **detected and skipped**, and recovered nodes **catch up** from healthy nodes. A **synchronization** step makes all healthy nodes consistent.

## Architecture
```
            VOTER
              |
          FRONTEND  (HTML/CSS/JS, fetch())
              |
           NODE 1  :5001  (primary coordinator)
          /        \
   NODE 2 :5002   NODE 3 :5003
          \        /
       SYNCHRONIZATION
              |
        FINAL RESULTS
```
Each node is a Flask process that stores votes in memory and in `backend/data/nodeX.json`.

## Distributed Computing Concepts
| Concept | How it is done |
|---|---|
| Distributed nodes | 3 independent Flask processes (ports 5001-5003), each with its own data |
| Communication | Nodes talk over HTTP using `requests` (2 s timeout) |
| Replication | Coordinator stores the vote, then POSTs it to `/api/replicate` on each peer |
| Failure detection | A peer that times out, is unreachable or answers 503 is marked failed and skipped |
| Fault tolerance | The vote still succeeds on the available nodes; if Node 1 is down the frontend uses Node 2, then Node 3 |
| Recovery | FAILED → RECOVERING → pulls state from all healthy peers and merges it → ONLINE |
| Synchronization | `/api/sync` collects all online nodes' votes, merges them, pushes the same state back |
| Consistency / idempotency | Every vote has a unique UUID `vote_id`; one vote per voter ID; merging never duplicates |

## Installation
```bash
cd distributed-voting/backend
python -m venv venv
# Windows:   venv\Scripts\activate
# Mac/Linux: source venv/bin/activate
pip install -r requirements.txt
```

## Running
From `backend/` (with the venv active):
```bash
python start_nodes.py
```
Or in three terminals:
```bash
python node.py 5001
python node.py 5002
python node.py 5003
```
Open the frontend: double-click `frontend/index.html`, or run `python -m http.server 8000` inside `frontend/` and open http://localhost:8000.

Reset the election: stop the nodes and delete `backend/data`.

## API Overview
| Method | Path | Purpose |
|---|---|---|
| POST | /api/vote | Cast a vote `{voter_id, candidate}` (node acts as coordinator) |
| POST | /api/replicate | Internal: receive a replicated vote |
| GET | /api/results | Counts, total, active nodes, winner |
| GET | /api/status | Status of all 3 nodes |
| POST | /api/sync | Synchronize all online nodes |
| POST | /api/node/fail | Simulate a node failure |
| POST | /api/node/recover | Recover a failed node |
| GET | /api/logs | Recent activity of a node |
| GET | /api/health, /api/state | Internal helpers (node info, full vote list) |

## Demo Steps
1. Start the nodes and open the frontend → Home shows **SYSTEM ONLINE**, 3/3 nodes.
2. **Vote** as `STU1` for Candidate A → the diagram shows Node 2 ✓, Node 3 ✓ (replication).
3. Vote as `STU1` again → rejected as a duplicate.
4. **Nodes** → **FAIL NODE** on Node 2 → red FAILED, 2/3 active.
5. Vote as `STU2` → still recorded; diagram shows Node 2 ✗ (fault tolerance).
6. **RECOVER NODE** on Node 2 → yellow RECOVERING, then green ONLINE with the missing vote.
7. **SYNCHRONIZE NODES** → all nodes show identical counts; **Results** shows bars and the winner.

## Known Limitations
- Simple academic model: no consensus algorithm (e.g. Raft); Node 1 is a fixed preferred coordinator.
- If two different nodes accept a vote from the same voter while they cannot reach each other (network partition), the merge keeps only the earliest one.
- All nodes run on one machine (127.0.0.1); no authentication of voters.
