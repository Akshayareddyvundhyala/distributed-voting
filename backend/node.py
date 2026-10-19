"""
Distributed Voting System - a single voting node.

Run three copies of this file on different ports to form the cluster:
    python node.py 5001
    python node.py 5002
    python node.py 5003

Every node keeps its OWN copy of the votes (in memory + a JSON file),
so the nodes behave like independent servers that talk over HTTP.
"""
import json
import os
import sys
import threading
import time
import uuid
from datetime import datetime

import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

# ---------------------------------------------------------------------------
# Cluster configuration
# ---------------------------------------------------------------------------
NODES = {
    1: "http://127.0.0.1:5001",
    2: "http://127.0.0.1:5002",
    3: "http://127.0.0.1:5003",
}
CANDIDATES = ["Candidate A", "Candidate B", "Candidate C"]
RECOVERY_DELAY = 2  # seconds a node stays RECOVERING (so the demo can show it)
TIMEOUT = 2  # seconds to wait for another node before treating it as unreachable

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 5001
NODE_ID = {5001: 1, 5002: 2, 5003: 3}.get(PORT, 1)
PEERS = {nid: url for nid, url in NODES.items() if nid != NODE_ID}

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DATA_FILE = os.path.join(DATA_DIR, f"node{NODE_ID}.json")

app = Flask(__name__)
CORS(app)  # allow the frontend (opened from file:// or another port) to call us

lock = threading.Lock()  # protects shared state between Flask threads
state = {
    "status": "ONLINE",  # ONLINE | FAILED | RECOVERING
    "votes": [],          # list of {vote_id, voter_id, candidate, timestamp, received_by}
}
logs = []                 # recent activity events of THIS node


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def now():
    return datetime.now().isoformat(timespec="milliseconds")


def log(message, level="info"):
    """Record a distributed-system event (kept to the last 100 entries)."""
    logs.append({"time": now(), "node": NODE_ID, "level": level, "message": message})
    del logs[:-100]
    print(f"[Node {NODE_ID}] {message}")


def save():
    """Persist this node's votes to its own JSON file."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(state["votes"], f, indent=2)


def load():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE) as f:
                state["votes"] = json.load(f)
        except (ValueError, OSError):
            state["votes"] = []


def has_vote_id(vote_id):
    return any(v["vote_id"] == vote_id for v in state["votes"])


def has_voter(voter_id):
    return any(v["voter_id"].lower() == voter_id.lower() for v in state["votes"])


def store_vote(vote):
    """Store a vote only once (vote_id prevents duplicate replication)."""
    if has_vote_id(vote["vote_id"]) or has_voter(vote["voter_id"]):
        return False
    state["votes"].append(vote)
    state["votes"].sort(key=lambda v: v["timestamp"])
    save()
    return True


def is_online():
    return state["status"] == "ONLINE"


def offline_response():
    return jsonify({"success": False, "error": f"Node {NODE_ID} is {state['status']}"}), 503


def peer_get(nid, path):
    """GET from another node. Returns JSON or None if the node is unreachable."""
    try:
        r = requests.get(PEERS[nid] + path, timeout=TIMEOUT)
        return r.json() if r.ok else None
    except requests.RequestException:
        return None


def peer_post(nid, path, payload=None):
    """POST to another node. Returns (ok, json_or_None) and never raises."""
    try:
        r = requests.post(PEERS[nid] + path, json=payload or {}, timeout=TIMEOUT)
        try:
            body = r.json()
        except ValueError:
            body = None
        return r.ok, body
    except requests.RequestException:
        return False, None


def counts(votes):
    c = {name: 0 for name in CANDIDATES}
    for v in votes:
        if v["candidate"] in c:
            c[v["candidate"]] += 1
    return c


def own_info():
    return {"node": NODE_ID, "url": NODES[NODE_ID], "status": state["status"],
            "votes": len(state["votes"]), "counts": counts(state["votes"])}


# ---------------------------------------------------------------------------
# Public / internal APIs
# ---------------------------------------------------------------------------
@app.get("/api/health")
def health():
    """Basic info about this node only (used by other nodes)."""
    return jsonify(own_info())


@app.get("/api/state")
def get_state():
    """Full vote list - used by recovery and synchronization."""
    if not is_online():
        return offline_response()
    return jsonify({"node": NODE_ID, "votes": state["votes"]})


@app.get("/api/status")
def status():
    """Status of all three nodes, as seen from this node."""
    nodes = []
    for nid in sorted(NODES):
        if nid == NODE_ID:
            nodes.append(own_info())
        else:
            info = peer_get(nid, "/api/health")
            # A node whose process is not running at all is also reported as FAILED.
            nodes.append(info or {"node": nid, "url": NODES[nid], "status": "FAILED",
                                  "votes": 0, "unreachable": True})
    active = sum(1 for n in nodes if n["status"] == "ONLINE")
    return jsonify({"nodes": nodes, "active_nodes": active, "total_nodes": len(NODES),
                    "reported_by": NODE_ID})


@app.get("/api/results")
def results():
    if not is_online():
        return offline_response()
    c = counts(state["votes"])
    total = sum(c.values())
    winner = None
    if total:
        top = max(c.values())
        leaders = [k for k, v in c.items() if v == top]
        winner = leaders[0] if len(leaders) == 1 else "Tie: " + ", ".join(leaders)
    active = 1 + sum(1 for nid in PEERS
                     if (peer_get(nid, "/api/health") or {}).get("status") == "ONLINE")
    return jsonify({"counts": c, "total_votes": total, "active_nodes": active,
                    "winner": winner, "reported_by": NODE_ID})


@app.post("/api/vote")
def vote():
    """
    Receive a vote from the frontend. This node acts as the coordinator:
    validate -> store locally -> replicate to every peer.
    """
    if not is_online():
        return offline_response()
    data = request.get_json(silent=True) or {}
    voter_id = str(data.get("voter_id", "")).strip()
    candidate = str(data.get("candidate", "")).strip()

    if not voter_id:
        return jsonify({"success": False, "error": "Voter ID is required"}), 400
    if candidate not in CANDIDATES:
        return jsonify({"success": False, "error": "Please select a valid candidate"}), 400

    with lock:
        if has_voter(voter_id):
            log(f"Duplicate vote rejected for voter '{voter_id}'", "warn")
            return jsonify({"success": False, "error": "This voter ID has already voted"}), 409
        new_vote = {
            "vote_id": str(uuid.uuid4()),
            "voter_id": voter_id,
            "candidate": candidate,
            "timestamp": now(),
            "received_by": NODE_ID,
        }
        store_vote(new_vote)
        log(f"Vote received by Node {NODE_ID} (voter '{voter_id}')", "success")

    # Replicate to peers. A failed peer must NOT break the vote.
    replication = {f"node{NODE_ID}": "stored"}
    for nid in PEERS:
        ok, body = peer_post(nid, "/api/replicate", new_vote)
        if ok:
            replication[f"node{nid}"] = "replicated"
            log(f"Vote replicated to Node {nid}", "success")
        else:
            replication[f"node{nid}"] = "failed"
            log(f"Node {nid} failure detected - replication skipped", "warn")
    stored_on = sum(1 for v in replication.values() if v != "failed")
    log(f"Vote processed by {stored_on}/{len(NODES)} available nodes", "info")

    return jsonify({"success": True, "message": "Vote successfully recorded",
                    "vote_id": new_vote["vote_id"], "coordinator": NODE_ID,
                    "replication": replication})


@app.post("/api/replicate")
def replicate():
    """Internal: another node sends us a copy of a vote."""
    if not is_online():
        return offline_response()
    v = request.get_json(silent=True) or {}
    if not all(k in v for k in ("vote_id", "voter_id", "candidate", "timestamp")):
        return jsonify({"success": False, "error": "Invalid vote payload"}), 400
    with lock:
        added = store_vote(v)
    if added:
        log(f"Replicated vote stored (from Node {v.get('received_by')})", "success")
    return jsonify({"success": True, "duplicate": not added})


@app.post("/api/load_state")
def load_state():
    """Internal: overwrite our votes with a consistent copy (used by sync)."""
    if not is_online():
        return offline_response()
    votes = (request.get_json(silent=True) or {}).get("votes", [])
    with lock:
        # Merge with what we already have so a vote that arrived during the sync is not lost.
        state["votes"] = merge([votes, state["votes"]])
        save()
    log(f"State updated by synchronization ({len(votes)} votes)", "info")
    return jsonify({"success": True, "votes": len(votes)})


def merge(vote_lists):
    """Merge several vote lists: unique by vote_id, one vote per voter (earliest wins)."""
    all_votes = sorted((v for lst in vote_lists for v in lst), key=lambda v: v["timestamp"])
    merged, ids, voters = [], set(), set()
    for v in all_votes:
        if v["vote_id"] in ids or v["voter_id"].lower() in voters:
            continue
        ids.add(v["vote_id"])
        voters.add(v["voter_id"].lower())
        merged.append(v)
    return merged


@app.post("/api/sync")
def sync():
    """
    Synchronize all ONLINE nodes:
    1. collect vote state from every healthy node
    2. merge into the latest consistent state
    3. push that state to every healthy node (failed nodes are skipped)
    """
    if not is_online():
        return offline_response()
    collected = {NODE_ID: list(state["votes"])}
    skipped = []
    for nid in PEERS:
        s = peer_get(nid, "/api/state")
        if s is None:
            skipped.append(nid)
        else:
            collected[nid] = s["votes"]
    merged = merge(collected.values())

    with lock:
        state["votes"] = merged
        save()
    synced = [NODE_ID]
    for nid in collected:
        if nid != NODE_ID:
            ok, _ = peer_post(nid, "/api/load_state", {"votes": merged})
            (synced if ok else skipped).append(nid)
    log(f"Synchronization completed: nodes {sorted(synced)} have {len(merged)} votes"
        + (f", skipped failed nodes {sorted(skipped)}" if skipped else ""), "success")
    return jsonify({"success": True, "synced_nodes": sorted(synced),
                    "skipped_nodes": sorted(skipped), "total_votes": len(merged)})


@app.post("/api/node/fail")
def fail():
    """Simulate a crash: the node stays reachable only to report FAILED."""
    with lock:
        state["status"] = "FAILED"
    log(f"Node {NODE_ID} has FAILED (simulated)", "error")
    return jsonify({"success": True, "node": NODE_ID, "status": "FAILED"})


def _do_recovery():
    """Background recovery: wait briefly, pull state from ALL healthy peers, go ONLINE."""
    time.sleep(RECOVERY_DELAY)
    peer_states = []
    sources = []
    for nid in PEERS:
        s = peer_get(nid, "/api/state")  # FAILED/unreachable peers return None
        if s is not None:
            peer_states.append(s["votes"])
            sources.append(nid)
    with lock:
        # Merge: keep everything we had, add every vote we missed (no duplicates).
        state["votes"] = merge(peer_states + [state["votes"]])
        save()
        state["status"] = "ONLINE"
    source = f"Node(s) {sources}" if sources else "local data only (no healthy peer found)"
    log(f"Node {NODE_ID} recovered - state copied from {source} "
        f"({len(state['votes'])} votes)", "success")


@app.post("/api/node/recover")
def recover():
    """
    Recovery: FAILED -> RECOVERING -> (copy latest state from healthy peers) -> ONLINE.
    Runs in the background so the dashboard can show the RECOVERING state.
    """
    with lock:
        if state["status"] != "FAILED":
            return jsonify({"success": True, "node": NODE_ID, "status": state["status"],
                            "message": f"Node is already {state['status']}"})
        state["status"] = "RECOVERING"
    log(f"Node {NODE_ID} is RECOVERING - fetching latest state from peers", "warn")
    threading.Thread(target=_do_recovery, daemon=True).start()
    return jsonify({"success": True, "node": NODE_ID, "status": "RECOVERING"})


@app.get("/api/logs")
def get_logs():
    """Recent activity of this node (works even when FAILED, for the dashboard)."""
    return jsonify({"node": NODE_ID, "logs": logs[-50:]})


if __name__ == "__main__":
    load()
    log(f"Node {NODE_ID} started on port {PORT} with {len(state['votes'])} stored votes")
    # threaded=True so a node can serve requests while it waits on peers
    app.run(host="127.0.0.1", port=PORT, debug=False, threaded=True)
