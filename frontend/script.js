// ---------------------------------------------------------------------------
// Distributed Voting System - frontend
// Talks to the three Flask nodes using fetch().
// ---------------------------------------------------------------------------
const NODES = {
  1: "http://127.0.0.1:5001",
  2: "http://127.0.0.1:5002",
  3: "http://127.0.0.1:5003",
};
let selectedCandidate = null;

// fetch with a timeout; returns {ok, status, data} and never throws
async function api(nodeId, path, options = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 4000);
  try {
    const res = await fetch(NODES[nodeId] + path, {
      ...options,
      headers: { "Content-Type": "application/json" },
      signal: controller.signal,
    });
    const data = await res.json().catch(() => ({}));
    return { ok: res.ok, status: res.status, data };
  } catch (err) {
    return { ok: false, status: 0, data: { error: `Node ${nodeId} is unreachable` } };
  } finally {
    clearTimeout(timer);
  }
}

// Node 1 is the primary. If it is down, fall back to the next ONLINE node
// (fault tolerance: the system keeps working while one node is failed).
async function callAvailable(path, options) {
  let last = null;
  for (const id of [1, 2, 3]) {
    const r = await api(id, path, options);
    if (r.status !== 503 && r.status !== 0) return { ...r, node: id };
    last = r;
  }
  return { ...last, node: null };
}

// ---------------- Navigation ----------------
function showSection(name) {
  document.querySelectorAll(".section").forEach((s) => s.classList.toggle("active", s.id === name));
  document.querySelectorAll(".nav-link").forEach((l) => l.classList.toggle("active", l.dataset.section === name));
  history.replaceState(null, "", "#" + name);
  refreshAll();
}
document.querySelectorAll(".nav-link").forEach((link) =>
  link.addEventListener("click", (e) => { e.preventDefault(); showSection(link.dataset.section); })
);

// ---------------- Voting ----------------
document.querySelectorAll(".candidate").forEach((btn) =>
  btn.addEventListener("click", () => {
    document.querySelectorAll(".candidate").forEach((b) => b.classList.remove("selected"));
    btn.classList.add("selected");
    selectedCandidate = btn.dataset.candidate;
  })
);

function showMessage(el, text, type) {
  el.textContent = text;
  el.className = `message ${type}`;
}

document.getElementById("castBtn").addEventListener("click", async () => {
  const msg = document.getElementById("voteMessage");
  const list = document.getElementById("replicationList");
  const voterId = document.getElementById("voterId").value.trim();
  list.innerHTML = "";
  if (!voterId) return showMessage(msg, "✗ Voter ID is required", "error");
  if (!selectedCandidate) return showMessage(msg, "✗ Please select a candidate", "error");

  const btn = document.getElementById("castBtn");
  btn.disabled = true;
  const r = await callAvailable("/api/vote", {
    method: "POST",
    body: JSON.stringify({ voter_id: voterId, candidate: selectedCandidate }),
  });
  btn.disabled = false;

  if (r.ok && r.data.success) {
    showMessage(msg, `✓ Vote successfully recorded (coordinator: Node ${r.data.coordinator})`, "success");
    for (const [node, status] of Object.entries(r.data.replication)) {
      const li = document.createElement("li");
      li.className = status === "failed" ? "error" : "success";
      li.textContent = `${status === "failed" ? "✗" : "✓"} ${node.replace("node", "Node ")}: ${status}`;
      list.appendChild(li);
    }
    showFlow(r.data);
    document.getElementById("voterId").value = "";
    document.querySelectorAll(".candidate").forEach((b) => b.classList.remove("selected"));
    selectedCandidate = null;
  } else {
    showMessage(msg, "✗ " + (r.data.error || "All nodes are unavailable"), "error");
  }
  refreshAll();
});

// ---------------- Visualization ----------------
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));

// Show the replication path step by step: Vote received -> Replicating -> Node X ✓/✗ -> done
async function showFlow(data) {
  const ul = document.getElementById("flowSteps");
  ul.innerHTML = "";
  const add = (text, cls = "") => { const li = document.createElement("li"); li.className = cls; li.textContent = text; ul.appendChild(li); };
  document.querySelectorAll(".topo-node").forEach((n) => n.classList.remove("ok", "bad", "coord"));
  document.getElementById("topo" + data.coordinator).classList.add("coord");
  add(`Vote received by Node ${data.coordinator} (coordinator)`, "success");
  await sleep(400); add("↓"); add("Replicating to peers...");
  let failed = 0;
  for (const [node, status] of Object.entries(data.replication)) {
    const id = node.replace("node", "");
    if (Number(id) === data.coordinator) continue;
    await sleep(400);
    const ok = status !== "failed";
    if (!ok) failed++;
    document.getElementById("topo" + id).classList.add(ok ? "ok" : "bad");
    add(`Node ${id} ${ok ? "✓" : "✗ (unavailable - skipped)"}`, ok ? "success" : "error");
  }
  await sleep(400); add("↓");
  add(failed ? "Vote stored on available nodes (fault tolerance)" : "Vote replicated successfully", failed ? "warn" : "success");
}

// ---------------- Nodes ----------------
const STATUS_ICON = { ONLINE: "🟢", FAILED: "🔴", RECOVERING: "🟡" };

async function loadStatus() {
  // Ask each node directly for its own health, so we still see the others
  // even when one node is completely unreachable.
  const nodes = await Promise.all([1, 2, 3].map(async (id) => {
    const r = await api(id, "/api/health");
    return r.ok ? r.data : { node: id, url: NODES[id], status: "FAILED", votes: 0, unreachable: true };
  }));
  const active = nodes.filter((n) => n.status === "ONLINE").length;

  document.getElementById("activeNodes").textContent = `${active}/3`;
  document.getElementById("statActive").textContent = `${active}/3`;
  const sys = document.getElementById("systemStatus");
  if (active === 3) { sys.textContent = "SYSTEM ONLINE"; sys.className = "system-status online"; }
  else if (active > 0) { sys.textContent = "SYSTEM ONLINE (DEGRADED)"; sys.className = "system-status degraded"; }
  else { sys.textContent = "SYSTEM OFFLINE"; sys.className = "system-status offline"; }
  document.getElementById("statSystem").textContent = active === 3 ? "Healthy" : active > 0 ? "Degraded" : "Offline";

  const grid = document.getElementById("nodeGrid");
  grid.innerHTML = nodes.map((n) => {
    const cls = n.status.toLowerCase();
    return `
      <div class="card node-card ${cls}">
        <h3>NODE ${n.node}</h3>
        <div class="url">${n.url}${n.unreachable ? " (process not reachable)" : ""}</div>
        <div class="status-badge ${cls}">${STATUS_ICON[n.status] || ""} ${n.status}</div>
        <div class="node-votes">Votes: <strong>${n.votes}</strong></div>
        <div class="node-counts">${n.counts && n.status !== "FAILED" ? Object.entries(n.counts).map(([c, v]) => `<span>${c.replace("Candidate ", "")}: <b>${v}</b></span>`).join("") : "<span class='muted'>no data</span>"}</div>
        <div class="node-actions">
          <button class="btn btn-danger" onclick="failNode(${n.node})" ${n.status !== "ONLINE" ? "disabled" : ""}>FAIL NODE</button>
          <button class="btn btn-success" onclick="recoverNode(${n.node})" ${n.status !== "FAILED" || n.unreachable ? "disabled" : ""}>RECOVER NODE</button>
        </div>
      </div>`;
  }).join("");
}

async function failNode(id) {
  const r = await api(id, "/api/node/fail", { method: "POST" });
  if (!r.ok) alert(r.data.error || `Could not reach Node ${id}`);
  refreshAll();
}

async function recoverNode(id) {
  const r = await api(id, "/api/node/recover", { method: "POST" });
  if (!r.ok) alert(r.data.error || `Could not reach Node ${id}`);
  refreshAll();
  // Recovery runs in the background (RECOVERING -> ONLINE); refresh again shortly
  setTimeout(refreshAll, 2500);
}

document.getElementById("syncBtn").addEventListener("click", async () => {
  const out = document.getElementById("syncMessage");
  out.textContent = "Synchronizing...";
  const r = await callAvailable("/api/sync", { method: "POST" });
  if (r.ok && r.data.success) {
    out.className = "sync-msg success";
    out.textContent = `✓ Synced nodes ${r.data.synced_nodes.join(", ")} → ${r.data.total_votes} votes` +
      (r.data.skipped_nodes.length ? ` (skipped failed: ${r.data.skipped_nodes.join(", ")})` : "");
  } else {
    out.className = "sync-msg error";
    out.textContent = "✗ " + (r.data.error || "No node available for synchronization");
  }
  refreshAll();
});

// ---------------- Results ----------------
async function loadResults() {
  const r = await callAvailable("/api/results");
  const bars = document.getElementById("resultBars");
  if (!r.ok) {
    bars.innerHTML = `<p class="muted">Results unavailable: ${r.data.error || "no node online"}</p>`;
    document.getElementById("statVotes").textContent = "-";
    document.getElementById("statLeader").textContent = "-";
    return;
  }
  const { counts, total_votes, winner } = r.data;
  document.getElementById("totalVotes").textContent = total_votes;
  document.getElementById("statVotes").textContent = total_votes;
  document.getElementById("statLeader").textContent = !winner ? "-" : winner.startsWith("Tie") ? "Tie" : winner;
  bars.innerHTML = Object.entries(counts).map(([name, count]) => {
    const pct = total_votes ? Math.round((count / total_votes) * 100) : 0;
    return `
      <div class="result-row">
        <div class="result-label"><span>${name}</span><span>${count} votes (${pct}%)</span></div>
        <div class="bar"><div class="bar-fill" style="width:${pct}%"></div></div>
      </div>`;
  }).join("");
  document.getElementById("winner").textContent = winner ? `🏆 ${winner}` : "No votes yet";
  document.getElementById("resultSource").textContent = `Results reported by Node ${r.data.reported_by}`;
}

// ---------------- Activity log ----------------
const LOG_ICON = { success: "✓", warn: "⚠", error: "✗", info: "•" };

function escapeHtml(s) {
  return s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

async function loadLogs() {
  // Merge the logs of all reachable nodes into one timeline (newest first)
  const all = (await Promise.all([1, 2, 3].map((id) => api(id, "/api/logs"))))
    .filter((r) => r.ok).flatMap((r) => r.data.logs);
  all.sort((a, b) => b.time.localeCompare(a.time));
  const ul = document.getElementById("activityLog");
  ul.innerHTML = all.slice(0, 60).map((l) => `
    <li><span class="time">${l.time.slice(11, 19)}</span>
    <span class="${l.level}">${LOG_ICON[l.level] || "•"} [Node ${l.node}] ${escapeHtml(l.message)}</span></li>`
  ).join("") || '<li class="muted">No activity yet. Start the nodes with: python start_nodes.py</li>';
}

// ---------------- Refresh ----------------
let refreshing = false;
async function refreshAll() {
  if (refreshing) return; // avoid piling up requests when a node is slow
  refreshing = true;
  try { await Promise.all([loadStatus(), loadResults(), loadLogs()]); }
  catch (e) { console.error(e); } // one bad response must never stop the dashboard
  finally { refreshing = false; }
}

const initial = location.hash.replace("#", "");
showSection(["home", "vote", "nodes", "results"].includes(initial) ? initial : "home");
setInterval(refreshAll, 3000);
