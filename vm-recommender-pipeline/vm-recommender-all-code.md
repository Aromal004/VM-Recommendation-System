# VM Recommender — Complete Codebase

---

## `index.html`
> Upload to S3 bucket as the static website frontend.

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>VM Recommender</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:        #0a0c0f;
    --surface:   #111318;
    --border:    #1e2229;
    --accent:    #00d4ff;
    --accent2:   #00ff9d;
    --warn:      #ffb800;
    --danger:    #ff4757;
    --text:      #e2e8f0;
    --muted:     #4a5568;
    --mono:      'IBM Plex Mono', monospace;
    --sans:      'IBM Plex Sans', sans-serif;
  }

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--sans);
    min-height: 100vh;
    overflow-x: hidden;
  }

  body::before {
    content: '';
    position: fixed;
    inset: 0;
    background-image:
      linear-gradient(rgba(0,212,255,0.03) 1px, transparent 1px),
      linear-gradient(90deg, rgba(0,212,255,0.03) 1px, transparent 1px);
    background-size: 40px 40px;
    pointer-events: none;
    z-index: 0;
  }

  .wrap {
    position: relative;
    z-index: 1;
    max-width: 960px;
    margin: 0 auto;
    padding: 48px 24px 80px;
  }

  header {
    margin-bottom: 48px;
    border-left: 3px solid var(--accent);
    padding-left: 20px;
  }
  header .eyebrow {
    font-family: var(--mono);
    font-size: 11px;
    letter-spacing: 0.2em;
    color: var(--accent);
    text-transform: uppercase;
    margin-bottom: 8px;
  }
  header h1 { font-size: 32px; font-weight: 600; letter-spacing: -0.02em; line-height: 1.1; }
  header p  { margin-top: 8px; color: var(--muted); font-size: 14px; font-family: var(--mono); }

  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 28px;
    margin-bottom: 24px;
  }
  .card-title {
    font-family: var(--mono);
    font-size: 11px;
    letter-spacing: 0.15em;
    color: var(--accent);
    text-transform: uppercase;
    margin-bottom: 20px;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .card-title::after {
    content: '';
    flex: 1;
    height: 1px;
    background: var(--border);
  }

  .form-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 16px;
  }
  .form-group { display: flex; flex-direction: column; gap: 6px; }
  .form-group.full { grid-column: 1 / -1; }

  label {
    font-family: var(--mono);
    font-size: 11px;
    letter-spacing: 0.1em;
    color: var(--muted);
    text-transform: uppercase;
  }

  input {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 3px;
    color: var(--text);
    font-family: var(--mono);
    font-size: 13px;
    padding: 10px 14px;
    transition: border-color 0.15s;
    outline: none;
    width: 100%;
  }
  input:focus { border-color: var(--accent); }
  input::placeholder { color: var(--muted); }

  #endpoint-list {
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-bottom: 8px;
  }

  .endpoint-row {
    display: grid;
    grid-template-columns: 160px 1fr 36px;
    gap: 8px;
    align-items: center;
    animation: fadeUp 0.2s forwards;
  }

  .ep-label {
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.1em;
    color: var(--muted);
    text-transform: uppercase;
    padding: 0 0 4px 0;
  }

  .ep-headers {
    display: grid;
    grid-template-columns: 160px 1fr 36px;
    gap: 8px;
    margin-bottom: 4px;
  }

  .btn-remove {
    background: transparent;
    border: 1px solid var(--border);
    border-radius: 3px;
    color: var(--danger);
    font-family: var(--mono);
    font-size: 16px;
    cursor: pointer;
    height: 38px;
    width: 36px;
    display: flex;
    align-items: center;
    justify-content: center;
    transition: border-color 0.15s, background 0.15s;
  }
  .btn-remove:hover { border-color: var(--danger); background: rgba(255,71,87,0.08); }

  .btn-add {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: transparent;
    border: 1px dashed var(--border);
    border-radius: 3px;
    color: var(--accent);
    font-family: var(--mono);
    font-size: 11px;
    letter-spacing: 0.08em;
    padding: 8px 16px;
    cursor: pointer;
    transition: border-color 0.15s, background 0.15s;
    margin-top: 4px;
  }
  .btn-add:hover { border-color: var(--accent); background: rgba(0,212,255,0.05); }

  .btn {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    background: var(--accent);
    color: #000;
    border: none;
    border-radius: 3px;
    font-family: var(--mono);
    font-size: 12px;
    font-weight: 600;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    padding: 12px 28px;
    cursor: pointer;
    transition: opacity 0.15s, transform 0.1s;
    margin-top: 8px;
  }
  .btn:hover { opacity: 0.85; }
  .btn:active { transform: scale(0.98); }
  .btn:disabled { opacity: 0.4; cursor: not-allowed; transform: none; }

  #status-bar {
    display: none;
    align-items: center;
    gap: 12px;
    padding: 14px 20px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    margin-bottom: 24px;
    font-family: var(--mono);
    font-size: 12px;
  }
  #status-bar.show { display: flex; }

  .spinner {
    width: 14px; height: 14px;
    border: 2px solid var(--border);
    border-top-color: var(--accent);
    border-radius: 50%;
    animation: spin 0.7s linear infinite;
    flex-shrink: 0;
  }
  @keyframes spin { to { transform: rotate(360deg); } }

  .dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .dot.running { background: var(--warn); animation: pulse 1.2s ease-in-out infinite; }
  .dot.success { background: var(--accent2); }
  .dot.failed  { background: var(--danger); }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }

  .metrics-row {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
    margin-bottom: 24px;
  }
  .metric-box {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 16px;
    opacity: 0;
    transform: translateY(8px);
    animation: fadeUp 0.4s forwards;
  }
  .metric-box:nth-child(1) { animation-delay: 0.05s; }
  .metric-box:nth-child(2) { animation-delay: 0.10s; }
  .metric-box:nth-child(3) { animation-delay: 0.15s; }
  .metric-box:nth-child(4) { animation-delay: 0.20s; }
  @keyframes fadeUp { to { opacity: 1; transform: translateY(0); } }

  .metric-label {
    font-family: var(--mono);
    font-size: 10px;
    letter-spacing: 0.12em;
    color: var(--muted);
    text-transform: uppercase;
    margin-bottom: 6px;
  }
  .metric-value { font-family: var(--mono); font-size: 22px; font-weight: 600; color: var(--accent); }
  .metric-unit  { font-size: 11px; color: var(--muted); margin-left: 4px; }

  #ab-section { display: none; margin-bottom: 24px; }
  #ab-section.show { display: block; }

  .ab-section-title {
    font-family: var(--mono);
    font-size: 11px;
    letter-spacing: 0.15em;
    color: var(--accent);
    text-transform: uppercase;
    margin-bottom: 16px;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .ab-section-title::after { content: ''; flex: 1; height: 1px; background: var(--border); }

  #results { display: none; }
  #results.show { display: block; }

  .results-table {
    width: 100%;
    border-collapse: collapse;
    font-family: var(--mono);
    font-size: 12px;
  }
  .results-table th {
    text-align: left;
    padding: 10px 14px;
    font-size: 10px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--muted);
    border-bottom: 1px solid var(--border);
  }
  .results-table td {
    padding: 12px 14px;
    border-bottom: 1px solid var(--border);
    vertical-align: middle;
  }
  .results-table tr:last-child td { border-bottom: none; }
  .results-table tr { opacity: 0; animation: fadeUp 0.35s forwards; }

  .ab-winner td { background: rgba(0,255,157,0.04); }
  .ab-winner .ep-name-cell { color: var(--accent2); }

  .rank-badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 22px; height: 22px;
    border-radius: 3px;
    font-size: 10px;
    font-weight: 600;
  }
  .rank-1 { background: rgba(0,255,157,0.15); color: var(--accent2); border: 1px solid var(--accent2); }
  .rank-2 { background: rgba(0,212,255,0.1);  color: var(--accent);  border: 1px solid var(--accent); }
  .rank-n { background: var(--border);        color: var(--muted);   border: 1px solid var(--border); }

  .instance-type { color: var(--text); font-weight: 500; }
  .processor { font-size: 10px; color: var(--muted); margin-top: 2px; }

  .score-bar-wrap { display: flex; align-items: center; gap: 8px; }
  .score-bar { flex: 1; height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; max-width: 80px; }
  .score-fill { height: 100%; background: var(--accent); border-radius: 2px; transition: width 0.8s ease; }
  .score-val  { color: var(--accent2); min-width: 36px; }

  .lat-bar-wrap { display: flex; align-items: center; gap: 8px; }
  .lat-bar { flex: 1; height: 4px; background: var(--border); border-radius: 2px; overflow: hidden; max-width: 60px; }
  .lat-fill { height: 100%; background: var(--warn); border-radius: 2px; }
  .lat-fill.best { background: var(--accent2); }

  .price-tag { color: var(--accent2); font-weight: 500; }

  .error-box {
    display: none;
    padding: 16px 20px;
    background: rgba(255,71,87,0.08);
    border: 1px solid rgba(255,71,87,0.3);
    border-radius: 4px;
    font-family: var(--mono);
    font-size: 12px;
    color: var(--danger);
    margin-bottom: 24px;
  }
  .error-box.show { display: block; }

  #log { font-family: var(--mono); font-size: 11px; color: var(--muted); min-height: 16px; }
  #log span { color: var(--accent); }

  @media (max-width: 600px) {
    .form-grid { grid-template-columns: 1fr; }
    .metrics-row { grid-template-columns: 1fr 1fr; }
    .endpoint-row { grid-template-columns: 1fr 1fr 36px; }
    .results-table th:nth-child(3),
    .results-table td:nth-child(3),
    .results-table th:nth-child(4),
    .results-table td:nth-child(4) { display: none; }
  }
</style>
</head>
<body>
<div class="wrap">

  <header>
    <div class="eyebrow">// EC2 Instance Recommender</div>
    <h1>Find Your Optimal VM</h1>
    <p>Profiles your workload live on AWS · A/B tests multiple endpoints · Returns ranked EC2 recommendations</p>
  </header>

  <div class="card">
    <div class="card-title">Workload Configuration</div>
    <div class="form-grid">

      <div class="form-group full">
        <label>Container Image URI</label>
        <input type="text" id="container_image"
               placeholder="aromal004/test-workload:latest"
               value="aromal004/test-workload:latest">
      </div>

      <div class="form-group">
        <label>Port</label>
        <input type="number" id="port" placeholder="5000" value="5000">
      </div>

      <div class="form-group">
        <label>Concurrency</label>
        <input type="number" id="concurrency" placeholder="100" value="100">
      </div>

      <div class="form-group full">
        <label>Total Requests (per endpoint)</label>
        <input type="number" id="total_requests" placeholder="10000" value="10000">
      </div>

      <div class="form-group full">
        <label>Endpoints to A/B Test</label>
        <div class="ep-headers">
          <span class="ep-label">Name</span>
          <span class="ep-label">Path</span>
          <span></span>
        </div>
        <div id="endpoint-list"></div>
        <button class="btn-add" onclick="addEndpoint()">
          <svg width="10" height="10" viewBox="0 0 10 10"><path d="M5 1v8M1 5h8" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>
          Add Endpoint
        </button>
      </div>

    </div>
    <button class="btn" id="run-btn" onclick="startPipeline()">
      <svg width="12" height="12" viewBox="0 0 12 12" fill="none">
        <path d="M2 1.5L10 6L2 10.5V1.5Z" fill="currentColor"/>
      </svg>
      Run Analysis
    </button>
  </div>

  <div id="status-bar">
    <div class="spinner" id="status-spinner"></div>
    <div class="dot" id="status-dot" style="display:none"></div>
    <span id="status-text">Starting pipeline...</span>
    <span id="log" style="margin-left:auto"></span>
  </div>

  <div class="error-box" id="error-box"></div>

  <div id="metrics-section" style="display:none">
    <div class="ab-section-title">Observed Metrics</div>
    <div class="metrics-row" id="metrics-row"></div>
  </div>

  <div id="ab-section">
    <div class="card">
      <div class="card-title">A/B Endpoint Comparison</div>
      <table class="results-table" id="ab-table">
        <thead>
          <tr>
            <th>Endpoint</th>
            <th>Path</th>
            <th>p50 Latency</th>
            <th>p95 Latency</th>
            <th>p99 Latency</th>
            <th>Req/s</th>
          </tr>
        </thead>
        <tbody id="ab-body"></tbody>
      </table>
    </div>
  </div>

  <div id="results">
    <div class="card">
      <div class="card-title">Recommended Instances</div>
      <table class="results-table">
        <thead>
          <tr>
            <th>#</th>
            <th>Instance</th>
            <th>vCPU / RAM</th>
            <th>Network</th>
            <th>Price/hr</th>
            <th>Score</th>
          </tr>
        </thead>
        <tbody id="results-body"></tbody>
      </table>
    </div>
  </div>

</div>

<script>
  const API_BASE = "https://m50ccolih8.execute-api.us-east-1.amazonaws.com/prod";

  let pollInterval = null;

  function createEndpointRow(name = "", path = "/") {
    const row = document.createElement("div");
    row.className = "endpoint-row";
    row.innerHTML = `
      <input type="text" class="ep-name" placeholder="e.g. control" value="${name}">
      <input type="text" class="ep-path" placeholder="e.g. /"        value="${path}">
      <button class="btn-remove" onclick="removeEndpoint(this)" title="Remove">×</button>
    `;
    return row;
  }

  function addEndpoint(name = "", path = "/") {
    document.getElementById("endpoint-list").appendChild(createEndpointRow(name, path));
    updateRemoveButtons();
  }

  function removeEndpoint(btn) {
    btn.closest(".endpoint-row").remove();
    updateRemoveButtons();
  }

  function updateRemoveButtons() {
    const rows = document.querySelectorAll(".endpoint-row");
    rows.forEach(row => {
      const btn = row.querySelector(".btn-remove");
      btn.disabled = rows.length === 1;
      btn.style.opacity = rows.length === 1 ? "0.2" : "1";
    });
  }

  function getEndpoints() {
    return [...document.querySelectorAll(".endpoint-row")].map(row => ({
      name: row.querySelector(".ep-name").value.trim() || "endpoint",
      path: row.querySelector(".ep-path").value.trim() || "/",
    }));
  }

  addEndpoint("control", "/");
  addEndpoint("variant", "/v2");

  function setStatus(state, text) {
    const bar      = document.getElementById("status-bar");
    const spinner  = document.getElementById("status-spinner");
    const dot      = document.getElementById("status-dot");
    const statusTxt = document.getElementById("status-text");
    bar.classList.add("show");
    statusTxt.textContent = text;
    if (state === "running") {
      spinner.style.display = "block";
      dot.style.display = "none";
    } else {
      spinner.style.display = "none";
      dot.style.display = "block";
      dot.className = "dot " + state;
    }
  }

  function setLog(msg) {
    document.getElementById("log").innerHTML = msg ? `<span>›</span> ${msg}` : "";
  }

  function showError(msg) {
    const box = document.getElementById("error-box");
    box.textContent = "ERROR: " + msg;
    box.classList.add("show");
  }

  function clearError() {
    document.getElementById("error-box").classList.remove("show");
  }

  function renderMetrics(metrics, inference) {
    const section = document.getElementById("metrics-section");
    const row     = document.getElementById("metrics-row");
    if (!metrics && !inference) return;
    const items = [
      { label: "CPU p95",       value: (metrics.cpu_p95        || 0).toFixed(1), unit: "%" },
      { label: "Memory p95",    value: (metrics.mem_p95        || 0).toFixed(1), unit: "%" },
      { label: "Latency p99",   value: (metrics.latency_p99_ms || 0).toFixed(0), unit: "ms" },
      { label: "Required vCPU", value: inference.vcpu || "-",                     unit: "cores" },
    ];
    row.innerHTML = items.map(i => `
      <div class="metric-box">
        <div class="metric-label">${i.label}</div>
        <div class="metric-value">${i.value}<span class="metric-unit">${i.unit}</span></div>
      </div>
    `).join("");
    section.style.display = "block";
  }

  function renderABResults(abResults) {
    if (!abResults || Object.keys(abResults).length === 0) return;
    const section  = document.getElementById("ab-section");
    const tbody    = document.getElementById("ab-body");
    const p99s     = Object.values(abResults).map(r => r.latency_p99_ms || 0);
    const minP99   = Math.min(...p99s);
    const maxP99   = Math.max(...p99s) || 1;
    const epMap    = {};
    getEndpoints().forEach(ep => { epMap[ep.name] = ep.path; });
    tbody.innerHTML = Object.entries(abResults).map(([name, r], idx) => {
      const p99      = r.latency_p99_ms || 0;
      const p95      = r.latency_p95_ms || 0;
      const p50      = r.latency_p50_ms || 0;
      const rps      = r.ab_requests_per_sec || 0;
      const isBest   = p99 === minP99 && p99 > 0;
      const barWidth = maxP99 > 0 ? (p99 / maxP99 * 100).toFixed(0) : 0;
      const delay    = (idx * 0.06).toFixed(2);
      const path     = epMap[name] || "-";
      return `
        <tr class="${isBest ? "ab-winner" : ""}" style="animation-delay:${delay}s">
          <td class="ep-name-cell">${name}${isBest ? " ✦" : ""}</td>
          <td style="color:var(--muted)">${path}</td>
          <td>${p50.toFixed(1)} ms</td>
          <td>${p95.toFixed(1)} ms</td>
          <td>
            <div class="lat-bar-wrap">
              <div class="lat-bar">
                <div class="lat-fill ${isBest ? "best" : ""}" style="width:${barWidth}%"></div>
              </div>
              ${p99.toFixed(1)} ms
            </div>
          </td>
          <td>${rps.toFixed(1)}</td>
        </tr>`;
    }).join("");
    section.classList.add("show");
  }

  function renderResults(instances) {
    const tbody   = document.getElementById("results-body");
    const section = document.getElementById("results");
    if (!instances || instances.length === 0) {
      showError("No instances matched your workload requirements.");
      return;
    }
    const scores   = instances.map(i => i.final_score || i.perf_per_dollar || 0);
    const maxScore = Math.max(...scores);
    tbody.innerHTML = instances.map((inst, idx) => {
      const rankClass = idx === 0 ? "rank-1" : idx === 1 ? "rank-2" : "rank-n";
      const score     = scores[idx];
      const barWidth  = maxScore > 0 ? (score / maxScore * 100).toFixed(0) : 0;
      const delay     = (idx * 0.06).toFixed(2);
      const vcpu  = inst.vcpu || "-";
      const mem   = inst.memory_gib ? `${inst.memory_gib} GiB` : "-";
      const net   = inst.network_mbps
        ? inst.network_mbps >= 1000
          ? `${(inst.network_mbps / 1000).toFixed(0)} Gbps`
          : `${inst.network_mbps} Mbps`
        : "-";
      const price = inst.price_per_hr ? `$${inst.price_per_hr.toFixed(4)}` : "-";
      const proc  = inst.physicalProcessor || "";
      return `
        <tr style="animation-delay:${delay}s">
          <td><span class="rank-badge ${rankClass}">${idx + 1}</span></td>
          <td>
            <div class="instance-type">${inst.instanceType}</div>
            ${proc ? `<div class="processor">${proc}</div>` : ""}
          </td>
          <td>${vcpu} vCPU · ${mem}</td>
          <td>${net}</td>
          <td class="price-tag">${price}</td>
          <td>
            <div class="score-bar-wrap">
              <div class="score-bar">
                <div class="score-fill" style="width:${barWidth}%"></div>
              </div>
              <span class="score-val">${score.toFixed ? score.toFixed(2) : score}</span>
            </div>
          </td>
        </tr>`;
    }).join("");
    section.classList.add("show");
  }

  async function startPipeline() {
    clearError();
    document.getElementById("results").classList.remove("show");
    document.getElementById("ab-section").classList.remove("show");
    document.getElementById("metrics-section").style.display = "none";
    document.getElementById("results-body").innerHTML = "";
    document.getElementById("ab-body").innerHTML = "";

    const btn       = document.getElementById("run-btn");
    btn.disabled    = true;
    const endpoints = getEndpoints();

    if (endpoints.length === 0) {
      showError("Add at least one endpoint.");
      btn.disabled = false;
      return;
    }

    const payload = {
      container_image: document.getElementById("container_image").value.trim(),
      port:            parseInt(document.getElementById("port").value),
      endpoints,
      total_requests:  parseInt(document.getElementById("total_requests").value),
      concurrency:     parseInt(document.getElementById("concurrency").value),
    };

    if (!payload.container_image) {
      showError("Container image is required.");
      btn.disabled = false;
      return;
    }

    setStatus("running", "Starting pipeline...");
    setLog(`Testing endpoints: ${endpoints.map(e => e.name).join(", ")}`);

    try {
      const res  = await fetch(`${API_BASE}/recommend`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      const data = await res.json();
      if (!res.ok || data.error) throw new Error(data.error || "Failed to start pipeline");
      pollResult(data.executionArn, btn, endpoints.length);
    } catch (err) {
      setStatus("failed", "Failed to start");
      showError(err.message);
      btn.disabled = false;
    }
  }

  function pollResult(execArn, btn, endpointCount) {
    let elapsed = 0;
    const steps = [
      { at: 0,   msg: `Running ${endpointCount} endpoint(s) on EC2...` },
      { at: 30,  msg: "Collecting CloudWatch metrics..." },
      { at: 60,  msg: "Inferring resource requirements..." },
      { at: 90,  msg: "Running Bayesian optimisation..." },
      { at: 120, msg: "Ranking instances..." },
    ];
    if (pollInterval) clearInterval(pollInterval);
    pollInterval = setInterval(async () => {
      elapsed += 15;
      const step = [...steps].reverse().find(s => elapsed >= s.at);
      if (step) setLog(step.msg);
      setStatus("running", `Pipeline running · ${elapsed}s elapsed`);
      try {
        const encoded = encodeURIComponent(execArn);
        const res     = await fetch(`${API_BASE}/result?executionArn=${encoded}`);
        const data    = await res.json();
        if (data.status === "SUCCEEDED") {
          clearInterval(pollInterval);
          setStatus("success", "Analysis complete");
          setLog("");
          renderMetrics(data.metrics, data.inference);
          renderABResults(data.ab_results);
          renderResults(data.recommendations);
          btn.disabled = false;
        } else if (data.status === "FAILED") {
          clearInterval(pollInterval);
          setStatus("failed", "Pipeline failed");
          showError(data.error || "Step Functions execution failed");
          btn.disabled = false;
        }
      } catch (err) {
        setLog("Retrying...");
      }
    }, 15000);
  }
</script>
</body>
</html>
```

---

## `state_machine.json`
> Paste into AWS Step Functions when updating the state machine definition.

```json
{
  "Comment": "EC2 workload profiling pipeline — multi-endpoint A/B testing support",
  "StartAt": "LaunchEC2",
  "States": {
    "LaunchEC2": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:634914382615:function:launch_profiling_ec2",
      "ResultPath": "$.launch_result",
      "Next": "WaitForWorkload"
    },
    "WaitForWorkload": {
      "Type": "Wait",
      "Seconds": 240,
      "Next": "CollectMetrics"
    },
    "CollectMetrics": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:634914382615:function:collect_metrics",
      "Parameters": {
        "instance_id.$": "$.launch_result.instance_id",
        "endpoints.$":   "$.endpoints"
      },
      "ResultPath": "$.metrics_result",
      "Next": "InferRequirements"
    },
    "InferRequirements": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:634914382615:function:infer_requirements",
      "Parameters": {
        "cpu_p95.$":                 "$.metrics_result.cpu_p95",
        "mem_p95.$":                 "$.metrics_result.mem_p95",
        "disk_avg.$":                "$.metrics_result.disk_avg",
        "network_in_total_bytes.$":  "$.metrics_result.network_in_total_bytes",
        "network_out_total_bytes.$": "$.metrics_result.network_out_total_bytes",
        "latency_p99_ms.$":          "$.metrics_result.latency_p99_ms"
      },
      "ResultPath": "$.inference_result",
      "Next": "RecommendVM"
    },
    "RecommendVM": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:634914382615:function:recommend_vm",
      "Parameters": {
        "vcpu.$":              "$.inference_result.vcpu",
        "memory_gib.$":        "$.inference_result.memory_gib",
        "network_mbps.$":      "$.inference_result.network_mbps",
        "max_price.$":         "$.inference_result.max_price",
        "latency_sensitive.$": "$.inference_result.latency_sensitive",
        "needs_ssd.$":         "$.inference_result.needs_ssd"
      },
      "ResultPath": "$.recommendation_result",
      "Next": "TerminateEC2"
    },
    "TerminateEC2": {
      "Type": "Task",
      "Resource": "arn:aws:lambda:us-east-1:634914382615:function:terminate_ec2",
      "Parameters": {
        "instance_id.$": "$.launch_result.instance_id"
      },
      "ResultPath": "$.terminate_result",
      "OutputPath": "$",
      "End": true
    }
  }
}
```

---

## `start_pipeline/lambda_function.py`

```python
import json
import boto3

sf = boto3.client("stepfunctions")

STATE_MACHINE_ARN = "arn:aws:states:us-east-1:634914382615:stateMachine:Recommendation-Pipeline"

HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST,OPTIONS",
    "Content-Type": "application/json"
}

def lambda_handler(event, context):

    if event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
        return {"statusCode": 200, "headers": HEADERS, "body": ""}

    try:
        body = event.get("body")
        if body:
            body = json.loads(body)
        else:
            body = event

        required_fields = [
            "container_image",
            "port",
            "endpoints",       # list of {name, path}
            "total_requests",
            "concurrency"
        ]

        for field in required_fields:
            if field not in body:
                return {
                    "statusCode": 400,
                    "headers": HEADERS,
                    "body": json.dumps({"error": f"{field} missing"})
                }

        endpoints = body["endpoints"]
        if not isinstance(endpoints, list) or len(endpoints) == 0:
            return {
                "statusCode": 400,
                "headers": HEADERS,
                "body": json.dumps({"error": "endpoints must be a non-empty list of {name, path}"})
            }

        for ep in endpoints:
            if "name" not in ep or "path" not in ep:
                return {
                    "statusCode": 400,
                    "headers": HEADERS,
                    "body": json.dumps({"error": "each endpoint must have 'name' and 'path' fields"})
                }

        response = sf.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(body)
        )

        return {
            "statusCode": 200,
            "headers": HEADERS,
            "body": json.dumps({
                "message": "Pipeline started",
                "executionArn": response["executionArn"]
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": HEADERS,
            "body": json.dumps({"error": str(e)})
        }
```

---

## `launch_profiling_ec2/lambda_function.py`

```python
import boto3

ec2 = boto3.client("ec2")

def lambda_handler(event, context):

    container_image = event["container_image"]
    port            = event["port"]
    endpoints       = event["endpoints"]   # list of {name, path}
    total_requests  = event["total_requests"]
    concurrency     = event["concurrency"]

    # Build one ab command + S3 upload per endpoint
    ab_commands = ""
    s3_uploads  = ""
    for ep in endpoints:
        name = ep["name"]
        path = ep["path"]
        ab_commands += f"""
echo "=== AB test: {name} ({path}) ==="
ab -n {total_requests} -c {concurrency} \\
   -e /tmp/ab_{name}.csv \\
   -g /tmp/ab_{name}.tsv \\
   http://localhost:{port}{path}
"""
        s3_uploads += f"""
aws s3 cp /tmp/ab_{name}.csv s3://vm-recommendation-data/profiling/$INSTANCE_ID/ab_{name}.csv
aws s3 cp /tmp/ab_{name}.tsv s3://vm-recommendation-data/profiling/$INSTANCE_ID/ab_{name}.tsv
"""

    user_data_script = f"""#!/bin/bash
set -e

docker run -d -p {port}:{port} {container_image}
sleep 30

TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \\
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
INSTANCE_ID=$(curl -s \\
  -H "X-aws-ec2-metadata-token: $TOKEN" \\
  http://169.254.169.254/latest/meta-data/instance-id)

{ab_commands}
{s3_uploads}

aws s3 cp /dev/stdin \\
  s3://vm-recommendation-data/profiling/$INSTANCE_ID/done.txt <<< "done"
"""

    response = ec2.run_instances(
        ImageId="ami-016d9022e2ce362d1",
        InstanceType="t3.large",
        MinCount=1,
        MaxCount=1,
        IamInstanceProfile={"Name": "ProfilingEC2Role2"},
        UserData=user_data_script
    )

    instance_id = response["Instances"][0]["InstanceId"]

    return {
        "instance_id":     instance_id,
        "container_image": container_image,
        "port":            port,
        "endpoints":       endpoints,
        "total_requests":  total_requests,
        "concurrency":     concurrency
    }
```

---

## `collect_metrics/lambda_function.py`

```python
import boto3
import csv
import io
import time
import statistics
from datetime import datetime, timedelta

cloudwatch = boto3.client("cloudwatch")
s3         = boto3.client("s3")

S3_BUCKET = "vm-recommendation-data"


def get_metric(namespace, instance_id, metric_name,
               statistic="Average", extra_dims=None, period=10):
    end_time   = datetime.utcnow()
    start_time = end_time - timedelta(seconds=300)

    dims = [{"Name": "InstanceId", "Value": instance_id}]
    if extra_dims:
        dims += extra_dims

    resp = cloudwatch.get_metric_statistics(
        Namespace=namespace,
        MetricName=metric_name,
        Dimensions=dims,
        StartTime=start_time,
        EndTime=end_time,
        Period=period,
        Statistics=[statistic]
    )
    pts = resp.get("Datapoints", [])
    return [p[statistic] for p in pts] if pts else []


def safe_p95(values):
    if len(values) >= 2:
        return statistics.quantiles(values, n=100)[94]
    return values[0] if values else 0


def wait_for_ab_results(instance_id, retries=8, delay=20):
    key = f"profiling/{instance_id}/done.txt"
    for attempt in range(retries):
        try:
            s3.head_object(Bucket=S3_BUCKET, Key=key)
            return True
        except Exception:
            print(f"Waiting for ab results… attempt {attempt+1}/{retries}")
            time.sleep(delay)
    return False


def parse_ab_csv(instance_id, name):
    """Parse ab -e CSV for a specific named endpoint."""
    defaults = {
        "latency_p50_ms":      0,
        "latency_p95_ms":      0,
        "latency_p99_ms":      0,
        "ab_requests_per_sec": 0,
    }
    try:
        obj    = s3.get_object(
            Bucket=S3_BUCKET,
            Key=f"profiling/{instance_id}/ab_{name}.csv"
        )
        reader = csv.DictReader(io.StringIO(obj["Body"].read().decode()))
        pct_map = {}
        for row in reader:
            pct = row.get("Percentage served", "").strip()
            ms  = row.get("Time in ms", "0").strip()
            if pct:
                pct_map[pct] = float(ms)
        defaults["latency_p50_ms"] = pct_map.get("50", 0)
        defaults["latency_p95_ms"] = pct_map.get("95", 0)
        defaults["latency_p99_ms"] = pct_map.get("99", 0)
    except Exception as e:
        print(f"ab CSV parse failed for '{name}': {e}")

    try:
        obj   = s3.get_object(
            Bucket=S3_BUCKET,
            Key=f"profiling/{instance_id}/ab_{name}.tsv"
        )
        lines = obj["Body"].read().decode().splitlines()
        data_rows = [l for l in lines if l and not l.startswith("starttime")]
        if data_rows:
            total_time_s = float(data_rows[-1].split("\t")[1])
            if total_time_s > 0:
                defaults["ab_requests_per_sec"] = round(
                    len(data_rows) / total_time_s, 2)
    except Exception as e:
        print(f"ab TSV parse failed for '{name}': {e}")

    return defaults


def lambda_handler(event, context):

    instance_id = event.get("instance_id")
    endpoints   = event.get("endpoints", [{"name": "default", "path": "/"}])

    if not instance_id:
        return {
            "cpu_avg": 0, "cpu_p95": 0,
            "mem_avg": 0, "mem_p95": 0,
            "disk_avg": 0,
            "network_in_total_bytes": 0,
            "network_out_total_bytes": 0,
            "latency_p50_ms": 0, "latency_p95_ms": 0, "latency_p99_ms": 0,
            "ab_requests_per_sec": 0, "ab_failed_requests": 0,
            "ab_results": {},
            "datapoints_collected": 0,
            "warning": "instance_id not provided"
        }

    # CPU
    cpu_values = get_metric(
        "CWAgent", instance_id, "cpu_usage_active",
        statistic="Average",
        extra_dims=[{"Name": "cpu", "Value": "cpu-total"}],
        period=10
    )
    if not cpu_values:
        cpu_values = get_metric("AWS/EC2", instance_id,
                                "CPUUtilization", "Average", period=60)
    if not cpu_values:
        cpu_values = [10.0]

    # Memory
    mem_values = get_metric(
        "CWAgent", instance_id, "mem_used_percent",
        statistic="Maximum", period=10
    )
    if not mem_values:
        mem_values = [50.0]

    # Disk
    disk_values = get_metric(
        "CWAgent", instance_id, "disk_used_percent",
        statistic="Average",
        extra_dims=[
            {"Name": "device", "Value": "xvda1"},
            {"Name": "fstype", "Value": "xfs"},
            {"Name": "path",   "Value": "/"}
        ],
        period=60
    )
    if not disk_values:
        disk_values = [10.0]

    # Network
    network_in  = get_metric("AWS/EC2", instance_id, "NetworkIn",  "Sum", period=60)
    network_out = get_metric("AWS/EC2", instance_id, "NetworkOut", "Sum", period=60)
    if not network_in:  network_in  = [0]
    if not network_out: network_out = [0]

    # ab results — one CSV per endpoint
    ab_ready   = wait_for_ab_results(instance_id)
    ab_results = {}
    if ab_ready:
        for ep in endpoints:
            ab_results[ep["name"]] = parse_ab_csv(instance_id, ep["name"])
    else:
        for ep in endpoints:
            ab_results[ep["name"]] = {
                "latency_p50_ms": 0, "latency_p95_ms": 0,
                "latency_p99_ms": 0, "ab_requests_per_sec": 0,
            }

    # Headline latency — worst-case p99 across all endpoints
    latency_p99 = max((r["latency_p99_ms"] for r in ab_results.values()), default=0)
    latency_p95 = max((r["latency_p95_ms"] for r in ab_results.values()), default=0)
    latency_p50 = max((r["latency_p50_ms"] for r in ab_results.values()), default=0)
    total_rps   = sum(r["ab_requests_per_sec"] for r in ab_results.values())

    return {
        "cpu_avg":  round(statistics.mean(cpu_values), 2),
        "cpu_p95":  round(safe_p95(cpu_values), 2),
        "mem_avg":  round(statistics.mean(mem_values), 2),
        "mem_p95":  round(safe_p95(mem_values), 2),
        "disk_avg": round(statistics.mean(disk_values), 2),
        "network_in_total_bytes":  sum(network_in),
        "network_out_total_bytes": sum(network_out),
        "latency_p50_ms":      latency_p50,
        "latency_p95_ms":      latency_p95,
        "latency_p99_ms":      latency_p99,
        "ab_requests_per_sec": round(total_rps, 2),
        "ab_failed_requests":  0,
        "ab_results":          ab_results,
        "datapoints_collected": len(cpu_values),
        "ab_results_found":     ab_ready
    }
```

---

## `infer_requirements/lambda_function.py`

```python
import math

BASELINE_VCPU    = 2      # t3.large
BASELINE_MEM_GIB = 8.0    # t3.large
PROFILING_SECS   = 240


def lambda_handler(event, context):

    cpu_p95  = event["cpu_p95"]
    mem_p95  = event.get("mem_p95", 50.0)
    disk_avg = event.get("disk_avg", 10.0)
    network_bytes = (
        event["network_in_total_bytes"] +
        event["network_out_total_bytes"]
    )
    latency_p99 = event.get("latency_p99_ms", 0)

    # vCPU
    cpu_ratio     = cpu_p95 / 100
    required_vcpu = max(math.ceil(cpu_ratio * BASELINE_VCPU * 2), 2)

    # Memory
    observed_mem_gib = (mem_p95 / 100) * BASELINE_MEM_GIB
    required_mem_gib = max(math.ceil(observed_mem_gib * 1.25), 4)
    standard_sizes   = [4, 8, 16, 32, 64, 128]
    required_mem_gib = next(
        (s for s in standard_sizes if s >= required_mem_gib),
        required_mem_gib
    )

    # Network
    network_mbps = max(
        math.ceil(((network_bytes * 8) / PROFILING_SECS) / 1_000_000 * 1.3),
        1000
    )

    # Latency sensitivity
    latency_sensitive = latency_p99 > 100
    max_price         = 20.0 if latency_sensitive else 10.0

    # Disk
    needs_ssd = disk_avg > 60.0

    return {
        "vcpu":              required_vcpu,
        "memory_gib":        required_mem_gib,
        "network_mbps":      network_mbps,
        "max_price":         max_price,
        "latency_sensitive": latency_sensitive,
        "needs_ssd":         needs_ssd,
        "observed_mem_p95":  mem_p95,
        "observed_cpu_p95":  cpu_p95,
        "latency_p99_ms":    latency_p99
    }
```

---

## `recommend_vm/lambda_handler.py`

```python
from main import run_recommendation

BASELINE_COREMARK_PER_CORE = 27000


def lambda_handler(event, context):

    vcpu         = event["vcpu"]
    memory_gib   = event["memory_gib"]
    network_mbps = event["network_mbps"]
    max_price    = event.get("max_price", 10.0)

    required_compute = vcpu * BASELINE_COREMARK_PER_CORE

    requirements = {
        "required_compute": required_compute,
        "memory_gib":       memory_gib,
        "network_mbps":     network_mbps,
        "max_price":        max_price,
    }

    results = run_recommendation(requirements)

    return {"recommended_instances": results}
```

---

## `recommend_vm/main.py`

```python
import pandas as pd
import boto3

from preprocessing.feature_engineering import add_features
from preprocessing.hard_filter         import hard_filter
from scoring.fit_score                 import add_fit_score
from optimization.bayesian_ranker      import optimize_weights
from scoring.final_scorer              import rank_instances
from postprocessing.diversify          import diversify

BASELINE_COREMARK_PER_CORE = 27000


def load_dataset():
    s3  = boto3.client("s3")
    obj = s3.get_object(
        Bucket="vm-recommendation-data",
        Key="aws_with_coremark.csv"
    )
    return pd.read_csv(obj["Body"])


def run_recommendation(requirements):
    df = load_dataset()

    df = add_features(df)
    if df.empty:
        return {"error": "Dataset empty after feature engineering"}

    df = hard_filter(df, requirements)
    if df.empty:
        return {"error": "No instances satisfy constraints"}

    df      = add_fit_score(df, requirements)
    weights = optimize_weights(df)
    ranked  = rank_instances(df, weights)
    final   = diversify(ranked, per_family=2, top_n=10)

    return final[[
        "instanceType",
        "physicalProcessor",
        "vcpu",
        "compute_score",
        "memory_gib",
        "network_mbps",
        "price_per_hr",
        "perf_per_dollar",
        "final_score"
    ]].to_dict(orient="records")
```

---

## `recommend_vm/optimization/bayesian_ranker.py`

```python
from skopt import gp_minimize
from skopt.space import Real


def optimize_weights(df, top_k=10, n_calls=30):
    space = [
        Real(0.3, 0.7, name="fit"),
        Real(0.1, 0.4, name="cost"),
        Real(0.05, 0.2, name="generation"),
    ]

    def objective(params):
        w = dict(zip(["fit", "cost", "generation"], params))
        s = sum(w.values())
        w = {k: v / s for k, v in w.items()}
        score = (
            w["fit"]          * df["fit_score"]
            + w["cost"]       * df["perf_per_dollar"]
            + w["generation"] * df["generation_score"]
        )
        return -score.nlargest(top_k).mean()

    res     = gp_minimize(objective, space, n_calls=n_calls, random_state=42)
    weights = dict(zip(["fit", "cost", "generation"], res.x))
    s       = sum(weights.values())
    return {k: v / s for k, v in weights.items()}
```

---

## `recommend_vm/postprocessing/diversify.py`

```python
def diversify(df, per_family=2, top_n=10):
    result = []
    counts = {}

    for _, row in df.iterrows():
        fam = row["family"]
        counts.setdefault(fam, 0)
        if counts[fam] < per_family:
            result.append(row)
            counts[fam] += 1
        if len(result) >= top_n:
            break

    return df.loc[[r.name for r in result]]
```

---

## `recommend_vm/preprocessing/feature_engineering.py`

```python
import pandas as pd
import re
import numpy as np


def parse_network_mbps(val):
    if pd.isna(val):
        return 0.0
    s = str(val).lower()
    m = re.search(r"([\d\.]+)\s*gigabit", s)
    if m:
        return float(m.group(1)) * 1000
    m = re.search(r"([\d\.]+)\s*megabit", s)
    if m:
        return float(m.group(1))
    m = re.search(r"([\d\.]+)\s*gbps", s)
    if m:
        return float(m.group(1)) * 1000
    m = re.search(r"([\d\.]+)\s*mbps", s)
    if m:
        return float(m.group(1))
    m = re.search(r"([\d\.]+)", s)
    return float(m.group(1)) if m else 0.0


def add_features(df):
    df = df.copy()

    df = df.dropna(subset=["instanceType"])
    df["instanceType"] = df["instanceType"].astype(str)
    df = df[df["price_per_hr"] > 0]
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["coremark_total", "coremark_per_dollar"])

    df["vcpu"] = df["vcpu"].astype(float)
    df["memory_gib"] = (
        df["memory"]
        .str.replace(" GiB", "", regex=False)
        .astype(float)
    )
    df["network_mbps"]    = df["networkPerformance"].apply(parse_network_mbps)
    df["compute_score"]   = df["coremark_total"]
    df["perf_per_dollar"] = df["coremark_per_dollar"]
    df["generation_score"] = (
        df["coremark_per_core"] / df["coremark_per_core"].max()
    )
    df["family"] = df["instanceType"].str.split(".").str[0]

    return df
```

---

## `recommend_vm/preprocessing/hard_filter.py`

```python
def hard_filter(df, req):
    df = df.copy()

    df = df[df["compute_score"] >= req["required_compute"]]
    df = df[df["memory_gib"]    >= req["memory_gib"]]

    if req.get("network_mbps", 0) > 0:
        df = df[df["network_mbps"] >= req["network_mbps"]]

    if "max_price" in req and req["max_price"] > 0:
        df = df[df["price_per_hr"] <= req["max_price"]]

    return df
```

---

## `recommend_vm/scoring/fit_score.py`

```python
import numpy as np

def add_fit_score(df, req):
    df = df.copy()

    compute_penalty = (
        (df["compute_score"] - req["required_compute"]) / req["required_compute"]
    ).clip(lower=0)

    mem_penalty = (
        (df["memory_gib"] - req["memory_gib"]) / req["memory_gib"]
    ).clip(lower=0)

    if req.get("network_mbps", 0) > 0:
        net_penalty = (
            (df["network_mbps"] - req["network_mbps"]) / req["network_mbps"]
        ).clip(lower=0)
    else:
        net_penalty = 0

    df["fit_score"] = 1 / (1 + compute_penalty + mem_penalty + net_penalty)

    return df
```

---

## `recommend_vm/scoring/final_scorer.py`

```python
def rank_instances(df, weights):
    df = df.copy()

    df["final_score"] = (
        weights["fit"]          * df["fit_score"]
        + weights["cost"]       * df["perf_per_dollar"]
        + weights["generation"] * df["generation_score"]
    )

    return df.sort_values("final_score", ascending=False)
```

---

## `terminate_ec2/lambda_function.py`

```python
import boto3

ec2 = boto3.client("ec2")

def lambda_handler(event, context):

    instance_id = event.get("instance_id")

    if not instance_id:
        return {"error": "instance_id not provided"}

    ec2.terminate_instances(InstanceIds=[instance_id])

    return {
        "message": "Instance terminated successfully",
        "instance_id": instance_id
    }
```

---

## `get_pipeline_result/lambda_function.py`

```python
import json
import boto3

sf = boto3.client("stepfunctions")

HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Content-Type": "application/json"
}

def lambda_handler(event, context):

    if event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
        return {"statusCode": 200, "headers": HEADERS, "body": ""}

    try:
        params        = event.get("queryStringParameters") or {}
        execution_arn = params.get("executionArn")

        if not execution_arn:
            return {
                "statusCode": 400,
                "headers": HEADERS,
                "body": json.dumps({"error": "executionArn query param required"})
            }

        response = sf.describe_execution(executionArn=execution_arn)
        status   = response["status"]

        if status == "SUCCEEDED":
            output = json.loads(response["output"])

            recommendations = output.get("recommendation_result", {}).get(
                "recommended_instances", []
            )
            metrics    = output.get("metrics_result", {})
            inference  = output.get("inference_result", {})
            ab_results = metrics.get("ab_results", {})

            return {
                "statusCode": 200,
                "headers": HEADERS,
                "body": json.dumps({
                    "status":          "SUCCEEDED",
                    "recommendations": recommendations,
                    "metrics":         metrics,
                    "inference":       inference,
                    "ab_results":      ab_results,
                })
            }

        if status == "FAILED":
            return {
                "statusCode": 200,
                "headers": HEADERS,
                "body": json.dumps({
                    "status": "FAILED",
                    "error":  response.get("cause", "Unknown error")
                })
            }

        return {
            "statusCode": 200,
            "headers": HEADERS,
            "body": json.dumps({
                "status":  status,
                "message": "Pipeline still running. Poll again in 15 seconds."
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": HEADERS,
            "body": json.dumps({"error": str(e)})
        }
```
