import React, { useEffect, useMemo, useState } from "react";

const API_BASE = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";
const REFRESH_MS = 5000;

const TABS = [
  { id: "upload", label: "Upload Events", useCase: "Use Case 1: batch/replay ingestion" },
  { id: "stream", label: "Stream Pipeline", useCase: "Use Case 2: Kafka to Spark flow" },
  { id: "dedup", label: "Dedup Analytics", useCase: "Use Case 3: duplicate detection" },
  { id: "radar", label: "RADAR Control", useCase: "Use Case 4: adaptive client limits" },
  { id: "evidence", label: "Experiment Evidence", useCase: "Use Case 5: benchmark-ready results" },
];
const EXPERIMENT_MODES = [
  { id: "none", label: "No Rate Limiting", detail: "Accept everything; measures raw Kafka/Spark load." },
  { id: "static", label: "Static Token Bucket", detail: "Same fixed limit for every client." },
  { id: "storage_only", label: "Storage-only Dedup", detail: "Accept everything; Spark/storage cleans duplicates later." },
  { id: "radar", label: "RADAR Adaptive", detail: "Use ClickHouse duplicate metrics to adapt Redis limits." },
];
const sampleEvents = `{"client_id":"seller_normal","event_type":"order_created","entity_id":"ORD-100","payload":{"amount":1200,"city":"Hyderabad"}}
{"client_id":"seller_normal","event_type":"order_created","entity_id":"ORD-101","payload":{"amount":1500,"city":"Mumbai"}}
{"client_id":"seller_replay","event_type":"order_created","entity_id":"ORD-DUP-1","payload":{"amount":500,"city":"Delhi"}}
{"client_id":"seller_replay","event_type":"order_created","entity_id":"ORD-DUP-1","payload":{"amount":500,"city":"Delhi"}}
{"client_id":"seller_replay","event_type":"order_created","entity_id":"ORD-DUP-1","payload":{"amount":500,"city":"Delhi"}}`;

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatRatio(value) {
  return `${(toNumber(value) * 100).toFixed(1)}%`;
}

function formatPercent(value) {
  return `${toNumber(value).toFixed(1)}%`;
}
function modeLabel(mode) {
  return EXPERIMENT_MODES.find((item) => item.id === mode)?.label ?? mode;
}

function parseCsvLine(line) {
  const cells = [];
  let current = "";
  let quoted = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"' && quoted && next === '"') {
      current += '"';
      i += 1;
    } else if (char === '"') {
      quoted = !quoted;
    } else if (char === "," && !quoted) {
      cells.push(current.trim());
      current = "";
    } else {
      current += char;
    }
  }

  cells.push(current.trim());
  return cells;
}

function parseEvents(text, fileName = "") {
  const trimmed = text.trim();
  if (!trimmed) return [];

  if (fileName.toLowerCase().endsWith(".csv")) {
    const lines = trimmed.split(/\r?\n/).filter(Boolean);
    const headers = parseCsvLine(lines[0]).map((header) => header.trim());
    return lines.slice(1).map((line) => {
      const cells = parseCsvLine(line);
      const row = Object.fromEntries(headers.map((header, index) => [header, cells[index] ?? ""]));
      let payload = {};
      if (row.payload) {
        try {
          payload = JSON.parse(row.payload);
        } catch {
          payload = { value: row.payload };
        }
      }
      return {
        client_id: row.client_id,
        event_type: row.event_type,
        entity_id: row.entity_id,
        event_time: row.event_time || undefined,
        payload,
      };
    });
  }

  if (trimmed.startsWith("[")) {
    const parsed = JSON.parse(trimmed);
    if (!Array.isArray(parsed)) throw new Error("JSON file must contain an array of events.");
    return parsed;
  }

  return trimmed.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

async function request(path, options) {
  const response = await fetch(`${API_BASE}${path}`, options);
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = body?.detail?.error || body?.detail?.message || body?.detail || body?.error || response.statusText;
    throw new Error(typeof message === "string" ? message : JSON.stringify(message));
  }
  return body;
}

function MetricCard({ label, value, tone = "neutral" }) {
  return (
    <div className={`metric-card metric-${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function StatusPill({ value }) {
  const ratio = toNumber(value);
  const tone = ratio >= 0.5 ? "danger" : ratio >= 0.15 ? "warn" : "ok";
  const label = ratio >= 0.5 ? "Replay risk" : ratio >= 0.15 ? "Watch" : "Healthy";
  return <span className={`pill pill-${tone}`}>{label}</span>;
}

function SectionTitle({ activeTab }) {
  const tab = TABS.find((item) => item.id === activeTab);
  return (
    <div className="section-title">
      <p>{tab?.useCase}</p>
      <h2>{tab?.label}</h2>
    </div>
  );
}

export default function App() {
  const [activeTab, setActiveTab] = useState("upload");
  const [metrics, setMetrics] = useState([]);
  const [selectedTopic, setSelectedTopic] = useState("all");
  const [baselineTopic, setBaselineTopic] = useState("");
  const [protectedTopic, setProtectedTopic] = useState("");
  const [experimentMode, setExperimentMode] = useState("radar");
  const [modeInfo, setModeInfo] = useState(null);
  const [experimentRuns, setExperimentRuns] = useState([]);
  const [limits, setLimits] = useState({});
  const [decisions, setDecisions] = useState([]);
  const [eventsText, setEventsText] = useState(sampleEvents);
  const [fileName, setFileName] = useState("sample.jsonl");
  const [useCaseName, setUseCaseName] = useState("Replay Attack Demo");
  const [uploadState, setUploadState] = useState({ total: 0, accepted: 0, rejected: 0, running: false });
  const [taskLog, setTaskLog] = useState([
    { stage: "Ready", detail: "Upload a use-case file to start the pipeline.", status: "idle" },
  ]);
  const [message, setMessage] = useState("Ready");
  const [error, setError] = useState("");

  const topics = useMemo(() => [...new Set(metrics.map((row) => row.use_case_topic ?? "raw_events"))], [metrics]);

  useEffect(() => {
    const guessedBaseline = topics.find((topic) => /round_1|baseline/i.test(topic)) ?? topics[0] ?? "";
    const guessedProtected = topics.find((topic) => /round_2|protected/i.test(topic)) ?? topics.find((topic) => topic !== guessedBaseline) ?? "";

    setBaselineTopic((current) => (current && topics.includes(current) ? current : guessedBaseline));
    setProtectedTopic((current) => (current && topics.includes(current) ? current : guessedProtected));
  }, [topics]);

  const visibleMetrics = useMemo(() => {
    if (selectedTopic === "all") return metrics;
    return metrics.filter((row) => (row.use_case_topic ?? "raw_events") === selectedTopic);
  }, [metrics, selectedTopic]);

  const summary = useMemo(() => {
    const raw = visibleMetrics.reduce((sum, row) => sum + toNumber(row.raw_events), 0);
    const unique = visibleMetrics.reduce((sum, row) => sum + toNumber(row.unique_events), 0);
    const duplicates = visibleMetrics.reduce((sum, row) => sum + toNumber(row.duplicate_events), 0);
    return {
      raw,
      unique,
      duplicates,
      ratio: raw ? duplicates / raw : 0,
      clients: new Set(visibleMetrics.map((row) => row.client_id)).size,
    };
  }, [visibleMetrics]);

  const replayClient = useMemo(
    () => [...visibleMetrics].sort((a, b) => toNumber(b.duplicate_ratio) - toNumber(a.duplicate_ratio))[0],
    [visibleMetrics]
  );

  const visibleDecisions = useMemo(() => {
    if (selectedTopic === "all") return decisions;
    return decisions.filter((decision) => (decision.use_case_topic ?? "raw_events") === selectedTopic);
  }, [decisions, selectedTopic]);

  const optimizationEvidence = useMemo(() => {
    if (!baselineTopic || !protectedTopic || baselineTopic === protectedTopic) return null;

    const totalsByTopic = metrics.reduce((acc, row) => {
      const topic = row.use_case_topic ?? "raw_events";
      if (!acc[topic]) acc[topic] = { topic, raw: 0, unique: 0, duplicates: 0 };
      acc[topic].raw += toNumber(row.raw_events);
      acc[topic].unique += toNumber(row.unique_events);
      acc[topic].duplicates += toNumber(row.duplicate_events);
      return acc;
    }, {});

    const baseline = totalsByTopic[baselineTopic];
    const protectedRun = totalsByTopic[protectedTopic];
    if (!baseline || !protectedRun) return null;

    const loadReduced = Math.max(0, baseline.raw - protectedRun.raw);
    const wasteReduced = Math.max(0, baseline.duplicates - protectedRun.duplicates);

    return {
      baseline,
      protectedRun,
      loadReduced,
      wasteReduced,
      loadReductionRatio: baseline.raw ? (loadReduced / baseline.raw) * 100 : 0,
      wasteReductionRatio: baseline.duplicates ? (wasteReduced / baseline.duplicates) * 100 : 0,
      protectedLoadRatio: baseline.raw ? (protectedRun.raw / baseline.raw) * 100 : 0,
    };
  }, [metrics, baselineTopic, protectedTopic]);

  const experimentResults = useMemo(() => {
    const totalsByTopic = metrics.reduce((acc, row) => {
      const topic = row.use_case_topic ?? "raw_events";
      if (!acc[topic]) acc[topic] = { raw: 0, unique: 0, duplicates: 0 };
      acc[topic].raw += toNumber(row.raw_events);
      acc[topic].unique += toNumber(row.unique_events);
      acc[topic].duplicates += toNumber(row.duplicate_events);
      return acc;
    }, {});

    return EXPERIMENT_MODES.map((mode) => {
      const runsForMode = experimentRuns.filter((run) => run.experiment_mode === mode.id);
      const runsWithMetrics = runsForMode.map((run) => {
        const metric = totalsByTopic[run.topic] ?? { raw: 0, unique: 0, duplicates: 0 };
        return {
          ...run,
          kafkaMessages: metric.raw,
          cleanEvents: metric.unique,
          duplicateWaste: metric.duplicates,
          duplicateRatio: metric.raw ? metric.duplicates / metric.raw : 0,
        };
      });
      const latest = runsWithMetrics[runsWithMetrics.length - 1];
      return { ...mode, runs: runsWithMetrics, latest };
    });
  }, [metrics, experimentRuns]);


  async function loadExperimentMode() {
    const data = await request("/experiment/mode");
    setModeInfo(data);
    setExperimentMode(data.mode ?? "radar");
  }

  async function changeExperimentMode(mode) {
    setExperimentMode(mode);
    const data = await request("/experiment/mode", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mode }),
    });
    setModeInfo(data);
    setMessage(`Experiment mode set to ${modeLabel(data.mode)}`);
  }
  async function loadExperimentRuns() {
    const data = await request("/experiment/runs");
    setExperimentRuns(data.runs ?? []);
  }
  async function loadMetrics() {
    const data = await request("/radar/metrics");
    const rows = data.metrics ?? [];
    setMetrics(rows);
    setSelectedTopic((current) => {
      if (current === "all") return current;
      return rows.some((row) => (row.use_case_topic ?? "raw_events") === current) ? current : "all";
    });
    await loadExperimentRuns();
    await Promise.all(rows.map(async (row) => {
      try {
        const limit = await request(`/ratelimit/${encodeURIComponent(row.client_id)}`);
        setLimits((current) => ({ ...current, [row.client_id]: limit }));
      } catch {
        // Keep previous value during transient backend/Redis failures.
      }
    }));
  }

  useEffect(() => {
    loadExperimentMode().catch((err) => setError(err.message));
    loadMetrics().catch((err) => setError(err.message));
    const id = setInterval(() => {
      loadMetrics().catch((err) => setError(err.message));
    }, REFRESH_MS);
    return () => clearInterval(id);
  }, []);

  async function handleFile(event) {
    const file = event.target.files?.[0];
    if (!file) return;
    setFileName(file.name);
    setUseCaseName(file.name.replace(/\.[^.]+$/, "") || "Uploaded Use Case");
    setEventsText(await file.text());
    setMessage(`Loaded ${file.name}`);
    setError("");
  }

  async function ingestEvents() {
    setError("");
    let events;
    try {
      events = parseEvents(eventsText, fileName);
    } catch (err) {
      setError(`Could not parse events: ${err.message}`);
      return;
    }

    setUploadState({ total: events.length, accepted: 0, rejected: 0, running: true });
    setTaskLog([
      { stage: "File parsed", detail: `${events.length} events parsed from ${fileName}.`, status: "done" },
      { stage: "Experiment mode", detail: `${modeLabel(experimentMode)} selected for this upload.`, status: "done" },
      { stage: "Use-case topic", detail: `Backend will create an isolated Kafka topic for ${useCaseName || fileName}.`, status: "running" },
    ]);
    setMessage(`Ingesting ${events.length} events`);

    let result;
    try {
      result = await request("/usecases/ingest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ use_case_name: useCaseName || fileName, experiment_mode: experimentMode, events }),
      });
    } catch (err) {
      setUploadState({ total: events.length, accepted: 0, rejected: events.length, running: false });
      setError(err.message);
      return;
    }

    setUploadState({ total: result.total_events, accepted: result.accepted, rejected: result.rejected, running: false });
    setTaskLog([
      { stage: "File parsed", detail: `${result.total_events} events parsed from ${fileName}.`, status: "done" },
      { stage: "Experiment mode", detail: `${modeLabel(result.experiment_mode)} used ${result.protection_strategy}.`, status: "done" },
      { stage: "Kafka topic created", detail: `${result.topic} is the isolated topic for this use case.`, status: "done" },
      { stage: "Backend rate limiter", detail: `${result.accepted} events accepted, ${result.rejected} blocked before Kafka.`, status: result.rejected > 0 ? "warn" : "done" },
      { stage: "Kafka publish", detail: `${result.accepted} accepted events were published to ${result.topic}.`, status: "done" },
      { stage: "Spark processing", detail: "Spark consumes usecase_* topics, builds dedup_key, and writes clean Parquet records.", status: "running" },
      { stage: "ClickHouse metrics", detail: "After Spark micro-batches finish, duplicate metrics appear in the analytics panels.", status: "running" },
    ]);
    setSelectedTopic(result.topic);
    setMessage(`Ingestion complete on ${result.topic} using ${modeLabel(result.experiment_mode)}: ${result.accepted} accepted, ${result.rejected} rejected`);
    await loadExperimentRuns();
    setTimeout(() => loadMetrics().catch((err) => setError(err.message)), 2500);
  }

  async function updateRadar() {
    setError("");
    try {
      const data = await request("/radar/update-limits", { method: "POST" });
      setDecisions(data.decisions ?? []);
      setTaskLog((current) => [
        ...current.filter((item) => item.stage !== "RADAR controller" && item.stage !== "Redis enforcement"),
        { stage: "RADAR controller", detail: `Read ClickHouse metrics and computed ${data.updated_clients ?? 0} client limit decisions.`, status: "done" },
        { stage: "Redis enforcement", detail: "Updated per-client token bucket limits used by the FastAPI gateway.", status: "done" },
      ]);
      setMessage(`RADAR updated ${data.updated_clients ?? 0} client limits`);
      await loadMetrics();
      setActiveTab("radar");
    } catch (err) {
      setError(err.message);
    }
  }

  return (
    <main className="app-shell">
      <header className="app-header">
        <div>
          <p className="eyebrow">Distributed streaming research prototype</p>
          <h1>RADAR Streaming Platform</h1>
        </div>
        <div className="header-actions">
          <button className="button button-secondary" onClick={() => loadMetrics().catch((err) => setError(err.message))}>Refresh</button>
          <button className="button" onClick={updateRadar}>Update RADAR</button>
        </div>
      </header>

      <div className="layout">
        <aside className="sidebar">
          <div className="sidebar-header">
            <div className="sidebar-title">RADAR Use Cases</div>
          </div>
          <nav className="sidebar-nav">
            {TABS.map((tab, index) => (
              <button
                className={`nav-item ${activeTab === tab.id ? "active" : ""}`}
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
              >
                <span className="nav-index">{index + 1}</span>
                <span className="nav-copy">
                  <strong>{tab.label}</strong>
                  <em>{tab.useCase.replace(/^Use Case \d+: /, "")}</em>
                </span>
              </button>
            ))}
          </nav>
        </aside>

        <section className="content-area">
          <div className="topic-filter">
            <label>
              Use-case topic
              <select value={selectedTopic} onChange={(event) => setSelectedTopic(event.target.value)}>
                <option value="all">All topics</option>
                {topics.map((topic) => <option key={topic} value={topic}>{topic}</option>)}
              </select>
            </label>
          </div>
          <section className="metric-grid">
        <MetricCard label="Raw Events" value={summary.raw} />
        <MetricCard label="Unique Events" value={summary.unique} tone="ok" />
        <MetricCard label="Duplicates" value={summary.duplicates} tone="danger" />
        <MetricCard label="Duplicate Ratio" value={formatRatio(summary.ratio)} tone="warn" />
        <MetricCard label="Active Clients" value={summary.clients} />
      </section>

      {activeTab === "upload" && (
        <section className="panel section-panel">
          <SectionTitle activeTab={activeTab} />
          <div className="two-column">
            <div>
              <div className="mode-selector">
                {EXPERIMENT_MODES.map((mode) => (
                  <button
                    type="button"
                    key={mode.id}
                    className={`mode-option ${experimentMode === mode.id ? "active" : ""}`}
                    onClick={() => changeExperimentMode(mode.id).catch((err) => setError(err.message))}
                  >
                    <strong>{mode.label}</strong>
                    <span>{mode.detail}</span>
                  </button>
                ))}
              </div>
              <label className="field-label">
                Use case name
                <input className="text-input" value={useCaseName} onChange={(event) => setUseCaseName(event.target.value)} />
              </label>
              <label className="file-drop">
                <input type="file" accept=".json,.jsonl,.ndjson,.csv,text/csv,application/json" onChange={handleFile} />
                <span>{fileName}</span>
              </label>
              <textarea value={eventsText} onChange={(event) => setEventsText(event.target.value)} spellCheck="false" />
              <div className="row-actions">
                <button className="button" onClick={ingestEvents} disabled={uploadState.running}>
                  {uploadState.running ? "Ingesting" : "Ingest Events"}
                </button>
                <span>{uploadState.total > 0 ? `${uploadState.accepted}/${uploadState.total} accepted, ${uploadState.rejected} rejected` : "No upload sent yet"}</span>
              </div>
            </div>
            <div className="usecase-card">
              <h3>Use Case</h3>
              <p>Upload an event batch from a client system. Normal clients send new entity IDs, while replay-heavy clients repeat the same entity ID many times.</p>
              <div className="callout">Supported: JSON array, JSONL, CSV.</div>
            </div>
          </div>
        </section>
      )}

      {activeTab === "stream" && (
        <section className="panel section-panel">
          <SectionTitle activeTab={activeTab} />
          <div className="pipeline-grid">
            {[
              ["Upload/API", "Events enter FastAPI and pass through Redis rate limiting."],
              ["Kafka usecase_* topic", "Accepted raw events are stored for Spark consumption."],
              ["Spark Streaming", "Structured Streaming reads the Kafka topic continuously."],
              ["Clean Storage", "Deduplicated events are written to Parquet."],
              ["ClickHouse", "Per-client duplicate metrics are inserted for fast queries."],
              ["Redis Limits", "RADAR writes adaptive limits back into Redis."],
            ].map(([title, body]) => (
              <div className="pipeline-step" key={title}>
                <strong>{title}</strong>
                <p>{body}</p>
              </div>
            ))}
          </div>
        </section>
      )}

      {activeTab === "dedup" && (
        <section className="panel section-panel">
          <SectionTitle activeTab={activeTab} />
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Use Case Topic</th>
                  <th>Client</th>
                  <th>Status</th>
                  <th>Raw</th>
                  <th>Unique</th>
                  <th>Duplicates</th>
                  <th>Duplicate Ratio</th>
                </tr>
              </thead>
              <tbody>
                {visibleMetrics.length === 0 ? (
                  <tr><td colSpan="7" className="empty-cell">No metrics yet. Ingest events and wait for Spark.</td></tr>
                ) : visibleMetrics.map((row) => (
                  <tr key={row.client_id}>
                    <td>{row.use_case_topic ?? "raw_events"}</td>
                    <td className="client-cell">{row.client_id}</td>
                    <td><StatusPill value={row.duplicate_ratio} /></td>
                    <td>{row.raw_events}</td>
                    <td>{row.unique_events}</td>
                    <td>{row.duplicate_events}</td>
                    <td>{formatRatio(row.duplicate_ratio)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {activeTab === "radar" && (
        <section className="panel section-panel">
          <SectionTitle activeTab={activeTab} />
          <div className="two-column">
            <div className="decision-list">
              {visibleMetrics.map((row) => {
                const limit = limits[row.client_id];
                return (
                  <div className="decision-row" key={row.client_id}>
                    <div>
                      <strong>{row.client_id}</strong>
                      <p>{row.use_case_topic ?? "raw_events"} · {formatRatio(row.duplicate_ratio)} duplicate ratio</p>
                    </div>
                    <span>{limit?.configured_limit_per_minute ?? "-"}/min</span>
                  </div>
                );
              })}
            </div>
            <div className="usecase-card">
              <h3>Latest Decisions</h3>
              {decisions.length === 0 ? (
                <p>Click Update RADAR to generate current limit decisions.</p>
              ) : visibleDecisions.map((decision) => (
                <div className="mini-row" key={decision.client_id}>
                  <span>{decision.client_id}</span>
                  <strong>{decision.new_limit_per_minute}/min</strong>
                </div>
              ))}
            </div>
          </div>
        </section>
      )}

      {activeTab === "evidence" && (
        <section className="panel section-panel">
          <SectionTitle activeTab={activeTab} />
          <div className="observability-links-simple">
            <span>Live metrics are available separately:</span>
            <a href="http://localhost:9090" target="_blank" rel="noreferrer">Prometheus</a>
            <a href="http://localhost:3000/d/radar-overview/radar-live-evidence" target="_blank" rel="noreferrer">Grafana Dashboard</a>
          </div>
          <div className="compare-selector">
            <label>
              Baseline use case
              <select value={baselineTopic} onChange={(event) => setBaselineTopic(event.target.value)}>
                <option value="">Select baseline topic</option>
                {topics.map((topic) => <option key={topic} value={topic}>{topic}</option>)}
              </select>
            </label>
            <label>
              Protected use case
              <select value={protectedTopic} onChange={(event) => setProtectedTopic(event.target.value)}>
                <option value="">Select protected topic</option>
                {topics.map((topic) => <option key={topic} value={topic}>{topic}</option>)}
              </select>
            </label>
          </div>
          {optimizationEvidence && (
            <div className="optimization-panel">
              <div className="optimization-header">
                <div>
                  <p className="eyebrow">Optimization proof</p>
                  <h3>Before RADAR vs After RADAR</h3>
                </div>
                <strong>{formatPercent(optimizationEvidence.loadReductionRatio)} less Kafka load</strong>
              </div>
              <div className="before-after-grid">
                <div className="compare-card before-card">
                  <span>Round 1 baseline</span>
                  <strong>{optimizationEvidence.baseline.raw}</strong>
                  <p>events reached Kafka and Spark</p>
                  <div className="load-bar"><i style={{ width: "100%" }} /></div>
                  <em>{optimizationEvidence.baseline.duplicates} duplicate records inspected by Spark</em>
                </div>
                <div className="compare-card after-card">
                  <span>Round 2 protected</span>
                  <strong>{optimizationEvidence.protectedRun.raw}</strong>
                  <p>events reached Kafka after RADAR limits</p>
                  <div className="load-bar"><i style={{ width: `${Math.min(100, optimizationEvidence.protectedLoadRatio)}%` }} /></div>
                  <em>{optimizationEvidence.protectedRun.duplicates} duplicate records inspected by Spark</em>
                </div>
                <div className="compare-card gain-card">
                  <span>System gain</span>
                  <strong>{optimizationEvidence.loadReduced}</strong>
                  <p>events blocked before Kafka</p>
                  <b>{formatPercent(optimizationEvidence.wasteReductionRatio)} less Spark duplicate waste</b>
                  <em>{optimizationEvidence.wasteReduced} duplicate inspections avoided</em>
                </div>
              </div>
            </div>
          )}
<div className="evidence-layout">
            <div className="task-panel">
              <h3>End-to-End Task Trace</h3>
              <p className="panel-note">This makes the hidden distributed-system work visible for demo and evaluation.</p>
              <div className="task-list">
                {taskLog.map((task, index) => (
                  <div className={`task-row task-${task.status}`} key={`${task.stage}-${index}`}>
                    <span>{index + 1}</span>
                    <div>
                      <strong>{task.stage}</strong>
                      <p>{task.detail}</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
            <div className="evidence-grid evidence-grid-compact">
              <div className="usecase-card">
                <h3>Kafka Load Proxy</h3>
                <p>For the selected use-case topic, admitted events became Kafka messages and blocked events were stopped before Kafka.</p>
                <strong>{uploadState.accepted} admitted / {uploadState.rejected} blocked</strong>
              </div>
              <div className="usecase-card">
                <h3>Spark Waste Signal</h3>
                <p>For the selected use-case topic, duplicate events are records Spark inspected but did not keep as clean events.</p>
                <strong>{summary.duplicates} duplicate records</strong>
              </div>
              <div className="usecase-card">
                <h3>Replay Client</h3>
                <p>Within the selected use-case topic, the highest duplicate-ratio client should receive a lower Redis limit after RADAR runs.</p>
                <strong>{replayClient ? `${replayClient.client_id}: ${formatRatio(replayClient.duplicate_ratio)}` : "No data"}</strong>
              </div>
              <div className="usecase-card">
                <h3>RADAR Decisions</h3>
                <p>Limit decisions show how downstream ClickHouse metrics change upstream ingestion behavior.</p>
                <strong>{visibleDecisions.length ? `${visibleDecisions.length} clients updated` : "Run RADAR"}</strong>
              </div>
            </div>
          </div>
        </section>
      )}

          {(message || error) && (
            <footer className="event-log">
              {message && <span>{message}</span>}
              {error && <strong>{error}</strong>}
            </footer>
          )}
        </section>
      </div>
    </main>
  );
}












