import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import axios from 'axios';
import API_BASE_URL from './config/api';
import GraphView from './GraphView';
import './App.css';

// Configure Axios with 5000ms timeout and 2 retries
axios.defaults.timeout = 5000;

axios.interceptors.response.use(
  (response) => response,
  (error) => {
    const config = error.config;
    if (!config) return Promise.reject(error);
    
    config.__retryCount = config.__retryCount || 0;
    if (config.__retryCount >= 2) {
      return Promise.reject(error);
    }
    
    config.__retryCount += 1;
    return axios(config);
  }
);

// ─── Node type config ─────────────────────────────────────────────────────────
const NODE_TYPES = {
  Product:   { color: "#22c55e", border: "#16a34a", label: "Product" },
  Group:     { color: "#f59e0b", border: "#d97706", label: "Group" },
  User:      { color: "#a78bfa", border: "#7c3aed", label: "User" },
  Division:  { color: "#fbbf24", border: "#d97706", label: "Division" },
  Sector:    { color: "#2dd4bf", border: "#0d9488", label: "Sector" },
  Attribute: { color: "#93c5fd", border: "#3b82f6", label: "Attribute" },
};

// ─── convertToGraph ───────────────────────────────────────────────────────────
const convertToGraph = (rows) => {
  if (!rows?.length) return { nodes: [], edges: [] };

  const nodeMap = new Map();
  const edgeSet = new Set();
  const nodes = [];
  const edges = [];

  const makeColor = (type) => ({
    background: NODE_TYPES[type]?.color || "#93c5fd",
    border: NODE_TYPES[type]?.border || "#3b82f6",
    highlight: { background: NODE_TYPES[type]?.color || "#60a5fa", border: "#1e40af" },
    hover:     { background: NODE_TYPES[type]?.color || "#60a5fa", border: "#1e40af" },
  });

  const addNode = (id, label, type, rawData = {}) => {
    if (nodeMap.has(id)) return;
    nodeMap.set(id, true);
    nodes.push({ id, label, type, color: makeColor(type), title: label, rawData });
  };

  const addEdge = (from, to) => {
    const key = `${from}→${to}`;
    if (edgeSet.has(key)) return;
    edgeSet.add(key);
    edges.push({ from, to });
  };

  rows.slice(0, 80).forEach((row, index) => {
    const primaryKey = row.product ?? row.customer ?? row.order_id ?? `row_${index}`;
    const primaryId  = `product_${primaryKey}`;
    const primaryLbl = String(primaryKey).slice(0, 20);
    addNode(primaryId, primaryLbl, "Product", row);

    // Smart relationship mapping
    if (row.product_group != null) {
      const gid = `group_${row.product_group}`;
      addNode(gid, String(row.product_group), "Group", { product_group: row.product_group });
      addEdge(primaryId, gid);
    }
    if (row.division != null) {
      const did = `div_${row.division}`;
      addNode(did, String(row.division), "Division", { division: row.division });
      addEdge(primaryId, did);
    }
    if (row.created_by_user != null) {
      const uid = `user_${row.created_by_user}`;
      addNode(uid, String(row.created_by_user), "User", { user: row.created_by_user });
      addEdge(primaryId, uid);
    }
    if (row.industry_sector != null) {
      const sid = `sector_${row.industry_sector}`;
      addNode(sid, String(row.industry_sector), "Sector", { industry_sector: row.industry_sector });
      addEdge(primaryId, sid);
    }

    // Remaining fields → attribute nodes
    const skip = new Set(["product","customer","order_id","product_group","division","created_by_user","industry_sector"]);
    Object.entries(row).forEach(([key, value]) => {
      if (skip.has(key) || value == null) return;
      const aid = `attr_${key}_${String(value).slice(0, 30)}`;
      addNode(aid, `${key}: ${String(value).slice(0, 14)}`, "Attribute", { [key]: value });
      addEdge(primaryId, aid);
    });
  });

  console.log("[convertToGraph]", nodes.length, "nodes,", edges.length, "edges");
  return { nodes, edges };
};

// ─── Graph insights ───────────────────────────────────────────────────────────
const computeInsights = (graphData) => {
  if (!graphData.nodes.length) return null;

  const degreeMap = new Map();
  graphData.edges.forEach(({ from, to }) => {
    degreeMap.set(from, (degreeMap.get(from) || 0) + 1);
    degreeMap.set(to,   (degreeMap.get(to)   || 0) + 1);
  });

  const sorted = [...degreeMap.entries()].sort((a, b) => b[1] - a[1]);
  const topNode = sorted[0]
    ? graphData.nodes.find(n => n.id === sorted[0][0])
    : null;

  const typeCounts = {};
  graphData.nodes.forEach(n => { typeCounts[n.type] = (typeCounts[n.type] || 0) + 1; });

  return { topNode, topDegree: sorted[0]?.[1] || 0, typeCounts, totalEdges: graphData.edges.length };
};

// ─── Clean answer extractor ──────────────────────────────────────────────────
const extractAnswer = (data) => {
  if (!data) return "No response received.";
  // Clean answer field
  if (data.answer && typeof data.answer === "string") return data.answer;
  if (data.message && typeof data.message === "string") return data.message;
  
  // Backend error field
  if (data.error && typeof data.error === "string") {
    if (data.error.includes("429") || data.error.toLowerCase().includes("quota"))
      return "⚠️ AI quota exceeded. Please wait a moment and try again.";
    if (data.error.toLowerCase().includes("rate"))
      return "⚠️ Rate limit hit. Please slow down and retry shortly.";
    return `⚠️ ${data.error}`;
  }

  if (Array.isArray(data.data)) {
    return `Found ${data.data.length} results.`;
  }

  return "No answer returned.";
};

// ─── App ──────────────────────────────────────────────────────────────────────
function App() {
  const [apiStatus, setApiStatus]   = useState("checking");
  const [query, setQuery]           = useState("");
  const [chatHistory, setChatHistory] = useState([
    {
      role: "ai",
      content: "Hi! I can help you analyze the Order to Cash process.",
      suggestions: [
        "Which products are in the highest number of billing documents?",
        "Trace the full flow of billing document 90000000",
        "How many orders were created by user CB99?",
      ]
    },
  ]);
  const [graphData, setGraphData]   = useState({ nodes: [], edges: [] });
  const [isLoading, setIsLoading]   = useState(false);
  const [isMinimized, setIsMinimized] = useState(false);
  const [showLabels, setShowLabels] = useState(false);
  const [selectedNode, setSelectedNode] = useState(null);
  const [activeTypes, setActiveTypes]   = useState(new Set(Object.keys(NODE_TYPES)));
  const [showFilters, setShowFilters]   = useState(false);
  const chatEndRef = useRef(null);

  // ── Boot ──
  useEffect(() => {
    axios.get(`${API_BASE_URL}/`)
      .then(() => setApiStatus("online"))
      .catch((err) => {
        console.error("API Status Check Failed:", err.message);
        setApiStatus("offline");
      });

    setIsLoading(true);
    axios.get(`${API_BASE_URL}/api/graph`)
      .then(res => {
        if (!res.data) throw new Error("Empty response");
        const g = { nodes: res.data.nodes || [], edges: res.data.edges || [] };
        setGraphData(g);
      })
      .catch(err => {
        console.error("Graph fetch error:", err.message);
        let errorMsg = "Failed to load graph data.";
        if (err.code === 'ECONNABORTED') errorMsg = "Request timed out.";
        else if (!err.response) errorMsg = "Network error while fetching graph.";
        setChatHistory(prev => [...prev, { role: "error", content: `⚠️ ${errorMsg}` }]);
      })
      .finally(() => {
        setIsLoading(false);
      });
  }, []);

  useEffect(() => { chatEndRef.current?.scrollIntoView({ behavior: "smooth" }); }, [chatHistory]);

  // ── Filtered graph data based on active type toggles ──
  const filteredGraphData = useMemo(() => {
    if (activeTypes.size === Object.keys(NODE_TYPES).length) return graphData;
    const activeIds = new Set(graphData.nodes.filter(n => activeTypes.has(n.type)).map(n => n.id));
    return {
      nodes: graphData.nodes.filter(n => activeTypes.has(n.type)),
      edges: graphData.edges.filter(e => activeIds.has(e.from) && activeIds.has(e.to)),
    };
  }, [graphData, activeTypes]);

  const insights = useMemo(() => computeInsights(filteredGraphData), [filteredGraphData]);

  // ── Node click: show popover instantly, never touch chat ──
  const handleNodeClick = useCallback((nodeId, nodeData) => {
    if (!nodeId) { setSelectedNode(null); return; }

    // Use rawData if available, otherwise build a minimal display from node fields
    const raw = nodeData?.rawData || {};
    const hasRaw = Object.keys(raw).length > 0;
    const displayData = hasRaw ? raw : {
      id:    nodeData?.id || nodeId,
      label: nodeData?.label || nodeId,
      type:  nodeData?.type || "Node",
    };

    setSelectedNode({ id: nodeId, data: { ...nodeData, displayData } });
  }, []);

  // ── Analyze Graph: client-side insights, NO API call ──
  const handleAnalyzeGraph = useCallback(() => {
    if (!filteredGraphData.nodes.length) {
      setChatHistory(prev => [...prev, { role: "ai", content: "No graph data to analyze. Ask a question first." }]);
      return;
    }

    const ins = computeInsights(filteredGraphData);
    if (!ins) return;

    const typeLines = Object.entries(ins.typeCounts)
      .map(([t, c]) => `  • ${t}: ${c} nodes`)
      .join("\n");

    const summary = [
      `📊 **Graph Analysis** (${filteredGraphData.nodes.length} nodes, ${filteredGraphData.edges.length} edges)`,
      ``,
      `**Most connected:** ${ins.topNode?.label || "N/A"} (${ins.topDegree} connections)`,
      ``,
      `**Node breakdown:**`,
      typeLines,
    ].join("\n");

    setChatHistory(prev => [...prev, { role: "ai", content: summary }]);
  }, [filteredGraphData]);

  const handleNodeHover = useCallback(() => {}, []);

  // ── Manual query ──
  const handleQuery = async (e, forcedQuery = null) => {
    if (e) e.preventDefault();
    const q = (forcedQuery || query).trim();
    if (!q || isLoading) return;

    setChatHistory(prev => [...prev, { role: "user", content: q }]);
    setQuery("");
    setIsLoading(true);
    setSelectedNode(null);

    try {
      const res = await axios.post(`${API_BASE_URL}/api/query`, { query: q });
      
      if (!res.data) {
        throw new Error("Empty response");
      }
      
      console.log("[api/query]", res.data);
      const answer = extractAnswer(res.data);
      setChatHistory(prev => [...prev, { role: "ai", content: answer }]);

      const rows = res.data.data;
      if (Array.isArray(rows) && rows.length > 0) {
        const graph = convertToGraph(rows);
        setGraphData(graph);
      }
    } catch (err) {
      let errorMsg = "Query failed. Please try again.";
      if (err.code === 'ECONNABORTED') errorMsg = "Request timed out. The server took too long to respond.";
      else if (!err.response) errorMsg = "Network error. Please check your connection.";
      else if (err.message === "Empty response") errorMsg = "Received an empty response from the server.";
      
      setChatHistory(prev => [...prev, { role: "error", content: `⚠️ ${errorMsg}` }]);
    } finally {
      setIsLoading(false);
    }
  };

  const toggleType = (type) =>
    setActiveTypes(prev => {
      const next = new Set(prev);
      next.has(type) ? next.delete(type) : next.add(type);
      return next;
    });

  const isOnline = apiStatus === "online";

  return (
    <div className="shell">
      {/* ── Top Bar ── */}
      <header className="topbar">
        <div className="topbar-left">
          <div className="logo-mark">⬡</div>
          <nav className="breadcrumb">
            <span className="bc-muted">Mapping</span>
            <span className="bc-sep">/</span>
            <span className="bc-active">Order to Cash</span>
          </nav>
          {/* Always-visible graph toggle */}
          <button className="topbar-graph-toggle" onClick={() => setIsMinimized(v => !v)} title={isMinimized ? "Show Graph" : "Hide Graph"}>
            {isMinimized ? <ExpandIcon /> : <MinusIcon />}
            {isMinimized ? "Show Graph" : "Hide Graph"}
          </button>
        </div>
        <div className="topbar-right">
          {filteredGraphData.nodes.length > 0 && (
            <span className="node-chip">{filteredGraphData.nodes.length} nodes</span>
          )}
          <span className={`status-pill ${apiStatus}`}>
            <span className="status-dot" />
            {isOnline ? "API Online" : apiStatus === "offline" ? "API Offline" : "Connecting…"}
          </span>
        </div>
      </header>

      <div className="workspace">
        {/* ── Graph Area ── */}
        <div className={`graph-area ${isMinimized ? "minimized" : ""}`}>

          {/* Toolbar */}
          <div className="graph-toolbar">
            <button
              className={`toolbar-btn ${showLabels ? "active" : ""}`}
              onClick={() => setShowLabels(v => !v)}
            >
              <OverlayIcon />
              {showLabels ? "Hide Labels" : "Show Labels"}
            </button>
            <button
              className={`toolbar-btn ${showFilters ? "active" : ""}`}
              onClick={() => setShowFilters(v => !v)}
            >
              <FilterIcon />
              Filter
            </button>
            {filteredGraphData.nodes.length > 0 && (
              <button className="toolbar-btn toolbar-btn-analyze" onClick={handleAnalyzeGraph}>
                <AnalyzeIcon />
                Analyze Graph
              </button>
            )}
            {filteredGraphData.nodes.length > 0 && (
              <span className="graph-meta">
                {filteredGraphData.nodes.length} nodes · {filteredGraphData.edges.length} edges
              </span>
            )}
          </div>

          {/* Filter Panel */}
          {showFilters && (
            <div className="filter-panel">
              <div className="filter-title">Node Types</div>
              {Object.entries(NODE_TYPES).map(([type, cfg]) => (
                <label key={type} className="filter-item">
                  <input
                    type="checkbox"
                    checked={activeTypes.has(type)}
                    onChange={() => toggleType(type)}
                  />
                  <span className="filter-dot" style={{ background: cfg.color }} />
                  <span className="filter-label">{cfg.label}</span>
                </label>
              ))}
              {insights && (
                <div className="insight-block">
                  <div className="insight-title">Graph Insights</div>
                  {insights.topNode && (
                    <div className="insight-row">
                      <span>🔗 Most connected</span>
                      <strong>{insights.topNode.label} ({insights.topDegree})</strong>
                    </div>
                  )}
                  <div className="insight-row">
                    <span>Total edges</span>
                    <strong>{insights.totalEdges}</strong>
                  </div>
                  {Object.entries(insights.typeCounts).map(([t, c]) => (
                    <div key={t} className="insight-row">
                      <span style={{ color: NODE_TYPES[t]?.color }}>● {t}</span>
                      <strong>{c}</strong>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Graph canvas */}
          <div className="graph-canvas">
            {isLoading && !chatHistory[chatHistory.length - 1]?.content.startsWith("📌") && (
              <div className="graph-spinner">
                <div className="spinner-ring" />
                <span>Analyzing…</span>
              </div>
            )}
            {filteredGraphData.nodes.length > 0 ? (
              <GraphView
                data={filteredGraphData}
                showLabels={showLabels}
                onNodeClick={handleNodeClick}
                onNodeHover={handleNodeHover}
              />
            ) : (
              !isLoading && (
                <div className="graph-empty">
                  <ScatterDots />
                  <p className="empty-hint">Ask a question to visualize the graph</p>
                </div>
              )
            )}
          </div>

          {/* Node detail popover */}
          {selectedNode?.data && (
            <div className="node-popover">
              <div className="popover-header">
                <div className="popover-type-badge" style={{ background: NODE_TYPES[selectedNode.data.type]?.color + "22", color: NODE_TYPES[selectedNode.data.type]?.color, borderColor: NODE_TYPES[selectedNode.data.type]?.color + "44" }}>
                  {selectedNode.data.type}
                </div>
                <button className="popover-close" onClick={() => setSelectedNode(null)}>✕</button>
              </div>
              <div className="popover-rows">
                {Object.entries(selectedNode.data.displayData || selectedNode.data.rawData || {}).map(([k, v]) => (
                  <div key={k} className="popover-row">
                    <span className="prow-key">{k}</span>
                    <span className="prow-val">{String(v ?? "—")}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Legend */}
          <div className="graph-legend">
            {Object.entries(NODE_TYPES).filter(([type]) => activeTypes.has(type)).map(([type, cfg]) => (
              <button key={type} className={`legend-item ${activeTypes.has(type) ? "" : "dim"}`} onClick={() => toggleType(type)}>
                <span className="legend-dot" style={{ background: cfg.color }} />
                <span className="legend-label">{cfg.label}</span>
              </button>
            ))}
          </div>
        </div>

        {/* ── Chat Panel ── */}
        <aside className="chat-panel">
          <div className="chat-header">
            <div>
              <div className="chat-title">Chat with Graph</div>
              <div className="chat-subtitle">Order to Cash</div>
            </div>
          </div>

          <div className="chat-body">
            {chatHistory.map((msg, i) => (
              <div key={i} className={`msg msg-${msg.role}`}>
                {msg.role === "ai" && <div className="ai-avatar"><span>D</span></div>}
                <div className="msg-inner">
                  {msg.role === "ai" && (
                    <span className="ai-name">Dodge AI <span className="ai-tag">Graph Agent</span></span>
                  )}
                  {msg.content.includes("partial flow") ? (
                    <div className="msg-text msg-warning">
                      <strong>⚠️ Partial Flow Detected</strong><br/><br/>
                      {msg.content}
                    </div>
                  ) : (
                    <p className="msg-text">{msg.content}</p>
                  )}
                  {msg.suggestions && (
                    <div className="suggestion-chips">
                      {msg.suggestions.map((sug, idx) => (
                        <button key={idx} className="sug-chip" onClick={() => handleQuery(null, sug)}>
                          <SparkleIcon />
                          <span className="sug-text">{sug}</span>
                        </button>
                      ))}
                    </div>
                  )}
                </div>
                {msg.role === "user" && <div className="user-avatar">You</div>}
              </div>
            ))}
            {isLoading && (
              <div className="msg msg-ai">
                <div className="ai-avatar"><span>D</span></div>
                <div className="msg-inner">
                  <span className="ai-name">Dodge AI</span>
                  <div className="typing-dots"><span /><span /><span /></div>
                </div>
              </div>
            )}
            <div ref={chatEndRef} />
          </div>

          <div className="chat-status">
            <span className="status-indicator" />
            Dodge AI is awaiting instructions
          </div>

          <form className="chat-input" onSubmit={handleQuery}>
            <input
              type="text"
              placeholder="Analyze anything…"
              value={query}
              onChange={e => setQuery(e.target.value)}
              disabled={isLoading}
            />
            <button type="submit" disabled={isLoading || !query.trim()}>Send</button>
          </form>
        </aside>
      </div>
    </div>
  );
}

// ── Tiny icon components ──────────────────────────────────────────────────────
const MinusIcon = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M5 12h14"/></svg>
);
const ExpandIcon = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M15 3h6v6M9 21H3v-6M21 3l-7 7M3 21l7-7"/></svg>
);
const OverlayIcon = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><circle cx="12" cy="12" r="3"/><path d="M12 2v2M12 20v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M2 12h2M20 12h2"/></svg>
);
const FilterIcon = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M22 3H2l8 9.46V19l4 2v-8.54L22 3z"/></svg>
);
const AnalyzeIcon = () => (
  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M18 20V10M12 20V4M6 20v-6"/></svg>
);
const SparkleIcon = () => (
  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M12 3l2 5h5l-4 4 1.5 5.5L12 15l-4.5 2.5L9 12 5 8h5l2-5z"/></svg>
);
const ScatterDots = () => (
  <div className="empty-dots" aria-hidden>
    {Array.from({ length: 30 }).map((_, i) => (
      <span key={i} className="empty-dot" style={{
        left: `${(i * 37 + 5) % 90 + 5}%`,
        top:  `${(i * 53 + 10) % 80 + 10}%`,
        animationDelay: `${(i * 0.13) % 3}s`,
        opacity: (i % 5) * 0.08 + 0.1,
      }} />
    ))}
  </div>
);

export default App;
