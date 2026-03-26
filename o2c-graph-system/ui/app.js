const API_BASE = "http://127.0.0.1:8002";

const outputArea = document.getElementById("outputArea");
const svg = d3.select("#graphSvg");
const width = +svg.attr("width");
const height = +svg.attr("height");

// New state variables for enhanced UI
let currentInsight = null;
let currentAiResponse = null;
let currentPlan = null;
let activeNodes = [];
let activeEdges = [];

function log(message) {
  outputArea.textContent = message;
}

function displayJson(obj) {
  outputArea.textContent = JSON.stringify(obj, null, 2);
}

// New function to update UI state
function updateUIState(insight, aiResponse, plan) {
  currentInsight = insight;
  currentAiResponse = aiResponse;
  currentPlan = plan;
  renderInsightPanel();
  renderAiResponse();
}

// Insight Panel Component
function renderInsightPanel() {
  const existingPanel = document.getElementById("insightPanel");
  if (existingPanel) {
    existingPanel.remove();
  }

  if (!currentInsight) return;

  const panel = document.createElement("div");
  panel.id = "insightPanel";
  panel.style.cssText = `
    background: #0f172a;
    color: white;
    padding: 12px;
    border-radius: 10px;
    margin-top: 10px;
    font-family: Inter, system-ui, sans-serif;
  `;

  panel.innerHTML = `
    <div style="font-weight: bold; margin-bottom: 6px;">💡 Insight</div>
    <div>${currentInsight.summary}</div>
    <div style="margin-top: 6px;">
      Status: <b>${currentInsight.status.toUpperCase()}</b>
    </div>
    ${currentInsight.issues?.length > 0 ?
      `<div style="margin-top: 6px; color: #f87171;">⚠️ ${currentInsight.issues.join(", ")}</div>` :
      ''
    }
  `;

  // Insert after controls section
  const controls = document.querySelector(".controls");
  controls.parentNode.insertBefore(panel, controls.nextSibling);
}

// AI Response Component
function renderAiResponse() {
  const existingResponse = document.getElementById("aiResponse");
  if (existingResponse) {
    existingResponse.remove();
  }

  if (!currentAiResponse) return;

  const responseDiv = document.createElement("div");
  responseDiv.id = "aiResponse";
  responseDiv.style.cssText = `
    background: #111827;
    color: #e5e7eb;
    padding: 10px;
    border-radius: 8px;
    margin-bottom: 10px;
    font-family: Inter, system-ui, sans-serif;
  `;

  responseDiv.innerHTML = `🤖 ${currentAiResponse}`;

  // Insert before controls section
  const controls = document.querySelector(".controls");
  controls.parentNode.insertBefore(responseDiv, controls);
}

function drawGraph(nodes, links) {
  svg.selectAll("*").remove();

  const simulation = d3.forceSimulation(nodes)
    .force("link", d3.forceLink(links).id(d => d.id).distance(90).strength(1))
    .force("charge", d3.forceManyBody().strength(-340))
    .force("center", d3.forceCenter(width / 2, height / 2));

  const link = svg.append("g").attr("stroke", "#9a9dc1").selectAll("line")
    .data(links).enter().append("line")
    .attr("class", d => `link ${activeEdges.includes(d.id) ? "active" : "inactive"}`)
    .attr("stroke-width", 2)
    .attr("opacity", d => activeEdges.includes(d.id) ? 1 : 0.1);

  const node = svg.append("g").attr("stroke", "#2a3f66").attr("stroke-width", 1.2)
    .selectAll("circle").data(nodes).enter().append("circle")
      .attr("class", d => `node ${activeNodes.includes(d.id) ? "active" : "inactive"}`)
      .attr("r", 12)
      .attr("fill", d => {
        if (activeNodes.includes(d.id)) {
          return "#3b82f6"; // highlight
        }
        return nodeColor(d.type); // faded
      })
      .attr("opacity", d => activeNodes.includes(d.id) ? 1 : 0.3)
      .call(drag(simulation));

  const labels = svg.append("g").selectAll("text").data(nodes).enter().append("text")
    .attr("class", "label").attr("dx", 14).attr("dy", 4).text(d => d.label);

  node.append("title").text(d => `${d.label}\n${d.type}`);

  simulation.on("tick", () => {
    link
      .attr("x1", d => d.source.x)
      .attr("y1", d => d.source.y)
      .attr("x2", d => d.target.x)
      .attr("y2", d => d.target.y);

    node
      .attr("cx", d => d.x)
      .attr("cy", d => d.y);

    labels
      .attr("x", d => d.x)
      .attr("y", d => d.y);
  });
}

function nodeColor(type) {
  switch(type) {
    case "SalesOrderItem": return "#65a1ff";
    case "OutboundDeliveryItem": return "#7ee787";
    case "BillingDocumentItem": return "#ffd166";
    case "JournalEntryItem": return "#e76f51";
    case "Product": return "#8ecae6";
    case "BusinessPartner": return "#ffafcc";
    default: return "#c9c9c9";
  }
}

function drag(simulation) {
  function dragstarted(event) {
    if (!event.active) simulation.alphaTarget(0.2).restart();
    event.subject.fx = event.subject.x;
    event.subject.fy = event.subject.y;
  }

  function dragged(event) {
    event.subject.fx = event.x;
    event.subject.fy = event.y;
  }

  function dragended(event) {
    if (!event.active) simulation.alphaTarget(0);
    event.subject.fx = null;
    event.subject.fy = null;
  }

  return d3.drag().on("start", dragstarted).on("drag", dragged).on("end", dragended);
}

async function fetchBrokenFlows() {
  try {
    const res = await fetch(`${API_BASE}/broken-flows`);
    const data = await res.json();
    displayJson(data);
    const nodes = [];
    const links = [];
    Object.entries(data).forEach(([key, value]) => {
      nodes.push({ id: key, label: key, type: "Summary" });
      links.push({ source: "Summary", target: key, type: "summary" });
    });
    nodes.push({ id: "Summary", label: "Summary", type: "BusinessPartner" });
    drawGraph(nodes, links);
  } catch (err) {
    log(`Error fetching broken flows: ${err.message}`);
  }
}

async function fetchTraceFlow() {
  const orderId = document.getElementById("orderId").value.trim();
  const itemId = document.getElementById("itemId").value.trim();
  try {
    const res = await fetch(`${API_BASE}/trace-flow`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ order_id: orderId, item_id: itemId })
    });
    const data = await res.json();
    displayJson(data);

    if (res.status !== 200 || !data.result) return;
    const path = data.result.flow_path;
    
    // Extract active nodes and edges
    activeNodes = path;
    activeEdges = [];
    for (let i = 0; i < path.length - 1; i++) {
      activeEdges.push(`${path[i]}->${path[i + 1]}`);
    }

    const nodes = path.map(id => {
      const type = id.split("_")[0] === "SOI" ? "SalesOrderItem"
        : id.startsWith("DEL_") ? "OutboundDeliveryItem"
        : id.startsWith("BILL_") ? "BillingDocumentItem"
        : id.startsWith("JRN_") ? "JournalEntryItem"
        : id.startsWith("PROD_") ? "Product"
        : id.startsWith("BP_") ? "BusinessPartner"
        : "Unknown";
      return { id, label: id, type };
    });
    const links = [];
    for (let i = 0; i < path.length - 1; i++) {
      links.push({ 
        id: `${path[i]}->${path[i + 1]}`,
        source: path[i], 
        target: path[i + 1], 
        type: "flow" 
      });
    }
    drawGraph(nodes, links);
  } catch (err) {
    log(`Error tracing flow: ${err.message}`);
  }
}

async function fetchLlmQuery() {
  const query = document.getElementById("llmQueryInput").value.trim();
  try {
    const res = await fetch(`${API_BASE}/llm-query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query })
    });
    const data = await res.json();
    displayJson(data);

    // Update UI state with insight and response
    updateUIState(data.insight, data.response, data.llm_plan);

    if (!data || !data.result) return;
    if (data.intent === "broken_flow_summary") {
      fetchBrokenFlows();
      return;
    } else if (data.intent === "trace_flow") {
      // if engine already did trace, re-render from result
      const path = data.result.flow_path;
      
      // Extract active nodes and edges
      activeNodes = path;
      activeEdges = [];
      for (let i = 0; i < path.length - 1; i++) {
        activeEdges.push(`${path[i]}->${path[i + 1]}`);
      }

      const nodes = path.map(id => ({ id, label: id, type: id.split("_")[0] }));
      const links = path.slice(0, -1).map((s, i) => ({ 
        id: `${s}->${path[i + 1]}`,
        source: s, 
        target: path[i + 1], 
        type: "flow" 
      }));
      drawGraph(nodes, links);
      return;
    }
  } catch (err) {
    log(`Error calling LLM query: ${err.message}`);
  }
}

// UI wiring
document.getElementById("brokenFlowsBtn").addEventListener("click", fetchBrokenFlows);
document.getElementById("traceFlowBtn").addEventListener("click", fetchTraceFlow);
document.getElementById("llmQueryBtn").addEventListener("click", fetchLlmQuery);

// initial output
log("UI loaded. Use controls to call API and inspect graph.");
