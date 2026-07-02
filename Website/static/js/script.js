(() => {
  const dashboard = document.getElementById("dashboardApp");
  if (!dashboard) {
    return;
  }

  const predictions = safeParse(dashboard.dataset.predictions, []);
  const companies = safeParse(dashboard.dataset.companyList, []);
  const searchInput = document.getElementById("tickerSearch");
  const heroTickerName = document.getElementById("heroTickerName");
  const heroTickerPrice = document.getElementById("heroTickerPrice");
  const heroTickerChange = document.getElementById("heroTickerChange");
  const statusNode = document.getElementById("scriptStatus");

  buildHeroChart(predictions);
  wireCompanyChips(searchInput);
  wireScriptTriggers(statusNode);
  hydrateHero(predictions, heroTickerName, heroTickerPrice, heroTickerChange);
  loadPredictionVsActual();

  if (searchInput && companies.length > 0) {
    searchInput.addEventListener("input", () => {
      const match = companies.find((name) => name.startsWith(searchInput.value.toLowerCase()));
      searchInput.dataset.match = match || "";
    });
  }

  function loadPredictionVsActual() {
    fetch("/prediction-vs-actual")
      .then((response) => {
        if (!response.ok) {
          throw new Error("Prediction comparison endpoint is unavailable.");
        }
        return response.json();
      })
      .then((items) => renderMovers(items))
      .catch((error) => {
        renderListState("gainersList", error.message);
        renderListState("losersList", error.message);
      });
  }
})();

function safeParse(rawValue, fallback) {
  try {
    return rawValue ? JSON.parse(rawValue) : fallback;
  } catch (error) {
    return fallback;
  }
}

function hydrateHero(predictions, nameNode, priceNode, changeNode) {
  if (!predictions.length || !nameNode || !priceNode || !changeNode) {
    return;
  }

  const topItem = predictions[0];
  nameNode.textContent = topItem.ticker;
  priceNode.textContent = formatMoney(topItem.price);
  changeNode.textContent = "+0.43%";
}

function wireCompanyChips(searchInput) {
  document.querySelectorAll("[data-company-fill]").forEach((button) => {
    button.addEventListener("click", () => {
      if (searchInput) {
        searchInput.value = button.dataset.companyFill || "";
        searchInput.focus();
      }
    });
  });
}

function wireScriptTriggers(statusNode) {
  document.querySelectorAll("[data-program-trigger]").forEach((button) => {
    button.addEventListener("click", async () => {
      const program = button.dataset.programTrigger;
      const originalLabel = button.textContent;

      button.disabled = true;
      button.textContent = "Starting...";
      setStatus(statusNode, "Launching " + program + " backend task...", "is-pending");

      try {
        const response = await fetch("/run_program", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ program })
        });

        const result = await response.json();
        if (!response.ok) {
          throw new Error(result.message || "Script start failed.");
        }

        setStatus(statusNode, result.message || "Script started.", "is-success");
      } catch (error) {
        setStatus(statusNode, error.message || "Unable to start script.", "is-error");
      } finally {
        button.disabled = false;
        button.textContent = originalLabel;
      }
    });
  });
}

function setStatus(node, message, tone) {
  if (!node) {
    return;
  }

  node.textContent = message;
  node.classList.remove("is-success", "is-error", "is-pending");
  if (tone) {
    node.classList.add(tone);
  }
}

function renderMovers(items) {
  if (!Array.isArray(items) || items.length === 0) {
    renderListState("gainersList", "No prediction vs actual records are available yet.");
    renderListState("losersList", "No prediction vs actual records are available yet.");
    return;
  }

  const enriched = items
    .filter((item) => Number.isFinite(item.predicted) && Number.isFinite(item.actual) && item.actual !== 0)
    .map((item) => {
      const changePct = ((item.predicted - item.actual) / item.actual) * 100;
      return {
        ...item,
        changePct,
        initials: (item.ticker || item.company || "?").slice(0, 2).toUpperCase()
      };
    });

  const gainers = [...enriched].sort((a, b) => b.changePct - a.changePct).slice(0, 5);
  const losers = [...enriched].sort((a, b) => a.changePct - b.changePct).slice(0, 5);

  renderMoverList("gainersList", gainers, "up");
  renderMoverList("losersList", losers, "down");
}

function renderMoverList(targetId, items, tone) {
  const container = document.getElementById(targetId);
  if (!container) {
    return;
  }

  if (!items.length) {
    container.innerHTML = '<div class="list-empty">No ranked records to display.</div>';
    return;
  }

  container.innerHTML = items.map((item) => {
    const positive = item.changePct >= 0;
    const signedChange = (positive ? "+" : "") + item.changePct.toFixed(2) + "%";
    const sparkline = buildSparklinePath(item.predicted, item.actual, positive);
    const href = "/company/" + encodeURIComponent((item.company || "").toLowerCase());

    return `
      <a class="mover-row" href="${href}">
        <span class="mover-badge ${tone === "up" ? "is-up" : "is-down"}">${item.initials}</span>
        <span class="mover-meta">
          <strong>${escapeHtml(item.ticker || item.company || "Unknown")}</strong>
          <small>${escapeHtml(item.company || "Tracked company")}</small>
        </span>
        <span class="mover-spark">
          <svg viewBox="0 0 120 42" preserveAspectRatio="none" aria-hidden="true">
            <path d="${sparkline}" />
          </svg>
        </span>
        <span class="mover-values">
          <strong>${formatMoney(item.predicted)}</strong>
          <small class="${positive ? "status-up" : "status-down"}">${signedChange}</small>
        </span>
      </a>
    `;
  }).join("");
}

function renderListState(targetId, message) {
  const container = document.getElementById(targetId);
  if (container) {
    container.innerHTML = '<div class="list-empty">' + escapeHtml(message) + "</div>";
  }
}

function buildSparklinePath(predicted, actual, positive) {
  const base = positive
    ? [18, 26, 22, 14, 19, 11, 15, 7]
    : [8, 16, 12, 19, 15, 23, 18, 26];
  const bias = predicted >= actual ? -2 : 2;
  return base.map((value, index) => {
    const x = index * 16;
    const y = value + bias;
    return (index === 0 ? "M" : "L") + x + "," + y;
  }).join(" ");
}

function buildHeroChart(predictions) {
  const chart = document.getElementById("marketLineChart");
  if (!chart) {
    return;
  }

  const values = predictions.length
    ? predictions.slice(0, 8).map((item) => Number(item.price) || 0)
    : [5120, 5160, 5140, 5205, 5240, 5222, 5280, 5248];
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = max - min || 1;
  const points = values.map((value, index) => {
    const x = (index / Math.max(values.length - 1, 1)) * 620 + 10;
    const y = 220 - ((value - min) / spread) * 150;
    return { x, y };
  });

  const path = points.map((point, index) => (index === 0 ? "M" : "L") + point.x + "," + point.y).join(" ");
  const areaPath = path + " L 630,240 L 10,240 Z";

  chart.innerHTML = `
    <defs>
      <linearGradient id="heroAreaFill" x1="0" x2="0" y1="0" y2="1">
        <stop offset="0%" stop-color="rgba(111, 203, 153, 0.36)" />
        <stop offset="100%" stop-color="rgba(111, 203, 153, 0.02)" />
      </linearGradient>
    </defs>
    <g class="chart-grid">
      <line x1="10" y1="40" x2="630" y2="40"></line>
      <line x1="10" y1="90" x2="630" y2="90"></line>
      <line x1="10" y1="140" x2="630" y2="140"></line>
      <line x1="10" y1="190" x2="630" y2="190"></line>
      <line x1="10" y1="240" x2="630" y2="240"></line>
    </g>
    <path class="chart-area" d="${areaPath}"></path>
    <path class="chart-line" d="${path}"></path>
    ${points.map((point) => `<circle cx="${point.x}" cy="${point.y}" r="3"></circle>`).join("")}
  `;
}

function formatMoney(value) {
  return Number(value || 0).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
