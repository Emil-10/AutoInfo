const form = document.getElementById("lookup-form");
const input = document.getElementById("vehicle-query");
const results = document.getElementById("results");
const searchStatus = document.getElementById("search-status");
const modeSwitch = document.getElementById("mode-switch");
const resultSource = document.getElementById("result-source");
const resultQuery = document.getElementById("result-query");
const resultSummary = document.getElementById("result-summary");
const highlightsGrid = document.getElementById("highlights-grid");
const resultSections = document.getElementById("result-sections");
const ownershipCard = document.getElementById("ownership-card");
const detailsStack = document.getElementById("details-stack");
const timelineCard = document.getElementById("timeline-card");
const loadingTemplate = document.getElementById("loading-template");

form.addEventListener("submit", handleSubmit);
input.addEventListener("input", handleInputChange);

handleInputChange();

async function handleSubmit(event) {
  event.preventDefault();
  const query = input.value.trim();

  if (!query) {
    setStatus("Zadej SPZ nebo VIN.", "warning");
    input.focus();
    return;
  }

  renderLoading();
  setStatus("Nacitam data...", "loading");

  try {
    const response = await fetch(`/api/lookup?query=${encodeURIComponent(query)}`, {
      headers: { Accept: "application/json" }
    });
    const payload = await response.json();

    if (!response.ok) {
      throw new Error(composeErrorMessage(payload));
    }

    renderResult(payload);
    document.body.classList.add("has-results");
    setStatus("Hotovo", "success");
  } catch (error) {
    document.body.classList.add("has-results");
    renderError(query, error.message);
    setStatus("Bez vysledku", "warning");
  }
}

function handleInputChange() {
  const type = detectInputType(input.value);
  modeSwitch.dataset.mode = type;
}

function detectInputType(value) {
  const compact = String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

  if (/^[A-HJ-NPR-Z0-9]{17}$/.test(compact)) {
    return "vin";
  }

  return "plate";
}

function renderLoading() {
  results.hidden = false;
  highlightsGrid.hidden = true;
  resultSections.hidden = true;
  timelineCard.hidden = true;
  resultSource.textContent = "Vyhledavani";
  resultQuery.textContent = "";
  resultSummary.innerHTML = "";
  highlightsGrid.innerHTML = "";
  ownershipCard.innerHTML = "";
  detailsStack.innerHTML = "";
  timelineCard.innerHTML = "";
  resultSummary.appendChild(loadingTemplate.content.cloneNode(true));
}

function renderResult(payload) {
  results.hidden = false;
  resultSource.textContent = payload.source?.label || "Zdroj";
  resultQuery.textContent = renderQueryLabel(payload.query);
  resultSummary.innerHTML = `
    <div class="summary-head">
      <div>
        <div class="summary-badge">${escapeHtml(payload.hero?.badge || "Vozidlo")}</div>
        <h1>${escapeHtml(payload.hero?.title || "Bez nazvu")}</h1>
      </div>
      <div class="summary-state">${escapeHtml(payload.hero?.status || "Neuvedeno")}</div>
    </div>
    <p class="summary-text">${escapeHtml(payload.hero?.subtitle || "")}</p>
    ${
      payload.source?.note
        ? `<p class="summary-note">${escapeHtml(payload.source.note)}</p>`
        : ""
    }
  `;

  highlightsGrid.innerHTML = (payload.highlights || [])
    .map(
      (item) => `
        <article class="metric-card">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </article>
      `
    )
    .join("");
  highlightsGrid.hidden = !payload.highlights || payload.highlights.length === 0;

  renderOwnership(payload.ownership || {});
  renderSections(payload.sections || []);
  renderTimeline(payload.timeline || []);
  resultSections.hidden = ownershipCard.innerHTML === "" && detailsStack.innerHTML === "";
  timelineCard.hidden = timelineCard.innerHTML === "";
}

function renderError(query, message) {
  results.hidden = false;
  highlightsGrid.hidden = true;
  resultSections.hidden = true;
  timelineCard.hidden = true;
  resultSource.textContent = "Nenalezeno";
  resultQuery.textContent = renderQueryLabel({
    raw: query,
    type: detectInputType(query)
  });
  resultSummary.innerHTML = `
    <div class="summary-head">
      <div>
        <div class="summary-badge warning-badge">Bez zaznamu</div>
        <h1>Pro tento identifikator zatim nic nemam</h1>
      </div>
    </div>
    <p class="summary-text">${escapeHtml(message)}</p>
  `;
  highlightsGrid.innerHTML = "";
  ownershipCard.innerHTML = "";
  detailsStack.innerHTML = "";
  timelineCard.innerHTML = "";
}

function renderOwnership(ownership) {
  const parties = Array.isArray(ownership.parties) ? ownership.parties : [];

  if (!parties.length && ownership.ownerCount == null && ownership.operatorCount == null && !ownership.note) {
    ownershipCard.innerHTML = "";
    return;
  }

  ownershipCard.innerHTML = `
    <p class="section-label">Vlastnictvi</p>
    <div class="ownership-metrics">
      <div class="mini-metric">
        <span>Vlastnici</span>
        <strong>${escapeHtml(String(ownership.ownerCount ?? "-"))}</strong>
      </div>
      <div class="mini-metric">
        <span>Provozovatele</span>
        <strong>${escapeHtml(String(ownership.operatorCount ?? "-"))}</strong>
      </div>
    </div>
    <p class="card-note">${escapeHtml(ownership.note || "Bez doplnujici poznamky.")}</p>
    <div class="party-list">
      ${
        parties.length
          ? parties
              .map(
                (party) => `
                  <article class="party-card">
                    <div class="party-role">${escapeHtml(party.role || "Subjekt")}</div>
                    <div class="party-name">${escapeHtml(party.name || "Bez nazvu")}</div>
                    <p class="card-note">
                      ${party.ico ? `ICO ${escapeHtml(party.ico)}` : "Bez verejneho ICO"}
                      ${party.address ? ` • ${escapeHtml(party.address)}` : ""}
                      ${party.since ? ` • od ${escapeHtml(party.since)}` : ""}
                    </p>
                  </article>
                `
              )
              .join("")
          : `<article class="party-card"><p class="card-note">Zdroj nevratil seznam subjektu.</p></article>`
      }
    </div>
  `;
}

function renderSections(sections) {
  if (!Array.isArray(sections) || sections.length === 0) {
    detailsStack.innerHTML = "";
    return;
  }

  detailsStack.innerHTML = sections
    .map(
      (section) => `
        <section class="info-card">
          <p class="section-label">${escapeHtml(section.title)}</p>
          <div class="info-grid">
            ${(section.items || [])
              .map(
                (item) => `
                  <div class="info-row">
                    <span>${escapeHtml(item.label)}</span>
                    <strong>${escapeHtml(item.value)}</strong>
                  </div>
                `
              )
              .join("")}
          </div>
        </section>
      `
    )
    .join("");
}

function renderTimeline(entries) {
  if (!Array.isArray(entries) || entries.length === 0) {
    timelineCard.innerHTML = "";
    return;
  }

  timelineCard.innerHTML = `
    <p class="section-label">Historie</p>
    <div class="timeline-list">
      ${entries
        .map(
          (entry) => `
            <article class="timeline-item">
              <span>${escapeHtml(formatTimelineDate(entry.date))}</span>
              <div>
                <strong>${escapeHtml(entry.title || "Udalost")}</strong>
                <p>${escapeHtml(entry.description || "")}</p>
              </div>
            </article>
          `
        )
        .join("")}
    </div>
  `;
}

function renderQueryLabel(query) {
  if (!query) {
    return "";
  }

  const type = query.type === "vin" ? "VIN" : "SPZ";
  return `${type}: ${query.raw || query.normalized || ""}`;
}

function composeErrorMessage(payload) {
  if (!payload) {
    return "Neznama chyba pri vyhledavani.";
  }

  const lines = [payload.message || "Vyhledavani selhalo."];

  if (Array.isArray(payload.hints) && payload.hints.length) {
    lines.push(payload.hints.join(" "));
  }

  if (payload.detail) {
    lines.push(payload.detail);
  }

  return lines.join(" ");
}

function setStatus(text, mode) {
  searchStatus.textContent = text;
  searchStatus.dataset.state = mode;
}

function formatTimelineDate(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value || "-";
  }

  return new Intl.DateTimeFormat("cs-CZ").format(parsed);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
