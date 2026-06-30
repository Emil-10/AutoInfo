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
  setStatus("Načítám data...", "loading");

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
    setStatus("Bez výsledků", "warning");
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
  resultSource.textContent = "Vyhledávání";
  resultQuery.textContent = "";
  resultSummary.innerHTML = "";
  highlightsGrid.innerHTML = "";
  ownershipCard.innerHTML = "";
  detailsStack.innerHTML = "";
  timelineCard.innerHTML = "";
  resultSummary.appendChild(loadingTemplate.content.cloneNode(true));
}

function renderResult(payload) {
  if (payload.kind === "fleet") {
    renderFleetResult(payload);
    return;
  }

  results.hidden = false;
  resultSource.textContent = payload.source?.label || "Zdroj";
  resultQuery.textContent = renderQueryLabel(payload.query);
  resultSummary.innerHTML = `
    <div class="summary-head">
      <div>
        <div class="summary-badge">${escapeHtml(payload.hero?.badge || "Vozidlo")}</div>
        <h1>${escapeHtml(payload.hero?.title || "Bez názvu")}</h1>
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

function renderFleetResult(payload) {
  const records = Array.isArray(payload.records) ? payload.records : [];
  const company = payload.company || {};
  const summary = payload.summary || {};
  const companyName = company.name || `Firma ${company.ico || payload.query?.normalized || ""}`.trim();

  results.hidden = false;
  resultSource.textContent = "Otevřená data RSV";
  resultQuery.textContent = renderQueryLabel(payload.query);
  resultSummary.innerHTML = `
    <div class="summary-head">
      <div>
        <div class="summary-badge">Právnická osoba</div>
        <h1>${escapeHtml(companyName || "Firma")}</h1>
      </div>
      <div class="summary-state">IČO ${escapeHtml(company.ico || payload.query?.normalized || "-")}</div>
    </div>
    <p class="summary-text">${escapeHtml(payload.message || "Aktuálně vlastněná nebo provozovaná vozidla podle otevřených dat Registru silničních vozidel.")}</p>
    ${
      summary.truncated
        ? `<p class="summary-note">Výsledek byl zkrácen na prvních ${escapeHtml(String(summary.displayedCount || records.length))} vozidel.</p>`
        : ""
    }
  `;

  highlightsGrid.innerHTML = [
    { label: "Vozidla", value: summary.vehicleCount ?? records.length },
    { label: "Aktuální", value: summary.currentVehicleCount ?? records.length },
    { label: "Vztahy", value: summary.relationshipCount ?? "-" },
    { label: "Zobrazeno", value: summary.displayedCount ?? records.length }
  ]
    .map(
      (item) => `
        <article class="metric-card">
          <span>${escapeHtml(item.label)}</span>
          <strong>${escapeHtml(item.value)}</strong>
        </article>
      `
    )
    .join("");
  highlightsGrid.hidden = false;

  ownershipCard.innerHTML = `
    <p class="section-label">Firma</p>
    <p class="card-note">IČO ${escapeHtml(company.ico || payload.query?.normalized || "-")}</p>
    ${company.address ? `<p class="card-note">${escapeHtml(company.address)}</p>` : ""}
  `;

  detailsStack.innerHTML = records.length
    ? records
        .map((record) => {
          const title = [record.make, record.model, record.type].filter(Boolean).join(" ").trim() || record.vin || record.pcv || "Vozidlo";
          const detail = [
            record.category,
            record.fuel,
            record.firstRegistration ? `1. registrace ${formatTimelineDate(record.firstRegistration)}` : null,
            record.status
          ].filter(Boolean).join(" - ");
          return `
            <article class="party-card">
              <div class="party-role">${escapeHtml(record.current ? "Aktuální vztah" : "Vozidlo")}</div>
              <div class="party-name">${escapeHtml(title)}</div>
              <p class="card-note">VIN ${escapeHtml(record.vin || "-")} - PCV ${escapeHtml(record.pcv || "-")}</p>
              ${detail ? `<p class="card-note">${escapeHtml(detail)}</p>` : ""}
            </article>
          `;
        })
        .join("")
    : `<article class="party-card"><p class="card-note">${escapeHtml(payload.message || "Pro zadané IČO nebyla nalezena žádná aktivní vozidla.")}</p></article>`;
  resultSections.hidden = false;
  timelineCard.hidden = true;
  timelineCard.innerHTML = "";
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
        <div class="summary-badge warning-badge">Bez záznamu</div>
        <h1>Pro tento identifikátor zatím nic nemám</h1>
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
    <p class="section-label">Vlastnictví</p>
    <div class="ownership-metrics">
      <div class="mini-metric">
        <span>Vlastníci</span>
        <strong>${escapeHtml(String(ownership.ownerCount ?? "-"))}</strong>
      </div>
      <div class="mini-metric">
        <span>Provozovatelé</span>
        <strong>${escapeHtml(String(ownership.operatorCount ?? "-"))}</strong>
      </div>
    </div>
    <p class="card-note">${escapeHtml(ownership.note || "Bez doplňující poznámky.")}</p>
    <div class="party-list">
      ${
        parties.length
          ? parties
              .map(
                (party) => `
                  <article class="party-card">
                    <div class="party-role">${escapeHtml(party.role || "Subjekt")}</div>
                    <div class="party-name">${escapeHtml(party.name || "Bez názvu")}</div>
                    <p class="card-note">
                      ${party.ico ? `IČO ${escapeHtml(party.ico)}` : "Bez veřejného IČO"}
                      ${party.address ? ` • ${escapeHtml(party.address)}` : ""}
                      ${party.since ? ` • od ${escapeHtml(party.since)}` : ""}
                    </p>
                  </article>
                `
              )
              .join("")
          : `<article class="party-card"><p class="card-note">Zdroj nevrátil seznam subjektů.</p></article>`
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
                <strong>${escapeHtml(entry.title || "Událost")}</strong>
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

function isValidIco(value) {
  const digits = String(value || "").replace(/\D/g, "");
  if (!/^\d{8}$/.test(digits)) {
    return false;
  }

  let control = 11 - [8, 7, 6, 5, 4, 3, 2].reduce((sum, weight, index) => {
    return sum + Number(digits[index]) * weight;
  }, 0) % 11;
  if (control === 10) {
    control = 0;
  } else if (control === 11) {
    control = 1;
  }

  return control === Number(digits[7]);
}

function composeErrorMessage(payload) {
  if (!payload) {
    return "Neznámá chyba při vyhledávání.";
  }

  const lines = [payload.message || "Vyhledávání selhalo."];

  if (Array.isArray(payload.hints) && payload.hints.length) {
    lines.push(payload.hints.map(sanitizeLookupErrorText).filter(Boolean).join(" "));
  }

  const detail = sanitizeLookupErrorText(payload.detail);
  if (detail && !isInternalLookupErrorDetail(detail)) {
    lines.push(detail);
  }

  return sanitizeLookupErrorText(lines.join(" "));
}

function sanitizeLookupErrorText(value) {
  const text = String(value || "");
  if (!text) {
    return "";
  }

  const internalProvider = ["P", "V", "Z", "P"].join("");
  const secondaryProvider = ["U", "N", "I", "Q", "A"].join("");
  return text
    .replace(new RegExp(`\\b${internalProvider}\\b`, "gi"), "externí zdroj")
    .replace(new RegExp(`\\b${secondaryProvider}\\b`, "gi"), "externí zdroj")
    .replace(/\bTRANSPORT_CUBE_LOOKUP_URL\b/g, "primární zdroj")
    .replace(/\bbrowserType\.launch:[^.;]*(?:[.;]|$)/gi, "")
    .replace(/\bEPERM:[^.;]*(?:[.;]|$)/gi, "")
    .replace(/\bmkdtemp\s+'[^']*'/gi, "")
    .replace(/\bconnect\s+(?:ECONNREFUSED|EACCES|ETIMEDOUT)\s+[^\s.;]+/gi, "zdroj je dočasně nedostupný")
    .replace(/\s+/g, " ")
    .trim();
}

function isInternalLookupErrorDetail(value) {
  const normalized = String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
  return [
    "browsertype launch",
    "mkdtemp",
    "eperm",
    "transport cube",
    "pvzp",
    "uniqa"
  ].some((marker) => normalized.includes(marker));
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
