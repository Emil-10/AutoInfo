const crypto = require("crypto");
const { getPool, isDatabaseConfigured } = require("./open-data-db");

const CONSENT_COOKIE = "autoinfo_cookie_consent";
const ANALYTICS_SESSION_COOKIE = "autoinfo_analytics_session";
const AUDIT_ENABLED = stringFlag(process.env.LOOKUP_AUDIT_ENABLED, true);
const STORE_IDENTIFIERS = stringFlag(process.env.LOOKUP_AUDIT_STORE_IDENTIFIERS, true);
const STORE_USER_AGENT = stringFlag(process.env.LOOKUP_AUDIT_STORE_USER_AGENT, true);
const STORE_REFERRER = stringFlag(process.env.LOOKUP_AUDIT_STORE_REFERRER, true);
const IP_HASH_ENABLED = stringFlag(process.env.LOOKUP_AUDIT_IP_HASH_ENABLED, false);
const HASH_SALT =
  process.env.LOOKUP_AUDIT_HASH_SALT ||
  process.env.SESSION_SECRET ||
  process.env.TRANSPORT_CUBE_API_KEY ||
  "autoinfo-lookup-audit";

let schemaPromise = null;

const LOOKUP_AUDIT_SCHEMA_SQL = `
create table if not exists lookup_events (
  id bigserial primary key,
  occurred_at timestamptz not null default now(),
  event_type text not null,
  endpoint text not null,
  http_method text not null,
  status_code integer not null,
  duration_ms integer,
  consent_state text not null default 'unknown',
  analytics_session_id text,
  actor_key text,
  ip_hash text,
  user_agent text,
  referrer text,
  query_type text,
  query_value text,
  query_hash text,
  query_masked text,
  result_kind text,
  result_status text,
  vehicle_pcv text,
  vehicle_vin text,
  vehicle_plate text,
  vehicle_make text,
  vehicle_model text,
  owner_count integer,
  operator_count integer,
  ownership_subject_count integer,
  inspection_status text,
  inspection_valid_until text,
  vignette_status text,
  source_labels text[],
  error_message text,
  result_summary jsonb,
  created_at timestamptz not null default now()
);

create index if not exists lookup_events_occurred_at_idx
  on lookup_events (occurred_at desc);

create index if not exists lookup_events_query_type_idx
  on lookup_events (query_type, occurred_at desc);

create index if not exists lookup_events_session_idx
  on lookup_events (analytics_session_id, occurred_at desc)
  where analytics_session_id is not null;

create index if not exists lookup_events_query_hash_idx
  on lookup_events (query_hash, occurred_at desc)
  where query_hash is not null;

create index if not exists lookup_events_vehicle_plate_idx
  on lookup_events (vehicle_plate, occurred_at desc)
  where vehicle_plate is not null;

create index if not exists lookup_events_vehicle_vin_idx
  on lookup_events (vehicle_vin, occurred_at desc)
  where vehicle_vin is not null;
`;

async function recordLookupEvent(input = {}) {
  if (!AUDIT_ENABLED || !isDatabaseConfigured()) {
    return null;
  }

  const pool = getPool();
  if (!pool) {
    return null;
  }

  await ensureLookupAuditSchema(pool);

  const request = input.req || null;
  const requestUrl = input.requestUrl || null;
  const payload = input.payload || null;
  const cookies = parseCookies(request?.headers?.cookie || "");
  const consentState = sanitizeConsentState(cookies[CONSENT_COOKIE]);
  const analyticsSessionId = consentState === "analytics"
    ? sanitizeAnalyticsSessionId(cookies[ANALYTICS_SESSION_COOKIE])
    : null;
  const queryContext = extractQueryContext(input, payload);
  const resultContext = extractResultContext(input, payload);
  const clientContext = extractClientContext(request, cookies);

  const params = [
    cleanText(input.eventType || "lookup", 80),
    cleanText(requestUrl?.pathname || input.endpoint || "unknown", 160),
    cleanText(request?.method || input.method || "GET", 16),
    normalizeInteger(input.statusCode, 0),
    normalizeInteger(input.durationMs, null),
    consentState,
    analyticsSessionId,
    clientContext.actorKey,
    clientContext.ipHash,
    clientContext.userAgent,
    clientContext.referrer,
    queryContext.type,
    STORE_IDENTIFIERS ? queryContext.value : null,
    queryContext.hash,
    queryContext.masked,
    resultContext.kind,
    resultContext.status,
    STORE_IDENTIFIERS ? resultContext.vehiclePcv : null,
    STORE_IDENTIFIERS ? resultContext.vehicleVin : null,
    STORE_IDENTIFIERS ? resultContext.vehiclePlate : null,
    resultContext.vehicleMake,
    resultContext.vehicleModel,
    resultContext.ownerCount,
    resultContext.operatorCount,
    resultContext.ownershipSubjectCount,
    resultContext.inspectionStatus,
    resultContext.inspectionValidUntil,
    resultContext.vignetteStatus,
    resultContext.sourceLabels,
    cleanText(input.error?.message || resultContext.errorMessage || "", 600) || null,
    JSON.stringify(resultContext.summary)
  ];

  await pool.query(
    `
      insert into lookup_events (
        event_type, endpoint, http_method, status_code, duration_ms, consent_state,
        analytics_session_id, actor_key, ip_hash, user_agent, referrer,
        query_type, query_value, query_hash, query_masked,
        result_kind, result_status, vehicle_pcv, vehicle_vin, vehicle_plate,
        vehicle_make, vehicle_model, owner_count, operator_count, ownership_subject_count,
        inspection_status, inspection_valid_until, vignette_status, source_labels,
        error_message, result_summary
      )
      values (
        $1, $2, $3, $4, $5, $6,
        $7, $8, $9, $10, $11,
        $12, $13, $14, $15,
        $16, $17, $18, $19, $20,
        $21, $22, $23, $24, $25,
        $26, $27, $28, $29,
        $30, $31::jsonb
      )
    `,
    params
  );

  return true;
}

async function getLookupStats(options = {}) {
  if (!isDatabaseConfigured()) {
    return {
      ok: false,
      configured: false,
      message: "DATABASE_URL neni nastaveny, lookup statistiky nejsou dostupne."
    };
  }

  const pool = getPool();
  await ensureLookupAuditSchema(pool);

  const days = clampInteger(options.days, 1, 365, 30);
  const limit = clampInteger(options.limit, 1, 200, 50);
  const rangeCondition = "occurred_at >= now() - ($1::int * interval '1 day')";

  const [
    totals,
    byDay,
    byType,
    byEndpoint,
    byResultStatus,
    topQueries,
    topVehicles,
    recentEvents
  ] = await Promise.all([
    pool.query(
      `
        select
          count(*)::int as total_events,
          count(*) filter (where status_code between 200 and 299)::int as successful_events,
          count(*) filter (where status_code >= 400)::int as failed_events,
          count(distinct analytics_session_id) filter (where analytics_session_id is not null)::int as analytics_sessions,
          round(avg(duration_ms))::int as avg_duration_ms,
          percentile_disc(0.95) within group (order by duration_ms)::int as p95_duration_ms
        from lookup_events
        where ${rangeCondition}
      `,
      [days]
    ),
    pool.query(
      `
        select
          to_char(date_trunc('day', occurred_at), 'YYYY-MM-DD') as day,
          count(*)::int as events,
          count(*) filter (where status_code between 200 and 299)::int as successful_events,
          count(*) filter (where status_code >= 400)::int as failed_events
        from lookup_events
        where ${rangeCondition}
        group by date_trunc('day', occurred_at)
        order by day
      `,
      [days]
    ),
    pool.query(
      `
        select coalesce(query_type, 'unknown') as query_type, count(*)::int as events
        from lookup_events
        where ${rangeCondition}
        group by coalesce(query_type, 'unknown')
        order by events desc, query_type
      `,
      [days]
    ),
    pool.query(
      `
        select endpoint, event_type, count(*)::int as events
        from lookup_events
        where ${rangeCondition}
        group by endpoint, event_type
        order by events desc, endpoint
      `,
      [days]
    ),
    pool.query(
      `
        select coalesce(result_status, 'unknown') as result_status, count(*)::int as events
        from lookup_events
        where ${rangeCondition}
        group by coalesce(result_status, 'unknown')
        order by events desc, result_status
      `,
      [days]
    ),
    pool.query(
      `
        select
          query_type,
          coalesce(query_value, query_masked, query_hash) as query,
          query_masked,
          count(*)::int as events,
          max(occurred_at) as last_seen_at
        from lookup_events
        where ${rangeCondition}
          and (query_value is not null or query_masked is not null or query_hash is not null)
        group by query_type, coalesce(query_value, query_masked, query_hash), query_masked
        order by events desc, last_seen_at desc
        limit $2
      `,
      [days, limit]
    ),
    pool.query(
      `
        select
          vehicle_plate,
          vehicle_vin,
          vehicle_pcv,
          vehicle_make,
          vehicle_model,
          count(*)::int as events,
          max(occurred_at) as last_seen_at
        from lookup_events
        where ${rangeCondition}
          and (vehicle_plate is not null or vehicle_vin is not null or vehicle_pcv is not null)
        group by vehicle_plate, vehicle_vin, vehicle_pcv, vehicle_make, vehicle_model
        order by events desc, last_seen_at desc
        limit $2
      `,
      [days, limit]
    ),
    pool.query(
      `
        select
          id,
          occurred_at,
          event_type,
          endpoint,
          status_code,
          duration_ms,
          consent_state,
          analytics_session_id,
          query_type,
          query_value,
          query_masked,
          result_kind,
          result_status,
          vehicle_plate,
          vehicle_vin,
          vehicle_pcv,
          vehicle_make,
          vehicle_model,
          owner_count,
          operator_count,
          ownership_subject_count,
          inspection_status,
          inspection_valid_until,
          vignette_status,
          source_labels,
          error_message,
          result_summary
        from lookup_events
        where ${rangeCondition}
        order by occurred_at desc
        limit $2
      `,
      [days, limit]
    )
  ]);

  return {
    ok: true,
    configured: true,
    range: {
      days,
      from: new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString(),
      to: new Date().toISOString()
    },
    totals: camelizeRow(totals.rows[0] || {}),
    byDay: byDay.rows.map(camelizeRow),
    byType: byType.rows.map(camelizeRow),
    byEndpoint: byEndpoint.rows.map(camelizeRow),
    byResultStatus: byResultStatus.rows.map(camelizeRow),
    topQueries: topQueries.rows.map(camelizeRow),
    topVehicles: topVehicles.rows.map(camelizeRow),
    recentEvents: recentEvents.rows.map((row) => ({
      id: row.id,
      occurredAt: row.occurred_at,
      eventType: row.event_type,
      endpoint: row.endpoint,
      statusCode: row.status_code,
      durationMs: row.duration_ms,
      consentState: row.consent_state,
      analyticsSessionId: row.analytics_session_id,
      query: {
        type: row.query_type,
        value: row.query_value || row.query_masked || null,
        masked: row.query_masked || null
      },
      result: {
        kind: row.result_kind,
        status: row.result_status,
        ownerCount: row.owner_count,
        operatorCount: row.operator_count,
        ownershipSubjectCount: row.ownership_subject_count,
        inspectionStatus: row.inspection_status,
        inspectionValidUntil: row.inspection_valid_until,
        vignetteStatus: row.vignette_status,
        sourceLabels: row.source_labels || [],
        summary: row.result_summary || null
      },
      vehicle: {
        plate: row.vehicle_plate,
        vin: row.vehicle_vin,
        pcv: row.vehicle_pcv,
        make: row.vehicle_make,
        model: row.vehicle_model
      },
      errorMessage: row.error_message
    }))
  };
}

function ensureLookupAuditSchema(pool) {
  if (!pool) {
    return Promise.resolve(false);
  }

  if (!schemaPromise) {
    schemaPromise = pool.query(LOOKUP_AUDIT_SCHEMA_SQL).then(() => true).catch((error) => {
      schemaPromise = null;
      throw error;
    });
  }

  return schemaPromise;
}

function extractClientContext(req, cookies) {
  const actorKey = cleanText(req?.headers?.["x-autoinfo-user"] || req?.headers?.["x-user-id"] || "", 120) || null;
  const userAgent = STORE_USER_AGENT ? cleanText(req?.headers?.["user-agent"] || "", 500) || null : null;
  const referrer = STORE_REFERRER ? cleanText(req?.headers?.referer || req?.headers?.referrer || "", 500) || null : null;
  const ipHash = IP_HASH_ENABLED ? hashValue(getClientIp(req)) : null;

  return {
    actorKey,
    userAgent,
    referrer,
    ipHash,
    consentState: sanitizeConsentState(cookies[CONSENT_COOKIE])
  };
}

function extractQueryContext(input, payload) {
  const payloadQuery = payload?.query || {};
  const rawValue = firstText([
    input.query,
    payloadQuery.raw,
    payloadQuery.normalized,
    input.requestUrl?.searchParams?.get("query"),
    input.requestUrl?.searchParams?.get("plate"),
    input.requestUrl?.searchParams?.get("spz"),
    input.requestUrl?.searchParams?.get("vin"),
    input.requestUrl?.searchParams?.get("pcv"),
    input.requestUrl?.searchParams?.get("ico")
  ]);
  const type = cleanText(
    input.queryType ||
      payloadQuery.type ||
      inferQueryType(rawValue, input.requestUrl),
    40
  ) || null;
  const value = normalizeLookupValue(rawValue, type);

  return {
    type,
    value,
    hash: value ? hashValue(`${type || "unknown"}:${value}`) : null,
    masked: value ? maskLookupValue(value, type) : null
  };
}

function extractResultContext(input, payload) {
  const highlights = Array.isArray(payload?.highlights) ? payload.highlights : [];
  const ownership = payload?.ownership || {};
  const parties = Array.isArray(ownership.parties) ? ownership.parties : [];
  const summary = payload?.summary || {};
  const inspections = payload?.inspections || null;
  const inspectionLookup = payload?.inspectionLookup || payload;
  const vignetteLookup = payload?.vignetteLookup || (input.eventType === "vignette_lookup" ? payload : null);
  const sourceLabels = collectSourceLabels(input, payload);
  const vehiclePlate =
    firstText([getHighlight(highlights, "SPZ"), payload?.plate, payload?.vehicle?.plate, summary.plate]) || null;
  const vehicleVin =
    firstText([getHighlight(highlights, "VIN"), payload?.vin, payload?.vehicle?.vin, summary.vin]) || null;
  const vehiclePcv =
    firstText([getHighlight(highlights, "PCV"), payload?.pcv, summary.pcv]) || null;
  const vehicleMake = firstText([
    getSectionValue(payload, "Tovarni znacka"),
    getSectionValue(payload, "Vyrobce"),
    payload?.vehicle?.make,
    summary.make
  ]);
  const vehicleModel = firstText([
    getSectionValue(payload, "Model"),
    payload?.vehicle?.model,
    summary.model
  ]);
  const inspectionStatus = cleanText(
    inspectionLookup?.status ||
      inspections?.summary?.status ||
      getHighlight(highlights, "STK") ||
      null,
    80
  ) || null;
  const inspectionValidUntil = cleanText(
    inspectionLookup?.validUntil ||
      inspections?.summary?.validUntil ||
      getHighlight(highlights, "STK do") ||
      null,
    80
  ) || null;
  const vignetteStatus = vignetteLookup
    ? cleanText(vignetteLookup.status || (vignetteLookup.valid === true ? "valid" : vignetteLookup.valid === false ? "invalid" : ""), 80) || null
    : null;
  const resultStatus = cleanText(
    payload?.status ||
      (payload?.message && input.statusCode >= 400 ? "error" : null) ||
      (input.statusCode >= 400 ? "error" : payload ? "ready" : "unknown"),
    80
  );
  const resultSummary = compactObject({
    heroTitle: cleanText(payload?.hero?.title, 240),
    heroStatus: cleanText(payload?.hero?.status, 120),
    company: compactObject({
      ico: STORE_IDENTIFIERS ? cleanText(payload?.company?.ico, 40) : null,
      name: cleanText(payload?.company?.name, 240)
    }),
    vehicleCount: normalizeInteger(summary.vehicleCount, null),
    displayedCount: normalizeInteger(summary.displayedCount, null),
    currentVehicleCount: normalizeInteger(summary.currentVehicleCount, null),
    relationshipCount: normalizeInteger(summary.relationshipCount, null),
    inspectionCount: normalizeInteger(summary.inspectionCount, null),
    missingInspectionCount: normalizeInteger(summary.missingInspectionCount, null),
    plateCount: normalizeInteger(summary.plateCount, null),
    missingPlateCount: normalizeInteger(summary.missingPlateCount, null),
    sourceLabels,
    message: cleanText(payload?.message, 400),
    error: cleanText(input.error?.message || payload?.detail || "", 400)
  });

  return {
    kind: cleanText(payload?.kind || input.eventType || "lookup", 80),
    status: resultStatus,
    vehiclePcv: normalizeLookupValue(vehiclePcv, "pcv"),
    vehicleVin: normalizeLookupValue(vehicleVin, "vin"),
    vehiclePlate: normalizeLookupValue(vehiclePlate, "plate"),
    vehicleMake: cleanText(vehicleMake, 120) || null,
    vehicleModel: cleanText(vehicleModel, 160) || null,
    ownerCount: normalizeInteger(ownership.ownerCount, null),
    operatorCount: normalizeInteger(ownership.operatorCount, null),
    ownershipSubjectCount: parties.length || null,
    inspectionStatus,
    inspectionValidUntil,
    vignetteStatus,
    sourceLabels,
    errorMessage: payload?.detail || payload?.message || null,
    summary: limitJson(resultSummary, 12000)
  };
}

function collectSourceLabels(input, payload) {
  const labels = [];
  if (payload?.source?.label) {
    labels.push(payload.source.label);
  }
  if (payload?.source?.host) {
    labels.push(payload.source.host);
  }
  if (payload?.vignetteLookup?.source?.label) {
    labels.push(payload.vignetteLookup.source.label);
  }
  if (payload?.vignetteLookup?.source?.host) {
    labels.push(payload.vignetteLookup.source.host);
  }
  if (Array.isArray(input.diagnostics?.attempts)) {
    input.diagnostics.attempts.forEach((attempt) => {
      if (attempt?.source) {
        labels.push(attempt.source);
      }
      if (attempt?.host) {
        labels.push(attempt.host);
      }
    });
  }
  return unique(labels.map((label) => cleanText(label, 120)).filter(Boolean)).slice(0, 12);
}

function getSectionValue(payload, label) {
  const sections = Array.isArray(payload?.sections) ? payload.sections : [];
  const target = normalizeLabel(label);
  for (const section of sections) {
    const items = Array.isArray(section?.items) ? section.items : [];
    for (const item of items) {
      if (normalizeLabel(item?.label) === target) {
        return item?.value || null;
      }
    }
  }
  return null;
}

function getHighlight(highlights, label) {
  const target = normalizeLabel(label);
  const match = highlights.find((item) => normalizeLabel(item?.label) === target);
  return match?.value || null;
}

function normalizeLabel(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function parseCookies(headerValue) {
  return String(headerValue || "")
    .split(";")
    .map((part) => part.trim())
    .filter(Boolean)
    .reduce((cookies, part) => {
      const separator = part.indexOf("=");
      if (separator === -1) {
        return cookies;
      }
      const key = part.slice(0, separator).trim();
      const value = part.slice(separator + 1).trim();
      if (key) {
        cookies[key] = decodeCookieValue(value);
      }
      return cookies;
    }, {});
}

function decodeCookieValue(value) {
  try {
    return decodeURIComponent(value);
  } catch (error) {
    return value;
  }
}

function sanitizeConsentState(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "analytics" || normalized === "necessary") {
    return normalized;
  }
  return "unknown";
}

function sanitizeAnalyticsSessionId(value) {
  const normalized = String(value || "").trim();
  return /^[a-f0-9-]{16,64}$/i.test(normalized) ? normalized.slice(0, 64) : null;
}

function getClientIp(req) {
  const forwardedFor = String(req?.headers?.["x-forwarded-for"] || "").split(",")[0].trim();
  return forwardedFor || req?.socket?.remoteAddress || "";
}

function inferQueryType(value, requestUrl) {
  const explicit = firstText([
    requestUrl?.searchParams?.get("type"),
    requestUrl?.searchParams?.has("ico") ? "ico" : "",
    requestUrl?.searchParams?.has("vin") ? "vin" : "",
    requestUrl?.searchParams?.has("plate") || requestUrl?.searchParams?.has("spz") ? "plate" : "",
    requestUrl?.searchParams?.has("pcv") ? "pcv" : ""
  ]);
  if (explicit) {
    return explicit;
  }

  const compact = normalizeLookupValue(value, "");
  if (/^\d{8}$/.test(compact)) {
    return "ico";
  }
  if (/^[A-HJ-NPR-Z0-9]{17}$/.test(compact)) {
    return "vin";
  }
  if (/^[A-Z0-9]{5,10}$/.test(compact)) {
    return "plate";
  }
  return null;
}

function normalizeLookupValue(value, type) {
  const normalized = cleanText(value, 400);
  if (!normalized) {
    return null;
  }

  if (type === "ico") {
    return normalized.replace(/\D/g, "").slice(0, 8) || null;
  }

  if (type === "vin" || type === "plate" || type === "pcv") {
    return normalized.toUpperCase().replace(/[^A-Z0-9]/g, "") || null;
  }

  return normalized.toUpperCase().replace(/\s+/g, " ").trim();
}

function maskLookupValue(value, type) {
  const normalized = String(value || "");
  if (!normalized) {
    return null;
  }

  if (type === "ico" && normalized.length === 8) {
    return `${normalized.slice(0, 2)}****${normalized.slice(-2)}`;
  }

  if (normalized.length <= 6) {
    return `${normalized.slice(0, 1)}***${normalized.slice(-1)}`;
  }

  return `${normalized.slice(0, 3)}***${normalized.slice(-3)}`;
}

function hashValue(value) {
  const normalized = String(value || "").trim();
  if (!normalized) {
    return null;
  }

  return crypto
    .createHash("sha256")
    .update(HASH_SALT)
    .update(":")
    .update(normalized)
    .digest("hex");
}

function firstText(values) {
  for (const value of values) {
    const normalized = cleanText(value, 400);
    if (normalized) {
      return normalized;
    }
  }
  return null;
}

function cleanText(value, maxLength = 1000) {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value)
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
}

function normalizeInteger(value, fallback) {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function clampInteger(value, min, max, fallback) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    return fallback;
  }
  return Math.min(max, Math.max(min, parsed));
}

function stringFlag(value, fallback) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  return !["false", "0", "off", "no", "disabled"].includes(String(value).trim().toLowerCase());
}

function unique(values) {
  return Array.from(new Set(values));
}

function compactObject(value) {
  return Object.fromEntries(
    Object.entries(value || {}).filter(([, entry]) => {
      if (entry === null || entry === undefined || entry === "") {
        return false;
      }
      if (Array.isArray(entry)) {
        return entry.length > 0;
      }
      if (typeof entry === "object") {
        return Object.keys(entry).length > 0;
      }
      return true;
    })
  );
}

function limitJson(value, maxChars) {
  const raw = JSON.stringify(value || {});
  if (raw.length <= maxChars) {
    return value || {};
  }
  return {
    truncated: true,
    value: raw.slice(0, maxChars)
  };
}

function camelizeRow(row) {
  return Object.fromEntries(
    Object.entries(row || {}).map(([key, value]) => [
      key.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase()),
      value
    ])
  );
}

module.exports = {
  ANALYTICS_SESSION_COOKIE,
  CONSENT_COOKIE,
  getLookupStats,
  recordLookupEvent
};
