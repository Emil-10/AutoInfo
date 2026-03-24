const fs = require("fs");
const http = require("http");
const https = require("https");
const path = require("path");
const readline = require("readline");
const { URL } = require("url");
const cheerio = require("cheerio");

const ENABLE_MOCK_DATA = String(process.env.ENABLE_MOCK_DATA || "true").toLowerCase() !== "false";
const ARES_ENABLED = String(process.env.ARES_ENABLED || "true").toLowerCase() !== "false";
const UNIQA_LOOKUP_ENABLED = String(process.env.UNIQA_LOOKUP_ENABLED || "true").toLowerCase() !== "false";
const UNIQA_PHONE = normalizeWhitespace(process.env.UNIQA_PHONE || "+420 700 700 700") || "+420 700 700 700";
const UNIQA_BROWSER_PATH = process.env.UNIQA_BROWSER_PATH || detectDefaultBrowserPath();
const UNIQA_CACHE_TTL_MS = Math.max(0, Number(process.env.UNIQA_CACHE_TTL_MS || 900000) || 900000);
const UNIQA_LOOKUP_CACHE = new Map();
const OPEN_DATA_CACHE_TTL_MS = Math.max(60000, Number(process.env.OPEN_DATA_CACHE_TTL_MS || 21600000) || 21600000);
const OPEN_DATA_TOKEN_CACHE = { value: null, expiresAt: 0 };
const OPEN_DATA_PCV_CACHE = new Map();
const OPEN_DATA_INSPECTION_CACHE = new Map();
const OPEN_DATA_PERSISTENT_PCV_INDEX = new Map();
const OPEN_DATA_PERSISTENT_INSPECTION_INDEX = new Map();
const OPEN_DATA_DOWNLOADS = new Map();
const OPEN_DATA_PERSIST_DIR = path.join(__dirname, ".cache", "open-data");
const OPEN_DATA_PCV_FILE = path.join(OPEN_DATA_PERSIST_DIR, "vin-to-pcv.json");
const OPEN_DATA_INSPECTION_FILE = path.join(OPEN_DATA_PERSIST_DIR, "inspections-by-pcv.json");
const OPEN_DATA_DATASET_FILE = path.join(OPEN_DATA_PERSIST_DIR, "datasets.json");
const OPEN_DATA_VEHICLE_ROUTE = "/vypiszregistru/vypisvozidel";
const OPEN_DATA_INSPECTION_ROUTE = "/vypiszregistru/technickeprohlidky";
let openDataPersistentLoaded = false;
let openDataPersistPromise = null;
const OPEN_DATA_DATASET_CACHE = Object.create(null);
const OPEN_DATA_JOBS = new Map();

const MOCK_VEHICLES = [
  {
    aliases: ["1AB2345", "TMBJJ7NE8L0123456"],
    data: {
      source: {
        mode: "demo",
        label: "Demo dataset",
        note: "Ukazkovy zaznam pro demonstraci aplikace."
      },
      hero: {
        badge: "Pravnicka osoba",
        title: "Skoda Octavia Combi 2.0 TDI Style",
        subtitle: "Prehled registracnich, technickych a vlastnickych udaju pro rychle interni overeni.",
        status: "Aktivni"
      },
      highlights: [
        { label: "SPZ", value: "1AB2345" },
        { label: "VIN", value: "TMBJJ7NE8L0123456" },
        { label: "Prvni registrace", value: "18.03.2019" },
        { label: "Palivo", value: "Nafta" },
        { label: "Vykon", value: "110 kW" },
        { label: "STK do", value: "11.02.2027", tone: "positive" }
      ],
      ownership: {
        ownerCount: 2,
        operatorCount: 1,
        note: "Pravnicke osoby mohou byt zobrazeny vcetne ICO a adresy.",
        parties: [
          {
            role: "Aktualni vlastnik",
            type: "company",
            name: "EX Leasing s.r.o.",
            ico: "27074358",
            address: "Budejovicka 778/3a, Praha 4",
            since: "04.09.2023"
          },
          {
            role: "Provozovatel",
            type: "company",
            name: "Fleet Operations CZ a.s.",
            ico: "27112233",
            address: "Vyskocilova 1461/2a, Praha 4",
            since: "04.09.2023"
          }
        ]
      },
      sections: [
        {
          title: "Registrace",
          items: [
            { label: "Kategorie", value: "M1 kombi" },
            { label: "Status registru", value: "V provozu" },
            { label: "Prvni registrace v CR", value: "18.03.2019" },
            { label: "ORV", value: "UC 458921" },
            { label: "TP", value: "ABC556712" },
            { label: "Urady", value: "Magistrat hl. m. Prahy" }
          ]
        },
        {
          title: "Technicke udaje",
          items: [
            { label: "Motor", value: "2.0 TDI" },
            { label: "Zdvihovy objem", value: "1 968 cm3" },
            { label: "Prevodovka", value: "DSG" },
            { label: "Barva", value: "Seda metaliza" },
            { label: "Pocet mist", value: "5" },
            { label: "Hmotnost", value: "1 485 kg" },
            { label: "CO2", value: "122 g/km" },
            { label: "Euro norma", value: "EURO 6" }
          ]
        },
        {
          title: "Kontroly a omezeni",
          items: [
            { label: "STK platna do", value: "11.02.2027", tone: "positive" },
            { label: "Emise platne do", value: "11.02.2027", tone: "positive" },
            { label: "Odcizeni", value: "Neevidovano", tone: "positive" },
            { label: "Vyrazeni z provozu", value: "Ne" },
            { label: "Zastavni pravo", value: "Bez zaznamu", tone: "positive" }
          ]
        }
      ],
      timeline: [
        {
          date: "2019-03-18",
          title: "Prvni registrace",
          description: "Vozidlo bylo poprve registrovano v Ceske republice.",
          tone: "neutral"
        },
        {
          date: "2023-09-04",
          title: "Zmena vlastnika",
          description: "Prevod na pravnickou osobu a aktualizace provozovatele.",
          tone: "accent"
        },
        {
          date: "2025-02-11",
          title: "Posledni technicka kontrola",
          description: "STK i emise bez zavady.",
          tone: "positive"
        }
      ]
    }
  },
  {
    aliases: ["5AC5678", "WBA11EV070N765432"],
    data: {
      source: {
        mode: "demo",
        label: "Demo dataset",
        note: "Ukazka zaznamu se soukromym vlastnictvim."
      },
      hero: {
        badge: "Fyzicka osoba",
        title: "BMW X5 xDrive30d",
        subtitle: "Ukazka odpovedi se stejnou strukturou, ale bez zobrazeni osobnich udaju.",
        status: "Aktivni"
      },
      highlights: [
        { label: "SPZ", value: "5AC5678" },
        { label: "VIN", value: "WBA11EV070N765432" },
        { label: "Prvni registrace", value: "06.05.2022" },
        { label: "Palivo", value: "Nafta" },
        { label: "Vykon", value: "210 kW" },
        { label: "STK do", value: "06.05.2026", tone: "positive" }
      ],
      ownership: {
        ownerCount: 1,
        operatorCount: 1,
        note: "Fyzicke osoby mohou byt zobrazeny pouze anonymizovane.",
        parties: [
          {
            role: "Aktualni vlastnik",
            type: "person",
            name: "Fyzicka osoba",
            address: "Praha",
            since: "06.05.2022"
          }
        ]
      },
      sections: [
        {
          title: "Registrace",
          items: [
            { label: "Kategorie", value: "M1 SUV" },
            { label: "Status registru", value: "V provozu" },
            { label: "Prvni registrace v CR", value: "06.05.2022" }
          ]
        },
        {
          title: "Technicke udaje",
          items: [
            { label: "Motor", value: "3.0d" },
            { label: "Prevodovka", value: "Automaticka" },
            { label: "Barva", value: "Carbon Black" },
            { label: "Pocet mist", value: "5" },
            { label: "CO2", value: "186 g/km" }
          ]
        }
      ],
      timeline: [
        {
          date: "2022-05-06",
          title: "Registrace vozidla",
          description: "Vozidlo bylo zavedeno do registru.",
          tone: "neutral"
        }
      ]
    }
  }
];

async function lookupVehicle(query, options = {}) {
  const lookup = parseLookupQuery(query);
  const liveRecord = await lookupFromConfiguredProvider(lookup);
  const publicVinRecord = liveRecord ? null : await lookupFromOfficialVinApi(lookup);
  const uniqaRecord = liveRecord || publicVinRecord ? null : await lookupFromUniqaBrowser(lookup);
  let ownershipRecord = null;
  const baseSeed = liveRecord || publicVinRecord || uniqaRecord || findMockVehicle(lookup);
  const baseRecord = mergeSupplementalRecord(baseSeed, uniqaRecord);
  const ownershipLookup = resolveOwnershipLookup(lookup, baseRecord);

  if (shouldUseHlidacOwnershipFallback(baseRecord, ownershipLookup)) {
    try {
      ownershipRecord = await lookupOwnershipFromHlidacStatu(ownershipLookup);
    } catch (error) {
      ownershipRecord = null;
    }
  }

  const record = mergeRecords(baseRecord, ownershipRecord) || ownershipRecord;

  if (!record) {
    return null;
  }

  const enriched = await enrichCompanies(record);
  const withInspectionState = await attachInspectionState(enriched, options);
  const sanitized = sanitizeClientRecord(withInspectionState);
  return {
    ...sanitized,
    query: {
      raw: query,
      normalized: lookup.compact,
      type: lookup.type,
      resolvedAt: new Date().toISOString()
    }
  };
}

function describeLookupFailure(query) {
  const lookup = parseLookupQuery(query);
  const liveConfigured = Boolean(process.env.TRANSPORT_CUBE_LOOKUP_URL);
  const hints = [];

  if (lookup.type === "vin" && !liveConfigured) {
    hints.push("Tento VIN neni v lokalnim demo datasetu. Pro realna data je potreba aktivni online napojeni.");
  }

  if (lookup.type === "vin" && !process.env.DATAOVOZIDLECH_API_KEY && !process.env.RSV_PUBLIC_API_KEY) {
    hints.push("Pro oficialni verejne VIN API chybi DATAOVOZIDLECH_API_KEY.");
  }

  if (lookup.type === "plate" && !liveConfigured) {
    hints.push("Vyhledani podle SPZ vyzaduje aktivni online napojeni.");
  }

  if (lookup.type === "unknown") {
    hints.push("Zadana hodnota nevypada jako bezna SPZ ani jako 17mistny VIN.");
  }

  if (!liveConfigured) {
    hints.push("Pro zive napojeni doplnte TRANSPORT_CUBE_LOOKUP_URL a souvisejici promenne do .env.");
  }

  if (ENABLE_MOCK_DATA) {
    hints.push("Pro demo si muzete vyzkouset 1AB2345 nebo TMBJJ7NE8L0123456.");
  }

  return {
    message: "Pro zadany identifikator jsem nic nenasel.",
    hints,
    queryType: lookup.type
  };
}

async function lookupFromConfiguredProvider(lookup) {
  const endpoint = process.env.TRANSPORT_CUBE_LOOKUP_URL;

  if (!endpoint) {
    return null;
  }

  const method = (process.env.TRANSPORT_CUBE_METHOD || "GET").toUpperCase();
  const identifierParam = process.env.TRANSPORT_CUBE_IDENTIFIER_PARAM || "identifier";
  const identifierTypeParam = process.env.TRANSPORT_CUBE_IDENTIFIER_TYPE_PARAM || "identifierType";
  const headers = { Accept: "application/json" };
  const apiKey = process.env.TRANSPORT_CUBE_API_KEY;
  const apiKeyHeader = process.env.TRANSPORT_CUBE_API_KEY_HEADER || "X-API-Key";
  const providerLabel = process.env.TRANSPORT_CUBE_PROVIDER_LABEL || "Napojene rozhrani";
  const providerNote = process.env.TRANSPORT_CUBE_PROVIDER_NOTE || "Data byla vracena z nakonfigurovaneho provideru.";
  const timeoutMs = Number(process.env.TRANSPORT_CUBE_TIMEOUT_MS || 15000);

  if (apiKey) {
    headers[apiKeyHeader] = apiKey;
  }

  let url = endpoint;
  let body = null;

  if (method === "GET") {
    const requestUrl = new URL(endpoint);
    requestUrl.searchParams.set(identifierParam, lookup.compact);
    requestUrl.searchParams.set(
      identifierTypeParam,
      lookup.type === "vin"
        ? process.env.TRANSPORT_CUBE_IDENTIFIER_TYPE_VIN || "vin"
        : process.env.TRANSPORT_CUBE_IDENTIFIER_TYPE_PLATE || "spz"
    );
    url = requestUrl.toString();
  } else {
    headers["Content-Type"] = "application/json; charset=utf-8";
    body = JSON.stringify({
      [identifierParam]: lookup.compact,
      [identifierTypeParam]:
        lookup.type === "vin"
          ? process.env.TRANSPORT_CUBE_IDENTIFIER_TYPE_VIN || "vin"
          : process.env.TRANSPORT_CUBE_IDENTIFIER_TYPE_PLATE || "spz"
    });
  }

  const response = await requestJson(url, {
    method,
    headers,
    timeoutMs,
    body
  });

  if (!response || (typeof response === "object" && Object.keys(response).length === 0)) {
    return null;
  }

  if (looksNormalized(response)) {
    return mergeWithSource(response, providerLabel, providerNote);
  }

  return normalizeGenericPayload(response, lookup, providerLabel, providerNote);
}

async function lookupFromOfficialVinApi(lookup) {
  const apiKey = process.env.DATAOVOZIDLECH_API_KEY || process.env.RSV_PUBLIC_API_KEY;

  if (lookup.type !== "vin" || !apiKey) {
    return null;
  }

  const response = await requestJson(
    `https://api.dataovozidlech.cz/api/vehicletechnicaldata/v2?vin=${encodeURIComponent(lookup.compact)}`,
    {
      method: "GET",
      headers: {
        Accept: "application/json",
        api_key: apiKey
      },
      timeoutMs: 15000
    }
  );

  if (!response) {
    return null;
  }

  return normalizeGenericPayload(
    response,
    lookup,
    "Datova kostka - verejna VIN API",
    "Technicke udaje a pocty vlastniku/provozovatelu jsou nactene z oficialni verejne VIN API. Jmena subjektu tato verejna API podle dostupne dokumentace neposkytuji."
  );
}

async function lookupFromUniqaBrowser(lookup) {
  if (!UNIQA_LOOKUP_ENABLED || !UNIQA_BROWSER_PATH || !["vin", "plate"].includes(lookup.type)) {
    return null;
  }

  const cached = getCachedUniqaRecord(lookup.compact);
  if (cached) {
    return clone(cached);
  }

  const attempts = [
    { initialDelayMs: 2500, typeDelayMs: 120, submitDelayMs: 2500, responseDelayMs: 7000 },
    { initialDelayMs: 3500, typeDelayMs: 180, submitDelayMs: 3500, responseDelayMs: 9000 }
  ];

  for (const attempt of attempts) {
    try {
      const response = await executeUniqaBrowserLookup(lookup, attempt);
      const record = normalizeUniqaPayload(response, lookup);

      if (record) {
        setCachedUniqaRecord(lookup.compact, record);
        return clone(record);
      }
    } catch (error) {
      continue;
    }
  }

  return null;
}

async function executeUniqaBrowserLookup(lookup, attempt) {
  const { chromium } = require("playwright-core");
  const browser = await chromium.launch({
    executablePath: UNIQA_BROWSER_PATH,
    headless: false,
    args: ["--start-minimized", "--window-position=-32000,-32000", "--window-size=1366,900"]
  });

  try {
    const page = await browser.newPage({
      locale: "cs-CZ",
      viewport: { width: 1366, height: 900 }
    });
    const responses = [];

    page.on("response", async (response) => {
      if (!response.url().includes("/rest/public/v1/calculator/motor/vehicle")) {
        return;
      }

      try {
        responses.push(JSON.parse(await response.text()));
      } catch (error) {
        responses.push(null);
      }
    });

    await page.goto("https://www.uniqa.cz/online/pojisteni-vozidla/", {
      waitUntil: "domcontentloaded",
      timeout: 60000
    });
    await page.waitForTimeout(attempt.initialDelayMs);

    const acceptCookiesButton = page.getByRole("button", { name: "Akceptovat vše" });
    if (await acceptCookiesButton.count()) {
      await acceptCookiesButton.first().click({ force: true }).catch(() => {});
      await page.waitForTimeout(1000);
    }

    if (lookup.type === "vin") {
      await page.locator('label[for="vehicle-identification1"]').click();
      await page.waitForTimeout(500);
      await page.locator("#vinId").type(lookup.compact, { delay: attempt.typeDelayMs });
    } else {
      await page.locator("#ecvId").type(lookup.compact, { delay: attempt.typeDelayMs });
    }

    await page.waitForTimeout(600);
    await page.locator("#phone-1").click();
    await page.keyboard.press("Control+A");
    await page.keyboard.type(UNIQA_PHONE, { delay: attempt.typeDelayMs });
    await page.waitForTimeout(attempt.submitDelayMs);
    await page.getByRole("button", { name: "Vyhledat vozidlo" }).click();
    await page.waitForTimeout(attempt.responseDelayMs);

    const successfulResponse = findSuccessfulUniqaResponse(responses);
    if (successfulResponse) {
      return successfulResponse;
    }

    return null;
  } finally {
    await browser.close().catch(() => {});
  }
}

function normalizeUniqaPayload(payload, lookup) {
  if (!payload || payload.result !== true) {
    return null;
  }

  const vehicleInfo = payload.vehicleInfo && typeof payload.vehicleInfo === "object" ? payload.vehicleInfo : null;
  const selectedVehicle =
    vehicleInfo ||
    (Array.isArray(payload.vehicleSelections) ? payload.vehicleSelections.find(Boolean) : null);

  if (!selectedVehicle) {
    return null;
  }

  return normalizeGenericPayload(
    { ...payload, vehicleInfo: selectedVehicle },
    lookup,
    "UNIQA kalkulacka",
    "Reverzni SPZ/VIN doplneni bylo nacteno z verejne kalkulacky UNIQA v realnem prohlizeci."
  );
}

async function lookupOwnershipFromHlidacStatu(lookup) {
  if (lookup.type !== "vin") {
    return null;
  }

  const html = await requestText(
    `https://www.hlidacstatu.cz/vozidla/VIN?ID=${encodeURIComponent(lookup.compact)}`,
    {
      method: "GET",
      headers: {
        Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        Pragma: "no-cache",
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
      },
      timeoutMs: 15000
    }
  );

  if (!html || !html.includes("Majitel")) {
    return null;
  }

  const $ = cheerio.load(html);
  const parties = [];

  $("table").each((_, table) => {
    const headers = $(table)
      .find("thead th")
      .map((__, cell) => normalizeWhitespace($(cell).text()))
      .get();

    const isOwnershipTable =
      headers.length >= 5 &&
      headers.some((header) => header === "Vztah") &&
      headers.some((header) => header === "Subjekt") &&
      headers.some((header) => header === "ICO" || header === "IČO");

    if (!isOwnershipTable) {
      return;
    }

    $(table)
      .find("tbody tr")
      .each((__, row) => {
        const cells = $(row).find("td");

        if (cells.length < 5) {
          return;
        }

        const role = normalizeWhitespace($(cells[0]).text());
        const name = normalizeWhitespace($(cells[1]).text());
        const ico = sanitizeIco(normalizeWhitespace($(cells[2]).text()));
        const address = normalizeWhitespace($(cells[3]).text());
        const period = normalizeWhitespace($(cells[4]).text());

        if (!name && !ico) {
          return;
        }

        parties.push({
          role: role || "Subjekt",
          type: ico ? "company" : "unknown",
          name: name || null,
          ico,
          address: address || null,
          period: period || null,
          since: extractPeriodStart(period)
        });
      });
  });

  if (parties.length === 0) {
    return null;
  }

  return {
    source: {
      mode: "live",
      label: "Hlídac státu",
      note: "Historie pravnickych vlastniku a provozovatelu je k dispozici u pravnickych osob."
    },
    hero: {
      badge: parties.some((party) => party.type === "company") ? "Pravnicka osoba" : "Bez rozliseni",
      title: `Vozidlo ${lookup.compact}`,
      subtitle: "Historie vlastniku a provozovatelu z verejneho weboveho prehledu.",
      status: "Doplnene vztahy"
    },
    highlights: [],
    ownership: {
      ownerCount: countRole(parties, "vlast"),
      operatorCount: countRole(parties, "provoz"),
      note: "U fyzickych osob se identita obvykle nezobrazuje.",
      parties
    },
    sections: [],
    timeline: []
  };
}

function mergeRecords(baseRecord, ownershipRecord) {
  if (!baseRecord && !ownershipRecord) {
    return null;
  }

  if (!baseRecord) {
    return ownershipRecord;
  }

  if (!ownershipRecord) {
    return baseRecord;
  }

  const baseParties = Array.isArray(baseRecord.ownership?.parties) ? baseRecord.ownership.parties : [];
  const ownershipParties = Array.isArray(ownershipRecord.ownership?.parties)
    ? ownershipRecord.ownership.parties
    : [];
  const parties = uniqueParties([...baseParties, ...ownershipParties]);

  return {
    ...baseRecord,
    source: {
      ...baseRecord.source,
      label: `${baseRecord.source?.label || "Zdroj"} + ${ownershipRecord.source?.label || "Hlídac státu"}`,
      note: [
        baseRecord.source?.note,
        ownershipRecord.source?.note
      ]
        .filter(Boolean)
        .join(" ")
    },
    hero: {
      ...baseRecord.hero,
      badge:
        parties.some((party) => party.type === "company")
          ? "Pravnicka osoba"
          : baseRecord.hero?.badge || "Bez rozliseni"
    },
    ownership: {
      ...baseRecord.ownership,
      ownerCount: ownershipRecord.ownership?.ownerCount || baseRecord.ownership?.ownerCount || null,
      operatorCount:
        ownershipRecord.ownership?.operatorCount || baseRecord.ownership?.operatorCount || null,
      note: [
        baseRecord.ownership?.note,
        ownershipRecord.ownership?.note
      ]
        .filter(Boolean)
        .join(" "),
      parties
    }
  };
}

function mergeSupplementalRecord(baseRecord, supplementRecord) {
  if (!baseRecord && !supplementRecord) {
    return null;
  }

  if (!baseRecord) {
    return supplementRecord;
  }

  if (!supplementRecord || baseRecord === supplementRecord) {
    return baseRecord;
  }

  const baseHighlights = Array.isArray(baseRecord.highlights) ? baseRecord.highlights : [];
  const supplementHighlights = Array.isArray(supplementRecord.highlights) ? supplementRecord.highlights : [];
  const baseSections = Array.isArray(baseRecord.sections) ? baseRecord.sections : [];
  const supplementSections = Array.isArray(supplementRecord.sections) ? supplementRecord.sections : [];
  const baseTimeline = Array.isArray(baseRecord.timeline) ? baseRecord.timeline : [];
  const supplementTimeline = Array.isArray(supplementRecord.timeline) ? supplementRecord.timeline : [];

  return {
    ...baseRecord,
    source: {
      mode: baseRecord.source?.mode || supplementRecord.source?.mode || "live",
      label: joinUniqueText([baseRecord.source?.label, supplementRecord.source?.label], " + "),
      note: joinUniqueText([baseRecord.source?.note, supplementRecord.source?.note], " ")
    },
    hero: {
      ...supplementRecord.hero,
      ...baseRecord.hero,
      badge: firstNonEmpty([baseRecord.hero?.badge, supplementRecord.hero?.badge]) || "Bez rozliseni",
      title: firstNonEmpty([baseRecord.hero?.title, supplementRecord.hero?.title]) || "Vozidlo",
      subtitle:
        firstNonEmpty([baseRecord.hero?.subtitle, supplementRecord.hero?.subtitle]) ||
        "Strukturovany vystup pripraveny pro interni overovani vozidel i dalsi napojeni.",
      status: firstNonEmpty([baseRecord.hero?.status, supplementRecord.hero?.status]) || "Neuvedeno"
    },
    highlights: mergeHighlights(baseHighlights, supplementHighlights),
    sections: mergeSections(baseSections, supplementSections),
    timeline: mergeTimeline(baseTimeline, supplementTimeline),
    ownership: {
      ...supplementRecord.ownership,
      ...baseRecord.ownership,
      note: joinUniqueText([baseRecord.ownership?.note, supplementRecord.ownership?.note], " ")
    }
  };
}

function findMockVehicle(lookup) {
  if (!ENABLE_MOCK_DATA) {
    return null;
  }

  const match = MOCK_VEHICLES.find((vehicle) =>
    vehicle.aliases.some((alias) => alias.toUpperCase().replace(/\s+/g, "") === lookup.compact)
  );

  return match ? clone(match.data) : null;
}

async function enrichCompanies(record) {
  if (!record || !record.ownership || !Array.isArray(record.ownership.parties) || !ARES_ENABLED) {
    return record;
  }

  const parties = await Promise.all(
    record.ownership.parties.map(async (party) => {
      if (party.type !== "company" || !party.ico) {
        return party;
      }

      try {
        const company = await fetchCompanyFromAres(party.ico);
        if (!company) {
          return party;
        }

        return {
          ...party,
          name: party.name || company.name,
          address: party.address || company.address
        };
      } catch (error) {
        return party;
      }
    })
  );

  return {
    ...record,
    ownership: {
      ...record.ownership,
      parties
    }
  };
}

async function fetchCompanyFromAres(ico) {
  if (!/^\d{8}$/.test(String(ico || ""))) {
    return null;
  }

  const response = await requestJson(
    `https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/${ico}`,
    {
      method: "GET",
      headers: { Accept: "application/json" },
      timeoutMs: 12000
    }
  );

  if (!response || !response.ico) {
    return null;
  }

  return {
    name: response.obchodniJmeno || null,
    address: formatAresAddress(response.sidlo)
  };
}

function normalizeGenericPayload(payload, lookup, providerLabel, providerNote) {
  const data = unwrapPayload(payload);
  const parties = collectParties(data);
  const ownerCount =
    pickFirstNumber(data, [
      "PocetVlastniku",
      "ownerCount",
      "ownersCount",
      "pocetVlastniku",
      "vehicle.ownerCount",
      "vehicle.pocetVlastniku",
      "vozidlo.pocetVlastniku"
    ]) || countRole(parties, "vlast");
  const operatorCount =
    pickFirstNumber(data, [
      "PocetProvozovatelu",
      "operatorCount",
      "operatorsCount",
      "pocetProvozovatelu",
      "vehicle.operatorCount",
      "vehicle.pocetProvozovatelu",
      "vozidlo.pocetProvozovatelu"
    ]) || countRole(parties, "provoz");

  const plate = pickFirstString(data, [
    "Rz",
    "RegZnacka",
    "plate",
    "spz",
    "rz",
    "plateNumber",
    "registrationNumber",
    "vehicleInfo.plateNumber",
    "vehicle.plate",
    "vehicle.spz",
    "vehicle.rz",
    "vozidlo.spz",
    "vozidlo.rz"
  ]);
  const vin = pickFirstString(data, [
    "VIN",
    "vin",
    "vehicleInfo.vin",
    "vehicle.vin",
    "vozidlo.vin"
  ]);
  const pcv = pickFirstString(data, [
    "PČV",
    "PCV",
    "pcv",
    "vehicle.pcv",
    "vehicle.PCV",
    "vozidlo.pcv",
    "vozidlo.PCV",
    "vehicleInfo.pcv",
    "vehicleInfo.PCV"
  ]);
  const make = pickFirstString(data, [
    "TovarniZnacka",
    "VyrobceVozidla",
    "make",
    "brand",
    "manufacturer",
    "vehicleInfo.manufacturer",
    "vehicle.make",
    "vehicle.brand",
    "vozidlo.znacka",
    "vozidlo.tovarniZnackaNazev"
  ]);
  const model = pickFirstString(data, [
    "ObchodniOznaceni",
    "model",
    "vehicleInfo.model",
    "vehicle.model",
    "vozidlo.model",
    "vozidlo.obchodniOznaceni"
  ]);
  const variant = pickFirstString(data, [
    "Varianta",
    "variant",
    "type",
    "vehicleInfo.type",
    "vehicle.variant",
    "vozidlo.varianta"
  ]);
  const status = pickFirstString(data, [
    "StatusNazev",
    "status",
    "state",
    "vehicle.status",
    "vozidlo.status",
    "vozidlo.statusNazev"
  ]);
  const category = pickFirstString(data, [
    "KategorieVozidla",
    "KategorieNazev",
    "category",
    "vehicleInfo.category.labelTranslated",
    "vehicleInfo.category.label",
    "vehicle.category",
    "vozidlo.kategorie",
    "vozidlo.kategorieNazev"
  ]);
  const firstRegistration = pickFirstString(data, [
    "DatumPrvniRegistrace",
    "firstRegistration",
    "firstRegDate",
    "vehicleInfo.firstRegDate",
    "vehicle.firstRegistration",
    "vozidlo.prvniRegistrace",
    "vozidlo.datumPrvniRegistrace"
  ]);
  const firstRegistrationCz = pickFirstString(data, [
    "DatumPrvniRegistraceVCr",
    "firstRegistrationCz",
    "vozidlo.prvniRegistraceCr",
    "vozidlo.datumPrvniRegistraceVcr"
  ]);
  const fuel = pickFirstString(data, [
    "Palivo",
    "MotorPalivo",
    "fuel",
    "vehicleInfo.fuel.labelTranslated",
    "vehicleInfo.fuel.label",
    "vehicle.fuel",
    "vozidlo.palivoNazev",
    "vozidlo.palivo"
  ]);
  const power = pickFirstString(data, [
    "MotorMaxVykon",
    "powerKw",
    "kw",
    "vehicleInfo.kw",
    "vehicle.powerKw",
    "vozidlo.vykonKw",
    "vozidlo.maxVykon"
  ]);
  const engineCapacity = pickFirstString(data, [
    "MotorZdvihObjem",
    "engineCapacityCm3",
    "ccm",
    "vehicleInfo.ccm",
    "vehicle.engineCapacityCm3",
    "vozidlo.zdvihovyObjem",
    "vozidlo.zdvihovyObjemCm3"
  ]);
  const color = pickFirstString(data, [
    "VozidloKaroserieBarva",
    "color",
    "vehicle.color",
    "vozidlo.barva",
    "vozidlo.barvaNazev"
  ]);
  const gearbox = pickFirstString(data, [
    "Prevodovka",
    "gearbox",
    "vehicle.gearbox",
    "vozidlo.prevodovkaNazev",
    "vozidlo.prevodovka"
  ]);
  const seats = pickFirstString(data, [
    "PocetMistKSezeni",
    "seats",
    "seatsNr",
    "vehicleInfo.seatsNr",
    "vehicle.seats",
    "vozidlo.pocetMist"
  ]);
  const inspectionUntil = pickFirstString(data, [
    "PravidelnaTechnickaProhlidkaDo",
    "inspectionUntil",
    "stkUntil",
    "vehicle.inspectionUntil",
    "vozidlo.stkPlatnaDo",
    "vozidlo.technickaProhlidkaDo"
  ]);
  const emissionsUntil = pickFirstString(data, [
    "PravidelnaMereniEmisiDo",
    "emissionsUntil",
    "vehicle.emissionsUntil",
    "vozidlo.emisePlatneDo"
  ]);
  const stolen = pickFirstString(data, [
    "Odcizeno",
    "stolen",
    "vehicle.stolen",
    "vozidlo.odcizeno"
  ]);
  const lien = pickFirstString(data, [
    "ZastavniPravo",
    "lien",
    "vehicle.lien",
    "vozidlo.zastavniPravo"
  ]);
  const title = [make, model, variant].filter(Boolean).join(" ").trim() || `Vozidlo ${vin || plate || lookup.compact}`;
  const badge = parties.some((party) => party.type === "company")
    ? "Pravnicka osoba"
    : parties.some((party) => party.type === "person")
      ? "Fyzicka osoba"
      : "Bez rozliseni";

  const sections = [
    createSection("Registrace", [
      item("Kategorie", category),
      item("Status registru", status),
      item("Prvni registrace", formatDate(firstRegistration)),
      item("Prvni registrace v CR", formatDate(firstRegistrationCz)),
      item("SPZ", plate),
      item("VIN", vin),
      item("PČV", pcv)
    ]),
    createSection("Technicke udaje", [
      item("Palivo", fuel),
      item("Vykon", power ? `${power} kW` : null),
      item("Zdvihovy objem", engineCapacity ? `${engineCapacity} cm3` : null),
      item("Barva", color),
      item("Prevodovka", gearbox),
      item("Pocet mist", seats)
    ]),
    createSection("Kontroly a omezeni", [
      item("STK platna do", formatDate(inspectionUntil), inspectionUntil ? "positive" : null),
      item("Emise platne do", formatDate(emissionsUntil), emissionsUntil ? "positive" : null),
      item("Odcizeni", normalizeBinaryState(stolen)),
      item("Zastavni pravo", normalizeBinaryState(lien))
    ])
  ].filter(Boolean);

  const highlights = [
    item("SPZ", plate),
    item("VIN", vin),
    item("PČV", pcv),
    item("Prvni registrace", formatDate(firstRegistration || firstRegistrationCz)),
    item("Palivo", fuel),
    item("Vykon", power ? `${power} kW` : null),
    item("STK do", formatDate(inspectionUntil), inspectionUntil ? "positive" : null)
  ].filter(Boolean);

  const timeline = buildTimeline(data, {
    firstRegistration,
    firstRegistrationCz,
    inspectionUntil
  });

  return {
    source: {
      mode: "live",
      label: providerLabel,
      note: providerNote
    },
    hero: {
      badge,
      title,
      subtitle: "Strukturovany vystup pripraveny pro interni overovani vozidel i dalsi napojeni.",
      status: status || "Neuvedeno"
    },
    highlights,
    ownership: {
      ownerCount: ownerCount || null,
      operatorCount: operatorCount || null,
      note:
        parties.length > 0
          ? "Pravnicke osoby mohou byt doplneny vcetne ICO a adresy."
          : ownerCount || operatorCount
            ? "U nekterych vozidel nemusi byt identita vlastniku a provozovatelu verejne dostupna."
          : "Detaily vlastnictvi nejsou pro tento dotaz k dispozici.",
      parties
    },
    sections,
    timeline
  };
}

async function enrichWithTechnicalInspections(record) {
  if (!record || typeof record !== "object") {
    return record;
  }

  try {
    const knownPcv = extractIdentifier(record, "PČV");
    const vin = extractIdentifier(record, "VIN");
    const pcv = knownPcv || (vin ? await resolvePcvForVin(vin) : null);

    if (!pcv) {
      return record;
    }

    const inspections = await lookupTechnicalInspectionsByPcv(pcv);
    if (!inspections) {
      return injectPcvIntoRecord(record, pcv);
    }

    return mergeInspectionData(injectPcvIntoRecord(record, pcv), inspections);
  } catch (error) {
    return record;
  }
}

async function resolvePcvForVin(vin) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase();
  if (!normalizedVin) {
    return null;
  }

  const cached = getTimedCacheValue(OPEN_DATA_PCV_CACHE, normalizedVin);
  if (cached !== undefined) {
    return cached;
  }

  let matchedPcv = null;
  const { response, request } = await createOpenDataCsvStream("/vypiszregistru/vypisvozidel");

  await new Promise((resolve, reject) => {
    let buffer = "";
    let headerIndex = null;
    let vinIndex = null;
    let pcvIndex = null;
    let finished = false;

    const finish = (value) => {
      if (finished) {
        return;
      }

      finished = true;
      matchedPcv = value || null;
      request.destroy();
      resolve();
    };

    response.setEncoding("utf8");

    response.on("data", (chunk) => {
      if (finished) {
        return;
      }

      buffer += chunk;

      if (headerIndex === null) {
        const headerEnd = buffer.indexOf("\n");
        if (headerEnd === -1) {
          return;
        }

        const headerLine = buffer.slice(0, headerEnd).replace(/\r$/, "").replace(/^\uFEFF/, "");
        const headers = parseCsvLine(headerLine).map(canonicalizeCsvHeader);
        vinIndex = headers.indexOf("VIN");
        pcvIndex = headers.indexOf("PCV");
        headerIndex = headerEnd + 1;
        buffer = buffer.slice(headerIndex);

        if (vinIndex === -1 || pcvIndex === -1) {
          finish(null);
          return;
        }
      }

      const matchIndex = buffer.indexOf(normalizedVin);
      if (matchIndex === -1) {
        if (buffer.length > 4096) {
          buffer = buffer.slice(-4096);
        }
        return;
      }

      const lineStart = buffer.lastIndexOf("\n", matchIndex);
      const lineEnd = buffer.indexOf("\n", matchIndex);

      if (lineEnd === -1) {
        return;
      }

      const line = buffer.slice(lineStart >= 0 ? lineStart + 1 : 0, lineEnd).replace(/\r$/, "");
      const values = parseCsvLine(line);
      const rowVin = normalizeWhitespace(values[vinIndex]).toUpperCase();

      if (rowVin !== normalizedVin) {
        buffer = buffer.slice(lineEnd + 1);
        return;
      }

      finish(normalizeWhitespace(values[pcvIndex]));
    });

    response.on("end", () => finish(null));
    response.on("error", reject);
    request.on("error", (error) => {
      if (finished && error.code === "ECONNRESET") {
        return;
      }
      reject(error);
    });
  });

  setTimedCacheValue(OPEN_DATA_PCV_CACHE, normalizedVin, matchedPcv || null);
  return matchedPcv || null;
}

async function lookupTechnicalInspectionsByPcv(pcv) {
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!normalizedPcv) {
    return null;
  }

  const cached = getTimedCacheValue(OPEN_DATA_INSPECTION_CACHE, normalizedPcv);
  if (cached !== undefined) {
    return cached;
  }

  const matches = [];
  const { response, request, metadata } = await createOpenDataCsvStream("/vypiszregistru/technickeprohlidky");

  await new Promise((resolve, reject) => {
    let buffer = "";
    let headerParsed = false;
    let headers = null;
    let canonicalHeaders = null;
    let finished = false;

    response.setEncoding("utf8");

    const processLine = (line) => {
      const sanitizedLine = line.replace(/\r$/, "");
      if (!headerParsed) {
        headers = parseCsvLine(sanitizedLine.replace(/^\uFEFF/, ""));
        canonicalHeaders = headers.map(canonicalizeCsvHeader);
        headerParsed = true;
        return;
      }

      if (!sanitizedLine || !sanitizedLine.startsWith(`${normalizedPcv},`)) {
        return;
      }

      const values = parseCsvLine(sanitizedLine);
      const row = Object.create(null);
      const canonicalRow = Object.create(null);

      headers.forEach((header, index) => {
        const value = values[index] === undefined ? "" : values[index];
        row[header] = value;
        canonicalRow[canonicalHeaders[index]] = value;
      });

      matches.push(normalizeInspectionRow(row, canonicalRow));
    };

    response.on("data", (chunk) => {
      if (finished) {
        return;
      }

      buffer += chunk;
      const lines = buffer.split(/\n/);
      buffer = lines.pop() || "";
      lines.forEach(processLine);
    });

    response.on("end", () => {
      if (buffer) {
        processLine(buffer);
      }
      finished = true;
      resolve();
    });
    response.on("error", reject);
    request.on("error", (error) => {
      if (finished && error.code === "ECONNRESET") {
        return;
      }
      reject(error);
    });
  });

  const records = matches
    .filter(Boolean)
    .sort((left, right) => compareDatesDesc(left.validFrom, right.validFrom));

  const summary = buildInspectionSummary(records);
  const result =
    records.length > 0
      ? {
          pcv: normalizedPcv,
          sourceFile: metadata?.filename || null,
          sourceUpdatedAt: metadata?.datasetDate || null,
          fetchedAt: new Date().toISOString(),
          summary,
          records
        }
      : null;

  setTimedCacheValue(OPEN_DATA_INSPECTION_CACHE, normalizedPcv, result);
  return result;
}

async function createOpenDataCsvStream(datasetPath) {
  const renToken = await getOpenDataRenToken();

  return new Promise((resolve, reject) => {
    const request = https.request(
      {
        protocol: "https:",
        hostname: "download.dataovozidlech.cz",
        path: datasetPath,
        method: "GET",
        headers: {
          Accept: "text/csv",
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
          _ren: renToken
        }
      },
      (response) => {
        if ((response.statusCode || 500) >= 400) {
          reject(new Error(`Open data download vratil chybu ${response.statusCode || 500}.`));
          response.resume();
          return;
        }

        resolve({
          request,
          response,
          metadata: {
            filename: parseContentDispositionFilename(response.headers["content-disposition"]),
            datasetDate: parseDatasetDateFromFilename(response.headers["content-disposition"])
          }
        });
      }
    );

    request.on("error", reject);
    request.end();
  });
}

async function scanOpenDataCsv(datasetPath, onRow) {
  const renToken = await getOpenDataRenToken();

  return new Promise((resolve, reject) => {
    let finished = false;
    const request = https.request(
      {
        protocol: "https:",
        hostname: "download.dataovozidlech.cz",
        path: datasetPath,
        method: "GET",
        headers: {
          Accept: "text/csv",
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
          _ren: renToken
        }
      },
      (response) => {
        if ((response.statusCode || 500) >= 400) {
          reject(
            new Error(`Open data download vratil chybu ${response.statusCode || 500}.`)
          );
          response.resume();
          return;
        }

        const metadata = {
          filename: parseContentDispositionFilename(response.headers["content-disposition"]),
          datasetDate: parseDatasetDateFromFilename(response.headers["content-disposition"])
        };
        const stream = readline.createInterface({
          input: response,
          crlfDelay: Infinity
        });

        let headers = null;
        let canonicalHeaders = null;

        const finish = () => {
          if (finished) {
            return;
          }

          finished = true;
          stream.close();
          request.destroy();
          resolve();
        };

        stream.on("line", (line) => {
          if (finished) {
            return;
          }

          const trimmedLine = headers ? line : line.replace(/^\uFEFF/, "");
          if (!headers) {
            headers = parseCsvLine(trimmedLine);
            canonicalHeaders = headers.map(canonicalizeCsvHeader);
            return;
          }

          if (!trimmedLine) {
            return;
          }

          const values = parseCsvLine(trimmedLine);
          const row = Object.create(null);
          const canonicalRow = Object.create(null);

          headers.forEach((header, index) => {
            const value = values[index] === undefined ? "" : values[index];
            row[header] = value;
            canonicalRow[canonicalHeaders[index]] = value;
          });

          try {
            const shouldStop = onRow({ row, canonicalRow, metadata });
            if (shouldStop) {
              finish();
            }
          } catch (error) {
            reject(error);
            finish();
          }
        });

        stream.on("close", () => {
          if (!finished) {
            finished = true;
            resolve();
          }
        });

        stream.on("error", (error) => {
          if (!finished) {
            finished = true;
            reject(error);
          }
        });
      }
    );

    request.on("error", (error) => {
      if (finished && error.code === "ECONNRESET") {
        return;
      }

      reject(error);
    });

    request.end();
  });
}

async function getOpenDataRenToken() {
  if (OPEN_DATA_TOKEN_CACHE.value && OPEN_DATA_TOKEN_CACHE.expiresAt > Date.now()) {
    return OPEN_DATA_TOKEN_CACHE.value;
  }

  const token = await new Promise((resolve, reject) => {
    const request = https.request(
      {
        protocol: "https:",
        hostname: "download.dataovozidlech.cz",
        path: "/",
        method: "GET",
        headers: {
          Accept: "text/html",
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }
      },
      (response) => {
        const headerToken = normalizeWhitespace(response.headers["_ren"]);
        response.resume();

        if (!headerToken) {
          reject(new Error("Nepodarilo se ziskat pristupovy token pro otevrena data."));
          return;
        }

        resolve(headerToken);
      }
    );

    request.on("error", reject);
    request.end();
  });

  OPEN_DATA_TOKEN_CACHE.value = token;
  OPEN_DATA_TOKEN_CACHE.expiresAt = Date.now() + OPEN_DATA_CACHE_TTL_MS;
  return token;
}

function normalizeInspectionRow(row, canonicalRow) {
  const type = normalizeWhitespace(firstNonEmpty([canonicalRow.TYP, row.Typ]));
  const state = normalizeWhitespace(firstNonEmpty([canonicalRow.STAV, row.Stav]));
  const stationCode = normalizeWhitespace(firstNonEmpty([canonicalRow.KODSTK, row["Kód STK"]]));
  const stationName = normalizeWhitespace(firstNonEmpty([canonicalRow.NAZEVSTK, row["Název STK"]]));
  const validFrom = normalizeOpenDataDate(firstNonEmpty([canonicalRow.PLATNOSTOD, row["Platnost od"]]));
  const validUntil = normalizeOpenDataDate(firstNonEmpty([canonicalRow.PLATNOSTDO, row["Platnost do"]]));
  const protocolNumber = normalizeWhitespace(
    firstNonEmpty([canonicalRow.CISLOPROTOKOLU, row["Číslo protokolu"]])
  );
  const current = normalizeBoolean(firstNonEmpty([canonicalRow.AKTUALNI, row.Aktuální]));

  return {
    type: type || null,
    state: state || null,
    stationCode: stationCode || null,
    stationName: stationName || null,
    validFrom,
    validUntil,
    protocolNumber: protocolNumber || null,
    current
  };
}

function buildInspectionSummary(records) {
  const currentRecord =
    records.find((record) => record.current) || records.slice().sort((left, right) => compareDatesDesc(left.validUntil, right.validUntil))[0] || null;
  const validUntil = currentRecord?.validUntil || null;
  const daysRemaining = validUntil ? diffDaysFromToday(validUntil) : null;
  const status = resolveInspectionStatus(validUntil);
  const uniqueTypes = Array.from(
    new Set(records.map((record) => normalizeWhitespace(record.type)).filter(Boolean))
  );

  return {
    status,
    daysRemaining,
    currentRecord,
    totalCount: records.length,
    currentCount: records.filter((record) => record.current).length,
    uniqueTypes,
    lastKnownDate: currentRecord?.validFrom || records[0]?.validFrom || null
  };
}

function mergeInspectionData(record, inspections) {
  const nextRecord = clone(record);
  const currentRecord = inspections.summary?.currentRecord || null;
  const statusTone = mapInspectionStatusToTone(inspections.summary?.status);
  const inspectionUntil = currentRecord?.validUntil ? formatDate(currentRecord.validUntil) : null;
  const inspectionFrom = currentRecord?.validFrom ? formatDate(currentRecord.validFrom) : null;

  nextRecord.inspections = inspections;

  if (inspectionUntil) {
    nextRecord.highlights = upsertHighlight(nextRecord.highlights, "STK do", inspectionUntil, statusTone);
    nextRecord.sections = upsertSectionItems(nextRecord.sections, "Kontroly a omezeni", [
      item("STK platna do", inspectionUntil, statusTone),
      item("Posledni kontrola od", inspectionFrom),
      item("Aktualni typ kontroly", currentRecord?.type),
      item("Aktualni stav", currentRecord?.state)
    ]);
  }

  if (inspections.summary?.status) {
    nextRecord.hero = {
      ...nextRecord.hero,
      status:
        nextRecord.hero?.status && nextRecord.hero.status !== "Neuvedeno"
          ? nextRecord.hero.status
          : inspections.summary.status
    };
  }

  nextRecord.timeline = mergeTimeline(
    nextRecord.timeline || [],
    buildInspectionTimeline(inspections.records)
  );

  return nextRecord;
}

function buildInspectionTimeline(records) {
  return records
    .filter((record) => record.current || record.validFrom || record.validUntil)
    .slice(0, 6)
    .map((record) => ({
      date: normalizeTimelineDate(record.validFrom || record.validUntil),
      title: record.current ? "Aktualni technicka prohlidka" : "Technicka prohlidka",
      description: [
        record.type || null,
        record.state || null,
        record.stationName || null,
        record.validUntil ? `platnost do ${formatDate(record.validUntil)}` : null
      ]
        .filter(Boolean)
        .join(" · "),
      tone: record.current ? mapInspectionStatusToTone(resolveInspectionStatus(record.validUntil)) : "neutral"
    }))
    .filter((entry) => entry.date || entry.description);
}

function injectPcvIntoRecord(record, pcv) {
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!normalizedPcv) {
    return record;
  }

  const nextRecord = clone(record);
  nextRecord.highlights = upsertHighlight(nextRecord.highlights, "PČV", normalizedPcv);
  nextRecord.sections = upsertSectionItems(nextRecord.sections, "Registrace", [item("PČV", normalizedPcv)]);
  return nextRecord;
}

async function attachInspectionState(record, options = {}) {
  if (!record || typeof record !== "object") {
    return record;
  }

  await ensureOpenDataPersistentCachesLoaded();

  const vin = normalizeWhitespace(extractIdentifier(record, "VIN")).toUpperCase() || null;
  const knownPcv = normalizeWhitespace(extractIdentifier(record, "PČV")) || null;
  const cachedPcv = knownPcv || (vin ? getPersistentPcv(vin) : null);
  let nextRecord = cachedPcv ? injectPcvIntoRecord(record, cachedPcv) : record;
  const cachedInspections = cachedPcv ? getPersistentInspections(cachedPcv) : null;

  if (cachedInspections) {
    nextRecord = mergeInspectionData(nextRecord, cachedInspections);
    nextRecord.inspectionLookup = buildInspectionLookupState("ready", vin, cachedPcv, cachedInspections);
    return nextRecord;
  }

  if (options.includeInspections) {
    const hydrated = await hydrateInspectionData({ vin, pcv: cachedPcv });
    if (hydrated) {
      nextRecord = mergeInspectionData(nextRecord, hydrated);
      nextRecord.inspectionLookup = buildInspectionLookupState("ready", vin, hydrated.pcv, hydrated);
      return nextRecord;
    }
  }

  if (vin || cachedPcv) {
    scheduleInspectionHydration({ vin, pcv: cachedPcv });
    nextRecord.inspectionLookup = buildInspectionLookupState("pending", vin, cachedPcv, null);
    return nextRecord;
  }

  nextRecord.inspectionLookup = buildInspectionLookupState("unavailable", null, null, null);
  return nextRecord;
}

async function lookupVehicleInspections(params = {}) {
  await ensureOpenDataPersistentCachesLoaded();

  const queryLookup = params.query ? parseLookupQuery(params.query) : null;
  const vin =
    normalizeWhitespace(params.vin || (queryLookup?.type === "vin" ? queryLookup.compact : "")).toUpperCase() ||
    null;
  const providedPcv = normalizeWhitespace(params.pcv) || null;
  const resolvedPcv = providedPcv || (vin ? getPersistentPcv(vin) : null);
  const cachedInspections = resolvedPcv ? getPersistentInspections(resolvedPcv) : null;

  if (cachedInspections) {
    return buildInspectionLookupState("ready", vin, resolvedPcv, cachedInspections);
  }

  if (vin || resolvedPcv) {
    scheduleInspectionHydration({ vin, pcv: resolvedPcv });
    return buildInspectionLookupState("pending", vin, resolvedPcv, null);
  }

  return buildInspectionLookupState("unavailable", vin, resolvedPcv, null);
}

function buildInspectionLookupState(status, vin, pcv, inspections) {
  return {
    status,
    vin: vin || null,
    pcv: pcv || null,
    inspections: status === "ready" ? inspections : null,
    resolvedAt: new Date().toISOString()
  };
}

function scheduleInspectionHydration({ vin, pcv }) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase() || null;
  const normalizedPcv = normalizeWhitespace(pcv) || null;
  const jobKey = normalizedPcv || normalizedVin;

  if (!jobKey || OPEN_DATA_JOBS.has(jobKey)) {
    return;
  }

  const job = hydrateInspectionData({ vin: normalizedVin, pcv: normalizedPcv })
    .catch(() => null)
    .finally(() => {
      OPEN_DATA_JOBS.delete(jobKey);
    });

  OPEN_DATA_JOBS.set(jobKey, job);
}

async function hydrateInspectionData({ vin, pcv }) {
  await ensureOpenDataPersistentCachesLoaded();

  const normalizedVin = normalizeWhitespace(vin).toUpperCase() || null;
  let resolvedPcv = normalizeWhitespace(pcv) || null;

  if (!resolvedPcv && normalizedVin) {
    resolvedPcv = getPersistentPcv(normalizedVin);
  }

  if (!resolvedPcv && normalizedVin) {
    const dataset = await ensureOpenDataDatasetLocal("vehicles", OPEN_DATA_VEHICLE_ROUTE);
    resolvedPcv = await findPcvByVinInDataset(dataset.localPath, normalizedVin);
    if (resolvedPcv) {
      await storePersistentPcv(normalizedVin, resolvedPcv);
    }
  }

  if (!resolvedPcv) {
    return null;
  }

  const cachedInspections = getPersistentInspections(resolvedPcv);
  if (cachedInspections) {
    return cachedInspections;
  }

  const dataset = await ensureOpenDataDatasetLocal("inspections", OPEN_DATA_INSPECTION_ROUTE);
  const records = await findInspectionsByPcvInDataset(dataset.localPath, resolvedPcv);

  if (!records || records.length === 0) {
    return null;
  }

  const normalizedRecords = records.sort((left, right) => compareDatesDesc(left.validFrom, right.validFrom));
  const payload = {
    pcv: resolvedPcv,
    sourceFile: dataset.filename || null,
    sourceUpdatedAt: dataset.datasetDate || null,
    fetchedAt: new Date().toISOString(),
    summary: buildInspectionSummary(normalizedRecords),
    records: normalizedRecords
  };

  await storePersistentInspections(resolvedPcv, payload);
  return payload;
}

async function ensureOpenDataPersistentCachesLoaded() {
  if (openDataPersistentLoaded) {
    return;
  }

  await fs.promises.mkdir(OPEN_DATA_PERSIST_DIR, { recursive: true });
  const [pcvRaw, inspectionRaw, datasetRaw] = await Promise.all([
    readJsonFile(OPEN_DATA_PCV_FILE),
    readJsonFile(OPEN_DATA_INSPECTION_FILE),
    readJsonFile(OPEN_DATA_DATASET_FILE)
  ]);

  Object.entries(pcvRaw || {}).forEach(([vin, pcv]) => {
    if (vin && pcv) {
      OPEN_DATA_PERSISTENT_PCV_INDEX.set(vin, pcv);
    }
  });

  Object.entries(inspectionRaw || {}).forEach(([pcv, payload]) => {
    if (pcv && payload) {
      OPEN_DATA_PERSISTENT_INSPECTION_INDEX.set(pcv, payload);
    }
  });

  Object.assign(OPEN_DATA_DATASET_CACHE, datasetRaw || {});
  openDataPersistentLoaded = true;
}

async function ensureOpenDataDatasetLocal(key, route) {
  await ensureOpenDataPersistentCachesLoaded();

  const metadata = await fetchOpenDataDatasetMetadata(route);
  const localPath = path.join(OPEN_DATA_PERSIST_DIR, metadata.filename);
  const current = OPEN_DATA_DATASET_CACHE[key];

  if (current?.filename === metadata.filename && fs.existsSync(current.localPath || localPath)) {
    OPEN_DATA_DATASET_CACHE[key] = {
      ...current,
      ...metadata,
      localPath: current.localPath || localPath
    };
    return OPEN_DATA_DATASET_CACHE[key];
  }

  if (fs.existsSync(localPath)) {
    OPEN_DATA_DATASET_CACHE[key] = {
      ...metadata,
      localPath
    };
    await persistOpenDataDatasets();
    return OPEN_DATA_DATASET_CACHE[key];
  }

  const downloadKey = `${key}:${metadata.filename}`;
  if (OPEN_DATA_DOWNLOADS.has(downloadKey)) {
    return OPEN_DATA_DOWNLOADS.get(downloadKey);
  }

  const downloadPromise = downloadOpenDataDataset(route, localPath, metadata).then(async () => {
    OPEN_DATA_DATASET_CACHE[key] = {
      ...metadata,
      localPath
    };
    await persistOpenDataDatasets();
    return OPEN_DATA_DATASET_CACHE[key];
  }).finally(() => {
    OPEN_DATA_DOWNLOADS.delete(downloadKey);
  });

  OPEN_DATA_DOWNLOADS.set(downloadKey, downloadPromise);
  return downloadPromise;
}

async function fetchOpenDataDatasetMetadata(route) {
  const renToken = await getOpenDataRenToken();

  return new Promise((resolve, reject) => {
    const request = https.request(
      {
        protocol: "https:",
        hostname: "download.dataovozidlech.cz",
        path: route,
        method: "HEAD",
        headers: {
          Accept: "text/csv",
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
          _ren: renToken
        }
      },
      (response) => {
        if ((response.statusCode || 500) >= 400) {
          reject(new Error(`Open data metadata vratila chybu ${response.statusCode || 500}.`));
          response.resume();
          return;
        }

        const header = response.headers["content-disposition"];
        const filename = parseContentDispositionFilename(header);
        response.resume();

        if (!filename) {
          reject(new Error("Nepodarilo se urcit nazev otevrene datove sady."));
          return;
        }

        resolve({
          route,
          filename,
          datasetDate: parseDatasetDateFromFilename(header)
        });
      }
    );

    request.on("error", reject);
    request.end();
  });
}

async function downloadOpenDataDataset(route, targetPath, metadata) {
  await fs.promises.mkdir(path.dirname(targetPath), { recursive: true });
  const tempPath = `${targetPath}.tmp`;
  const renToken = await getOpenDataRenToken();

  return new Promise((resolve, reject) => {
    const fileStream = fs.createWriteStream(tempPath);
    const request = https.request(
      {
        protocol: "https:",
        hostname: "download.dataovozidlech.cz",
        path: route,
        method: "GET",
        headers: {
          Accept: "text/csv",
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
          _ren: renToken
        }
      },
      (response) => {
        if ((response.statusCode || 500) >= 400) {
          fileStream.destroy();
          reject(new Error(`Open data download vratil chybu ${response.statusCode || 500}.`));
          response.resume();
          return;
        }

        response.pipe(fileStream);
      }
    );

    request.on("error", async (error) => {
      fileStream.destroy();
      await fs.promises.rm(tempPath, { force: true }).catch(() => {});
      reject(error);
    });

    fileStream.on("finish", async () => {
      fileStream.close(async () => {
        try {
          await fs.promises.rename(tempPath, targetPath);
          resolve({
            ...metadata,
            localPath: targetPath
          });
        } catch (error) {
          reject(error);
        }
      });
    });

    fileStream.on("error", async (error) => {
      await fs.promises.rm(tempPath, { force: true }).catch(() => {});
      reject(error);
    });

    request.end();
  });
}

async function findPcvByVinInDataset(filePath, vin) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase();
  if (!normalizedVin) {
    return null;
  }

  const stream = fs.createReadStream(filePath, { encoding: "utf8" });
  const reader = readline.createInterface({ input: stream, crlfDelay: Infinity });
  let headers = null;
  let vinIndex = -1;
  let pcvIndex = -1;

  for await (const rawLine of reader) {
    const line = headers ? rawLine : rawLine.replace(/^\uFEFF/, "");
    if (!headers) {
      headers = parseCsvLine(line);
      const canonicalHeaders = headers.map(canonicalizeCsvHeader);
      vinIndex = canonicalHeaders.indexOf("VIN");
      pcvIndex = canonicalHeaders.indexOf("PCV");
      continue;
    }

    if (!line.includes(normalizedVin)) {
      continue;
    }

    const values = parseCsvLine(line);
    if (normalizeWhitespace(values[vinIndex]).toUpperCase() === normalizedVin) {
      reader.close();
      stream.destroy();
      return normalizeWhitespace(values[pcvIndex]) || null;
    }
  }

  return null;
}

async function findInspectionsByPcvInDataset(filePath, pcv) {
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!normalizedPcv) {
    return [];
  }

  const stream = fs.createReadStream(filePath, { encoding: "utf8" });
  const reader = readline.createInterface({ input: stream, crlfDelay: Infinity });
  let headers = null;
  let canonicalHeaders = null;
  const results = [];

  for await (const rawLine of reader) {
    const line = headers ? rawLine : rawLine.replace(/^\uFEFF/, "");
    if (!headers) {
      headers = parseCsvLine(line);
      canonicalHeaders = headers.map(canonicalizeCsvHeader);
      continue;
    }

    if (!line.startsWith(`${normalizedPcv},`)) {
      continue;
    }

    const values = parseCsvLine(line);
    const row = Object.create(null);
    const canonicalRow = Object.create(null);
    headers.forEach((header, index) => {
      const value = values[index] === undefined ? "" : values[index];
      row[header] = value;
      canonicalRow[canonicalHeaders[index]] = value;
    });
    results.push(normalizeInspectionRow(row, canonicalRow));
  }

  return results.filter(Boolean);
}

function getPersistentPcv(vin) {
  return OPEN_DATA_PERSISTENT_PCV_INDEX.get(normalizeWhitespace(vin).toUpperCase()) || null;
}

function getPersistentInspections(pcv) {
  const value = OPEN_DATA_PERSISTENT_INSPECTION_INDEX.get(normalizeWhitespace(pcv));
  return value ? clone(value) : null;
}

async function storePersistentPcv(vin, pcv) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase();
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!normalizedVin || !normalizedPcv) {
    return;
  }

  OPEN_DATA_PERSISTENT_PCV_INDEX.set(normalizedVin, normalizedPcv);
  setTimedCacheValue(OPEN_DATA_PCV_CACHE, normalizedVin, normalizedPcv);
  await persistOpenDataCacheFiles();
}

async function storePersistentInspections(pcv, payload) {
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!normalizedPcv || !payload) {
    return;
  }

  OPEN_DATA_PERSISTENT_INSPECTION_INDEX.set(normalizedPcv, clone(payload));
  setTimedCacheValue(OPEN_DATA_INSPECTION_CACHE, normalizedPcv, payload);
  await persistOpenDataCacheFiles();
}

async function persistOpenDataCacheFiles() {
  if (openDataPersistPromise) {
    await openDataPersistPromise;
  }

  openDataPersistPromise = Promise.all([
    writeJsonFile(OPEN_DATA_PCV_FILE, Object.fromEntries(OPEN_DATA_PERSISTENT_PCV_INDEX)),
    writeJsonFile(OPEN_DATA_INSPECTION_FILE, Object.fromEntries(OPEN_DATA_PERSISTENT_INSPECTION_INDEX))
  ]).finally(() => {
    openDataPersistPromise = null;
  });

  await openDataPersistPromise;
}

async function persistOpenDataDatasets() {
  await writeJsonFile(OPEN_DATA_DATASET_FILE, OPEN_DATA_DATASET_CACHE);
}

function looksNormalized(payload) {
  return Boolean(
    payload &&
      typeof payload === "object" &&
      payload.hero &&
      Array.isArray(payload.sections)
  );
}

function mergeWithSource(payload, label, note) {
  return {
    ...payload,
    source: {
      mode: payload.source && payload.source.mode ? payload.source.mode : "live",
      label: payload.source && payload.source.label ? payload.source.label : label,
      note: payload.source && payload.source.note ? payload.source.note : note
    }
  };
}

function unwrapPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return {};
  }

  if (Array.isArray(payload)) {
    return payload[0] || {};
  }

  if (payload.data && typeof payload.data === "object") {
    return payload.data;
  }

  if (payload.result && typeof payload.result === "object") {
    return payload.result;
  }

  if (payload.Data && typeof payload.Data === "object") {
    return payload.Data;
  }

  return payload;
}

function collectParties(data) {
  const candidates = [];
  const groups = [
    { role: "Vlastnik", entries: ensureArray(getByPath(data, "owners")) },
    { role: "Provozovatel", entries: ensureArray(getByPath(data, "operators")) },
    { role: null, entries: ensureArray(getByPath(data, "parties")) },
    { role: "Vlastnik", entries: ensureArray(getByPath(data, "ownerHistory")) },
    { role: "Provozovatel", entries: ensureArray(getByPath(data, "operatorHistory")) },
    { role: "Vlastnik", entries: ensureArray(getByPath(data, "vlastnici")) },
    { role: "Provozovatel", entries: ensureArray(getByPath(data, "provozovatele")) },
    { role: "Vlastnik", entries: ensureArray(getByPath(data, "vlastnik")) },
    { role: "Provozovatel", entries: ensureArray(getByPath(data, "provozovatel")) },
    { role: "Vlastnik", entries: ensureArray(getByPath(data, "historieVlastniku")) },
    { role: "Provozovatel", entries: ensureArray(getByPath(data, "historieProvozovatelu")) },
    { role: "Vlastnik", entries: ensureArray(getByPath(data, "seznamVlastniku")) },
    { role: "Provozovatel", entries: ensureArray(getByPath(data, "seznamProvozovatelu")) },
    { role: "Vlastnik", entries: ensureArray(getByPath(data, "vehicle.owners")) },
    { role: "Provozovatel", entries: ensureArray(getByPath(data, "vehicle.operators")) },
    { role: "Vlastnik", entries: ensureArray(getByPath(data, "vehicle.owner")) },
    { role: "Provozovatel", entries: ensureArray(getByPath(data, "vehicle.operator")) },
    { role: "Vlastnik", entries: ensureArray(getByPath(data, "vehicle.ownerHistory")) },
    { role: "Provozovatel", entries: ensureArray(getByPath(data, "vehicle.operatorHistory")) },
    { role: "Vlastnik", entries: ensureArray(getByPath(data, "vozidlo.vlastnici")) },
    { role: "Provozovatel", entries: ensureArray(getByPath(data, "vozidlo.provozovatele")) },
    { role: "Vlastnik", entries: ensureArray(getByPath(data, "vozidlo.vlastnik")) },
    { role: "Provozovatel", entries: ensureArray(getByPath(data, "vozidlo.provozovatel")) },
    { role: "Vlastnik", entries: ensureArray(getByPath(data, "vozidlo.historieVlastniku")) },
    { role: "Provozovatel", entries: ensureArray(getByPath(data, "vozidlo.historieProvozovatelu")) },
    { role: null, entries: ensureArray(getByPath(data, "subjekty")) },
    { role: null, entries: ensureArray(getByPath(data, "vehicle.parties")) },
    { role: null, entries: ensureArray(getByPath(data, "vozidlo.subjekty")) }
  ];

  groups.forEach(({ entries, role }) => {
    entries.forEach((entry) => {
      const party = normalizeParty(entry, role);
      if (party) {
        candidates.push(party);
      }
    });
  });

  collectFlatParties(data).forEach((party) => candidates.push(party));

  return uniqueParties(candidates);
}

function normalizeParty(entry, fallbackRole) {
  if (!entry || typeof entry !== "object") {
    return null;
  }

  const role = firstNonEmpty([
    entry.role,
    entry.roleName,
    entry.typVztahu,
    entry.vztahNazev,
    entry.vztah,
    entry.typ,
    entry.kind,
    entry.relationship
  ]);
  const ico = sanitizeIco(
    firstNonEmpty([
      entry.ico,
      entry.ic,
      entry.companyId,
      entry.icO,
      entry.icoSubjektu,
      entry.icSubjektu,
      entry.identifikacniCislo,
      entry.icoPravnickeOsoby
    ])
  );
  const typeHint = String(
    firstNonEmpty([
      entry.type,
      entry.osobaTyp,
      entry.typOsoby,
      entry.typSubjektu,
      entry.druhOsoby,
      entry.subjektTyp,
      entry.jePravnickaOsoba ? "pravnicka" : null
    ]) || ""
  ).toLowerCase();
  const name = firstNonEmpty([
    entry.name,
    entry.nazev,
    entry.nazevSubjektu,
    entry.obchodniJmeno,
    entry.obchodniFirma,
    entry.companyName,
    entry.obchodniNazev,
    entry.firma,
    entry.subjekt,
    entry.subjektNazev
  ]);
  const address = firstNonEmpty([
    entry.address,
    entry.adresa,
    entry.sidlo,
    entry.companyAddress,
    entry.adresaText,
    entry.sidloText,
    entry.adresaSubjektu,
    entry.subjektAdresa,
    entry.mistoPodnikani
  ]);
  const since = formatDate(
    firstNonEmpty([entry.since, entry.od, entry.validFrom, entry.platnostOd, entry.datumOd, entry.from])
  );
  const period = firstNonEmpty([
    entry.period,
    entry.obdobi,
    entry.validityPeriod,
    entry.platnost,
    entry.obdobiText
  ]);

  if (!ico && !name && !address) {
    return null;
  }

  return {
    role: role || fallbackRole || inferRole(typeHint),
    type: inferPartyType(typeHint, ico, entry),
    name: name || (ico ? null : "Fyzicka osoba"),
    ico,
    address,
    period: period || null,
    since
  };
}

function collectFlatParties(data) {
  const prefixes = [
    {
      role: "Vlastnik",
      aliases: [
        "Vlastnik",
        "AktualniVlastnik",
        "VlastnikSubjekt",
        "SubjektVlastnik",
        "Owner",
        "CurrentOwner",
        "OwnerSubject"
      ]
    },
    {
      role: "Provozovatel",
      aliases: [
        "Provozovatel",
        "AktualniProvozovatel",
        "ProvozovatelSubjekt",
        "SubjektProvozovatel",
        "Operator",
        "CurrentOperator",
        "OperatorSubject"
      ]
    }
  ];

  return prefixes
    .map(({ role, aliases }) => buildFlatParty(data, role, aliases))
    .filter(Boolean);
}

function buildFlatParty(data, role, aliases) {
  const ico = sanitizeIco(
    firstNonEmpty(
      aliases.flatMap((prefix) => [
        getByPath(data, `${prefix}ICO`),
        getByPath(data, `${prefix}Ico`),
        getByPath(data, `${prefix}IC`),
        getByPath(data, `${prefix}IdentifikacniCislo`),
        getByPath(data, `${prefix}.ICO`),
        getByPath(data, `${prefix}.Ico`),
        getByPath(data, `${prefix}.ico`),
        getByPath(data, `${prefix}.identifikacniCislo`)
      ])
    )
  );
  const name = firstNonEmpty(
    aliases.flatMap((prefix) => [
      getByPath(data, `${prefix}Nazev`),
      getByPath(data, `${prefix}NazevSubjektu`),
      getByPath(data, `${prefix}Name`),
      getByPath(data, `${prefix}Firma`),
      getByPath(data, `${prefix}Subjekt`),
      getByPath(data, `${prefix}ObchodniJmeno`),
      getByPath(data, `${prefix}ObchodniFirma`),
      getByPath(data, `${prefix}.nazev`),
      getByPath(data, `${prefix}.nazevSubjektu`),
      getByPath(data, `${prefix}.name`),
      getByPath(data, `${prefix}.firma`),
      getByPath(data, `${prefix}.subjekt`),
      getByPath(data, `${prefix}.obchodniJmeno`),
      getByPath(data, `${prefix}.obchodniFirma`)
    ])
  );
  const address = firstNonEmpty(
    aliases.flatMap((prefix) => [
      getByPath(data, `${prefix}Adresa`),
      getByPath(data, `${prefix}AdresaText`),
      getByPath(data, `${prefix}Sidlo`),
      getByPath(data, `${prefix}SidloText`),
      getByPath(data, `${prefix}.adresa`),
      getByPath(data, `${prefix}.adresaText`),
      getByPath(data, `${prefix}.sidlo`),
      getByPath(data, `${prefix}.sidloText`)
    ])
  );
  const since = firstNonEmpty(
    aliases.flatMap((prefix) => [
      getByPath(data, `${prefix}Od`),
      getByPath(data, `${prefix}DatumOd`),
      getByPath(data, `${prefix}PlatnostOd`),
      getByPath(data, `${prefix}ValidFrom`),
      getByPath(data, `${prefix}.od`),
      getByPath(data, `${prefix}.datumOd`),
      getByPath(data, `${prefix}.platnostOd`),
      getByPath(data, `${prefix}.validFrom`)
    ])
  );
  const period = firstNonEmpty(
    aliases.flatMap((prefix) => [
      getByPath(data, `${prefix}Obdobi`),
      getByPath(data, `${prefix}Platnost`),
      getByPath(data, `${prefix}.obdobi`),
      getByPath(data, `${prefix}.platnost`)
    ])
  );
  const typeHint = String(
    firstNonEmpty(
      aliases.flatMap((prefix) => [
        getByPath(data, `${prefix}TypSubjektu`),
        getByPath(data, `${prefix}TypSubjektuNazev`),
        getByPath(data, `${prefix}DruhOsoby`),
        getByPath(data, `${prefix}OsobaTyp`),
        getByPath(data, `${prefix}.typSubjektu`),
        getByPath(data, `${prefix}.typSubjektuNazev`),
        getByPath(data, `${prefix}.druhOsoby`),
        getByPath(data, `${prefix}.osobaTyp`)
      ])
    ) || ""
  ).toLowerCase();

  return normalizeParty(
    {
      ico,
      nazev: name,
      adresa: address,
      od: since,
      obdobi: period,
      typOsoby: typeHint
    },
    role
  );
}

function buildTimeline(data, fallback) {
  const candidates = [
    getByPath(data, "timeline"),
    getByPath(data, "events"),
    getByPath(data, "udalosti"),
    getByPath(data, "vehicle.timeline")
  ].find(Array.isArray);

  if (candidates && candidates.length > 0) {
    return candidates
      .map((entry) => ({
        date: normalizeTimelineDate(firstNonEmpty([entry.date, entry.datum, entry.createdAt])),
        title: firstNonEmpty([entry.title, entry.nazev, entry.label]) || "Udalost",
        description: firstNonEmpty([entry.description, entry.popis, entry.detail]) || "",
        tone: firstNonEmpty([entry.tone, entry.variant]) || "neutral"
      }))
      .filter((entry) => entry.date || entry.title);
  }

  const synthesized = [];

  if (fallback.firstRegistration) {
    synthesized.push({
      date: normalizeTimelineDate(fallback.firstRegistration),
      title: "Prvni registrace",
      description: "Datum prvni registrace vozidla.",
      tone: "neutral"
    });
  }

  if (fallback.firstRegistrationCz && fallback.firstRegistrationCz !== fallback.firstRegistration) {
    synthesized.push({
      date: normalizeTimelineDate(fallback.firstRegistrationCz),
      title: "Prvni registrace v CR",
      description: "Vozidlo bylo evidovano v ceskem registru.",
      tone: "accent"
    });
  }

  if (fallback.inspectionUntil) {
    synthesized.push({
      date: normalizeTimelineDate(fallback.inspectionUntil),
      title: "Platnost technicke kontroly",
      description: "Posledni znamy termin platnosti STK.",
      tone: "positive"
    });
  }

  return synthesized;
}

function parseCsvLine(line) {
  const values = [];
  let value = "";
  let quoted = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];

    if (quoted) {
      if (char === '"') {
        if (line[index + 1] === '"') {
          value += '"';
          index += 1;
        } else {
          quoted = false;
        }
      } else {
        value += char;
      }
      continue;
    }

    if (char === '"') {
      quoted = true;
      continue;
    }

    if (char === ",") {
      values.push(value);
      value = "";
      continue;
    }

    value += char;
  }

  values.push(value);
  return values;
}

function canonicalizeCsvHeader(value) {
  return String(value || "")
    .replace(/^\uFEFF/, "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase();
}

function parseContentDispositionFilename(header) {
  const value = normalizeWhitespace(header);
  const match = value.match(/filename=([^;]+)/i);
  return match ? match[1].replace(/"/g, "") : null;
}

function parseDatasetDateFromFilename(header) {
  const filename = parseContentDispositionFilename(header);
  if (!filename) {
    return null;
  }

  const match = filename.match(/(\d{4})(\d{2})(\d{2})/);
  if (!match) {
    return null;
  }

  return `${match[1]}-${match[2]}-${match[3]}`;
}

function normalizeOpenDataDate(value) {
  const normalized = normalizeWhitespace(value);
  if (!normalized) {
    return null;
  }

  const parsed = new Date(normalized);
  return Number.isNaN(parsed.getTime()) ? normalized : parsed.toISOString();
}

function normalizeBoolean(value) {
  const normalized = normalizeWhitespace(value).toLowerCase();
  if (!normalized) {
    return null;
  }

  if (["true", "1", "ano"].includes(normalized)) {
    return true;
  }

  if (["false", "0", "ne"].includes(normalized)) {
    return false;
  }

  return null;
}

function compareDatesDesc(left, right) {
  return normalizeDateScore(right) - normalizeDateScore(left);
}

function normalizeDateScore(value) {
  if (!value) {
    return 0;
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? 0 : parsed.getTime();
}

function diffDaysFromToday(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  const today = new Date();
  const utcToday = Date.UTC(today.getFullYear(), today.getMonth(), today.getDate());
  const utcTarget = Date.UTC(parsed.getFullYear(), parsed.getMonth(), parsed.getDate());
  return Math.round((utcTarget - utcToday) / 86400000);
}

function resolveInspectionStatus(validUntil) {
  if (!validUntil) {
    return "Nezjisteno";
  }

  const daysRemaining = diffDaysFromToday(validUntil);
  if (daysRemaining === null) {
    return "Nezjisteno";
  }

  if (daysRemaining < 0) {
    return "Propadla";
  }

  if (daysRemaining <= 30) {
    return "Konci brzy";
  }

  return "Platna";
}

function mapInspectionStatusToTone(status) {
  const normalized = normalizeWhitespace(status).toLowerCase();
  if (normalized.includes("platna")) {
    return "positive";
  }

  if (normalized.includes("konci")) {
    return "warning";
  }

  if (normalized.includes("propadla")) {
    return "danger";
  }

  return "neutral";
}

function upsertHighlight(highlights, label, value, tone) {
  const nextHighlights = Array.isArray(highlights) ? [...highlights] : [];
  const index = nextHighlights.findIndex(
    (item) => normalizeWhitespace(item?.label).toUpperCase() === normalizeWhitespace(label).toUpperCase()
  );
  const nextItem = item(label, value, tone);

  if (!nextItem) {
    return nextHighlights;
  }

  if (index >= 0) {
    nextHighlights[index] = nextItem;
  } else {
    nextHighlights.push(nextItem);
  }

  return nextHighlights;
}

function upsertSectionItems(sections, title, items) {
  const nextSections = Array.isArray(sections) ? clone(sections) : [];
  const filteredItems = items.filter(Boolean);
  if (filteredItems.length === 0) {
    return nextSections;
  }

  const existing = nextSections.find(
    (section) => normalizeWhitespace(section?.title) === normalizeWhitespace(title)
  );

  if (!existing) {
    nextSections.push({
      title,
      items: filteredItems
    });
    return nextSections;
  }

  filteredItems.forEach((nextItem) => {
    const index = existing.items.findIndex(
      (currentItem) =>
        normalizeWhitespace(currentItem?.label) === normalizeWhitespace(nextItem.label)
    );

    if (index >= 0) {
      existing.items[index] = nextItem;
    } else {
      existing.items.push(nextItem);
    }
  });

  return nextSections;
}

function getTimedCacheValue(cache, key) {
  const cached = cache.get(key);
  if (!cached) {
    return undefined;
  }

  if (cached.expiresAt <= Date.now()) {
    cache.delete(key);
    return undefined;
  }

  return clone(cached.value);
}

function setTimedCacheValue(cache, key, value) {
  cache.set(key, {
    value: clone(value),
    expiresAt: Date.now() + OPEN_DATA_CACHE_TTL_MS
  });
}

async function readJsonFile(filePath) {
  try {
    const content = await fs.promises.readFile(filePath, "utf8");
    return JSON.parse(content);
  } catch (error) {
    if (error.code === "ENOENT") {
      return null;
    }

    throw error;
  }
}

async function writeJsonFile(filePath, value) {
  await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
  await fs.promises.writeFile(filePath, JSON.stringify(value, null, 2), "utf8");
}

function parseLookupQuery(query) {
  const compact = String(query || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

  const vinPattern = /^[A-HJ-NPR-Z0-9]{17}$/;
  const platePattern = /^[A-Z0-9]{5,10}$/;

  return {
    compact,
    type: vinPattern.test(compact) ? "vin" : platePattern.test(compact) ? "plate" : "unknown"
  };
}

function item(label, value, tone) {
  if (!value) {
    return null;
  }

  return {
    label,
    value,
    tone: tone || null
  };
}

function createSection(title, items) {
  const filteredItems = items.filter(Boolean);
  if (filteredItems.length === 0) {
    return null;
  }

  return {
    title,
    items: filteredItems
  };
}

function countRole(parties, fragment) {
  return parties.filter((party) =>
    String(party.role || "").toLowerCase().includes(fragment)
  ).length || null;
}

function ensureArray(value) {
  if (Array.isArray(value)) {
    return value;
  }

  if (value && typeof value === "object") {
    return [value];
  }

  return [];
}

function uniqueParties(parties) {
  const seen = new Set();
  return parties.filter((party) => {
    const key = [party.role, party.type, party.ico, party.name, party.address].join("|");
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function inferRole(typeHint) {
  if (typeHint.includes("provoz")) {
    return "Provozovatel";
  }

  if (typeHint.includes("vlast")) {
    return "Vlastnik";
  }

  return "Subjekt";
}

function inferPartyType(typeHint, ico, entry) {
  if (ico || typeHint.includes("pravnick")) {
    return "company";
  }

  if (
    typeHint.includes("fyzick") ||
    firstNonEmpty([entry.jmeno, entry.prijmeni, entry.firstName, entry.lastName])
  ) {
    return "person";
  }

  return "unknown";
}

function normalizeBinaryState(value) {
  const normalized = String(value || "").toLowerCase().trim();
  if (!normalized) {
    return null;
  }

  if (["ne", "no", "false", "0", "neevidovano", "bez zaznamu"].includes(normalized)) {
    return "Bez zaznamu";
  }

  if (["ano", "yes", "true", "1"].includes(normalized)) {
    return "Ano";
  }

  return value;
}

function pickFirstString(source, paths) {
  const value = firstNonEmpty(paths.map((path) => getByPath(source, path)));
  if (value === null || value === undefined) {
    return null;
  }

  return String(value).trim() || null;
}

function pickFirstNumber(source, paths) {
  const value = firstNonEmpty(paths.map((path) => getByPath(source, path)));
  if (value === null || value === undefined || value === "") {
    return null;
  }

  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function getByPath(source, path) {
  if (!source || typeof source !== "object") {
    return null;
  }

  return String(path)
    .split(".")
    .reduce((current, key) => {
      if (current && Object.prototype.hasOwnProperty.call(current, key)) {
        return current[key];
      }

      return null;
    }, source);
}

function requestJson(targetUrl, options) {
  return requestStructured(targetUrl, options, "json");
}

function requestText(targetUrl, options) {
  return requestStructured(targetUrl, options, "text");
}

function requestStructured(targetUrl, options, responseType) {
  return new Promise((resolve, reject) => {
    const requestUrl = new URL(targetUrl);
    const transport = requestUrl.protocol === "http:" ? http : https;
    const requestOptions = {
      hostname: requestUrl.hostname,
      path: `${requestUrl.pathname}${requestUrl.search}`,
      port: requestUrl.port || (requestUrl.protocol === "http:" ? 80 : 443),
      method: options.method || "GET",
      headers: options.headers || {}
    };

    const request = transport.request(requestOptions, (response) => {
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () => {
        const payload = Buffer.concat(chunks).toString("utf8");

        if (response.statusCode < 200 || response.statusCode >= 300) {
          reject(
            new Error(
              `Rozhrani vratilo ${response.statusCode}: ${payload.slice(0, 300) || "bez detailu"}`
            )
          );
          return;
        }

        if (!payload) {
          resolve(null);
          return;
        }

        if (responseType === "text") {
          resolve(payload);
          return;
        }

        try {
          resolve(JSON.parse(payload));
        } catch (error) {
          reject(new Error("Rozhrani nevratilo validni JSON."));
        }
      });
    });

    request.setTimeout(options.timeoutMs || 15000, () => {
      request.destroy(new Error("Vyprsel casovy limit rozhrani."));
    });

    request.on("error", (error) => reject(error));

    if (options.body) {
      request.write(options.body);
    }

    request.end();
  });
}

function normalizeTimelineDate(value) {
  if (!value) {
    return null;
  }

  const timestampCandidate =
    typeof value === "number" || /^\d{10,13}$/.test(String(value).trim()) ? Number(value) : null;
  const parsed = timestampCandidate ? new Date(timestampCandidate) : new Date(value);
  return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toISOString();
}

function formatDate(value) {
  if (!value) {
    return null;
  }

  const timestampCandidate =
    typeof value === "number" || /^\d{10,13}$/.test(String(value).trim()) ? Number(value) : null;
  const parsed = timestampCandidate ? new Date(timestampCandidate) : new Date(value);
  if (!Number.isNaN(parsed.getTime())) {
    return new Intl.DateTimeFormat("cs-CZ").format(parsed);
  }

  return String(value);
}

function formatAresAddress(address) {
  if (!address || typeof address !== "object") {
    return null;
  }

  return [
    firstNonEmpty([address.nazevUlice, address.nazevCastiObce]),
    [address.cisloDomovni, address.cisloOrientacni].filter(Boolean).join("/"),
    firstNonEmpty([address.nazevObce, address.nazevMestskeCasti]),
    address.psc
  ]
    .filter(Boolean)
    .join(", ");
}

function sanitizeIco(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits.length === 8 ? digits : null;
}

function firstNonEmpty(values) {
  for (const value of values) {
    if (value === 0) {
      return value;
    }

    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }

    if (value !== null && value !== undefined && value !== "") {
      return value;
    }
  }

  return null;
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function normalizeWhitespace(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

function extractPeriodStart(period) {
  const value = normalizeWhitespace(period);

  if (!value) {
    return null;
  }

  const [start] = value.split("-");
  return normalizeWhitespace(start) || null;
}

function sanitizeClientRecord(record) {
  if (!record || typeof record !== "object") {
    return record;
  }

  const nextRecord = clone(record);
  delete nextRecord.source;
  return nextRecord;
}

function shouldUseHlidacOwnershipFallback(record, ownershipLookup) {
  if (!ownershipLookup || ownershipLookup.type !== "vin") {
    return false;
  }

  const parties = Array.isArray(record?.ownership?.parties) ? record.ownership.parties : [];
  if (parties.length === 0) {
    return true;
  }

  const resolvedCompanyParties = parties.filter(
    (party) => party.type === "company" && (party.ico || party.name)
  );

  return resolvedCompanyParties.length === 0;
}

function resolveOwnershipLookup(originalLookup, record) {
  if (originalLookup.type === "vin") {
    return originalLookup;
  }

  const resolvedVin = extractIdentifier(record, "VIN");
  if (!resolvedVin) {
    return null;
  }

  const vinLookup = parseLookupQuery(resolvedVin);
  return vinLookup.type === "vin" ? vinLookup : null;
}

function extractIdentifier(record, label) {
  if (!record || !Array.isArray(record.highlights)) {
    return null;
  }

  const match = record.highlights.find((item) => normalizeWhitespace(item.label).toUpperCase() === label);
  return match ? normalizeWhitespace(match.value) : null;
}

function mergeHighlights(primary, supplemental) {
  const seen = new Set();
  return [...primary, ...supplemental].filter((item) => {
    if (!item || !item.label || !item.value) {
      return false;
    }

    const key = normalizeWhitespace(item.label).toUpperCase();
    if (seen.has(key)) {
      return false;
    }

    seen.add(key);
    return true;
  });
}

function mergeSections(primary, supplemental) {
  const merged = new Map();

  [...supplemental, ...primary].forEach((section) => {
    if (!section || !section.title) {
      return;
    }

    const key = normalizeWhitespace(section.title).toUpperCase();
    const existing = merged.get(key);

    if (!existing) {
      merged.set(key, clone(section));
      return;
    }

    const items = Array.isArray(existing.items) ? existing.items : [];
    const nextItems = Array.isArray(section.items) ? section.items : [];
    const seenItems = new Set(items.map((item) => normalizeWhitespace(item.label).toUpperCase()));

    nextItems.forEach((item) => {
      const itemKey = normalizeWhitespace(item.label).toUpperCase();
      if (!itemKey || seenItems.has(itemKey)) {
        return;
      }

      seenItems.add(itemKey);
      items.push(item);
    });

    existing.items = items;
  });

  return Array.from(merged.values());
}

function mergeTimeline(primary, supplemental) {
  const seen = new Set();
  return [...primary, ...supplemental].filter((entry) => {
    if (!entry || !entry.title) {
      return false;
    }

    const key = [
      normalizeWhitespace(entry.date),
      normalizeWhitespace(entry.title),
      normalizeWhitespace(entry.description)
    ].join("|");
    if (seen.has(key)) {
      return false;
    }

    seen.add(key);
    return true;
  });
}

function joinUniqueText(values, separator) {
  return Array.from(new Set(values.map((value) => normalizeWhitespace(value)).filter(Boolean))).join(separator);
}

function detectDefaultBrowserPath() {
  const candidates = [
    "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
    "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
  ];

  return candidates.find((candidate) => fs.existsSync(candidate)) || null;
}

function getCachedUniqaRecord(key) {
  const cached = UNIQA_LOOKUP_CACHE.get(key);
  if (!cached) {
    return null;
  }

  if (cached.expiresAt <= Date.now()) {
    UNIQA_LOOKUP_CACHE.delete(key);
    return null;
  }

  return cached.record;
}

function setCachedUniqaRecord(key, record) {
  UNIQA_LOOKUP_CACHE.set(key, {
    record: clone(record),
    expiresAt: Date.now() + UNIQA_CACHE_TTL_MS
  });
}

function findSuccessfulUniqaResponse(responses) {
  for (let index = responses.length - 1; index >= 0; index -= 1) {
    const response = responses[index];
    if (
      response &&
      response.result === true &&
      (response.vehicleFound || response.vehicleInfo || (Array.isArray(response.vehicleSelections) && response.vehicleSelections.length > 0))
    ) {
      return response;
    }
  }

  return null;
}

module.exports = {
  lookupVehicle,
  lookupVehicleInspections,
  describeLookupFailure
};
