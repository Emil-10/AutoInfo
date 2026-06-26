const fs = require("fs");
const http = require("http");
const https = require("https");
const net = require("net");
const path = require("path");
const readline = require("readline");
const zlib = require("zlib");
const { URL } = require("url");
const cheerio = require("cheerio");
const openDataDb = require("./open-data-db");
const { createTimedLruCache } = require("./lib/timed-cache");

const ENABLE_MOCK_DATA = String(process.env.ENABLE_MOCK_DATA || "true").toLowerCase() !== "false";
const LISTING_URL_LOOKUP_ENABLED = String(process.env.LISTING_URL_LOOKUP_ENABLED || "true").toLowerCase() !== "false";
const LISTING_URL_LOOKUP_TIMEOUT_MS = Math.max(2000, Number(process.env.LISTING_URL_LOOKUP_TIMEOUT_MS || 8000) || 8000);
const OCR_PLATE_LOOKUP_ENABLED = String(process.env.OCR_PLATE_LOOKUP_ENABLED || "false").toLowerCase() === "true";
const OCR_PLATE_MAX_IMAGE_BYTES = Math.max(256000, Number(process.env.OCR_PLATE_MAX_IMAGE_BYTES || 5500000) || 5500000);
const ALPR_PLATE_LOOKUP_ENABLED = String(process.env.ALPR_PLATE_LOOKUP_ENABLED || "false").toLowerCase() === "true";
const ALPR_PROVIDER = normalizeWhitespace(process.env.ALPR_PROVIDER || "auto").toLowerCase();
const PLATE_RECOGNIZER_API_URL = normalizeWhitespace(
  process.env.PLATE_RECOGNIZER_API_URL || "https://api.platerecognizer.com/v1/plate-reader/"
);
const PLATE_RECOGNIZER_API_TOKEN = normalizeWhitespace(process.env.PLATE_RECOGNIZER_API_TOKEN || "");
const PLATE_RECOGNIZER_REGIONS = normalizePlateRecognizerRegions(process.env.PLATE_RECOGNIZER_REGIONS || "cz");
const PLATE_RECOGNIZER_CONFIG = normalizeWhitespace(process.env.PLATE_RECOGNIZER_CONFIG || "");
const PLATE_RECOGNIZER_TIMEOUT_MS = Math.max(
  1000,
  Number(process.env.PLATE_RECOGNIZER_TIMEOUT_MS || 10000) || 10000
);
const LOCAL_ALPR_API_URL = normalizeWhitespace(
  process.env.LOCAL_ALPR_API_URL || (ALPR_PROVIDER === "local" ? "http://127.0.0.1:8080/v1/plate-reader/" : "")
);
const LOCAL_ALPR_TIMEOUT_MS = Math.max(1000, Number(process.env.LOCAL_ALPR_TIMEOUT_MS || 10000) || 10000);
const ARES_ENABLED = String(process.env.ARES_ENABLED || "true").toLowerCase() !== "false";
const PVZP_LOOKUP_ENABLED = String(process.env.PVZP_LOOKUP_ENABLED || "false").toLowerCase() === "true";
const PVZP_HEADLESS = String(process.env.PVZP_HEADLESS || (process.platform === "linux" ? "true" : "false")).toLowerCase() === "true";
const PVZP_BROWSER_INFO = resolvePvzpBrowserInfo();
const PVZP_BROWSER_PATH = PVZP_BROWSER_INFO.path;
const PVZP_USER_AGENT =
  process.env.PVZP_USER_AGENT ||
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36";
const PVZP_CACHE_TTL_MS = Math.max(0, Number(process.env.PVZP_CACHE_TTL_MS || 900000) || 900000);
const VIN_PLATE_PVZP_LOOKUP_ENABLED =
  String(process.env.VIN_PLATE_PVZP_LOOKUP_ENABLED || "false").toLowerCase() === "true";
const VIN_PLATE_PVZP_LOOKUP_TIMEOUT_MS = Math.max(
  3000,
  Number(process.env.VIN_PLATE_PVZP_LOOKUP_TIMEOUT_MS || 25000) || 25000
);
const UNIQA_LOOKUP_ENABLED = String(process.env.UNIQA_LOOKUP_ENABLED || "false").toLowerCase() === "true";
const UNIQA_PHONE = normalizeUniqaPhone(process.env.UNIQA_PHONE || "");
const UNIQA_BROWSER_INFO = resolveUniqaBrowserInfo();
const UNIQA_BROWSER_PATH = UNIQA_BROWSER_INFO.path;
const UNIQA_USER_AGENT =
  process.env.UNIQA_USER_AGENT ||
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36";
const UNIQA_CACHE_TTL_MS = Math.max(0, Number(process.env.UNIQA_CACHE_TTL_MS || 900000) || 900000);
const BROWSERLESS_WS_URL = normalizeWhitespace(process.env.BROWSERLESS_WS_URL || process.env.BROWSERLESS_URL || "");
const BROWSERLESS_ENABLED = Boolean(BROWSERLESS_WS_URL);
const THIRD_PARTY_OWNERSHIP_FALLBACK_ENABLED =
  String(process.env.THIRD_PARTY_OWNERSHIP_FALLBACK_ENABLED || "false").toLowerCase() === "true";
const FAST_LOOKUP_MODE = String(process.env.FAST_LOOKUP_MODE || "true").toLowerCase() !== "false";
const ALLOW_RUNTIME_OPEN_DATA_INSPECTION_SCAN =
  String(process.env.ALLOW_RUNTIME_OPEN_DATA_INSPECTION_SCAN || "false").toLowerCase() === "true";
const ALLOW_RUNTIME_OPEN_DATA_OWNERSHIP_SCAN =
  String(process.env.ALLOW_RUNTIME_OPEN_DATA_OWNERSHIP_SCAN || "false").toLowerCase() === "true";
const ALLOW_RUNTIME_OPEN_DATA_ICO_SCAN =
  String(process.env.ALLOW_RUNTIME_OPEN_DATA_ICO_SCAN || "false").toLowerCase() === "true";
const ICO_FLEET_MAX_RECORDS = Math.max(1, Math.min(1000, Number(process.env.ICO_FLEET_MAX_RECORDS || 200) || 200));
const ICO_FLEET_PLATE_BACKFILL_LIMIT = Math.max(
  0,
  Math.min(ICO_FLEET_MAX_RECORDS, Number(process.env.ICO_FLEET_PLATE_BACKFILL_LIMIT || ICO_FLEET_MAX_RECORDS) || 0)
);
const OPEN_DATA_CACHE_TTL_MS = Math.max(60000, Number(process.env.OPEN_DATA_CACHE_TTL_MS || 21600000) || 21600000);
const RUNTIME_CACHE_MAX_ENTRIES = Math.max(50, Number(process.env.RUNTIME_CACHE_MAX_ENTRIES || 1000) || 1000);
const OPENDATALAB_STK_LOOKUP_ENABLED =
  String(process.env.OPENDATALAB_STK_LOOKUP_ENABLED || "true").toLowerCase() !== "false";
const OPENDATALAB_STK_API_URL = normalizeWhitespace(
  process.env.OPENDATALAB_STK_API_URL || "https://stk.opendatalab.cz/api/inspections"
);
const OPENDATALAB_STK_TIMEOUT_MS = Math.max(1000, Number(process.env.OPENDATALAB_STK_TIMEOUT_MS || 10000) || 10000);
const OPENDATALAB_STK_CACHE_TTL_MS = Math.max(0, Number(process.env.OPENDATALAB_STK_CACHE_TTL_MS || 21600000) || 21600000);
const STK_DEFECTS_API_URL = normalizeWhitespace(
  process.env.STK_DEFECTS_API_URL || "https://stk.opendatalab.cz/api/defects"
);
const STK_DEFECTS_CACHE_TTL_MS = Math.max(0, Number(process.env.STK_DEFECTS_CACHE_TTL_MS || 21600000) || 21600000);
const ISTP_STK_LOOKUP_ENABLED =
  String(process.env.ISTP_STK_LOOKUP_ENABLED || "true").toLowerCase() !== "false";
const ISTP_STK_SPARQL_URL = normalizeWhitespace(process.env.ISTP_STK_SPARQL_URL || "https://data.gov.cz/sparql");
const ISTP_STK_TIMEOUT_MS = Math.max(1000, Number(process.env.ISTP_STK_TIMEOUT_MS || 20000) || 20000);
const ISTP_STK_CACHE_TTL_MS = Math.max(0, Number(process.env.ISTP_STK_CACHE_TTL_MS || 21600000) || 21600000);
const ISTP_STK_MAX_CANDIDATE_DATES = Math.max(
  1,
  Math.min(12, Number(process.env.ISTP_STK_MAX_CANDIDATE_DATES || 8) || 8)
);
const FLEET_DB_FALLBACK_MODE = String(process.env.FLEET_DB_FALLBACK_ENABLED || "auto").toLowerCase();
const FLEET_DB_ALLOW_WHOLE_SHARD_CACHE =
  String(process.env.FLEET_DB_ALLOW_WHOLE_SHARD_CACHE || "false").toLowerCase() === "true";
const VIGNETTE_LOOKUP_URL = normalizeWhitespace(process.env.VIGNETTE_LOOKUP_URL || "");
const VIGNETTE_LOOKUP_METHOD = String(process.env.VIGNETTE_LOOKUP_METHOD || "GET").toUpperCase() === "POST" ? "POST" : "GET";
const VIGNETTE_LOOKUP_PLATE_PARAM = normalizeWhitespace(process.env.VIGNETTE_LOOKUP_PLATE_PARAM || "plate") || "plate";
const VIGNETTE_LOOKUP_COUNTRY_PARAM = normalizeWhitespace(process.env.VIGNETTE_LOOKUP_COUNTRY_PARAM || "country") || "country";
const VIGNETTE_LOOKUP_COUNTRY = normalizeWhitespace(process.env.VIGNETTE_LOOKUP_COUNTRY || "CZ").toUpperCase() || "CZ";
const VIGNETTE_LOOKUP_TIMEOUT_MS = Math.max(1000, Number(process.env.VIGNETTE_LOOKUP_TIMEOUT_MS || 10000) || 10000);
const VIGNETTE_CACHE_TTL_MS = Math.max(0, Number(process.env.VIGNETTE_CACHE_TTL_MS || 900000) || 900000);
const EDALNICE_LOOKUP_ENABLED = String(process.env.EDALNICE_LOOKUP_ENABLED || "true").toLowerCase() !== "false";
const EDALNICE_CONFIG_URL = normalizeWhitespace(process.env.EDALNICE_CONFIG_URL || "https://edalnice.cz/");
const EDALNICE_CONFIG_CACHE_TTL_MS = Math.max(60000, Number(process.env.EDALNICE_CONFIG_CACHE_TTL_MS || 21600000) || 21600000);
const EDALNICE_TOKEN_REFRESH_OFFSET_MS = 30000;
const PVZP_LOOKUP_CACHE = createTimedLruCache({
  ttlMs: PVZP_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const UNIQA_LOOKUP_CACHE = createTimedLruCache({
  ttlMs: UNIQA_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const PLATE_RESOLUTION_TTL_MS = Math.max(
  60000,
  Number(process.env.PLATE_RESOLUTION_TTL_MS || 2592000000) || 2592000000
);
const PLATE_RESOLUTION_BROWSER_FALLBACK_ENABLED =
  String(process.env.PLATE_RESOLUTION_BROWSER_FALLBACK_ENABLED || "false").toLowerCase() === "true";
const OPEN_DATA_TOKEN_CACHE = { value: null, expiresAt: 0 };
const STK_DEFECTS_CACHE = { value: null, expiresAt: 0, promise: null };
const CZECH_PLATE_REGION_LETTERS = "ABCEHJKLMPSUTZ";
let OCR_WORKER_PROMISE = null;
let OCR_QUEUE = Promise.resolve();
const OPEN_DATA_PCV_CACHE = createTimedLruCache({
  ttlMs: OPEN_DATA_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const OPEN_DATA_INSPECTION_CACHE = createTimedLruCache({
  ttlMs: OPEN_DATA_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const OPENDATALAB_STK_CACHE = createTimedLruCache({
  ttlMs: OPENDATALAB_STK_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const ISTP_STK_URL_CACHE = createTimedLruCache({
  ttlMs: ISTP_STK_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const ISTP_STK_DAILY_VIN_CACHE = createTimedLruCache({
  ttlMs: ISTP_STK_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const OPEN_DATA_OWNERSHIP_CACHE = createTimedLruCache({
  ttlMs: OPEN_DATA_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const OPEN_DATA_VEHICLE_SUMMARY_CACHE = createTimedLruCache({
  ttlMs: OPEN_DATA_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const OPEN_DATA_PERSISTENT_PCV_INDEX = new Map();
const OPEN_DATA_PERSISTENT_INSPECTION_INDEX = new Map();
const OPEN_DATA_PERSISTENT_OWNER_INDEX = new Map();
const OPEN_DATA_DOWNLOADS = new Map();
const OPEN_DATA_PERSIST_DIR = path.resolve(process.env.OPEN_DATA_PERSIST_DIR || path.join(__dirname, ".cache", "open-data"));
const OPEN_DATA_PCV_FILE = path.join(OPEN_DATA_PERSIST_DIR, "vin-to-pcv.json");
const OPEN_DATA_INSPECTION_FILE = path.join(OPEN_DATA_PERSIST_DIR, "inspections-by-pcv.json");
const OPEN_DATA_OWNERSHIP_FILE = path.join(OPEN_DATA_PERSIST_DIR, "ownership-by-pcv.json");
const OPEN_DATA_DATASET_FILE = path.join(OPEN_DATA_PERSIST_DIR, "datasets.json");
const ISTP_STK_CACHE_DIR = path.join(OPEN_DATA_PERSIST_DIR, "istp-stk");
const OPEN_DATA_VEHICLE_ROUTE = "/vypiszregistru/vypisvozidel";
const OPEN_DATA_INSPECTION_ROUTE = "/vypiszregistru/technickeprohlidky";
const OPEN_DATA_OWNER_ROUTE = "/vypiszregistru/vlastnikprovozovatelvozidla";
const OPEN_DATA_IMPORT_ROUTE = "/vypiszregistru/vozidladovoz";
const OPEN_DATA_DEREG_ROUTE = "/vypiszregistru/vozidlavyrazenazprovozu";
let openDataPersistentLoaded = false;
let openDataPersistPromise = null;
const OPEN_DATA_DATASET_CACHE = Object.create(null);
const OPEN_DATA_JOBS = new Map();
const OPEN_DATA_OWNER_JOBS = new Map();
const TAXI_LOOKUP_CACHE = createTimedLruCache({
  ttlMs: OPEN_DATA_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const POLICE_WANTED_CACHE = createTimedLruCache({
  ttlMs: OPEN_DATA_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const OPEN_DATA_IMPORT_CACHE = createTimedLruCache({
  ttlMs: OPEN_DATA_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const OPEN_DATA_DEREG_CACHE = createTimedLruCache({
  ttlMs: OPEN_DATA_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const ICO_FLEET_CACHE = createTimedLruCache({
  ttlMs: OPEN_DATA_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const VIGNETTE_LOOKUP_CACHE = createTimedLruCache({
  ttlMs: VIGNETTE_CACHE_TTL_MS,
  maxEntries: RUNTIME_CACHE_MAX_ENTRIES,
  cloneValue: clone
});
const EDALNICE_CONFIG_CACHE = { value: null, expiresAt: 0 };
const EDALNICE_TOKEN_CACHE = { value: null, expiresAt: 0 };
const PLATE_BACKFILL_IN_FLIGHT = new Set();
const FLEET_DB_DIR = path.join(OPEN_DATA_PERSIST_DIR, "fleet-db");
const FLEET_DB_META_FILE = path.join(FLEET_DB_DIR, "meta.json");
const FLEET_DB_OWNER_DIR = path.join(FLEET_DB_DIR, "owners");
const FLEET_DB_OWNER_NAME_DIR = path.join(FLEET_DB_DIR, "owner-names");
const FLEET_DB_VEHICLE_DIR = path.join(FLEET_DB_DIR, "vehicles");
const FLEET_DB_OWNERSHIP_PCV_DIR = path.join(FLEET_DB_DIR, "ownership-pcv");
const FLEET_DB_VIN_PCV_DIR = path.join(FLEET_DB_DIR, "vin-pcv");
const FLEET_DB_OWNER_SHARD_CACHE = new Map();
const FLEET_DB_OWNER_NAME_SHARD_CACHE = new Map();
const FLEET_DB_VEHICLE_SHARD_CACHE = new Map();
const FLEET_DB_OWNERSHIP_PCV_SHARD_CACHE = new Map();
const FLEET_DB_VIN_PCV_SHARD_CACHE = new Map();
const ARES_NAME_LOOKUP_CACHE = new Map();
const KNOWN_STK_STATION_NAMES = {
  "3114": "Bohdalec"
};

const MOCK_VEHICLES = [
  {
    aliases: ["1AB2345", "TMBJJ7NE8L0123456"],
    data: {
      source: {
        mode: "demo",
        label: "Demo dataset",
        note: "Ukázkový záznam pro demonstraci aplikace."
      },
      hero: {
        badge: "Právnická osoba",
        title: "Skoda Octavia Combi 2.0 TDI Style",
        subtitle: "Přehled registračních, technických a vlastnických údajů pro rychlé interní ověření.",
        status: "Aktivní"
      },
      highlights: [
        { label: "SPZ", value: "1AB2345" },
        { label: "VIN", value: "TMBJJ7NE8L0123456" },
        { label: "První registrace", value: "18.03.2019" },
        { label: "Palivo", value: "Nafta" },
        { label: "Výkon", value: "110 kW" },
        { label: "STK do", value: "11.02.2027", tone: "positive" }
      ],
      ownership: {
        ownerCount: 2,
        operatorCount: 1,
        note: "Právnické osoby mohou být zobrazeny včetně IČO a adresy.",
        parties: [
          {
            role: "Aktuální vlastník",
            type: "company",
            name: "EX Leasing s.r.o.",
            ico: "27074358",
            address: "Budějovická 778/3a, Praha 4",
            since: "04.09.2023"
          },
          {
            role: "Provozovatel",
            type: "company",
            name: "Fleet Operations CZ a.s.",
            ico: "27112233",
            address: "Vyskočilova 1461/2a, Praha 4",
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
            { label: "První registrace v ČR", value: "18.03.2019" },
            { label: "ORV", value: "UC 458921" },
            { label: "TP", value: "ABC556712" },
            { label: "Úřady", value: "Magistrát hl. m. Prahy" }
          ]
        },
        {
          title: "Technické údaje",
          items: [
            { label: "Motor", value: "2.0 TDI" },
            { label: "Zdvihový objem", value: "1 968 cm3" },
            { label: "Převodovka", value: "DSG" },
            { label: "Barva", value: "Šedá metalíza" },
            { label: "Počet míst", value: "5" },
            { label: "Délka", value: "4 861 mm" },
            { label: "Šířka", value: "1 864 mm" },
            { label: "Výška", value: "1 468 mm" },
            { label: "Rozvor", value: "2 841 mm" },
            { label: "Provozní hmotnost", value: "1 485 kg" },
            { label: "CO2", value: "122 g/km" },
            { label: "Euro norma", value: "EURO 6" }
          ]
        },
        {
          title: "Kontroly a omezení",
          items: [
            { label: "STK platná do", value: "11.02.2027", tone: "positive" },
            { label: "Emise platné do", value: "11.02.2027", tone: "positive" },
            { label: "Odcizení", value: "Neevidováno", tone: "positive" },
            { label: "Vyřazení z provozu", value: "Ne" },
            { label: "Zástavní právo", value: "Bez záznamu", tone: "positive" }
          ]
        }
      ],
      timeline: [
        {
          date: "2019-03-18",
          title: "První registrace",
          description: "Vozidlo bylo poprvé registrováno v České republice.",
          tone: "neutral"
        },
        {
          date: "2023-09-04",
          title: "Změna vlastníka",
          description: "Převod na právnickou osobu a aktualizace provozovatele.",
          tone: "accent"
        },
        {
          date: "2025-02-11",
          title: "Poslední technická kontrola",
          description: "STK i emise bez závady.",
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
        note: "Ukázka záznamu se soukromým vlastnictvím."
      },
      hero: {
        badge: "Fyzická osoba",
        title: "BMW X5 xDrive30d",
        subtitle: "Ukázka odpovědi se stejnou strukturou, ale bez zobrazení osobních údajů.",
        status: "Aktivní"
      },
      highlights: [
        { label: "SPZ", value: "5AC5678" },
        { label: "VIN", value: "WBA11EV070N765432" },
        { label: "První registrace", value: "06.05.2022" },
        { label: "Palivo", value: "Nafta" },
        { label: "Výkon", value: "210 kW" },
        { label: "STK do", value: "06.05.2026", tone: "positive" }
      ],
      ownership: {
        ownerCount: 1,
        operatorCount: 1,
        note: "Fyzické osoby mohou být zobrazeny pouze anonymizovaně.",
        parties: [
          {
            role: "Aktuální vlastník",
            type: "person",
            name: "Fyzická osoba",
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
            { label: "První registrace v ČR", value: "06.05.2022" }
          ]
        },
        {
          title: "Technické údaje",
          items: [
            { label: "Motor", value: "3.0d" },
            { label: "Převodovka", value: "Automatická" },
            { label: "Barva", value: "Carbon Black" },
            { label: "Počet míst", value: "5" },
            { label: "Délka", value: "4 935 mm" },
            { label: "Šířka", value: "2 004 mm" },
            { label: "Výška", value: "1 765 mm" },
            { label: "Rozvor", value: "2 975 mm" },
            { label: "Provozní hmotnost", value: "2 110 kg" },
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

function getLookupRuntimeStatus() {
  const transportEndpoint = normalizeWhitespace(process.env.TRANSPORT_CUBE_LOOKUP_URL || "");
  const transportConfigured = Boolean(transportEndpoint);
  const officialVinApiConfigured = Boolean(process.env.DATAOVOZIDLECH_API_KEY || process.env.RSV_PUBLIC_API_KEY);
  const plateBrowserConfigured = Boolean(PVZP_BROWSER_PATH);
  const runtime = {
    platform: process.platform,
    nodeVersion: process.version,
    mockDataEnabled: ENABLE_MOCK_DATA,
    transportProvider: {
      configured: transportConfigured,
      method: (process.env.TRANSPORT_CUBE_METHOD || "GET").toUpperCase(),
      host: extractUrlHost(transportEndpoint),
      identifierParam: process.env.TRANSPORT_CUBE_IDENTIFIER_PARAM || "identifier",
      identifierTypeParam: process.env.TRANSPORT_CUBE_IDENTIFIER_TYPE_PARAM || "identifierType",
      hasApiKey: Boolean(process.env.TRANSPORT_CUBE_API_KEY)
    },
    officialVinApi: {
      configured: officialVinApiConfigured
    },
	    openDataDatabase: openDataDb.getDatabaseRuntimeStatus(),
	    localFleetDbFallback: {
	      mode: FLEET_DB_FALLBACK_MODE,
	      enabled: shouldUseFleetDbFallback(),
	      wholeShardCacheEnabled: FLEET_DB_ALLOW_WHOLE_SHARD_CACHE
	    },
	    runtimeCaches: {
	      maxEntries: RUNTIME_CACHE_MAX_ENTRIES
	    },
    alprProvider: {
      enabled: ALPR_PLATE_LOOKUP_ENABLED,
      provider: ALPR_PROVIDER,
      configuredProviders: getAlprProviderConfigs().map((provider) => provider.name),
      plateRecognizer: {
        configured: Boolean(PLATE_RECOGNIZER_API_TOKEN),
        host: extractUrlHost(PLATE_RECOGNIZER_API_URL),
        regions: PLATE_RECOGNIZER_REGIONS,
        timeoutMs: PLATE_RECOGNIZER_TIMEOUT_MS
      },
      local: {
        configured: Boolean(LOCAL_ALPR_API_URL),
        host: extractUrlHost(LOCAL_ALPR_API_URL),
        timeoutMs: LOCAL_ALPR_TIMEOUT_MS
      }
    },
    odometerProvider: {
      configured:
        (OPENDATALAB_STK_LOOKUP_ENABLED && Boolean(OPENDATALAB_STK_API_URL)) ||
        (ISTP_STK_LOOKUP_ENABLED && Boolean(ISTP_STK_SPARQL_URL)),
      provider: [
        OPENDATALAB_STK_LOOKUP_ENABLED ? "OpenDataLab STK portal" : null,
        ISTP_STK_LOOKUP_ENABLED ? "NKOD ISTP STK XML" : null
      ].filter(Boolean).join(" + ") || "none",
      host: [
        extractUrlHost(OPENDATALAB_STK_API_URL),
        extractUrlHost(ISTP_STK_SPARQL_URL)
      ].filter(Boolean).join(" + ") || null,
      timeoutMs: Math.max(OPENDATALAB_STK_TIMEOUT_MS, ISTP_STK_TIMEOUT_MS)
    },
    vignetteProvider: {
      configured: Boolean(VIGNETTE_LOOKUP_URL) || EDALNICE_LOOKUP_ENABLED,
      provider: VIGNETTE_LOOKUP_URL ? "custom" : EDALNICE_LOOKUP_ENABLED ? "edalnice" : "none",
      method: VIGNETTE_LOOKUP_URL ? VIGNETTE_LOOKUP_METHOD : "GET",
      host: extractUrlHost(VIGNETTE_LOOKUP_URL || EDALNICE_CONFIG_URL),
      plateParam: VIGNETTE_LOOKUP_PLATE_PARAM,
      countryParam: VIGNETTE_LOOKUP_COUNTRY_PARAM,
      defaultCountry: VIGNETTE_LOOKUP_COUNTRY,
      hasApiKey: Boolean(process.env.VIGNETTE_LOOKUP_API_KEY),
      edalniceEnabled: EDALNICE_LOOKUP_ENABLED
    },
	    ownershipThirdPartyFallback: {
      enabled: THIRD_PARTY_OWNERSHIP_FALLBACK_ENABLED
    },
    plateFallback: {
      enabled: PVZP_LOOKUP_ENABLED,
      browserConfigured: plateBrowserConfigured,
      browserSource: PVZP_BROWSER_INFO.source,
      headless: PVZP_HEADLESS,
      displayAvailable: Boolean(process.env.DISPLAY),
      debugCaptureEnabled: false,
      debugTokenConfigured: false
    },
    uniqaFallback: {
      enabled: UNIQA_LOOKUP_ENABLED,
      phoneConfigured: Boolean(UNIQA_PHONE),
      browserConfigured: Boolean(UNIQA_BROWSER_PATH || BROWSERLESS_ENABLED),
      browserSource: BROWSERLESS_ENABLED ? "browserless" : UNIQA_BROWSER_INFO.source,
      headless: PVZP_HEADLESS,
      displayAvailable: Boolean(process.env.DISPLAY)
    },
    warnings: []
  };

  if (!runtime.transportProvider.configured) {
    runtime.warnings.push("Chybí TRANSPORT_CUBE_LOOKUP_URL; primární provider není nakonfigurovaný.");
  }

  if (!runtime.officialVinApi.configured) {
    runtime.warnings.push("Chybí DATAOVOZIDLECH_API_KEY nebo RSV_PUBLIC_API_KEY; oficiální VIN API není nakonfigurované.");
  }

  if (runtime.plateFallback.enabled && !runtime.plateFallback.browserConfigured) {
    runtime.warnings.push("Externí doplňkový zdroj nemá dostupný browser.");
  }

  if (runtime.plateFallback.browserSource === "env-missing") {
    runtime.warnings.push("Cesta k browseru pro externí doplňkový zdroj je neplatná.");
  }

  if (runtime.plateFallback.enabled && runtime.platform === "linux" && runtime.plateFallback.headless) {
    runtime.warnings.push("Externí doplňkový zdroj běží v headless režimu.");
  }

  if (runtime.plateFallback.enabled && !runtime.plateFallback.headless && runtime.platform === "linux" && !runtime.plateFallback.displayAvailable) {
    runtime.warnings.push(
      "Externí doplňkový zdroj v headed režimu na Linuxu potřebuje DISPLAY."
    );
  }

  if (runtime.uniqaFallback.enabled && !runtime.uniqaFallback.phoneConfigured) {
    runtime.warnings.push("Externí doplňkový zdroj není kompletně nakonfigurovaný.");
  }

  if (runtime.uniqaFallback.enabled && !runtime.uniqaFallback.browserConfigured) {
    runtime.warnings.push("Externí doplňkový zdroj nemá dostupný browser ani Browserless endpoint.");
  }

  return runtime;
}

async function getOpenDataRuntimeStatus() {
  return await openDataDb.getOpenDataStatus([
    "ownership",
    "vehicles",
    "inspections",
    "deregistered",
    "imports",
    "equipment",
    "manufacturer_reports"
  ]);
}

function createLookupDiagnostics(lookup) {
  return {
    queryType: lookup.type,
    runtime: getLookupRuntimeStatus(),
    attempts: []
  };
}

function recordLookupAttempt(diagnostics, attempt) {
  if (!diagnostics || !attempt) {
    return;
  }

  diagnostics.attempts.push({
    source: attempt.source || "unknown",
    status: attempt.status || "unknown",
    detail: attempt.detail || null,
    host: attempt.host || null,
    method: attempt.method || null
  });
}

function formatLookupSource(source) {
  const labels = {
    "transport-cube": "Transport provider",
    "open-data-db": "Open data DB",
    "official-vin-api": "Veřejné VIN API",
    "pvzp-browser": "Externí zdroj",
    "uniqa-browser": "Externí zdroj",
    "hlidac-statu": "Hlídač státu",
    demo: "Demo dataset"
  };

  return labels[source] || "Lookup krok";
}

function uniqueText(values) {
  return Array.from(new Set((Array.isArray(values) ? values : []).map((value) => normalizeWhitespace(value)).filter(Boolean)));
}

function extractUrlHost(value) {
  if (!value) {
    return null;
  }

  try {
    return new URL(value).host || null;
  } catch (error) {
    return "invalid-url";
  }
}

async function resolveListingLookupQuery(query, diagnostics) {
  const listingUrl = normalizeListingLookupUrl(query);

  if (!listingUrl) {
    return null;
  }

  if (!LISTING_URL_LOOKUP_ENABLED) {
    recordLookupAttempt(diagnostics, {
      source: "listing-url",
      status: "skipped",
      host: listingUrl.host,
      detail: "Lookup podle URL je vypnutý."
    });
    return null;
  }

  if (!isSafePublicHttpUrl(listingUrl)) {
    recordLookupAttempt(diagnostics, {
      source: "listing-url",
      status: "miss",
      host: listingUrl.host,
      detail: "URL není veřejná http/https adresa."
    });
    return null;
  }

  const candidateFromUrl = extractVehicleIdentifierFromText(safeDecodeUrlText(listingUrl.toString()), { preferPlate: false });
  if (candidateFromUrl) {
    recordLookupAttempt(diagnostics, {
      source: "listing-url",
      status: "success",
      host: listingUrl.host,
      detail: `Identifikátor typu ${candidateFromUrl.type} byl rozpoznán přímo z URL.`
    });
    return candidateFromUrl;
  }

  try {
    const html = await requestHtml(listingUrl.toString(), {
      timeoutMs: LISTING_URL_LOOKUP_TIMEOUT_MS,
      headers: {
        Accept: "text/html,application/xhtml+xml",
        "User-Agent": PVZP_USER_AGENT
      }
    });
    const searchableText = extractSearchableTextFromHtml(html);
    const candidate = extractVehicleIdentifierFromText(searchableText, { preferPlate: false });

    if (!candidate) {
      recordLookupAttempt(diagnostics, {
        source: "listing-url",
        status: "miss",
        host: listingUrl.host,
        detail: "V inzerátu se nepodařilo najít VIN ani SPZ."
      });
      return null;
    }

    recordLookupAttempt(diagnostics, {
      source: "listing-url",
      status: "success",
      host: listingUrl.host,
      detail: `Z odkazu byl rozpoznán identifikátor typu ${candidate.type}.`
    });

    return candidate;
  } catch (error) {
    recordLookupAttempt(diagnostics, {
      source: "listing-url",
      status: "error",
      host: listingUrl.host,
      detail: formatLookupError(error)
    });
    return null;
  }
}

async function scanPlateImage(imageData) {
  if (!ALPR_PLATE_LOOKUP_ENABLED && !OCR_PLATE_LOOKUP_ENABLED) {
    const error = new Error("Čtení SPZ z fotky je dočasně vypnuté.");
    error.code = "OCR_DISABLED";
    throw error;
  }

  const image = parsePlateImagePayload(imageData);
  const alpr = await scanPlateImageWithAlpr(image.buffer);
  if (alpr?.candidate) {
    return {
      ...alpr.candidate,
      plate: alpr.candidate.type === "plate" ? alpr.candidate.identifier : null,
      rawText: alpr.rawText,
      confidence: alpr.confidence,
      scanRegion: alpr.region || null,
      provider: alpr.provider,
      bytes: image.buffer.length
    };
  }

  if (!OCR_PLATE_LOOKUP_ENABLED) {
    const error = new Error("SPZ se přes ALPR nepodařilo bezpečně přečíst.");
    error.code = "ALPR_NO_CANDIDATE";
    throw error;
  }

  const ocr = await queuePlateOcr(image.buffer);
  const candidate = ocr.candidate || extractVehicleIdentifierFromText(ocr.text, { preferPlate: true, strictPlate: true });

  if (!candidate) {
    const error = new Error("SPZ se z fotky nepodařilo bezpečně přečíst.");
    error.code = "OCR_NO_CANDIDATE";
    error.ocrText = ocr.text;
    throw error;
  }

  return {
    ...candidate,
    plate: candidate.type === "plate" ? candidate.identifier : null,
    rawText: ocr.text,
    confidence: ocr.confidence,
    scanRegion: ocr.region || null,
    provider: "tesseract",
    bytes: image.buffer.length
  };
}

function normalizeListingLookupUrl(value) {
  const raw = normalizeWhitespace(value);
  if (!/^https?:\/\//i.test(raw) || raw.length > 2000) {
    return null;
  }

  try {
    return new URL(raw);
  } catch (error) {
    return null;
  }
}

function safeDecodeUrlText(value) {
  try {
    return decodeURIComponent(String(value || ""));
  } catch (error) {
    return String(value || "");
  }
}

function isSafePublicHttpUrl(targetUrl) {
  if (!targetUrl || !["http:", "https:"].includes(targetUrl.protocol)) {
    return false;
  }

  if (targetUrl.username || targetUrl.password) {
    return false;
  }

  const hostname = String(targetUrl.hostname || "").toLowerCase();
  if (!hostname || hostname === "localhost" || hostname.endsWith(".localhost")) {
    return false;
  }

  const ipVersion = net.isIP(hostname);
  if (ipVersion) {
    return !isPrivateIpAddress(hostname, ipVersion);
  }

  return ![
    "169.254.169.254",
    "metadata.google.internal"
  ].includes(hostname);
}

function isPrivateIpAddress(hostname, ipVersion) {
  if (ipVersion === 4) {
    const parts = hostname.split(".").map((part) => Number(part));
    if (parts.length !== 4 || parts.some((part) => !Number.isInteger(part) || part < 0 || part > 255)) {
      return true;
    }

    return (
      parts[0] === 10 ||
      parts[0] === 127 ||
      (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) ||
      (parts[0] === 192 && parts[1] === 168) ||
      (parts[0] === 169 && parts[1] === 254) ||
      parts[0] === 0
    );
  }

  const normalized = hostname.toLowerCase();
  return (
    normalized === "::1" ||
    normalized.startsWith("fc") ||
    normalized.startsWith("fd") ||
    normalized.startsWith("fe80") ||
    normalized.startsWith("::ffff:127.") ||
    normalized.startsWith("::ffff:10.") ||
    normalized.startsWith("::ffff:192.168.")
  );
}

function extractSearchableTextFromHtml(html) {
  const $ = cheerio.load(String(html || ""));
  $("script,style,noscript,svg").remove();
  const metaText = [
    $("title").text(),
    $('meta[name="description"]').attr("content"),
    $('meta[property="og:title"]').attr("content"),
    $('meta[property="og:description"]').attr("content")
  ];

  return normalizeWhitespace([...metaText, $("body").text()].filter(Boolean).join(" "));
}

function extractVehicleIdentifierFromText(text, options = {}) {
  const normalized = normalizeWhitespace(text).toUpperCase();
  if (!normalized) {
    return null;
  }

  const vin = findBestVinCandidate(normalized);
  if (vin && !options.preferPlate) {
    return {
      identifier: vin,
      type: "vin"
    };
  }

  const plate = findBestPlateCandidate(normalized, options);
  if (plate) {
    return {
      identifier: plate,
      type: "plate"
    };
  }

  if (vin) {
    return {
      identifier: vin,
      type: "vin"
    };
  }

  return null;
}

function findBestVinCandidate(text) {
  const candidates = text.match(/[A-HJ-NPR-Z0-9]{17}/g) || [];
  return candidates.find((candidate) => parseLookupQuery(candidate, "vin").type === "vin") || null;
}

function findBestPlateCandidate(text, options = {}) {
  const scored = new Map();
  const labelPattern = /(SPZ|RZ|REGISTRA[CČ]N[IÍ]\s*ZNA[CČ]KA|ZNA[CČ]KA|PLATE)[^A-Z0-9]{0,28}([A-Z0-9][A-Z0-9\s-]{3,14})/g;
  let labelMatch;

  const pushCandidate = (value, bonus = 0) => {
    buildPlateCandidateVariants(value, options).forEach((candidate) => {
      const score = scorePlateCandidate(candidate, options) + bonus;
      if (score <= 0) {
        return;
      }

      const previous = scored.get(candidate);
      if (!previous || score > previous.score) {
        scored.set(candidate, { candidate, score });
      }
    });
  };

  while ((labelMatch = labelPattern.exec(text)) !== null) {
    pushCandidate(labelMatch[2], 5);
  }

  const looseMatches = text.match(/[A-Z0-9][A-Z0-9\s-]{3,12}[A-Z0-9]/g) || [];
  looseMatches.forEach((match) => {
    pushCandidate(match);
  });

  if (options.strictPlate) {
    pushCandidate(normalizePlateCandidate(text), -1);
  }

  const ranked = Array.from(scored.values())
    .sort((left, right) => right.score - left.score || left.candidate.length - right.candidate.length);
  return ranked[0]?.candidate || null;
}

function normalizePlateCandidate(value) {
  return String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

function buildPlateCandidateVariants(value, options = {}) {
  const normalized = normalizePlateCandidate(value);
  const rawCandidates = new Set();
  const addRaw = (candidate) => {
    const compact = normalizePlateCandidate(candidate);
    if (compact.length >= 5 && compact.length <= 10) {
      rawCandidates.add(compact);
    }
  };

  addRaw(normalized);
  if (normalized.startsWith("CZ")) {
    addRaw(normalized.slice(2));
  }

  if (options.strictPlate) {
    const compact = normalized.startsWith("CZ") ? normalized.slice(2) : normalized;
    if (compact.length > 8) {
      [7, 8, 6, 5, 9, 10].forEach((length) => {
        if (compact.length <= length) {
          return;
        }

        for (let index = 0; index + length <= compact.length; index += 1) {
          addRaw(compact.slice(index, index + length));
        }
      });
    }
  }

  const variants = new Set(rawCandidates);
  rawCandidates.forEach((candidate) => {
    addCorrectedPlateVariant(variants, candidate, normalizeCzechStandardPlateByPosition(candidate));
    addCorrectedPlateVariant(variants, candidate, normalizeCzechSpecialPlateByPosition(candidate));
    addCorrectedPlateVariant(variants, candidate, normalizeCzechWrappedStandardPlateByPosition(candidate));
  });

  return Array.from(variants);
}

function addPlateVariant(variants, candidate) {
  const compact = normalizePlateCandidate(candidate);
  if (compact.length >= 5 && compact.length <= 10) {
    variants.add(compact);
  }
}

function addCorrectedPlateVariant(variants, source, candidate) {
  const compactSource = normalizePlateCandidate(source);
  const compactCandidate = normalizePlateCandidate(candidate);
  if (!/[A-Z]/.test(compactSource) && /[A-Z]/.test(compactCandidate)) {
    return;
  }

  addPlateVariant(variants, compactCandidate);
}

function normalizeCzechStandardPlateByPosition(candidate) {
  const compact = normalizePlateCandidate(candidate);
  if (compact.length !== 7) {
    return compact;
  }

  return compact
    .split("")
    .map((char, index) => {
      if (index === 0 || index >= 3) {
        return normalizeOcrPlateDigit(char);
      }
      if (index === 1) {
        return normalizeOcrPlateLetter(char);
      }
      return char;
    })
    .join("");
}

function normalizeCzechSpecialPlateByPosition(candidate) {
  const compact = normalizePlateCandidate(candidate);
  if (compact.length !== 7) {
    return compact;
  }

  return compact
    .split("")
    .map((char, index) => {
      if (index >= 2 && index <= 4) {
        return normalizeOcrPlateDigit(char);
      }
      return normalizeOcrPlateLetter(char);
    })
    .join("");
}

function normalizeCzechWrappedStandardPlateByPosition(candidate) {
  const compact = normalizePlateCandidate(candidate);
  if (
    compact.length !== 7 ||
    !/^\d[A-Z][A-Z0-9]\d{4}$/.test(compact) ||
    isValidCzechPlateRegionLetter(compact[1]) ||
    !isOcrDigitLikeLetter(compact[1])
  ) {
    return compact;
  }

  return normalizeCzechStandardPlateByPosition(compact.slice(1) + compact[0]);
}

function normalizeOcrPlateDigit(char) {
  return ({
    O: "0",
    Q: "0",
    D: "0",
    I: "1",
    L: "1",
    Z: "2",
    S: "5",
    G: "6",
    T: "7",
    B: "8",
    A: "4"
  })[char] || char;
}

function normalizeOcrPlateLetter(char) {
  return ({
    0: "O",
    1: "I",
    2: "Z",
    5: "S",
    6: "G",
    7: "T",
    8: "B"
  })[char] || char;
}

function isOcrDigitLikeLetter(char) {
  return Boolean(({
    O: true,
    Q: true,
    D: true,
    I: true,
    L: true,
    Z: true,
    S: true,
    G: true,
    T: true,
    B: true
  })[char]);
}

function isValidCzechPlateRegionLetter(char) {
  return CZECH_PLATE_REGION_LETTERS.includes(char);
}

function scorePlateCandidate(candidate, options = {}) {
  if (!candidate || parseLookupQuery(candidate, "plate").type !== "plate") {
    return 0;
  }

  const hasLetter = /[A-Z]/.test(candidate);
  const hasDigit = /\d/.test(candidate);
  if (!hasLetter || !hasDigit) {
    return 0;
  }

  const commonWords = new Set([
    "AUTOMOBIL",
    "MOTORIZ",
    "KAROSERIE",
    "POJISTENI",
    "PROVOZ",
    "TECHNICK",
    "REGISTR",
    "TACHOMETR",
    "HISTORIE"
  ]);
  if (commonWords.has(candidate)) {
    return 0;
  }

  const isStrict = Boolean(options.strictPlate);
  if (/^[1-9][A-Z][A-Z0-9][0-9]{4}$/.test(candidate) && isValidCzechPlateRegionLetter(candidate[1])) {
    return isStrict ? 14 : 7;
  }

  if (/^[A-Z]{2}[0-9]{3}[A-Z]{2}$/.test(candidate)) {
    return isStrict ? 12 : 7;
  }

  if (options.allowCustomPlate && isLikelyCzechCustomPlate(candidate)) {
    return isStrict ? 10 : 6;
  }

  if (isStrict) {
    return 0;
  }

  if (/^[A-Z]{1,3}[0-9]{2,4}[A-Z]{0,2}$/.test(candidate) && candidate.length <= 7) {
    return 4;
  }

  let score = 2;
  if (/^[0-9][A-Z0-9]{1,2}[0-9]{4}$/.test(candidate)) {
    score += 4;
  }
  if (/^[A-Z]{1,3}[0-9]{2,4}[A-Z]{0,3}$/.test(candidate)) {
    score += 2;
  }
  if (candidate.length >= 6 && candidate.length <= 8) {
    score += 2;
  }

  return score;
}

function isLikelyCzechCustomPlate(candidate) {
  if (!/^[A-Z0-9]{7,8}$/.test(candidate)) {
    return false;
  }

  if (!/[0-9]/.test(candidate)) {
    return false;
  }

  if (/[GOQW]/.test(candidate)) {
    return false;
  }

  return true;
}

async function scanPlateImageWithAlpr(buffer) {
  const providers = getAlprProviderConfigs();
  if (!providers.length) {
    return null;
  }

  for (const provider of providers) {
    try {
      const payload = await requestPlateRecognizerCompatibleAlpr(provider, buffer);
      const result = selectBestAlprCandidate(payload, provider.name);
      if (result?.candidate) {
        return result;
      }
    } catch (error) {
      // ALPR is a primary path, but OCR fallback should still get a chance.
    }
  }

  return null;
}

function getAlprProviderConfigs() {
  if (!ALPR_PLATE_LOOKUP_ENABLED || ["disabled", "none", "off", "false"].includes(ALPR_PROVIDER)) {
    return [];
  }

  const providers = [];
  const wantsLocal = ALPR_PROVIDER === "auto" || ALPR_PROVIDER === "local";
  const wantsPlateRecognizer =
    ALPR_PROVIDER === "auto" || ALPR_PROVIDER === "plate-recognizer" || ALPR_PROVIDER === "platerecognizer";

  if (wantsLocal && LOCAL_ALPR_API_URL) {
    providers.push({
      name: "local-alpr",
      endpoint: LOCAL_ALPR_API_URL,
      token: "",
      timeoutMs: LOCAL_ALPR_TIMEOUT_MS,
      regions: PLATE_RECOGNIZER_REGIONS,
      config: PLATE_RECOGNIZER_CONFIG
    });
  }

  if (wantsPlateRecognizer && PLATE_RECOGNIZER_API_TOKEN && PLATE_RECOGNIZER_API_URL) {
    providers.push({
      name: "plate-recognizer",
      endpoint: PLATE_RECOGNIZER_API_URL,
      token: PLATE_RECOGNIZER_API_TOKEN,
      timeoutMs: PLATE_RECOGNIZER_TIMEOUT_MS,
      regions: PLATE_RECOGNIZER_REGIONS,
      config: PLATE_RECOGNIZER_CONFIG
    });
  }

  return providers;
}

async function requestPlateRecognizerCompatibleAlpr(provider, buffer) {
  const fields = [
    {
      name: "upload",
      filename: "vehicle.jpg",
      contentType: "image/jpeg",
      value: buffer
    }
  ];

  (provider.regions || []).forEach((region) => {
    fields.push({ name: "regions", value: region });
  });

  if (provider.config) {
    fields.push({ name: "config", value: provider.config });
  }

  const multipart = createMultipartBody(fields);
  const headers = {
    Accept: "application/json",
    "Content-Type": multipart.contentType,
    "Content-Length": String(multipart.body.length)
  };

  if (provider.token) {
    headers.Authorization = `Token ${provider.token}`;
  }

  return await requestJson(provider.endpoint, {
    method: "POST",
    headers,
    body: multipart.body,
    timeoutMs: provider.timeoutMs
  });
}

function createMultipartBody(fields) {
  const boundary = `----autoinfo-${Date.now().toString(16)}-${Math.random().toString(16).slice(2)}`;
  const chunks = [];

  fields.forEach((field) => {
    const disposition = [
      `form-data; name="${escapeMultipartHeaderValue(field.name)}"`,
      field.filename ? `filename="${escapeMultipartHeaderValue(field.filename)}"` : null
    ].filter(Boolean).join("; ");
    const headers = [
      `--${boundary}`,
      `Content-Disposition: ${disposition}`,
      field.contentType ? `Content-Type: ${field.contentType}` : null,
      "",
      ""
    ].filter((line) => line !== null).join("\r\n");

    chunks.push(Buffer.from(headers, "utf8"));
    chunks.push(Buffer.isBuffer(field.value) ? field.value : Buffer.from(String(field.value || ""), "utf8"));
    chunks.push(Buffer.from("\r\n", "utf8"));
  });

  chunks.push(Buffer.from(`--${boundary}--\r\n`, "utf8"));

  return {
    body: Buffer.concat(chunks),
    contentType: `multipart/form-data; boundary=${boundary}`
  };
}

function escapeMultipartHeaderValue(value) {
  return String(value || "").replace(/["\r\n]/g, "");
}

function selectBestAlprCandidate(payload, providerName) {
  const rows = [];

  normalizeAlprResults(payload).forEach((result, resultIndex) => {
    pushAlprCandidate(rows, result.plate, result.score, result, resultIndex, providerName);
    (Array.isArray(result.candidates) ? result.candidates : []).forEach((candidate) => {
      pushAlprCandidate(rows, candidate?.plate, candidate?.score, result, resultIndex, providerName);
    });
  });

  const best = rows.sort((left, right) => right.score - left.score || left.resultIndex - right.resultIndex)[0] || null;
  if (!best) {
    return null;
  }

  return {
    provider: providerName,
    candidate: best.candidate,
    confidence: best.confidence,
    rawText: best.rawText,
    region: best.region
  };
}

function normalizeAlprResults(payload) {
  if (Array.isArray(payload?.results)) {
    return payload.results;
  }

  if (Array.isArray(payload?.plates)) {
    return payload.plates;
  }

  if (Array.isArray(payload?.predictions)) {
    return payload.predictions;
  }

  if (payload?.plate || payload?.text || payload?.number) {
    return [payload];
  }

  return [];
}

function pushAlprCandidate(rows, value, confidence, result, resultIndex, providerName) {
  const rawText = normalizePlateCandidate(value);
  if (!rawText) {
    return;
  }

  const candidate = extractVehicleIdentifierFromText(rawText, {
    preferPlate: true,
    strictPlate: true,
    allowCustomPlate: true
  });
  if (!candidate || candidate.type !== "plate") {
    return;
  }

  const providerConfidence = normalizeConfidenceScore(confidence ?? result?.score ?? result?.confidence);
  const regionCode = normalizeWhitespace(result?.region?.code || result?.region || result?.country || "").toLowerCase();
  const plateScore = scorePlateCandidate(candidate.identifier, {
    strictPlate: true,
    allowCustomPlate: true
  });

  rows.push({
    provider: providerName,
    candidate,
    confidence: providerConfidence,
    rawText,
    region: normalizeAlprRegion(result),
    resultIndex,
    score:
      plateScore +
      providerConfidence * 10 +
      (regionCode === "cz" ? 2 : 0) +
      (providerName === "plate-recognizer" ? 1 : 0)
  });
}

function normalizeAlprRegion(result) {
  const box = result?.box || result?.bbox || result?.bounding_box || null;
  if (!box || typeof box !== "object") {
    return null;
  }

  const left = firstFiniteNumber(box.xmin, box.left, box.x);
  const top = firstFiniteNumber(box.ymin, box.top, box.y);
  const right = firstFiniteNumber(box.xmax, box.right);
  const bottom = firstFiniteNumber(box.ymax, box.bottom);
  const width = firstFiniteNumber(box.width, right !== null && left !== null ? right - left : null);
  const height = firstFiniteNumber(box.height, bottom !== null && top !== null ? bottom - top : null);

  return {
    label: "alpr-box",
    left,
    top,
    width,
    height
  };
}

function firstFiniteNumber(...values) {
  for (const value of values) {
    const number = Number(value);
    if (Number.isFinite(number)) {
      return number;
    }
  }

  return null;
}

function normalizeConfidenceScore(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return 0;
  }

  return number > 1 ? Math.max(0, Math.min(1, number / 100)) : Math.max(0, Math.min(1, number));
}

function normalizePlateRecognizerRegions(value) {
  return String(value || "")
    .split(/[,\s]+/)
    .map((region) => normalizeWhitespace(region).toLowerCase())
    .filter(Boolean);
}

function parsePlateImagePayload(value) {
  const raw = String(value || "");
  const match = raw.match(/^data:image\/(?:png|jpe?g|webp|bmp);base64,([a-z0-9+/=\r\n]+)$/i);
  const base64 = match ? match[1] : raw;

  if (!base64 || base64.length > Math.ceil(OCR_PLATE_MAX_IMAGE_BYTES * 1.4)) {
    const error = new Error("Fotka je příliš velká.");
    error.code = "OCR_IMAGE_TOO_LARGE";
    throw error;
  }

  const buffer = Buffer.from(base64.replace(/\s+/g, ""), "base64");
  if (!buffer.length || buffer.length > OCR_PLATE_MAX_IMAGE_BYTES) {
    const error = new Error("Fotka je příliš velká nebo poškozená.");
    error.code = "OCR_IMAGE_INVALID";
    throw error;
  }

  return { buffer };
}

async function queuePlateOcr(buffer) {
  const task = OCR_QUEUE.catch(() => null).then(() => runPlateOcr(buffer));
  OCR_QUEUE = task.catch(() => null);
  return task;
}

async function runPlateOcr(buffer) {
  const worker = await getOcrWorker();
  const regions = buildPlateOcrRegions(buffer);
  const passes = [];
  const seenText = new Set();

  for (const region of regions) {
    try {
      await worker.setParameters({
        tessedit_pageseg_mode: region.psm
      });
      const result = await worker.recognize(
        buffer,
        region.rectangle ? { rectangle: region.rectangle } : {}
      );
      const text = normalizeWhitespace(result?.data?.text || "");
      const confidence =
        typeof result?.data?.confidence === "number" ? Math.round(result.data.confidence) : null;
      const candidate = extractVehicleIdentifierFromText(text, { preferPlate: true, strictPlate: true });

      if (text && !seenText.has(text)) {
        seenText.add(text);
      }

      passes.push({
        label: region.label,
        priority: region.priority,
        text,
        confidence,
        candidate,
        score: scorePlateOcrPass(candidate, confidence, region.priority)
      });
      if (passes[passes.length - 1].score >= 14 && (confidence === null || confidence >= 45)) {
        break;
      }
    } catch (error) {
      passes.push({
        label: region.label,
        priority: region.priority,
        text: "",
        confidence: null,
        candidate: null,
        score: 0
      });
    }
  }

  const best = passes
    .filter((pass) => pass.candidate)
    .sort((left, right) => right.score - left.score || (right.confidence || 0) - (left.confidence || 0))[0] || null;

  return {
    text: normalizeWhitespace([
      best?.candidate?.identifier,
      ...Array.from(seenText)
    ].filter(Boolean).join(" ")),
    confidence: best?.confidence ?? passes.find((pass) => pass.confidence !== null)?.confidence ?? null,
    candidate: best?.candidate || null,
    region: best?.label || null
  };
}

function buildPlateOcrRegions(buffer) {
  const { PSM } = require("tesseract.js");
  const dimensions = getImageDimensions(buffer);
  const regions = [
    {
      label: "full-image",
      rectangle: null,
      priority: 0,
      psm: PSM.SPARSE_TEXT
    }
  ];

  if (!dimensions?.width || !dimensions?.height) {
    return regions;
  }

  const aspectRatio = dimensions.width / dimensions.height;
  const targetedRegions = [];

  if (aspectRatio < 0.85) {
    targetedRegions.push(
      makeRelativeOcrRegion(dimensions, "portrait-front-plate-text-tight", 0.39, 0.475, 0.34, 0.095, 12, PSM.SINGLE_LINE),
      makeRelativeOcrRegion(dimensions, "portrait-front-plate-text-wide", 0.36, 0.462, 0.42, 0.13, 11, PSM.SINGLE_LINE),
      makeRelativeOcrRegion(dimensions, "portrait-front-plate-tight", 0.32, 0.47, 0.44, 0.13, 10, PSM.SINGLE_LINE),
      makeRelativeOcrRegion(dimensions, "portrait-front-plate-wide", 0.25, 0.44, 0.56, 0.20, 9, PSM.SPARSE_TEXT)
    );
  }

  return [
    ...targetedRegions,
    makeRelativeOcrRegion(dimensions, "front-plate-lower-center", 0.14, 0.48, 0.44, 0.23, 7, PSM.SINGLE_LINE),
    makeRelativeOcrRegion(dimensions, "front-plate-lower-left", 0.06, 0.48, 0.48, 0.27, 6, PSM.SINGLE_LINE),
    makeRelativeOcrRegion(dimensions, "bumper-wide", 0.06, 0.43, 0.62, 0.34, 5, PSM.SPARSE_TEXT),
    makeRelativeOcrRegion(dimensions, "lower-half", 0.00, 0.44, 0.72, 0.40, 4, PSM.SPARSE_TEXT),
    makeRelativeOcrRegion(dimensions, "lower-center", 0.18, 0.50, 0.50, 0.30, 4, PSM.SPARSE_TEXT),
    makeRelativeOcrRegion(dimensions, "full-lower-band", 0.00, 0.48, 1.00, 0.34, 2, PSM.SPARSE_TEXT),
    ...regions
  ].filter(Boolean);
}

function makeRelativeOcrRegion(dimensions, label, left, top, width, height, priority, psm) {
  const imageWidth = dimensions.width;
  const imageHeight = dimensions.height;
  const rectangle = {
    left: Math.max(0, Math.round(imageWidth * left)),
    top: Math.max(0, Math.round(imageHeight * top)),
    width: Math.max(1, Math.round(imageWidth * width)),
    height: Math.max(1, Math.round(imageHeight * height))
  };

  if (rectangle.left + rectangle.width > imageWidth) {
    rectangle.width = imageWidth - rectangle.left;
  }
  if (rectangle.top + rectangle.height > imageHeight) {
    rectangle.height = imageHeight - rectangle.top;
  }

  if (rectangle.width < 40 || rectangle.height < 20) {
    return null;
  }

  return {
    label,
    rectangle,
    priority,
    psm
  };
}

function getImageDimensions(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 24) {
    return null;
  }

  const pngSignature = buffer.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]));
  if (pngSignature && buffer.length >= 24) {
    return {
      width: buffer.readUInt32BE(16),
      height: buffer.readUInt32BE(20)
    };
  }

  if (buffer[0] === 0xff && buffer[1] === 0xd8) {
    return getJpegDimensions(buffer);
  }

  if (buffer.toString("ascii", 0, 4) === "RIFF" && buffer.toString("ascii", 8, 12) === "WEBP") {
    return getWebpDimensions(buffer);
  }

  return null;
}

function getJpegDimensions(buffer) {
  let offset = 2;
  while (offset + 9 < buffer.length) {
    if (buffer[offset] !== 0xff) {
      offset += 1;
      continue;
    }

    const marker = buffer[offset + 1];
    if (marker === 0xd8 || marker === 0xd9) {
      offset += 2;
      continue;
    }

    const length = buffer.readUInt16BE(offset + 2);
    if (!length || offset + 2 + length > buffer.length) {
      return null;
    }

    if (
      (marker >= 0xc0 && marker <= 0xc3) ||
      (marker >= 0xc5 && marker <= 0xc7) ||
      (marker >= 0xc9 && marker <= 0xcb) ||
      (marker >= 0xcd && marker <= 0xcf)
    ) {
      return {
        height: buffer.readUInt16BE(offset + 5),
        width: buffer.readUInt16BE(offset + 7)
      };
    }

    offset += 2 + length;
  }

  return null;
}

function getWebpDimensions(buffer) {
  const chunk = buffer.toString("ascii", 12, 16);
  if (chunk === "VP8X" && buffer.length >= 30) {
    return {
      width: 1 + buffer.readUIntLE(24, 3),
      height: 1 + buffer.readUIntLE(27, 3)
    };
  }

  if (chunk === "VP8L" && buffer.length >= 25) {
    const b0 = buffer[21];
    const b1 = buffer[22];
    const b2 = buffer[23];
    const b3 = buffer[24];
    return {
      width: 1 + (((b1 & 0x3f) << 8) | b0),
      height: 1 + (((b3 & 0x0f) << 10) | (b2 << 2) | ((b1 & 0xc0) >> 6))
    };
  }

  return null;
}

function scorePlateOcrPass(candidate, confidence, regionPriority) {
  if (!candidate?.identifier || candidate.type !== "plate") {
    return 0;
  }

  return scorePlateCandidate(candidate.identifier, { strictPlate: true }) + Math.max(0, Number(confidence || 0)) / 20 + regionPriority;
}

async function getOcrWorker() {
  if (!OCR_WORKER_PROMISE) {
    OCR_WORKER_PROMISE = (async () => {
      const { createWorker, PSM } = require("tesseract.js");
      const worker = await createWorker("eng");
      await worker.setParameters({
        tessedit_char_whitelist: "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
        preserve_interword_spaces: "1",
        user_defined_dpi: "300",
        tessedit_pageseg_mode: PSM.SPARSE_TEXT
      });
      return worker;
    })();
  }

  return OCR_WORKER_PROMISE;
}

function formatLookupError(error) {
  if (!error) {
    return "Neznámá chyba.";
  }

  const details = [];
  const message = normalizeWhitespace(error.message || "");

  if (error.code && !message.includes(error.code)) {
    details.push(error.code);
  }

  if (typeof error.statusCode === "number") {
    details.push(`HTTP ${error.statusCode}`);
  }

  if (message) {
    details.push(message);
  }

  return uniqueText(details).join(" - ") || "Neznámá chyba.";
}

async function lookupVehicle(query, options = {}) {
  let lookup = parseLookupQuery(query, options.type);
  const diagnostics = createLookupDiagnostics(lookup);
  const listingResolution = await resolveListingLookupQuery(query, diagnostics);
  const lookupQuery = listingResolution?.identifier || query;
  if (listingResolution?.identifier) {
    lookup = parseLookupQuery(lookupQuery, listingResolution.type || options.type);
    diagnostics.queryType = lookup.type;
    diagnostics.resolvedLookup = lookup;
    diagnostics.resolvedFromUrl = true;
  }
  const databaseRecord = await lookupFromOpenDataDatabase(lookup, diagnostics);
  const liveRecord = databaseRecord ? null : await lookupFromConfiguredProvider(lookup, diagnostics);
  const liveDatabaseRecord = databaseRecord || !liveRecord
    ? null
    : await lookupResolvedRecordFromOpenDataDatabase(lookup, liveRecord, diagnostics);
  const localDatabaseRecord = databaseRecord || liveDatabaseRecord;
  const directVinRecord = lookup.type === "vin" && !liveRecord && !localDatabaseRecord
    ? await lookupFromOfficialVinApiWithBudget(lookup, diagnostics, false)
    : null;
  const shouldUseBrowserPlateFallback =
    lookup.type === "plate" || (lookup.type === "vin" && VIN_PLATE_PVZP_LOOKUP_ENABLED);
  const pvzpRecord = shouldUseBrowserPlateFallback && !databaseRecord && !liveRecord && !liveDatabaseRecord
    ? await lookupFromPvzpBrowser(lookup, diagnostics)
    : null;
  const uniqaRecord = lookup.type === "plate" && shouldUseBrowserPlateFallback && !databaseRecord && !liveRecord && !liveDatabaseRecord && !pvzpRecord
    ? await lookupFromUniqaBrowser(lookup, diagnostics)
    : null;
  const plateFallbackRecord = pvzpRecord || uniqaRecord;
  const fallbackDatabaseRecord = databaseRecord || liveDatabaseRecord || !plateFallbackRecord
    ? null
    : await lookupResolvedRecordFromOpenDataDatabase(lookup, plateFallbackRecord, diagnostics);
  const resolvedDatabaseRecord = localDatabaseRecord || fallbackDatabaseRecord;
  const officialVinLookup = resolveSupplementalVinLookup(lookup, liveRecord || resolvedDatabaseRecord || plateFallbackRecord);
  const publicVinRecord = directVinRecord || lookup.type === "vin" || liveRecord || resolvedDatabaseRecord
    ? null
    : await lookupFromOfficialVinApiWithBudget(officialVinLookup || lookup, diagnostics, lookup.type === "plate");
  const mockRecord = liveRecord || resolvedDatabaseRecord || directVinRecord || publicVinRecord || plateFallbackRecord ? null : findMockVehicle(lookup);
  const baseSeed = resolvedDatabaseRecord || liveRecord || directVinRecord || publicVinRecord || plateFallbackRecord || mockRecord;
  const missingOfficialVinDetails =
    baseSeed && (!hasInspectionValiditySignal(baseSeed) || !hasVehicleDimensionsSignal(baseSeed));
  const officialVinSupplementRecord =
    lookup.type === "vin" && baseSeed && baseSeed !== directVinRecord && missingOfficialVinDetails
      ? await lookupFromOfficialVinApiWithBudget(lookup, diagnostics, hasVehicleDimensionsSignal(baseSeed))
      : null;
  const baseRecord = mergeSupplementalRecord(
    mergeSupplementalRecord(baseSeed, officialVinSupplementRecord),
    plateFallbackRecord
  );

  if (!liveRecord && !resolvedDatabaseRecord && !directVinRecord && !publicVinRecord && !plateFallbackRecord) {
    recordLookupAttempt(diagnostics, {
      source: "demo",
      status: mockRecord ? "success" : ENABLE_MOCK_DATA ? "miss" : "skipped",
      detail: mockRecord
        ? "Byl nalezen záznam v lokálním demo datasetu."
        : ENABLE_MOCK_DATA
          ? "Dotaz nebyl nalezen v lokálním demo datasetu."
          : "Demo dataset je vypnuty."
    });
  }

  const record = await enrichVinLookupWithResolvedPlate(lookup, baseRecord, diagnostics);

  if (!record) {
    return { record: null, diagnostics };
  }

  const enriched = await enrichCompanies(record);
  const withInspectionState = await attachInspectionState(enriched, options);
  const withOwnershipState = await attachOwnershipState(withInspectionState, options);
  const shouldAttachRegistryState = !FAST_LOOKUP_MODE;
  const withRegistryState = shouldAttachRegistryState
    ? await attachPublicRegistryState(withOwnershipState)
    : withOwnershipState;
  await persistPlateResolutionSnapshot(lookup, withRegistryState).catch(() => null);
  await persistSupplementalOwnershipSnapshot(withRegistryState).catch(() => null);
  const sanitized = sanitizeClientRecord(withRegistryState);
  return {
    diagnostics,
    record: {
      ...sanitized,
      query: {
        raw: lookupQuery,
        normalized: lookup.compact,
        type: lookup.type,
        resolvedAt: new Date().toISOString()
      }
    }
  };
}

async function enrichVinLookupWithResolvedPlate(lookup, record, diagnostics) {
  if (!record || lookup?.type !== "vin" || !VIN_PLATE_PVZP_LOOKUP_ENABLED) {
    return record;
  }

  const existingPlate = normalizeWhitespace(extractIdentifier(record, "SPZ")).toUpperCase() || null;
  if (existingPlate) {
    return record;
  }

  const vin = normalizeWhitespace(extractIdentifier(record, "VIN") || lookup.compact).toUpperCase() || null;
  const pcv = normalizeWhitespace(extractIdentifier(record, "PČV") || extractIdentifier(record, "PCV")) || null;
  if (!vin || parseLookupQuery(vin, "vin").type !== "vin") {
    return record;
  }

  const result = await resolveVehiclePlateWithBudget({
    vin,
    pcv,
    allowPvzpFallback: true,
    allowUniqaFallback: false
  }, VIN_PLATE_PVZP_LOOKUP_TIMEOUT_MS);

  if (result?.status === "ready" && result.plate) {
    recordLookupAttempt(diagnostics, {
      source: "vin-plate-resolution",
      status: "success",
      detail: "SPZ byla doplněna pro VIN dotaz."
    });
    return injectPlateIntoRecord(record, result.plate);
  }

  recordLookupAttempt(diagnostics, {
    source: "vin-plate-resolution",
    status: result?.status === "timeout" ? "timeout" : "miss",
    detail: result?.message || "SPZ se pro VIN dotaz nepodařilo doplnit."
  });
  return record;
}

async function resolveVehiclePlateWithBudget(params, timeoutMs) {
  const budget = Math.max(0, Number(timeoutMs || 0) || 0);
  if (!budget) {
    return await resolveVehiclePlate(params);
  }

  let timeoutId = null;
  try {
    return await Promise.race([
      resolveVehiclePlate(params),
      new Promise((resolve) => {
        timeoutId = setTimeout(() => {
          resolve({
            status: "timeout",
            plate: null,
            vin: params?.vin || null,
            pcv: params?.pcv || null,
            message: "Dohledání SPZ překročilo časový limit."
          });
        }, budget);
      })
    ]);
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}

function describeLookupFailure(query, diagnostics, requestedType = null) {
  const lookup = diagnostics?.resolvedLookup || parseLookupQuery(query, requestedType);
  const runtime = diagnostics?.runtime || getLookupRuntimeStatus();
  const hints = buildPublicLookupFailureHints(lookup, diagnostics, runtime);

  return {
    message: "Pro zadaný identifikátor zatím nemám výsledek.",
    hints: uniqueText(hints),
    queryType: lookup.type,
    diagnostics: sanitizeDiagnosticsForClient(diagnostics || {
      queryType: lookup.type,
      runtime,
      attempts: []
    })
  };
}

function buildPublicLookupFailureHints(lookup, diagnostics, runtime) {
  const hints = [];
  const attempts = Array.isArray(diagnostics?.attempts) ? diagnostics.attempts : [];
  const hasDatabaseError = attempts.some((attempt) => {
    return attempt.source === "plate-resolution-cache" && attempt.status === "error";
  });
  const hasOpenDataError = attempts.some((attempt) => {
    return attempt.source === "open-data-db" && attempt.status === "error";
  });

  if (hasDatabaseError || hasOpenDataError) {
    hints.push("Lokální databáze s uloženými vazbami není dostupná. Zkuste to prosím znovu později.");
    return hints;
  }

  if (lookup.type === "unknown") {
    hints.push("Zkontrolujte formát: SPZ bez mezer, VIN má 17 znaků a IČO má 8 číslic.");
    return hints;
  }

  if (lookup.type === "plate") {
    hints.push("Zkontrolujte zadanou SPZ. Pokud je správně, záznam pro ni zatím nemáme v lokální databázi.");
    return hints;
  }

  if (lookup.type === "vin") {
    hints.push("Zkontrolujte zadaný VIN. Pokud je správně, záznam pro něj zatím nemáme v lokální databázi.");
    return hints;
  }

  if (lookup.type === "ico") {
    hints.push("Zkontrolujte zadané IČO. Pokud je správně, firma zatím nemá v importovaných datech dohledatelná vozidla.");
    return hints;
  }

  if (runtime?.transportProvider && runtime.transportProvider.configured === false) {
    hints.push("Vyhledávání je dostupné jen pro záznamy uložené v lokální databázi.");
  }

  return hints.length ? hints : ["Zkontrolujte zadanou hodnotu a zkuste vyhledávání zopakovat."];
}

function sanitizeDiagnosticsForClient(diagnostics) {
  if (!diagnostics || typeof diagnostics !== "object") {
    return diagnostics;
  }

  return {
    queryType: diagnostics.queryType || "unknown",
    attempts: sanitizeLookupAttemptsForClient(diagnostics.attempts),
    runtime: sanitizeRuntimeForClient(diagnostics.runtime)
  };
}

function sanitizeLookupAttemptsForClient(attempts) {
  return (Array.isArray(attempts) ? attempts : [])
    .filter((attempt) => !["pvzp-browser", "uniqa-browser"].includes(attempt?.source))
    .map((attempt) => ({
      source: sanitizePublicLookupSource(attempt?.source),
      status: attempt?.status || null,
      detail: sanitizePublicLookupDetail(attempt?.source, attempt?.status)
    }));
}

function sanitizeRuntimeForClient(runtime) {
  if (!runtime || typeof runtime !== "object") {
    return runtime;
  }

  return {
    platform: runtime.platform,
    nodeVersion: runtime.nodeVersion,
    mockDataEnabled: runtime.mockDataEnabled,
    transportProvider: {
      configured: Boolean(runtime.transportProvider?.configured)
    },
    officialVinApi: {
      configured: Boolean(runtime.officialVinApi?.configured)
    },
    openDataDatabase: runtime.openDataDatabase || null,
    warnings: sanitizePublicWarnings(runtime.warnings)
  };
}

function sanitizePublicLookupSource(source) {
  if (source === "plate-resolution-cache") {
    return "local-plate-cache";
  }
  if (source === "transport-cube") {
    return "primary-provider";
  }
  if (source === "open-data-db" || source === "open-data-db-resolved") {
    return "local-open-data-db";
  }
  if (source === "official-vin-api") {
    return "public-vin-api";
  }
  if (source === "vin-plate-resolution") {
    return "plate-resolver";
  }
  return source || "lookup";
}

function sanitizePublicLookupDetail(source, status) {
  if (status === "missing_config") {
    return "Zdroj není nakonfigurovaný.";
  }
  if (status === "error") {
    return source === "plate-resolution-cache" || source === "open-data-db"
      ? "Lokální databáze není dostupná."
      : "Zdroj teď není dostupný.";
  }
  if (status === "miss") {
    return "Záznam nebyl nalezen.";
  }
  if (status === "timeout") {
    return "Zdroj teď neodpověděl včas.";
  }
  return null;
}

function sanitizePublicWarnings(warnings) {
  return (Array.isArray(warnings) ? warnings : [])
    .filter((warning) => {
      const normalized = normalizeForMatch(warning);
      return !normalized.includes("pvzp") && !normalized.includes("uniqa");
    })
    .map((warning) => sanitizePublicText(warning))
    .filter(Boolean);
}

function sanitizePublicText(value) {
  return normalizeWhitespace(value)
    .replace(/\bPVZP_BROWSER_PATH\b/g, "BROWSER_PATH")
    .replace(/\bUNIQA_PHONE\b/g, "CONTACT_PHONE")
    .replace(/\bTRANSPORT_CUBE_LOOKUP_URL\b/g, "primární zdroj")
    .replace(/\bPVZP\b/gi, "externí zdroj")
    .replace(/\bUNIQA\b/gi, "externí zdroj")
    .replace(/\s+/g, " ")
    .trim();
}

async function lookupFromConfiguredProvider(lookup, diagnostics) {
  const endpoint = process.env.TRANSPORT_CUBE_LOOKUP_URL;

  if (!endpoint) {
    recordLookupAttempt(diagnostics, {
      source: "transport-cube",
      status: "missing_config",
      detail: "Chybí TRANSPORT_CUBE_LOOKUP_URL."
    });
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

  try {
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
      recordLookupAttempt(diagnostics, {
        source: "transport-cube",
        status: "miss",
        detail: "Provider nevrátil žádná data.",
        host: extractUrlHost(endpoint),
        method
      });
      return null;
    }

    const normalized = looksNormalized(response)
      ? mergeWithSource(response, providerLabel, providerNote)
      : normalizeGenericPayload(response, lookup, providerLabel, providerNote);

    recordLookupAttempt(diagnostics, {
      source: "transport-cube",
      status: normalized ? "success" : "miss",
      detail: normalized
        ? "Provider vrátil použitelná data."
        : "Provider vrátil data, ale nepodařilo se je znormalizovat.",
      host: extractUrlHost(endpoint),
      method
    });

    return normalized;
  } catch (error) {
    recordLookupAttempt(diagnostics, {
      source: "transport-cube",
      status: "error",
      detail: formatLookupError(error),
      host: extractUrlHost(endpoint),
      method
    });
    return null;
  }
}

async function lookupFromFleetDbByVin(lookup, diagnostics) {
  if (lookup.type !== "vin") {
    return null;
  }

  try {
    const pcv = await readFleetDbPcvByVin(lookup.compact);
    if (!pcv) {
      recordLookupAttempt(diagnostics, {
        source: "fleet-db",
        status: "miss",
        detail: "VIN nebyl nalezen v lokalnim flotilovem indexu."
      });
      return null;
    }

    const summary = await readFleetDbVehicleSummaryByPcv(pcv);
    if (!summary) {
      recordLookupAttempt(diagnostics, {
        source: "fleet-db",
        status: "miss",
        detail: "Flotilovy index nasel PCV, ale chybi souhrn vozidla."
      });
      return null;
    }

    const meta = await readJsonFile(FLEET_DB_META_FILE).catch(() => null);
    recordLookupAttempt(diagnostics, {
      source: "fleet-db",
      status: "success",
      detail: "VIN byl nalezen v lokalnim flotilovem indexu.",
      host: "local-jsonl"
    });

    return buildVehicleRecordFromOpenDataSummary(summary, {
      sourceFile: meta?.vehicleFilename || null,
      sourceUpdatedAt: meta?.vehicleDatasetDate || null
    });
  } catch (error) {
    recordLookupAttempt(diagnostics, {
      source: "fleet-db",
      status: "error",
      detail: formatLookupError(error),
      host: "local-jsonl"
    });
    return null;
  }
}

async function lookupFromOpenDataDatabase(lookup, diagnostics) {
  if (lookup.type === "plate") {
    return await lookupFromPlateResolutionCache(lookup, diagnostics);
  }

  if (lookup.type !== "vin") {
    recordLookupAttempt(diagnostics, {
      source: "open-data-db",
      status: "skipped",
      detail: "Lokální open-data DB podporuje přímý lookup podle VIN a uložených SPZ resolverů."
    });
    return null;
  }

  try {
    const payload = await openDataDb.queryVehicleByVin(lookup.compact);
    if (!payload?.summary) {
      recordLookupAttempt(diagnostics, {
        source: "open-data-db",
        status: "miss",
        detail: "VIN nebyl nalezen v lokálním open-data indexu."
      });
      return await lookupFromFleetDbByVin(lookup, diagnostics);
    }

    recordLookupAttempt(diagnostics, {
      source: "open-data-db",
      status: "success",
      detail: "VIN byl nalezen v lokálním open-data indexu.",
      host: "local-postgres"
    });
    return buildVehicleRecordFromOpenDataSummary(payload.summary, payload);
  } catch (error) {
    recordLookupAttempt(diagnostics, {
      source: "open-data-db",
      status: "error",
      detail: formatLookupError(error),
      host: "local-postgres"
    });
    return await lookupFromFleetDbByVin(lookup, diagnostics);
  }
}

async function lookupFromPlateResolutionCache(lookup, diagnostics) {
  try {
    const cached = await openDataDb.getCachedPlateResolution(lookup.compact);
    if (!cached) {
      recordLookupAttempt(diagnostics, {
        source: "plate-resolution-cache",
        status: "miss",
        detail: "SPZ není v lokální cache resolveru."
      });
      return null;
    }

    const payload = await queryOpenDataVehicleByIdentifiers({
      vin: cached.vin,
      pcv: cached.pcv
    });
    if (!payload?.summary) {
      recordLookupAttempt(diagnostics, {
        source: "plate-resolution-cache",
        status: "miss",
        detail: "Cache SPZ existuje, ale navázané VIN/PČV není v lokální DB."
      });
      return null;
    }

    recordLookupAttempt(diagnostics, {
      source: "plate-resolution-cache",
      status: "success",
      detail: "SPZ byla vyřešena z lokální cache na VIN/PČV.",
      host: "local-postgres"
    });

    return injectPlateIntoRecord(
      buildVehicleRecordFromOpenDataSummary(payload.summary, payload),
      cached.plate || lookup.compact
    );
  } catch (error) {
    recordLookupAttempt(diagnostics, {
      source: "plate-resolution-cache",
      status: "error",
      detail: formatLookupError(error),
      host: "local-postgres"
    });
    return null;
  }
}

async function lookupResolvedRecordFromOpenDataDatabase(lookup, seedRecord, diagnostics) {
  if (!seedRecord || !["plate", "vin"].includes(lookup.type)) {
    return null;
  }

  const vin = normalizeWhitespace(extractIdentifier(seedRecord, "VIN")).toUpperCase() || null;
  const pcv = normalizeWhitespace(extractIdentifier(seedRecord, "PČV") || extractIdentifier(seedRecord, "PCV")) || null;
  if (!vin && !pcv) {
    recordLookupAttempt(diagnostics, {
      source: "open-data-db-resolved",
      status: "skipped",
      detail: "Resolver nevrátil VIN ani PČV pro dohledání v lokální DB."
    });
    return null;
  }

  try {
    const payload = await queryOpenDataVehicleByIdentifiers({ vin, pcv });
    if (!payload?.summary) {
      recordLookupAttempt(diagnostics, {
        source: "open-data-db-resolved",
        status: "miss",
        detail: "VIN/PČV z resolveru nebylo nalezeno v lokální DB."
      });
      return null;
    }

    const resolvedPlate = lookup.type === "plate"
      ? lookup.compact
      : normalizeWhitespace(extractIdentifier(seedRecord, "SPZ")).toUpperCase();
    if (resolvedPlate) {
      const storedPlate = await openDataDb.storePlateResolution({
        plate: resolvedPlate,
        vin: payload.summary.vin || vin,
        pcv: payload.summary.pcv || pcv,
        source: seedRecord.source?.label || "SPZ resolver",
        confidence: 0.9,
        ttlMs: PLATE_RESOLUTION_TTL_MS
      }).catch(() => null);
      await invalidateIcoFleetCacheForVehicle(storedPlate?.pcv || payload.summary.pcv || pcv).catch(() => null);
    }

    recordLookupAttempt(diagnostics, {
      source: "open-data-db-resolved",
      status: "success",
      detail: "VIN/PČV z resolveru bylo napojeno na plný záznam z lokální DB.",
      host: "local-postgres"
    });

    const record = buildVehicleRecordFromOpenDataSummary(payload.summary, payload);
    return resolvedPlate ? injectPlateIntoRecord(record, resolvedPlate) : record;
  } catch (error) {
    recordLookupAttempt(diagnostics, {
      source: "open-data-db-resolved",
      status: "error",
      detail: formatLookupError(error),
      host: "local-postgres"
    });
    return null;
  }
}

async function queryOpenDataVehicleByIdentifiers({ vin, pcv }) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase();
  const normalizedPcv = normalizeWhitespace(pcv);
  if (normalizedVin) {
    const byVin = await openDataDb.queryVehicleByVin(normalizedVin).catch(() => null);
    if (byVin?.summary) {
      return byVin;
    }
  }
  if (normalizedPcv) {
    return await openDataDb.queryVehicleByPcv(normalizedPcv).catch(() => null);
  }
  return null;
}

function injectPlateIntoRecord(record, plate) {
  const normalizedPlate = normalizeWhitespace(plate).toUpperCase() || null;
  if (!record || !normalizedPlate) {
    return record;
  }

  const nextRecord = clone(record);
  nextRecord.highlights = upsertHighlight(nextRecord.highlights, "SPZ", normalizedPlate);
  nextRecord.sections = upsertSectionItems(nextRecord.sections, "Registrace", [item("SPZ", normalizedPlate)]);
  return nextRecord;
}

function buildVehicleRecordFromOpenDataSummary(summary, payload = {}) {
  const title = [summary.make, summary.model, summary.variant]
    .filter(Boolean)
    .join(" ")
    .trim() || summary.type || summary.vin || `Vozidlo ${summary.pcv}`;
  const firstRegistration = summary.firstRegistration || summary.firstRegistrationCz || null;
  const power = normalizeWhitespace(summary.power).replace(/\s*\/\s*$/, "");

  return {
    source: {
      mode: "open-data",
      label: "Open data DB",
      note: payload.sourceFile
        ? `Lokální index z datasetu ${payload.sourceFile}.`
        : "Lokální Postgres index otevřených dat."
    },
    hero: {
      badge: summary.status || "Registr vozidel",
      title,
      subtitle: "Záznam z lokální databáze otevřených dat napojený přes VIN/PČV.",
      status: summary.status || "Neuvedeno"
    },
    highlights: [
      item("SPZ", summary.plate),
      item("VIN", summary.vin),
      item("PČV", summary.pcv),
      item("První registrace", formatDate(firstRegistration)),
      item("Palivo", summary.fuel),
      item("Výkon", power ? `${power} kW` : null)
    ].filter(Boolean),
    ownership: {
      ownerCount: null,
      operatorCount: null,
      note: "Vlastnické a provozovatelské vazby se doplňují podle PČV z lokální databáze.",
      parties: []
    },
    sections: [
      createSection("Registrace", [
        item("SPZ", summary.plate),
        item("VIN", summary.vin),
        item("PČV", summary.pcv),
        item("První registrace", formatDate(summary.firstRegistration)),
        item("První registrace v ČR", formatDate(summary.firstRegistrationCz)),
        item("Status registru", summary.status)
      ]),
      createSection("Technické údaje", [
        item("Tovární značka", summary.make),
        item("Model", summary.model),
        item("Typ", summary.type),
        item("Varianta", summary.variant),
        item("Kategorie", summary.category),
        item("Palivo", summary.fuel),
        item("Výkon", power ? `${power} kW` : null),
        item("Barva", summary.color)
      ]),
      createSection("Rozměry a hmotnost", [
        item("Délka", formatVehicleMeasure(summary.lengthMm, "mm")),
        item("Šířka", formatVehicleMeasure(summary.widthMm, "mm")),
        item("Výška", formatVehicleMeasure(summary.heightMm, "mm")),
        item("Rozvor", formatVehicleMeasure(summary.wheelbaseMm, "mm")),
        item("Provozní hmotnost", formatVehicleMeasure(summary.weightKg, "kg"))
      ])
    ].filter(Boolean),
    timeline: buildTimeline({}, {
      firstRegistration: summary.firstRegistration,
      firstRegistrationCz: summary.firstRegistrationCz
    })
  };
}

async function persistSupplementalOwnershipSnapshot(record) {
  if (!record || !record.ownership || !Array.isArray(record.ownership.parties)) {
    return 0;
  }

  const pcv = normalizeWhitespace(extractIdentifier(record, "PČV"));
  const vin = normalizeWhitespace(extractIdentifier(record, "VIN")).toUpperCase();
  const plate = normalizeWhitespace(extractIdentifier(record, "SPZ")).toUpperCase() || null;
  if (!pcv || !vin) {
    return 0;
  }

  const relations = record.ownership.parties
    .filter((party) => party && party.type === "company" && sanitizeIco(party.ico))
    .map((party) => ({
      ico: party.ico,
      name: party.name,
      address: party.address,
      relation: party.role,
      current: party.current === undefined || party.current === null ? true : Boolean(party.current),
      since: party.since || extractPeriodStart(party.period),
      dateFrom: party.dateFrom || extractPeriodStart(party.period),
      dateTo: party.dateTo || extractPeriodEnd(party.period)
    }));

  if (relations.length === 0) {
    return 0;
  }

  const stored = await openDataDb.storeSupplementalOwnershipRelations({
    pcv,
    vin,
    plate,
    source: record.source?.label || "lookup",
    relations
  });
  invalidateIcoFleetCache(relations.map((relation) => relation.ico));
  return stored;
}

async function persistPlateResolutionSnapshot(lookup, record) {
  if (!lookup || !record) {
    return null;
  }

  const plate =
    lookup.type === "plate"
      ? lookup.compact
      : normalizeWhitespace(extractIdentifier(record, "SPZ")).toUpperCase();
  const vin = normalizeWhitespace(extractIdentifier(record, "VIN")).toUpperCase() || null;
  const pcv = normalizeWhitespace(extractIdentifier(record, "PČV") || extractIdentifier(record, "PCV")) || null;
  if (!plate || (!vin && !pcv)) {
    return null;
  }

  const storedPlate = await openDataDb.storePlateResolution({
    plate,
    vin,
    pcv,
    source: record.source?.label || "SPZ lookup",
    confidence: vin && pcv ? 0.95 : 0.8,
    ttlMs: PLATE_RESOLUTION_TTL_MS
  });
  await invalidateIcoFleetCacheForVehicle(storedPlate?.pcv || pcv).catch(() => null);
  return storedPlate;
}

async function lookupFromOfficialVinApi(lookup, diagnostics) {
  const apiKey = process.env.DATAOVOZIDLECH_API_KEY || process.env.RSV_PUBLIC_API_KEY;

  if (lookup.type !== "vin") {
    return null;
  }

  if (!apiKey) {
    recordLookupAttempt(diagnostics, {
      source: "official-vin-api",
      status: "missing_config",
      detail: "Chybí DATAOVOZIDLECH_API_KEY nebo RSV_PUBLIC_API_KEY."
    });
    return null;
  }

  try {
    const response = await requestJson(
      `https://api.dataovozidlech.cz/api/vehicletechnicaldata/v2?vin=${encodeURIComponent(lookup.compact)}`,
      {
        method: "GET",
        headers: {
          Accept: "application/json",
          api_key: apiKey
        },
        timeoutMs: 6000
      }
    );

    if (!response) {
      recordLookupAttempt(diagnostics, {
        source: "official-vin-api",
        status: "miss",
        detail: "Veřejné VIN API nevrátilo žádná data."
      });
      return null;
    }

    const normalized = normalizeGenericPayload(
      response,
      lookup,
      "Datová kostka - veřejná VIN API",
      "Technické údaje a počty vlastníků/provozovatelů jsou načtené z oficiálního veřejného VIN API. Jména subjektů tato veřejná API podle dostupné dokumentace neposkytují."
    );

    recordLookupAttempt(diagnostics, {
      source: "official-vin-api",
      status: normalized ? "success" : "miss",
      detail: normalized
        ? "Veřejné VIN API vrátilo použitelná data."
        : "Veřejné VIN API vrátilo odpověď, ale nepodařilo se ji znormalizovat."
    });

    return normalized;
  } catch (error) {
    recordLookupAttempt(diagnostics, {
      source: "official-vin-api",
      status: "error",
      detail: formatLookupError(error)
    });
    return null;
  }
}

async function lookupFromOfficialVinApiWithBudget(lookup, diagnostics, useFastBudget) {
  if (!useFastBudget || !FAST_LOOKUP_MODE) {
    return await lookupFromOfficialVinApi(lookup, diagnostics);
  }

  return await Promise.race([
    lookupFromOfficialVinApi(lookup, diagnostics),
    new Promise((resolve) => {
      setTimeout(() => resolve(null), 2500);
    })
  ]);
}

async function lookupFromPvzpBrowser(lookup, diagnostics) {
  if (!["vin", "plate"].includes(lookup.type)) {
    return null;
  }

  if (!PVZP_LOOKUP_ENABLED) {
    recordLookupAttempt(diagnostics, {
      source: "pvzp-browser",
      status: "skipped",
      detail: "Externí doplňkový zdroj je vypnutý."
    });
    return null;
  }

  if (!PVZP_BROWSER_PATH) {
    recordLookupAttempt(diagnostics, {
      source: "pvzp-browser",
      status: "missing_config",
      detail: "Nebyl nalezen browser pro externí doplňkový zdroj."
    });
    return null;
  }

  if (process.platform === "linux" && !PVZP_HEADLESS && !process.env.DISPLAY) {
    recordLookupAttempt(diagnostics, {
      source: "pvzp-browser",
      status: "missing_config",
      detail: "Na Linuxu chybí DISPLAY pro externí doplňkový zdroj."
    });
    return null;
  }

  const cached = getCachedPvzpRecord(lookup.compact);
  if (cached) {
    recordLookupAttempt(diagnostics, {
      source: "pvzp-browser",
      status: "success",
      detail: "Použita byla cache externího doplňkového zdroje."
    });
    return clone(cached);
  }

  const attempts = [
    { initialDelayMs: 700, typeDelayMs: 40, responseDelayMs: 1800 },
    { initialDelayMs: 1200, typeDelayMs: 80, responseDelayMs: 2600 }
  ];

  let lastError = null;
  for (const attempt of attempts) {
    try {
      const response = await executePvzpBrowserLookup(lookup, attempt);
      const record = normalizePvzpPayload(response, lookup);

      if (record) {
        setCachedPvzpRecord(lookup.compact, record);
        recordLookupAttempt(diagnostics, {
          source: "pvzp-browser",
          status: "success",
          detail: "Externí doplňkový zdroj vrátil použitelná data."
        });
        return clone(record);
      }
    } catch (error) {
      lastError = error;
      continue;
    }
  }

  recordLookupAttempt(diagnostics, {
    source: "pvzp-browser",
    status: lastError ? "error" : "miss",
    detail: lastError ? formatLookupError(lastError) : "Externí doplňkový zdroj nevrátil žádná data."
  });

  return null;
}

async function lookupFromUniqaBrowser(lookup, diagnostics) {
  if (!["vin", "plate"].includes(lookup.type)) {
    return null;
  }

  if (!UNIQA_LOOKUP_ENABLED) {
    recordLookupAttempt(diagnostics, {
      source: "uniqa-browser",
      status: "skipped",
      detail: "Externí doplňkový zdroj je vypnutý."
    });
    return null;
  }

  if (!UNIQA_PHONE) {
    recordLookupAttempt(diagnostics, {
      source: "uniqa-browser",
      status: "missing_config",
      detail: "Chybí kontaktní telefon pro externí doplňkový zdroj."
    });
    return null;
  }

  if (!UNIQA_BROWSER_PATH && !BROWSERLESS_ENABLED) {
    recordLookupAttempt(diagnostics, {
      source: "uniqa-browser",
      status: "missing_config",
      detail: "Nebyl nalezen browser ani Browserless endpoint pro externí doplňkový zdroj."
    });
    return null;
  }

  const cached = getCachedUniqaRecord(lookup.compact);
  if (cached) {
    recordLookupAttempt(diagnostics, {
      source: "uniqa-browser",
      status: "success",
      detail: "Použita byla cache externího doplňkového zdroje."
    });
    return clone(cached);
  }

  const attempts = [
    { initialDelayMs: 900, typeDelayMs: 45, submitDelayMs: 400, responseDelayMs: 3500 },
    { initialDelayMs: 1400, typeDelayMs: 85, submitDelayMs: 800, responseDelayMs: 5500 }
  ];

  let lastError = null;
  for (const attempt of attempts) {
    try {
      const response = await executeUniqaBrowserLookup(lookup, attempt);
      const record = normalizeUniqaPayload(response, lookup);

      if (record) {
        setCachedUniqaRecord(lookup.compact, record);
        recordLookupAttempt(diagnostics, {
          source: "uniqa-browser",
          status: "success",
          detail: "Externí doplňkový zdroj vrátil použitelný VIN."
        });
        return clone(record);
      }
    } catch (error) {
      lastError = error;
    }
  }

  recordLookupAttempt(diagnostics, {
    source: "uniqa-browser",
    status: lastError ? "error" : "miss",
    detail: lastError ? formatLookupError(lastError) : "Externí doplňkový zdroj nevrátil použitelný VIN."
  });

  return null;
}

async function executeUniqaBrowserLookup(lookup, attempt) {
  const browserSession = await createUniqaBrowserSession();
  const { browser, page } = browserSession;
  const debugSession = await createUniqaDebugSession(lookup, attempt);

  try {
    const responses = [];

    await applyUniqaStealth(page);
    await configureUniqaPage(page);

    page.on("response", async (response) => {
      if (!response.url().includes("/rest/public/v1/calculator/motor/vehicle")) {
        return;
      }

      try {
        const payload = JSON.parse(await response.text());
        responses.push(payload);
        await appendUniqaDebugResponse(debugSession, payload);
      } catch (error) {
        responses.push(null);
      }
    });

    await page.goto("https://www.uniqa.cz/online/pojisteni-vozidla/", {
      waitUntil: "domcontentloaded",
      timeout: 60000
    });
    await page.waitForTimeout(attempt.initialDelayMs);
    await captureUniqaDebugSnapshot(page, debugSession, "01-loaded");

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
    await captureUniqaDebugSnapshot(page, debugSession, "02-filled");
    await page.getByRole("button", { name: "Vyhledat vozidlo" }).click();
    await captureUniqaDebugPostSubmit(page, debugSession, attempt.responseDelayMs);

    const successfulResponse = findSuccessfulUniqaResponse(responses);
    if (successfulResponse) {
      await finalizeUniqaDebugSession(debugSession, "success", {
        responseCount: responses.length
      });
      return successfulResponse;
    }

    const domPayload = await extractUniqaVehicleDataFromDom(page, lookup);
    if (domPayload?.vin) {
      await finalizeUniqaDebugSession(debugSession, "success", {
        responseCount: responses.length,
        source: "dom"
      });
      return domPayload;
    }

    await finalizeUniqaDebugSession(debugSession, "miss", {
      responseCount: responses.length,
      lastResponse: responses.length > 0 ? responses[responses.length - 1] : null
    });
    return null;
  } catch (error) {
    if (debugSession) {
      await finalizeUniqaDebugSession(debugSession, "error", {
        error: formatLookupError(error)
      }).catch(() => {});
      error.uniqaDebugId = debugSession.id;
      error.message = `${error.message} [debugId:${debugSession.id}]`;
    }
    throw error;
  } finally {
    await browser.close().catch(() => {});
  }
}

async function createUniqaBrowserSession() {
  const { chromium } = require("playwright-core");

  if (BROWSERLESS_ENABLED) {
    const browser = await chromium.connectOverCDP(getBrowserlessWebSocketUrl());
    const context =
      browser.contexts()[0] ||
      (typeof browser.newContext === "function"
        ? await browser.newContext({
            locale: "cs-CZ",
            viewport: { width: 1366, height: 900 }
          })
        : null);

    if (!context) {
      throw new Error("Browserless nepripravil pouzitelny browser context.");
    }

    const page = context.pages()[0] || (await context.newPage());
    return { browser, page };
  }

  const browser = await chromium.launch(buildUniqaLaunchOptions());
  const page = await browser.newPage({
    locale: "cs-CZ",
    viewport: { width: 1366, height: 900 },
    userAgent: UNIQA_USER_AGENT
  });
  return { browser, page };
}

function getBrowserlessWebSocketUrl() {
  return BROWSERLESS_WS_URL;
}

function buildUniqaLaunchOptions() {
  const args = ["--window-size=1366,900", "--disable-blink-features=AutomationControlled"];

  if (process.platform === "win32") {
    args.push("--start-minimized", "--window-position=-32000,-32000");
  }

  if (process.platform === "linux") {
    args.push("--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage");
  }

  return {
    executablePath: UNIQA_BROWSER_PATH || undefined,
    headless: PVZP_HEADLESS,
    args
  };
}

async function applyUniqaStealth(page) {
  return applyBrowserStealth(page);
}

async function configureUniqaPage(page) {
  if (!page) {
    return;
  }

  try {
    await page.setViewportSize({ width: 1366, height: 900 });
  } catch (error) {}

  try {
    await page.setExtraHTTPHeaders({
      "Accept-Language": "cs-CZ,cs;q=0.9,en-US;q=0.8,en;q=0.7"
    });
  } catch (error) {}

  try {
    const cdp = await page.context().newCDPSession(page);
    await cdp.send("Network.setUserAgentOverride", {
      userAgent: UNIQA_USER_AGENT,
      acceptLanguage: "cs-CZ,cs;q=0.9,en-US;q=0.8,en;q=0.7",
      platform: "Windows"
    });
  } catch (error) {}
}

async function createUniqaDebugSession() {
  return null;
}

async function appendUniqaDebugResponse() {}

async function captureUniqaDebugSnapshot() {}

async function captureUniqaDebugPostSubmit(page, debugSession, responseDelayMs) {
  await page.waitForTimeout(responseDelayMs);
  await captureUniqaDebugSnapshot(page, debugSession, "03-submitted");
}

async function finalizeUniqaDebugSession() {}

function findSuccessfulUniqaResponse(responses) {
  return (responses || []).find((payload) => Boolean(extractVinFromAny(payload))) || null;
}

function normalizeUniqaPayload(payload, lookup) {
  const vin = extractVinFromAny(payload);

  if (!vin) {
    return null;
  }

  return normalizeGenericPayload(
    {
      response: payload,
      vin,
      plateNumber: lookup.type === "plate" ? lookup.compact : extractPlateFromAny(payload),
      brand: extractFirstStringByKey(payload, ["brand", "make", "manufacturer", "znacka"]),
      model: extractFirstStringByKey(payload, ["model", "modelName"]),
      fuel: extractFirstStringByKey(payload, ["fuel", "palivo"]),
      status: "Doplněno z UNIQA"
    },
    lookup,
    "UNIQA kalkulačka",
    "Reverzní SPZ/VIN doplnění bylo načteno z veřejného formuláře UNIQA. Telefonní číslo pro volání formuláře musí být nastavené provozovatelem."
  );
}

async function extractUniqaVehicleDataFromDom(page, lookup) {
  try {
    const toggle = page.locator('xpath=//*[@id="vehicleDetail-section-0"]/div[2]/div[1]/div[2]/div/span/svg').first();
    if (await toggle.count()) {
      await toggle.click({ force: true }).catch(() => {});
      await page.waitForTimeout(500);
    }
  } catch (error) {}

  return await page.evaluate((query) => {
    const vinPattern = /\b[A-HJ-NPR-Z0-9]{17}\b/i;
    const inputValue = (selector) => document.querySelector(selector)?.value?.trim() || null;
    const text = document.body?.innerText || "";
    const vin = inputValue("#vinId") || inputValue('input[name*="vin" i]') || text.match(vinPattern)?.[0] || null;

    if (!vin) {
      return null;
    }

    return {
      vin,
      plateNumber: query?.type === "plate" ? query.compact : inputValue("#ecvId"),
      status: "Doplněno z UNIQA"
    };
  }, { type: lookup.type, compact: lookup.compact });
}

function extractVinFromAny(value) {
  const match = findPatternInAny(value, /\b[A-HJ-NPR-Z0-9]{17}\b/i);
  return match ? match.toUpperCase() : null;
}

function extractPlateFromAny(value) {
  return extractFirstStringByKey(value, ["plate", "plateNumber", "registrationPlateNumber", "ecv", "spz", "rz"]);
}

function extractFirstStringByKey(value, keys, seen = new Set()) {
  if (!value || typeof value !== "object" || seen.has(value)) {
    return null;
  }

  seen.add(value);
  const normalizedKeys = new Set(keys.map((key) => normalizeForMatch(key)));

  if (Array.isArray(value)) {
    for (const item of value) {
      const nested = extractFirstStringByKey(item, keys, seen);
      if (nested) {
        return nested;
      }
    }
    return null;
  }

  for (const [key, entry] of Object.entries(value)) {
    if (normalizedKeys.has(normalizeForMatch(key)) && typeof entry === "string" && normalizeWhitespace(entry)) {
      return normalizeWhitespace(entry);
    }
  }

  for (const entry of Object.values(value)) {
    const nested = extractFirstStringByKey(entry, keys, seen);
    if (nested) {
      return nested;
    }
  }

  return null;
}

function findPatternInAny(value, pattern, seen = new Set()) {
  if (typeof value === "string") {
    return value.match(pattern)?.[0] || null;
  }

  if (!value || typeof value !== "object" || seen.has(value)) {
    return null;
  }

  seen.add(value);
  const entries = Array.isArray(value) ? value : Object.values(value);

  for (const entry of entries) {
    const match = findPatternInAny(entry, pattern, seen);
    if (match) {
      return match;
    }
  }

  return null;
}

function resolveSupplementalVinLookup(originalLookup, record) {
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

async function executePvzpBrowserLookup(lookup, attempt) {
  const browserSession = await createPvzpBrowserSession();
  const { browser, page } = browserSession;

  try {
    await applyBrowserStealth(page);
    await configurePvzpPage(page);

    await page.goto("https://online.pvzp.cz/clfe/motor/#/policy", {
      waitUntil: "domcontentloaded",
      timeout: 60000
    });
    await page.waitForTimeout(attempt.initialDelayMs);

    if (lookup.type === "vin") {
      await page.locator('input[formcontrolname="vin"]').fill(lookup.compact);
    } else {
      await page.locator('input[formcontrolname="registrationPlateNumber"]').fill(lookup.compact);
    }
    await page.waitForTimeout(Math.max(600, attempt.typeDelayMs));
    await page.locator('input[type="button"]').first().click({ force: true });
    await waitForPvzpAutofill(page, lookup, attempt.responseDelayMs);

    return await extractPvzpVehicleData(page, lookup);
  } finally {
    await browser.close().catch(() => {});
  }
}

async function createPvzpBrowserSession() {
  const { chromium } = require("playwright-core");
  const browser = await chromium.launch({
    executablePath: PVZP_BROWSER_PATH || undefined,
    headless: PVZP_HEADLESS,
    args: buildPvzpLaunchOptions()
  });
  const page = await browser.newPage({
    locale: "cs-CZ",
    viewport: { width: 1440, height: 1200 },
    userAgent: PVZP_USER_AGENT
  });
  return { browser, page };
}

async function configurePvzpPage(page) {
  if (!page) {
    return;
  }

  try {
    await page.setViewportSize({ width: 1440, height: 1200 });
  } catch (error) {}

  try {
    await page.setExtraHTTPHeaders({
      "Accept-Language": "cs-CZ,cs;q=0.9,en-US;q=0.8,en;q=0.7"
    });
  } catch (error) {}

  try {
    const cdp = await page.context().newCDPSession(page);
    await cdp.send("Network.setUserAgentOverride", {
      userAgent: PVZP_USER_AGENT,
      acceptLanguage: "cs-CZ,cs;q=0.9,en-US;q=0.8,en;q=0.7",
      platform: "Windows"
    });
  } catch (error) {}
}

async function extractPvzpVehicleData(page, lookup) {
  const result = await page.evaluate(() => {
    const valueOf = (selector) => document.querySelector(selector)?.value?.trim() || null;
    const selectedText = (selector) => {
      const el = document.querySelector(selector);
      if (!el || el.tagName !== "SELECT") {
        return null;
      }
      return el.options[el.selectedIndex]?.text?.trim() || null;
    };

    return {
      registrationPlateNumber: valueOf('input[formcontrolname="registrationPlateNumber"]'),
      vin: valueOf('input[formcontrolname="vin"]'),
      vehicleType: selectedText('select[formcontrolname="vehicleType"]'),
      usage: selectedText('select[formcontrolname="vehicleUsage"]') || selectedText('select[formcontrolname="usage"]')
    };
  });

  if (lookup.type === "vin") {
    return result?.registrationPlateNumber ? result : null;
  }

  return result?.vin ? result : null;
}

async function waitForPvzpAutofill(page, lookup, timeoutMs) {
  const waitForPlate = lookup?.type === "vin";
  try {
    await page.waitForFunction(
      ({ waitForPlate }) => {
        const selector = waitForPlate
          ? 'input[formcontrolname="registrationPlateNumber"]'
          : 'input[formcontrolname="vin"]';
        const input = document.querySelector(selector);
        const value = input && input.value ? input.value.trim() : "";
        return waitForPlate ? value.length >= 5 : value.length >= 17;
      },
      { waitForPlate },
      { timeout: timeoutMs }
    );
  } catch (error) {
    await page.waitForTimeout(Math.min(600, timeoutMs));
  }
}

function normalizePvzpPayload(payload, lookup) {
  if (!payload?.vin) {
    return null;
  }

  return normalizeGenericPayload(
    {
      plateNumber: payload.registrationPlateNumber || (lookup.type === "plate" ? lookup.compact : null),
      vin: payload.vin,
      category: payload.vehicleType,
      status: "Doplněno z registru"
    },
    lookup,
    "Doplňkový zdroj",
    "Identifikace vozidla byla doplněna z veřejně dostupného zdroje."
  );
}

async function lookupOwnershipFromOpenData(originalLookup, record) {
  if (!record || typeof record !== "object") {
    return null;
  }

  await ensureOpenDataPersistentCachesLoaded();

  const vin = normalizeWhitespace(extractIdentifier(record, "VIN")).toUpperCase() || null;
  let pcv = normalizeWhitespace(extractIdentifier(record, "PČV")) || null;

  if (!pcv && vin) {
    pcv = getPersistentPcv(vin) || (await resolveIndexedPcvForVin(vin)) || null;
  }

  if (!pcv && vin) {
    try {
      pcv = await resolvePcvForVin(vin);
      if (pcv) {
        await storePersistentPcv(vin, pcv);
      }
    } catch (error) {
      pcv = null;
    }
  }

  if (!pcv) {
    return null;
  }

  const dataset = await ensureOpenDataDatasetLocal("owners", OPEN_DATA_OWNER_ROUTE);
  const relations = await findOpenDataRowsByPcv(dataset.localPath, pcv, normalizeCompanyVehicleRelation);
  const legalParties = uniqueParties(await resolveMissingCompanyIcosInParties(relations
    .filter((relation) => relation?.current)
    .map(mapOwnershipRelationToParty)
    .filter(isLegalEntityParty)));

  if (legalParties.length === 0) {
    return null;
  }

  return {
    source: {
      mode: "live",
      label: "RSV vlastníci/provozovatelé",
      note: "Právnické osoby jsou doplněné z otevřené sady vlastník/provozovatel vozidla."
    },
    hero: {
      badge: legalParties.some((party) => party.type === "company") ? "Právnická osoba" : record.hero?.badge || "Bez rozlišení",
      title: record.hero?.title || `Vozidlo ${vin || pcv || originalLookup.compact}`,
      subtitle: record.hero?.subtitle || "Historie vlastníků a provozovatelů z otevřených dat.",
      status: record.hero?.status || "Neuvedeno"
    },
    highlights: [],
    ownership: {
      ownerCount: countRole(legalParties, "vlast") || null,
      operatorCount: countRole(legalParties, "provoz") || null,
      note: "Fyzické osoby jsou ve veřejných datech anonymizované.",
      parties: legalParties
    },
    sections: [],
    timeline: []
  };
}

async function applyBrowserStealth(page) {
  await page.addInitScript(() => {
    Object.defineProperty(navigator, "webdriver", {
      get: () => undefined
    });
    Object.defineProperty(navigator, "languages", {
      get: () => ["cs-CZ", "cs", "en-US", "en"]
    });
    Object.defineProperty(navigator, "plugins", {
      get: () => [1, 2, 3, 4]
    });
  });
}


async function lookupOwnershipFromHlidacStatu(lookup) {
  if (lookup.type !== "vin") {
    return null;
  }

  const url = `https://www.hlidacstatu.cz/vozidla/VIN?ID=${encodeURIComponent(lookup.compact)}`;
  const headers = {
    Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    Pragma: "no-cache",
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
  };
  const html = await requestHtml(url, { headers, timeoutMs: 15000 });

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

    const canonicalHeaders = headers.map(canonicalizeCsvHeader);
    const isCanonicalOwnershipTable =
      canonicalHeaders.length >= 5 &&
      canonicalHeaders.some((header) => header === "VZTAH") &&
      canonicalHeaders.some((header) => header === "SUBJEKT") &&
      canonicalHeaders.some((header) => header === "ICO");

    if (!isOwnershipTable && !isCanonicalOwnershipTable) {
      return;
    }

    $(table)
      .find("tbody tr")
      .each((__, row) => {
        const cells = $(row).find("td");

        if (cells.length < 5) {
          return;
        }

        const role = normalizeVehicleRelation($(cells[0]).text());
        const name = normalizeWhitespace($(cells[1]).text());
        const ico = sanitizeIco(normalizeWhitespace($(cells[2]).text()));
        const address = normalizeWhitespace($(cells[3]).text());
        const period = normalizeWhitespace($(cells[4]).text());

        if (!name && !ico) {
          return;
        }

        parties.push({
          role: role || "Subjekt",
          type: ico || looksLikeCompanyName(name) ? "company" : "unknown",
          name: name || null,
          ico,
          address: address || null,
          period: period || null,
          since: extractPeriodStart(period),
          current: !period || /-\s*$/.test(period)
        });
      });
  });

  if (parties.length === 0) {
    return null;
  }

  const legalParties = uniqueParties(parties.filter(isLegalEntityParty));
  if (legalParties.length === 0) {
    return null;
  }

  return {
    source: {
      mode: "live",
      label: "Hlídač státu",
      note: "Historie právnických vlastníků a provozovatelů je k dispozici u právnických osob."
    },
    hero: {
      badge: "Právnická osoba",
      title: `Vozidlo ${lookup.compact}`,
      subtitle: "Historie vlastníků a provozovatelů z veřejného webového přehledu.",
      status: "Doplněné vztahy"
    },
    highlights: [],
    ownership: {
      ownerCount: countRole(legalParties, "vlast"),
      operatorCount: countRole(legalParties, "provoz"),
      note: "Z veřejného VIN fallbacku jsou vypsané pouze právnické osoby s IČO; anonymní subjekty nejsou zobrazeny.",
      parties: legalParties
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
      label: `${baseRecord.source?.label || "Zdroj"} + ${ownershipRecord.source?.label || "Hlídač státu"}`,
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
          ? "Právnická osoba"
          : baseRecord.hero?.badge || "Bez rozlišení"
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
      badge: firstNonEmpty([baseRecord.hero?.badge, supplementRecord.hero?.badge]) || "Bez rozlišení",
      title: firstNonEmpty([baseRecord.hero?.title, supplementRecord.hero?.title]) || "Vozidlo",
      subtitle:
        firstNonEmpty([baseRecord.hero?.subtitle, supplementRecord.hero?.subtitle]) ||
        "Strukturovaný výstup připravený pro interní ověřování vozidel i další napojení.",
      status: firstNonEmpty([baseRecord.hero?.status, supplementRecord.hero?.status]) || "Neuvedeno"
    },
    highlights: mergeHighlights(baseHighlights, supplementHighlights),
    sections: mergeSections(baseSections, supplementSections),
    timeline: mergeTimeline(baseTimeline, supplementTimeline),
    inspectionHints: mergeInspectionHints(baseRecord.inspectionHints, supplementRecord.inspectionHints),
    ownership: {
      ...supplementRecord.ownership,
      ...baseRecord.ownership,
      note: joinUniqueText([baseRecord.ownership?.note, supplementRecord.ownership?.note], " ")
    }
  };
}

function mergeInspectionHints(primary, supplemental) {
  const seen = new Set();
  return [...(Array.isArray(primary) ? primary : []), ...(Array.isArray(supplemental) ? supplemental : [])]
    .filter((hint) => {
      const date = normalizeInspectionDateKey(hint?.date || hint?.validFrom);
      if (!date) {
        return false;
      }

      const key = `${date}:${normalizeForMatch(hint?.type || hint?.label)}`;
      if (seen.has(key)) {
        return false;
      }

      seen.add(key);
      return true;
    })
    .map((hint) => ({
      ...hint,
      date: normalizeOpenDataDate(hint.date || hint.validFrom)
    }));
}

function hasInspectionValiditySignal(record) {
  return Boolean(extractRecordItemValue(record, [
    "STK do",
    "STK platna do",
    "Pravidelna technicka prohlidka do"
  ]));
}

function hasVehicleDimensionsSignal(record) {
  return Boolean(extractRecordItemValue(record, [
    "Delka",
    "Sirka",
    "Vyska",
    "Rozvor",
    "Provozni hmotnost"
  ]));
}

function extractRecordItemValue(record, labels) {
  if (!record || !Array.isArray(labels) || labels.length === 0) {
    return null;
  }

  const normalizedLabels = new Set(labels.map(normalizeForMatch).filter(Boolean));
  const items = [
    ...(Array.isArray(record.highlights) ? record.highlights : []),
    ...(Array.isArray(record.sections)
      ? record.sections.flatMap((section) => Array.isArray(section?.items) ? section.items : [])
      : [])
  ];

  const match = items.find((item) => {
    const label = normalizeForMatch(item?.label);
    const value = normalizeWhitespace(item?.value);
    return label && normalizedLabels.has(label) && value && value !== "-";
  });

  return normalizeWhitespace(match?.value) || null;
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
      if (party.type !== "company") {
        return party;
      }

      try {
        const resolvedIco = party.ico || await resolveCompanyIcoFromAresByName(party);
        if (!resolvedIco) {
          return party;
        }

        const company = await fetchCompanyFromAres(resolvedIco);
        if (!company) {
          return {
            ...party,
            ico: party.ico || resolvedIco
          };
        }

        return {
          ...party,
          ico: party.ico || resolvedIco,
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

  const cachedCompany = await openDataDb.getCachedAresCompany(ico).catch(() => null);
  if (cachedCompany) {
    return {
      name: cachedCompany.name || null,
      address: cachedCompany.address || null
    };
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

  const company = {
    name: response.obchodniJmeno || null,
    address: formatAresAddress(response.sidlo)
  };
  await openDataDb.storeCachedAresCompany(ico, company).catch(() => null);
  return company;
}

async function resolveCompanyIcoFromAresByName(party) {
  const name = normalizeWhitespace(party?.name);
  const address = normalizeWhitespace(party?.address);
  if (!name || isGenericOwnershipName(name)) {
    return null;
  }

  const cacheKey = `${normalizeForMatch(name)}|${normalizeForMatch(address)}`;
  if (ARES_NAME_LOOKUP_CACHE.has(cacheKey)) {
    return ARES_NAME_LOOKUP_CACHE.get(cacheKey);
  }

  const candidates = await searchCompaniesInAresByName(name).catch(() => []);
  const scored = candidates
    .map((candidate) => ({
      ...candidate,
      score: scoreAresCompanyCandidate({ name, address }, candidate)
    }))
    .filter((candidate) => candidate.ico && candidate.score >= 4)
    .sort((left, right) => right.score - left.score);

  const best = scored[0] || null;
  const second = scored[1] || null;
  const resolvedIco = best && (!second || best.score > second.score) ? best.ico : null;
  ARES_NAME_LOOKUP_CACHE.set(cacheKey, resolvedIco);
  return resolvedIco;
}

async function searchCompaniesInAresByName(name) {
  const response = await requestJson(
    "https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/vyhledat",
    {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json; charset=utf-8"
      },
      timeoutMs: 12000,
      body: JSON.stringify({
        obchodniJmeno: name,
        pocet: 10,
        start: 0
      })
    }
  );

  const items = Array.isArray(response?.ekonomickeSubjekty)
    ? response.ekonomickeSubjekty
    : Array.isArray(response?.seznam)
      ? response.seznam
      : [];

  return items
    .map((item) => ({
      ico: sanitizeIco(item.ico),
      name: normalizeWhitespace(item.obchodniJmeno || item.nazev || item.name),
      address: formatAresAddress(item.sidlo) || normalizeWhitespace(item.adresa || item.textovaAdresa)
    }))
    .filter((item) => item.ico && item.name);
}

function scoreAresCompanyCandidate(target, candidate) {
  const targetName = normalizeCompanyNameForMatch(target.name);
  const candidateName = normalizeCompanyNameForMatch(candidate.name);
  if (!targetName || !candidateName) {
    return 0;
  }

  let score = 0;
  if (targetName === candidateName) {
    score += 4;
  } else if (targetName.includes(candidateName) || candidateName.includes(targetName)) {
    score += 2;
  }

  if (!target.address) {
    return score === 4 ? score : 0;
  }

  const addressScore = scoreAddressMatch(target.address, candidate.address);
  return addressScore > 0 ? score + addressScore : 0;
}

function scoreAddressMatch(left, right) {
  const normalizedLeft = normalizeForMatch(left);
  const normalizedRight = normalizeForMatch(right);
  if (!normalizedLeft || !normalizedRight) {
    return 0;
  }

  if (normalizedLeft === normalizedRight) {
    return 4;
  }

  if (normalizedLeft.includes(normalizedRight) || normalizedRight.includes(normalizedLeft)) {
    return 3;
  }

  const leftTokens = new Set(normalizedLeft.split(" ").filter((token) => token.length > 2));
  const rightTokens = new Set(normalizedRight.split(" ").filter((token) => token.length > 2));
  const overlap = Array.from(leftTokens).filter((token) => rightTokens.has(token)).length;
  return overlap >= 3 ? 2 : 0;
}

function normalizeCompanyNameForMatch(value) {
  return normalizeForMatch(value)
    .replace(/\b(s r o|spol s r o|a s|akc spol|akciova spolecnost|spolecnost s rucenim omezenym)\b/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function isGenericOwnershipName(value) {
  const normalized = normalizeForMatch(value);
  return !normalized || normalized.includes("anonymizovan") || normalized.includes("fyzicka osoba");
}

async function resolveCompanyDetailsForIco(ico, relations = []) {
  const normalizedIco = sanitizeIco(ico);
  const companyRelations = Array.isArray(relations)
    ? relations.filter((relation) => relationBelongsToIco(relation, normalizedIco))
    : [];
  const localName = firstNonEmpty(companyRelations.map((relation) => relation.name));
  const localAddress = firstNonEmpty(companyRelations.map((relation) => relation.address));

  if (localName && localAddress) {
    return { name: localName, address: localAddress };
  }

  const aresCompany = await fetchCompanyFromAres(normalizedIco).catch(() => null);
  return {
    name: aresCompany?.name || localName || null,
    address: aresCompany?.address || localAddress || null
  };
}

async function resolveMissingCompanyIcosInParties(parties) {
  if (!ARES_ENABLED || !Array.isArray(parties) || parties.length === 0) {
    return parties;
  }

  return await Promise.all(
    parties.map(async (party) => {
      if (!party || party.type !== "company" || party.ico) {
        return party;
      }

      const ico = await resolveCompanyIcoFromAresByName(party).catch(() => null);
      return ico ? { ...party, ico } : party;
    })
  );
}

async function resolveMissingCompanyIcosInOwnershipRecord(record) {
  if (!record?.ownership || !Array.isArray(record.ownership.parties)) {
    return record;
  }

  const parties = await resolveMissingCompanyIcosInParties(record.ownership.parties);
  return {
    ...record,
    ownership: {
      ...record.ownership,
      parties
    }
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
  const overallDimensions = parseVehicleDimensionTriplet(pickFirstString(data, [
    "Rozmery",
    "Celková délka/šířka/výška [mm]",
    "Celková délka/šířka/výška",
    "vehicle.dimensions",
    "vozidlo.rozmery"
  ]));
  const lengthMm = normalizeVehicleMeasure(pickFirstString(data, [
    "RozmeryDelka",
    "Delka",
    "lengthMm",
    "vehicle.lengthMm",
    "vozidlo.delka"
  ]) || overallDimensions[0]);
  const widthMm = normalizeVehicleMeasure(pickFirstString(data, [
    "RozmerySirka",
    "Sirka",
    "widthMm",
    "vehicle.widthMm",
    "vozidlo.sirka"
  ]) || overallDimensions[1]);
  const heightMm = normalizeVehicleMeasure(pickFirstString(data, [
    "RozmeryVyska",
    "Vyska",
    "heightMm",
    "vehicle.heightMm",
    "vozidlo.vyska"
  ]) || overallDimensions[2]);
  const wheelbaseMm = normalizeVehicleMeasure(pickFirstString(data, [
    "RozmeryRozvor",
    "Rozvor",
    "wheelbaseMm",
    "vehicle.wheelbaseMm",
    "vozidlo.rozvor"
  ]));
  const weightKg = normalizeVehicleMeasure(pickFirstString(data, [
    "HmotnostiProvozni",
    "ProvozniHmotnost",
    "weightKg",
    "vehicle.weightKg",
    "vozidlo.provozniHmotnost"
  ]));
  const inspectionUntil = pickFirstString(data, [
    "PravidelnaTechnickaProhlidkaDo",
    "inspectionUntil",
    "stkUntil",
    "vehicle.inspectionUntil",
    "vozidlo.stkPlatnaDo",
    "vozidlo.technickaProhlidkaDo"
  ]);
  const inspectionHints = buildGenericInspectionHints(data);
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
    ? "Právnická osoba"
    : parties.some((party) => party.type === "person")
      ? "Fyzická osoba"
      : "Bez rozlišení";

  const sections = [
    createSection("Registrace", [
      item("Kategorie", category),
      item("Status registru", status),
      item("První registrace", formatDate(firstRegistration)),
      item("První registrace v ČR", formatDate(firstRegistrationCz)),
      item("SPZ", plate),
      item("VIN", vin),
      item("PČV", pcv)
    ]),
    createSection("Technické údaje", [
      item("Palivo", fuel),
      item("Výkon", power ? `${power} kW` : null),
      item("Zdvihový objem", engineCapacity ? `${engineCapacity} cm3` : null),
      item("Barva", color),
      item("Převodovka", gearbox),
      item("Počet míst", seats)
    ]),
    createSection("Rozměry a hmotnost", [
      item("Délka", formatVehicleMeasure(lengthMm, "mm")),
      item("Šířka", formatVehicleMeasure(widthMm, "mm")),
      item("Výška", formatVehicleMeasure(heightMm, "mm")),
      item("Rozvor", formatVehicleMeasure(wheelbaseMm, "mm")),
      item("Provozní hmotnost", formatVehicleMeasure(weightKg, "kg"))
    ]),
    createSection("Kontroly a omezení", [
      item("STK platná do", formatDate(inspectionUntil), inspectionUntil ? "positive" : null),
      item("Emise platné do", formatDate(emissionsUntil), emissionsUntil ? "positive" : null),
      item("Odcizení", normalizeBinaryState(stolen)),
      item("Zástavní právo", normalizeBinaryState(lien))
    ])
  ].filter(Boolean);

  const highlights = [
    item("SPZ", plate),
    item("VIN", vin),
    item("PČV", pcv),
    item("První registrace", formatDate(firstRegistration || firstRegistrationCz)),
    item("Palivo", fuel),
    item("Výkon", power ? `${power} kW` : null),
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
      subtitle: "Strukturovaný výstup připravený pro interní ověřování vozidel i další napojení.",
      status: status || "Neuvedeno"
    },
    highlights,
    inspectionHints,
    ownership: {
      ownerCount: ownerCount || null,
      operatorCount: operatorCount || null,
      note:
        parties.length > 0
          ? "Právnické osoby mohou být doplněny včetně IČO a adresy."
          : ownerCount || operatorCount
            ? "U některých vozidel nemusí být identita vlastníků a provozovatelů veřejně dostupná."
          : "Detaily vlastnictví nejsou pro tento dotaz k dispozici.",
      parties
    },
    sections,
    timeline
  };
}

function buildGenericInspectionHints(data) {
  return [
    {
      type: "P - Pravidelná",
      label: "Pravidelná technická prohlídka",
      date: null,
      validUntil: pickFirstString(data, [
        "PravidelnaTechnickaProhlidkaDo",
        "inspectionUntil",
        "stkUntil",
        "vehicle.inspectionUntil",
        "vozidlo.stkPlatnaDo",
        "vozidlo.technickaProhlidkaDo"
      ])
    },
    {
      type: "Před registrací",
      label: "Prohlídka před registrací",
      date: pickFirstString(data, [
        "PredRegistraciProhlidkaDne",
        "beforeRegistrationInspectionDate",
        "vehicle.beforeRegistrationInspectionDate",
        "vozidlo.prohlidkaPredRegistraciDne"
      ])
    },
    {
      type: "Před schválením technické způsobilosti",
      label: "Prohlídka před schválením",
      date: pickFirstString(data, [
        "PredSchvalenimProhlidkaDne",
        "beforeApprovalInspectionDate",
        "vehicle.beforeApprovalInspectionDate",
        "vozidlo.prohlidkaPredSchvalenimDne"
      ])
    },
    {
      type: "E - Evidenční",
      label: "Evidenční prohlídka",
      date: pickFirstString(data, [
        "EvidencniProhlidkaDne",
        "evidenceInspectionDate",
        "vehicle.evidenceInspectionDate",
        "vozidlo.evidencniProhlidkaDne"
      ])
    },
    {
      type: "Historické vozidlo",
      label: "Prohlídka historického vozidla",
      date: pickFirstString(data, [
        "HistorickeVozidloProhlidkaDne",
        "historicVehicleInspectionDate",
        "vehicle.historicVehicleInspectionDate",
        "vozidlo.historickeVozidloProhlidkaDne"
      ])
    }
  ]
    .map((hint) => ({
      ...hint,
      date: normalizeOpenDataDate(hint.date),
      validUntil: normalizeOpenDataDate(hint.validUntil)
    }))
    .filter((hint) => hint.date || hint.validUntil);
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
      const inspections = await enrichInspectionsWithMileage(null, vin, record);
      return inspections ? mergeInspectionData(record, inspections) : record;
    }

    const inspections = withResolvedInspectionPcv(
      await enrichInspectionsWithMileage(await lookupTechnicalInspectionsByPcv(pcv), vin, record),
      pcv
    );
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

async function enrichInspectionsWithOpenDataLab(inspections, vin) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase();
  if (!normalizedVin || !OPENDATALAB_STK_LOOKUP_ENABLED || !OPENDATALAB_STK_API_URL) {
    return inspections;
  }

  const odometerRecords = await lookupOpenDataLabInspectionsByVin(normalizedVin).catch(() => []);
  if (!odometerRecords.length) {
    return inspections;
  }

  return mergeOpenDataLabInspectionRecords(inspections, odometerRecords, normalizedVin);
}

async function enrichInspectionsWithMileage(inspections, vin, record = null) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase();
  if (!normalizedVin) {
    return inspections;
  }

  const withOpenDataLab = await enrichInspectionsWithOpenDataLab(inspections, normalizedVin);
  const withValidity = record ? mergeInspectionValidityFromRecord(withOpenDataLab, record) : withOpenDataLab;
  return enrichInspectionsWithIstpDailyData(withValidity, normalizedVin, record);
}

async function lookupOpenDataLabInspectionsByVin(vin) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase();
  if (!normalizedVin) {
    return [];
  }

  const cached = getTimedCacheValue(OPENDATALAB_STK_CACHE, normalizedVin);
  if (cached !== undefined) {
    return cached || [];
  }

  const targetUrl = new URL(OPENDATALAB_STK_API_URL);
  targetUrl.searchParams.set("vin", `eq.${normalizedVin}`);
  targetUrl.searchParams.set("order", "date.asc");

  const payload = await requestJson(targetUrl.toString(), {
    method: "GET",
    timeoutMs: OPENDATALAB_STK_TIMEOUT_MS,
    headers: {
      Accept: "application/json",
      "User-Agent": "AutoInfo/1.0 (+https://info.exleasing.cz)"
    }
  });

  const records = (Array.isArray(payload) ? payload : [])
    .map(normalizeOpenDataLabInspectionRecord)
    .filter(Boolean)
    .sort((left, right) => normalizeDateScore(left.validFrom) - normalizeDateScore(right.validFrom));

  setTimedCacheValue(OPENDATALAB_STK_CACHE, normalizedVin, records);
  return records;
}

function normalizeOpenDataLabInspectionRecord(row) {
  const date = normalizeOpenDataLabDate(row?.date);
  const odometer = normalizeOdometer(row?.mileage);
  if (!date && odometer === null) {
    return null;
  }

  return {
    type: mapOpenDataLabInspectionType(row?.inspection_type),
    state: mapOpenDataLabInspectionResult(row?.result),
    stationCode: normalizeWhitespace(row?.station_id) || null,
    stationName: normalizeWhitespace(row?.station_id) ? `STK ${normalizeWhitespace(row.station_id)}` : null,
    validFrom: date,
    validUntil: null,
    protocolNumber: row?.id === null || row?.id === undefined ? null : `ODL-${row.id}`,
    odometer,
    odometerUnit: odometer === null ? null : "km",
    current: false,
    source: "OpenDataLab STK portal",
    sourceUrl: "https://stk.opendatalab.cz/about",
    sourceId: row?.id === null || row?.id === undefined ? null : String(row.id),
    rawInspectionType: normalizeWhitespace(row?.inspection_type) || null
  };
}

function mergeOpenDataLabInspectionRecords(inspections, odometerRecords, vin) {
  const baseRecords = Array.isArray(inspections?.records) ? inspections.records.map(clone) : [];
  const recordsByDate = new Map();

  odometerRecords.forEach((record, index) => {
    const key = normalizeInspectionDateKey(record.validFrom);
    if (!key) {
      return;
    }

    if (!recordsByDate.has(key)) {
      recordsByDate.set(key, []);
    }
    recordsByDate.get(key).push({ index, record });
  });

  const matchedIndexes = new Set();
  const mergedRecords = baseRecords.map((record) => {
    const key = normalizeInspectionDateKey(record.validFrom || record.performedOn || record.validUntil);
    const candidateEntries = key ? (recordsByDate.get(key) || []).filter((entry) => !matchedIndexes.has(entry.index)) : [];
    if (!candidateEntries.length) {
      return record;
    }

    const candidateRecord = pickBestOpenDataLabInspectionMatch(record, candidateEntries.map((entry) => entry.record));
    const candidateEntry = candidateEntries.find((entry) => entry.record === candidateRecord) || candidateEntries[0];
    const candidate = candidateEntry.record;
    matchedIndexes.add(candidateEntry.index);

    return {
      ...record,
      type: record.type || candidate.type,
      state: record.state || candidate.state,
      stationCode: record.stationCode || candidate.stationCode || null,
      stationName: record.stationName || candidate.stationName || null,
      validFrom: record.validFrom || candidate.validFrom,
      validUntil: record.validUntil || candidate.validUntil,
      protocolNumber: record.protocolNumber || candidate.protocolNumber || null,
      odometer: record.odometer === null || record.odometer === undefined ? candidate.odometer : record.odometer,
      odometerUnit: record.odometerUnit || candidate.odometerUnit || null,
      odometerSource: record.odometerSource || candidate.source || "OpenDataLab STK portal",
      odometerSourceUrl: record.odometerSourceUrl || candidate.sourceUrl || null,
      sourceId: record.sourceId || candidate.sourceId || null,
      source: record.source || candidate.source || null,
      rawInspectionType: record.rawInspectionType || candidate.rawInspectionType || null,
      defects: mergeInspectionDefects(record.defects, candidate.defects)
    };
  });

  odometerRecords.forEach((record, index) => {
    const key = normalizeInspectionDateKey(record.validFrom);
    if (!key || matchedIndexes.has(index)) {
      return;
    }

    mergedRecords.push(record);
  });

  const records = mergedRecords
    .filter(Boolean)
    .sort((left, right) => compareDatesDesc(left.validFrom || left.validUntil, right.validFrom || right.validUntil));

  const result = {
    ...(inspections || {}),
    pcv: inspections?.pcv || null,
    sourceFile: inspections?.sourceFile || null,
    sourceUpdatedAt: inspections?.sourceUpdatedAt || null,
    odometerSource: {
      name: "OpenDataLab STK portal",
      url: "https://stk.opendatalab.cz/about",
      fetchedAt: new Date().toISOString(),
      vin,
      recordCount: odometerRecords.length
    },
    fetchedAt: new Date().toISOString(),
    summary: buildInspectionSummary(records),
    records
  };

  return records.length > 0 ? result : inspections;
}

function withResolvedInspectionPcv(inspections, pcv) {
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!inspections || !normalizedPcv || inspections.pcv) {
    return inspections;
  }

  return {
    ...inspections,
    pcv: normalizedPcv
  };
}

function pickBestOpenDataLabInspectionMatch(record, candidates) {
  if (candidates.length === 1) {
    return candidates[0];
  }

  const type = normalizeForMatch(record?.type);
  const state = normalizeForMatch(record?.state);
  const repeatedNeedle = type.includes("opak");
  const failedNeedle = state === "b" || state.includes("nezpus");
  const regularNeedle = type.includes("pravid");

  return (
    candidates.find((candidate) => repeatedNeedle && normalizeForMatch(candidate.rawInspectionType).includes("repeated")) ||
    candidates.find((candidate) => failedNeedle && String(candidate.state || "").toUpperCase() === "B") ||
    candidates.find((candidate) => regularNeedle && normalizeForMatch(candidate.rawInspectionType) === "regular") ||
    candidates[0]
  );
}

function normalizeInspectionDateKey(value) {
  if (!value) {
    return "";
  }

  const raw = normalizeWhitespace(value);
  const isoDate = raw.match(/^(\d{4})-(\d{2})-(\d{2})(?:$|[T\s])/);
  if (isoDate) {
    return `${isoDate[1]}-${isoDate[2]}-${isoDate[3]}`;
  }

  const displayDate = raw.match(/^(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})$/);
  if (displayDate) {
    return `${displayDate[3]}-${String(displayDate[2]).padStart(2, "0")}-${String(displayDate[1]).padStart(2, "0")}`;
  }

  const parsed = new Date(value);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString().slice(0, 10);
  }

  return "";
}

function normalizeOpenDataLabDate(value) {
  const raw = normalizeWhitespace(value);
  if (!raw) {
    return null;
  }

  const parsed = new Date(raw);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function mapOpenDataLabInspectionType(value) {
  const normalized = normalizeWhitespace(value).toLowerCase() || normalizeForMatch(value);
  const map = {
    regular: "P - Pravidelná",
    repeated: "P - Opakovaná",
    evidence: "E - Evidenční",
    road: "Silniční",
    road_repeated: "Silniční - opakovaná",
    road_repeated_after_dn: "Silniční - opakovaná po DN",
    before_registration: "Před registrací",
    before_registration_repeated: "Před registrací - opakovaná",
    before_acceptance: "Před schválením technické způsobilosti",
    before_acceptance_repeated: "Před schválením technické způsobilosti - opakovaná",
    ordered: "Nařízená",
    on_demand: "Na žádost zákazníka",
    adr: "ADR",
    adr_repeated: "ADR - opakovaná"
  };

  return map[normalized] || normalizeWhitespace(value) || "Technická prohlídka";
}

function mapOpenDataLabInspectionResult(value) {
  const normalized = String(value ?? "").trim();
  if (normalized === "0") {
    return "A";
  }

  if (normalized === "1") {
    return "B";
  }

  if (normalized === "2") {
    return "C";
  }

  return normalizeWhitespace(value) || "Nezjištěno";
}

async function enrichInspectionsWithIstpDailyData(inspections, vin, record = null) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase();
  if (!normalizedVin || !ISTP_STK_LOOKUP_ENABLED || !ISTP_STK_SPARQL_URL) {
    return inspections;
  }

  const candidateDates = collectIstpCandidateDates(inspections, record);
  if (!candidateDates.length) {
    return inspections;
  }

  const records = [];
  for (const dateKey of candidateDates.slice(0, ISTP_STK_MAX_CANDIDATE_DATES)) {
    const dailyRecords = await lookupIstpDailyInspectionsByVin(normalizedVin, dateKey).catch(() => []);
    records.push(...dailyRecords);
  }

  const uniqueRecords = uniqueIstpInspectionRecords(records);
  if (!uniqueRecords.length) {
    return inspections;
  }

  return mergeIstpInspectionRecords(inspections, uniqueRecords, normalizedVin);
}

function collectIstpCandidateDates(inspections, record) {
  const seen = new Set();
  const dates = [];
  const todayKey = new Date().toISOString().slice(0, 10);

  const addDate = (value) => {
    const key = normalizeInspectionDateKey(value);
    if (!key || key > todayKey || seen.has(key)) {
      return;
    }

    seen.add(key);
    dates.push(key);
  };

  const addValidityCandidates = (validUntil) => {
    addDate(subtractYearsFromDate(validUntil, 2));
    addDate(subtractYearsFromDate(validUntil, 4));
  };

  const addDateWindow = (value, offsets) => {
    offsets.forEach((offset) => addDate(addDaysToDate(value, offset)));
  };

  (Array.isArray(inspections?.records) ? inspections.records : []).forEach((inspection) => {
    addDate(inspection?.validFrom || inspection?.performedOn);
    addValidityCandidates(inspection?.validUntil);
  });

  // ponytail: import evidence checks usually sit right before Czech registration; widen only that date.
  addDateWindow(extractRecordItemValue(record, [
    "První registrace v ČR",
    "Datum první registrace v ČR"
  ]), [0, -1, -2, -3, -4, -5, -6, -7, 1]);

  (Array.isArray(record?.inspectionHints) ? record.inspectionHints : []).forEach((hint) => {
    addDate(hint?.date || hint?.validFrom || hint?.performedOn);
    addValidityCandidates(hint?.validUntil);
  });

  return dates;
}

function subtractYearsFromDate(value, years) {
  const key = normalizeInspectionDateKey(value);
  if (!key) {
    return null;
  }

  const [year, month, day] = key.split("-").map(Number);
  if (!year || !month || !day) {
    return null;
  }

  return new Date(Date.UTC(year - years, month - 1, day)).toISOString();
}

function addDaysToDate(value, days) {
  const key = normalizeInspectionDateKey(value);
  if (!key) {
    return null;
  }

  const [year, month, day] = key.split("-").map(Number);
  if (!year || !month || !day) {
    return null;
  }

  return new Date(Date.UTC(year, month - 1, day + days)).toISOString();
}

async function lookupIstpDailyInspectionsByVin(vin, dateKey) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase();
  const normalizedDate = normalizeInspectionDateKey(dateKey);
  if (!normalizedVin || !normalizedDate) {
    return [];
  }

  const cacheKey = `${normalizedDate}:${normalizedVin}`;
  const cached = getTimedCacheValue(ISTP_STK_DAILY_VIN_CACHE, cacheKey);
  if (cached !== undefined) {
    return cached || [];
  }

  const localPath = await ensureIstpDailyDatasetLocal(normalizedDate);
  if (!localPath) {
    setTimedCacheValue(ISTP_STK_DAILY_VIN_CACHE, cacheKey, []);
    return [];
  }

  const payload = await fs.promises.readFile(localPath);
  const xml = readIstpXmlBuffer(payload);
  const records = extractIstpInspectionsFromXml(xml, normalizedVin)
    .sort((left, right) => normalizeDateScore(left.validFrom) - normalizeDateScore(right.validFrom));
  const enrichedRecords = await enrichInspectionDefects(records);

  setTimedCacheValue(ISTP_STK_DAILY_VIN_CACHE, cacheKey, enrichedRecords);
  return enrichedRecords;
}

async function ensureIstpDailyDatasetLocal(dateKey) {
  const normalizedDate = normalizeInspectionDateKey(dateKey);
  if (!normalizedDate) {
    return null;
  }

  await fs.promises.mkdir(ISTP_STK_CACHE_DIR, { recursive: true });
  const localPath = path.join(ISTP_STK_CACHE_DIR, `${normalizedDate}.xml.gz`);
  if (fs.existsSync(localPath)) {
    return localPath;
  }

  const downloadUrl = await resolveIstpDailyDownloadUrl(normalizedDate);
  if (!downloadUrl) {
    return null;
  }

  const payload = await requestBuffer(downloadUrl, {
    method: "GET",
    timeoutMs: ISTP_STK_TIMEOUT_MS,
    headers: {
      Accept: "application/gzip, application/xml, text/xml, */*",
      "User-Agent": "AutoInfo/1.0 (+https://info.exleasing.cz)"
    }
  });

  if (!payload || !payload.length) {
    return null;
  }

  const tempPath = `${localPath}.${process.pid}.tmp`;
  await fs.promises.writeFile(tempPath, payload);
  await fs.promises.rename(tempPath, localPath);
  return localPath;
}

async function resolveIstpDailyDownloadUrl(dateKey) {
  const normalizedDate = normalizeInspectionDateKey(dateKey);
  if (!normalizedDate) {
    return null;
  }

  const cached = getTimedCacheValue(ISTP_STK_URL_CACHE, normalizedDate);
  if (cached !== undefined) {
    return cached || null;
  }

  const [year, month, day] = normalizedDate.split("-");
  const title = `Prohlídky vozidel STK a SME za ${day}-${month}-${year}`;
  const query = `
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dcterms: <http://purl.org/dc/terms/>
SELECT ?download WHERE {
  ?dataset dcterms:title ?title ;
           dcat:distribution ?distribution .
  ?distribution dcat:downloadURL ?download .
  FILTER(STR(?title) = "${escapeSparqlString(title)}")
}
LIMIT 1`;
  const targetUrl = new URL(ISTP_STK_SPARQL_URL);
  targetUrl.searchParams.set("query", query);
  targetUrl.searchParams.set("format", "application/sparql-results+json");

  const payload = await requestJson(targetUrl.toString(), {
    method: "GET",
    timeoutMs: ISTP_STK_TIMEOUT_MS,
    headers: {
      Accept: "application/sparql-results+json, application/json",
      "User-Agent": "AutoInfo/1.0 (+https://info.exleasing.cz)"
    }
  });
  const downloadUrl = normalizeWhitespace(payload?.results?.bindings?.[0]?.download?.value) || null;

  setTimedCacheValue(ISTP_STK_URL_CACHE, normalizedDate, downloadUrl);
  return downloadUrl;
}

function readIstpXmlBuffer(payload) {
  if (!payload || !payload.length) {
    return "";
  }

  const buffer = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  const isGzip = buffer.length > 2 && buffer[0] === 0x1f && buffer[1] === 0x8b;
  return (isGzip ? zlib.gunzipSync(buffer) : buffer).toString("utf8");
}

function extractIstpInspectionsFromXml(xml, vin) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase();
  if (!xml || !normalizedVin) {
    return [];
  }

  const records = [];
  const blockPattern = new RegExp(
    `<(?:[\\w.-]+:)?Prohlidka(?:\\s[^>]*)?>[\\s\\S]*?<\\/(?:[\\w.-]+:)?Prohlidka>`,
    "gi"
  );
  let match;
  while ((match = blockPattern.exec(xml))) {
    const record = normalizeIstpInspectionBlock(match[0]);
    if (record && normalizeWhitespace(record.vin).toUpperCase() === normalizedVin) {
      records.push(record);
    }
  }

  return records;
}

function normalizeIstpInspectionBlock(block) {
  const vehicleBlock = getXmlElementBlock(block, "Vozidlo") || block;
  const stationBlock = getXmlElementBlock(block, "Stanice") || block;
  const resultBlock = getXmlElementBlock(block, "Vysledek") || block;
  const inspectionTimeBlock = getXmlElementBlock(block, "CasoveUdaje") || block;
  const technicalBlock = getXmlElementBlock(block, "TechnickaCast") || "";
  const technicalTimeBlock = getXmlElementBlock(technicalBlock, "CasoveUdaje") || "";
  const vin = normalizeWhitespace(getXmlElementText(vehicleBlock, "Vin")).toUpperCase() || null;
  const validFrom = normalizeOpenDataDate(getXmlElementText(block, "DatumProhlidky"));
  const validUntil = normalizeOpenDataDate(getXmlElementText(resultBlock, "DatumPristiProhlidky"));
  const odometer = normalizeOdometer(getXmlElementText(resultBlock, "Odometr"));
  const defects = extractIstpInspectionDefects(resultBlock);
  const inspectionStartedAt = normalizeOpenDataDate(getXmlElementText(inspectionTimeBlock, "Zahajeni"));
  const inspectionEndedAt = normalizeOpenDataDate(getXmlElementText(inspectionTimeBlock, "Ukonceni"));
  const technicalStartedAt = normalizeOpenDataDate(getXmlElementText(technicalTimeBlock, "Zahajeni"));
  const technicalEndedAt = normalizeOpenDataDate(getXmlElementText(technicalTimeBlock, "Ukonceni"));

  if (!vin || (!validFrom && odometer === null && !validUntil)) {
    return null;
  }

  const stationCode = normalizeWhitespace(getXmlElementText(stationBlock, "Cislo")) || null;
  const protocolNumber = normalizeWhitespace(getXmlElementText(block, "CisloProtokolu")) || null;

  return {
    vin,
    type: mapIstpInspectionType(getXmlElementText(block, "DruhProhlidky")),
    state: mapIstpInspectionResult(getXmlElementText(resultBlock, "VysledekCelkovy")),
    stationCode,
    stationName: stationCode ? `STK ${stationCode}` : null,
    stationRegion: normalizeWhitespace(getXmlElementText(stationBlock, "Kraj")) || null,
    stationMunicipality: normalizeWhitespace(getXmlElementText(stationBlock, "Obec")) || null,
    validFrom,
    validUntil,
    protocolNumber,
    odometer,
    odometerUnit: odometer === null ? null : "km",
    odometerSource: odometer === null ? null : "NKOD ISTP STK XML",
    odometerSourceUrl: odometer === null
      ? null
      : "https://data.gov.cz/datov%C3%A9-sady?kl%C3%AD%C4%8Dov%C3%A1-slova=prohl%C3%ADdky%20vozidel",
    current: false,
    source: "NKOD ISTP STK XML",
    sourceUrl: "https://data.gov.cz/datov%C3%A9-sady?kl%C3%AD%C4%8Dov%C3%A1-slova=prohl%C3%ADdky%20vozidel",
    sourceId: protocolNumber,
    rawInspectionType: normalizeWhitespace(getXmlElementText(block, "DruhProhlidky")) || null,
    inspectionStartedAt,
    inspectionEndedAt,
    technicalStartedAt,
    technicalEndedAt,
    defects: defects.length ? defects : null
  };
}

function extractIstpInspectionDefects(resultBlock) {
  const listBlock = getXmlElementBlock(resultBlock, "ZavadaSeznam") || resultBlock;
  const defects = getXmlElementBlocks(listBlock, "Zavada")
    .map((defectBlock) => {
      const code = normalizeStkDefectCode(getXmlElementText(defectBlock, "Kod"));
      const severity = normalizeStkDefectSeverity(getXmlElementText(defectBlock, "Zavaznost"));
      if (!code && !severity) {
        return null;
      }

      return {
        code,
        severity,
        type: severity,
        description: null
      };
    })
    .filter(Boolean);

  const freeText = normalizeWhitespace(
    getXmlElementText(resultBlock, "ZavadyText") ||
      getXmlElementText(resultBlock, "PopisZavad") ||
      getXmlElementText(resultBlock, "PopisZávad")
  );
  if (!defects.length && freeText) {
    defects.push({
      code: null,
      severity: null,
      type: null,
      description: freeText
    });
  }

  return normalizeInspectionDefects(defects);
}

async function enrichInspectionDefects(records) {
  const list = Array.isArray(records) ? records : [];
  if (!list.length) {
    return list;
  }

  const hasDefects = list.some((record) => Array.isArray(record?.defects) && record.defects.length);
  if (!hasDefects) {
    return list;
  }

  const definitionMap = await getStkDefectDefinitionMap();
  return list.map((record) => {
    const defects = normalizeInspectionDefects(record?.defects, definitionMap);
    if (!defects.length) {
      return record;
    }

    return {
      ...record,
      defects
    };
  });
}

async function getStkDefectDefinitionMap() {
  if (!STK_DEFECTS_API_URL) {
    return new Map();
  }

  const now = Date.now();
  if (STK_DEFECTS_CACHE.value && STK_DEFECTS_CACHE.expiresAt > now) {
    return STK_DEFECTS_CACHE.value;
  }

  if (STK_DEFECTS_CACHE.promise) {
    return STK_DEFECTS_CACHE.promise;
  }

  STK_DEFECTS_CACHE.promise = requestJson(STK_DEFECTS_API_URL, {
    method: "GET",
    timeoutMs: OPENDATALAB_STK_TIMEOUT_MS,
    headers: {
      Accept: "application/json",
      "User-Agent": "AutoInfo/1.0 (+https://info.exleasing.cz)"
    }
  })
    .then((payload) => {
      const rows = Array.isArray(payload) ? payload : Array.isArray(payload?.data) ? payload.data : [];
      const definitionMap = new Map();
      rows.forEach((row) => {
        const code = normalizeStkDefectCode(row?.code || row?.kod || row?.Kod || row?.id);
        if (!code) {
          return;
        }

        const severity = normalizeStkDefectSeverity(
          row?.type || row?.severity || row?.zavaznost || row?.Zavaznost
        );
        definitionMap.set(code, {
          code,
          severity,
          type: severity,
          description:
            normalizeWhitespace(row?.description || row?.popis || row?.text || row?.name || row?.nazev) || null
        });
      });

      STK_DEFECTS_CACHE.value = definitionMap;
      STK_DEFECTS_CACHE.expiresAt = Date.now() + STK_DEFECTS_CACHE_TTL_MS;
      return definitionMap;
    })
    .catch(() => {
      const emptyMap = new Map();
      STK_DEFECTS_CACHE.value = emptyMap;
      STK_DEFECTS_CACHE.expiresAt = Date.now() + Math.min(STK_DEFECTS_CACHE_TTL_MS || 300000, 300000);
      return emptyMap;
    })
    .finally(() => {
      STK_DEFECTS_CACHE.promise = null;
    });

  return STK_DEFECTS_CACHE.promise;
}

function mergeInspectionDefects(primary, fallback) {
  const primaryDefects = normalizeInspectionDefects(primary);
  const fallbackDefects = normalizeInspectionDefects(fallback);
  const merged = normalizeInspectionDefects([...primaryDefects, ...fallbackDefects]);
  return merged.length ? merged : null;
}

function normalizeInspectionDefects(defects, definitionMap = null) {
  const source = Array.isArray(defects) ? defects : [];
  const seen = new Set();
  return source
    .map((defect) => {
      const code = normalizeStkDefectCode(
        typeof defect === "string" ? defect : defect?.code || defect?.kod || defect?.Kod || defect?.id
      );
      const definition = code && definitionMap && typeof definitionMap.get === "function"
        ? definitionMap.get(code)
        : null;
      const severity =
        normalizeStkDefectSeverity(
          typeof defect === "string"
            ? null
            : defect?.severity || defect?.type || defect?.zavaznost || defect?.Zavaznost
        ) ||
        definition?.severity ||
        definition?.type ||
        null;
      const description =
        normalizeWhitespace(
          typeof defect === "string"
            ? null
            : defect?.description || defect?.text || defect?.popis || defect?.name || defect?.nazev
        ) ||
        definition?.description ||
        null;

      if (!code && !severity && !description) {
        return null;
      }

      return {
        code,
        severity,
        type: severity,
        description
      };
    })
    .filter((defect) => {
      if (!defect) {
        return false;
      }

      const key = defect.code || `${defect.severity || ""}:${normalizeForMatch(defect.description)}`;
      if (seen.has(key)) {
        return false;
      }

      seen.add(key);
      return true;
    });
}

function normalizeStkDefectCode(value) {
  return normalizeWhitespace(value).replace(/\s+/g, "").toUpperCase() || null;
}

function normalizeStkDefectSeverity(value) {
  return normalizeWhitespace(value).toUpperCase() || null;
}

function uniqueIstpInspectionRecords(records) {
  const seen = new Set();
  return (Array.isArray(records) ? records : []).filter((record) => {
    const key = [
      normalizeWhitespace(record?.protocolNumber),
      normalizeInspectionDateKey(record?.validFrom),
      normalizeWhitespace(record?.vin).toUpperCase()
    ].join(":");
    if (seen.has(key)) {
      return false;
    }

    seen.add(key);
    return true;
  });
}

function mergeIstpInspectionRecords(inspections, istpRecords, vin) {
  const baseRecords = Array.isArray(inspections?.records) ? inspections.records.map(clone) : [];
  const usedIndexes = new Set();

  const mergedRecords = baseRecords.map((record) => {
    const matchIndex = istpRecords.findIndex((candidate, index) => {
      if (usedIndexes.has(index)) {
        return false;
      }

      const sameProtocol =
        normalizeWhitespace(record?.protocolNumber) &&
        normalizeWhitespace(record.protocolNumber) === normalizeWhitespace(candidate.protocolNumber);
      const samePerformed =
        normalizeInspectionDateKey(record?.validFrom || record?.performedOn) &&
        normalizeInspectionDateKey(record?.validFrom || record?.performedOn) === normalizeInspectionDateKey(candidate.validFrom);
      const sameValidity =
        normalizeInspectionDateKey(record?.validUntil) &&
        normalizeInspectionDateKey(record.validUntil) === normalizeInspectionDateKey(candidate.validUntil);

      return sameProtocol || samePerformed || sameValidity;
    });

    if (matchIndex < 0) {
      return record;
    }

    const candidate = istpRecords[matchIndex];
    usedIndexes.add(matchIndex);
    return {
      ...record,
      type: record.type || candidate.type,
      state: record.state || candidate.state,
      stationCode: record.stationCode || candidate.stationCode || null,
      stationName: record.stationName || candidate.stationName || null,
      stationRegion: record.stationRegion || candidate.stationRegion || null,
      stationMunicipality: record.stationMunicipality || candidate.stationMunicipality || null,
      validFrom: record.validFrom || candidate.validFrom,
      validUntil: record.validUntil || candidate.validUntil,
      protocolNumber: record.protocolNumber || candidate.protocolNumber || null,
      odometer: record.odometer === null || record.odometer === undefined ? candidate.odometer : record.odometer,
      odometerUnit: record.odometerUnit || candidate.odometerUnit || null,
      odometerSource: record.odometerSource || candidate.source || "NKOD ISTP STK XML",
      odometerSourceUrl: record.odometerSourceUrl || candidate.sourceUrl || null,
      sourceId: record.sourceId || candidate.sourceId || null,
      source: record.source || candidate.source || null,
      inspectionStartedAt: record.inspectionStartedAt || candidate.inspectionStartedAt || null,
      inspectionEndedAt: record.inspectionEndedAt || candidate.inspectionEndedAt || null,
      technicalStartedAt: record.technicalStartedAt || candidate.technicalStartedAt || null,
      technicalEndedAt: record.technicalEndedAt || candidate.technicalEndedAt || null,
      defects: mergeInspectionDefects(record.defects, candidate.defects)
    };
  });

  istpRecords.forEach((record, index) => {
    if (!usedIndexes.has(index)) {
      mergedRecords.push(record);
    }
  });

  const records = mergedRecords
    .filter(Boolean)
    .sort((left, right) => compareDatesDesc(left.validFrom || left.validUntil, right.validFrom || right.validUntil));

  const result = {
    ...(inspections || {}),
    pcv: inspections?.pcv || null,
    sourceFile: inspections?.sourceFile || null,
    sourceUpdatedAt: inspections?.sourceUpdatedAt || null,
    odometerSource: {
      name: "NKOD ISTP STK XML",
      url: "https://data.gov.cz/datov%C3%A9-sady?kl%C3%AD%C4%8Dov%C3%A1-slova=prohl%C3%ADdky%20vozidel",
      fetchedAt: new Date().toISOString(),
      vin,
      recordCount: istpRecords.length
    },
    fetchedAt: new Date().toISOString(),
    summary: buildInspectionSummary(records),
    records
  };

  return records.length > 0 ? result : inspections;
}

function mapIstpInspectionType(value) {
  const normalized = normalizeWhitespace(value).toLowerCase();
  if (!normalized) {
    return "Technická prohlídka";
  }

  if (normalized.includes("pravideln")) {
    return "P - Pravidelná";
  }

  if (normalized.includes("opakovan")) {
    return "P - Opakovaná";
  }

  if (normalized.includes("eviden")) {
    return "E - Evidenční";
  }

  if (normalized.includes("registr")) {
    return "Před registrací";
  }

  return normalizeWhitespace(value) || "Technická prohlídka";
}

function mapIstpInspectionResult(value) {
  const normalized = String(value ?? "").trim();
  if (normalized === "1") {
    return "A";
  }

  if (normalized === "2") {
    return "B";
  }

  if (normalized === "3") {
    return "C";
  }

  return normalizeWhitespace(value) || "Nezjištěno";
}

function getXmlElementBlock(xml, tagName) {
  const pattern = new RegExp(
    `<(?:[\\w.-]+:)?${escapeRegExp(tagName)}(?:\\s[^>]*)?>[\\s\\S]*?<\\/(?:[\\w.-]+:)?${escapeRegExp(tagName)}>`,
    "i"
  );
  return xml?.match(pattern)?.[0] || "";
}

function getXmlElementBlocks(xml, tagName) {
  const pattern = new RegExp(
    `<(?:[\\w.-]+:)?${escapeRegExp(tagName)}(?:\\s[^>]*)?>[\\s\\S]*?<\\/(?:[\\w.-]+:)?${escapeRegExp(tagName)}>`,
    "gi"
  );
  const blocks = [];
  let match;
  while ((match = pattern.exec(xml || ""))) {
    blocks.push(match[0]);
  }

  return blocks;
}

function getXmlElementText(xml, tagName) {
  const pattern = new RegExp(
    `<(?:[\\w.-]+:)?${escapeRegExp(tagName)}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/(?:[\\w.-]+:)?${escapeRegExp(tagName)}>`,
    "i"
  );
  return decodeXmlText(xml?.match(pattern)?.[1] || "");
}

function decodeXmlText(value) {
  return String(value ?? "")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&");
}

function escapeSparqlString(value) {
  return String(value ?? "").replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function escapeRegExp(value) {
  return String(value ?? "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
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
          reject(new Error(`Open data download vrátil chybu ${response.statusCode || 500}.`));
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
            new Error(`Open data download vrátil chybu ${response.statusCode || 500}.`)
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
          reject(new Error("Nepodařilo se získat přístupový token pro otevřená data."));
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
    odometer: normalizeOdometer(firstNonEmpty([
      canonicalRow.STAVTACHOMETRU,
      canonicalRow.STAVKM,
      canonicalRow.STAVKILOMETRU,
      canonicalRow.TACHOMETR,
      canonicalRow.KM,
      row["Stav tachometru"],
      row["Stav km"],
      row.Tachometr,
      row.KM
    ])),
    odometerUnit: normalizeOdometer(firstNonEmpty([
      canonicalRow.STAVTACHOMETRU,
      canonicalRow.STAVKM,
      canonicalRow.STAVKILOMETRU,
      canonicalRow.TACHOMETR,
      canonicalRow.KM,
      row["Stav tachometru"],
      row["Stav km"],
      row.Tachometr,
      row.KM
    ])) === null ? null : "km",
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

function mergeInspectionValidityFromRecord(inspections, record) {
  const validUntil = normalizeInspectionValidityDate(extractRecordItemValue(record, [
    "STK do",
    "STK platna do",
    "Pravidelna technicka prohlidka do"
  ]));
  if (!validUntil) {
    return inspections;
  }

  const baseRecords = Array.isArray(inspections?.records) ? inspections.records.map(clone) : [];
  if (baseRecords.some((inspection) => inspection.current && inspection.validUntil)) {
    return inspections;
  }

  const sortedRecords = baseRecords
    .filter(Boolean)
    .sort((left, right) => compareDatesDesc(left.validFrom || left.validUntil, right.validFrom || right.validUntil));
  const currentIndex = sortedRecords.findIndex((inspection) => !inspection.validUntil);
  let records;

  if (currentIndex >= 0) {
    records = sortedRecords.map((inspection, index) =>
      index === currentIndex
        ? {
            ...inspection,
            validUntil,
            current: true,
            type: inspection.type || "P - Pravidelná",
            state: inspection.state || "A",
            validitySource: inspection.validitySource || "Datová kostka RSV"
          }
        : {
            ...inspection,
            current: Boolean(inspection.current && inspection.validUntil)
          }
    );
  } else {
    records = [
      {
        type: "P - Pravidelná",
        state: "A",
        stationCode: null,
        stationName: null,
        validFrom: null,
        validUntil,
        protocolNumber: null,
        odometer: null,
        odometerUnit: null,
        current: true,
        source: "Datová kostka RSV",
        sourceUrl: "https://dataovozidlech.cz/"
      },
      ...sortedRecords.map((inspection) => ({ ...inspection, current: false }))
    ];
  }

  return {
    ...(inspections || {}),
    fetchedAt: inspections?.fetchedAt || new Date().toISOString(),
    summary: buildInspectionSummary(records),
    records
  };
}

function getLatestInspectionRecord(records, fallback = null) {
  const sourceRecords = Array.isArray(records) ? records : [];
  if (sourceRecords.length === 0) {
    return fallback || null;
  }

  return sourceRecords
    .map((record, index) => ({
      record,
      index,
      score: normalizeDateScore(record?.validFrom || record?.performedOn || record?.validUntil)
    }))
    .sort((left, right) => right.score - left.score || left.index - right.index)[0]?.record || fallback || null;
}

function mergeInspectionData(record, inspections) {
  const nextRecord = clone(record);
  const currentRecord = getLatestInspectionRecord(inspections.records, inspections.summary?.currentRecord);
  const statusTone = mapInspectionStatusToTone(inspections.summary?.status);
  const inspectionUntil = currentRecord?.validUntil ? formatDate(currentRecord.validUntil) : null;
  const inspectionFrom = currentRecord?.validFrom ? formatDate(currentRecord.validFrom) : null;

  nextRecord.inspections = inspections;

  if (inspectionUntil) {
    nextRecord.highlights = upsertHighlight(nextRecord.highlights, "STK do", inspectionUntil, statusTone);
    nextRecord.sections = upsertSectionItems(nextRecord.sections, "Kontroly a omezení", [
      item("STK platná do", inspectionUntil, statusTone),
      item("Poslední kontrola od", inspectionFrom),
      item("Aktuální typ kontroly", currentRecord?.type),
      item("Aktuální stav", currentRecord?.state),
      item("Stav km", formatInspectionOdometer(currentRecord))
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

  nextRecord.timeline = removeStandaloneInspectionValidityTimelineEntry(mergeTimeline(
    nextRecord.timeline || [],
    buildInspectionTimeline(inspections.records)
  ));

  return nextRecord;
}

function removeStandaloneInspectionValidityTimelineEntry(entries) {
  return (Array.isArray(entries) ? entries : []).filter((entry) => {
    return normalizeForMatch(entry?.title) !== normalizeForMatch("Platnost technické kontroly");
  });
}

function buildInspectionTimeline(records) {
  return records
    .filter((record) => record.current || record.validFrom || record.validUntil)
    .slice(0, 6)
    .map((record) => ({
      date: normalizeTimelineDate(record.validFrom || record.validUntil),
      title: record.current ? "Aktuální technická prohlídka" : "Technická prohlídka",
      description: [
        record.type || null,
        record.state || null,
        formatInspectionStationLabel(record),
        formatInspectionOdometer(record) ? `stav tachometru: ${formatInspectionOdometer(record)}` : null,
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
  let cachedPcv = knownPcv || (vin ? getPersistentPcv(vin) : null);
  if (!cachedPcv && vin) {
    cachedPcv = await resolveIndexedPcvForVin(vin);
  }
  let nextRecord = cachedPcv ? injectPcvIntoRecord(record, cachedPcv) : record;
  const cachedInspections = cachedPcv ? getPersistentInspections(cachedPcv) : null;

  if (cachedPcv) {
    const databaseInspections = withResolvedInspectionPcv(
      await enrichInspectionsWithMileage(
        await lookupInspectionsFromDatabaseByPcv(cachedPcv).catch(() => null),
        vin,
        nextRecord
      ),
      cachedPcv
    );
    if (databaseInspections) {
      await storePersistentInspections(cachedPcv, databaseInspections);
      nextRecord = mergeInspectionData(nextRecord, databaseInspections);
      nextRecord.inspectionLookup = buildInspectionLookupState("ready", vin, cachedPcv, databaseInspections);
      return nextRecord;
    }
  }

  if (cachedInspections) {
    const enrichedCachedInspections = withResolvedInspectionPcv(
      await enrichInspectionsWithMileage(cachedInspections, vin, nextRecord),
      cachedPcv
    );
    nextRecord = mergeInspectionData(nextRecord, enrichedCachedInspections);
    nextRecord.inspectionLookup = buildInspectionLookupState("ready", vin, cachedPcv, enrichedCachedInspections);
    return nextRecord;
  }

  if (!cachedPcv && vin) {
    const databaseInspectionsByVin = await lookupInspectionsFromDatabaseByVin(vin).catch(() => null);
    if (databaseInspectionsByVin) {
      cachedPcv = databaseInspectionsByVin.pcv || null;
      if (cachedPcv) {
        await storePersistentInspections(cachedPcv, databaseInspectionsByVin);
        nextRecord = injectPcvIntoRecord(nextRecord, cachedPcv);
      }
      nextRecord = mergeInspectionData(nextRecord, databaseInspectionsByVin);
      nextRecord.inspectionLookup = buildInspectionLookupState("ready", vin, cachedPcv, databaseInspectionsByVin);
      return nextRecord;
    }
  }

  const vinOnlyInspections = vin
    ? withResolvedInspectionPcv(await enrichInspectionsWithMileage(null, vin, nextRecord), cachedPcv)
    : null;
  if (vinOnlyInspections) {
    nextRecord = mergeInspectionData(nextRecord, vinOnlyInspections);
    nextRecord.inspectionLookup = buildInspectionLookupState("ready", vin, cachedPcv, vinOnlyInspections);
    return nextRecord;
  }

  if (options.includeInspections) {
    const hydrated = withResolvedInspectionPcv(
      await enrichInspectionsWithMileage(await hydrateInspectionData({ vin, pcv: cachedPcv }), vin, nextRecord),
      cachedPcv
    );
    if (hydrated) {
      nextRecord = mergeInspectionData(nextRecord, hydrated);
      nextRecord.inspectionLookup = buildInspectionLookupState("ready", vin, hydrated.pcv || cachedPcv, hydrated);
      return nextRecord;
    }
  }

  if (vin || cachedPcv) {
    if (!ALLOW_RUNTIME_OPEN_DATA_INSPECTION_SCAN) {
      nextRecord.inspectionLookup = buildInspectionLookupState(
        "unavailable",
        vin,
        cachedPcv,
        null,
        "Detailní záznamy STK nejsou v dostupném indexu."
      );
      return nextRecord;
    }

    scheduleInspectionHydration({ vin, pcv: cachedPcv });
    nextRecord.inspectionLookup = buildInspectionLookupState("pending", vin, cachedPcv, null);
    return nextRecord;
  }

  nextRecord.inspectionLookup = buildInspectionLookupState(
    "unavailable",
    null,
    null,
    null,
    "K vozidlu chybí VIN nebo PCV pro dohledání STK."
  );
  return nextRecord;
}

async function attachOwnershipState(record, options = {}) {
  if (!record || typeof record !== "object") {
    return record;
  }

  await ensureOpenDataPersistentCachesLoaded();

  const parties = Array.isArray(record.ownership?.parties) ? record.ownership.parties : [];
  const hasConcreteParties = parties.some((party) => party && (party.name || party.ico));
  const vin = normalizeWhitespace(extractIdentifier(record, "VIN")).toUpperCase() || null;
  const knownPcv = normalizeWhitespace(extractIdentifier(record, "PČV")) || null;
  let cachedPcv = knownPcv || (vin ? getPersistentPcv(vin) : null);
  if (!cachedPcv && vin) {
    cachedPcv = await resolveIndexedPcvForVin(vin);
  }
  let nextRecord = cachedPcv ? injectPcvIntoRecord(record, cachedPcv) : record;
  const cachedOwnership = cachedPcv ? getPersistentOwnership(cachedPcv) : null;

  if (hasConcreteParties) {
    nextRecord.ownershipLookup = buildOwnershipLookupState("ready", vin, cachedPcv, record.ownership || null);
    return nextRecord;
  }

  if (cachedPcv) {
    const databaseOwnership = await lookupOwnershipFromDatabaseByPcv(cachedPcv, nextRecord, vin).catch(() => null);
    if (databaseOwnership) {
      await storePersistentOwnership(cachedPcv, databaseOwnership);
      nextRecord = mergeRecords(nextRecord, databaseOwnership) || nextRecord;
      nextRecord.ownershipLookup = buildOwnershipLookupState("ready", vin, cachedPcv, databaseOwnership.ownership || null);
      return nextRecord;
    }
  }

  if (cachedOwnership) {
    const enrichedCachedOwnership = await resolveMissingCompanyIcosInOwnershipRecord(cachedOwnership);
    nextRecord = mergeRecords(nextRecord, enrichedCachedOwnership) || nextRecord;
    nextRecord.ownershipLookup = buildOwnershipLookupState("ready", vin, cachedPcv, enrichedCachedOwnership.ownership || null);
    return nextRecord;
  }

  if (options.includeOwnership) {
    const hydrated = await hydrateOwnershipData({ vin, pcv: cachedPcv, record: nextRecord });
    if (hydrated) {
      nextRecord = mergeRecords(nextRecord, hydrated) || nextRecord;
      nextRecord.ownershipLookup = buildOwnershipLookupState("ready", vin, hydrated.pcv || cachedPcv, hydrated.ownership || null);
      return nextRecord;
    }
  }

  if (cachedPcv) {
    scheduleOwnershipHydration({ vin, pcv: cachedPcv, record: nextRecord });
    nextRecord.ownershipLookup = buildOwnershipLookupState("pending", vin, cachedPcv, null);
    return nextRecord;
  }

  if (vin) {
    scheduleOwnershipHydration({ vin, pcv: null, record: nextRecord });
    nextRecord.ownershipLookup = buildOwnershipLookupState("pending", vin, null, null);
    return nextRecord;
  }

  nextRecord.ownershipLookup = buildOwnershipLookupState(
    "unavailable",
    vin,
    cachedPcv,
    null,
    "K vozidlu chybí VIN nebo PCV pro dohledání historie subjektu."
  );
  return nextRecord;
}

function buildOwnershipLookupState(status, vin, pcv, ownership, message = null) {
  return {
    status,
    vin: vin || null,
    pcv: pcv || null,
    ownership: status === "ready" ? ownership : null,
    message: message || null,
    resolvedAt: new Date().toISOString()
  };
}

async function lookupVehicleOwnership(params = {}) {
  await ensureOpenDataPersistentCachesLoaded();

  const queryLookup = params.query ? parseLookupQuery(params.query) : null;
  const vin =
    normalizeWhitespace(params.vin || (queryLookup?.type === "vin" ? queryLookup.compact : "")).toUpperCase() ||
    null;
  const plate =
    normalizeWhitespace(params.plate || (queryLookup?.type === "plate" ? queryLookup.compact : "")).toUpperCase() ||
    null;
  const providedPcv = normalizeWhitespace(params.pcv) || null;
  let resolvedPcv = providedPcv || (vin ? getPersistentPcv(vin) : null);
  if (!resolvedPcv && vin) {
    resolvedPcv = await resolveIndexedPcvForVin(vin);
  }
  const cachedOwnership = resolvedPcv ? getPersistentOwnership(resolvedPcv) : null;

  if (resolvedPcv) {
    const databaseOwnership = await lookupOwnershipFromDatabaseByPcv(resolvedPcv, null, vin).catch(() => null);
    if (databaseOwnership) {
      await storePersistentOwnership(resolvedPcv, databaseOwnership);
      return buildOwnershipLookupState("ready", vin, resolvedPcv, databaseOwnership.ownership || null);
    }
  }

  if (cachedOwnership) {
    const enrichedCachedOwnership = await resolveMissingCompanyIcosInOwnershipRecord(cachedOwnership);
    return buildOwnershipLookupState("ready", vin, resolvedPcv, enrichedCachedOwnership.ownership || null);
  }

  if (resolvedPcv) {
    const hydrated = await hydrateOwnershipData({ vin, pcv: resolvedPcv, record: null, plate }).catch(() => null);
    if (hydrated) {
      return buildOwnershipLookupState("ready", vin, hydrated.pcv || resolvedPcv, hydrated.ownership || null);
    }

    return buildOwnershipLookupState(
      "unavailable",
      vin,
      resolvedPcv,
      null,
      "V dostupných datech není detailní historie vlastníků nebo provozovatelů pro toto PCV."
    );
  }

  if (vin) {
    const hydrated = await hydrateOwnershipData({ vin, pcv: null, record: null, plate }).catch(() => null);
    if (hydrated) {
      return buildOwnershipLookupState("ready", vin, null, hydrated.ownership || null);
    }

    return buildOwnershipLookupState(
      "unavailable",
      vin,
      null,
      null,
      "K vozidlu se nepodařilo dohledat detailní právnické subjekty podle VIN."
    );
  }

  if (plate) {
    const hydrated = await hydrateOwnershipData({ vin: null, pcv: resolvedPcv, record: null, plate }).catch(() => null);
    if (hydrated) {
      return buildOwnershipLookupState("ready", null, hydrated.pcv || resolvedPcv, hydrated.ownership || null);
    }

    return buildOwnershipLookupState(
      "unavailable",
      null,
      resolvedPcv,
      null,
      "K vozidlu se nepodařilo dohledat detailní právnické subjekty podle SPZ."
    );
  }

  return buildOwnershipLookupState(
    "unavailable",
    vin,
    resolvedPcv,
    null,
    "Zadejte VIN nebo PCV pro dohledání historie subjektu."
  );
}

function scheduleOwnershipHydration({ vin, pcv, record }) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase() || null;
  const normalizedPcv = normalizeWhitespace(pcv) || null;
  const jobKey = normalizedPcv || normalizedVin;

  if (!jobKey || OPEN_DATA_OWNER_JOBS.has(jobKey)) {
    return;
  }

  const job = hydrateOwnershipData({ vin: normalizedVin, pcv: normalizedPcv, record })
    .catch(() => null)
    .finally(() => {
      OPEN_DATA_OWNER_JOBS.delete(jobKey);
    });

  OPEN_DATA_OWNER_JOBS.set(jobKey, job);
}

async function lookupOwnershipFromPublicVinFallback(vin, options = {}) {
  if (!THIRD_PARTY_OWNERSHIP_FALLBACK_ENABLED) {
    return null;
  }

  const normalizedVin = normalizeWhitespace(vin).toUpperCase() || null;
  const normalizedPlate = normalizeWhitespace(options.plate).toUpperCase() || null;
  if (!normalizedVin && !normalizedPlate) {
    return null;
  }

  if (normalizedVin) {
    const lookup = parseLookupQuery(normalizedVin);
    if (lookup.type === "vin") {
      const hlidacOwnership = await lookupOwnershipFromHlidacStatu(lookup).catch(() => null);
      if (hlidacOwnership) {
        return hlidacOwnership;
      }
    }
  }

  return null;
}

async function hydrateOwnershipData({ vin, pcv, record, plate }) {
  await ensureOpenDataPersistentCachesLoaded();

  const normalizedVin = normalizeWhitespace(vin).toUpperCase() || null;
  const normalizedPlate = normalizeWhitespace(plate || extractIdentifier(record, "SPZ")).toUpperCase() || null;
  let resolvedPcv = normalizeWhitespace(pcv) || null;

  if (!resolvedPcv && normalizedVin) {
    resolvedPcv = getPersistentPcv(normalizedVin);
  }

  if (!resolvedPcv && normalizedVin) {
    resolvedPcv = await resolveIndexedPcvForVin(normalizedVin);
  }

  if (!resolvedPcv && normalizedVin && ALLOW_RUNTIME_OPEN_DATA_OWNERSHIP_SCAN) {
    resolvedPcv = await resolvePcvForVin(normalizedVin).catch(() => null);
    if (resolvedPcv) {
      await storePersistentPcv(normalizedVin, resolvedPcv);
    }
  }

  if (!resolvedPcv) {
    return normalizedVin || normalizedPlate
      ? await lookupOwnershipFromPublicVinFallback(normalizedVin, {
          plate: normalizedPlate,
          pcv: resolvedPcv,
          record
        })
      : null;
  }

  const cachedOwnership = getPersistentOwnership(resolvedPcv);
  if (cachedOwnership) {
    return await resolveMissingCompanyIcosInOwnershipRecord(cachedOwnership);
  }

  const databaseOwnership = await lookupOwnershipFromDatabaseByPcv(resolvedPcv, record, normalizedVin).catch(() => null);
  if (databaseOwnership) {
    await storePersistentOwnership(resolvedPcv, databaseOwnership);
    return databaseOwnership;
  }

  const fleetDbOwnership = await lookupOwnershipFromFleetDbByPcv(resolvedPcv, record, normalizedVin).catch(() => null);
  if (fleetDbOwnership) {
    await storePersistentOwnership(resolvedPcv, fleetDbOwnership);
    return fleetDbOwnership;
  }

  const publicVinOwnership =
    normalizedVin || normalizedPlate
      ? await lookupOwnershipFromPublicVinFallback(normalizedVin, {
          plate: normalizedPlate,
          pcv: resolvedPcv,
          record
        })
      : null;
  if (publicVinOwnership) {
    await storePersistentOwnership(resolvedPcv, publicVinOwnership);
    return publicVinOwnership;
  }

  if (!ALLOW_RUNTIME_OPEN_DATA_OWNERSHIP_SCAN) {
    return null;
  }

  const dataset = await ensureOpenDataDatasetLocal("owners", OPEN_DATA_OWNER_ROUTE);
  const relations = await findOpenDataRowsByPcv(dataset.localPath, resolvedPcv, normalizeCompanyVehicleRelation);
  const ownershipRecord = await buildOwnershipRecordFromRelations(relations, record, normalizedVin, resolvedPcv);

  if (!ownershipRecord) {
    return null;
  }

  await storePersistentOwnership(resolvedPcv, ownershipRecord);
  return ownershipRecord;
}

async function buildOwnershipRecordFromRelations(relations, record, vin, pcv) {
  const parties = await resolveMissingCompanyIcosInParties((Array.isArray(relations) ? relations : [])
    .map(mapOwnershipRelationToParty)
    .filter(isDisplayableOwnershipParty)
    .sort((left, right) => {
      if (left.current !== right.current) {
        return left.current ? -1 : 1;
      }
      return compareDatesDesc(left.since, right.since);
    }));

  if (parties.length === 0) {
    return null;
  }

  const legalParties = uniqueParties(parties.filter(isLegalEntityParty));
  const currentParties = uniqueParties(parties.filter((party) => party.current));

  return {
    source: {
      mode: "live",
      label: "RSV vlastníci/provozovatelé",
      note: "Právnické osoby jsou doplněné z otevřené sady vlastník/provozovatel vozidla."
    },
    hero: {
      badge: parties.some((party) => party.type === "company") ? "Právnická osoba" : record?.hero?.badge || "Bez rozlišení",
      title: record?.hero?.title || `Vozidlo ${vin || pcv || ""}`.trim(),
      subtitle: record?.hero?.subtitle || "Historie vlastníků a provozovatelů z otevřených dat.",
      status: record?.hero?.status || "Neuvedeno"
    },
    highlights: [],
    ownership: {
      ownerCount: countRole(parties, "vlast") || null,
      operatorCount: countRole(parties, "provoz") || null,
      note: "Fyzické osoby jsou ve veřejných datech anonymizované.",
      summary: {
        totalCount: parties.length,
        legalEntityCount: legalParties.length,
        currentCount: currentParties.length
      },
      currentParties,
      historyAvailable: parties.length > currentParties.length,
      parties
    },
    sections: [],
    timeline: []
  };
}

function mapOwnershipRelationToParty(relation) {
  const subjectType = normalizeWhitespace(relation?.subjectType) || null;
  const type = inferOwnershipPartyType(relation);

  return {
    role: normalizeVehicleRelation(relation?.relation),
    type,
    subjectType,
    name: normalizeWhitespace(relation?.name) || (type === "person" ? "Fyzická osoba / anonymizováno" : "Anonymizovaný subjekt"),
    ico: sanitizeIco(relation?.ico),
    address: type === "company" ? normalizeWhitespace(relation?.address) || null : null,
    period: [relation?.dateFrom ? formatDate(relation.dateFrom) : null, relation?.dateTo ? formatDate(relation.dateTo) : "-"]
      .filter(Boolean)
      .join(" - "),
    since: relation?.dateFrom ? formatDate(relation.dateFrom) : null,
    dateFrom: relation?.dateFrom || null,
    dateTo: relation?.dateTo || null,
    current: isActiveRelation(relation)
  };
}

function isDisplayableOwnershipParty(party) {
  if (!party || !isOwnershipVehicleRelation(party.role)) {
    return false;
  }

  if (isLegalEntityParty(party)) {
    return true;
  }

  return party.type === "person" || party.type === "unknown";
}

function maskNonCompanyParty(party) {
  if (!party || isLegalEntityParty(party)) {
    return party;
  }

  return {
    ...party,
    name: party.type === "person" ? "Fyzická osoba / anonymizováno" : "Anonymizovaný subjekt",
    ico: null,
    address: null
  };
}

function inferOwnershipPartyType(relation) {
  const subjectType = relation?.subjectType;
  const name = relation?.name;

  if (sanitizeIco(relation?.ico) || isLegalEntitySubjectType(subjectType) || looksLikeCompanyName(name)) {
    return "company";
  }

  if (isPhysicalSubjectType(subjectType)) {
    return "person";
  }

  return "unknown";
}

function isLegalEntitySubjectType(value) {
  const normalized = normalizeForMatch(value);
  return normalized.includes("pravnick") || normalized.includes("company") || normalized.includes("firma");
}

function isPhysicalSubjectType(value) {
  const normalized = normalizeForMatch(value);
  return normalized.includes("fyzick") || normalized.includes("person");
}

function looksLikeCompanyName(value) {
  const normalized = normalizeForMatch(value);
  return /\b(s\s*r\s*o|spol|a\s*s|druzstvo|zapsany ustav|statni podnik|obec|mesto|kraj)\b/.test(normalized);
}

function hasDisplayableOwnershipText(value) {
  const normalized = normalizeForMatch(value);
  if (!normalized || normalized === "-") {
    return false;
  }

  return !(
    normalized.includes("fyzicka osoba") ||
    normalized.includes("anonym") ||
    normalized.includes("nezverej") ||
    normalized.includes("neuved")
  );
}

function resolvePvzpBrowserInfo() {
  const explicitPath = normalizeWhitespace(process.env.PVZP_BROWSER_PATH || "");
  if (explicitPath) {
    return {
      path: fs.existsSync(explicitPath) ? explicitPath : null,
      source: fs.existsSync(explicitPath) ? "env" : "env-missing"
    };
  }

  const systemBrowserPath = detectSystemBrowserPath();
  if (systemBrowserPath) {
    return { path: systemBrowserPath, source: "system" };
  }

  const playwrightBrowserPath = detectPlaywrightBrowserPath();
  if (playwrightBrowserPath) {
    return { path: playwrightBrowserPath, source: "playwright" };
  }

  return { path: null, source: null };
}

function resolveUniqaBrowserInfo() {
  const explicitPath = normalizeWhitespace(process.env.UNIQA_BROWSER_PATH || "");
  if (explicitPath) {
    return {
      path: fs.existsSync(explicitPath) ? explicitPath : null,
      source: fs.existsSync(explicitPath) ? "env" : "env-missing"
    };
  }

  if (PVZP_BROWSER_PATH) {
    return { path: PVZP_BROWSER_PATH, source: PVZP_BROWSER_INFO.source };
  }

  const systemBrowserPath = detectSystemBrowserPath();
  if (systemBrowserPath) {
    return { path: systemBrowserPath, source: "system" };
  }

  const playwrightBrowserPath = detectPlaywrightBrowserPath();
  if (playwrightBrowserPath) {
    return { path: playwrightBrowserPath, source: "playwright" };
  }

  return { path: null, source: null };
}

function buildPvzpLaunchOptions() {
  const args = ["--window-size=1440,1200", "--disable-blink-features=AutomationControlled"];

  if (process.platform === "win32") {
    args.push("--start-minimized", "--window-position=-32000,-32000");
  }

  if (process.platform === "linux") {
    args.push("--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage");
  }

  return args;
}

function getCachedPvzpRecord(key) {
  return PVZP_LOOKUP_CACHE.get(key) || null;
}

function setCachedPvzpRecord(key, record) {
  PVZP_LOOKUP_CACHE.set(key, record, PVZP_CACHE_TTL_MS);
}

function getCachedUniqaRecord(key) {
  return UNIQA_LOOKUP_CACHE.get(key) || null;
}

function setCachedUniqaRecord(key, record) {
  UNIQA_LOOKUP_CACHE.set(key, record, UNIQA_CACHE_TTL_MS);
}

async function attachPublicRegistryState(record) {
  if (!record || typeof record !== "object") {
    return record;
  }

  await ensureOpenDataPersistentCachesLoaded();

  const plate = normalizeWhitespace(extractIdentifier(record, "SPZ")).toUpperCase() || null;
  const vin = normalizeWhitespace(extractIdentifier(record, "VIN")).toUpperCase() || null;
  let pcv = normalizeWhitespace(extractIdentifier(record, "PČV")) || null;

  if (!pcv && vin) {
    pcv = getPersistentPcv(vin) || null;
  }

  if (!pcv && vin) {
    try {
      pcv = await resolvePcvForVin(vin);
      if (pcv) {
        await storePersistentPcv(vin, pcv);
      }
    } catch (error) {
      pcv = null;
    }
  }

  const [taxi, policeWanted, importRecord, deregistration] = await Promise.all([
    plate ? lookupTaxiVehicleRegistration(plate) : Promise.resolve(null),
    plate || vin ? lookupPoliceWantedVehicle({ plate, vin }) : Promise.resolve(null),
    pcv ? lookupImportedVehicleByPcv(pcv) : Promise.resolve(null),
    pcv ? lookupDeregisteredVehicleByPcv(pcv) : Promise.resolve(null)
  ]);

  const inspectionAudit = buildInspectionAudit(record.inspections || null, record.inspectionLookup || null, pcv);
  const nextRecord = clone(record);
  nextRecord.registryChecks = {
    taxi,
    policeWanted,
    importRecord,
    deregistration,
    inspectionAudit
  };

  return mergeRegistryStateIntoRecord(nextRecord);
}

async function lookupTaxiVehicleRegistration(plate) {
  const normalizedPlate = normalizeWhitespace(plate).toUpperCase();
  if (!normalizedPlate) {
    return null;
  }

  const cached = getTimedCacheValue(TAXI_LOOKUP_CACHE, normalizedPlate);
  if (cached !== undefined) {
    return cached;
  }

  try {
    const response = await requestJson("https://doprava.gov.cz/pd-api/rpsd/services/taxiApi/v1/checkVehicleRegistration", {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        registrationPlates: [normalizedPlate]
      }),
      timeoutMs: 15000
    });

    const resultEntry = Array.isArray(response?.registrationPlates)
      ? response.registrationPlates.find((entry) => normalizeWhitespace(entry?.registrationPlate).toUpperCase() === normalizedPlate)
      : null;
    const payload = {
      plate: normalizedPlate,
      status: normalizeTaxiResult(resultEntry?.result),
      result: resultEntry?.result || null,
      checkedAt: new Date().toISOString(),
      dataValidAsOf: response?.dataValidAsOf || null,
      sourceStatus: response?.status || null,
      sourceMessages: uniqueText(response?.statusMessagesCz || [])
    };

    setTimedCacheValue(TAXI_LOOKUP_CACHE, normalizedPlate, payload);
    return payload;
  } catch (error) {
    const payload = {
      plate: normalizedPlate,
      status: "error",
      checkedAt: new Date().toISOString(),
      detail: formatLookupError(error)
    };
    setTimedCacheValue(TAXI_LOOKUP_CACHE, normalizedPlate, payload);
    return payload;
  }
}

async function lookupPoliceWantedVehicle({ plate, vin }) {
  const normalizedPlate = normalizeWhitespace(plate).toUpperCase() || null;
  const normalizedVin = normalizeWhitespace(vin).toUpperCase() || null;
  const cacheKey = normalizedPlate || normalizedVin;

  if (!cacheKey) {
    return null;
  }

  const cached = getTimedCacheValue(POLICE_WANTED_CACHE, cacheKey);
  if (cached !== undefined) {
    return cached;
  }

  try {
    const initialHtml = await requestText("https://aplikace.policie.gov.cz/patrani-vozidla/", {
      method: "GET",
      headers: buildHtmlRequestHeaders("https://aplikace.policie.gov.cz/patrani-vozidla/")
    });
    const $initial = cheerio.load(initialHtml);
    const formBody = new URLSearchParams();

    $initial("input[type='hidden'][name]").each((_, input) => {
      const name = $initial(input).attr("name");
      if (name) {
        formBody.set(name, $initial(input).attr("value") || "");
      }
    });

    formBody.set("ctl00$Application$txtSPZ", normalizedPlate || "");
    formBody.set("ctl00$Application$txtVIN", normalizedVin || "");
    formBody.set("ctl00$Application$cmdHledej", "Vyhledat");
    if (!formBody.has("ctl00$Application$CurrentPage")) {
      formBody.set("ctl00$Application$CurrentPage", "1");
    }

    const html = await requestText("https://aplikace.policie.gov.cz/patrani-vozidla/", {
      method: "POST",
      headers: {
        ...buildHtmlRequestHeaders("https://aplikace.policie.gov.cz/patrani-vozidla/"),
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: formBody.toString(),
      timeoutMs: 20000
    });

    const payload = parsePoliceWantedResponse(html, { plate: normalizedPlate, vin: normalizedVin });
    setTimedCacheValue(POLICE_WANTED_CACHE, cacheKey, payload);
    return payload;
  } catch (error) {
    const payload = {
      query: normalizedPlate || normalizedVin,
      status: "error",
      checkedAt: new Date().toISOString(),
      detail: formatLookupError(error)
    };
    setTimedCacheValue(POLICE_WANTED_CACHE, cacheKey, payload);
    return payload;
  }
}

function parsePoliceWantedResponse(html, query) {
  const $ = cheerio.load(html || "");
  const output = normalizeWhitespace($("#Application_lblOutput").text());
  const sourceUpdatedAt = normalizeWhitespace($("#Application_lblAktualizace b").text()) || null;
  const listing = normalizeWhitespace($(".vypisZaznamu").text());
  const clear =
    output.toLowerCase().includes("nebyl nalezen") ||
    listing.toLowerCase().includes("nebyl nalezen");

  return {
    query: query?.plate || query?.vin || null,
    status: clear ? "clear" : "wanted",
    checkedAt: new Date().toISOString(),
    sourceUpdatedAt,
    detail: clear
      ? output || "V policejní evidenci nebylo nalezeno aktivní pátrání."
      : firstNonEmpty([
          output,
          listing.replace(output, "").trim(),
          "V policejní evidenci bylo nalezeno aktivní pátrání."
        ])
  };
}

async function lookupImportedVehicleByPcv(pcv) {
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!normalizedPcv) {
    return null;
  }

  const cached = getTimedCacheValue(OPEN_DATA_IMPORT_CACHE, normalizedPcv);
  if (cached !== undefined) {
    return cached;
  }

  try {
    const dataset = await ensureOpenDataDatasetLocal("imports", OPEN_DATA_IMPORT_ROUTE);
    const payload = await findSingleOpenDataRowByPcv(dataset.localPath, normalizedPcv, normalizeImportRow);
    const result = payload
      ? {
          ...payload,
          sourceUpdatedAt: dataset.datasetDate || null,
          sourceFile: dataset.filename || null
        }
      : null;
    setTimedCacheValue(OPEN_DATA_IMPORT_CACHE, normalizedPcv, result);
    return result;
  } catch (error) {
    const payload = {
      pcv: normalizedPcv,
      status: "error",
      detail: formatLookupError(error)
    };
    setTimedCacheValue(OPEN_DATA_IMPORT_CACHE, normalizedPcv, payload);
    return payload;
  }
}

async function lookupDeregisteredVehicleByPcv(pcv) {
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!normalizedPcv) {
    return null;
  }

  const cached = getTimedCacheValue(OPEN_DATA_DEREG_CACHE, normalizedPcv);
  if (cached !== undefined) {
    return cached;
  }

  try {
    const dataset = await ensureOpenDataDatasetLocal("deregistered", OPEN_DATA_DEREG_ROUTE);
    const matches = await findOpenDataRowsByPcv(dataset.localPath, normalizedPcv, normalizeDeregisteredRow);
    const latest = matches.sort((left, right) => compareDatesDesc(left.dateFrom || left.dateTo, right.dateFrom || right.dateTo))[0] || null;
    const result = latest
      ? {
          ...latest,
          active: isDeregisteredRecordActive(latest),
          sourceUpdatedAt: dataset.datasetDate || null,
          sourceFile: dataset.filename || null
        }
      : null;
    setTimedCacheValue(OPEN_DATA_DEREG_CACHE, normalizedPcv, result);
    return result;
  } catch (error) {
    const payload = {
      pcv: normalizedPcv,
      status: "error",
      detail: formatLookupError(error)
    };
    setTimedCacheValue(OPEN_DATA_DEREG_CACHE, normalizedPcv, payload);
    return payload;
  }
}

function buildInspectionAudit(inspections, inspectionLookup, pcv) {
  const summary = inspections?.summary || null;
  const sourceUpdatedAt = inspections?.sourceUpdatedAt || null;
  const status = inspectionLookup?.status || (summary ? "ready" : "unavailable");
  const currentStatus = summary?.status || null;
  const recordCount = summary?.totalCount || 0;
  const score = computeInspectionAuditScore({ status, currentStatus, recordCount, sourceUpdatedAt });

  return {
    status,
    currentStatus,
    recordCount,
    sourceUpdatedAt,
    lastKnownDate: summary?.lastKnownDate || null,
    pcv: normalizeWhitespace(pcv) || inspections?.pcv || null,
    score
  };
}

function computeInspectionAuditScore({ status, currentStatus, recordCount, sourceUpdatedAt }) {
  if (status !== "ready") {
    return null;
  }

  let score = 35;
  if (recordCount >= 3) {
    score += 20;
  } else if (recordCount >= 1) {
    score += 10;
  }

  const normalizedCurrentStatus = normalizeForMatch(currentStatus);
  if (normalizedCurrentStatus.includes("plat")) {
    score += 30;
  } else if (normalizedCurrentStatus.includes("konci")) {
    score += 10;
  }

  if (sourceUpdatedAt && diffDaysFromToday(sourceUpdatedAt) !== null && Math.abs(diffDaysFromToday(sourceUpdatedAt)) <= 40) {
    score += 15;
  }

  return Math.max(0, Math.min(100, score));
}

function mergeRegistryStateIntoRecord(record) {
  if (!record || typeof record !== "object") {
    return record;
  }

  const nextRecord = clone(record);
  const checks = nextRecord.registryChecks || {};

  if (checks.taxi) {
    nextRecord.highlights = upsertHighlight(
      nextRecord.highlights,
      "Taxi",
      formatTaxiBadge(checks.taxi),
      mapStatusTone(checks.taxi.status, { valid: "positive", error: "warning", defaultTone: "neutral" })
    );
  }

  if (checks.policeWanted) {
    nextRecord.highlights = upsertHighlight(
      nextRecord.highlights,
      "Patrani PCR",
      checks.policeWanted.status === "wanted" ? "Aktivní" : checks.policeWanted.status === "clear" ? "Bez záznamu" : "Neověřeno",
      mapStatusTone(checks.policeWanted.status, { wanted: "danger", error: "warning", defaultTone: "neutral" })
    );
  }

  if (checks.deregistration?.active) {
    nextRecord.hero = {
      ...nextRecord.hero,
      status: "Vyřazeno z provozu"
    };
  } else if (checks.policeWanted?.status === "wanted") {
    nextRecord.hero = {
      ...nextRecord.hero,
      status: "Aktivní pátrání"
    };
  }

  nextRecord.sections = upsertSectionItems(nextRecord.sections, "Veřejné registry", [
    checks.taxi ? item("Evidence taxi", formatTaxiSectionValue(checks.taxi), mapStatusTone(checks.taxi.status, { valid: "positive", error: "warning", defaultTone: "neutral" })) : null,
    checks.policeWanted ? item("Pátrání PČR", formatPoliceWantedValue(checks.policeWanted), mapStatusTone(checks.policeWanted.status, { wanted: "danger", error: "warning", defaultTone: "neutral" })) : null,
    checks.importRecord ? item("Dovoz vozidla", formatImportRecordValue(checks.importRecord)) : null,
    checks.deregistration ? item("Vyřazení z provozu", formatDeregistrationValue(checks.deregistration), checks.deregistration.active ? "warning" : "neutral") : null
  ]);

  nextRecord.sections = upsertSectionItems(nextRecord.sections, "Audit dat", [
    checks.inspectionAudit ? item("Audit STK", formatInspectionAuditValue(checks.inspectionAudit), checks.inspectionAudit.score >= 75 ? "positive" : checks.inspectionAudit.score >= 45 ? "warning" : "neutral") : null,
    checks.inspectionAudit?.recordCount ? item("Záznamy STK", String(checks.inspectionAudit.recordCount)) : null,
    checks.inspectionAudit?.sourceUpdatedAt ? item("Dataset STK", formatDate(checks.inspectionAudit.sourceUpdatedAt)) : null,
    checks.inspectionAudit?.pcv ? item("PČV audit", checks.inspectionAudit.pcv) : null
  ]);

  nextRecord.timeline = mergeTimeline(nextRecord.timeline || [], [
    checks.importRecord?.importDate
      ? {
          date: normalizeTimelineDate(checks.importRecord.importDate),
          title: "Dovoz vozidla",
          description: checks.importRecord.country ? `Stát dovozu ${checks.importRecord.country}` : "Vozidlo bylo evidováno jako dovezené.",
          tone: "neutral"
        }
      : null,
    checks.deregistration?.dateFrom
      ? {
          date: normalizeTimelineDate(checks.deregistration.dateFrom),
          title: "Vyřazení z provozu",
          description: formatDeregistrationValue(checks.deregistration),
          tone: checks.deregistration.active ? "warning" : "neutral"
        }
      : null
  ].filter(Boolean));

  return nextRecord;
}

async function lookupVehicleInspections(params = {}) {
  await ensureOpenDataPersistentCachesLoaded();

  const queryLookup = params.query ? parseLookupQuery(params.query) : null;
  const vin =
    normalizeWhitespace(params.vin || (queryLookup?.type === "vin" ? queryLookup.compact : "")).toUpperCase() ||
    null;
  const providedPcv = normalizeWhitespace(params.pcv) || null;
  let resolvedPcv = providedPcv || (vin ? getPersistentPcv(vin) : null);
  if (!resolvedPcv && vin) {
    resolvedPcv = await resolveIndexedPcvForVin(vin);
  }
  const officialInspectionHintRecord = vin
    ? await lookupFromOfficialVinApiWithBudget({ type: "vin", raw: vin, compact: vin }, null, true).catch(() => null)
    : null;
  const openDataHintPayload = vin || resolvedPcv
    ? await queryOpenDataVehicleByIdentifiers({ vin, pcv: resolvedPcv }).catch(() => null)
    : null;
  const openDataHintRecord = openDataHintPayload?.summary
    ? buildVehicleRecordFromOpenDataSummary(openDataHintPayload.summary, openDataHintPayload)
    : null;
  const inspectionHintRecord = mergeSupplementalRecord(officialInspectionHintRecord, openDataHintRecord);
  const cachedInspections = resolvedPcv ? getPersistentInspections(resolvedPcv) : null;

  if (resolvedPcv) {
    const databaseInspections = withResolvedInspectionPcv(
      await enrichInspectionsWithMileage(
        await lookupInspectionsFromDatabaseByPcv(resolvedPcv).catch(() => null),
        vin,
        inspectionHintRecord
      ),
      resolvedPcv
    );
    if (databaseInspections) {
      await storePersistentInspections(resolvedPcv, databaseInspections);
      return buildInspectionLookupState("ready", vin, resolvedPcv, databaseInspections);
    }
  }

  if (!resolvedPcv && vin) {
    const databaseInspectionsByVin = await lookupInspectionsFromDatabaseByVin(vin).catch(() => null);
    if (databaseInspectionsByVin) {
      resolvedPcv = databaseInspectionsByVin.pcv || null;
      if (resolvedPcv) {
        await storePersistentInspections(resolvedPcv, databaseInspectionsByVin);
      }
      return buildInspectionLookupState("ready", vin, resolvedPcv, databaseInspectionsByVin);
    }
  }

  if (cachedInspections) {
    const enrichedCachedInspections = withResolvedInspectionPcv(
      await enrichInspectionsWithMileage(cachedInspections, vin, inspectionHintRecord),
      resolvedPcv
    );
    return buildInspectionLookupState("ready", vin, resolvedPcv, enrichedCachedInspections);
  }

  const odometerOnlyInspections = vin
    ? withResolvedInspectionPcv(await enrichInspectionsWithMileage(null, vin, inspectionHintRecord), resolvedPcv)
    : null;
  if (odometerOnlyInspections) {
    return buildInspectionLookupState("ready", vin, resolvedPcv, odometerOnlyInspections);
  }

  if (vin || resolvedPcv) {
    if (!ALLOW_RUNTIME_OPEN_DATA_INSPECTION_SCAN) {
      return buildInspectionLookupState(
        "unavailable",
        vin,
        resolvedPcv,
        null,
        "Detailní záznamy STK nejsou v dostupném indexu."
      );
    }

    scheduleInspectionHydration({ vin, pcv: resolvedPcv });
    return buildInspectionLookupState("pending", vin, resolvedPcv, null);
  }

  return buildInspectionLookupState(
    "unavailable",
    vin,
    resolvedPcv,
    null,
    "Zadejte VIN nebo PCV pro dohledání technických prohlídek."
  );
}

function buildInspectionLookupState(status, vin, pcv, inspections, message = null) {
  return {
    status,
    vin: vin || null,
    pcv: pcv || null,
    inspections: status === "ready" ? inspections : null,
    message: message || null,
    resolvedAt: new Date().toISOString()
  };
}

async function lookupVignette(params = {}) {
  const rawPlate = firstNonEmpty([params.plate, params.query]);
  const plateLookup = rawPlate ? parseLookupQuery(rawPlate, "plate") : null;
  const plate = plateLookup?.type === "plate" ? plateLookup.compact : normalizePlateForVignette(rawPlate);
  const country = normalizeWhitespace(params.country || VIGNETTE_LOOKUP_COUNTRY).toUpperCase() || VIGNETTE_LOOKUP_COUNTRY;

  if (!plate) {
    return buildVignetteLookupState("unavailable", {
      plate: null,
      country,
      message: "Zadejte SPZ pro overeni dalnicni znamky."
    });
  }

  if (!VIGNETTE_LOOKUP_URL && !EDALNICE_LOOKUP_ENABLED) {
    return buildVignetteLookupState("unconfigured", {
      plate,
      country,
      message: "Overeni dalnicni znamky neni nakonfigurovane."
    });
  }

  const cacheKey = `${country}:${plate}`;
  const cached = VIGNETTE_LOOKUP_CACHE.get(cacheKey);
  if (cached) {
    return cached;
  }

  try {
    const payload = VIGNETTE_LOOKUP_URL
      ? await requestVignettePayload({ plate, country })
      : await requestEdalniceVignettePayload({ plate, country });
    const result = normalizeVignettePayload(payload, { plate, country });
    VIGNETTE_LOOKUP_CACHE.set(cacheKey, result);
    return result;
  } catch (error) {
    const result = buildVignetteLookupState("error", {
      plate,
      country,
      message: "Nepodarilo se overit dalnicni znamku.",
      detail: formatLookupError(error)
    });
    VIGNETTE_LOOKUP_CACHE.set(cacheKey, result, Math.min(VIGNETTE_CACHE_TTL_MS || 60000, 60000));
    return result;
  }
}

async function requestVignettePayload({ plate, country }) {
  const apiKey = normalizeWhitespace(process.env.VIGNETTE_LOOKUP_API_KEY || "");
  const apiKeyHeader = normalizeWhitespace(process.env.VIGNETTE_LOOKUP_API_KEY_HEADER || "X-API-Key") || "X-API-Key";
  const headers = {
    Accept: "application/json"
  };

  if (apiKey) {
    headers[apiKeyHeader] = apiKey;
  }

  if (VIGNETTE_LOOKUP_METHOD === "POST") {
    const body = JSON.stringify({
      [VIGNETTE_LOOKUP_PLATE_PARAM]: plate,
      [VIGNETTE_LOOKUP_COUNTRY_PARAM]: country
    });
    headers["Content-Type"] = "application/json";
    headers["Content-Length"] = Buffer.byteLength(body);
    return await requestJson(VIGNETTE_LOOKUP_URL, {
      method: "POST",
      headers,
      body,
      timeoutMs: VIGNETTE_LOOKUP_TIMEOUT_MS
    });
  }

  const targetUrl = new URL(VIGNETTE_LOOKUP_URL);
  targetUrl.searchParams.set(VIGNETTE_LOOKUP_PLATE_PARAM, plate);
  targetUrl.searchParams.set(VIGNETTE_LOOKUP_COUNTRY_PARAM, country);
  return await requestJson(targetUrl.toString(), {
    method: "GET",
    headers,
    timeoutMs: VIGNETTE_LOOKUP_TIMEOUT_MS
  });
}

async function requestEdalniceVignettePayload({ plate, country }) {
  const config = await getEdalniceConfig();
  const countryId = resolveEdalniceCountryId(country, config);
  const token = await getEdalniceAccessToken(config);
  const apiUrl = normalizeWhitespace(process.env.EDALNICE_API_URL || config.REACT_APP_API_URL);

  if (!apiUrl || !countryId || !token?.accessToken) {
    throw new Error("eDalnice provider neni kompletne nakonfigurovany.");
  }

  const targetUrl = `${apiUrl.replace(/\/+$/, "")}/api/v3/charge_registrations/${encodeURIComponent(countryId)}/${encodeURIComponent(plate)}`;
  const payload = await requestJson(targetUrl, {
    method: "GET",
    timeoutMs: VIGNETTE_LOOKUP_TIMEOUT_MS,
    headers: {
      Accept: "application/json",
      "Accept-Language": "cs",
      Authorization: `${token.tokenType || "Bearer"} ${token.accessToken}`,
      "User-Agent": "Mozilla/5.0 AutoInfo/1.0"
    }
  });

  return {
    ...payload,
    sourceLabel: "eDalnice",
    sourceHost: extractUrlHost(apiUrl),
    countryCode: country,
    countryId
  };
}

async function getEdalniceConfig() {
  const now = Date.now();
  if (EDALNICE_CONFIG_CACHE.value && EDALNICE_CONFIG_CACHE.expiresAt > now) {
    return EDALNICE_CONFIG_CACHE.value;
  }

  const configFromEnv = buildEdalniceConfigFromEnv();
  if (isCompleteEdalniceConfig(configFromEnv)) {
    EDALNICE_CONFIG_CACHE.value = configFromEnv;
    EDALNICE_CONFIG_CACHE.expiresAt = now + EDALNICE_CONFIG_CACHE_TTL_MS;
    return configFromEnv;
  }

  const html = await requestText(EDALNICE_CONFIG_URL, {
    method: "GET",
    timeoutMs: VIGNETTE_LOOKUP_TIMEOUT_MS,
    headers: {
      Accept: "text/html,application/xhtml+xml",
      "User-Agent": "Mozilla/5.0 AutoInfo/1.0"
    }
  });
  const match = String(html || "").match(/window\.edazConfig\s*=\s*(\{[\s\S]*?\})<\/script>/);
  if (!match) {
    throw new Error("eDalnice konfigurace nebyla ve strance nalezena.");
  }

  const config = {
    ...JSON.parse(match[1]),
    ...configFromEnv
  };
  if (!isCompleteEdalniceConfig(config)) {
    throw new Error("eDalnice konfigurace neobsahuje API nebo OAuth udaje.");
  }

  EDALNICE_CONFIG_CACHE.value = config;
  EDALNICE_CONFIG_CACHE.expiresAt = now + EDALNICE_CONFIG_CACHE_TTL_MS;
  return config;
}

function buildEdalniceConfigFromEnv() {
  const config = {
    REACT_APP_API_URL: normalizeWhitespace(process.env.EDALNICE_API_URL || ""),
    REACT_APP_TOKEN_URL: normalizeWhitespace(process.env.EDALNICE_TOKEN_URL || ""),
    REACT_APP_CLIENT_ID: normalizeWhitespace(process.env.EDALNICE_CLIENT_ID || ""),
    REACT_APP_CLIENT_SECRET: normalizeWhitespace(process.env.EDALNICE_CLIENT_SECRET || ""),
    REACT_APP_SCOPE: normalizeWhitespace(process.env.EDALNICE_SCOPE || ""),
    REACT_APP_ESHOP_PAYMENT_API_SCOPE: normalizeWhitespace(process.env.EDALNICE_PAYMENT_SCOPE || ""),
    REACT_APP_CZECH_ID: normalizeWhitespace(process.env.EDALNICE_CZECH_COUNTRY_ID || "")
  };

  return Object.fromEntries(Object.entries(config).filter(([, value]) => Boolean(value)));
}

function isCompleteEdalniceConfig(config) {
  return Boolean(
    config?.REACT_APP_API_URL &&
      config?.REACT_APP_TOKEN_URL &&
      config?.REACT_APP_CLIENT_ID &&
      config?.REACT_APP_CLIENT_SECRET &&
      config?.REACT_APP_SCOPE &&
      config?.REACT_APP_CZECH_ID
  );
}

async function getEdalniceAccessToken(config) {
  const now = Date.now();
  if (EDALNICE_TOKEN_CACHE.value && EDALNICE_TOKEN_CACHE.expiresAt > now) {
    return EDALNICE_TOKEN_CACHE.value;
  }

  const scope = uniqueText([
    config.REACT_APP_SCOPE,
    config.REACT_APP_ESHOP_PAYMENT_API_SCOPE
  ]).join(" ");
  const body = new URLSearchParams({
    grant_type: "client_credentials",
    client_id: config.REACT_APP_CLIENT_ID,
    client_secret: config.REACT_APP_CLIENT_SECRET,
    scope
  }).toString();
  const tokenPayload = await requestJson(config.REACT_APP_TOKEN_URL, {
    method: "POST",
    timeoutMs: VIGNETTE_LOOKUP_TIMEOUT_MS,
    headers: {
      Accept: "application/json",
      "Content-Type": "application/x-www-form-urlencoded",
      "Content-Length": Buffer.byteLength(body),
      "User-Agent": "Mozilla/5.0 AutoInfo/1.0"
    },
    body
  });
  const accessToken = normalizeWhitespace(tokenPayload?.access_token);
  if (!accessToken) {
    throw new Error("eDalnice token response neobsahuje access_token.");
  }

  const expiresInMs = Math.max(60000, Number(tokenPayload?.expires_in || 3600) * 1000 - EDALNICE_TOKEN_REFRESH_OFFSET_MS);
  const token = {
    accessToken,
    tokenType: normalizeWhitespace(tokenPayload?.token_type) || "Bearer"
  };
  EDALNICE_TOKEN_CACHE.value = token;
  EDALNICE_TOKEN_CACHE.expiresAt = now + expiresInMs;
  return token;
}

function resolveEdalniceCountryId(country, config) {
  const normalized = normalizeWhitespace(country).toUpperCase();
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(normalized)) {
    return normalized.toLowerCase();
  }

  if (!normalized || normalized === "CZ" || normalized === "CZE") {
    return config.REACT_APP_CZECH_ID;
  }

  throw new Error(`eDalnice provider zatim podporuje jen CZ, ne ${normalized}.`);
}

function normalizeVignettePayload(payload, context) {
  if (isEdalniceVignettePayload(payload)) {
    return normalizeEdalniceVignettePayload(payload, context);
  }

  const validFrom = normalizeVignetteDate(firstNonEmpty([
    extractFirstValueByKey(payload, ["validFrom", "validityFrom", "platnostOd", "dateFrom", "from", "startDate", "start"]),
    getByPath(payload, "data.validFrom")
  ]));
  const validUntil = normalizeVignetteDate(firstNonEmpty([
    extractFirstValueByKey(payload, ["validUntil", "validTo", "validityTo", "platnostDo", "dateTo", "to", "expiresAt", "expirationDate", "endDate", "end"]),
    getByPath(payload, "data.validUntil")
  ]));
  const rawStatus = firstNonEmpty([
    extractFirstValueByKey(payload, ["status", "state", "result", "validityStatus"]),
    getByPath(payload, "data.status")
  ]);
  const exempt = coerceBoolean(firstNonEmpty([
    extractFirstValueByKey(payload, ["exempt", "exempted", "isExempt", "exemption", "osvobozeno"]),
    normalizeForMatch(rawStatus).includes("osvobo") ? true : null
  ]));
  const explicitValid = coerceBoolean(firstNonEmpty([
    extractFirstValueByKey(payload, ["valid", "isValid", "active", "isActive", "hasValidVignette", "paid", "paymentValid"]),
    rawStatus
  ]));
  const valid = exempt === true ? true : explicitValid !== null ? explicitValid : inferVignetteValidity(validFrom, validUntil);
  const sourceCandidate = firstNonEmpty([
    extractFirstValueByKey(payload, ["sourceLabel", "provider", "source"]),
    "Vignette provider"
  ]);
  const sourceLabel = typeof sourceCandidate === "string" ? normalizeWhitespace(sourceCandidate) : "Vignette provider";
  const message = normalizeWhitespace(firstNonEmpty([
    extractFirstValueByKey(payload, ["message", "note", "description"]),
    rawStatus,
    valid === true
      ? exempt ? "Vozidlo je osvobozeno od dalnicni znamky." : "Dalnicni znamka je platna."
      : valid === false
        ? "Dalnicni znamka neni platna."
        : "Provider nevratil jednoznacny stav dalnicni znamky."
  ]));

  return buildVignetteLookupState("ready", {
    ...context,
    valid,
    exempt: exempt === true,
    validFrom,
    validUntil,
    rawStatus: rawStatus ? String(rawStatus) : null,
    sourceLabel,
    message
  });
}

function isEdalniceVignettePayload(payload) {
  return Boolean(
    payload &&
      typeof payload === "object" &&
      (Array.isArray(payload.charges) ||
        Object.prototype.hasOwnProperty.call(payload, "isGivenExemption") ||
        payload.vehicle?.licensePlate)
  );
}

function normalizeEdalniceVignettePayload(payload, context) {
  const charges = Array.isArray(payload.charges) ? payload.charges : [];
  const currentCharges = charges
    .filter((charge) => coerceBoolean(charge?.isCurrentlyValid) === true)
    .sort((left, right) => parseVignetteDateTime(right?.validUntil, true) - parseVignetteDateTime(left?.validUntil, true));
  const upcomingCharges = charges
    .filter((charge) => coerceBoolean(charge?.isCurrentlyValid) !== true)
    .sort((left, right) => parseVignetteDateTime(left?.validSince, false) - parseVignetteDateTime(right?.validSince, false));
  const selectedCharge = currentCharges[0] || upcomingCharges[0] || null;
  const exempt = payload.isGivenExemption === true;
  const possibleExemptionReasonIds = Array.isArray(payload.possibleExemptionReasonIds)
    ? payload.possibleExemptionReasonIds.filter(Boolean)
    : [];
  const valid = exempt || currentCharges.length > 0;
  const rawStatus = exempt
    ? "exempted"
    : currentCharges.length > 0
      ? "valid"
      : upcomingCharges.length > 0
        ? "future"
        : possibleExemptionReasonIds.length > 0
          ? "possible_exemption"
          : "invalid";
  const message = exempt
    ? "Vozidlo je podle eDalnice osvobozeno od dalnicniho poplatku."
    : currentCharges.length > 0
      ? "Dalnicni znamka je podle eDalnice platna."
      : upcomingCharges.length > 0
        ? "eDalnice eviduje znamku s budouci platnosti."
        : possibleExemptionReasonIds.length > 0
          ? "eDalnice eviduje mozny duvod osvobozeni, ale ne platnou znamku."
          : "eDalnice neeviduje platnou dalnicni znamku.";

  return buildVignetteLookupState("ready", {
    ...context,
    configured: true,
    plate: payload.vehicle?.licensePlate || context.plate,
    country: payload.countryCode || context.country,
    valid,
    exempt,
    validFrom: normalizeVignetteDate(selectedCharge?.validSince),
    validUntil: normalizeVignetteDate(selectedCharge?.validUntil),
    rawStatus,
    sourceLabel: "eDalnice",
    sourceHost: payload.sourceHost || "eshop.edalnice.cz",
    message,
    chargeCount: charges.length,
    upcomingChargeCount: upcomingCharges.length,
    possibleExemptionReasonIds
  });
}

function buildVignetteLookupState(status, details = {}) {
  return {
    status,
    configured: typeof details.configured === "boolean" ? details.configured : Boolean(VIGNETTE_LOOKUP_URL) || EDALNICE_LOOKUP_ENABLED,
    plate: details.plate || null,
    country: details.country || VIGNETTE_LOOKUP_COUNTRY,
    valid: typeof details.valid === "boolean" ? details.valid : null,
    exempt: Boolean(details.exempt),
    validFrom: details.validFrom || null,
    validUntil: details.validUntil || null,
    rawStatus: details.rawStatus || null,
    chargeCount: Number.isFinite(Number(details.chargeCount)) ? Number(details.chargeCount) : null,
    upcomingChargeCount: Number.isFinite(Number(details.upcomingChargeCount)) ? Number(details.upcomingChargeCount) : null,
    possibleExemptionReasonIds: Array.isArray(details.possibleExemptionReasonIds) ? details.possibleExemptionReasonIds : [],
    message: details.message || null,
    detail: details.detail || null,
    source: {
      label: details.sourceLabel || "Dalnicni znamka",
      host: details.sourceHost || extractUrlHost(VIGNETTE_LOOKUP_URL || EDALNICE_CONFIG_URL)
    },
    checkedAt: new Date().toISOString()
  };
}

function normalizePlateForVignette(value) {
  const compact = String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
  return /^[A-Z0-9]{5,10}$/.test(compact) ? compact : null;
}

function normalizeVignetteDate(value) {
  const normalized = normalizeWhitespace(value);
  if (!normalized) {
    return null;
  }

  return normalizeTimelineDate(normalized);
}

function inferVignetteValidity(validFrom, validUntil) {
  const now = Date.now();
  const start = parseVignetteDateTime(validFrom, false);
  const end = parseVignetteDateTime(validUntil, true);

  if (!start && !end) {
    return null;
  }

  if (start && start > now) {
    return false;
  }

  if (end) {
    return end >= now;
  }

  return null;
}

function parseVignetteDateTime(value, endOfDay) {
  const normalized = normalizeWhitespace(value);
  if (!normalized) {
    return null;
  }

  const localized = normalized.match(/^(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})$/);
  if (localized) {
    const date = new Date(Number(localized[3]), Number(localized[2]) - 1, Number(localized[1]));
    if (endOfDay) {
      date.setHours(23, 59, 59, 999);
    }
    return date.getTime();
  }

  const isoDateOnly = normalized.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoDateOnly) {
    const date = new Date(Number(isoDateOnly[1]), Number(isoDateOnly[2]) - 1, Number(isoDateOnly[3]));
    if (endOfDay) {
      date.setHours(23, 59, 59, 999);
    }
    return date.getTime();
  }

  const parsed = Date.parse(normalized);
  return Number.isNaN(parsed) ? null : parsed;
}

function coerceBoolean(value) {
  if (typeof value === "boolean") {
    return value;
  }

  if (typeof value === "number") {
    return value !== 0;
  }

  const normalized = normalizeForMatch(value);
  if (!normalized) {
    return null;
  }

  if (
    normalized.includes("neplat") ||
    normalized.includes("invalid") ||
    normalized.includes("expired") ||
    normalized.includes("inactive") ||
    normalized.includes("not valid") ||
    normalized.includes("bez platne")
  ) {
    return false;
  }

  if (
    normalized === "1" ||
    normalized === "true" ||
    normalized === "yes" ||
    normalized === "ano" ||
    normalized.includes("platna") ||
    normalized.includes("platne") ||
    normalized.includes("valid") ||
    normalized.includes("active") ||
    normalized.includes("paid") ||
    normalized.includes("osvobo")
  ) {
    return true;
  }

  if (normalized === "0" || normalized === "false" || normalized === "no" || normalized === "ne") {
    return false;
  }

  return null;
}

function extractFirstValueByKey(value, keys, seen = new Set()) {
  if (!value || typeof value !== "object" || seen.has(value)) {
    return null;
  }

  seen.add(value);
  const normalizedKeys = new Set(keys.map((key) => normalizeForMatch(key)));
  const entries = Array.isArray(value) ? value.entries() : Object.entries(value);

  for (const entry of entries) {
    const [key, nestedValue] = Array.isArray(value) ? [String(entry[0]), entry[1]] : entry;
    if (normalizedKeys.has(normalizeForMatch(key)) && nestedValue !== null && nestedValue !== undefined && nestedValue !== "") {
      return nestedValue;
    }
  }

  for (const nestedValue of Array.isArray(value) ? value : Object.values(value)) {
    const nested = extractFirstValueByKey(nestedValue, keys, seen);
    if (nested !== null && nested !== undefined && nested !== "") {
      return nested;
    }
  }

  return null;
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
    resolvedPcv = await resolveIndexedPcvForVin(normalizedVin);
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

  const databaseInspections = await lookupInspectionsFromDatabaseByPcv(resolvedPcv).catch(() => null);
  if (databaseInspections) {
    await storePersistentInspections(resolvedPcv, databaseInspections);
    return databaseInspections;
  }

  const cachedInspections = getPersistentInspections(resolvedPcv);
  if (cachedInspections) {
    return cachedInspections;
  }

  if (!ALLOW_RUNTIME_OPEN_DATA_INSPECTION_SCAN) {
    return null;
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
  const [pcvRaw, inspectionRaw, ownershipRaw, datasetRaw] = await Promise.all([
    readJsonFile(OPEN_DATA_PCV_FILE),
    readJsonFile(OPEN_DATA_INSPECTION_FILE),
    readJsonFile(OPEN_DATA_OWNERSHIP_FILE),
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

  Object.entries(ownershipRaw || {}).forEach(([pcv, payload]) => {
    if (pcv && payload && isAllowedPersistentOwnership(payload)) {
      OPEN_DATA_PERSISTENT_OWNER_INDEX.set(pcv, payload);
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
          reject(new Error(`Open data metadata vrátila chybu ${response.statusCode || 500}.`));
          response.resume();
          return;
        }

        const header = response.headers["content-disposition"];
        const filename = parseContentDispositionFilename(header);
        response.resume();

        if (!filename) {
          reject(new Error("Nepodařilo se určit název otevřené datové sady."));
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
          reject(new Error(`Open data download vrátil chybu ${response.statusCode || 500}.`));
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

async function resolveIndexedPcvForVin(vin) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase();
  if (!normalizedVin) {
    return null;
  }

  const timed = getTimedCacheValue(OPEN_DATA_PCV_CACHE, normalizedVin);
  if (timed !== undefined) {
    return timed;
  }

  const persistent = getPersistentPcv(normalizedVin);
  if (persistent) {
    setTimedCacheValue(OPEN_DATA_PCV_CACHE, normalizedVin, persistent);
    return persistent;
  }

  const databasePcv = await openDataDb.queryPcvByVin(normalizedVin).catch(() => null);
  if (databasePcv) {
    await storePersistentPcv(normalizedVin, databasePcv);
    return databasePcv;
  }

  const fleetDbPcv = await readFleetDbPcvByVin(normalizedVin).catch(() => null);
  if (fleetDbPcv) {
    await storePersistentPcv(normalizedVin, fleetDbPcv);
    return fleetDbPcv;
  }

  setTimedCacheValue(OPEN_DATA_PCV_CACHE, normalizedVin, null);
  return null;
}

function getPersistentInspections(pcv) {
  const value = OPEN_DATA_PERSISTENT_INSPECTION_INDEX.get(normalizeWhitespace(pcv));
  return value ? clone(value) : null;
}

function getPersistentOwnership(pcv) {
  const normalizedPcv = normalizeWhitespace(pcv);
  const cached = getTimedCacheValue(OPEN_DATA_OWNERSHIP_CACHE, normalizedPcv);
  if (cached !== undefined) {
    return cached ? normalizeOwnershipPayload(cached) : cached;
  }

  const value = OPEN_DATA_PERSISTENT_OWNER_INDEX.get(normalizedPcv);
  return value && isAllowedPersistentOwnership(value) ? normalizeOwnershipPayload(value) : null;
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

async function storePersistentOwnership(pcv, payload) {
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!normalizedPcv || !payload) {
    return;
  }

  if (!isAllowedPersistentOwnership(payload)) {
    return;
  }

  const normalizedPayload = normalizeOwnershipPayload(payload);
  if (!normalizedPayload) {
    return;
  }

  OPEN_DATA_PERSISTENT_OWNER_INDEX.set(normalizedPcv, clone(normalizedPayload));
  setTimedCacheValue(OPEN_DATA_OWNERSHIP_CACHE, normalizedPcv, normalizedPayload);
  await persistOpenDataCacheFiles();
}

function isAllowedPersistentOwnership(payload) {
  if (THIRD_PARTY_OWNERSHIP_FALLBACK_ENABLED) {
    return true;
  }

  const label = normalizeForMatch(payload?.source?.label);
  return !(
    label.includes("hlidac statu") ||
    label.includes("overeniauta") ||
    label.includes("overeni auta")
  );
}

function normalizeOwnershipPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return null;
  }

  const nextPayload = clone(payload);
  const parties = Array.isArray(nextPayload.ownership?.parties) ? nextPayload.ownership.parties : [];
  const cleanedParties = uniqueParties(
    parties
      .map((party) => ({
        ...party,
        role: normalizeVehicleRelation(party.role),
        ico: sanitizeIco(party.ico),
        name: normalizeWhitespace(party.name) || null,
        address: normalizeWhitespace(party.address) || null
      }))
      .filter(isLegalEntityParty)
  );

  if (parties.length > 0 && cleanedParties.length === 0) {
    return null;
  }

  nextPayload.ownership = {
    ...(nextPayload.ownership || {}),
    ownerCount: cleanedParties.length ? countRole(cleanedParties, "vlast") || null : nextPayload.ownership?.ownerCount || null,
    operatorCount: cleanedParties.length ? countRole(cleanedParties, "provoz") || null : nextPayload.ownership?.operatorCount || null,
    parties: cleanedParties
  };

  return nextPayload;
}

function isLegalEntityParty(party) {
  if (!party || typeof party !== "object") {
    return false;
  }

  const ico = sanitizeIco(party.ico);
  const type = normalizeForMatch(party.type || party.subjectType);
  const name = normalizeWhitespace(party.name);

  return Boolean(ico || ((type.includes("pravnick") || type === "company") && hasDisplayableOwnershipText(name)));
}

async function persistOpenDataCacheFiles() {
  if (openDataPersistPromise) {
    await openDataPersistPromise;
  }

  openDataPersistPromise = Promise.all([
    writeJsonFile(OPEN_DATA_PCV_FILE, Object.fromEntries(OPEN_DATA_PERSISTENT_PCV_INDEX)),
    writeJsonFile(OPEN_DATA_INSPECTION_FILE, Object.fromEntries(OPEN_DATA_PERSISTENT_INSPECTION_INDEX)),
    writeJsonFile(OPEN_DATA_OWNERSHIP_FILE, Object.fromEntries(OPEN_DATA_PERSISTENT_OWNER_INDEX))
  ]).finally(() => {
    openDataPersistPromise = null;
  });

  await openDataPersistPromise;
}

async function persistOpenDataDatasets() {
  await writeJsonFile(OPEN_DATA_DATASET_FILE, OPEN_DATA_DATASET_CACHE);
}

async function lookupVehiclesByIco(queryIco) {
  const ico = sanitizeIco(queryIco);
  const normalizedQuery = normalizeWhitespace(queryIco);

  if (!ico) {
    return buildEmptyCompanyFleetPayload(null, normalizedQuery, {
      normalized: normalizedQuery,
      message: "Zadané IČO nemá platný osmimístný formát."
    });
  }

  const cached = getTimedCacheValue(ICO_FLEET_CACHE, ico);
  if (cached !== undefined) {
    return cached;
  }

  const databasePayload = await lookupVehiclesByIcoFromDatabase(ico, normalizedQuery);
  if (databasePayload) {
    setTimedCacheValue(ICO_FLEET_CACHE, ico, databasePayload);
    return databasePayload;
  }

  if (shouldUseFleetDbFallback()) {
    const fleetDbPayload = await lookupVehiclesByIcoFromFleetDb(ico, normalizedQuery);
    if (fleetDbPayload) {
      setTimedCacheValue(ICO_FLEET_CACHE, ico, fleetDbPayload);
      return fleetDbPayload;
    }
  }

  if (ALLOW_RUNTIME_OPEN_DATA_ICO_SCAN) {
    const fallbackPayload = await lookupVehiclesByIcoDirectScan(queryIco);
    setTimedCacheValue(ICO_FLEET_CACHE, ico, fallbackPayload);
    return fallbackPayload;
  }

  return buildEmptyCompanyFleetPayload(ico, normalizedQuery, {
    message: "Databázový index otevřených dat není dostupný nebo zatím nemá aktivní ownership dataset."
  });
}

function buildEmptyCompanyFleetPayload(ico, rawQuery, options = {}) {
  const normalized = options.normalized || ico || normalizeWhitespace(rawQuery);
  return {
    kind: "fleet",
    query: {
      raw: normalizeWhitespace(rawQuery),
      normalized,
      type: "ico",
      resolvedAt: new Date().toISOString()
    },
    company: ico
      ? {
          ico,
          name: null,
          address: null
        }
      : null,
    message: options.message || null,
    summary: {
      vehicleCount: 0,
      displayedCount: 0,
      activeVehicleCount: 0,
      historicalVehicleCount: 0,
      companyHistoryVehicleCount: 0,
      currentVehicleCount: 0,
      relationshipCount: 0,
      truncated: false,
      sourceUpdatedAt: null
    },
    records: [],
    historyRecords: [],
    companyHistoryRecords: []
  };
}

function hasFleetPayloadRows(payload) {
  if (!payload || typeof payload !== "object") {
    return false;
  }

  return [
    payload.relations,
    payload.summaries,
    payload.historicalRelations,
    payload.historicalSummaries,
    payload.companyHistoryRelations,
    payload.companyHistorySummaries
  ].some((value) => Array.isArray(value) && value.length > 0) ||
    Number(payload.candidateCount || 0) > 0 ||
    Number(payload.allVehicleCount || 0) > 0 ||
    Number(payload.historicalVehicleCount || 0) > 0 ||
    Number(payload.companyHistoryVehicleCount || 0) > 0;
}

async function lookupVehiclesByIcoDirectScan(queryIco) {
  const ico = sanitizeIco(queryIco);
  const normalizedQuery = normalizeWhitespace(queryIco);

  if (!ico) {
    return buildEmptyCompanyFleetPayload(null, normalizedQuery, { normalized: normalizedQuery });
  }

  const cached = getTimedCacheValue(ICO_FLEET_CACHE, ico);
  if (cached !== undefined) {
    return cached;
  }

  await ensureOpenDataPersistentCachesLoaded();

  const company = await resolveCompanyDetailsForIco(ico, []);
  const companyNameKey = normalizeCompanyNameForMatch(company.name);
  const relations = [];
  let sourceUpdatedAt = null;

  await scanOpenDataCsv(OPEN_DATA_OWNER_ROUTE, ({ row, canonicalRow, metadata }) => {
    if (sourceUpdatedAt === null) {
      sourceUpdatedAt = metadata?.datasetDate || null;
    }

    const rowIco = sanitizeIco(firstNonEmpty([canonicalRow.ICO, row["IČO"]]));
    const rowName = normalizeWhitespace(firstNonEmpty([canonicalRow.NAZEV, row["Název"]]));
    const rowNameKey = normalizeCompanyNameForMatch(rowName);
    const matchesIco = rowIco === ico;
    const matchesAresName = !rowIco && companyNameKey && rowNameKey === companyNameKey;

    if (!matchesIco && !matchesAresName) {
      return false;
    }

    const relation = normalizeCompanyVehicleRelation(row, canonicalRow);
    if (matchesAresName && !relation.ico) {
      relation.ico = ico;
    }
    relations.push(relation);
    return false;
  });

  const groupedRecords = Array.from(
    relations.reduce((map, relation) => {
      const key = relation.pcv || `${relation.relation}-${relation.dateFrom}-${relation.dateTo}`;
      if (!map.has(key)) {
        map.set(key, {
          pcv: relation.pcv || null,
          title: relation.pcv ? `Vozidlo ${relation.pcv}` : "Vozidlo bez PČV",
          current: Boolean(relation.current),
          firstSeen: relation.dateFrom || null,
          lastSeen: relation.dateTo || null,
          relations: []
        });
      }

      const current = map.get(key);
      current.current = current.current || Boolean(relation.current);
      current.firstSeen = current.firstSeen ? (compareDatesDesc(current.firstSeen, relation.dateFrom) > 0 ? current.firstSeen : relation.dateFrom || current.firstSeen) : relation.dateFrom || null;
      current.lastSeen = current.lastSeen ? (compareDatesDesc(current.lastSeen, relation.dateTo) < 0 ? current.lastSeen : relation.dateTo || current.lastSeen) : relation.dateTo || null;
      current.relations.push(relation);
      return map;
    }, new Map()).values()
  );

  const records = buildCompanyFleetRecords(relations.map(normalizeFleetDbRelationRow), new Map(), ico);
  const relationshipCount = countActiveFleetRelationshipsForIco(records, ico);
  const limited = limitCompanyFleetRecords(records);
  const resolvedCompany = company.name || company.address ? company : await resolveCompanyDetailsForIco(ico, relations);

  const coverage = calculateFleetRecordCoverage(limited.records);
  const payload = {
    kind: "fleet",
    query: {
      raw: normalizedQuery,
      normalized: ico,
      type: "ico",
      resolvedAt: new Date().toISOString()
    },
    company: {
      ico,
      name: resolvedCompany.name,
      address: resolvedCompany.address
    },
    summary: {
      vehicleCount: limited.totalCount,
      displayedCount: limited.records.length,
	      activeVehicleCount: limited.totalCount,
	      historicalVehicleCount: 0,
	      companyHistoryVehicleCount: 0,
	      currentVehicleCount: limited.totalCount,
	      relationshipCount,
        plateCount: coverage.plateCount,
        missingPlateCount: coverage.missingPlateCount,
        inspectionCount: coverage.inspectionCount,
        missingInspectionCount: coverage.missingInspectionCount,
	      truncated: limited.truncated,
	      sourceUpdatedAt
	    },
	    records: limited.records,
	    historyRecords: [],
	    companyHistoryRecords: []
  };

  setTimedCacheValue(ICO_FLEET_CACHE, ico, payload);
  return payload;
}

async function lookupVehiclesByIcoFromDatabase(ico, normalizedQuery) {
  const aresCompany = await resolveCompanyDetailsForIco(ico, []);
  const companyNames = [aresCompany.name].filter(Boolean);
  const payload = await openDataDb.queryVehiclesByIco(ico, {
    limit: ICO_FLEET_MAX_RECORDS,
    companyNames
  }).catch(() => null);
  if (!payload) {
    return null;
  }
  if (!hasFleetPayloadRows(payload)) {
    return null;
  }

	  const relations = payload.relations.map(normalizeFleetDbRelationRow);
		  const historicalRelations = Array.isArray(payload.historicalRelations)
		    ? payload.historicalRelations.map(normalizeFleetDbRelationRow)
		    : [];
		  const companyHistoryRelations = Array.isArray(payload.companyHistoryRelations)
		    ? payload.companyHistoryRelations.map(normalizeFleetDbRelationRow)
		    : [];
		  const companyRelations = relations.filter((relation) => relationBelongsToIco(relation, ico));
		  const company = aresCompany.name || aresCompany.address
		    ? aresCompany
		    : await resolveCompanyDetailsForIco(ico, companyRelations);
	  const summaryMap = new Map();
  payload.summaries.forEach((summary) => {
    if (summary?.pcv && !summaryMap.has(normalizeWhitespace(summary.pcv))) {
	      summaryMap.set(normalizeWhitespace(summary.pcv), summary);
	    }
	  });
	  const historicalSummaryMap = new Map(summaryMap);
		  (payload.historicalSummaries || []).forEach((summary) => {
		    if (summary?.pcv && !historicalSummaryMap.has(normalizeWhitespace(summary.pcv))) {
		      historicalSummaryMap.set(normalizeWhitespace(summary.pcv), summary);
		    }
		  });
		  const companyHistorySummaryMap = new Map(historicalSummaryMap);
		  (payload.companyHistorySummaries || []).forEach((summary) => {
		    if (summary?.pcv && !companyHistorySummaryMap.has(normalizeWhitespace(summary.pcv))) {
		      companyHistorySummaryMap.set(normalizeWhitespace(summary.pcv), summary);
		    }
		  });
			  const records = buildCompanyFleetRecords(relations, summaryMap, ico);
			  const limited = limitCompanyFleetRecords(records, payload.candidateCount);
			  const relationshipCount = countActiveFleetRelationshipsForIco(records, ico);
			  const historicalVehicleCount = Number.isFinite(Number(payload.historicalVehicleCount))
			    ? Number(payload.historicalVehicleCount)
			    : 0;
			  const companyHistoryVehicleCount = Number.isFinite(Number(payload.companyHistoryVehicleCount))
			    ? Number(payload.companyHistoryVehicleCount)
			    : historicalVehicleCount;
			  const historicalRecords = buildHistoricalCompanyFleetRecords(historicalRelations, historicalSummaryMap, ico);
			  const historicalLimited = limitCompanyFleetRecords(historicalRecords, historicalVehicleCount);
			  const companyHistoryRecords = buildHistoricalCompanyFleetRecords(companyHistoryRelations, companyHistorySummaryMap, ico);
				  const companyHistoryLimited = limitCompanyFleetRecords(companyHistoryRecords, companyHistoryVehicleCount);

          const activeCoverage = calculateFleetRecordCoverage(limited.records);
          const historicalCoverage = calculateFleetRecordCoverage(historicalLimited.records);
          const companyHistoryCoverage = calculateFleetRecordCoverage(companyHistoryLimited.records);
		  const response = {
    kind: "fleet",
    query: {
      raw: normalizedQuery,
      normalized: ico,
      type: "ico",
      resolvedAt: new Date().toISOString()
    },
    company: {
      ico,
      name: company.name,
      address: company.address
    },
	    message:
	      limited.totalCount > 0 || companyHistoryVehicleCount > 0
        ? null
        : "Firma je dohledaná v ARES, ale v importovaném veřejném RSV datasetu vlastník/provozovatel pro ni není žádná vazba na vozidlo.",
    summary: {
      vehicleCount: limited.totalCount,
	      displayedCount: limited.records.length,
	      activeVehicleCount: limited.totalCount,
	      historicalVehicleCount,
	      companyHistoryVehicleCount,
			      allVehicleCount: Number(payload.allVehicleCount || limited.totalCount + historicalVehicleCount),
			      currentVehicleCount: limited.totalCount,
			      relationshipCount,
              plateCount: activeCoverage.plateCount,
              missingPlateCount: activeCoverage.missingPlateCount,
              inspectionCount: activeCoverage.inspectionCount,
              missingInspectionCount: activeCoverage.missingInspectionCount,
			      historicalDisplayedCount: historicalLimited.records.length,
              historicalPlateCount: historicalCoverage.plateCount,
              historicalMissingPlateCount: historicalCoverage.missingPlateCount,
              historicalInspectionCount: historicalCoverage.inspectionCount,
              historicalMissingInspectionCount: historicalCoverage.missingInspectionCount,
			      companyHistoryDisplayedCount: companyHistoryLimited.records.length,
              companyHistoryPlateCount: companyHistoryCoverage.plateCount,
              companyHistoryMissingPlateCount: companyHistoryCoverage.missingPlateCount,
              companyHistoryInspectionCount: companyHistoryCoverage.inspectionCount,
              companyHistoryMissingInspectionCount: companyHistoryCoverage.missingInspectionCount,
			      truncated: Boolean(payload.truncated || limited.truncated),
		      historicalTruncated: historicalLimited.truncated,
		      companyHistoryTruncated: companyHistoryLimited.truncated,
		      sourceUpdatedAt: payload.sourceUpdatedAt || null
		    },
		    records: limited.records,
		    historyRecords: historicalLimited.records,
		    companyHistoryRecords: companyHistoryLimited.records
		  };

		  scheduleFleetPlateBackfill(ico, [
		    ...limited.records,
		    ...historicalLimited.records,
		    ...companyHistoryLimited.records
		  ]);

			  return response;
	}

function shouldUseFleetDbFallback() {
  if (FLEET_DB_FALLBACK_MODE === "false" || FLEET_DB_FALLBACK_MODE === "0" || FLEET_DB_FALLBACK_MODE === "off") {
    return false;
  }

  if (FLEET_DB_FALLBACK_MODE === "true" || FLEET_DB_FALLBACK_MODE === "1" || FLEET_DB_FALLBACK_MODE === "on") {
    return true;
  }

  return !openDataDb.isDatabaseConfigured();
}

async function lookupCompanyVehicleHistory(queryIco, queryPcv) {
  const ico = sanitizeIco(queryIco);
  const pcv = normalizeWhitespace(queryPcv);
  if (!ico || !pcv) {
    return {
      kind: "companyVehicleHistory",
      message: "Zadejte platné IČO a PČV.",
      company: ico ? { ico, name: null, address: null } : null,
      vehicle: pcv ? { pcv } : null,
      summary: {
        relationshipCount: 0,
        currentCount: 0,
        truncated: false,
        sourceUpdatedAt: null
      },
      relations: []
    };
  }

  const aresCompany = await resolveCompanyDetailsForIco(ico, []);
  const payload = await openDataDb.queryCompanyVehicleHistory(ico, pcv, {
    limit: 500,
    companyNames: [aresCompany.name].filter(Boolean)
  }).catch(() => null);
  if (!payload) {
    return {
      kind: "companyVehicleHistory",
      message: "Historie vazeb není v lokální DB dostupná.",
      company: { ico, name: null, address: null },
      vehicle: { pcv },
      summary: {
        relationshipCount: 0,
        currentCount: 0,
        truncated: false,
        sourceUpdatedAt: null
      },
      relations: []
    };
  }

  const relations = dedupeFleetRelations(payload.relations.map(normalizeFleetDbRelationRow));
  const company = aresCompany.name || aresCompany.address
    ? aresCompany
    : await resolveCompanyDetailsForIco(ico, relations);

  return {
    kind: "companyVehicleHistory",
    company: {
      ico,
      name: company.name,
      address: company.address
    },
    vehicle: payload.summary || { pcv },
    summary: {
      relationshipCount: relations.length,
      currentCount: relations.filter(isActiveFleetOwnershipRelation).length,
      truncated: Boolean(payload.truncated),
      sourceUpdatedAt: payload.sourceUpdatedAt || null
    },
    relations
  };
}

async function lookupVehicleHistory(params = {}) {
  const providedPcv = normalizeWhitespace(params.pcv);
  const vin = normalizeWhitespace(params.vin).toUpperCase();
  let pcv = providedPcv || null;
  if (!pcv && vin) {
    pcv = await resolveIndexedPcvForVin(vin).catch(() => null);
  }

  if (!pcv) {
    return {
      kind: "vehicleHistory",
      message: "Zadejte PČV nebo VIN, ze kterého lze PČV dohledat.",
      vehicle: vin ? { vin } : null,
      summary: {
        relationshipCount: 0,
        currentCount: 0,
        legalEntityCount: 0,
        anonymizedCount: 0,
        truncated: false,
        sourceUpdatedAt: null
      },
      parties: []
    };
  }

  const payload = await openDataDb.queryVehicleOwnershipHistory(pcv, { limit: 1000 }).catch(() => null);
  if (!payload) {
    return {
      kind: "vehicleHistory",
      message: "Historie vozidla není v lokální DB dostupná.",
      vehicle: { pcv, vin: vin || null },
      summary: {
        relationshipCount: 0,
        currentCount: 0,
        legalEntityCount: 0,
        anonymizedCount: 0,
        truncated: false,
        sourceUpdatedAt: null
      },
      parties: []
    };
  }

  const parties = await resolveMissingCompanyIcosInParties(payload.relations
    .map(mapOwnershipRelationToParty)
    .filter(isDisplayableOwnershipParty)
    .map(maskNonCompanyParty));

  return {
    kind: "vehicleHistory",
    vehicle: payload.summary || { pcv, vin: vin || null },
    summary: {
      relationshipCount: parties.length,
      currentCount: parties.filter((party) => party.current).length,
      legalEntityCount: parties.filter(isLegalEntityParty).length,
      anonymizedCount: parties.filter((party) => !isLegalEntityParty(party)).length,
      truncated: Boolean(payload.truncated),
      sourceUpdatedAt: payload.sourceUpdatedAt || null
    },
    parties
	  };
}

function scheduleFleetPlateBackfill(ico, records) {
  const normalizedIco = sanitizeIco(ico);
  if (!normalizedIco || ICO_FLEET_PLATE_BACKFILL_LIMIT <= 0 || !canAttemptPlateResolution()) {
    return;
  }

  const candidates = [];
  const seen = new Set();
  (Array.isArray(records) ? records : []).forEach((record) => {
    if (!record || record.plate) {
      return;
    }

    const vin = normalizeWhitespace(record.vin).toUpperCase();
    const pcv = normalizeWhitespace(record.pcv);
    if (!vin && !pcv) {
      return;
    }

    const key = vin || pcv;
    if (seen.has(key) || PLATE_BACKFILL_IN_FLIGHT.has(key)) {
      return;
    }

    seen.add(key);
    candidates.push({ vin, pcv, key });
  });

  if (candidates.length === 0) {
    return;
  }

  Promise.resolve().then(async () => {
    let resolvedAny = false;
    for (const candidate of candidates.slice(0, ICO_FLEET_PLATE_BACKFILL_LIMIT)) {
      PLATE_BACKFILL_IN_FLIGHT.add(candidate.key);
      try {
        const result = await resolveVehiclePlate(candidate);
        resolvedAny = resolvedAny || Boolean(result?.plate);
      } catch (error) {
      } finally {
        PLATE_BACKFILL_IN_FLIGHT.delete(candidate.key);
      }
    }

    if (resolvedAny) {
      invalidateIcoFleetCache(normalizedIco);
    }
  }).catch(() => {});
}

function canAttemptPlateResolution() {
  return Boolean(
    process.env.TRANSPORT_CUBE_LOOKUP_URL ||
    process.env.DATAOVOZIDLECH_API_KEY ||
    process.env.RSV_PUBLIC_API_KEY ||
    (
      PLATE_RESOLUTION_BROWSER_FALLBACK_ENABLED &&
      (
        (UNIQA_LOOKUP_ENABLED && UNIQA_PHONE && (UNIQA_BROWSER_PATH || BROWSERLESS_ENABLED)) ||
        (PVZP_LOOKUP_ENABLED && PVZP_BROWSER_PATH)
      )
    )
  );
}

async function resolveVehiclePlate(params = {}) {
  let vin = normalizeWhitespace(params.vin).toUpperCase() || null;
  let pcv = normalizeWhitespace(params.pcv) || null;
  const cached = await openDataDb.getCachedPlateResolutionByVehicle({ vin, pcv }).catch(() => null);
  if (cached?.plate) {
    return {
      status: "ready",
      plate: cached.plate,
      vin: cached.vin || vin,
      pcv: cached.pcv || pcv,
      source: cached.source || "cache"
    };
  }

  if (!vin && pcv) {
    const payload = await queryOpenDataVehicleByIdentifiers({ pcv }).catch(() => null);
    vin = normalizeWhitespace(payload?.summary?.vin).toUpperCase() || null;
  }

  if (vin && !pcv) {
    const payload = await queryOpenDataVehicleByIdentifiers({ vin }).catch(() => null);
    pcv = normalizeWhitespace(payload?.summary?.pcv) || null;
  }

  const linkedCached = await openDataDb.getCachedPlateResolutionByVehicle({ vin, pcv }).catch(() => null);
  if (linkedCached?.plate) {
    return {
      status: "ready",
      plate: linkedCached.plate,
      vin: linkedCached.vin || vin,
      pcv: linkedCached.pcv || pcv,
      source: linkedCached.source || "cache"
    };
  }

  if (!vin || parseLookupQuery(vin, "vin").type !== "vin") {
    return {
      status: "unavailable",
      plate: null,
      vin,
      pcv,
      message: "SPZ není v lokální cache a bez VIN ji nelze bezpečně dohledat."
    };
  }

  const lookup = parseLookupQuery(vin, "vin");
  const diagnostics = createLookupDiagnostics(lookup);
  const allowBrowserFallback = Boolean(params.allowBrowserFallback || PLATE_RESOLUTION_BROWSER_FALLBACK_ENABLED);
  const allowPvzpFallback = Boolean(params.allowPvzpFallback || allowBrowserFallback);
  const allowUniqaFallback = Boolean(params.allowUniqaFallback || allowBrowserFallback);
  const lookupAttempts = [
    () => lookupFromConfiguredProvider(lookup, diagnostics),
    () => lookupFromOfficialVinApiWithBudget(lookup, diagnostics, true),
    ...(allowPvzpFallback ? [() => lookupFromPvzpBrowser(lookup, diagnostics)] : []),
    ...(allowUniqaFallback ? [() => lookupFromUniqaBrowser(lookup, diagnostics)] : [])
  ];
  let providerRecord = null;
  let supplementalRecord = null;
  let plate = null;

  for (const attemptLookup of lookupAttempts) {
    const record = await attemptLookup();
    if (!record) {
      continue;
    }

    supplementalRecord = supplementalRecord || record;
    const resolvedPlate = normalizeWhitespace(extractIdentifier(record, "SPZ")).toUpperCase() || null;
    if (resolvedPlate) {
      providerRecord = record;
      plate = resolvedPlate;
      break;
    }
  }

  const sourceRecord = providerRecord || supplementalRecord;
  const resolvedVin = normalizeWhitespace(extractIdentifier(sourceRecord, "VIN")).toUpperCase() || vin;
  const resolvedPcv = normalizeWhitespace(extractIdentifier(sourceRecord, "PČV") || extractIdentifier(sourceRecord, "PCV")) || pcv;

  if (!plate) {
    return {
      status: "unavailable",
      plate: null,
      vin,
      pcv,
      message: "SPZ se nepodařilo dohledat z dostupných zdrojů.",
      diagnostics
    };
  }

  const storedPlate = await openDataDb.storePlateResolution({
    plate,
    vin: resolvedVin,
    pcv: resolvedPcv,
    source: providerRecord?.source?.label || "VIN SPZ resolver",
    confidence: resolvedVin && resolvedPcv ? 0.9 : 0.75,
    ttlMs: PLATE_RESOLUTION_TTL_MS
  }).catch(() => null);
  await invalidateIcoFleetCacheForVehicle(storedPlate?.pcv || resolvedPcv).catch(() => null);

  return {
    status: "ready",
    plate,
    vin: resolvedVin,
    pcv: resolvedPcv,
    source: providerRecord?.source?.label || "resolver"
  };
}

async function lookupVehiclesByIcoFromFleetDb(ico, normalizedQuery) {
  const meta = await readJsonFile(FLEET_DB_META_FILE);
  if (!meta?.ready) {
    return null;
  }

  const company = await resolveCompanyDetailsForIco(ico, []);
  let companyRelations = await readFleetDbOwnerRelations(ico);
  if (companyRelations.length === 0) {
    const [nameRelations, persistentNameRelations, persistentIcoRelations] = await Promise.all([
      company.name ? readFleetDbOwnerRelationsByCompanyName(company.name, ico).catch(() => []) : [],
      company.name ? readPersistentOwnershipRelationsByCompanyName(company.name, ico).catch(() => []) : [],
      readPersistentOwnershipRelationsByIco(ico).catch(() => [])
    ]);
    companyRelations = [...nameRelations, ...persistentNameRelations, ...persistentIcoRelations];
  }

  const candidatePcvs = getActiveCompanyFleetPcvs(companyRelations, ico);
  const pcvRelations = candidatePcvs.length > 0
    ? await readFleetDbOwnershipRelationsForPcvs(candidatePcvs)
    : [];
  const relations = [...companyRelations, ...pcvRelations];
  const summaryMap = await readFleetDbVehicleSummaries(relations);
	  const records = buildCompanyFleetRecords(relations, summaryMap, ico);
	  const relationshipCount = countActiveFleetRelationshipsForIco(records, ico);
	  const limited = limitCompanyFleetRecords(records);
	  const coverage = calculateFleetRecordCoverage(limited.records);
	  const resolvedCompany = company.name || company.address ? company : await resolveCompanyDetailsForIco(ico, companyRelations);

  return {
    kind: "fleet",
    query: {
      raw: normalizedQuery,
      normalized: ico,
      type: "ico",
      resolvedAt: new Date().toISOString()
    },
    company: {
      ico,
	      name: resolvedCompany.name,
	      address: resolvedCompany.address
    },
    summary: {
      vehicleCount: limited.totalCount,
      displayedCount: limited.records.length,
	      activeVehicleCount: limited.totalCount,
	      historicalVehicleCount: 0,
	      companyHistoryVehicleCount: 0,
	      currentVehicleCount: limited.totalCount,
	      relationshipCount,
	      plateCount: coverage.plateCount,
	      missingPlateCount: coverage.missingPlateCount,
	      inspectionCount: coverage.inspectionCount,
	      missingInspectionCount: coverage.missingInspectionCount,
	      truncated: limited.truncated,
	      sourceUpdatedAt: meta.ownerDatasetDate || meta.vehicleDatasetDate || null
	    },
	    records: limited.records,
	    historyRecords: [],
	    companyHistoryRecords: []
	  };
}

async function lookupOwnershipFromFleetDbByPcv(pcv, record, vin) {
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!normalizedPcv) {
    return null;
  }

  const meta = await readJsonFile(FLEET_DB_META_FILE);
  if (!meta?.ready) {
    return null;
  }

  const relations = await readFleetDbOwnershipRelations(normalizedPcv);
  if (relations.length === 0) {
    return null;
  }

  return await buildOwnershipRecordFromRelations(relations, record, vin, normalizedPcv);
}

async function lookupOwnershipFromDatabaseByPcv(pcv, record, vin) {
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!normalizedPcv) {
    return null;
  }

  const payload = await openDataDb.queryOwnershipByPcv(normalizedPcv).catch(() => null);
  if (!payload || payload.relations.length === 0) {
    return null;
  }

  return await buildOwnershipRecordFromRelations(payload.relations, record, vin, normalizedPcv);
}

async function lookupInspectionsFromDatabaseByPcv(pcv) {
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!normalizedPcv) {
    return null;
  }

  const payload = await openDataDb.queryInspectionsByPcv(normalizedPcv).catch(() => null);
  if (!payload || payload.records.length === 0) {
    return null;
  }

  return buildDatabaseInspectionPayload(payload, normalizedPcv);
}

async function lookupInspectionsFromDatabaseByVin(vin) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase();
  if (!normalizedVin) {
    return null;
  }

  const payload = await openDataDb.queryInspectionsByVin(normalizedVin).catch(() => null);
  if (!payload || payload.records.length === 0) {
    return null;
  }

  return buildDatabaseInspectionPayload(payload, payload.pcv || null);
}

function buildDatabaseInspectionPayload(payload, pcv = null) {
  const records = payload.records.sort((left, right) => compareDatesDesc(left.validFrom, right.validFrom));
  return {
    pcv: normalizeWhitespace(pcv) || null,
    sourceFile: payload.sourceFile || null,
    sourceUpdatedAt: payload.sourceUpdatedAt || null,
    fetchedAt: new Date().toISOString(),
    summary: buildInspectionSummary(records),
    records
  };
}

async function readFleetDbOwnerRelations(ico) {
  const shard = getFleetDbShardKey(ico);
  const rows = await readFleetDbShardMatches(
    FLEET_DB_OWNER_DIR,
    shard,
    (row) => row.ico === ico
  );
  return rows.map(normalizeFleetDbRelationRow);
}

async function readFleetDbOwnerRelationsByCompanyName(companyName, ico) {
  const nameKey = normalizeCompanyNameForMatch(companyName);
  if (!nameKey) {
    return [];
  }

  const shard = getFleetDbShardKey(nameKey);
  const rows = await readFleetDbShardMatches(
    FLEET_DB_OWNER_NAME_DIR,
    shard,
    (row) => row.nameKey === nameKey
  );
  return rows
    .filter((row) => {
      const rowIco = sanitizeIco(row.ico);
      return !rowIco || rowIco === ico;
    })
    .map((row) => normalizeFleetDbRelationRow({
      ...row,
      ico: sanitizeIco(row.ico) || ico
    }));
}

async function readPersistentOwnershipRelationsByCompanyName(companyName, ico) {
  const nameKey = normalizeCompanyNameForMatch(companyName);
  if (!nameKey) {
    return [];
  }

  await ensureOpenDataPersistentCachesLoaded();
  const relations = [];
  OPEN_DATA_PERSISTENT_OWNER_INDEX.forEach((payload, pcv) => {
    const parties = Array.isArray(payload?.ownership?.parties) ? payload.ownership.parties : [];
    parties.forEach((party) => {
      const partyIco = sanitizeIco(party?.ico);
      const partyNameKey = normalizeCompanyNameForMatch(party?.name);
      if (partyIco !== ico && partyNameKey !== nameKey) {
        return;
      }
      if (partyIco && partyIco !== ico && partyNameKey === nameKey) {
        return;
      }

      relations.push(normalizeFleetDbRelationRow({
        pcv: normalizeWhitespace(pcv),
        ico: partyIco || ico,
        relation: normalizeVehicleRelation(party?.role) || "Subjekt",
        subjectType: party?.type || party?.subjectType || "company",
        current: party?.current,
        name: normalizeWhitespace(party?.name),
        address: normalizeWhitespace(party?.address),
        dateFrom: normalizeWhitespace(party?.dateFrom || party?.since || extractPeriodStart(party?.period)),
        dateTo: normalizeWhitespace(party?.dateTo || extractPeriodEnd(party?.period)),
        source: payload?.source?.label || "ownership-cache"
      }));
    });
  });

  return relations;
}

async function readPersistentOwnershipRelationsByIco(ico) {
  const normalizedIco = sanitizeIco(ico);
  if (!normalizedIco) {
    return [];
  }

  await ensureOpenDataPersistentCachesLoaded();
  const relations = [];
  OPEN_DATA_PERSISTENT_OWNER_INDEX.forEach((payload, pcv) => {
    const parties = Array.isArray(payload?.ownership?.parties) ? payload.ownership.parties : [];
    parties.forEach((party) => {
      const partyIco = sanitizeIco(party?.ico);
      if (partyIco !== normalizedIco) {
        return;
      }

      relations.push(normalizeFleetDbRelationRow({
        pcv: normalizeWhitespace(pcv),
        ico: partyIco,
        relation: normalizeVehicleRelation(party?.role) || "Subjekt",
        subjectType: party?.type || party?.subjectType || "company",
        current: party?.current,
        name: normalizeWhitespace(party?.name),
        address: normalizeWhitespace(party?.address),
        dateFrom: normalizeWhitespace(party?.dateFrom || party?.since || extractPeriodStart(party?.period)),
        dateTo: normalizeWhitespace(party?.dateTo || extractPeriodEnd(party?.period)),
        source: payload?.source?.label || "ownership-cache"
      }));
    });
  });

  return relations;
}

async function readFleetDbOwnershipRelations(pcv) {
  const normalizedPcv = normalizeWhitespace(pcv);
  const shard = getFleetDbShardKey(normalizedPcv);
  const rows = await readFleetDbShardMatches(
    FLEET_DB_OWNERSHIP_PCV_DIR,
    shard,
    (row) => normalizeWhitespace(row.pcv) === normalizedPcv
  );
  return rows.filter((row) => normalizeWhitespace(row.pcv) === normalizedPcv).map(normalizeFleetDbRelationRow);
}

async function readFleetDbOwnershipRelationsForPcvs(pcvs) {
  const normalizedPcvs = Array.from(new Set(pcvs.map((pcv) => normalizeWhitespace(pcv)).filter(Boolean)));
  const groupedByShard = new Map();

  normalizedPcvs.forEach((pcv) => {
    const shard = getFleetDbShardKey(pcv);
    if (!groupedByShard.has(shard)) {
      groupedByShard.set(shard, new Set());
    }
    groupedByShard.get(shard).add(pcv);
  });

  const relations = [];
  for (const [shard, wantedPcvs] of groupedByShard.entries()) {
    const rows = await readFleetDbShardMatches(
      FLEET_DB_OWNERSHIP_PCV_DIR,
      shard,
      (row) => wantedPcvs.has(normalizeWhitespace(row.pcv))
    );
    rows.forEach((row) => {
      if (wantedPcvs.has(normalizeWhitespace(row.pcv))) {
        relations.push(normalizeFleetDbRelationRow(row));
      }
    });
  }

  return relations;
}

function normalizeFleetDbRelationRow(row) {
  return {
    ...row,
    relation: normalizeVehicleRelation(row.relation),
    current: isActiveRelation(row)
  };
}

async function readFleetDbPcvByVin(vin) {
  const normalizedVin = normalizeWhitespace(vin).toUpperCase();
  if (!normalizedVin) {
    return null;
  }

  const meta = await readJsonFile(FLEET_DB_META_FILE);
  if (!meta?.ready) {
    return null;
  }

  const shard = getFleetDbShardKey(normalizedVin);
  const rows = await readFleetDbShardMatches(
    FLEET_DB_VIN_PCV_DIR,
    shard,
    (row) => normalizeWhitespace(row.vin).toUpperCase() === normalizedVin
  );
  const match = rows.find((row) => normalizeWhitespace(row.vin).toUpperCase() === normalizedVin);
  return match ? normalizeWhitespace(match.pcv) || null : null;
}

async function readFleetDbVehicleSummaryByPcv(pcv) {
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!normalizedPcv) {
    return null;
  }

  const meta = await readJsonFile(FLEET_DB_META_FILE);
  if (!meta?.ready) {
    return null;
  }

  const shard = getFleetDbShardKey(normalizedPcv);
  const rows = await readFleetDbShardMatches(
    FLEET_DB_VEHICLE_DIR,
    shard,
    (row) => normalizeWhitespace(row.pcv) === normalizedPcv
  );
  return rows.find((row) => normalizeWhitespace(row.pcv) === normalizedPcv) || null;
}

async function readFleetDbShardMatches(directory, shardKey, predicate) {
  const filePath = path.join(directory, `${shardKey}.jsonl`);
  const matches = [];

  try {
    const stream = fs.createReadStream(filePath, { encoding: "utf8" });
    const reader = readline.createInterface({
      input: stream,
      crlfDelay: Infinity
    });

    for await (const line of reader) {
      const trimmed = line.trim();
      if (!trimmed) {
        continue;
      }

      const row = JSON.parse(trimmed);
      if (predicate(row)) {
        matches.push(row);
      }
    }
  } catch (error) {
    if (error.code === "ENOENT") {
      return [];
    }

    throw error;
  }

  return matches;
}

async function readFleetDbVehicleSummaries(relations) {
  const pcvs = Array.from(new Set(relations.map((relation) => normalizeWhitespace(relation.pcv)).filter(Boolean)));
  const groupedByShard = new Map();

  pcvs.forEach((pcv) => {
    const shard = getFleetDbShardKey(pcv);
    if (!groupedByShard.has(shard)) {
      groupedByShard.set(shard, []);
    }
    groupedByShard.get(shard).push(pcv);
  });

  const summaryMap = new Map();
  for (const [shard, shardPcvs] of groupedByShard.entries()) {
    const wanted = new Set(shardPcvs);
    const rows = await readFleetDbShardMatches(
      FLEET_DB_VEHICLE_DIR,
      shard,
      (row) => wanted.has(normalizeWhitespace(row.pcv))
    );
    rows.forEach((row) => {
      if (wanted.has(normalizeWhitespace(row.pcv))) {
        const normalizedPcv = normalizeWhitespace(row.pcv);
        summaryMap.set(normalizedPcv, row);
        setTimedCacheValue(OPEN_DATA_VEHICLE_SUMMARY_CACHE, normalizedPcv, row);
      }
    });
  }

  const missingPcvs = [];
  pcvs.forEach((pcv) => {
    if (summaryMap.has(pcv)) {
      return;
    }

    const cached = getTimedCacheValue(OPEN_DATA_VEHICLE_SUMMARY_CACHE, pcv);
    if (cached !== undefined) {
      if (cached) {
        summaryMap.set(pcv, cached);
      }
      return;
    }

    missingPcvs.push(pcv);
  });

  if (missingPcvs.length > 0) {
    const csvSummaries = await readLocalVehicleSummariesByPcvs(missingPcvs).catch(() => []);
    const foundPcvs = new Set();

    csvSummaries.forEach((summary) => {
      const pcv = normalizeWhitespace(summary?.pcv);
      if (!pcv) {
        return;
      }

      foundPcvs.add(pcv);
      summaryMap.set(pcv, summary);
      setTimedCacheValue(OPEN_DATA_VEHICLE_SUMMARY_CACHE, pcv, summary);
    });

    if (csvSummaries.length > 0) {
      await persistFleetDbVehicleSummaries(csvSummaries).catch(() => null);
    }

    missingPcvs.forEach((pcv) => {
      if (!foundPcvs.has(pcv)) {
        setTimedCacheValue(OPEN_DATA_VEHICLE_SUMMARY_CACHE, pcv, null);
      }
    });
  }

  return summaryMap;
}

async function persistFleetDbVehicleSummaries(summaries) {
  const rows = (Array.isArray(summaries) ? summaries : [])
    .map((summary) => ({
      ...summary,
      pcv: normalizeWhitespace(summary?.pcv)
    }))
    .filter((summary) => summary.pcv);

  if (rows.length === 0) {
    return;
  }

  await fs.promises.mkdir(FLEET_DB_VEHICLE_DIR, { recursive: true });
  const groupedByShard = new Map();

  rows.forEach((summary) => {
    const shard = getFleetDbShardKey(summary.pcv);
    if (!groupedByShard.has(shard)) {
      groupedByShard.set(shard, []);
    }
    groupedByShard.get(shard).push(summary);
  });

  for (const [shard, shardRows] of groupedByShard.entries()) {
    const filePath = path.join(FLEET_DB_VEHICLE_DIR, `${shard}.jsonl`);
    const content = `${shardRows.map((row) => JSON.stringify(row)).join("\n")}\n`;
    await fs.promises.appendFile(filePath, content, "utf8");
    FLEET_DB_VEHICLE_SHARD_CACHE.delete(shard);
  }
}

async function readLocalVehicleSummariesByPcvs(pcvs) {
  const normalizedPcvs = Array.from(new Set((Array.isArray(pcvs) ? pcvs : []).map((pcv) => normalizeWhitespace(pcv)).filter(Boolean)));
  if (normalizedPcvs.length === 0) {
    return [];
  }

  const datasetPath = await resolveLocalVehicleDatasetPath();
  if (!datasetPath) {
    return [];
  }

  return await findVehicleSummariesByPcvsInDataset(datasetPath, normalizedPcvs);
}

async function resolveLocalVehicleDatasetPath() {
  await ensureOpenDataPersistentCachesLoaded();

  const candidates = [];
  const cached = OPEN_DATA_DATASET_CACHE.vehicles;
  if (cached?.localPath) {
    candidates.push(cached.localPath);
  }
  if (cached?.filename) {
    candidates.push(path.join(OPEN_DATA_PERSIST_DIR, cached.filename));
  }

  const fleetMeta = await readJsonFile(FLEET_DB_META_FILE).catch(() => null);
  if (fleetMeta?.vehicleFilename) {
    candidates.push(path.join(OPEN_DATA_PERSIST_DIR, fleetMeta.vehicleFilename));
  }

  const newestLocal = await findNewestLocalVehicleDatasetPath().catch(() => null);
  if (newestLocal) {
    candidates.push(newestLocal);
  }

  for (const candidate of candidates) {
    if (candidate && fs.existsSync(candidate)) {
      return candidate;
    }
  }

  return null;
}

async function findNewestLocalVehicleDatasetPath() {
  const entries = await fs.promises.readdir(OPEN_DATA_PERSIST_DIR, { withFileTypes: true });
  const vehicleFiles = entries
    .filter((entry) => entry.isFile() && /^RSV_vypis_vozidel_\d{8}\.csv$/i.test(entry.name))
    .map((entry) => ({
      name: entry.name,
      filePath: path.join(OPEN_DATA_PERSIST_DIR, entry.name)
    }))
    .sort((left, right) => right.name.localeCompare(left.name));

  return vehicleFiles[0]?.filePath || null;
}

async function readFleetDbShard(directory, shardKey, cache) {
  if (!FLEET_DB_ALLOW_WHOLE_SHARD_CACHE) {
    return await readFleetDbShardMatches(directory, shardKey, () => true);
  }

  const cached = cache.get(shardKey);
  if (cached) {
    return cached;
  }

  const filePath = path.join(directory, `${shardKey}.jsonl`);
  try {
    const content = await fs.promises.readFile(filePath, "utf8");
    const rows = content
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => JSON.parse(line));
    cache.set(shardKey, rows);
    return rows;
  } catch (error) {
    if (error.code === "ENOENT") {
      cache.set(shardKey, []);
      return [];
    }

    throw error;
  }
}

function mergeFleetDbRecords(relations, summaryMap) {
  const groupedRecords = Array.from(
    relations.reduce((map, relation) => {
      const key = relation.pcv || `${relation.relation}-${relation.dateFrom}-${relation.dateTo}`;
      const summary = relation.pcv ? summaryMap.get(normalizeWhitespace(relation.pcv)) : null;

      if (!map.has(key)) {
        const title = summary
          ? [summary.make, summary.model, summary.type].filter(Boolean).join(" ").trim() || summary.plate || summary.vin || summary.pcv || `Vozidlo ${key}`
          : relation.pcv
            ? `Vozidlo ${relation.pcv}`
            : "Vozidlo bez PČV";
        map.set(key, {
          id: key,
          pcv: relation.pcv || summary?.pcv || null,
          plate: summary?.plate || null,
          vin: summary?.vin || null,
          make: summary?.make || null,
          model: summary?.model || null,
          type: summary?.type || null,
	          category: summary?.category || null,
	          fuel: summary?.fuel || null,
	          firstRegistration: summary?.firstRegistration || null,
            inspection: summary?.inspection || null,
	          status: summary?.status || null,
          title,
          current: Boolean(relation.current),
          firstSeen: relation.dateFrom || null,
          lastSeen: relation.dateTo || null,
          historyRelationCount: Number(relation.historyRelationCount || 0),
          relations: []
        });
      }

      const current = map.get(key);
      current.historyRelationCount = Math.max(
        Number(current.historyRelationCount || 0),
        Number(relation.historyRelationCount || 0)
      );
      current.current = current.current || Boolean(relation.current);
      current.firstSeen = current.firstSeen
        ? compareDatesDesc(current.firstSeen, relation.dateFrom) > 0
          ? current.firstSeen
          : relation.dateFrom || current.firstSeen
        : relation.dateFrom || null;
      current.lastSeen = current.lastSeen
        ? compareDatesDesc(current.lastSeen, relation.dateTo) < 0
          ? current.lastSeen
          : relation.dateTo || current.lastSeen
        : relation.dateTo || null;
      current.relations.push(relation);
      return map;
    }, new Map()).values()
  );

  groupedRecords.forEach((record) => {
    record.relations = sortFleetRelations(dedupeFleetRelations(record.relations.filter(isDisplayableFleetRelation)));
    record.current = record.relations.some(isActiveFleetOwnershipRelation);
    record.activeRelations = record.relations.filter(isActiveFleetOwnershipRelation);
    record.historyRelations = record.relations.filter((relation) => !isActiveFleetOwnershipRelation(relation));
    record.historyAvailable = Number(record.historyRelationCount || 0) > record.activeRelations.length;
  });

  return groupedRecords.sort(compareFleetRecords);
}

function dedupeFleetRelations(relations) {
  const seen = new Set();
  return relations.filter((relation) => {
    const isActive = isActiveFleetOwnershipRelation(relation);
    const key = [
      normalizeWhitespace(relation?.relation),
      sanitizeIco(relation?.ico),
      normalizeWhitespace(relation?.name).toLowerCase(),
      normalizeWhitespace(relation?.address).toLowerCase(),
      relation?.current === null || relation?.current === undefined ? "" : String(Boolean(relation.current)),
      isActive ? "" : normalizeWhitespace(relation?.dateFrom),
      isActive ? "" : normalizeWhitespace(relation?.dateTo)
    ].join("|");
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function buildCompanyFleetRecords(relations, summaryMap, ico) {
  return mergeFleetDbRecords(relations, summaryMap).filter((record) =>
    record.relations.some((relation) => isActiveFleetRelationForIco(relation, ico))
  );
}

function buildHistoricalCompanyFleetRecords(relations, summaryMap, ico) {
  return mergeFleetDbRecords(relations, summaryMap).filter((record) =>
    record.relations.some((relation) =>
      relationBelongsToIco(relation, ico) &&
        isOwnershipVehicleRelation(relation?.relation) &&
        !isActiveFleetOwnershipRelation(relation)
    )
  );
}

function limitCompanyFleetRecords(records, totalCount = null) {
  const allRecords = Array.isArray(records) ? records : [];
  const parsedTotal = totalCount === null || totalCount === undefined || totalCount === "" ? NaN : Number(totalCount);
  const resolvedTotal = Number.isFinite(parsedTotal) ? parsedTotal : allRecords.length;
  const displayedRecords = allRecords.slice(0, ICO_FLEET_MAX_RECORDS);

  return {
    records: displayedRecords,
    totalCount: resolvedTotal,
    truncated: resolvedTotal > displayedRecords.length || allRecords.length > displayedRecords.length
  };
}

function calculateFleetRecordCoverage(records) {
  const list = Array.isArray(records) ? records : [];
  const plateCount = list.filter((record) => Boolean(normalizeWhitespace(record?.plate))).length;
  const inspectionCount = list.filter((record) =>
    Boolean(record?.inspection?.performedOn && record?.inspection?.validUntil)
  ).length;

  return {
    totalCount: list.length,
    plateCount,
    missingPlateCount: Math.max(0, list.length - plateCount),
    inspectionCount,
    missingInspectionCount: Math.max(0, list.length - inspectionCount)
  };
}

function getActiveCompanyFleetPcvs(relations, ico) {
  const pcvs = new Set();
  relations.forEach((relation) => {
    if (isActiveFleetRelationForIco(relation, ico) && relation.pcv) {
      pcvs.add(normalizeWhitespace(relation.pcv));
    }
  });
  return Array.from(pcvs);
}

function countFleetRecordRelationships(records) {
  return records.reduce((total, record) => total + (Array.isArray(record.relations) ? record.relations.length : 0), 0);
}

function countActiveFleetRelationshipsForIco(records, ico) {
  return records.reduce(
    (total, record) => total + (Array.isArray(record.relations)
      ? record.relations.filter((relation) => isActiveFleetRelationForIco(relation, ico)).length
      : 0),
    0
  );
}

function isActiveFleetRelationForIco(relation, ico) {
  return relationBelongsToIco(relation, ico) && isActiveFleetOwnershipRelation(relation);
}

function isActiveFleetOwnershipRelation(relation) {
  return isActiveRelation(relation) && isOwnershipVehicleRelation(relation?.relation);
}

function isDisplayableFleetRelation(relation) {
  if (!relation || !isOwnershipVehicleRelation(relation.relation)) {
    return false;
  }

  if (sanitizeIco(relation.ico)) {
    return true;
  }

  const subjectType = relation.subjectType;
  const name = relation.name;
  return (isLegalEntitySubjectType(subjectType) || looksLikeCompanyName(name)) &&
    (hasDisplayableOwnershipText(name) || hasDisplayableOwnershipText(relation.address));
}

function relationBelongsToIco(relation, ico) {
  return sanitizeIco(relation?.ico) === ico;
}

function isOwnershipVehicleRelation(relation) {
  const normalized = normalizeVehicleRelation(relation);
  return normalized === "Vlastnik" || normalized === "Provozovatel";
}

function sortFleetRelations(relations) {
  return [...(Array.isArray(relations) ? relations : [])].sort(compareFleetRelations);
}

function compareFleetRelations(left, right) {
  const leftRole = getFleetRolePriority(left?.relation);
  const rightRole = getFleetRolePriority(right?.relation);
  if (leftRole !== rightRole) {
    return leftRole - rightRole;
  }

  const leftCurrent = isActiveFleetOwnershipRelation(left);
  const rightCurrent = isActiveFleetOwnershipRelation(right);
  if (leftCurrent !== rightCurrent) {
    return leftCurrent ? -1 : 1;
  }

  const dateDifference = normalizeDateScore(right?.dateFrom || right?.dateTo) - normalizeDateScore(left?.dateFrom || left?.dateTo);
  if (dateDifference !== 0) {
    return dateDifference;
  }

  return [
    normalizeWhitespace(left?.pcv).localeCompare(normalizeWhitespace(right?.pcv), "cs"),
    normalizeWhitespace(left?.ico).localeCompare(normalizeWhitespace(right?.ico), "cs"),
    normalizeWhitespace(left?.name).localeCompare(normalizeWhitespace(right?.name), "cs"),
    normalizeWhitespace(left?.address).localeCompare(normalizeWhitespace(right?.address), "cs")
  ].find((value) => value !== 0) || 0;
}

function compareFleetRecords(left, right) {
  if (Boolean(left?.current) !== Boolean(right?.current)) {
    return left?.current ? -1 : 1;
  }

  const leftRole = getFleetRecordRolePriority(left);
  const rightRole = getFleetRecordRolePriority(right);
  if (leftRole !== rightRole) {
    return leftRole - rightRole;
  }

  const dateDifference = getFleetRecordDateScore(right) - getFleetRecordDateScore(left);
  if (dateDifference !== 0) {
    return dateDifference;
  }

  return [
    normalizeWhitespace(left?.title).localeCompare(normalizeWhitespace(right?.title), "cs"),
    normalizeWhitespace(left?.pcv).localeCompare(normalizeWhitespace(right?.pcv), "cs"),
    normalizeWhitespace(left?.vin).localeCompare(normalizeWhitespace(right?.vin), "cs")
  ].find((value) => value !== 0) || 0;
}

function getFleetRecordRolePriority(record) {
  const relations = Array.isArray(record?.activeRelations) && record.activeRelations.length > 0
    ? record.activeRelations
    : Array.isArray(record?.relations) ? record.relations : [];
  return relations.reduce((priority, relation) => Math.min(priority, getFleetRolePriority(relation?.relation)), 99);
}

function getFleetRecordDateScore(record) {
  const relations = Array.isArray(record?.activeRelations) && record.activeRelations.length > 0
    ? record.activeRelations
    : Array.isArray(record?.relations) ? record.relations : [];
  return relations.reduce(
    (score, relation) => Math.max(score, normalizeDateScore(relation?.dateFrom || relation?.dateTo)),
    0
  );
}

function getFleetRolePriority(relation) {
  const normalized = normalizeVehicleRelation(relation);
  if (normalized === "Vlastnik") {
    return 0;
  }
  if (normalized === "Provozovatel") {
    return 1;
  }
  return 2;
}

function getFleetDbShardKey(value) {
  const normalized = String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

  if (!normalized) {
    return "__";
  }

  return normalized.slice(0, 2).padEnd(2, "_");
}

async function lookupVehiclesByIcoLegacy(queryIco) {
  const ico = sanitizeIco(queryIco);
  const normalizedQuery = normalizeWhitespace(queryIco);

  if (!ico) {
    return {
      kind: "fleet",
      query: {
        raw: normalizedQuery,
        normalized: normalizedQuery,
        type: "ico",
        resolvedAt: new Date().toISOString()
      },
      company: null,
      summary: {
        vehicleCount: 0,
        displayedCount: 0,
        currentVehicleCount: 0,
        relationshipCount: 0,
        truncated: false,
        sourceUpdatedAt: null
      },
      records: []
    };
  }

  const cached = getTimedCacheValue(ICO_FLEET_CACHE, ico);
  if (cached !== undefined) {
    return cached;
  }

  await ensureOpenDataPersistentCachesLoaded();

  const [ownerDataset, vehicleDataset] = await Promise.all([
    ensureOpenDataDatasetLocal("owners", OPEN_DATA_OWNER_ROUTE),
    ensureOpenDataDatasetLocal("vehicles", OPEN_DATA_VEHICLE_ROUTE)
  ]);

  const relations = await findCompanyVehicleRelationsByIcoInDataset(ownerDataset.localPath, ico);
  const uniquePcvs = Array.from(new Set(relations.map((relation) => relation.pcv).filter(Boolean)));
  const cappedPcvs = uniquePcvs.slice(0, 200);
  const summaries = cappedPcvs.length > 0 ? await findVehicleSummariesByPcvsInDataset(vehicleDataset.localPath, cappedPcvs) : [];
  const relationMap = new Map();

  relations.forEach((relation) => {
    const key = relation.pcv;
    if (!key) {
      return;
    }

    if (!relationMap.has(key)) {
      relationMap.set(key, []);
    }
    relationMap.get(key).push(relation);
  });

  const records = summaries.map((summary) => ({
    ...summary,
    relations: relationMap.get(summary.pcv) || []
  })).sort((left, right) => {
    const leftName = normalizeWhitespace([left.make, left.model, left.type].filter(Boolean).join(" "));
    const rightName = normalizeWhitespace([right.make, right.model, right.type].filter(Boolean).join(" "));
    return leftName.localeCompare(rightName, "cs");
  });
  const company = await resolveCompanyDetailsForIco(ico, relations);

  const payload = {
    kind: "fleet",
    query: {
      raw: normalizedQuery,
      normalized: ico,
      type: "ico",
      resolvedAt: new Date().toISOString()
    },
    company: {
      ico,
      name: company.name,
      address: company.address
    },
    summary: {
      vehicleCount: uniquePcvs.length,
      displayedCount: records.length,
      currentVehicleCount: new Set(relations.filter((relation) => relation.current).map((relation) => relation.pcv)).size,
      relationshipCount: relations.length,
      truncated: uniquePcvs.length > records.length,
      sourceUpdatedAt: ownerDataset.datasetDate || vehicleDataset.datasetDate || null
    },
    records
  };

  setTimedCacheValue(ICO_FLEET_CACHE, ico, payload);
  return payload;
}

async function findCompanyVehicleRelationsByIcoInDataset(filePath, ico) {
  const normalizedIco = sanitizeIco(ico);
  if (!normalizedIco) {
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

    if (!line || !line.includes(normalizedIco)) {
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

    if (sanitizeIco(firstNonEmpty([canonicalRow.ICO, row["IČO"]])) !== normalizedIco) {
      continue;
    }

    results.push(normalizeCompanyVehicleRelation(row, canonicalRow));
  }

  return results.filter(Boolean);
}

async function findVehicleSummariesByPcvsInDataset(filePath, pcvs) {
  const wanted = new Set((Array.isArray(pcvs) ? pcvs : []).map((pcv) => normalizeWhitespace(pcv)).filter(Boolean));
  if (wanted.size === 0) {
    return [];
  }
  const wantedPattern = new RegExp(Array.from(wanted).map(escapeRegExp).join("|"));

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

    if (!line) {
      continue;
    }

    if (!wantedPattern.test(line)) {
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

    const pcv = normalizeWhitespace(firstNonEmpty([canonicalRow.PCV, row["PČV"]]));
    if (!pcv || !wanted.has(pcv)) {
      continue;
    }

    results.push(normalizeCompanyVehicleSummary(row, canonicalRow));
    wanted.delete(pcv);

    if (wanted.size === 0) {
      break;
    }
  }

  return results.filter(Boolean);
}

async function findSingleOpenDataRowByPcv(filePath, pcv, normalizer) {
  const matches = await findOpenDataRowsByPcv(filePath, pcv, normalizer, 1);
  return matches[0] || null;
}

async function findOpenDataRowsByPcv(filePath, pcv, normalizer, limit) {
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

    results.push(normalizer(row, canonicalRow));
    if (limit && results.length >= limit) {
      break;
    }
  }

  return results.filter(Boolean);
}

function normalizeCompanyVehicleRelation(row, canonicalRow) {
  return {
    pcv: normalizeWhitespace(firstNonEmpty([canonicalRow.PCV, row["PČV"]])),
    relation: normalizeVehicleRelation(firstNonEmpty([canonicalRow.VZTAHKVOZIDLU, row["Vztah k vozidlu"]])),
    subjectType: normalizeWhitespace(firstNonEmpty([canonicalRow.TYPSUBJEKTU, row["Typ subjektu"]])) || null,
    current: normalizeBoolean(firstNonEmpty([canonicalRow.AKTUALNI, row["Aktuální"]])),
    ico: sanitizeIco(firstNonEmpty([canonicalRow.ICO, row["IČO"]])),
    name: normalizeWhitespace(firstNonEmpty([canonicalRow.NAZEV, row["Název"]])),
    address: normalizeWhitespace(firstNonEmpty([canonicalRow.ADRESA, row.Adresa])),
    dateFrom: normalizeOpenDataDate(firstNonEmpty([canonicalRow.DATUMOD, row["Datum od"]])),
    dateTo: normalizeOpenDataDate(firstNonEmpty([canonicalRow.DATUMDO, row["Datum do"]]))
  };
}

function normalizeCompanyVehicleSummary(row, canonicalRow) {
  const dimensions = parseVehicleDimensionTriplet(firstNonEmpty([
    canonicalRow.CELKOVADELKASIRKAVYSKAMM,
    row["Celková délka/šířka/výška [mm]"]
  ]));

  return {
    pcv: normalizeWhitespace(firstNonEmpty([canonicalRow.PCV, row["PČV"]])),
    vin: normalizeWhitespace(firstNonEmpty([canonicalRow.VIN, row.VIN])) || null,
    make: normalizeWhitespace(firstNonEmpty([canonicalRow.TOVARNIZNACKA, row["Tovární značka"]])),
    model: normalizeWhitespace(firstNonEmpty([canonicalRow.OBCHODNIOZNACENI, row["Obchodní označení"]])),
    type: normalizeWhitespace(firstNonEmpty([canonicalRow.TYP, row.Typ])),
    variant: normalizeWhitespace(firstNonEmpty([canonicalRow.VARIANTA, row.Varianta])),
    status: normalizeWhitespace(firstNonEmpty([canonicalRow.STATUS, row.Status])) || null,
    category: normalizeWhitespace(firstNonEmpty([canonicalRow.KATEGORIEVOZIDLA, row["Kategorie vozidla"]])),
    fuel: normalizeWhitespace(firstNonEmpty([canonicalRow.PALIVO, row.Palivo])),
    firstRegistration: normalizeOpenDataDate(firstNonEmpty([canonicalRow.DATUM1REGISTRACE, row["Datum 1. registrace"]])),
    firstRegistrationCz: normalizeOpenDataDate(firstNonEmpty([canonicalRow.DATUM1REGISTRACEVCR, row["Datum 1. registrace v ČR"]])),
	    power: normalizeWhitespace(firstNonEmpty([canonicalRow.MAXVYKONKWMIN, row["Max. výkon [kW] / [min⁻¹]"]])),
    color: normalizeWhitespace(firstNonEmpty([canonicalRow.BARVA, row.Barva])) || null,
    lengthMm: dimensions[0] || null,
    widthMm: dimensions[1] || null,
    heightMm: dimensions[2] || null,
    wheelbaseMm: normalizeVehicleMeasure(firstNonEmpty([canonicalRow.ROZVORMM, row["Rozvor [mm]"]])),
    weightKg: normalizeVehicleMeasure(firstNonEmpty([canonicalRow.PROVOZNIHMOTNOST, row["Provozní hmotnost"]]))
  };
}

function normalizeImportRow(row, canonicalRow) {
  return {
    pcv: normalizeWhitespace(firstNonEmpty([canonicalRow.PCV, row["PČV"]])),
    country: normalizeWhitespace(firstNonEmpty([canonicalRow.STAT, row["Stát"]])) || null,
    importDate: normalizeOpenDataDate(firstNonEmpty([canonicalRow.DATUMDOVOZU, row["Datum dovozu"]]))
  };
}

function normalizeDeregisteredRow(row, canonicalRow) {
  return {
    pcv: normalizeWhitespace(firstNonEmpty([canonicalRow.PCV, row["PČV"]])),
    dateFrom: normalizeOpenDataDate(firstNonEmpty([canonicalRow.DATUMOD, row["Datum od"]])),
    dateTo: normalizeOpenDataDate(firstNonEmpty([canonicalRow.DATUMDO, row["Datum do"]])),
    reason: normalizeWhitespace(firstNonEmpty([canonicalRow.DUVOD, row["Důvod"]])),
    rmCode: normalizeWhitespace(firstNonEmpty([canonicalRow.RMKOD, row["RM kód"]])),
    rmName: normalizeWhitespace(firstNonEmpty([canonicalRow.RMNAZEV, row["RM Název"]]))
  };
}

function isDeregisteredRecordActive(record) {
  if (!record?.dateFrom) {
    return false;
  }

  if (!record.dateTo) {
    return true;
  }

  const parsed = new Date(record.dateTo);
  return !Number.isNaN(parsed.getTime()) && parsed.getTime() >= Date.now();
}

function buildHtmlRequestHeaders(referer) {
  return {
    Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    Pragma: "no-cache",
    Referer: referer,
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
  };
}

function normalizeTaxiResult(value) {
  const normalized = normalizeWhitespace(value).toUpperCase();
  if (normalized === "VALID") {
    return "valid";
  }
  if (normalized === "NOT_VALID") {
    return "not_valid";
  }
  if (normalized === "WRONG_FORMAT") {
    return "wrong_format";
  }
  if (normalized === "SERVICE_ERROR") {
    return "error";
  }
  return normalized ? normalized.toLowerCase() : "unknown";
}

function mapStatusTone(status, tones = {}) {
  const normalized = normalizeWhitespace(status).toLowerCase();
  if (normalized && tones[normalized]) {
    return tones[normalized];
  }
  return tones.defaultTone || "neutral";
}

function formatTaxiBadge(taxi) {
  if (!taxi) {
    return null;
  }

  if (taxi.status === "valid") {
    return "Evidováno";
  }

  if (taxi.status === "not_valid") {
    return "Neevidováno";
  }

  if (taxi.status === "error") {
    return "Neověřeno";
  }

  return "Nezjištěno";
}

function formatTaxiSectionValue(taxi) {
  if (!taxi) {
    return null;
  }

  if (taxi.status === "valid") {
    return `Platná evidence taxi k ${formatDate(taxi.dataValidAsOf)}`;
  }

  if (taxi.status === "not_valid") {
    return `V evidenci taxi není k ${formatDate(taxi.dataValidAsOf)}`;
  }

  if (taxi.status === "error") {
    return taxi.detail || "Taxi evidenci se nepodařilo ověřit.";
  }

  return "Taxi evidenci se nepodařilo jednoznačně vyhodnotit.";
}

function formatPoliceWantedValue(policeWanted) {
  if (!policeWanted) {
    return null;
  }

  if (policeWanted.status === "wanted") {
    return policeWanted.detail || "V policejní evidenci je aktivní pátrání.";
  }

  if (policeWanted.status === "clear") {
    return policeWanted.sourceUpdatedAt
      ? `Bez záznamu v aktualizaci ${policeWanted.sourceUpdatedAt}`
      : "Bez aktivního záznamu.";
  }

  return policeWanted.detail || "Policejní pátrání se nepodařilo ověřit.";
}

function formatImportRecordValue(importRecord) {
  if (!importRecord) {
    return null;
  }

  if (importRecord.status === "error") {
    return importRecord.detail || "Záznam o dovozu se nepodařilo načíst.";
  }

  return [importRecord.country ? `stát ${importRecord.country}` : null, importRecord.importDate ? `datum ${formatDate(importRecord.importDate)}` : null]
    .filter(Boolean)
    .join(" · ") || "Vozidlo je evidováno jako dovezené.";
}

function formatDeregistrationValue(record) {
  if (!record) {
    return null;
  }

  if (record.status === "error") {
    return record.detail || "Záznam o vyřazení se nepodařilo načíst.";
  }

  const parts = [
    record.reason || record.rmName || null,
    record.dateFrom ? `od ${formatDate(record.dateFrom)}` : null,
    record.dateTo ? `do ${formatDate(record.dateTo)}` : record.active ? "stále aktivní" : null
  ].filter(Boolean);

  return parts.join(" · ") || "Vozidlo má záznam o vyřazení z provozu.";
}

function formatInspectionAuditValue(audit) {
  if (!audit) {
    return null;
  }

  if (audit.status === "pending") {
    return "Načítání podkladových dat probíhá.";
  }

  if (audit.status !== "ready") {
    return "Audit STK není k dispozici.";
  }

  return [
    audit.currentStatus || null,
    audit.score !== null ? `score ${audit.score}/100` : null,
    audit.lastKnownDate ? `poslední záznam ${formatDate(audit.lastKnownDate)}` : null
  ]
    .filter(Boolean)
    .join(" · ");
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
    name: name || (ico ? null : "Fyzická osoba"),
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
    return sortTimelineEntries(candidates
      .map((entry) => ({
        date: normalizeTimelineDate(firstNonEmpty([entry.date, entry.datum, entry.createdAt])),
        title: firstNonEmpty([entry.title, entry.nazev, entry.label]) || "Udalost",
        description: firstNonEmpty([entry.description, entry.popis, entry.detail]) || "",
        tone: firstNonEmpty([entry.tone, entry.variant]) || "neutral"
      }))
      .filter((entry) => entry.date || entry.title));
  }

  const synthesized = [];

  if (fallback.firstRegistration) {
    synthesized.push({
      date: normalizeTimelineDate(fallback.firstRegistration),
      title: "První registrace",
      description: "Datum první registrace vozidla.",
      tone: "neutral"
    });
  }

  if (fallback.firstRegistrationCz && fallback.firstRegistrationCz !== fallback.firstRegistration) {
    synthesized.push({
      date: normalizeTimelineDate(fallback.firstRegistrationCz),
      title: "První registrace v ČR",
      description: "Vozidlo bylo evidováno v českém registru.",
      tone: "accent"
    });
  }

  if (fallback.inspectionUntil) {
    synthesized.push({
      date: normalizeTimelineDate(fallback.inspectionUntil),
      title: "Platnost technické kontroly",
      description: "Poslední známý termín platnosti STK.",
      tone: "positive"
    });
  }

  return sortTimelineEntries(synthesized);
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

  const dateOnly = normalized.match(
    /^(\d{4})-(\d{2})-(\d{2})(?:$|T00:00:00(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?$)/
  );
  if (dateOnly) {
    return new Date(Date.UTC(Number(dateOnly[1]), Number(dateOnly[2]) - 1, Number(dateOnly[3]))).toISOString();
  }

  const parsed = new Date(normalized);
  return Number.isNaN(parsed.getTime()) ? normalized : parsed.toISOString();
}

function normalizeInspectionValidityDate(value) {
  const normalized = normalizeWhitespace(value);
  if (!normalized) {
    return null;
  }

  const displayDate = normalized.match(/^(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})$/);
  if (displayDate) {
    return new Date(Date.UTC(
      Number(displayDate[3]),
      Number(displayDate[2]) - 1,
      Number(displayDate[1])
    )).toISOString();
  }

  return normalizeOpenDataDate(normalized);
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

function normalizeOdometer(value) {
  const normalized = normalizeWhitespace(value).replace(/\s+/g, "").replace(",", ".");
  if (!normalized) {
    return null;
  }

  const parsed = Number.parseInt(normalized, 10);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatInspectionOdometer(record) {
  const value = normalizeOdometer(record?.odometer);
  if (value === null) {
    return null;
  }

  const unit = normalizeWhitespace(record?.odometerUnit) || "km";
  return `${value.toLocaleString("cs-CZ")} ${unit}`;
}

function formatInspectionStationLabel(record) {
  const rawCode = normalizeWhitespace(record?.stationCode).replace(/^STK\s+/i, "");
  const code = rawCode || null;
  const rawName = normalizeWhitespace(record?.stationName);
  const genericName = code && (!rawName || normalizeForMatch(rawName) === normalizeForMatch(`STK ${code}`) || rawName === code);
  const mappedName = code ? KNOWN_STK_STATION_NAMES[code] : null;
  const name = genericName ? mappedName : rawName.replace(/^STK\s+/i, "") || mappedName;

  if (code && name) {
    return `STK ${name} (${code})`;
  }

  if (rawName) {
    return rawName;
  }

  return code ? `STK ${code}` : null;
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
    return "Nezjištěno";
  }

  const daysRemaining = diffDaysFromToday(validUntil);
  if (daysRemaining === null) {
    return "Nezjištěno";
  }

  if (daysRemaining < 0) {
    return "Propadla";
  }

  if (daysRemaining <= 30) {
    return "Končí brzy";
  }

  return "Platná";
}

function mapInspectionStatusToTone(status) {
  const normalized = normalizeForMatch(status);
  if (normalized.includes("platna") || normalized.includes("platn")) {
    return "positive";
  }

  if (normalized.includes("konci") || normalized.includes("kon")) {
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
  if (cached === undefined) {
    return undefined;
  }

  if (!cached || typeof cached !== "object" || !Object.prototype.hasOwnProperty.call(cached, "expiresAt")) {
    return clone(cached);
  }

  if (cached.expiresAt <= Date.now()) {
    cache.delete(key);
    return undefined;
  }

  return clone(cached.value);
}

function setTimedCacheValue(cache, key, value) {
  if (cache && typeof cache.set === "function" && Object.prototype.hasOwnProperty.call(cache, "ttlMs")) {
    cache.set(key, value);
    return;
  }

  cache.set(key, {
    value: clone(value),
    expiresAt: Date.now() + OPEN_DATA_CACHE_TTL_MS
  });
}

function invalidateIcoFleetCache(icos) {
  (Array.isArray(icos) ? icos : [icos])
    .map(sanitizeIco)
    .filter(Boolean)
    .forEach((ico) => ICO_FLEET_CACHE.delete(ico));
}

async function invalidateIcoFleetCacheForVehicle(pcv) {
  const normalizedPcv = normalizeWhitespace(pcv);
  if (!normalizedPcv) {
    return;
  }

  const payload = await openDataDb.queryOwnershipByPcv(normalizedPcv).catch(() => null);
  const icos = (payload?.relations || [])
    .map((relation) => relation?.ico)
    .filter(Boolean);
  invalidateIcoFleetCache(icos);
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

function parseLookupQuery(query, requestedType = null) {
  const compact = String(query || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

  const icoPattern = /^\d{8}$/;
  const vinPattern = /^[A-HJ-NPR-Z0-9]{17}$/;
  const platePattern = /^[A-Z0-9]{5,10}$/;
  const typeHint = normalizeLookupTypeHint(requestedType);

  if (typeHint === "ico") {
    return {
      compact,
      type: icoPattern.test(compact) && isValidIco(compact) ? "ico" : "unknown"
    };
  }

  if (typeHint === "vin") {
    return {
      compact,
      type: vinPattern.test(compact) ? "vin" : "unknown"
    };
  }

  if (typeHint === "plate") {
    return {
      compact,
      type: platePattern.test(compact) ? "plate" : "unknown"
    };
  }

  if (icoPattern.test(compact)) {
    return {
      compact,
      type: isValidIco(compact) ? "ico" : "unknown"
    };
  }

  return {
    compact,
    type: vinPattern.test(compact) ? "vin" : platePattern.test(compact) ? "plate" : "unknown"
  };
}

function normalizeLookupTypeHint(value) {
  const normalized = String(value || "").trim().toLowerCase();

  if (normalized === "spz" || normalized === "plate") {
    return "plate";
  }

  if (normalized === "vin") {
    return "vin";
  }

  if (normalized === "ico" || normalized === "ic") {
    return "ico";
  }

  return "";
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

function formatVehicleMeasure(value, unit) {
  const text = normalizeWhitespace(value);
  if (!text) {
    return null;
  }

  return new RegExp(`\\b${unit}\\b`, "i").test(text) ? text : `${text} ${unit}`;
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

function normalizeVehicleRelation(value) {
  const normalized = normalizeWhitespace(value);
  const lower = normalized.toLowerCase();

  if (normalized === "1" || lower.includes("vlast") || lower.includes("majit")) {
    return "Vlastnik";
  }

  if (normalized === "2" || lower.includes("provoz")) {
    return "Provozovatel";
  }

  return normalized || "Subjekt";
}

function isActiveRelation(relation) {
  return Boolean(relation?.current) && !normalizeWhitespace(relation?.dateTo);
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
    return "Bez záznamu";
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

function requestBuffer(targetUrl, options) {
  return requestStructured(targetUrl, options, "buffer");
}

async function requestHtml(targetUrl, options = {}) {
  if (typeof fetch === "function") {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), Math.max(1000, options.timeoutMs || 15000));
    try {
      const response = await fetch(targetUrl, {
        method: options.method || "GET",
        headers: options.headers || {},
        redirect: "follow",
        signal: controller.signal
      });
      const payload = await response.text();
      if (!response.ok) {
        const error = new Error(`Rozhraní vrátilo ${response.status}: ${payload.slice(0, 300) || "bez detailů"}`);
        error.code = "HTTP_ERROR";
        error.statusCode = response.status;
        error.responseSnippet = payload.slice(0, 300) || "bez detailů";
        throw error;
      }
      return payload;
    } finally {
      clearTimeout(timeout);
    }
  }

  return requestText(targetUrl, options);
}

function requestStructured(targetUrl, options, responseType) {
  return new Promise((resolve, reject) => {
    let requestUrl;
    try {
      requestUrl = new URL(targetUrl);
    } catch (error) {
      const invalidUrlError = new Error(`Neplatná cílová URL: ${targetUrl}`);
      invalidUrlError.code = "INVALID_URL";
      reject(invalidUrlError);
      return;
    }

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
        const bodyBuffer = Buffer.concat(chunks);
        const payload = bodyBuffer.toString("utf8");

        if (response.statusCode < 200 || response.statusCode >= 300) {
          const requestError = new Error(
            `Rozhraní vrátilo ${response.statusCode}: ${payload.slice(0, 300) || "bez detailů"}`
          );
          requestError.code = "HTTP_ERROR";
          requestError.statusCode = response.statusCode;
          requestError.responseSnippet = payload.slice(0, 300) || "bez detailů";
          requestError.targetHost = requestUrl.host;
          reject(requestError);
          return;
        }

        if (!bodyBuffer.length) {
          resolve(null);
          return;
        }

        if (responseType === "buffer") {
          resolve(bodyBuffer);
          return;
        }

        if (responseType === "text") {
          resolve(payload);
          return;
        }

        try {
          resolve(JSON.parse(payload));
        } catch (error) {
          const parseError = new Error(
            `Rozhraní nevrátilo validní JSON: ${payload.slice(0, 200) || "bez detailů"}`
          );
          parseError.code = "INVALID_JSON";
          parseError.targetHost = requestUrl.host;
          parseError.responseSnippet = payload.slice(0, 200) || "bez detailů";
          reject(parseError);
        }
      });
    });

    request.setTimeout(options.timeoutMs || 15000, () => {
      const timeoutError = new Error("Vypršel časový limit rozhraní.");
      timeoutError.code = "ETIMEDOUT";
      request.destroy(timeoutError);
    });

    request.on("error", (error) => {
      error.targetHost = error.targetHost || requestUrl.host;
      reject(error);
    });

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

function parseVehicleDimensionTriplet(value) {
  return normalizeWhitespace(value)
    .replace(/[x×]/gi, "/")
    .split("/")
    .map(normalizeVehicleMeasure)
    .filter(Boolean);
}

function normalizeVehicleMeasure(value) {
  const text = normalizeWhitespace(value).replace(/\b(?:mm|kg)\b/gi, "").trim();
  if (!text) {
    return null;
  }

  return text.replace(/\s+/g, " ");
}

function normalizeUniqaPhone(value) {
  return normalizeWhitespace(value).replace(/\s+/g, "");
}

function normalizeForMatch(value) {
  return normalizeWhitespace(value)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
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

function extractPeriodEnd(period) {
  const value = normalizeWhitespace(period);
  if (!value) {
    return null;
  }

  const parts = value.split("-");
  if (parts.length < 2) {
    return null;
  }

  const end = normalizeWhitespace(parts.slice(1).join("-"));
  return end && end !== "-" ? end : null;
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
  const entries = [...primary, ...supplemental].filter((entry) => {
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

  return sortTimelineEntries(entries);
}

function sortTimelineEntries(entries) {
  return (Array.isArray(entries) ? entries : [])
    .map((entry, index) => ({ entry, index }))
    .sort((left, right) => {
      const leftScore = getTimelineDateSortScore(left.entry?.date);
      const rightScore = getTimelineDateSortScore(right.entry?.date);

      if (leftScore !== rightScore) {
        return leftScore - rightScore;
      }

      return left.index - right.index;
    })
    .map(({ entry }) => entry);
}

function getTimelineDateSortScore(value) {
  const parsed = new Date(value);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.getTime();
  }

  const dateKey = normalizeInspectionDateKey(value);
  if (dateKey) {
    const [year, month, day] = dateKey.split("-").map(Number);
    return Date.UTC(year, month - 1, day);
  }

  return Number.MAX_SAFE_INTEGER;
}

function joinUniqueText(values, separator) {
  return Array.from(new Set(values.map((value) => normalizeWhitespace(value)).filter(Boolean))).join(separator);
}

function detectSystemBrowserPath() {
  const candidates = [
    "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
    "C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe",
    "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
  ];

  return candidates.find((candidate) => fs.existsSync(candidate)) || null;
}

function detectPlaywrightBrowserPath() {
  try {
    const { chromium } = require("playwright-core");
    const executablePath = typeof chromium.executablePath === "function" ? chromium.executablePath() : null;
    return executablePath && fs.existsSync(executablePath) ? executablePath : null;
  } catch (error) {
    return null;
  }
}

module.exports = {
  getLookupRuntimeStatus,
  getOpenDataRuntimeStatus,
  lookupVehicle,
	  lookupCompanyVehicleHistory,
	  lookupVehicleHistory,
	  resolveVehiclePlate,
  lookupVehiclesByIco,
  lookupVehicleOwnership,
  lookupVehicleInspections,
  lookupVignette,
  scanPlateImage,
  describeLookupFailure,
  parseLookupQuery
};
