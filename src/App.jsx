import { useEffect, useMemo, useRef, useState } from "react";
import {
  AlertCircle,
  AlertTriangle,
  ArrowUpRight,
  Building2,
  Camera,
  CalendarRange,
  CarFront,
  ChevronDown,
  CheckCircle2,
  Clock3,
  Copy,
  Cookie,
  FileText,
  Fingerprint,
  Gauge,
  LineChart,
  Loader2,
  MapPin,
  Search
} from "lucide-react";
import { Badge } from "./components/ui/badge";
import { Button } from "./components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "./components/ui/card";
import { Input } from "./components/ui/input";
import { Skeleton } from "./components/ui/skeleton";
import { cn } from "./lib/utils";

const FLEET_PAGE_SIZE = 10;
const COOKIE_CONSENT_NAME = "autoinfo_cookie_consent";
const ANALYTICS_SESSION_COOKIE_NAME = "autoinfo_analytics_session";
const COOKIE_MAX_AGE_DAYS = 365;
const KNOWN_STK_STATION_NAMES = {
  "3114": "Bohdalec"
};

function withReadyOwnership(payload) {
  const lookupOwnership = payload?.ownershipLookup?.status === "ready" ? payload.ownershipLookup.ownership : null;
  return lookupOwnership?.parties?.length ? { ...payload, ownership: lookupOwnership } : payload;
}

export default function App() {
  const [query, setQuery] = useState("");
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [hasSearched, setHasSearched] = useState(false);
  const [selectedType, setSelectedType] = useState("plate");
  const [statusText, setStatusText] = useState("");
  const [shareCopied, setShareCopied] = useState(false);
  const [scanLoading, setScanLoading] = useState(false);
  const activeRouteLookupRef = useRef("");
  const plateImageInputRef = useRef(null);

  const detectedType = useMemo(() => detectType(query), [query]);
  const shareLink = useMemo(() => {
    if (!result) {
      return "";
    }

    return buildShareUrl(
      result.query?.type || selectedType || detectedType,
      result.query?.raw || result.query?.normalized || query
    );
  }, [detectedType, query, result, selectedType]);

  useEffect(() => {
    const runLookupFromUrl = () => {
      const sharedLookup = readSharedLookupFromLocation();

      if (!sharedLookup) {
        activeRouteLookupRef.current = "";
        setQuery("");
        setSelectedType("plate");
        setResult(null);
        setError("");
        setHasSearched(false);
        setStatusText("");
        return;
      }

      const lookupKey = getShareLookupKey(sharedLookup.type, sharedLookup.value);
      if (activeRouteLookupRef.current === lookupKey) {
        return;
      }

      activeRouteLookupRef.current = lookupKey;
      setQuery(sharedLookup.value);
      setSelectedType(sharedLookup.type);
      performLookup(sharedLookup.value, sharedLookup.type, { updateUrl: false });
    };

    runLookupFromUrl();
    window.addEventListener("popstate", runLookupFromUrl);

    return () => {
      window.removeEventListener("popstate", runLookupFromUrl);
    };
  }, []);

  useEffect(() => {
    const inspectionLookup = result?.inspectionLookup;

    if (!inspectionLookup || inspectionLookup.status !== "pending") {
      return undefined;
    }

    let cancelled = false;
    let attempts = 0;
    let timeoutId = null;

    const poll = async () => {
      attempts += 1;

      const params = new URLSearchParams();
      if (inspectionLookup.vin) {
        params.set("vin", inspectionLookup.vin);
      }
      if (inspectionLookup.pcv) {
        params.set("pcv", inspectionLookup.pcv);
      }
      if (!inspectionLookup.vin && !inspectionLookup.pcv && result?.query?.raw) {
        params.set("query", result.query.raw);
      }

      try {
        const response = await fetch(`/api/lookup/inspections?${params.toString()}`, {
          headers: { Accept: "application/json" }
        });
        const payload = await response.json();

        if (cancelled) {
          return;
        }

        if (response.ok && payload.status === "ready" && payload.inspections) {
          setResult((current) =>
            current
              ? {
                  ...current,
                  inspections: payload.inspections,
                  inspectionLookup: payload,
                  ownershipLookup: current.ownershipLookup
                    ? {
                        ...current.ownershipLookup,
                        pcv: payload.pcv || current.ownershipLookup.pcv
                      }
                    : current.ownershipLookup
                }
              : current
          );
          setStatusText("Základ je připravený a technické prohlídky byly doplněny.");
          return;
        }

        if (payload.status && payload.status !== "pending") {
          setResult((current) =>
            current
              ? {
                  ...current,
                  inspections: payload.inspections || current.inspections,
                  inspectionLookup: payload
                }
              : current
          );
          setStatusText(formatFrontendText(payload.message || "Detailní technické prohlídky nejsou pro tento dotaz dostupné."));
          return;
        }

        if (payload.status === "pending" && attempts < 30) {
          timeoutId = window.setTimeout(poll, attempts < 8 ? 2000 : 4000);
          return;
        }

        if (payload.status === "pending") {
          setResult((current) =>
            current
              ? {
                  ...current,
                  inspectionLookup: {
                    ...payload,
                    status: "unavailable",
                    message: "Detailní technické prohlídky se nepodařilo dočíst v časovém limitu."
                  }
                }
              : current
          );
        }
      } catch (error) {
        if (!cancelled && attempts < 12) {
          timeoutId = window.setTimeout(poll, 4000);
        }
      }
    };

    poll();

    return () => {
      cancelled = true;
      if (timeoutId) {
        window.clearTimeout(timeoutId);
      }
    };
  }, [result?.inspectionLookup?.status, result?.inspectionLookup?.vin, result?.inspectionLookup?.pcv, result?.query?.raw]);

  useEffect(() => {
    const ownershipLookup = result?.ownershipLookup;

    if (!ownershipLookup || ownershipLookup.status !== "pending") {
      return undefined;
    }

    let cancelled = false;
    let attempts = 0;
    let timeoutId = null;

    const poll = async () => {
      attempts += 1;

      const params = new URLSearchParams();
      if (ownershipLookup.vin) {
        params.set("vin", ownershipLookup.vin);
      }
      if (ownershipLookup.pcv) {
        params.set("pcv", ownershipLookup.pcv);
      }
      const ownershipPlate = getHighlightValue(result?.highlights, "SPZ");
      if (ownershipPlate) {
        params.set("plate", ownershipPlate);
      }
      if (!ownershipLookup.vin && !ownershipLookup.pcv && result?.query?.raw) {
        params.set("query", result.query.raw);
      }

      try {
        const response = await fetch(`/api/lookup/ownership?${params.toString()}`, {
          headers: { Accept: "application/json" }
        });
        const payload = await response.json();

        if (cancelled) {
          return;
        }

        if (response.ok && payload.status === "ready" && payload.ownership) {
          setResult((current) =>
            current
              ? {
                  ...current,
                  ownership: payload.ownership,
                  ownershipLookup: payload
                }
              : current
          );
          return;
        }

        if (payload.status && payload.status !== "pending") {
          setResult((current) =>
            current
              ? {
                  ...current,
                  ownership: payload.ownership || current.ownership,
                  ownershipLookup: payload
                }
              : current
          );
          setStatusText(formatFrontendText(payload.message || "Detailní historie subjektu není pro tento dotaz dostupná."));
          return;
        }

        if (payload.status === "pending" && attempts < 24) {
          timeoutId = window.setTimeout(poll, attempts < 6 ? 1500 : 3000);
          return;
        }

        if (payload.status === "pending") {
          setResult((current) =>
            current
              ? {
                  ...current,
                  ownershipLookup: {
                    ...payload,
                    status: "unavailable",
                    message: "Detailní historii subjektu se nepodařilo dočíst v časovém limitu."
                  }
                }
              : current
          );
        }
      } catch (error) {
        if (!cancelled && attempts < 10) {
          timeoutId = window.setTimeout(poll, 3000);
        }
      }
    };

    poll();

    return () => {
      cancelled = true;
      if (timeoutId) {
        window.clearTimeout(timeoutId);
      }
    };
  }, [result?.ownershipLookup?.status, result?.ownershipLookup?.vin, result?.ownershipLookup?.pcv, result?.query?.raw, result?.highlights]);

  useEffect(() => {
    if (!result || result.kind === "fleet" || result.query?.type !== "vin" || getResultPlate(result)) {
      return undefined;
    }

    const vin = getResultVin(result);
    if (!vin) {
      return undefined;
    }

    let cancelled = false;
    const params = new URLSearchParams({ vin });
    const pcv = getResultPcv(result);
    if (pcv) {
      params.set("pcv", pcv);
    }

    fetch(`/api/resolve-plate?${params.toString()}`, {
      headers: { Accept: "application/json" }
    })
      .then(async (response) => {
        const payload = await response.json().catch(() => null);
        const plate = normalizeSharedLookupValue("plate", payload?.plate);
        if (cancelled || !response.ok || payload?.status !== "ready" || !plate) {
          return;
        }

        setResult((current) => {
          if (!current || current.kind === "fleet" || getResultPlate(current) || getResultVin(current) !== vin) {
            return current;
          }
          return withResolvedPlate(current, plate);
        });
        setStatusText(`SPZ dohledána: ${plate}.`);
      })
      .catch(() => {});

    return () => {
      cancelled = true;
    };
  }, [result?.kind, result?.query?.type, result?.query?.raw, result?.query?.normalized, result?.highlights]);

  useEffect(() => {
    const plate = getResultPlate(result);

    if (!result || result.kind === "fleet" || !plate) {
      return undefined;
    }

    if (result.vignetteLookup?.plate === plate && result.vignetteLookup?.status) {
      return undefined;
    }

    let cancelled = false;
    const params = new URLSearchParams({ plate, country: "CZ" });

    fetch(`/api/lookup/vignette?${params.toString()}`, {
      headers: { Accept: "application/json" }
    })
      .then(async (response) => {
        const payload = await response.json().catch(() => null);
        if (cancelled) {
          return;
        }

        setResult((current) =>
          current && getResultPlate(current) === plate
            ? {
                ...current,
                vignetteLookup: {
                  ...(payload || {}),
                  status: payload?.status || (response.ok ? "ready" : "error"),
                  plate: payload?.plate || plate,
                  country: payload?.country || "CZ",
                  message: payload?.message || (response.ok ? null : "Dalnicni znamku se nepodarilo overit.")
                }
              }
            : current
        );
      })
      .catch((error) => {
        if (cancelled) {
          return;
        }

        setResult((current) =>
          current && getResultPlate(current) === plate
            ? {
                ...current,
                vignetteLookup: {
                  status: "error",
                  configured: true,
                  plate,
                  country: "CZ",
                  valid: null,
                  message: error.message || "Dalnicni znamku se nepodarilo overit."
                }
              }
            : current
        );
      });

    return () => {
      cancelled = true;
    };
  }, [result?.kind, result?.query?.raw, result?.query?.normalized, result?.highlights, result?.vignetteLookup?.plate, result?.vignetteLookup?.status]);

  async function performLookup(nextQuery, requestedType = selectedType, options = {}) {
    const trimmed = nextQuery.trim();

    if (!trimmed) {
      setStatusText("Nejdřív zadej SPZ, VIN nebo odkaz.");
      setError("");
      setResult(null);
      return;
    }

    setLoading(true);
    setHasSearched(true);
    setResult(null);
    setError("");
    setStatusText("Načítám data...");

    try {
      const queryType = resolveLookupType(trimmed, requestedType);
      if (queryType) {
        setSelectedType(queryType);
      }
      const endpoint = buildLookupEndpoint(trimmed, queryType);
      const response = await fetch(endpoint, {
        headers: { Accept: "application/json" }
      });
      const payload = await response.json();

      if (!response.ok) {
        throw new Error(composeErrorMessage(payload));
      }

      const nextPayload = withReadyOwnership(payload);
      setResult(nextPayload);
      const shareType = nextPayload.query?.type || queryType;
      const shareValue = nextPayload.query?.raw || nextPayload.query?.normalized || trimmed;
      const normalizedShareType = normalizeLookupType(shareType);
      if (normalizedShareType) {
        setSelectedType(normalizedShareType);
      }
      const nextShareUrl = buildShareUrl(shareType, shareValue);
      activeRouteLookupRef.current = getShareLookupKey(shareType, shareValue);
      if (options.updateUrl !== false) {
        pushShareUrl(nextShareUrl);
      }
      setStatusText(
        payload.message
          ? payload.message
          : payload.inspectionLookup?.status === "pending"
          ? "Základ je připravený. Technické prohlídky se načítají na pozadí."
          : "Hotovo. Výsledek je připravený."
      );
    } catch (lookupError) {
      setError(lookupError.message || "Vyhledávání selhalo.");
      setStatusText("Bez výsledků pro zadaný dotaz.");
    } finally {
      setLoading(false);
    }
  }

  async function handleCopyShareLink() {
    if (!shareLink) {
      return;
    }

    const copied = await copyTextToClipboard(shareLink);
    setShareCopied(copied);
    setStatusText(copied ? "Odkaz je připravený ke sdílení." : shareLink);

    if (copied) {
      window.setTimeout(() => setShareCopied(false), 2200);
    }
  }

  function handleVehicleVinLookup(vin) {
    const normalizedVin = normalizeSharedLookupValue("vin", vin);
    if (!normalizedVin) {
      return;
    }

    setQuery(normalizedVin);
    setSelectedType("vin");
    performLookup(normalizedVin, "vin");
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function handleSubmit(event) {
    event.preventDefault();
    performLookup(query);
  }

  function handleQueryChange(value) {
    setQuery(value);
    if (isLikelyUrlInput(value)) {
      return;
    }

    setSelectedType(detectType(value));
  }

  async function handlePlateImageChange(event) {
    const file = event.target.files?.[0];
    event.target.value = "";

    if (!file) {
      return;
    }

    if (!file.type.startsWith("image/")) {
      setStatusText("Nahraj fotku SPZ ve formátu obrázku.");
      return;
    }

    setScanLoading(true);
    setStatusText("Čtu SPZ z fotky...");

    try {
      const image = await readFileAsDataUrl(file);
      const response = await fetch("/api/scan-plate", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ image })
      });
      const payload = await response.json();

      if (!response.ok) {
        throw new Error(composeErrorMessage(payload));
      }

      const identifier = payload.identifier || payload.plate;
      const type = normalizeLookupType(payload.type) || detectType(identifier);
      setQuery(identifier);
      setSelectedType(type || "plate");
      setStatusText(`Rozpoznáno ${type === "vin" ? "VIN" : "SPZ"} ${identifier}. Vyhledávám...`);
      await performLookup(identifier, type || "plate");
    } catch (scanError) {
      setStatusText(scanError.message || "SPZ se z fotky nepodařilo přečíst.");
    } finally {
      setScanLoading(false);
    }
  }

  return (
    <div className="relative min-h-screen overflow-x-clip bg-background text-foreground">
      {!hasSearched ? (
        <>
          <div className="pointer-events-none absolute inset-x-0 top-0 h-[560px] bg-[radial-gradient(ellipse_72%_58%_at_50%_0%,rgba(255,255,255,0.13)_0%,rgba(255,255,255,0.075)_30%,rgba(255,255,255,0.028)_62%,transparent_100%)]" />
          <div className="pointer-events-none absolute inset-x-0 top-0 h-[680px] bg-[linear-gradient(180deg,rgba(255,255,255,0.025)_0%,rgba(255,255,255,0.014)_42%,transparent_100%)]" />
          <div className="pointer-events-none absolute inset-0 bg-app-grid bg-[size:72px_72px] opacity-[0.03]" />
        </>
      ) : null}

      {hasSearched ? <StickyHeader loading={loading} result={result} /> : null}

      <main
        className={cn(
          "container relative z-10 flex flex-col transition-all duration-500",
          hasSearched ? "gap-5 py-5" : "min-h-[72vh] justify-center py-4 sm:py-6"
        )}
      >
        <section className={cn("mx-auto w-full scroll-mt-24 transition-all duration-500", hasSearched ? "max-w-4xl" : "max-w-3xl")} id="search">
          {!hasSearched ? (
            <div className="mb-6 text-center">
              <h1 className="mx-auto max-w-4xl text-5xl font-semibold tracking-[-0.05em] text-white sm:text-7xl">
                AutoInfo
              </h1>
            </div>
          ) : null}

          <Card className="border-white/10 bg-card/80">
            <CardContent className={cn(hasSearched ? "p-3 sm:p-4" : "p-4 sm:p-5")}>
              <div className={cn("flex justify-center", hasSearched ? "mb-3" : "mb-5")}>
	                <div aria-label="Typ vyhledávání" className="inline-grid w-full max-w-[240px] grid-cols-2 rounded-full border border-border bg-secondary p-1" role="group">
	                  <button
	                    aria-pressed={selectedType === "plate"}
	                    className={cn(
                      "rounded-full px-4 py-2 text-sm font-semibold transition-all duration-300",
                      selectedType === "plate"
                        ? "bg-primary text-primary-foreground"
                        : "text-muted-foreground"
                    )}
                    onClick={() => setSelectedType("plate")}
                    type="button"
                  >
                    SPZ
                  </button>
	                  <button
	                    aria-pressed={selectedType === "vin"}
	                    className={cn(
                      "rounded-full px-4 py-2 text-sm font-semibold transition-all duration-300",
                      selectedType === "vin"
                        ? "bg-primary text-primary-foreground"
                        : "text-muted-foreground"
                    )}
                    onClick={() => setSelectedType("vin")}
                    type="button"
	                  >
	                    VIN
	                  </button>
                </div>
              </div>

              <form className="space-y-4" onSubmit={handleSubmit}>
                <div className="flex flex-col gap-3 sm:flex-row">
                  <div className="relative flex-1">
                    <Search className="pointer-events-none absolute left-5 top-1/2 size-5 -translate-y-1/2 text-muted-foreground" />
	                    <Input
	                      aria-label="Zadejte SPZ, VIN nebo odkaz na inzerát"
	                      autoComplete="off"
                      className={cn(
                        "border-white/10 bg-secondary pl-12 pr-4 text-base placeholder:text-muted-foreground/80",
                        hasSearched ? "h-12" : "h-14"
                      )}
                      maxLength={400}
                      onChange={(event) => handleQueryChange(event.target.value)}
                      placeholder="SPZ, VIN nebo odkaz na inzerát"
                      value={query}
                    />
                  </div>

                  <input
                    accept="image/*"
                    className="hidden"
                    onChange={handlePlateImageChange}
                    ref={plateImageInputRef}
                    type="file"
                  />

                  <Button
                    aria-label="Nahrát foto SPZ"
                    className={cn("shrink-0 px-0", hasSearched ? "h-12 w-12" : "h-14 w-14")}
                    disabled={scanLoading || loading}
                    onClick={() => plateImageInputRef.current?.click()}
                    type="button"
                    variant="outline"
                  >
                    {scanLoading ? <Loader2 className="size-5 animate-spin" /> : <Camera className="size-5" />}
                  </Button>

                  <Button className={cn("rounded-full px-6 sm:px-7", hasSearched ? "h-12" : "h-14")} disabled={scanLoading} size="lg" type="submit">
                    {loading ? (
                      <>
                        <Loader2 className="mr-2 size-4 animate-spin" />
                        Hledám
                      </>
                    ) : (
                      <>
                        Vyhledat
                        <ArrowUpRight className="ml-2 size-4" />
                      </>
                    )}
                  </Button>
                </div>

                <div className={cn("flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between", hasSearched ? "text-xs" : "")}>
		                <p aria-live="polite" className="text-sm leading-6 text-muted-foreground">{formatFrontendText(statusText)}</p>
                  {shareLink ? (
                    <Button
                      className="h-10 justify-center px-4"
                      onClick={handleCopyShareLink}
                      type="button"
                      variant="ghost"
                    >
                      {shareCopied ? <CheckCircle2 className="mr-2 size-4" /> : <Copy className="mr-2 size-4" />}
                      {shareCopied ? "Zkopírováno" : "Kopírovat odkaz"}
                    </Button>
                  ) : null}
                </div>
	              </form>
            </CardContent>
          </Card>
        </section>

        {hasSearched && (
          <section className="mx-auto w-full max-w-6xl">
          {loading ? <LoadingState /> : null}
          {!loading && error ? <ErrorState message={error} type={detectedType} query={query} /> : null}
          {!loading && !error && result ? (
            result.kind === "fleet" ? <CompanyFleetState onVehicleVinLookup={handleVehicleVinLookup} result={result} /> : <ResultState result={result} />
          ) : null}
        </section>
      )}
      </main>
      <CookieConsentBanner />
    </div>
  );
}

function CookieConsentBanner() {
  const [choice, setChoice] = useState(() => readCookie(COOKIE_CONSENT_NAME));

  if (choice) {
    return null;
  }

  function acceptAnalytics() {
    writeCookie(COOKIE_CONSENT_NAME, "analytics", COOKIE_MAX_AGE_DAYS);
    writeCookie(ANALYTICS_SESSION_COOKIE_NAME, getOrCreateAnalyticsSessionId(), COOKIE_MAX_AGE_DAYS);
    setChoice("analytics");
  }

  function keepNecessaryOnly() {
    writeCookie(COOKIE_CONSENT_NAME, "necessary", COOKIE_MAX_AGE_DAYS);
    deleteCookie(ANALYTICS_SESSION_COOKIE_NAME);
    setChoice("necessary");
  }

  return (
    <div className="fixed inset-x-0 bottom-0 z-50 px-3 pb-3 sm:px-6 sm:pb-6">
      <div className="mx-auto flex max-w-3xl flex-col gap-3 rounded-lg border border-white/10 bg-black/95 p-4 sm:flex-row sm:items-center sm:justify-between">
        <div className="flex min-w-0 gap-3">
          <span className="mt-0.5 inline-flex size-9 shrink-0 items-center justify-center rounded-full border border-white/10 bg-white/[0.06] text-white">
            <Cookie className="size-4" />
          </span>
          <p className="text-sm leading-6 text-muted-foreground">
            Používáme nutné cookies. Analytická cookie nám pomůže anonymně spojit vyhledávání do jedné návštěvy.
          </p>
        </div>
        <div className="flex shrink-0 gap-2 sm:justify-end">
          <Button className="h-10 px-4" onClick={keepNecessaryOnly} type="button" variant="ghost">
            Pouze nutné
          </Button>
          <Button className="h-10 px-4" onClick={acceptAnalytics} type="button">
            Povolit
          </Button>
        </div>
      </div>
    </div>
  );
}

function StickyHeader({ loading, result }) {
  const hasVehicleResult = Boolean(result && result.kind !== "fleet");
  const dimensions = hasVehicleResult ? getVehicleDimensions(result) : null;
  const inspection = hasVehicleResult ? getInspectionOverview(result.inspections, result.inspectionLookup) : null;
  const vignette = hasVehicleResult ? getVignetteOverview(result.vignetteLookup, getResultPlate(result)) : null;

  return (
    <header className="relative z-40 border-b border-white/10 bg-background">
      <div className="container relative flex min-h-16 flex-col gap-3 py-3 sm:flex-row sm:items-center sm:justify-between">
        <a
          className="inline-flex w-fit items-center gap-3 rounded-full border border-white/10 bg-white/[0.04] px-3 py-2 text-sm font-semibold text-white transition-colors hover:border-white/20"
          href="#search"
        >
          <span className="inline-flex size-8 items-center justify-center rounded-full bg-primary text-primary-foreground">
            {loading ? <Loader2 className="size-4 animate-spin" /> : <CarFront className="size-4" />}
          </span>
          <span>AutoInfo</span>
        </a>

        <nav aria-label="Sekce reportu" className="flex min-w-0 flex-wrap items-center gap-2">
          <HeaderAnchor href="#search" icon={Search} label="Hledat" />
          {result ? <HeaderAnchor href="#summary" icon={FileText} label={result.kind === "fleet" ? "Flotila" : "Souhrn"} /> : null}
          {hasVehicleResult ? <HeaderAnchor href="#inspection" icon={Gauge} label="STK" /> : null}
          {hasVehicleResult ? <HeaderAnchor href="#vignette" icon={CheckCircle2} label="Znamka" /> : null}
          {dimensions ? <HeaderAnchor href="#dimensions" icon={CarFront} label="Rozmery" /> : null}
        </nav>

        {hasVehicleResult ? (
          <div className="hidden min-w-0 flex-wrap justify-end gap-2 lg:flex">
            <HeaderStatusChip href="#inspection" overview={inspection} />
            <HeaderStatusChip href="#vignette" overview={vignette} />
          </div>
        ) : null}
      </div>
    </header>
  );
}

function HeaderAnchor({ href, icon: Icon, label }) {
  return (
    <a
      className="inline-flex h-9 items-center gap-2 rounded-full border border-white/10 bg-white/[0.035] px-3 text-xs font-semibold uppercase tracking-[0.16em] text-muted-foreground transition-colors hover:border-white/20 hover:text-white"
      href={href}
    >
      <Icon className="size-3.5" />
      <span>{label}</span>
    </a>
  );
}

function HeaderStatusChip({ href, overview }) {
  if (!overview) {
    return null;
  }

  const Icon = overview.icon;
  return (
    <a
      className={cn(
        "inline-flex min-w-0 max-w-[220px] items-center gap-2 rounded-full border px-3 py-2 text-xs transition-colors",
        overview.tone === "success"
          ? "border-emerald-300/25 bg-emerald-300/[0.08] text-emerald-100"
          : overview.tone === "warning"
            ? "border-amber-300/25 bg-amber-300/[0.08] text-amber-100"
            : "border-white/10 bg-white/[0.04] text-muted-foreground"
      )}
      href={href}
    >
      <Icon className={cn("size-3.5 shrink-0", overview.loading ? "animate-spin" : "")} />
      <span className="truncate font-semibold">{overview.title}</span>
      <span className="truncate text-white/80">{formatFrontendText(overview.status)}</span>
    </a>
  );
}

function sortTimelineEntriesForDisplay(entries) {
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
  if (!value) {
    return Number.MAX_SAFE_INTEGER;
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? Number.MAX_SAFE_INTEGER : parsed.getTime();
}

function ResultState({ result }) {
  const timelineEntries = useMemo(
    () => sortTimelineEntriesForDisplay(Array.isArray(result.timeline) ? result.timeline : []),
    [result.timeline]
  );
  const parties = Array.isArray(result.ownership?.parties) ? result.ownership.parties : [];
  const legalParties = parties.filter(isLegalEntityParty);
  const inspections = result.inspections || null;
  const inspectionLookup = result.inspectionLookup || null;
  const ownershipLookup = result.ownershipLookup || null;
  const identifierHighlights = extractIdentifierHighlights(result.highlights);
  const summaryHighlights = buildSummaryHighlights(result);
  const dimensions = getVehicleDimensions(result);
  const subjectHistoryMeta = formatSubjectHistoryMeta(legalParties.length);

  return (
    <div className="space-y-4">
      <Card className="scroll-mt-24 border-white/10 bg-card/85" id="summary">
        <CardContent className="p-6 sm:p-8">
          <div className="space-y-6">
            <div className="flex flex-col gap-6 xl:flex-row xl:items-start xl:justify-between">
              <div className="space-y-4">
	                <div className="flex flex-wrap gap-2">
	                  <Badge>{formatFrontendText(result.hero?.badge || "Vozidlo")}</Badge>
	                  <Badge variant={statusVariant(result.hero?.status)}>
	                    {formatFrontendText(result.hero?.status || "Neuvedeno")}
	                  </Badge>
	                </div>

                <div className="space-y-3">
	                  <h2 className="text-3xl font-semibold tracking-[-0.04em] text-white sm:text-5xl">
	                    {formatFrontendText(result.hero?.title || "Bez názvu")}
	                  </h2>
                </div>
              </div>

              <div className="grid gap-2 sm:max-w-[380px] xl:w-[380px] xl:shrink-0">
                {identifierHighlights.map((item) => (
                  <IdentifierCard key={`${item.label}-${item.value}`} label={item.label} value={item.value} />
                ))}
              </div>
            </div>

            <div className="flex flex-wrap gap-2 text-sm text-muted-foreground">
              <InlineMeta icon={CarFront} value={renderQuery(result.query)} />
              {subjectHistoryMeta ? <InlineMeta icon={LineChart} value={subjectHistoryMeta} /> : null}
              <InlineMeta icon={Clock3} value={`Aktualizováno ${formatResolvedTime(result.query?.resolvedAt)}`} />
            </div>

            {summaryHighlights.length > 0 ? (
              <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                {summaryHighlights.map((item) => (
                  <HighlightCard item={item} key={`${item.label}-${item.value}`} />
                ))}
              </div>
            ) : null}
          </div>
        </CardContent>
      </Card>

      <RoadStatusPanel inspections={inspections} inspectionLookup={inspectionLookup} result={result} />

      {dimensions ? <VehicleDimensionsPanel dimensions={dimensions} /> : null}

	      <OwnershipPanel ownership={result.ownership} parties={parties} lookup={ownershipLookup} query={result.query} />

      {inspections || inspectionLookup ? (
        <InspectionPanel inspections={inspections} inspectionLookup={inspectionLookup} />
      ) : null}

      {timelineEntries.length > 0 ? (
        <Card className="border-white/10 bg-card/72">
          <CardHeader className="pb-4">
            <CardDescription className="uppercase tracking-[0.22em] text-muted-foreground">
              Historie
            </CardDescription>
              <CardTitle className="text-2xl text-white">Časová osa</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2.5">
            {timelineEntries.map((entry, index) => (
              <div
                className="grid gap-3 rounded-[1.1rem] border border-white/8 bg-background/50 px-4 py-3 md:grid-cols-[160px_minmax(0,1fr)]"
                key={`${entry.title}-${index}`}
              >
                <p className="text-sm font-medium text-muted-foreground">{formatDate(entry.date)}</p>
                <div>
	                  <p className="text-sm font-semibold text-white">{formatFrontendText(entry.title || "Událost")}</p>
                  <p className="mt-1 text-sm leading-6 text-muted-foreground">
	                    {formatFrontendText(entry.description || "Bez dalšího detailu.")}
	                  </p>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}

function RoadStatusPanel({ result, inspections, inspectionLookup }) {
  const plate = getResultPlate(result);
  const inspection = getInspectionOverview(inspections, inspectionLookup);
  const vignette = getVignetteOverview(result?.vignetteLookup, plate);

  return (
    <section className="grid gap-4 lg:grid-cols-2">
      <RoadStatusCard id="inspection" overview={inspection} />
      <RoadStatusCard id="vignette" overview={vignette} />
    </section>
  );
}

function RoadStatusCard({ id, overview }) {
  const Icon = overview.icon;

  return (
    <article
      className={cn(
        "scroll-mt-24 rounded-[1.5rem] border p-5",
        overview.tone === "success"
          ? "border-emerald-300/20 bg-emerald-300/[0.07]"
          : overview.tone === "warning"
            ? "border-amber-300/20 bg-amber-300/[0.08]"
            : "border-white/10 bg-card/78"
      )}
      id={id}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <p className="text-[11px] font-semibold uppercase tracking-[0.26em] text-muted-foreground">
            {overview.kicker}
          </p>
          <h3 className="mt-2 text-2xl font-semibold text-white">{overview.title}</h3>
          <p className="mt-2 text-sm leading-7 text-muted-foreground">
            {formatFrontendText(overview.description)}
          </p>
        </div>

        <div className="flex shrink-0 flex-col items-end gap-3">
          <div className="rounded-full border border-white/10 bg-background/80 p-2.5">
            <Icon
              className={cn(
                "size-5",
                overview.loading ? "animate-spin text-muted-foreground" : overview.tone === "success" ? "text-emerald-200" : overview.tone === "warning" ? "text-amber-200" : "text-muted-foreground"
              )}
            />
          </div>
          <Badge variant={overview.tone === "success" ? "success" : overview.tone === "warning" ? "warning" : "muted"}>
            {formatFrontendText(overview.status)}
          </Badge>
        </div>
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-2">
        {overview.items.map((item) => (
          <div className="min-w-0 rounded-[1rem] border border-white/10 bg-background/45 px-3 py-3" key={`${id}-${item.label}`}>
            <p className="text-[10px] font-semibold uppercase tracking-[0.22em] text-muted-foreground">
              {formatFrontendText(item.label)}
            </p>
            <p className="mt-1 break-words text-sm font-semibold leading-6 text-white">
              {formatFrontendText(item.value || "-")}
            </p>
          </div>
        ))}
      </div>
    </article>
  );
}

function VehicleDimensionsPanel({ dimensions }) {
  return (
    <Card className="scroll-mt-24 border-white/10 bg-card/80" id="dimensions">
      <CardHeader className="pb-4">
        <CardDescription className="uppercase tracking-[0.22em] text-muted-foreground">
          Rozměry
        </CardDescription>
        <CardTitle className="text-2xl text-white">Rozměry a hmotnost vozidla</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="rounded-[1.5rem] border border-white/10 bg-[#101722] p-3 text-slate-100 shadow-soft sm:p-4">
          <div className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_300px]">
            <VehicleDimensionsDrawing dimensions={dimensions} />

            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-1">
              <div className="rounded-[1.25rem] border border-white/10 bg-[#0c121b] p-3">
                <VehicleFrontDimensions dimensions={dimensions} />
              </div>
              <div className="flex min-h-[180px] items-center justify-center rounded-[1.25rem] border border-white/10 bg-[#0c121b] p-5">
                <WeightIcon className="mr-5 size-16 text-[#8fb4ff]" />
                <div>
                  <p className="text-2xl font-semibold text-[#8fb4ff]">Hmotnost</p>
                  <p className="text-4xl font-semibold text-slate-50">{dimensions.weight || "-"}</p>
                </div>
              </div>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function VehicleSideSilhouette(props) {
  return (
    <svg aria-hidden="true" viewBox="0 0 150 54" {...props}>
      <path fill="#474647" d="m14.8 19.8c-4.3 1-7.5 2.9-9.5 5-0.5 0.7-1.7 3.3-1.9 4.6-0.1 0.7-1.8 1.2-1.8 2.1-0.1 0.9-0.1 5 0 5.2 0.4 0.7 0.3 3.1 0.3 3.1s-0.6 0.8-0.5 1.5c0.1 1.4 0.6 1.9 1.6 2.4 2.6 1.4 7 1.9 11.8 2.7l27.7-0.9h0.1c3.5-0.3 4.3-0.2 6.4-0.2l53-0.1 31.4-0.6c1.1 0.1 7.6-0.8 10.6-1.2 1.7-0.3 2.7-1.3 4.1-3 1-1.2 0.5-2.3 0.2-3.1-0.2-0.7 0.2-5.9-0.3-6.7-0.4-0.5-1.7-1.1-1.9-1.7-0.5-1.3-0.1-6.2-0.3-10.2-0.1-1.6-0.4-2.4-1.4-3.4-0.5-0.6-1.6-1.6-1.9-1.7l-9.1-7.1s-0.3-0.2-0.2-0.5c0.2-0.8 2.9-0.8 2.7-2.4-0.2-0.4-0.7-0.4-3-0.6-5.1-0.5-19.3-2-39.5-2.3-5.1 0-12.3-0.1-20.4 0.7-4 0.4-7.3 1.1-11.3 2.9-5.2 2.3-12.7 6.4-19.5 11.7-6.6 0.5-17.5 1.1-27.4 3.8z" />
      <path fill="#2B2A29" d="m14.7 46.4h28.4l-0.6-0.9c0-6.3-0.7-10.8-3.9-14-2.5-2.4-5.5-4.2-10-4.1-5.2 0.1-9.2 2.9-11.2 6.1-2.7 4.2-2.6 9.7-2.7 12.9z" />
      <path fill="#2B2A29" d="m105.2 45.3h28.2c0.1-4.5-0.7-9.8-3.2-13.1-2.5-3.2-6.3-4.9-10.6-4.8-5 0.1-8 1.9-10.5 5-3.1 4.1-3.9 9.1-3.9 12.9z" />
      <path fill="#434242" d="m134 3c-1-0.1-15.8-2.1-39.7-2.5-5.3 0-13.2 0-20.5 0.8-6.6 0.7-9.9 1.9-12.2 2.9-4.5 2-13.2 6.7-19.4 11.1v1c3.7-2.3 12.3-7.4 19.8-11.1 5-2.2 8.6-3.3 14-3.8 4-0.4 11.1-0.8 18-0.7 19.2 0.2 36.4 1.5 41.2 2.1 0.5-0.3-0.1 0.3-1.2 0.2z" />
      <path fill="none" stroke="#383838" strokeMiterlimit="10" strokeWidth="0.25" d="m139.6 13.8" />
      <path fill="#9D9E9E" d="m82.4 2.9c-8.9 0.3-13.2 0.7-18.3 3.1-3.9 1.8-9.7 5.3-13.6 8.5-0.5 0.3-1 3.4-0.5 3.5l29.4-1.4 3.1-13.2c0.1-0.4 0.1-0.5-0.1-0.5zm24.5 0.5c-4.8-0.3-10.9-0.5-19.8-0.6l-1.1 0.2-1.1 13.4 27.6-1.8-5.6-11.2zm12.1 2.3c-1.6-0.9-3.1-1.3-10.5-2l5.4 10.4c3.7-0.4 10.1-2.1 10.5-3.1 0.3-0.4-1.4-1.8-5.4-4.3z" />
      <path fill="none" stroke="#3C3B3B" strokeLinecap="round" strokeLinejoin="round" strokeMiterlimit="10" strokeWidth="0.25" d="m82.2 16.5c-1 4.5-1.6 7-1.7 10.9-0.1 7 0.1 15.7 0.1 15.7" />
      <path fill="none" stroke="#3C3B3B" strokeLinecap="round" strokeLinejoin="round" strokeMiterlimit="10" strokeWidth="0.25" d="m103.4 42.7c0.8-7.2 4.1-14.2 11-16.8l0.9-7.8c0.2-1.1-1.4-3.8-1.4-3.8" />
      <path fill="none" stroke="#383838" strokeMiterlimit="10" strokeWidth="0.25" d="m42.4 43.5 62.8-0.8h0.3" />
      <path fill="#333232" d="m17.2 42c0 5.8 4.6 11.5 11.4 11.6 6.1 0 11.3-4.6 11.4-11.6 0-5.5-4.4-11.5-11.4-11.5-6.2 0-11.4 5-11.4 11.5z" />
      <path fill="#656565" d="m21.1 41.8c0 3.9 2.9 7.6 7.5 7.6 3.8 0 7.8-2.8 7.8-7.5 0-3.8-2.9-7.6-7.8-7.7-3.9 0-7.5 3-7.5 7.6z" />
      <path fill="#333232" d="m108 41.9c0 5.8 4.4 11.6 11.4 11.7 6.2 0 11.8-4.4 11.8-11.7-0.1-5.5-4.3-11.4-11.6-11.4-6.5 0-11.6 5.2-11.6 11.4z" />
      <path fill="#656565" d="m112 41.8c0 3.8 3 7.6 7.4 7.6 3.8 0 7.8-2.8 7.8-7.5 0-3.9-2.8-7.6-7.7-7.6-4 0-7.5 3.1-7.5 7.5z" />
      <path fill="none" stroke="#383838" strokeMiterlimit="10" strokeWidth="0.25" d="m126.1 3.2c0.5 1.7 2.1 2.3 3.5 3.3l8.3 5.6" />
      <path fill="#363535" d="m72.9 21c-0.9 0.3-0.7 1.1 0.2 1.1l5.8-0.3c0.8 0 0.7 0 0.6-0.9-0.2-0.5-4.9-0.6-6.6 0.1z" />
      <path fill="#363535" d="m105.5 19.8" />
      <path fill="#363535" d="m105.5 19.8" />
      <path fill="#363535" d="m105.5 20" />
      <path fill="#363535" d="m105.4 20" />
      <path fill="#363535" d="m105.5 19.8" />
      <path fill="#363535" d="m105.5 20c0.1 0.7 0.7 0.5 1.5 0.5l5.1-0.2c0.5 0 0.9 0 0.8-0.8-0.2-0.6-5.2-0.9-7.5 0.1l0.1 0.4z" />
    </svg>
  );
}

function VehicleFrontSilhouette(props) {
  return (
    <svg aria-hidden="true" viewBox="0 0 150 150" {...props}>
      <path fill="#282726" d="m4.7 96v20.4c0 10 0.2 13.6 3.4 13.6h13.5c1.6 0 2.5-1.7 2.6-3.8s0.1-8 0.1-8h8c0.8 0 1.2 0.2 1.5-1.2l0.2-0.5-29.3-20.5z" />
      <path fill="#282726" d="m144.4 96v20.4c-0.1 10.1-0.1 10.7-0.4 11.8-0.4 0.8-1.6 1.8-2.7 1.8h-13.4c-1.9-0.1-2.9-1.6-2.9-4.2v-7.6h-7.6c-1.4 0-1.5-0.4-1.7-1.3l-0.1-0.4 28.8-20.5z" />
      <path fill="#444344" d="m136.5 57.3-0.1-0.3 1.6-2.5c6.8-0.5 9.6 0.5 10.4-4.5 0.7-3.6-0.9-5.5-10.1-6.6-3-0.4-4.4 0-4.4 2.6 0 2.8 0.4 5.4 0.4 5.4l-1 0.5c-2.6-6.1-8.1-18-12-24-2.6-3.8-5-5.6-7.7-5.8-4.6-0.3-17.5-2.1-38.2-2.1-16.7 0-26.8 0.7-38.8 2.1-4.2 0.5-7 4.1-9 7.7-3.8 6.8-8.3 17.2-11.1 22.3l-1.1-0.5c0.4-1.4 0.6-5.5 0.5-7-0.2-1.2-1.1-1.7-2.8-1.5-9 0.5-11.9 1.8-11.4 6 0.6 4.9 2.8 4.7 7.3 5.1l2.5 0.2 1.5 2.9c-2.2 2.8-7.1 9.2-7.9 12.7-1.2 5.2-1.3 17.3-0.1 28 0.8 7.3 1.6 16.5 7 17.6 3.1 0.7 8.5 0.6 22.3 0.9h81.3c12.3-0.1 19.4 0.3 22.8-1.1 4-1.8 5.5-8.6 6.2-19.4 0.9-9.9 0.7-23-0.8-26.5-2.1-5.1-6.7-11.1-7.3-12.2z" />
      <path fill="#99999A" d="m23.3 49.1c2.1-5.7 5.8-13.9 9-19.7 0.3-0.8 1.2-1 1.8-1.1 8.6-1 21.1-2.1 39.7-2.2 16.8 0 32.2 1 41.8 2 1 0.1 1.5 0.5 1.9 1.3 3.3 6.2 8.9 19.5 9 19.9 0.5 1.3-1 2.1-3.2 2-4.6 0-22.2-0.6-49.4-0.6-26.1 0-36.4 0.3-46.5 0.6-4.2 0.1-4.4-1.4-4.1-2.2z" />
      <path fill="#99999A" d="m12.2 66.1c0.4-0.2 0.7 0 1.7 0.3 5 0.9 10.2 1.7 14.5 2.6 1.6 0.4 3.3 1 4 2.3l4.4 8.3c0.2 0.3 0.2 1-0.7 0.8-4.5-0.5-17.8-2-23.5-3.5-1.7-0.7-2.4-1.5-2.2-7.4 0.3-2.6 0.7-3.1 1.8-3.4z" />
      <path fill="#99999A" d="m136.8 66.1c-0.2-0.1-0.6 0.1-1.4 0.3-3.3 0.7-9.5 1.6-14.3 2.7-1.6 0.3-3.5 1.1-4.2 2.4l-4 7.9c-0.3 0.5-0.2 1.1 1.1 1 4.2-0.5 18.1-2.4 22.2-3.3 1.9-0.6 2.5-1.3 2.6-4.6 0-2.5-0.2-6-2-6.4z" />
      <path fill="#2B2B2C" d="m25.5 111.5 98.2-0.1c0.9 0 0.6-0.5 0.3-0.8l-8.4-11.7c-1.1-1.5-2.1-2.2-4.1-2.2l-73.7-0.1c-1.4 0-2.7 0.8-4 2.4l-8.6 11.5c-0.3 0.4-0.2 1 0.3 1z" />
    </svg>
  );
}

function VehicleDimensionsDrawing({ dimensions }) {
  return (
    <div className="rounded-[1.25rem] border border-white/10 bg-[#0c121b] p-2">
      <svg className="h-auto w-full" role="img" aria-label="Grafické zobrazení rozměrů vozidla" viewBox="0 0 880 520">
        <defs>
          <marker id="dimension-arrow" markerHeight="7" markerWidth="7" orient="auto" refX="3.5" refY="3.5">
            <path d="M0 0 7 3.5 0 7Z" fill="#8fb4ff" />
          </marker>
        </defs>

        <rect width="880" height="520" fill="#0c121b" />
        <VehicleSideSilhouette x="45" y="120" width="745" height="268" preserveAspectRatio="xMidYMid meet" />

        <g stroke="#8fb4ff" strokeWidth="2.2">
          <path d="M66 345v142M800 345v142M187 337v92M638 337v92" strokeDasharray="8 9" />
          <path d="M73 468H793" markerEnd="url(#dimension-arrow)" markerStart="url(#dimension-arrow)" />
          <path d="M196 385H630" markerEnd="url(#dimension-arrow)" markerStart="url(#dimension-arrow)" />
          <path d="M846 117V400" markerEnd="url(#dimension-arrow)" markerStart="url(#dimension-arrow)" />
          <path d="M768 117h58M768 400h58" strokeDasharray="8 9" />
        </g>
        <g fontFamily="inherit" fontWeight="700">
          <text fill="#8fb4ff" fontSize="24" textAnchor="middle" x="420" y="437">Rozvor</text>
          <text fill="#f8fafc" fontSize="28" textAnchor="start" x="500" y="437">{dimensions.wheelbase || "-"}</text>
          <text fill="#8fb4ff" fontSize="24" textAnchor="middle" x="420" y="510">Délka</text>
          <text fill="#f8fafc" fontSize="28" textAnchor="start" x="496" y="510">{dimensions.length || "-"}</text>
          <rect fill="#101722" stroke="#263241" height="76" rx="10" width="122" x="738" y="204" />
          <text fill="#8fb4ff" fontSize="24" x="752" y="236">Výška</text>
          <text fill="#f8fafc" fontSize="26" x="752" y="274">{dimensions.height || "-"}</text>
        </g>
      </svg>
    </div>
  );
}

function VehicleFrontDimensions({ dimensions }) {
  return (
    <svg className="h-auto w-full" role="img" aria-label="Grafické zobrazení šířky vozidla" viewBox="0 0 320 235">
      <defs>
        <marker id="front-dimension-arrow" markerHeight="7" markerWidth="7" orient="auto" refX="3.5" refY="3.5">
          <path d="M0 0 7 3.5 0 7Z" fill="#8fb4ff" />
        </marker>
      </defs>
      <rect width="320" height="235" fill="#0c121b" />
      <VehicleFrontSilhouette x="55" y="0" width="210" height="210" />
      <g stroke="#8fb4ff" strokeWidth="2">
        <path d="M45 139v70M275 139v70" strokeDasharray="7 8" />
        <path d="M52 198H268" markerEnd="url(#front-dimension-arrow)" markerStart="url(#front-dimension-arrow)" />
      </g>
      <text fill="#8fb4ff" fontSize="20" fontWeight="700" textAnchor="end" x="160" y="225">Šířka</text>
      <text fill="#f8fafc" fontSize="22" fontWeight="700" x="174" y="225">{dimensions.width || "-"}</text>
    </svg>
  );
}

function WeightIcon({ className }) {
  return (
    <svg aria-hidden="true" className={className} viewBox="0 0 72 72">
      <path d="M19 28h34l7 35H12l7-35Z" fill="currentColor" />
      <path d="M27 28a9 9 0 0 1 18 0" fill="none" stroke="currentColor" strokeWidth="7" />
    </svg>
  );
}

function OwnershipPanel({ ownership, parties, lookup, query }) {
  const sourceParties = Array.isArray(parties) ? parties : [];
  const legalParties = sourceParties.filter(isLegalEntityParty);
  const { currentParties, historicalParties } = splitOwnershipParties(legalParties);
  const timelineParties = [...currentParties, ...historicalParties];
  const [showHistory, setShowHistory] = useState(false);
  const legalOwnerCount = countLegalRole(legalParties, "vlast");
  const legalOperatorCount = countLegalRole(legalParties, "provoz");
  const uniqueLegalCount = countUniqueLegalEntities(legalParties);
  const sourceOwnerCount = ownership?.ownerCount ?? null;
  const sourceOperatorCount = ownership?.operatorCount ?? null;
  const lookupMessage = buildOwnershipPanelMessage({ lookup, parties: legalParties, sourceOwnerCount, sourceOperatorCount, query });
  const statusText = formatOwnershipStatus(lookup, uniqueLegalCount || legalParties.length);

  return (
    <section className="overflow-hidden rounded-[2rem] border border-white/10 bg-card/78">
      <div className="p-5 sm:p-7">
          <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
              <p className="text-[11px] font-semibold uppercase tracking-[0.32em] text-muted-foreground">
                Vlastnictví
              </p>
                <Badge variant={lookup?.status === "pending" ? "muted" : legalParties.length ? "success" : "warning"}>
                  {lookup?.status === "pending" ? "Dohledávám" : legalParties.length ? "Detail připraven" : "Bez detailu"}
                </Badge>
              </div>
              <h3 className="mt-3 text-3xl font-semibold text-white sm:text-4xl">Aktuální vlastník a provozovatel</h3>
              <p className="mt-3 max-w-3xl text-sm leading-7 text-muted-foreground">
                {lookupMessage}
              </p>
            </div>

            <div className="grid grid-cols-2 gap-2 sm:min-w-[440px] sm:grid-cols-4">
              <MetricTile label="Subjekty" value={uniqueLegalCount || "-"} muted={!uniqueLegalCount} />
              <MetricTile label="Vazby" value={legalParties.length || "-"} muted={!legalParties.length} />
              <MetricTile label="Vlastníci" value={legalOwnerCount || "-"} muted={!legalOwnerCount} />
              <MetricTile label="Provoz." value={legalOperatorCount || "-"} muted={!legalOperatorCount} />
            </div>
          </div>

          <div className="mt-5 flex flex-wrap gap-2 text-sm text-muted-foreground">
            {lookup?.vin ? <InlineMeta icon={Fingerprint} value={`VIN ${lookup.vin}`} /> : null}
            <InlineMeta icon={CheckCircle2} value={statusText} />
          </div>

          <div className="mt-6">
            {legalParties.length > 0 ? (
                <OwnershipGroup
                  title="Aktuální stav"
                  description="Nejnovější neukončená vazba pro každou roli."
                  parties={currentParties}
                  emptyText="Aktuální právnická osoba není v detailu zdroje uvedena."
                  emphasis
                />
            ) : (
                <EmptyPanel text="Aktuální právnický vlastník nebo provozovatel není v detailu zdroje uveden." />
            )}
          </div>

          <div className="mt-6 border-t border-white/10 pt-5">
            <button
              className="flex w-full flex-col gap-3 rounded-[1.1rem] border border-white/10 bg-background/45 px-4 py-3 text-left transition-colors hover:border-white/20 hover:bg-white/[0.04] sm:flex-row sm:items-center sm:justify-between"
              onClick={() => setShowHistory((current) => !current)}
              type="button"
            >
              <span className="flex min-w-0 items-center gap-3">
                <span className="rounded-full border border-white/10 bg-white/[0.06] p-2">
                  <ChevronDown className={cn("size-4 text-white transition-transform", showHistory ? "rotate-180" : "")} />
                </span>
                <span className="min-w-0">
                  <span className="block text-sm font-semibold text-white">
                    {showHistory ? "Skrýt historii vztahů" : "Zobrazit historii vztahů"}
                  </span>
                  <span className="mt-1 block text-sm leading-6 text-muted-foreground">
                    Historické právnické vazby v přehledné tabulce.
                  </span>
                </span>
              </span>
              <Badge variant={historicalParties.length ? "muted" : "warning"}>{historicalParties.length} záznamů</Badge>
            </button>

            {showHistory ? (
              <div className="mt-4">
                <OwnershipHistoryTable parties={timelineParties} />
              </div>
            ) : null}
          </div>
      </div>
    </section>
  );
}

function OwnershipHistoryTable({ parties }) {
  const rows = groupOwnershipHistoryRows(parties);
  const showAdditionalRoles = rows.some((row) => row.others.length > 0);

  if (!rows.length) {
    return <EmptyPanel text="Historické právnické vazby nejsou ve zdroji uvedené." />;
  }

  return (
    <div className="overflow-hidden rounded-[1.1rem] border border-white/10">
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-white/10 text-left text-sm">
          <thead className="bg-white/[0.03] text-[11px] font-semibold uppercase tracking-[0.2em] text-muted-foreground">
            <tr>
              <th className="px-4 py-3">Období</th>
              <th className="px-4 py-3">Vlastník</th>
              <th className="px-4 py-3">Provozovatel</th>
              {showAdditionalRoles ? <th className="px-4 py-3">Další role</th> : null}
            </tr>
          </thead>
          <tbody className="divide-y divide-white/10">
            {rows.map((row) => (
              <tr className="align-top transition-colors hover:bg-white/[0.03]" key={row.key}>
                <td className="min-w-[180px] whitespace-nowrap px-4 py-4 font-semibold text-white">
                  {row.period}
                </td>
                <td className="min-w-[260px] px-4 py-4">
                  <OwnershipHistoryPartyCell emptyText="Vlastník ve zdroji neuveden" parties={row.owners} />
                </td>
                <td className="min-w-[260px] px-4 py-4">
                  <OwnershipHistoryPartyCell emptyText="Provozovatel ve zdroji neuveden" parties={row.operators} />
                </td>
                {showAdditionalRoles ? (
                  <td className="min-w-[220px] px-4 py-4">
                    <OwnershipHistoryPartyCell emptyText="Další role ve zdroji neuvedena" parties={row.others} showRole />
                  </td>
                ) : null}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function OwnershipHistoryPartyCell({ parties, showRole = false, emptyText = "-" }) {
  if (!parties.length) {
    return <span className="text-muted-foreground">{emptyText}</span>;
  }

  return (
    <div className="space-y-3">
      {parties.map((party, index) => (
        <div className="min-w-0" key={`${party.role}-${party.ico || party.name}-${index}`}>
          {showRole ? (
            <Badge variant="muted">{party.role || "Subjekt"}</Badge>
          ) : null}
          <div className={cn("font-semibold text-white", showRole ? "mt-2" : "")}>
            {party.ico ? (
              <a className="inline-flex max-w-full items-center gap-2 hover:text-primary" href={buildAresUrl(party.ico)} rel="noreferrer" target="_blank">
                <span className="truncate">{party.name || party.ico}</span>
                <ArrowUpRight className="size-3.5 shrink-0" />
              </a>
            ) : (
              party.name || "Právnická osoba"
            )}
          </div>
          <div className="mt-1 text-sm leading-6 text-muted-foreground">
            <IcoLookupLink emptyText="IČO neuvedeno" ico={party.ico} />
            {party.address ? (
              <span className="block break-words">{party.address}</span>
            ) : null}
          </div>
        </div>
      ))}
    </div>
  );
}

function OwnershipGroup({ title, description, parties, emptyText, emphasis = false }) {
  return (
    <section className="space-y-3">
      <div className="flex flex-col gap-1 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <p className="text-[11px] font-semibold uppercase tracking-[0.26em] text-muted-foreground">{title}</p>
          <p className="mt-1 text-sm leading-6 text-muted-foreground">{description}</p>
        </div>
        <Badge variant={emphasis ? "success" : "muted"}>{parties.length || 0} záznamů</Badge>
      </div>

      {parties.length > 0 ? (
        <div className="grid gap-3 lg:grid-cols-2">
          {parties.map((party, index) => (
            <LegalEntityCard
              party={party}
              emphasis={emphasis}
              key={`${title}-${party.role}-${party.ico || party.name}-${index}`}
            />
          ))}
        </div>
      ) : (
        <EmptyPanel text={emptyText} />
      )}
    </section>
  );
}

function LegalEntityCard({ party, emphasis = false }) {
  const aresUrl = party.ico ? buildAresUrl(party.ico) : null;

  return (
    <article
      className={cn(
        "group rounded-[1.25rem] border p-4 transition-colors hover:border-white/20",
        emphasis ? "border-emerald-300/20 bg-emerald-300/[0.07]" : "border-white/10 bg-background/60"
      )}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="flex min-w-0 items-start gap-3">
          <div className="rounded-full border border-white/10 bg-white/[0.06] p-2.5">
            <Building2 className="size-5 text-white" />
          </div>
          <div className="min-w-0">
            <div className="flex flex-wrap gap-2">
              <Badge variant={party.current ? "success" : "muted"}>{party.role || "Subjekt"}</Badge>
              {party.current ? <Badge variant="muted">Aktuální</Badge> : null}
              {party.sourceOpenEnded ? <Badge variant="muted">Neukončeno ve zdroji</Badge> : null}
            </div>
            {aresUrl ? (
              <a
                href={aresUrl}
                target="_blank"
                rel="noreferrer"
                className="mt-3 inline-flex items-center gap-2 break-words text-lg font-semibold leading-6 text-white transition-colors hover:text-primary"
              >
                {party.name || party.ico}
                <ArrowUpRight className="size-4 shrink-0" />
              </a>
            ) : (
              <p className="mt-3 break-words text-lg font-semibold leading-6 text-white">{party.name || "Právnická osoba"}</p>
            )}
          </div>
        </div>
      </div>

      <div className="mt-4 grid gap-2 text-sm leading-6 text-muted-foreground sm:grid-cols-2">
        <IcoLookupLink ico={party.ico} />
        <span>{party.period || (party.since ? `od ${party.since}` : "Období neuvedeno")}</span>
        {party.address ? <span className="sm:col-span-2">{party.address}</span> : null}
      </div>
    </article>
  );
}

function CompanyFleetState({ onVehicleVinLookup, result }) {
  const records = Array.isArray(result.records) ? result.records : [];
  const historyRecords = Array.isArray(result.historyRecords) ? result.historyRecords : [];
  const companyHistorySourceRecords = Array.isArray(result.companyHistoryRecords) && result.companyHistoryRecords.length > 0
    ? result.companyHistoryRecords
    : historyRecords;
  const activeRecords = records.filter((record) => record.current !== false);
  const companyHistoryRecords = companyHistorySourceRecords.filter((record) => record.current !== true);
  const companyName = result.company?.name || `Firma ${result.company?.ico || result.query?.normalized || ""}`;
  const [expandedVehicleId, setExpandedVehicleId] = useState(null);
  const [activePage, setActivePage] = useState(1);
  const [historyPage, setHistoryPage] = useState(1);
  const [showCompanyHistory, setShowCompanyHistory] = useState(false);
  const activeTotal = activeRecords.length;
  const companyHistoryTotal = Math.max(
    companyHistoryRecords.length,
    Number(result.summary?.companyHistoryVehicleCount ?? result.summary?.historicalVehicleCount ?? 0)
  );
  const visibleActiveRecords = paginateRecords(activeRecords, activePage, FLEET_PAGE_SIZE);
  const visibleCompanyHistoryRecords = showCompanyHistory
    ? paginateRecords(companyHistoryRecords, historyPage, FLEET_PAGE_SIZE)
    : [];

  useEffect(() => {
    setExpandedVehicleId(null);
    setActivePage(1);
    setHistoryPage(1);
    setShowCompanyHistory(false);
  }, [result.query?.normalized]);

  useEffect(() => {
    setActivePage((page) => clampPage(page, activeTotal, FLEET_PAGE_SIZE));
  }, [activeTotal]);

  useEffect(() => {
    setHistoryPage((page) => clampPage(page, companyHistoryRecords.length, FLEET_PAGE_SIZE));
  }, [companyHistoryRecords.length]);

  return (
    <div className="space-y-4">
      <Card className="scroll-mt-24 border-white/10 bg-card/85" id="summary">
        <CardContent className="p-6 sm:p-8">
          <div className="space-y-5">
            <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
              <div className="space-y-3">
                <div className="flex flex-wrap gap-2">
                  <Badge>Právnická osoba</Badge>
                  <Badge variant="muted">Flotila dle IČO</Badge>
                </div>
                <div>
                  <h2 className="text-3xl font-semibold tracking-[-0.04em] text-white sm:text-5xl">
                    {companyName}
                  </h2>
	                  <p className="mt-3 max-w-3xl text-sm leading-7 text-muted-foreground sm:text-base">
	                    Aktuálně vlastněná nebo provozovaná vozidla podle otevřených dat Registru silničních vozidel. Historii vztahu rozbalíte u konkrétního vozidla.
	                  </p>
                </div>
              </div>

              <div className="grid gap-2 sm:grid-cols-2 xl:min-w-[320px]">
                <IdentifierCard
                  href={result.company?.ico || result.query?.normalized ? buildSharePath("ico", result.company?.ico || result.query?.normalized) : null}
                  label="IČO"
                  onClick={(ico) => navigateToSharedLookup("ico", ico)}
                  value={result.company?.ico || result.query?.normalized || "-"}
                />
                <IdentifierCard label="Adresa" value={result.company?.address || "-"} />
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
	              <MiniStat label="Aktuální" value={result.summary?.activeVehicleCount ?? result.summary?.currentVehicleCount} />
		              <MiniStat label="Historie" value={companyHistoryTotal} />
		              <MiniStat label="Vztahy" value={result.summary?.relationshipCount} />
			              <MiniStat label="Zobrazeno" value={`${activeTotal}${companyHistoryTotal ? ` + ${companyHistoryTotal}` : ""}`} />
            </div>

            <div className="flex flex-wrap gap-2 text-sm text-muted-foreground">
              <InlineMeta icon={Building2} value={`IČO | ${result.query?.normalized || "-"}`} />
              <InlineMeta icon={Clock3} value={`Aktualizováno ${formatResolvedTime(result.query?.resolvedAt)}`} />
              {result.summary?.sourceUpdatedAt ? (
                <InlineMeta icon={FileText} value={`Dataset ${formatDate(result.summary.sourceUpdatedAt)}`} />
              ) : null}
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="border-white/10 bg-card/78">
        <CardHeader className="pb-4">
	              <CardDescription className="uppercase tracking-[0.22em] text-muted-foreground">
	            Vozidla firmy
	          </CardDescription>
	          <CardTitle className="text-2xl text-white">
	            {activeRecords.length > 0 ? "Aktuálně vlastněná/provozovaná vozidla" : "Bez aktivních vozidel"}
	          </CardTitle>
	        </CardHeader>
	        <CardContent className="space-y-3">
		          {activeTotal > 0 ? (
		            visibleActiveRecords.map((record, index) => {
		              const recordId = getFleetVehicleId(record, index);
		              return (
				                <CompanyFleetVehicleRow
			                  expanded={expandedVehicleId === recordId}
			                  key={recordId}
			                  companyIco={result.company?.ico || result.query?.normalized}
			                  onToggle={() => setExpandedVehicleId((current) => (current === recordId ? null : recordId))}
                        onVehicleVinLookup={onVehicleVinLookup}
			                  record={record}
		                  recordId={recordId}
			                />
			              );
		            })
		          ) : (
		            <EmptyPanel text={result.message || "Pro zadané IČO nebyla v otevřených datech nalezena žádná aktuálně vlastněná nebo provozovaná vozidla."} />
		          )}
          <FleetPagination
            onPageChange={(page) => {
              setActivePage(page);
              setExpandedVehicleId(null);
            }}
            page={activePage}
            pageSize={FLEET_PAGE_SIZE}
            total={activeTotal}
          />
	          {result.summary?.truncated ? (
	            <p className="text-sm leading-7 text-muted-foreground">
	              Výsledek byl zkrácen na prvních 200 vozidel, aby zůstal rychlý a čitelný.
	            </p>
	          ) : null}
	        </CardContent>
	      </Card>
	      {companyHistoryTotal > 0 ? (
        <Card className="border-white/10 bg-card/78">
          <CardHeader className="pb-4">
            <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
              <div>
                <CardDescription className="uppercase tracking-[0.22em] text-muted-foreground">
                  Historie firmy
                </CardDescription>
                <CardTitle className="mt-2 text-2xl text-white">Dříve vlastněná/provozovaná vozidla</CardTitle>
              </div>
              <Button
                className="w-full sm:w-auto"
                onClick={() => {
                  setShowCompanyHistory((current) => !current);
                  setExpandedVehicleId(null);
                }}
                type="button"
                variant="outline"
              >
                <ChevronDown className={cn("size-4 transition-transform", showCompanyHistory ? "rotate-180" : "")} />
                {showCompanyHistory ? "Skrýt historii" : "Zobrazit historii"}
              </Button>
            </div>
          </CardHeader>
          {showCompanyHistory ? (
          <CardContent className="space-y-3">
            {visibleCompanyHistoryRecords.length > 0 ? (
              visibleCompanyHistoryRecords.map((record, index) => {
                const recordId = `history-${getFleetVehicleId(record, index)}`;
                return (
                  <CompanyFleetVehicleRow
                    expanded={expandedVehicleId === recordId}
                    historical
                    key={recordId}
                    companyIco={result.company?.ico || result.query?.normalized}
                    onToggle={() => setExpandedVehicleId((current) => (current === recordId ? null : recordId))}
                    onVehicleVinLookup={onVehicleVinLookup}
                    record={record}
                    recordId={recordId}
                  />
                );
              })
            ) : (
              <EmptyPanel text="Souhrn uvádí historická vozidla, ale detailní seznam není v aktuálním indexu načtený." />
            )}
            <FleetPagination
              onPageChange={(page) => {
                setHistoryPage(page);
                setExpandedVehicleId(null);
              }}
              page={historyPage}
              pageSize={FLEET_PAGE_SIZE}
              total={companyHistoryRecords.length}
            />
            {result.summary?.companyHistoryTruncated || result.summary?.historicalTruncated ? (
              <p className="text-sm leading-7 text-muted-foreground">
                Historický seznam byl zkrácen na prvních 200 vozidel.
              </p>
            ) : null}
          </CardContent>
          ) : null}
        </Card>
      ) : null}
	    </div>
	  );
}

function FleetPagination({ page, pageSize, total, onPageChange }) {
  const totalPages = Math.ceil(total / pageSize);
  if (totalPages <= 1) {
    return null;
  }

  const start = (page - 1) * pageSize + 1;
  const end = Math.min(total, page * pageSize);

  return (
    <div className="flex flex-col gap-3 rounded-[1rem] border border-white/8 bg-background/45 px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
      <p className="text-sm text-muted-foreground">
        Zobrazeno {start}-{end} z {total}
      </p>
      <div className="flex items-center gap-2">
        <Button
          className="h-9 px-3"
          disabled={page <= 1}
          onClick={() => onPageChange(Math.max(1, page - 1))}
          type="button"
          variant="outline"
        >
          Předchozí
        </Button>
        <span className="min-w-14 text-center text-sm font-semibold text-white">
          {page}/{totalPages}
        </span>
        <Button
          className="h-9 px-3"
          disabled={page >= totalPages}
          onClick={() => onPageChange(Math.min(totalPages, page + 1))}
          type="button"
          variant="outline"
        >
          Další
        </Button>
      </div>
    </div>
  );
}

function InspectionPanel({ inspections, inspectionLookup }) {
  if (!inspections) {
    const isPending = inspectionLookup?.status === "pending";

    return (
      <Card className="border-white/10 bg-card/78">
        <CardHeader className="pb-4">
          <div className="flex items-center justify-between gap-4">
            <div>
              <CardDescription className="uppercase tracking-[0.22em] text-muted-foreground">
                Kontroly
              </CardDescription>
              <CardTitle className="mt-2 text-2xl text-white sm:text-3xl">
                Technické prohlídky
              </CardTitle>
            </div>

            <Badge variant="muted">{isPending ? "Načítám" : "Nedostupné"}</Badge>
          </div>
        </CardHeader>

        <CardContent>
          <div className="rounded-[1.5rem] border border-white/10 bg-background/55 p-5">
            <div className="flex items-center gap-3">
              {isPending ? (
                <Loader2 className="size-5 animate-spin text-muted-foreground" />
              ) : (
                <AlertCircle className="size-5 text-muted-foreground" />
              )}
              <p className="text-sm leading-7 text-muted-foreground">
                {isPending
                  ? "Základní data vozidla už jsou připravená. Technické prohlídky se dočtou na pozadí a objeví se tady automaticky."
	                  : formatFrontendText(inspectionLookup?.message || "Detailní záznamy technických prohlídek nejsou pro tento dotaz dostupné.")}
              </p>
            </div>

            {inspectionLookup?.vin ? (
              <div className="mt-4 flex flex-wrap gap-2">
                {inspectionLookup?.vin ? <Badge variant="muted">VIN {inspectionLookup.vin}</Badge> : null}
              </div>
            ) : null}
          </div>
        </CardContent>
      </Card>
    );
  }

  const records = Array.isArray(inspections.records) ? inspections.records : [];
  const summary = inspections.summary || {};
  const current = getLatestInspectionRecord(records, summary);
  const status = summary.status || "Nezjištěno";
  const tone = inspectionTone(status);
  const daysRemaining =
    typeof summary.daysRemaining === "number" ? formatDaysRemaining(summary.daysRemaining) : "Bez termínu";

  return (
    <Card className="border-white/10 bg-card/78">
      <CardHeader className="pb-4">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <CardDescription className="uppercase tracking-[0.22em] text-muted-foreground">
              Kontroly
            </CardDescription>
            <CardTitle className="mt-2 text-2xl text-white sm:text-3xl">
              Technické prohlídky
            </CardTitle>
          </div>

          <div className="flex flex-wrap gap-2">
            <Badge variant={tone}>{status}</Badge>
            {inspections.sourceUpdatedAt ? (
              <Badge variant="muted">
                Dataset {formatDate(inspections.sourceUpdatedAt)}
              </Badge>
            ) : null}
          </div>
        </div>
      </CardHeader>

      <CardContent className="space-y-5">
        <div className="grid gap-3 lg:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)]">
          <div className="rounded-[1.5rem] border border-white/10 bg-background/55 p-5">
            <div className="flex items-start justify-between gap-4">
              <div>
                <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">
                  Aktuální stav
                </p>
                <p className="mt-3 text-2xl font-semibold text-white sm:text-3xl">{status}</p>
              </div>
              {tone === "success" ? (
                <CheckCircle2 className="size-6 text-emerald-300" />
              ) : (
                <AlertTriangle className="size-6 text-amber-200" />
              )}
            </div>

            <div className="mt-5 grid gap-3 sm:grid-cols-2">
              <QuickMeta
                icon={CalendarRange}
                label="Platnost do"
                value={current?.validUntil ? formatDate(current.validUntil) : "Neuvedeno"}
              />
              <QuickMeta
                icon={Clock3}
                label="Rezerva"
                value={daysRemaining}
              />
              <QuickMeta
                icon={FileText}
                label="Typ kontroly"
                value={current?.type || "Neuvedeno"}
              />
              <QuickMeta
                icon={Gauge}
                label="Kilometry pri STK"
                value={formatOdometer(current) || "Neuvedeno"}
              />
              <QuickMeta
                icon={MapPin}
                label="Stanice"
                value={formatInspectionStationLabel(current) || "Neuvedeno"}
              />
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-1">
            <MiniStat label="Záznamy" value={summary.totalCount ?? records.length} />
            <MiniStat label="Aktuální" value={summary.currentCount ?? 0} />
          </div>
        </div>

        <InspectionRecordsTable records={records} />
        <InspectionMileagePanel records={records} />
      </CardContent>
    </Card>
  );
}

function InspectionRecordsTable({ records }) {
  const [expandedRecordKey, setExpandedRecordKey] = useState("");
  const visibleRecords = (Array.isArray(records) ? records : []).slice(0, 12);

  if (!visibleRecords.length) {
    return <EmptyPanel text="Detailní záznamy STK nejsou pro tento dotaz dostupné." />;
  }

  return (
    <div className="overflow-hidden rounded-[1.1rem] border border-white/10 bg-background/55">
      <div className="hidden grid-cols-[150px_minmax(0,1fr)_140px_170px_130px_120px] gap-3 border-b border-white/10 bg-white/[0.04] px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-muted-foreground lg:grid">
        <span>Datum prohlídky</span>
        <span>Druh prohlídky</span>
        <span>Stav km</span>
        <span>Příští prohlídka</span>
        <span>Výsledek</span>
        <span className="text-right">Detail</span>
      </div>

      <div className="divide-y divide-white/10">
        {visibleRecords.map((record, index) => {
          const recordKey = getInspectionRecordKey(record, index);
          const expanded = expandedRecordKey === recordKey;

          return (
            <InspectionTableRow
              expanded={expanded}
              key={recordKey}
              onToggle={() => setExpandedRecordKey((current) => (current === recordKey ? "" : recordKey))}
              record={record}
            />
          );
        })}
      </div>
    </div>
  );
}

function InspectionTableRow({ record, expanded, onToggle }) {
  const performedOn = getInspectionPerformedDate(record);
  const odometer = formatOdometer(record);
  const result = formatInspectionResult(record);

  return (
    <div>
      <div className="grid gap-3 px-4 py-4 lg:grid-cols-[150px_minmax(0,1fr)_140px_170px_130px_120px] lg:items-center">
        <InspectionCell label="Datum prohlídky" value={performedOn ? formatDate(performedOn) : "Neuvedeno"} />
        <InspectionCell label="Druh prohlídky" value={formatInspectionType(record.type || record.rawInspectionType)} />
        <InspectionCell label="Stav km" value={odometer || "Neuvedeno"} />
        <InspectionCell label="Příští prohlídka" value={formatInspectionNext(record)} />
        <div>
          <p className="text-[11px] font-semibold uppercase text-muted-foreground lg:hidden">Výsledek</p>
          <Badge className="mt-1 w-fit lg:mt-0" variant={inspectionResultVariant(record)}>
            {result}
          </Badge>
        </div>
        <div className="lg:text-right">
          <Button className="h-9 px-3" onClick={onToggle} type="button" variant={expanded ? "secondary" : "outline"}>
            Detail
            <ChevronDown className={cn("ml-2 size-4 transition-transform", expanded ? "rotate-180" : "")} />
          </Button>
        </div>
      </div>

      {expanded ? <InspectionDetailPanel record={record} /> : null}
    </div>
  );
}

function InspectionCell({ label, value }) {
  return (
    <div className="min-w-0">
      <p className="text-[11px] font-semibold uppercase text-muted-foreground lg:hidden">{label}</p>
      <p className="mt-1 break-words text-sm font-semibold text-white lg:mt-0">{formatFrontendText(value)}</p>
    </div>
  );
}

function InspectionDetailPanel({ record }) {
  const details = [
    ["VIN vozidla", record.vin],
    ["Datum prohlídky", formatInspectionPerformedDateTime(record)],
    ["Číslo protokolu", record.protocolNumber],
    ["Stanice", formatInspectionStation(record)],
    ["Druh prohlídky", formatInspectionType(record.type || record.rawInspectionType)],
    ["Stav tachometru", formatOdometer(record)],
    ["Příští prohlídka", formatInspectionNext(record)],
    ["Výsledek prohlídky", formatInspectionResult(record)],
    ["Identifikátor záznamu", record.sourceId && record.sourceId !== record.protocolNumber ? record.sourceId : null]
  ].filter(([, value]) => value);

  return (
    <div className="border-t border-white/10 bg-black/20 px-4 py-4">
      <div className="grid gap-2 md:grid-cols-2">
        {details.map(([label, value]) => (
          <div className="grid gap-1 rounded-[0.9rem] border border-white/8 bg-background/50 px-3 py-3 sm:grid-cols-[150px_minmax(0,1fr)] sm:gap-3" key={label}>
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">{label}</p>
            <p className="break-words text-sm font-semibold text-white">{formatFrontendText(value)}</p>
          </div>
        ))}
      </div>

      <InspectionDefectsPanel defects={record.defects} />
    </div>
  );
}

function InspectionDefectsPanel({ defects }) {
  const items = normalizeInspectionDefectsForUi(defects);

  return (
    <div className="mt-3 rounded-[0.9rem] border border-white/8 bg-background/50 px-3 py-3">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Závady ve výsledku</p>
        <Badge className="w-fit px-2 py-0.5 text-[10px]" variant={items.length ? "muted" : "success"}>
          {items.length ? `${items.length} ${formatRecordCountLabel(items.length)}` : "Neuvedeno"}
        </Badge>
      </div>

      {items.length ? (
        <div className="mt-3 grid gap-2">
          {items.map((defect, index) => (
            <div
              className={cn(
                "grid gap-2 rounded-[0.75rem] border px-3 py-3 sm:grid-cols-[130px_56px_minmax(0,1fr)] sm:items-start",
                inspectionDefectRowClassName(defect)
              )}
              key={`${defect.code || defect.description || "defect"}-${index}`}
            >
              <p className="break-words font-mono text-sm font-semibold text-white">{defect.code || "Bez kódu"}</p>
              <InspectionDefectSeverityBadge severity={defect.severity} />
              <p className="text-sm font-semibold leading-6 text-white/90">
                {formatFrontendText(defect.description || "Popis závady není uvedený.")}
              </p>
            </div>
          ))}
        </div>
      ) : (
        <p className="mt-3 text-sm leading-6 text-muted-foreground">V tomto záznamu nejsou závady uvedené.</p>
      )}
    </div>
  );
}

function InspectionDefectSeverityBadge({ severity }) {
  const label = normalizeDefectText(severity).toUpperCase() || "-";
  const info = getInspectionDefectSeverityInfo(label);

  return (
    <span
      aria-label={info.ariaLabel}
      className="group relative inline-flex w-fit focus:outline-none"
      tabIndex={0}
      title={info.tooltip}
    >
      <Badge className="w-fit px-2 py-0.5 text-[10px]" variant={inspectionDefectSeverityVariant({ severity: label })}>
        {label}
      </Badge>
      <span
        aria-hidden="true"
        className="pointer-events-none absolute left-0 top-full z-20 mt-2 hidden w-64 rounded-[0.75rem] border border-white/10 bg-black/95 px-3 py-2 text-left text-xs font-semibold leading-5 text-white shadow-xl shadow-black/40 group-hover:block group-focus:block"
      >
        {info.tooltip}
      </span>
    </span>
  );
}

function InspectionMileagePanel({ records }) {
  const rows = buildInspectionMileageRows(records);
  const mileageEntries = rows
    .filter((row) => row.odometerValue !== null)
    .slice()
    .sort((left, right) => left.timestamp - right.timestamp || left.index - right.index);
  const latestMileage = mileageEntries[mileageEntries.length - 1] || null;
  const firstMileage = mileageEntries[0] || null;
  const mileageDelta =
    latestMileage && firstMileage && mileageEntries.length > 1
      ? latestMileage.odometerValue - firstMileage.odometerValue
      : null;

  return (
    <div className="rounded-[1.5rem] border border-white/10 bg-background/55 p-5">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-sm font-semibold text-white">
            <LineChart className="size-4 text-primary" />
            <span>Nájezd podle STK</span>
          </div>
          <p className="mt-2 max-w-3xl text-sm leading-6 text-muted-foreground">
            Přehled ukazuje datum technické kontroly a stav tachometru, pokud jej zdroj pro konkrétní záznam poskytl.
          </p>
        </div>

        <Badge variant={mileageEntries.length ? "success" : "muted"} className="w-fit">
          {mileageEntries.length ? `${mileageEntries.length} záznamů s km` : "Bez km ve zdroji"}
        </Badge>
      </div>

      <div className="mt-5 grid gap-3 sm:grid-cols-3">
        <MiniStat
          label="Poslední STK"
          value={rows[0]?.date ? formatDate(rows[0].date) : "Neuvedeno"}
        />
        <MiniStat
          label="Poslední km"
          value={latestMileage ? formatMileageValue(latestMileage.odometerValue, latestMileage.record) : "Nedostupné"}
        />
        <MiniStat
          label="Rozdíl v historii"
          value={mileageDelta === null ? "Nedostupné" : formatMileageDelta(mileageDelta)}
        />
      </div>

      <div className="mt-5">
        <InspectionMileageChart entries={mileageEntries} />
      </div>

      {!mileageEntries.length ? (
        <div className="mt-4 rounded-[1.1rem] border border-amber-300/20 bg-amber-300/[0.08] p-4">
          <div className="flex gap-3">
            <AlertCircle className="mt-0.5 size-5 shrink-0 text-amber-200" />
            <p className="text-sm leading-6 text-amber-50/90">
              Aktuální open-data soubor STK neobsahuje sloupec se stavem tachometru. Jakmile import nebo databáze dodá
              pole <span className="font-semibold text-white">odometer</span>, tabulka a graf se vyplní automaticky.
            </p>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function InspectionMileageChart({ entries }) {
  if (!entries.length) {
    return (
      <div className="flex min-h-[220px] items-center justify-center rounded-[1.1rem] border border-dashed border-white/10 bg-black/20 px-5 text-center">
        <div>
          <LineChart className="mx-auto size-8 text-muted-foreground" />
          <p className="mt-3 text-sm font-semibold text-white">Graf nájezdu zatím nelze vykreslit</p>
          <p className="mt-2 max-w-md text-sm leading-6 text-muted-foreground">
            V dostupných záznamech STK nejsou kilometry. Datumy kontrol jsou uvedené v tabulce níže.
          </p>
        </div>
      </div>
    );
  }

  const width = 720;
  const height = 240;
  const padding = { top: 24, right: 26, bottom: 46, left: 64 };
  const times = entries.map((entry) => entry.timestamp);
  const values = entries.map((entry) => entry.odometerValue);
  const minTime = Math.min(...times);
  const maxTime = Math.max(...times);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const valueRange = Math.max(1, maxValue - minValue);
  const timeRange = Math.max(1, maxTime - minTime);
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const points = entries.map((entry, index) => {
    const x =
      entries.length === 1
        ? padding.left + plotWidth / 2
        : padding.left + ((entry.timestamp - minTime) / timeRange) * plotWidth;
    const y =
      entries.length === 1
        ? padding.top + plotHeight / 2
        : padding.top + plotHeight - ((entry.odometerValue - minValue) / valueRange) * plotHeight;

    return { ...entry, x, y, index };
  });
  const linePoints = points.map((point) => `${point.x},${point.y}`).join(" ");
  const areaPath = [
    `M ${points[0].x} ${padding.top + plotHeight}`,
    ...points.map((point) => `L ${point.x} ${point.y}`),
    `L ${points[points.length - 1].x} ${padding.top + plotHeight}`,
    "Z"
  ].join(" ");
  const gridValues = [maxValue, Math.round(minValue + valueRange / 2), minValue];

  return (
    <div className="rounded-[1.1rem] border border-white/10 bg-black/20 p-3">
      <svg viewBox={`0 0 ${width} ${height}`} className="h-[240px] w-full overflow-visible" role="img" aria-label="Graf stavu tachometru podle STK">
        <defs>
          <linearGradient id="inspectionMileageFill" x1="0" x2="0" y1="0" y2="1">
            <stop offset="0%" stopColor="rgb(125 211 252)" stopOpacity="0.28" />
            <stop offset="100%" stopColor="rgb(125 211 252)" stopOpacity="0.02" />
          </linearGradient>
        </defs>

        {gridValues.map((value, index) => {
          const y = padding.top + plotHeight - ((value - minValue) / valueRange) * plotHeight;
          return (
            <g key={`${value}-${index}`}>
              <line x1={padding.left} x2={width - padding.right} y1={y} y2={y} stroke="rgba(255,255,255,0.08)" />
              <text x={padding.left - 12} y={y + 4} textAnchor="end" className="fill-slate-400 text-[11px]">
                {formatCompactKm(value)}
              </text>
            </g>
          );
        })}

        <path d={areaPath} fill="url(#inspectionMileageFill)" />
        <polyline points={linePoints} fill="none" stroke="rgb(125 211 252)" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round" />

        {points.map((point) => (
          <g key={`${point.date}-${point.odometerValue}-${point.index}`}>
            <circle cx={point.x} cy={point.y} r="5" fill="rgb(14 165 233)" stroke="rgb(240 249 255)" strokeWidth="2" />
            <title>{`${formatDate(point.date)}: ${formatMileageValue(point.odometerValue, point.record)}`}</title>
          </g>
        ))}

        <text x={padding.left} y={height - 14} className="fill-slate-400 text-[11px]">
          {formatDate(entries[0].date)}
        </text>
        <text x={width - padding.right} y={height - 14} textAnchor="end" className="fill-slate-400 text-[11px]">
          {formatDate(entries[entries.length - 1].date)}
        </text>
      </svg>
    </div>
  );
}

function InspectionRow({ record }) {
  const odometer = formatOdometer(record);

  return (
    <div className="grid gap-3 rounded-[1.1rem] border border-white/8 bg-background/50 px-4 py-3 md:grid-cols-[140px_minmax(0,1fr)_150px_auto] md:items-center">
      <div className="space-y-1">
        <Badge variant={record.current ? "success" : "muted"} className="w-fit">
          {record.current ? "Aktuální" : "Historie"}
        </Badge>
        <p className="text-sm font-medium text-muted-foreground">
          {record.validFrom ? formatDate(record.validFrom) : "Neuvedeno"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-sm font-semibold text-white">
          {[record.type, record.state].filter(Boolean).join(" · ") || "Technická prohlídka"}
        </p>
        <p className="mt-1 text-sm leading-6 text-muted-foreground">
          {[record.stationName, record.protocolNumber ? `protokol ${record.protocolNumber}` : null]
            .filter(Boolean)
            .join(" · ") || "Bez doplňujících detailů."}
        </p>
      </div>

      <div className="text-left md:text-right">
        <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">
          Kilometry
        </p>
        <p className="mt-1 text-sm font-semibold text-white">
          {odometer || "Neuvedeno"}
        </p>
      </div>

      <div className="text-left md:text-right">
        <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">
          Platnost do
        </p>
        <p className="mt-1 text-sm font-semibold text-white">
          {record.validUntil ? formatDate(record.validUntil) : "Neuvedeno"}
        </p>
      </div>
    </div>
  );
}

function HighlightCard({ item }) {
  return (
    <div className="rounded-[1.25rem] border border-white/10 bg-background/55 px-4 py-4">
      <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">
        {formatFrontendText(item.label)}
      </p>
      <p className="mt-2 text-base font-semibold text-white">{formatFrontendText(item.value)}</p>
    </div>
  );
}

function IdentifierCard({ label, value, href, onClick }) {
  const displayValue = formatFrontendText(value || "-");
  const normalizedLabel = normalizeLabel(label);
  const isVin = normalizedLabel === "VIN";
  const interactive = Boolean(href && onClick && value && value !== "-");
  const ariaLabel = `Vyhledat ${formatFrontendText(label || "identifikátor")} ${value}`;
  const content = (
    <>
      <p className="text-[11px] font-semibold uppercase tracking-[0.3em] text-muted-foreground">
        {formatFrontendText(label)}
      </p>
      <p className={cn(
        "mt-2 flex min-w-0 items-center gap-2 text-base font-semibold tracking-[0.02em] text-white",
        isVin ? "font-mono text-[0.95rem] sm:text-base" : "break-all"
      )}>
        <span className={cn("min-w-0", isVin ? "whitespace-nowrap" : "break-all")}>{displayValue}</span>
        {interactive ? <ArrowUpRight className="size-3.5 shrink-0 text-primary" /> : null}
      </p>
    </>
  );

  if (interactive) {
    return (
      <a
        aria-label={ariaLabel}
        className="block rounded-[1.25rem] border border-white/10 bg-background/55 px-4 py-4 text-left transition-colors hover:border-primary/45 hover:bg-primary/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background"
        href={href}
        onClick={(event) => {
          if (event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) {
            return;
          }

          event.preventDefault();
          onClick(value);
        }}
      >
        {content}
      </a>
    );
  }

  return (
    <div className="rounded-[1.25rem] border border-white/10 bg-background/55 px-4 py-4">
      {content}
    </div>
  );
}

function InlineMeta({ icon: Icon, value, tone = "default" }) {
  const accent = tone === "accent";

  return (
    <div className={cn(
      "inline-flex items-center gap-2 rounded-full border px-3 py-2",
      accent ? "border-primary/30 bg-primary/10" : "border-white/10 bg-background/45"
    )}>
      <Icon className={cn("size-3.5", accent ? "text-primary" : "text-muted-foreground")} />
      <span className={cn("text-sm", accent ? "font-medium text-white" : "text-muted-foreground")}>
        {formatFrontendText(value)}
      </span>
    </div>
  );
}

function IcoLookupLink({ ico, emptyText = "IČO není ve zdroji" }) {
  const normalizedIco = normalizeSharedLookupValue("ico", ico);
  const aresUrl = buildAresUrl(normalizedIco);

  if (!aresUrl) {
    return <span>{emptyText}</span>;
  }

  return (
    <a
      className="inline-flex break-all font-medium text-muted-foreground transition-colors hover:text-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background"
      href={aresUrl}
      rel="noreferrer"
      target="_blank"
    >
      IČO {normalizedIco}
      <ArrowUpRight className="ml-1 size-3.5 shrink-0" />
    </a>
  );
}

function CompactPartyCard({ party }) {
  const aresUrl = party.type === "company" && party.ico ? buildAresUrl(party.ico) : null;

  return (
    <div className="rounded-[1.1rem] border border-white/8 bg-background/55 px-4 py-3">
      <div className="flex items-center gap-2">
        <Badge variant="muted">{party.role || "Subjekt"}</Badge>
        {party.type === "company" ? (
          <Building2 className="size-4 text-muted-foreground" />
        ) : (
          <Fingerprint className="size-4 text-muted-foreground" />
        )}
      </div>

      {aresUrl ? (
        <a
          href={aresUrl}
          target="_blank"
          rel="noreferrer"
          className="mt-3 inline-flex items-center gap-2 text-sm font-semibold text-white transition-colors hover:text-primary"
        >
          {party.name || party.ico}
          <ArrowUpRight className="size-3.5" />
        </a>
      ) : (
        <p className="mt-3 text-sm font-semibold text-white">{party.name || "Bez názvu"}</p>
      )}
      <p className="mt-1 text-sm leading-6 text-muted-foreground">{formatFrontendText(compactPartyMeta(party))}</p>
    </div>
  );
}

function CompanyVehicleRow({ record }) {
  const title = [record.make, record.model, record.type].filter(Boolean).join(" ").trim() || record.vin || "Vozidlo";

  return (
    <div className="grid gap-4 rounded-[1.1rem] border border-white/8 bg-background/50 px-4 py-4 xl:grid-cols-[minmax(0,1fr)_260px]">
      <div className="min-w-0">
        <div className="flex flex-wrap gap-2">
          {(record.relations || []).map((relation, index) => (
            <Badge key={`${relation.relation}-${index}`} variant={relation.current ? "success" : "muted"}>
              {relation.relation || "Subjekt"}{relation.current ? " · aktuální" : ""}
            </Badge>
          ))}
        </div>

        <p className="mt-3 text-lg font-semibold text-white">{title}</p>
        <p className="mt-2 text-sm leading-7 text-muted-foreground">
          {[
            record.category || null,
            record.fuel || null,
            record.firstRegistration ? `1. registrace ${formatDate(record.firstRegistration)}` : null,
            record.status || null,
            !record.firstRegistration && record.firstSeen ? `vztah od ${formatDate(record.firstSeen)}` : null,
            !record.status && record.lastSeen ? `do ${formatDate(record.lastSeen)}` : null
          ]
            .filter(Boolean)
            .join(" · ") || "Bez dalších technických detailů."}
        </p>
      </div>

	      <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-1">
	        <IdentifierCard label="VIN" value={record.vin || "-"} />
	        <IdentifierCard label="STK provedena" value={formatFleetInspectionValue(record, "performed")} />
	        <IdentifierCard label="STK platná do" value={formatFleetInspectionValue(record, "validUntil")} />
	      </div>
    </div>
  );
}

function CompanyFleetVehicleRow({ record, recordId, companyIco, expanded, onToggle, onVehicleVinLookup, historical = false }) {
  const title = [record.make, record.model, record.type].filter(Boolean).join(" ").trim() || record.vin || "Vozidlo";
  const [historyPayload, setHistoryPayload] = useState(null);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState("");
  const relationRecord = historyPayload?.relations?.length
    ? { ...record, relations: historyPayload.relations }
    : record;
  const { currentRelations, historicalRelations } = splitFleetRelations(relationRecord);
  const badgeRelations = currentRelations.length > 0 ? currentRelations : historical ? historicalRelations : [];
  const safeId = sanitizeDomId(recordId);
  const panelId = `fleet-history-${safeId}`;
  const titleId = `fleet-title-${safeId}`;
  const toggleLabelId = `fleet-toggle-${safeId}`;

  useEffect(() => {
    if (!expanded || historyPayload || historyLoading || !companyIco || !record.pcv) {
      return undefined;
    }

    let cancelled = false;
    const params = new URLSearchParams({
      ico: companyIco,
      pcv: record.pcv
    });

    setHistoryLoading(true);
    setHistoryError("");

    fetch(`/api/company-fleet/history?${params.toString()}`, {
      headers: { Accept: "application/json" }
    })
      .then(async (response) => {
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.message || "Historii se nepodařilo načíst.");
        }
        if (!cancelled) {
          setHistoryPayload(payload);
        }
      })
      .catch((error) => {
        if (!cancelled) {
          setHistoryError(error.message || "Historii se nepodařilo načíst.");
        }
      })
      .finally(() => {
        if (!cancelled) {
          setHistoryLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [companyIco, expanded, record.pcv]);

  return (
    <article className="rounded-[1.1rem] border border-white/8 bg-background/50 px-4 py-4">
      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_260px_auto] xl:items-start">
        <div className="min-w-0">
	          <div className="flex flex-wrap gap-2">
	            {badgeRelations.length > 0 ? (
	              badgeRelations.map((relation) => (
	                <Badge key={getFleetRelationKey(relation)} variant={relation.current ? "success" : "muted"}>
	                  {formatFleetRelationBadge(relation)}
	                </Badge>
	              ))
	            ) : (
	              <Badge variant="muted">{historical ? "Historická vazba" : "Aktuální vztah neuveden"}</Badge>
	            )}
	          </div>

          <p className="mt-3 break-words text-lg font-semibold text-white" id={titleId}>{title}</p>
          <p className="mt-2 text-sm leading-7 text-muted-foreground">
            {[
              record.category || null,
              record.fuel || null,
              record.firstRegistration ? `1. registrace ${formatDate(record.firstRegistration)}` : null,
              record.status || null,
              !record.firstRegistration && record.firstSeen ? `vztah od ${formatDate(record.firstSeen)}` : null,
              !record.status && record.lastSeen ? `do ${formatDate(record.lastSeen)}` : null
            ]
              .filter(Boolean)
              .join(" · ") || "Bez dalších technických detailů."}
          </p>
        </div>

        <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-1">
          <IdentifierCard
            href={record.vin ? buildSharePath("vin", record.vin) : null}
            label="VIN"
            onClick={onVehicleVinLookup}
            value={record.vin || "-"}
          />
	          <IdentifierCard label="STK provedena" value={formatFleetInspectionValue(record, "performed")} />
	          <IdentifierCard label="STK platná do" value={formatFleetInspectionValue(record, "validUntil")} />
	        </div>

        <button
          aria-controls={panelId}
          aria-expanded={expanded}
          aria-labelledby={`${toggleLabelId} ${titleId}`}
          className="inline-flex h-11 items-center justify-center gap-2 rounded-full border border-white/10 bg-white/[0.04] px-4 text-sm font-semibold text-white transition-colors hover:border-white/20 hover:bg-white/[0.08] xl:w-11 xl:px-0"
          onClick={onToggle}
          type="button"
        >
          <ChevronDown className={cn("size-4 transition-transform", expanded ? "rotate-180" : "")} />
          <span className="xl:sr-only" id={toggleLabelId}>
            {expanded ? "Sbalit historii" : "Historie"}
          </span>
        </button>
      </div>

	      {expanded ? (
	        <div className="mt-4 min-w-0 border-t border-white/8 pt-4" id={panelId} role="region" aria-labelledby={titleId}>
	          {historyLoading ? (
	            <div className="mb-4 flex items-center gap-3 rounded-[1rem] border border-white/10 bg-background/55 px-4 py-3 text-sm text-muted-foreground">
	              <Loader2 className="size-4 animate-spin" />
	              <span>Načítám historii vztahu z lokální databáze.</span>
	            </div>
	          ) : null}
	          {historyError ? (
	            <div className="mb-4 rounded-[1rem] border border-amber-300/20 bg-amber-300/[0.08] px-4 py-3 text-sm text-amber-100">
	              {historyError}
	            </div>
	          ) : null}
	          <div className="grid min-w-0 gap-4 lg:grid-cols-2">
            <FleetRelationGroup
              description="Platné role podle poslední dostupné vazby."
              emptyText="Aktuální právnický subjekt není v detailu zdroje uveden."
              emphasis
              relations={currentRelations}
              title="Aktuální vztahy"
            />
            <FleetRelationGroup
              description="Dříve evidovaní vlastníci nebo provozovatelé."
              emptyText="Historické právnické vazby nejsou ve zdroji uvedené."
              relations={historicalRelations}
              title="Historie vztahu"
            />
          </div>
        </div>
      ) : null}
    </article>
  );
}

function FleetRelationGroup({ title, description, relations, emptyText, emphasis = false }) {
  const [page, setPage] = useState(1);
  const visibleRelations = paginateRecords(relations, page, FLEET_PAGE_SIZE);

  useEffect(() => {
    setPage((current) => clampPage(current, relations.length, FLEET_PAGE_SIZE));
  }, [relations.length]);

  return (
    <section className="min-w-0 space-y-3">
      <div className="flex flex-col gap-1 sm:flex-row sm:items-end sm:justify-between">
        <div className="min-w-0">
          <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">{title}</p>
          <p className="mt-1 text-sm leading-6 text-muted-foreground">{description}</p>
        </div>
        <Badge variant={emphasis ? "success" : "muted"}>{relations.length || 0} záznamů</Badge>
      </div>

      {relations.length > 0 ? (
        <div className="grid min-w-0 gap-2">
          {visibleRelations.map((relation) => (
            <FleetRelationCard
              current={emphasis}
              key={getFleetRelationKey(relation)}
              relation={relation}
            />
          ))}
          <FleetPagination
            onPageChange={setPage}
            page={page}
            pageSize={FLEET_PAGE_SIZE}
            total={relations.length}
          />
        </div>
      ) : (
        <EmptyPanel text={emptyText} />
      )}
    </section>
  );
}

function FleetRelationCard({ relation, current }) {
  const aresUrl = relation.ico ? buildAresUrl(relation.ico) : null;

  return (
    <article
      className={cn(
        "min-w-0 rounded-[1rem] border p-3",
        current ? "border-emerald-300/20 bg-emerald-300/[0.07]" : "border-white/10 bg-background/55"
      )}
    >
      <div className="flex min-w-0 items-start gap-3">
        <div className="rounded-full border border-white/10 bg-white/[0.06] p-2">
          <Building2 className="size-4 text-white" />
        </div>
        <div className="min-w-0">
          <div className="flex flex-wrap gap-2">
            <Badge variant={current ? "success" : "muted"}>{relation.relation || "Subjekt"}</Badge>
            <Badge variant="muted">{current ? "Aktuální" : "Historie"}</Badge>
            {relation.sourceOpenEnded ? <Badge variant="muted">Neukončeno ve zdroji</Badge> : null}
          </div>
          {aresUrl ? (
            <a
              className="mt-3 inline-flex min-w-0 items-center gap-2 break-words text-sm font-semibold leading-6 text-white transition-colors hover:text-primary"
              href={aresUrl}
              rel="noreferrer"
              target="_blank"
            >
              {relation.name || relation.ico}
              <ArrowUpRight className="size-3.5 shrink-0" />
            </a>
          ) : (
            <p className="mt-3 break-words text-sm font-semibold leading-6 text-white">{relation.name || "Právnická osoba"}</p>
          )}
        </div>
      </div>

      <div className="mt-3 grid min-w-0 gap-2 text-sm leading-6 text-muted-foreground sm:grid-cols-2">
        <IcoLookupLink ico={relation.ico} />
        <span>{formatFleetRelationPeriod(relation)}</span>
        {relation.address ? <span className="break-words sm:col-span-2">{relation.address}</span> : null}
      </div>
    </article>
  );
}

function getFleetVehicleId(record, index = 0) {
  return [record.id, record.pcv, record.vin, getFleetRelationKey((record.relations || [])[0]), record.title, record.make, record.model, record.type]
    .filter(Boolean)
    .join("|") || `vehicle-${index}`;
}

function formatFleetInspectionValue(record, field) {
  const inspection = record?.inspection || null;
  if (!inspection) {
    return "Nedostupná";
  }

  const value = field === "validUntil"
    ? inspection.validUntil
    : inspection.performedOn || inspection.validFrom;

  return value ? formatDate(value) : "Nedostupná";
}

function paginateRecords(records, page, pageSize) {
  const safePage = clampPage(page, records.length, pageSize);
  const start = (safePage - 1) * pageSize;
  return records.slice(start, start + pageSize);
}

function clampPage(page, total, pageSize) {
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  return Math.min(Math.max(Number(page) || 1, 1), totalPages);
}

function splitFleetRelations(record) {
  const sourceRelations = getDisplayableFleetRelations(record);
  const latestOpenByRole = new Map();

  sourceRelations.forEach((relation, index) => {
    if (!isCurrentFleetRelation(relation)) {
      return;
    }

    const roleKey = normalizeOwnershipRoleKey(relation.relation);
    const sinceTime = parseOwnershipDate(relation.dateFrom);
    const previous = latestOpenByRole.get(roleKey);
    if (!previous || sinceTime > previous.sinceTime || (sinceTime === previous.sinceTime && index > previous.index)) {
      latestOpenByRole.set(roleKey, { index, sinceTime });
    }
  });

  const groups = sourceRelations.reduce(
    (result, relation, index) => {
      const roleKey = normalizeOwnershipRoleKey(relation.relation);
      const selected = latestOpenByRole.get(roleKey);
      const isSourceOpenEnded = isCurrentFleetRelation(relation);
      const isEffectiveCurrent = isSourceOpenEnded && selected?.index === index;
      const normalizedRelation = {
        ...relation,
        current: isEffectiveCurrent,
        sourceOpenEnded: isSourceOpenEnded && !isEffectiveCurrent
      };

      if (isEffectiveCurrent) {
        result.currentRelations.push(normalizedRelation);
      } else {
        result.historicalRelations.push(normalizedRelation);
      }

      return result;
    },
    { currentRelations: [], historicalRelations: [] }
  );

  groups.currentRelations.sort(compareFleetRelationsForUi);
  groups.historicalRelations.sort(compareFleetRelationsForUi);
  return groups;
}

function getDisplayableFleetRelations(record) {
  const relations = Array.isArray(record.relations) ? record.relations : [];
  return relations.filter((relation) => isFleetOwnershipRelation(relation) && isLegalEntityParty(relation));
}

function isFleetOwnershipRelation(relation) {
  const roleKey = normalizeOwnershipRoleKey(relation?.relation);
  return roleKey === "owner" || roleKey === "operator";
}

function isCurrentFleetRelation(relation) {
  return relation?.current === true && !relation?.dateTo;
}

function compareFleetRelationsForUi(left, right) {
  const roleDifference = getFleetRolePriorityForUi(left.relation) - getFleetRolePriorityForUi(right.relation);
  if (roleDifference !== 0) {
    return roleDifference;
  }

  if (Boolean(left.current) !== Boolean(right.current)) {
    return left.current ? -1 : 1;
  }

  const dateDifference = parseOwnershipDate(right.dateFrom || right.dateTo) - parseOwnershipDate(left.dateFrom || left.dateTo);
  if (dateDifference !== 0) {
    return dateDifference;
  }

  return [
    String(left.ico || "").localeCompare(String(right.ico || ""), "cs"),
    String(left.name || "").localeCompare(String(right.name || ""), "cs"),
    String(left.address || "").localeCompare(String(right.address || ""), "cs")
  ].find((value) => value !== 0) || 0;
}

function getFleetRolePriorityForUi(role) {
  const roleKey = normalizeOwnershipRoleKey(role);
  if (roleKey === "owner") {
    return 0;
  }
  if (roleKey === "operator") {
    return 1;
  }
  return 2;
}

function getFleetRelationKey(relation) {
  if (!relation) {
    return "relation";
  }

  return [
    relation.pcv,
    relation.ico,
    relation.relation,
    relation.name,
    relation.dateFrom || "from",
    relation.dateTo || "open"
  ]
    .filter(Boolean)
    .join("|");
}

function formatFleetRelationPeriod(relation) {
  const from = relation.dateFrom ? formatDate(relation.dateFrom) : null;
  const to = relation.dateTo ? formatDate(relation.dateTo) : relation.current ? "-" : null;
  const period = [from, to].filter(Boolean).join(" - ");
  return period || "Období neuvedeno";
}

function formatFleetRelationBadge(relation) {
  const label = relation.relation || "Subjekt";
  if (relation.current) {
    return `${label} · aktuální`;
  }
  if (relation.dateTo) {
    return `${label} · do ${formatDate(relation.dateTo)}`;
  }
  return `${label} · historie`;
}

function sanitizeDomId(value) {
  return String(value || "vehicle")
    .replace(/[^A-Za-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "") || "vehicle";
}

function extractIdentifierHighlights(highlights) {
  const items = Array.isArray(highlights) ? highlights : [];
  const preferred = ["SPZ", "VIN"]
    .map((label) => items.find((item) => normalizeLabel(item.label) === label))
    .filter(Boolean);

  return preferred.length > 0 ? preferred : items.slice(0, 2);
}

function buildSummaryHighlights(result) {
  const highlights = Array.isArray(result?.highlights)
    ? result.highlights.filter((item) => !isHiddenSummaryHighlight(item.label))
    : [];
  const seen = new Set(highlights.map((item) => normalizeForMatch(item?.label)).filter(Boolean));
  const extraLabels = [
    "První registrace v ČR",
    "Typ",
    "Varianta",
    "Kategorie",
    "Zdvihový objem",
    "Barva",
    "Převodovka",
    "Počet míst",
    "Emise platné do",
    "Odcizení",
    "Zástavní právo"
  ];
  const extras = extraLabels
    .map((label) => findVehicleSectionItem(result, label))
    .filter((item) => {
      const key = normalizeForMatch(item?.label);
      if (!key || seen.has(key)) {
        return false;
      }
      seen.add(key);
      return true;
    });

  return [...highlights, ...extras];
}

function findVehicleSectionItem(result, label) {
  const labelKey = normalizeForMatch(label);
  for (const section of Array.isArray(result?.sections) ? result.sections : []) {
    for (const item of Array.isArray(section?.items) ? section.items : []) {
      if (normalizeForMatch(item?.label) === labelKey && normalizeWhitespace(item?.value)) {
        return item;
      }
    }
  }

  return null;
}

function isHiddenSummaryHighlight(label) {
  return ["spz", "vin", "pcv"].includes(normalizeForMatch(label));
}

function getVehicleDimensions(result) {
  const items = [
    ...(Array.isArray(result?.highlights) ? result.highlights : []),
    ...(Array.isArray(result?.sections)
      ? result.sections.flatMap((section) => (Array.isArray(section?.items) ? section.items : []))
      : [])
  ];
  const overall = splitVehicleDimensions(findVehicleItemValue(items, ["Celková délka/šířka/výška", "Celková délka/šířka/výška [mm]"]));

  const dimensions = {
    length: formatVehicleMeasurement(findVehicleItemValue(items, ["Délka", "Celková délka"]) || overall[0], "mm"),
    width: formatVehicleMeasurement(findVehicleItemValue(items, ["Šířka", "Celková šířka"]) || overall[1], "mm"),
    height: formatVehicleMeasurement(findVehicleItemValue(items, ["Výška", "Celková výška"]) || overall[2], "mm"),
    wheelbase: formatVehicleMeasurement(findVehicleItemValue(items, ["Rozvor"]), "mm"),
    weight: formatVehicleMeasurement(findVehicleItemValue(items, ["Provozní hmotnost", "Hmotnost"]), "kg")
  };

  return Object.values(dimensions).some(Boolean) ? dimensions : null;
}

function findVehicleItemValue(items, labels) {
  for (const label of labels) {
    const normalizedLabel = normalizeForMatch(label);
    const match = items.find((item) => normalizeForMatch(item?.label) === normalizedLabel);
    const value = normalizeWhitespace(match?.value);
    if (value) {
      return value;
    }
  }

  return "";
}

function splitVehicleDimensions(value) {
  return normalizeWhitespace(value)
    .replace(/[x×]/gi, "/")
    .split("/")
    .map((part) => normalizeWhitespace(part).replace(/\bmm\b/gi, ""))
    .filter(Boolean);
}

function formatVehicleMeasurement(value, unit) {
  const text = normalizeWhitespace(value);
  if (!text) {
    return "";
  }

  if (new RegExp(`\\b${unit}\\b`, "i").test(text)) {
    return text;
  }

  const compact = text.replace(/\s+/g, "").replace(",", ".");
  const number = Number(compact);
  return Number.isFinite(number) ? `${new Intl.NumberFormat("cs-CZ").format(number)} ${unit}` : `${text} ${unit}`;
}

function formatSubjectHistoryMeta(count) {
  const normalizedCount = Number(count) || 0;
  if (normalizedCount <= 0) {
    return "";
  }

  const suffix = normalizedCount === 1
    ? "záznam v historii subjektu"
    : normalizedCount >= 2 && normalizedCount <= 4
      ? "záznamy v historii subjektu"
      : "záznamů v historii subjektu";

  return `${normalizedCount} ${suffix}`;
}

function getInspectionOverview(inspections, inspectionLookup) {
  if (!inspections) {
    const pending = inspectionLookup?.status === "pending";
    return {
      kicker: "Technicka kontrola",
      title: "STK",
      status: pending ? "Nacitam" : "Nezjisteno",
      tone: pending ? "muted" : "warning",
      icon: pending ? Loader2 : AlertCircle,
      loading: pending,
      description: pending
        ? "Technicke prohlidky se docitaji na pozadi."
        : formatFrontendText(inspectionLookup?.message || "Detailni zaznam STK neni dostupny."),
      items: [
        { label: "Platnost do", value: "Neuvedeno" },
        { label: "Kilometry pri STK", value: "Neuvedeno" },
        { label: "VIN", value: inspectionLookup?.vin || "-" }
      ]
    };
  }

  const records = Array.isArray(inspections.records) ? inspections.records : [];
  const summary = inspections.summary || {};
  const current = getLatestInspectionRecord(records, summary);
  const status = summary.status || "Nezjisteno";
  const valid = isInspectionCurrentlyValid(summary, status);
  const baseTone = inspectionTone(status);
  const tone = valid ? "success" : baseTone === "warning" ? "warning" : "muted";

  return {
    kicker: "Technicka kontrola",
    title: "STK",
    status,
    tone,
    icon: valid ? CheckCircle2 : AlertTriangle,
    loading: false,
    description: valid
      ? "Technicka prohlidka je podle posledniho zaznamu platna."
      : "Zkontrolujte platnost STK pred provozem vozidla.",
    items: [
      { label: "Platnost do", value: current?.validUntil ? formatDate(current.validUntil) : "Neuvedeno" },
      {
        label: "Rezerva",
        value: typeof summary.daysRemaining === "number" ? formatDaysRemaining(summary.daysRemaining) : "Bez terminu"
      },
      { label: "Posledni STK", value: getInspectionPerformedDate(current) ? formatDate(getInspectionPerformedDate(current)) : "Neuvedeno" },
      { label: "Kilometry pri STK", value: formatOdometer(current) || "Neuvedeno" }
    ]
  };
}

function getVignetteOverview(lookup, plate) {
  if (!plate) {
    return {
      kicker: "Dalnicni poplatek",
      title: "Dalnicni znamka",
      status: "Bez SPZ",
      tone: "muted",
      icon: AlertCircle,
      loading: false,
      description: "Pro overeni dalnicni znamky je potreba SPZ.",
      items: [
        { label: "SPZ", value: "-" },
        { label: "Platnost do", value: "Neuvedeno" },
        { label: "Zdroj", value: "Nenacteno" },
        { label: "Stav", value: "Nezjisteno" }
      ]
    };
  }

  if (!lookup || lookup.status === "pending") {
    return {
      kicker: "Dalnicni poplatek",
      title: "Dalnicni znamka",
      status: "Overuji",
      tone: "muted",
      icon: Loader2,
      loading: true,
      description: "Platnost dalnicni znamky se overuje podle SPZ.",
      items: [
        { label: "SPZ", value: plate },
        { label: "Platnost do", value: "Cekam na zdroj" },
        { label: "Zdroj", value: "Backend proxy" },
        { label: "Stav", value: "Overuji" }
      ]
    };
  }

  if (lookup.status === "unconfigured") {
    return {
      kicker: "Dalnicni poplatek",
      title: "Dalnicni znamka",
      status: "Nenakonfigurovano",
      tone: "muted",
      icon: AlertCircle,
      loading: false,
      description: lookup.message || "Doplnte VIGNETTE_LOOKUP_URL pro online overeni.",
      items: [
        { label: "SPZ", value: plate },
        { label: "Platnost do", value: "Neuvedeno" },
        { label: "Zdroj", value: "VIGNETTE_LOOKUP_URL" },
        { label: "Stav", value: "Neni pripojeno" }
      ]
    };
  }

  if (lookup.status === "error") {
    return {
      kicker: "Dalnicni poplatek",
      title: "Dalnicni znamka",
      status: "Nedostupne",
      tone: "warning",
      icon: AlertTriangle,
      loading: false,
      description: lookup.message || "Dalnicni znamku se nepodarilo overit.",
      items: [
        { label: "SPZ", value: lookup.plate || plate },
        { label: "Platnost do", value: lookup.validUntil ? formatDate(lookup.validUntil) : "Neuvedeno" },
        { label: "Zdroj", value: formatVignetteSource(lookup) },
        { label: "Stav", value: lookup.detail || "Chyba zdroje" }
      ]
    };
  }

  const valid = lookup.valid === true;
  const exempt = lookup.exempt === true;
  const status = exempt ? "Osvobozeno" : valid ? "Platna" : lookup.valid === false ? "Neplatna" : "Nezjisteno";
  const tone = valid ? "success" : lookup.valid === false ? "warning" : "muted";

  return {
    kicker: "Dalnicni poplatek",
    title: "Dalnicni znamka",
    status,
    tone,
    icon: valid ? CheckCircle2 : lookup.valid === false ? AlertTriangle : AlertCircle,
    loading: false,
    description: lookup.message || (valid ? "Dalnicni znamka je podle zdroje platna." : "Platnost dalnicni znamky neni potvrzena."),
    items: [
      { label: "SPZ", value: lookup.plate || plate },
      { label: "Platnost od", value: lookup.validFrom ? formatDate(lookup.validFrom) : "Neuvedeno" },
      { label: "Platnost do", value: lookup.validUntil ? formatDate(lookup.validUntil) : "Neuvedeno" },
      { label: "Zdroj", value: formatVignetteSource(lookup) }
    ]
  };
}

function getResultPlate(result) {
  if (!result || result.kind === "fleet") {
    return "";
  }

  return normalizeSharedLookupValue(
    "plate",
    firstNonEmpty([
      getHighlightValue(result.highlights, "SPZ"),
      getHighlightValue(result.highlights, "RZ"),
      result.plate,
      result.registrationPlate,
      result.query?.type === "plate" ? result.query.raw || result.query.normalized : null
    ])
  );
}

function getResultVin(result) {
  if (!result || result.kind === "fleet") {
    return "";
  }

  return normalizeSharedLookupValue(
    "vin",
    firstNonEmpty([
      getHighlightValue(result.highlights, "VIN"),
      result.vin,
      result.query?.type === "vin" ? result.query.raw || result.query.normalized : null
    ])
  );
}

function getResultPcv(result) {
  if (!result || result.kind === "fleet") {
    return "";
  }

  return normalizeSharedLookupValue(
    "pcv",
    firstNonEmpty([
      getHighlightValue(result.highlights, "PČV"),
      getHighlightValue(result.highlights, "PCV"),
      result.pcv
    ])
  );
}

function withResolvedPlate(result, plate) {
  const item = { label: "SPZ", value: plate, tone: null };
  const highlights = [item, ...(Array.isArray(result.highlights) ? result.highlights : [])];
  const sections = upsertSectionItem(result.sections, "Registrace", item);

  return {
    ...result,
    highlights: uniqueItemsByLabel(highlights),
    sections
  };
}

function upsertSectionItem(sections, title, item) {
  const list = Array.isArray(sections) ? sections : [];
  let found = false;
  const next = list.map((section) => {
    if (normalizeForMatch(section?.title) !== normalizeForMatch(title)) {
      return section;
    }
    found = true;
    return {
      ...section,
      items: uniqueItemsByLabel([item, ...(Array.isArray(section.items) ? section.items : [])])
    };
  });

  return found ? next : [{ title, items: [item] }, ...next];
}

function uniqueItemsByLabel(items) {
  const seen = new Set();
  return (Array.isArray(items) ? items : []).filter((item) => {
    const key = normalizeForMatch(item?.label);
    if (!key || seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  });
}

function isInspectionCurrentlyValid(summary, status) {
  if (typeof summary?.daysRemaining === "number") {
    return summary.daysRemaining >= 0;
  }

  const normalized = normalizeForMatch(status);
  if (normalized.includes("neplat") || normalized.includes("propad")) {
    return false;
  }

  return normalized.includes("plat");
}

function formatVignetteSource(lookup) {
  return lookup?.source?.host || lookup?.source?.label || "Backend proxy";
}

function getHighlightValue(highlights, label) {
  const match = Array.isArray(highlights)
    ? highlights.find((item) => normalizeLabel(item.label) === normalizeLabel(label))
    : null;

  return match?.value || null;
}

function normalizeLabel(value) {
  return String(value || "").trim().toUpperCase();
}

function formatFrontendText(value) {
  if (value === null || value === undefined) {
    return value;
  }

  const internalProvider = ["P", "V", "Z", "P"].join("");
  const secondaryProvider = ["U", "N", "I", "Q", "A"].join("");

  return String(value)
    .replace(new RegExp(`\\bDopln[eě]ne z ${internalProvider}\\b`, "gi"), "Doplněné z registru")
    .replace(new RegExp(`\\bDopln[eě]ne z ${secondaryProvider}\\b`, "gi"), "Doplněné z registru")
    .replace(new RegExp(`\\b${internalProvider}\\s+kalkula[cč]ka\\b`, "gi"), "veřejný zdroj")
    .replace(new RegExp(`\\b${secondaryProvider}\\s+kalkula[cč]ka\\b`, "gi"), "veřejný zdroj")
    .replace(new RegExp(`\\b${internalProvider}\\b`, "gi"), "externí zdroj")
    .replace(new RegExp(`\\b${secondaryProvider}\\b`, "gi"), "externí zdroj");
}

function sanitizeLookupErrorText(value) {
  const text = formatFrontendText(value || "");
  if (!text) {
    return "";
  }

  return text
    .replace(/\bTRANSPORT_CUBE_LOOKUP_URL\b/g, "primární zdroj")
    .replace(/\bbrowserType\.launch:[^.;]*(?:[.;]|$)/gi, "")
    .replace(/\bEPERM:[^.;]*(?:[.;]|$)/gi, "")
    .replace(/\bmkdtemp\s+'[^']*'/gi, "")
    .replace(/\bconnect\s+(?:ECONNREFUSED|EACCES|ETIMEDOUT)\s+[^\s.;]+/gi, "zdroj je dočasně nedostupný")
    .replace(/\s+/g, " ")
    .trim();
}

function isInternalLookupErrorDetail(value) {
  const normalized = normalizeForMatch(value);
  return [
    "browsertype launch",
    "mkdtemp",
    "eperm",
    "transport cube",
    "pvzp",
    "uniqa"
  ].some((marker) => normalized.includes(marker));
}

function isLegalEntityParty(party) {
  if (!party) {
    return false;
  }

  const type = normalizeForMatch(party.type || party.subjectType);
  const name = String(party.name || "").trim();

  return Boolean(party.ico || ((type.includes("pravnick") || type === "company") && isDisplayablePartyText(name)));
}

function isCurrentOwnershipParty(party) {
  return party?.current !== false;
}

function splitOwnershipParties(parties) {
  const sourceParties = Array.isArray(parties) ? parties : [];
  const latestOpenByRole = new Map();

  sourceParties.forEach((party, index) => {
    if (!isCurrentOwnershipParty(party)) {
      return;
    }

    const roleKey = normalizeOwnershipRoleKey(party.role);
    const sinceTime = getOwnershipPartyStartTime(party);
    const previous = latestOpenByRole.get(roleKey);

    if (!previous || sinceTime > previous.sinceTime || (sinceTime === previous.sinceTime && index > previous.index)) {
      latestOpenByRole.set(roleKey, { index, sinceTime });
    }
  });

  return sourceParties.reduce(
    (groups, party, index) => {
      const roleKey = normalizeOwnershipRoleKey(party.role);
      const selected = latestOpenByRole.get(roleKey);
      const isSourceOpenEnded = isCurrentOwnershipParty(party);
      const isEffectiveCurrent = isSourceOpenEnded && selected?.index === index;
      const sinceTime = getOwnershipPartyStartTime(party);
      const inferredDateTo =
        isSourceOpenEnded && !isEffectiveCurrent && selected?.sinceTime > sinceTime
          ? new Date(selected.sinceTime).toISOString()
          : party.dateTo;
      const inferredPeriod =
        inferredDateTo && !party.dateTo
          ? `${firstNonEmpty([party.since, party.dateFrom ? formatDate(party.dateFrom) : null, extractPeriodStartForUi(party.period)])} - ${formatDate(inferredDateTo)}`
          : party.period;
      const normalizedParty = {
        ...party,
        dateTo: inferredDateTo,
        period: inferredPeriod,
        current: isEffectiveCurrent,
        sourceOpenEnded: isSourceOpenEnded && !isEffectiveCurrent
      };

      if (isEffectiveCurrent) {
        groups.currentParties.push(normalizedParty);
      } else {
        groups.historicalParties.push(normalizedParty);
      }

      return groups;
    },
    { currentParties: [], historicalParties: [] }
  );
}

function groupOwnershipHistoryRows(parties) {
  const intervals = buildOwnershipTimelineIntervals(parties);

  if (!intervals.length) {
    return groupOwnershipHistoryRowsByExactPeriod(parties);
  }

  const boundaries = Array.from(
    new Set(
      intervals.flatMap((interval) =>
        Number.isFinite(interval.endTime) ? [interval.startTime, interval.endTime] : [interval.startTime]
      )
    )
  ).sort((left, right) => left - right);

  const rows = [];

  boundaries.forEach((startTime, index) => {
    const endTime = boundaries[index + 1] ?? Infinity;
    const activeIntervals = intervals.filter((interval) => interval.startTime <= startTime && interval.endTime > startTime);

    if (!activeIntervals.length || (endTime === Infinity && !activeIntervals.some((interval) => interval.endTime === Infinity))) {
      return;
    }

    if (activeIntervals.every((interval) => interval.party.current !== false)) {
      return;
    }

    const row = {
      key: `ownership-${startTime}-${Number.isFinite(endTime) ? endTime : "open"}`,
      period: formatOwnershipIntervalDisplay(startTime, endTime),
      startTime,
      endTime,
      sourceIndex: Math.min(...activeIntervals.map((interval) => interval.sourceIndex)),
      owners: [],
      operators: [],
      others: []
    };

    activeIntervals.sort(compareOwnershipTimelineIntervals).forEach((interval) => {
      const roleKey = normalizeOwnershipRoleKey(interval.party.role);
      if (roleKey === "owner") {
        row.owners.push(interval.party);
      } else if (roleKey === "operator") {
        row.operators.push(interval.party);
      } else {
        row.others.push(interval.party);
      }
    });

    rows.push(row);
  });

  return rows.sort(compareOwnershipHistoryRows);
}

function buildOwnershipTimelineIntervals(parties) {
  return (Array.isArray(parties) ? parties : [])
    .map((party, sourceIndex) => {
      const period = getOwnershipPeriodForUi(party);
      const startTime = getOwnershipPartyStartTime(party, period);
      const endTime = getOwnershipPartyEndTime(party, period);

      if (!startTime || endTime <= startTime) {
        return null;
      }

      return {
        party,
        period,
        sourceIndex,
        startTime,
        endTime
      };
    })
    .filter(Boolean);
}

function compareOwnershipTimelineIntervals(left, right) {
  const roleDifference = getOwnershipRoleSortScore(left.party.role) - getOwnershipRoleSortScore(right.party.role);
  if (roleDifference !== 0) {
    return roleDifference;
  }

  const startDifference = left.startTime - right.startTime;
  if (startDifference !== 0) {
    return startDifference;
  }

  return left.sourceIndex - right.sourceIndex;
}

function groupOwnershipHistoryRowsByExactPeriod(parties) {
  const rowsByPeriod = new Map();

  sortOwnershipPartiesByPeriodStartDesc(parties).forEach((party, index) => {
    if (party.current !== false) {
      return;
    }

    const period = getOwnershipPeriodForUi(party);
    const key = `${period.start || "unknown"}|${period.end || ""}|${period.display}`;

    if (!rowsByPeriod.has(key)) {
      rowsByPeriod.set(key, {
        key,
        period: period.display,
        startTime: parseOwnershipDate(period.start),
        endTime: parseOwnershipDate(period.end),
        sourceIndex: index,
        owners: [],
        operators: [],
        others: []
      });
    }

    const row = rowsByPeriod.get(key);
    const roleKey = normalizeOwnershipRoleKey(party.role);
    if (roleKey === "owner") {
      row.owners.push(party);
    } else if (roleKey === "operator") {
      row.operators.push(party);
    } else {
      row.others.push(party);
    }
  });

  return Array.from(rowsByPeriod.values()).sort(compareOwnershipHistoryRows);
}

function sortOwnershipPartiesByPeriodStartDesc(parties) {
  return [...(Array.isArray(parties) ? parties : [])].sort(compareOwnershipPartiesByPeriodStartDesc);
}

function compareOwnershipPartiesByPeriodStartDesc(left, right) {
  const leftPeriod = getOwnershipPeriodForUi(left);
  const rightPeriod = getOwnershipPeriodForUi(right);
  const startDifference = parseOwnershipDate(rightPeriod.start) - parseOwnershipDate(leftPeriod.start);
  if (startDifference !== 0) {
    return startDifference;
  }

  const endDifference = parseOwnershipDate(rightPeriod.end) - parseOwnershipDate(leftPeriod.end);
  if (endDifference !== 0) {
    return endDifference;
  }

  return getOwnershipRoleSortScore(left.role) - getOwnershipRoleSortScore(right.role);
}

function compareOwnershipHistoryRows(left, right) {
  if (left.startTime !== right.startTime) {
    return right.startTime - left.startTime;
  }

  if (left.endTime !== right.endTime) {
    return right.endTime - left.endTime;
  }

  return left.sourceIndex - right.sourceIndex;
}

function getOwnershipRoleSortScore(role) {
  const roleKey = normalizeOwnershipRoleKey(role);
  if (roleKey === "owner") {
    return 0;
  }
  if (roleKey === "operator") {
    return 1;
  }
  return 2;
}

function getOwnershipPeriodForUi(party) {
  const period = String(party?.period || "").trim();
  const start = party?.since || extractPeriodStartForUi(period);
  const end = extractPeriodEndForUi(period);

  return {
    start,
    end,
    display: period || (start ? `od ${start}` : "Neuvedeno")
  };
}

function getOwnershipPartyStartTime(party, period = getOwnershipPeriodForUi(party)) {
  return parseOwnershipDate(firstNonEmpty([party?.dateFrom, party?.validFrom, party?.firstSeen, party?.since, period.start]));
}

function getOwnershipPartyEndTime(party, period = getOwnershipPeriodForUi(party)) {
  const parsedEnd = parseOwnershipDate(firstNonEmpty([party?.dateTo, party?.validTo, party?.lastSeen, party?.until, period.end]));
  return parsedEnd || Infinity;
}

function formatOwnershipIntervalDisplay(startTime, endTime) {
  const start = formatOwnershipIntervalDate(startTime);
  const end = Number.isFinite(endTime) ? formatOwnershipIntervalDate(endTime) : "-";
  return start ? `${start} - ${end}` : "Neuvedeno";
}

function formatOwnershipIntervalDate(timestamp) {
  if (!timestamp || !Number.isFinite(timestamp)) {
    return "";
  }

  return new Intl.DateTimeFormat("cs-CZ", {
    day: "numeric",
    month: "numeric",
    year: "numeric"
  }).format(new Date(timestamp));
}

function normalizeOwnershipRoleKey(role) {
  const normalized = normalizeForMatch(role);

  if (normalized.includes("vlast")) {
    return "owner";
  }

  if (normalized.includes("provoz")) {
    return "operator";
  }

  return normalized || "subject";
}

function parseOwnershipDate(value) {
  const normalized = String(value || "").trim();
  const localizedDate = normalized.match(/^(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})$/);

  if (localizedDate) {
    return Date.UTC(Number(localizedDate[3]), Number(localizedDate[2]) - 1, Number(localizedDate[1]));
  }

  const parsed = Date.parse(normalized);
  return Number.isNaN(parsed) ? 0 : parsed;
}

function extractPeriodStartForUi(period) {
  return String(period || "").split("-")[0].trim();
}

function extractPeriodEndForUi(period) {
  const value = String(period || "").trim();
  const parts = value.split(/\s+-\s+/);
  return parts.length > 1 ? parts.slice(1).join(" - ").trim() : "";
}

function isDisplayablePartyText(value) {
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

function normalizeForMatch(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function normalizeWhitespace(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function firstNonEmpty(values) {
  for (const value of Array.isArray(values) ? values : []) {
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

function countLegalRole(parties, roleNeedle) {
  return (Array.isArray(parties) ? parties : []).filter((party) =>
    normalizeLabel(party.role).toLowerCase().includes(roleNeedle)
  ).length;
}

function countUniqueLegalEntities(parties) {
  const keys = new Set();
  (Array.isArray(parties) ? parties : []).forEach((party) => {
    const key = normalizeSharedLookupValue("ico", party?.ico) || normalizeForMatch(party?.name);
    if (key) {
      keys.add(key);
    }
  });
  return keys.size;
}

function formatOwnershipRelationCount(count) {
  const normalizedCount = Number(count) || 0;
  if (normalizedCount === 1) {
    return "1 vazba";
  }
  if (normalizedCount >= 2 && normalizedCount <= 4) {
    return `${normalizedCount} vazby`;
  }
  return `${normalizedCount} vazeb`;
}

function buildOwnershipPanelMessage({ lookup, parties, sourceOwnerCount, sourceOperatorCount, query }) {
  if (parties.length > 0) {
    const uniqueCount = countUniqueLegalEntities(parties);
    const subjectText = uniqueCount === 1 ? "1 právnický subjekt" : `${uniqueCount} právnických subjektů`;
    const relationText = formatOwnershipRelationCount(parties.length);
    return `Nalezeno ${subjectText} a ${relationText} navázaných na ${lookup?.vin || query?.type === "vin" ? "VIN" : "vozidlo"}.`;
  }

  if (lookup?.status === "pending") {
    return "Právnické osoby se dohledávají podle VIN a dostupných vazeb v registru. Jakmile otevřená data vrátí detail, panel se automaticky doplní.";
  }

  if (lookup?.message) {
    return formatFrontendText(lookup.message);
  }

  if (sourceOwnerCount != null || sourceOperatorCount != null) {
    return "Zdroj vrátil souhrnné počty vazeb, ale detail právnických osob pro toto vozidlo není v dostupné sadě.";
  }

  return "Pro tento dotaz zatím není dostupná žádná právnická osoba.";
}

function formatOwnershipStatus(lookup, legalPartyCount) {
  if (legalPartyCount > 0) {
    return legalPartyCount === 1
      ? "1 právnický subjekt připravený k zobrazení."
      : `${legalPartyCount} právnické subjekty připravené k zobrazení.`;
  }

  if (lookup?.status === "pending") {
    return "Čeká se na detail z otevřených dat.";
  }

  if (lookup?.message) {
    return formatFrontendText(lookup.message);
  }

  return "Bez detailů právnických osob.";
}

function compactPartyMeta(party) {
  return [
    party.ico ? `IČO ${party.ico}` : null,
    party.address || null,
    party.period || null,
    !party.period && party.since ? `od ${party.since}` : null
  ]
    .filter(Boolean)
    .join(" · ");
}

function buildAresUrl(ico) {
  const digits = String(ico || "").replace(/\D/g, "");
  return digits.length === 8 ? `https://ares.gov.cz/ekonomicke-subjekty?ico=${digits}` : null;
}

function ErrorState({ message, query, type }) {
  return (
    <Card className="border-white/10 bg-card/85">
      <CardContent className="p-6 sm:p-8">
        <div className="flex flex-col gap-5 sm:flex-row sm:items-start sm:justify-between">
          <div className="space-y-4">
            <Badge variant="warning" className="w-fit">
              Bez výsledků
            </Badge>
            <div>
              <h2 className="text-3xl font-semibold tracking-[-0.04em] text-white sm:text-4xl">
                Pro tento identifikátor zatím nic nemám
              </h2>
              <p className="mt-4 max-w-3xl text-sm leading-7 text-muted-foreground sm:text-base">
                {message}
              </p>
            </div>
          </div>

          <div className="rounded-[1.5rem] border border-white/10 bg-background/60 p-4 sm:min-w-[280px]">
            <div className="mb-3 flex items-center gap-2">
              <AlertCircle className="size-4 text-amber-200" />
              <p className="text-sm font-semibold text-white">Poslední dotaz</p>
            </div>
            <p className="text-sm leading-7 text-muted-foreground">
              {(type === "vin" ? "VIN" : "SPZ") + (query ? `: ${query}` : "")}
            </p>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function LoadingState() {
  return (
    <div className="space-y-4">
      <Card className="border-white/10 bg-card/75">
        <CardContent className="space-y-4 p-6 sm:p-8">
          <Skeleton className="h-6 w-40 rounded-full" />
          <Skeleton className="h-12 w-3/4" />
          <Skeleton className="h-5 w-full max-w-3xl" />
          <Skeleton className="h-5 w-2/3" />
        </CardContent>
      </Card>

      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-6">
        {Array.from({ length: 6 }).map((_, index) => (
          <Card className="border-white/10 bg-card/65" key={index}>
            <CardContent className="p-5">
              <Skeleton className="h-4 w-16" />
              <Skeleton className="mt-4 h-8 w-24" />
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}

function EmptyPanel({ text }) {
  return (
    <Card className="border-dashed border-white/10 bg-card/70">
      <CardContent className="p-6">
        <p className="text-sm leading-7 text-muted-foreground">{text}</p>
      </CardContent>
    </Card>
  );
}

function MiniStat({ label, value }) {
  return (
    <div className="rounded-[1.25rem] border border-white/10 bg-background/60 p-4">
	      <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">
	        {formatFrontendText(label)}
	      </p>
	      <p className="mt-3 text-3xl font-semibold text-white">{formatFrontendText(value ?? "-")}</p>
    </div>
  );
}

function MetricTile({ label, value, muted = false }) {
  return (
    <div className={cn("rounded-[1rem] border p-3", muted ? "border-white/8 bg-background/35" : "border-white/12 bg-white/[0.06]")}>
	      <p className="text-[10px] font-semibold uppercase tracking-[0.22em] text-muted-foreground">{formatFrontendText(label)}</p>
	      <p className={cn("mt-2 text-2xl font-semibold", muted ? "text-muted-foreground" : "text-white")}>{formatFrontendText(value)}</p>
    </div>
  );
}

function StatusPill({ lookup, partyCount }) {
  const pending = lookup?.status === "pending";
  const ready = partyCount > 0 || lookup?.status === "ready";
  const Icon = pending ? Loader2 : ready ? CheckCircle2 : AlertTriangle;

  return (
    <div className="flex items-center gap-3 rounded-[1.1rem] border border-white/10 bg-white/[0.04] p-4">
      <div className="rounded-full border border-white/10 bg-background/80 p-2">
        <Icon className={cn("size-4", pending ? "animate-spin text-muted-foreground" : ready ? "text-emerald-200" : "text-amber-200")} />
      </div>
      <div>
        <p className="text-sm font-semibold text-white">
          {pending ? "Dohledávám subjekty" : ready ? "Detail připraven" : "Detail není ve zdroji"}
        </p>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">{formatOwnershipStatus(lookup, partyCount)}</p>
      </div>
    </div>
  );
}

function QuickMeta({ icon: Icon, label, value }) {
  return (
    <div className="flex items-start gap-3 rounded-[1rem] border border-white/10 bg-white/[0.03] p-3">
      <div className="rounded-full border border-white/10 bg-background/80 p-2">
        <Icon className="size-4 text-muted-foreground" />
      </div>
      <div className="min-w-0">
        <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">
	          {formatFrontendText(label)}
	        </p>
	        <p className="mt-1 text-sm leading-6 text-white">{formatFrontendText(value)}</p>
      </div>
    </div>
  );
}

function buildLookupEndpoint(value, type) {
  const params = new URLSearchParams();
  params.set("query", value);

  const normalizedType = normalizeLookupType(type);
  if (normalizedType) {
    params.set("type", normalizedType);
  }

  return `/api/lookup?${params.toString()}`;
}

function resolveLookupType(value, requestedType) {
  if (isLikelyUrlInput(value)) {
    return "";
  }

  const normalizedType = normalizeLookupType(requestedType);
  return normalizedType || detectType(value);
}

function readSharedLookupFromLocation() {
  if (typeof window === "undefined") {
    return null;
  }

  const url = new URL(window.location.href);
  const segments = url.pathname.split("/").filter(Boolean);
  const pathType = normalizeLookupType(segments[0]);

  if (pathType && segments[1]) {
    const value = normalizeSharedLookupValue(pathType, safeDecodeURIComponent(segments.slice(1).join("/")));
    return value ? { type: pathType, value } : null;
  }

  for (const [paramName, type] of [
    ["spz", "plate"],
    ["plate", "plate"],
    ["vin", "vin"]
  ]) {
    const value = normalizeSharedLookupValue(type, url.searchParams.get(paramName));
    if (value) {
      return { type, value };
    }
  }

  const typedQuery = normalizeSharedLookupValue(
    normalizeLookupType(url.searchParams.get("type")) || null,
    url.searchParams.get("q") || url.searchParams.get("query")
  );

  if (typedQuery) {
    return {
      type: normalizeLookupType(url.searchParams.get("type")) || detectType(typedQuery),
      value: typedQuery
    };
  }

  return null;
}

function buildShareUrl(type, value) {
  if (typeof window === "undefined") {
    return "";
  }

  const normalizedType = normalizeLookupType(type) || detectType(value);
  const normalizedValue = normalizeSharedLookupValue(normalizedType, value);
  if (!normalizedValue) {
    return "";
  }

  const url = new URL(window.location.href);
  url.pathname = buildSharePath(normalizedType, normalizedValue);
  url.search = "";
  url.hash = "";
  return url.toString();
}

function buildSharePath(type, value) {
  const slug = type === "vin" ? "vin" : "spz";
  return `/${slug}/${encodeURIComponent(value)}`;
}

function pushShareUrl(url) {
  if (!url || typeof window === "undefined" || window.location.href === url) {
    return;
  }

  window.history.pushState({ lookup: true }, "", url);
}

function navigateToSharedLookup(type, value) {
  const url = buildShareUrl(type, value);
  if (!url || typeof window === "undefined") {
    return;
  }

  if (window.location.href !== url) {
    window.history.pushState({ lookup: true }, "", url);
  }

  window.dispatchEvent(typeof PopStateEvent === "function" ? new PopStateEvent("popstate", { state: { lookup: true } }) : new Event("popstate"));
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function getShareLookupKey(type, value) {
  const normalizedType = normalizeLookupType(type) || detectType(value);
  return `${normalizedType}:${normalizeSharedLookupValue(normalizedType, value)}`;
}

function normalizeLookupType(value) {
  const normalized = String(value || "").trim().toLowerCase();

  if (normalized === "spz" || normalized === "plate") {
    return "plate";
  }

  if (normalized === "vin") {
    return "vin";
  }

  return "";
}

function normalizeSharedLookupValue(type, value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }

  if (type === "ico") {
    return raw.replace(/\D/g, "");
  }

  return raw.toUpperCase().replace(/\s+/g, "");
}

function safeDecodeURIComponent(value) {
  try {
    return decodeURIComponent(value);
  } catch (error) {
    return value;
  }
}

function isLikelyUrlInput(value) {
  const raw = String(value || "").trim();
  return /^https?:\/\//i.test(raw);
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("Fotku se nepodařilo načíst."));
    reader.readAsDataURL(file);
  });
}

async function copyTextToClipboard(value) {
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(value);
      return true;
    } catch (error) {
      // Fall back to the hidden textarea path below.
    }
  }

  const textarea = document.createElement("textarea");
  textarea.value = value;
  textarea.setAttribute("readonly", "");
  textarea.style.position = "fixed";
  textarea.style.opacity = "0";
  document.body.appendChild(textarea);
  textarea.select();

  try {
    return document.execCommand("copy");
  } finally {
    document.body.removeChild(textarea);
  }
}

function detectType(value) {
  const compact = String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

  if (/^[A-HJ-NPR-Z0-9]{17}$/.test(compact)) {
    return "vin";
  }

  return "plate";
}

function isValidIco(value) {
  const digits = String(value || "").replace(/\D/g, "");
  if (!/^\d{8}$/.test(digits)) {
    return false;
  }

  const weights = [8, 7, 6, 5, 4, 3, 2];
  const sum = weights.reduce((accumulator, weight, index) => accumulator + Number(digits[index]) * weight, 0);
  const mod = sum % 11;
  let checkDigit = 11 - mod;

  if (checkDigit === 10) {
    checkDigit = 0;
  } else if (checkDigit === 11) {
    checkDigit = 1;
  }

  return checkDigit === Number(digits[7]);
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

  return sanitizeLookupErrorText(formatFrontendText(lines.join(" ")));
}

function renderQuery(query) {
  if (!query) {
    return "Bez dotazu";
  }

  return `${query.type === "vin" ? "VIN" : "SPZ"} | ${query.raw || query.normalized || ""}`;
}

function formatResolvedTime(value) {
  if (!value) {
    return "Neuvedeno";
  }

  const parsed = new Date(value);

  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("cs-CZ", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(parsed);
}

function formatDate(value) {
  if (!value) {
    return "Neuvedeno";
  }

  const parsed = new Date(value);

  if (Number.isNaN(parsed.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat("cs-CZ", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric"
  }).format(parsed);
}

function getLatestInspectionRecord(records, summary = {}) {
  const sourceRecords = Array.isArray(records) ? records : [];
  if (sourceRecords.length === 0) {
    return summary?.currentRecord || null;
  }

  return sourceRecords
    .map((record, index) => ({
      record,
      index,
      timestamp: getDateTimestamp(getInspectionPerformedDate(record))
    }))
    .sort((left, right) => right.timestamp - left.timestamp || left.index - right.index)[0]?.record || summary?.currentRecord || null;
}

function buildInspectionMileageRows(records) {
  return (Array.isArray(records) ? records : [])
    .map((record, index) => {
      const date = record?.performedOn || record?.validFrom || record?.validUntil || null;

      return {
        record,
        index,
        date,
        timestamp: getDateTimestamp(date),
        odometerValue: getOdometerValue(record)
      };
    })
    .filter((row) => row.date || row.odometerValue !== null)
    .sort((left, right) => right.timestamp - left.timestamp || left.index - right.index);
}

function getInspectionRecordKey(record, index) {
  return [
    record?.protocolNumber,
    record?.sourceId,
    record?.validFrom,
    record?.validUntil,
    index
  ]
    .filter((value) => value !== null && value !== undefined && value !== "")
    .join("|");
}

function getInspectionPerformedDate(record) {
  return record?.performedOn || record?.validFrom || null;
}

function formatInspectionPerformedDateTime(record) {
  const performedOn = getInspectionPerformedDate(record);
  if (!performedOn) {
    return null;
  }

  const start = record?.technicalStartedAt || record?.inspectionStartedAt || null;
  const end = record?.technicalEndedAt || record?.inspectionEndedAt || null;
  const timeRange = formatInspectionTimeRange(start, end);
  const date = formatDate(performedOn);

  return timeRange ? `${date}, ${timeRange}` : date;
}

function formatInspectionTimeRange(start, end) {
  const formattedStart = start ? formatResolvedTime(start) : "";
  const formattedEnd = end ? formatResolvedTime(end) : "";

  if (formattedStart && formattedEnd) {
    return `${formattedStart} → ${formattedEnd}`;
  }

  return formattedStart || formattedEnd || "";
}

function formatInspectionType(value) {
  const formatted = formatFrontendText(value || "").replace(/^[A-Z]\s*[-–]\s*/, "").trim();
  return formatted || "Technická prohlídka";
}

function formatInspectionNext(record) {
  if (!record?.validUntil) {
    return "Neuvedeno";
  }

  const days = diffDaysFromNow(record.validUntil);
  const date = formatDate(record.validUntil);
  if (days === null) {
    return date;
  }

  if (days < 0) {
    return `${date} (${Math.abs(days)} d po termínu)`;
  }

  if (days === 0) {
    return `${date} (dnes)`;
  }

  return `${date} (zbývá ${days} d)`;
}

function formatInspectionResult(record) {
  const state = formatFrontendText(record?.state || "");
  const normalized = normalizeForMatch(state);

  if (!state || normalized === "nezjisteno") {
    return "Nezjištěno";
  }

  if (normalized === "a") {
    return "Vyhovuje";
  }

  return state.length === 1 ? `Stav ${state}` : state;
}

function inspectionResultVariant(record) {
  const normalized = normalizeForMatch(record?.state);

  if (normalized === "a" || normalized.includes("vyhov")) {
    return "success";
  }

  if (normalized && normalized !== "nezjisteno") {
    return "warning";
  }

  return "muted";
}

function normalizeInspectionDefectsForUi(defects) {
  const source = Array.isArray(defects) ? defects : [];
  const seen = new Set();
  return source
    .map((defect) => {
      const code = normalizeDefectText(
        typeof defect === "string" ? defect : defect?.code || defect?.kod || defect?.id
      );
      const severity = normalizeDefectText(
        typeof defect === "string" ? "" : defect?.severity || defect?.type || defect?.zavaznost
      ).toUpperCase();
      const description = normalizeDefectText(
        typeof defect === "string" ? "" : defect?.description || defect?.text || defect?.popis || defect?.name
      );

      if (!code && !severity && !description) {
        return null;
      }

      return {
        code,
        severity,
        description
      };
    })
    .filter((defect) => {
      if (!defect) {
        return false;
      }

      const key = defect.code || `${defect.severity}:${normalizeForMatch(defect.description)}`;
      if (seen.has(key)) {
        return false;
      }

      seen.add(key);
      return true;
    });
}

function normalizeDefectText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function inspectionDefectSeverityVariant(defect) {
  const severity = normalizeDefectText(defect?.severity).toUpperCase();
  if (severity === "A") {
    return "success";
  }

  if (severity) {
    return "warning";
  }

  return "muted";
}

function getInspectionDefectSeverityInfo(severity) {
  const normalized = normalizeDefectText(severity).toUpperCase();

  if (normalized === "A") {
    return {
      ariaLabel: "Stupeň závady A, lehká závada",
      tooltip: "A - lehká závada. Nemá bezprostřední vliv na bezpečnost provozu, ale je vhodné ji odstranit."
    };
  }

  if (normalized === "B") {
    return {
      ariaLabel: "Stupeň závady B, vážná závada",
      tooltip: "B - vážná závada. Může omezit způsobilost vozidla a obvykle vyžaduje odstranění a opakovanou kontrolu."
    };
  }

  if (normalized === "C") {
    return {
      ariaLabel: "Stupeň závady C, nebezpečná závada",
      tooltip: "C - nebezpečná závada. Může bezprostředně ohrožovat bezpečnost provozu nebo životní prostředí."
    };
  }

  return {
    ariaLabel: normalized ? `Stupeň závady ${normalized}` : "Stupeň závady neuveden",
    tooltip: normalized ? `${normalized} - stupeň závady podle výsledku STK.` : "Stupeň závady není v záznamu uvedený."
  };
}

function inspectionDefectRowClassName(defect) {
  const severity = normalizeDefectText(defect?.severity).toUpperCase();
  if (severity === "A") {
    return "border-emerald-500/15 bg-emerald-500/[0.07]";
  }

  if (severity === "B") {
    return "border-amber-500/20 bg-amber-500/[0.08]";
  }

  if (severity === "C") {
    return "border-red-500/20 bg-red-500/[0.08]";
  }

  return "border-white/8 bg-black/20";
}

function formatRecordCountLabel(count) {
  if (count === 1) {
    return "záznam";
  }

  if (count >= 2 && count <= 4) {
    return "záznamy";
  }

  return "záznamů";
}

function formatInspectionStation(record) {
  return [
    formatInspectionStationLabel(record),
    record?.stationMunicipality,
    record?.stationRegion
  ]
    .filter(Boolean)
    .join(" · ") || null;
}

function formatInspectionStationLabel(record) {
  const rawCode = String(record?.stationCode || "").trim().replace(/^STK\s+/i, "");
  const code = rawCode || "";
  const rawName = String(record?.stationName || "").trim();
  const genericName = code && (!rawName || normalizeForMatch(rawName) === normalizeForMatch(`STK ${code}`) || rawName === code);
  const mappedName = code ? KNOWN_STK_STATION_NAMES[code] : "";
  const name = genericName ? mappedName : rawName.replace(/^STK\s+/i, "") || mappedName;

  if (code && name) {
    return `STK ${name} (${code})`;
  }

  if (rawName) {
    return rawName;
  }

  return code ? `STK ${code}` : null;
}
function getDateTimestamp(value) {
  if (!value) {
    return 0;
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? 0 : parsed.getTime();
}

function getOdometerValue(record) {
  const raw = record?.odometer;
  if (raw === null || raw === undefined || raw === "") {
    return null;
  }

  if (typeof raw === "number") {
    return Number.isFinite(raw) ? Math.round(raw) : null;
  }

  const normalized = String(raw)
    .trim()
    .replace(/\s+/g, "")
    .replace(/[^\d,.-]/g, "")
    .replace(",", ".");

  if (!normalized) {
    return null;
  }

  const value = Number.parseInt(normalized, 10);
  return Number.isFinite(value) ? value : null;
}

function formatOdometer(record) {
  const value = getOdometerValue(record);
  return value === null ? null : formatMileageValue(value, record);
}

function formatMileageValue(value, record) {
  return `${new Intl.NumberFormat("cs-CZ").format(value)} ${record?.odometerUnit || "km"}`;
}

function formatMileageDelta(value) {
  const sign = value > 0 ? "+" : value < 0 ? "-" : "";
  return `${sign}${new Intl.NumberFormat("cs-CZ").format(Math.abs(value))} km`;
}

function formatCompactKm(value) {
  if (value >= 1000000) {
    return `${Math.round(value / 100000) / 10} mil.`;
  }

  if (value >= 1000) {
    return `${Math.round(value / 1000)} tis.`;
  }

  return String(value);
}

function formatDaysRemaining(value) {
  if (value < 0) {
    return `${Math.abs(value)} d po termínu`;
  }

  if (value === 0) {
    return "Končí dnes";
  }

  return `${value} d do konce`;
}

function diffDaysFromNow(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const target = new Date(parsed);
  target.setHours(0, 0, 0, 0);

  return Math.round((target.getTime() - today.getTime()) / 86400000);
}

function inspectionTone(status) {
  const normalized = normalizeForMatch(status);

  if (normalized.includes("plat")) {
    return "success";
  }

  if (normalized.includes("konci") || normalized.includes("propad")) {
    return "warning";
  }

  return "muted";
}

function statusVariant(status) {
  const normalized = normalizeForMatch(status);

  if (normalized.includes("aktiv") || normalized.includes("provozu")) {
    return "success";
  }

  if (
    normalized.includes("pozor") ||
    normalized.includes("omezen") ||
    normalized.includes("konci") ||
    normalized.includes("propadl")
  ) {
    return "warning";
  }

  return "muted";
}

function readCookie(name) {
  if (typeof document === "undefined") {
    return "";
  }

  const prefix = `${encodeURIComponent(name)}=`;
  const match = document.cookie
    .split(";")
    .map((part) => part.trim())
    .find((part) => part.startsWith(prefix));

  if (!match) {
    return "";
  }

  try {
    return decodeURIComponent(match.slice(prefix.length));
  } catch (error) {
    return "";
  }
}

function writeCookie(name, value, maxAgeDays) {
  if (typeof document === "undefined") {
    return;
  }

  const maxAge = Math.max(0, Number(maxAgeDays) || 0) * 24 * 60 * 60;
  const secure = window.location.protocol === "https:" ? "; Secure" : "";
  document.cookie = `${encodeURIComponent(name)}=${encodeURIComponent(value)}; Max-Age=${maxAge}; Path=/; SameSite=Lax${secure}`;
}

function deleteCookie(name) {
  if (typeof document === "undefined") {
    return;
  }

  const secure = window.location.protocol === "https:" ? "; Secure" : "";
  document.cookie = `${encodeURIComponent(name)}=; Max-Age=0; Path=/; SameSite=Lax${secure}`;
}

function getOrCreateAnalyticsSessionId() {
  const existing = readCookie(ANALYTICS_SESSION_COOKIE_NAME);
  if (existing) {
    return existing;
  }

  if (window.crypto?.randomUUID) {
    return window.crypto.randomUUID();
  }

  const bytes = new Uint8Array(16);
  if (window.crypto?.getRandomValues) {
    window.crypto.getRandomValues(bytes);
  } else {
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Math.floor(Math.random() * 256);
    }
  }

  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}
