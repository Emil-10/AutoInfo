import { useEffect, useMemo, useState } from "react";
import {
  AlertCircle,
  AlertTriangle,
  ArrowUpRight,
  Building2,
  CalendarRange,
  CarFront,
  CheckCircle2,
  Clock3,
  FileText,
  Fingerprint,
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

const DEMO_EXAMPLES = ["1AB2345", "TMBJJ7NE8L0123456", "5AC5678", "27074358"];

export default function App() {
  const [query, setQuery] = useState("");
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [hasSearched, setHasSearched] = useState(false);
  const [selectedType, setSelectedType] = useState("plate");
  const [statusText, setStatusText] = useState(
    "Zadej SPZ, ICO nebo 17mistny VIN. Rozhrani typ pozna automaticky."
  );

  const detectedType = useMemo(() => detectType(query), [query]);

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
                  inspectionLookup: payload
                }
              : current
          );
          setStatusText("Zaklad je pripraveny a technicke prohlidky byly doplneny.");
          return;
        }

        if (payload.status === "pending" && attempts < 30) {
          timeoutId = window.setTimeout(poll, attempts < 8 ? 2000 : 4000);
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

  async function performLookup(nextQuery) {
    const trimmed = nextQuery.trim();

    if (!trimmed) {
      setStatusText("Nejdriv zadej SPZ, ICO nebo VIN.");
      setError("");
      setResult(null);
      return;
    }

    setLoading(true);
    setHasSearched(true);
    setResult(null);
    setError("");
    setStatusText("Nacitam data...");

    try {
      const queryType = detectType(trimmed);
      const endpoint =
        queryType === "ico"
          ? `/api/company-fleet?ico=${encodeURIComponent(trimmed)}`
          : `/api/lookup?query=${encodeURIComponent(trimmed)}`;
      const response = await fetch(endpoint, {
        headers: { Accept: "application/json" }
      });
      const payload = await response.json();

      if (!response.ok) {
        throw new Error(composeErrorMessage(payload));
      }

      setResult(payload);
      setStatusText(
        payload.kind === "fleet"
          ? "Hotovo. Seznam vozidel firmy je pripraveny."
          : payload.inspectionLookup?.status === "pending"
          ? "Zaklad je pripraveny. Technicke prohlidky se nacitaji na pozadi."
          : "Hotovo. Vysledek je pripraveny."
      );
    } catch (lookupError) {
      setError(lookupError.message || "Vyhledavani selhalo.");
      setStatusText("Bez vysledku pro zadany dotaz.");
    } finally {
      setLoading(false);
    }
  }

  function handleSubmit(event) {
    event.preventDefault();
    performLookup(query);
  }

  function handleQueryChange(value) {
    setQuery(value);
    const compact = String(value || "").replace(/\D/g, "");
    if (selectedType === "ico" && compact.length <= 8) {
      setSelectedType("ico");
      return;
    }

    setSelectedType(detectType(value));
  }

  return (
    <div className="relative min-h-screen overflow-hidden bg-background text-foreground">
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_top,rgba(255,255,255,0.10),transparent_28%)]" />
      <div className="pointer-events-none absolute inset-0 bg-app-grid bg-[size:72px_72px] opacity-[0.03]" />
      <div className="pointer-events-none absolute inset-x-0 top-0 h-72 bg-[radial-gradient(circle_at_center,rgba(255,255,255,0.08),transparent_68%)]" />

      <main
        className={cn(
          "container relative z-10 flex flex-col transition-all duration-500",
          hasSearched ? "gap-8 py-8" : "min-h-[72vh] justify-center py-4 sm:py-6"
        )}
      >
        <section className={cn("mx-auto w-full transition-all duration-500", hasSearched ? "max-w-5xl" : "max-w-3xl")}>
          <div className="mb-6 text-center">
            <h1 className={cn("mx-auto max-w-4xl font-semibold tracking-[-0.05em] text-white", hasSearched ? "text-4xl sm:text-5xl" : "text-5xl sm:text-7xl")}>
              Najdi vozidlo podle VIN nebo SPZ.
            </h1>
            <p className="mx-auto mt-5 max-w-2xl text-balance text-sm leading-7 text-muted-foreground sm:text-base">
              Jedno pole, automaticke rozpoznani typu a okamzity prehled toho
              podstatneho v co nejcistsim rozlozeni.
            </p>
          </div>

          <Card className="border-white/10 bg-card/80 backdrop-blur-xl">
            <CardContent className="p-4 sm:p-5">
              <div className="mb-5 flex justify-center">
                <div className="inline-grid w-[320px] grid-cols-3 rounded-full border border-border bg-secondary p-1">
                  <button
                    className={cn(
                      "rounded-full px-4 py-2 text-sm font-semibold transition-all duration-300",
                      selectedType === "plate"
                        ? "bg-primary text-primary-foreground shadow-lg shadow-black/30"
                        : "text-muted-foreground"
                    )}
                    onClick={() => setSelectedType("plate")}
                    type="button"
                  >
                    SPZ
                  </button>
                  <button
                    className={cn(
                      "rounded-full px-4 py-2 text-sm font-semibold transition-all duration-300",
                      selectedType === "vin"
                        ? "bg-primary text-primary-foreground shadow-lg shadow-black/30"
                        : "text-muted-foreground"
                    )}
                    onClick={() => setSelectedType("vin")}
                    type="button"
                  >
                    VIN
                  </button>
                  <button
                    className={cn(
                      "rounded-full px-4 py-2 text-sm font-semibold transition-all duration-300",
                      selectedType === "ico"
                        ? "bg-primary text-primary-foreground shadow-lg shadow-black/30"
                        : "text-muted-foreground"
                    )}
                    onClick={() => setSelectedType("ico")}
                    type="button"
                  >
                    ICO
                  </button>
                </div>
              </div>

              <form className="space-y-4" onSubmit={handleSubmit}>
                <div className="flex flex-col gap-3 sm:flex-row">
                  <div className="relative flex-1">
                    <Search className="pointer-events-none absolute left-5 top-1/2 size-5 -translate-y-1/2 text-muted-foreground" />
                    <Input
                      autoComplete="off"
                      className="h-14 border-white/10 bg-secondary pl-12 pr-4 text-base shadow-inner shadow-black/20 placeholder:text-muted-foreground/80"
                      maxLength={24}
                      onChange={(event) => handleQueryChange(event.target.value)}
                      placeholder={
                        selectedType === "vin"
                          ? "Napr. WAUZZZF41NA010563"
                          : selectedType === "ico"
                            ? "Napr. 27074358"
                          : "Napr. 1AB2345"
                      }
                      value={query}
                    />
                  </div>

                  <Button className="h-14 rounded-full px-6 sm:px-7" size="lg" type="submit">
                    {loading ? (
                      <>
                        <Loader2 className="mr-2 size-4 animate-spin" />
                        Hledam
                      </>
                    ) : (
                      <>
                        Vyhledat
                        <ArrowUpRight className="ml-2 size-4" />
                      </>
                    )}
                  </Button>
                </div>

                <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
                  <p className="text-sm leading-6 text-muted-foreground">{statusText}</p>
                  <div className="flex flex-wrap gap-2">
                    {DEMO_EXAMPLES.map((example) => (
                      <Button
                        className="rounded-full"
                        key={example}
                        onClick={() => {
                          setQuery(example);
                          setSelectedType(detectType(example));
                          performLookup(example);
                        }}
                        size="sm"
                        type="button"
                        variant="ghost"
                      >
                        {example}
                      </Button>
                    ))}
                  </div>
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
            result.kind === "fleet" ? <CompanyFleetState result={result} /> : <ResultState result={result} />
          ) : null}
        </section>
      )}
      </main>
    </div>
  );
}

function ResultState({ result }) {
  const detailSections = Array.isArray(result.sections) ? result.sections : [];
  const timelineEntries = Array.isArray(result.timeline) ? result.timeline : [];
  const parties = Array.isArray(result.ownership?.parties) ? result.ownership.parties : [];
  const inspections = result.inspections || null;
  const inspectionLookup = result.inspectionLookup || null;
  const identifierHighlights = extractIdentifierHighlights(result.highlights);
  const summaryHighlights = Array.isArray(result.highlights)
    ? result.highlights.filter((item) => !["SPZ", "VIN"].includes(normalizeLabel(item.label)))
    : [];
  const compactSections = detailSections.filter((section) => Array.isArray(section.items) && section.items.length > 0);
  const heroBlurb = buildHeroBlurb(result, parties.length);

  return (
    <div className="space-y-4">
      <Card className="border-white/10 bg-card/85 backdrop-blur-xl">
        <CardContent className="p-6 sm:p-8">
          <div className="space-y-6">
            <div className="flex flex-col gap-6 xl:flex-row xl:items-start xl:justify-between">
              <div className="space-y-4">
                <div className="flex flex-wrap gap-2">
                  <Badge>{result.hero?.badge || "Vozidlo"}</Badge>
                  <Badge variant={statusVariant(result.hero?.status)}>
                    {result.hero?.status || "Neuvedeno"}
                  </Badge>
                </div>

                <div className="space-y-3">
                  <h2 className="text-3xl font-semibold tracking-[-0.04em] text-white sm:text-5xl">
                    {result.hero?.title || "Bez nazvu"}
                  </h2>
                  <p className="max-w-3xl text-sm leading-7 text-muted-foreground sm:text-base">
                    {heroBlurb}
                  </p>
                </div>
              </div>

              <div className="grid gap-2 sm:grid-cols-2 xl:min-w-[320px]">
                {identifierHighlights.map((item) => (
                  <IdentifierCard key={`${item.label}-${item.value}`} label={item.label} value={item.value} />
                ))}
              </div>
            </div>

            <div className="flex flex-wrap gap-2 text-sm text-muted-foreground">
              <InlineMeta icon={CarFront} value={renderQuery(result.query)} />
              <InlineMeta icon={Clock3} value={`Aktualizovano ${formatResolvedTime(result.query?.resolvedAt)}`} />
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

      <div className="grid gap-4 xl:grid-cols-[360px_minmax(0,1fr)]">
        <Card className="border-white/10 bg-card/80">
          <CardHeader className="pb-4">
            <CardDescription className="uppercase tracking-[0.22em] text-muted-foreground">
              Vlastnictvi
            </CardDescription>
            <CardTitle className="text-2xl text-white">Subjekty</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-3">
              <MiniStat label="Vlastnici" value={result.ownership?.ownerCount} />
              <MiniStat label="Provozovatele" value={result.ownership?.operatorCount} />
            </div>

            <div className="space-y-2.5">
              {parties.length > 0 ? (
                parties.map((party, index) => (
                  <CompactPartyCard party={party} key={`${party.role}-${party.name}-${index}`} />
                ))
              ) : (
                <EmptyPanel text="Pro tento dotaz nejsou dostupne zadne subjekty." />
              )}
            </div>
          </CardContent>
        </Card>

        <Card className="border-white/10 bg-card/78">
          <CardHeader className="pb-4">
            <CardDescription className="uppercase tracking-[0.22em] text-muted-foreground">
              Detaily
            </CardDescription>
            <CardTitle className="text-2xl text-white">Registrace a technicke udaje</CardTitle>
          </CardHeader>
          <CardContent className="space-y-6">
            {compactSections.length > 0 ? (
              compactSections.map((section) => (
                <div className="space-y-3" key={section.title}>
                  <div className="border-b border-white/8 pb-2">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.28em] text-muted-foreground">
                      {section.title}
                    </p>
                  </div>

                  <div className="grid gap-3 sm:grid-cols-2">
                    {(section.items || []).map((item) => (
                      <DetailRow item={item} key={`${section.title}-${item.label}`} />
                    ))}
                  </div>
                </div>
              ))
            ) : (
              <EmptyPanel text="Pro tento dotaz nejsou dostupne dalsi detaily." />
            )}
          </CardContent>
        </Card>
      </div>

      {inspections || inspectionLookup?.status === "pending" ? (
        <InspectionPanel inspections={inspections} inspectionLookup={inspectionLookup} />
      ) : null}

      {timelineEntries.length > 0 ? (
        <Card className="border-white/10 bg-card/72">
          <CardHeader className="pb-4">
            <CardDescription className="uppercase tracking-[0.22em] text-muted-foreground">
              Historie
            </CardDescription>
            <CardTitle className="text-2xl text-white">Casova osa</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2.5">
            {timelineEntries.map((entry, index) => (
              <div
                className="grid gap-3 rounded-[1.1rem] border border-white/8 bg-background/50 px-4 py-3 md:grid-cols-[160px_minmax(0,1fr)]"
                key={`${entry.title}-${index}`}
              >
                <p className="text-sm font-medium text-muted-foreground">{formatDate(entry.date)}</p>
                <div>
                  <p className="text-sm font-semibold text-white">{entry.title || "Udalost"}</p>
                  <p className="mt-1 text-sm leading-6 text-muted-foreground">
                    {entry.description || "Bez dalsiho detailu."}
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

function CompanyFleetState({ result }) {
  const records = Array.isArray(result.records) ? result.records : [];
  const companyName = result.company?.name || `Firma ${result.company?.ico || result.query?.normalized || ""}`;

  return (
    <div className="space-y-4">
      <Card className="border-white/10 bg-card/85 backdrop-blur-xl">
        <CardContent className="p-6 sm:p-8">
          <div className="space-y-5">
            <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
              <div className="space-y-3">
                <div className="flex flex-wrap gap-2">
                  <Badge>Pravnicka osoba</Badge>
                  <Badge variant="muted">Flotila dle ICO</Badge>
                </div>
                <div>
                  <h2 className="text-3xl font-semibold tracking-[-0.04em] text-white sm:text-5xl">
                    {companyName}
                  </h2>
                  <p className="mt-3 max-w-3xl text-sm leading-7 text-muted-foreground sm:text-base">
                    Seznam vozidel navazanych na ICO v otevrenych datech Registru silnicnich vozidel.
                  </p>
                </div>
              </div>

              <div className="grid gap-2 sm:grid-cols-2 xl:min-w-[320px]">
                <IdentifierCard label="ICO" value={result.company?.ico || result.query?.normalized || "-"} />
                <IdentifierCard label="Adresa" value={result.company?.address || "-"} />
              </div>
            </div>

            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
              <MiniStat label="Vozidla" value={result.summary?.vehicleCount} />
              <MiniStat label="Aktualni" value={result.summary?.currentVehicleCount} />
              <MiniStat label="Vztahy" value={result.summary?.relationshipCount} />
              <MiniStat label="Zobrazeno" value={result.summary?.displayedCount} />
            </div>

            <div className="flex flex-wrap gap-2 text-sm text-muted-foreground">
              <InlineMeta icon={Building2} value={`ICO | ${result.query?.normalized || "-"}`} />
              <InlineMeta icon={Clock3} value={`Aktualizovano ${formatResolvedTime(result.query?.resolvedAt)}`} />
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
            {records.length > 0 ? "Seznam vozidel" : "Bez vozidel"}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          {records.length > 0 ? (
            records.map((record, index) => (
              <CompanyVehicleRow key={`${record.pcv || record.vin || index}-${index}`} record={record} />
            ))
          ) : (
            <EmptyPanel text="Pro zadane ICO nebyla v otevrenych datech nalezena zadna vozidla." />
          )}
          {result.summary?.truncated ? (
            <p className="text-sm leading-7 text-muted-foreground">
              Vysledek byl zkracen na prvnich 200 vozidel, aby zustal rychly a citelny.
            </p>
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
}

function InspectionPanel({ inspections, inspectionLookup }) {
  if (!inspections) {
    return (
      <Card className="border-white/10 bg-card/78">
        <CardHeader className="pb-4">
          <div className="flex items-center justify-between gap-4">
            <div>
              <CardDescription className="uppercase tracking-[0.22em] text-muted-foreground">
                Kontroly
              </CardDescription>
              <CardTitle className="mt-2 text-2xl text-white sm:text-3xl">
                Technicke prohlidky
              </CardTitle>
            </div>

            <Badge variant="muted">Nacitam</Badge>
          </div>
        </CardHeader>

        <CardContent>
          <div className="rounded-[1.5rem] border border-white/10 bg-background/55 p-5">
            <div className="flex items-center gap-3">
              <Loader2 className="size-5 animate-spin text-muted-foreground" />
              <p className="text-sm leading-7 text-muted-foreground">
                Zakladni data vozidla uz jsou pripraveny. Technicke prohlidky se doctou na pozadi a objevi se tady automaticky.
              </p>
            </div>

            {(inspectionLookup?.vin || inspectionLookup?.pcv) ? (
              <div className="mt-4 flex flex-wrap gap-2">
                {inspectionLookup?.vin ? <Badge variant="muted">VIN {inspectionLookup.vin}</Badge> : null}
                {inspectionLookup?.pcv ? <Badge variant="muted">PČV {inspectionLookup.pcv}</Badge> : null}
              </div>
            ) : null}
          </div>
        </CardContent>
      </Card>
    );
  }

  const records = Array.isArray(inspections.records) ? inspections.records : [];
  const summary = inspections.summary || {};
  const current = summary.currentRecord || records[0] || null;
  const status = summary.status || "Nezjisteno";
  const tone = inspectionTone(status);
  const daysRemaining =
    typeof summary.daysRemaining === "number" ? formatDaysRemaining(summary.daysRemaining) : "Bez terminu";

  return (
    <Card className="border-white/10 bg-card/78">
      <CardHeader className="pb-4">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <CardDescription className="uppercase tracking-[0.22em] text-muted-foreground">
              Kontroly
            </CardDescription>
            <CardTitle className="mt-2 text-2xl text-white sm:text-3xl">
              Technicke prohlidky
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
                  Aktualni stav
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
                icon={MapPin}
                label="Stanice"
                value={current?.stationName || "Neuvedeno"}
              />
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-3 lg:grid-cols-1">
            <MiniStat label="Zaznamy" value={summary.totalCount ?? records.length} />
            <MiniStat label="Aktualni" value={summary.currentCount ?? 0} />
            <MiniStat
              label="PČV"
              value={inspections.pcv || "-"}
            />
          </div>
        </div>

        <div className="space-y-2.5">
          {records.slice(0, 8).map((record, index) => (
            <InspectionRow key={`${record.protocolNumber || record.validFrom || index}-${index}`} record={record} />
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

function InspectionRow({ record }) {
  return (
    <div className="grid gap-3 rounded-[1.1rem] border border-white/8 bg-background/50 px-4 py-3 md:grid-cols-[140px_minmax(0,1fr)_auto] md:items-center">
      <div className="space-y-1">
        <Badge variant={record.current ? "success" : "muted"} className="w-fit">
          {record.current ? "Aktualni" : "Historie"}
        </Badge>
        <p className="text-sm font-medium text-muted-foreground">
          {record.validFrom ? formatDate(record.validFrom) : "Neuvedeno"}
        </p>
      </div>

      <div className="min-w-0">
        <p className="text-sm font-semibold text-white">
          {[record.type, record.state].filter(Boolean).join(" · ") || "Technicka prohlidka"}
        </p>
        <p className="mt-1 text-sm leading-6 text-muted-foreground">
          {[record.stationName, record.protocolNumber ? `protokol ${record.protocolNumber}` : null]
            .filter(Boolean)
            .join(" · ") || "Bez doplnujicich detailu."}
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
        {item.label}
      </p>
      <p className="mt-2 text-base font-semibold text-white">{item.value}</p>
    </div>
  );
}

function IdentifierCard({ label, value }) {
  return (
    <div className="rounded-[1.25rem] border border-white/10 bg-background/55 px-4 py-4">
      <p className="text-[11px] font-semibold uppercase tracking-[0.3em] text-muted-foreground">
        {label}
      </p>
      <p className="mt-2 break-all text-base font-semibold tracking-[0.02em] text-white">
        {value || "-"}
      </p>
    </div>
  );
}

function InlineMeta({ icon: Icon, value }) {
  return (
    <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-background/45 px-3 py-2">
      <Icon className="size-3.5 text-muted-foreground" />
      <span className="text-sm text-muted-foreground">{value}</span>
    </div>
  );
}

function CompactPartyCard({ party }) {
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

      <p className="mt-3 text-sm font-semibold text-white">{party.name || "Bez nazvu"}</p>
      <p className="mt-1 text-sm leading-6 text-muted-foreground">{compactPartyMeta(party)}</p>
    </div>
  );
}

function DetailRow({ item }) {
  return (
    <div className="rounded-[1.1rem] border border-white/8 bg-background/50 px-4 py-3">
      <p className="text-[11px] font-semibold uppercase tracking-[0.24em] text-muted-foreground">
        {item.label}
      </p>
      <p className="mt-2 text-sm font-medium leading-6 text-white">{item.value}</p>
    </div>
  );
}

function CompanyVehicleRow({ record }) {
  const title = [record.make, record.model, record.type].filter(Boolean).join(" ").trim() || record.vin || record.pcv || "Vozidlo";

  return (
    <div className="grid gap-4 rounded-[1.1rem] border border-white/8 bg-background/50 px-4 py-4 xl:grid-cols-[minmax(0,1fr)_260px]">
      <div className="min-w-0">
        <div className="flex flex-wrap gap-2">
          {(record.relations || []).map((relation, index) => (
            <Badge key={`${relation.relation}-${index}`} variant={relation.current ? "success" : "muted"}>
              {relation.relation || "Subjekt"}{relation.current ? " · aktualni" : ""}
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
            .join(" · ") || "Bez dalsich technickych detailu."}
        </p>
      </div>

      <div className="grid gap-2 sm:grid-cols-2 xl:grid-cols-1">
        <IdentifierCard label="VIN" value={record.vin || "-"} />
        <IdentifierCard label="PCV" value={record.pcv || "-"} />
      </div>
    </div>
  );
}

function extractIdentifierHighlights(highlights) {
  const items = Array.isArray(highlights) ? highlights : [];
  const preferred = ["SPZ", "VIN"]
    .map((label) => items.find((item) => normalizeLabel(item.label) === label))
    .filter(Boolean);

  return preferred.length > 0 ? preferred : items.slice(0, 2);
}

function buildHeroBlurb(result, partyCount) {
  const snippets = [];
  const firstRegistration = getHighlightValue(result.highlights, "Prvni registrace");
  const fuel = getHighlightValue(result.highlights, "Palivo");

  if (firstRegistration) {
    snippets.push(`prvni registrace ${firstRegistration}`);
  }

  if (fuel) {
    snippets.push(`palivo ${String(fuel).toLowerCase()}`);
  }

  if (partyCount > 0) {
    snippets.push(`${partyCount} zaznamu v historii subjektu`);
  }

  return snippets.length > 0
    ? `Kompaktni prehled identifikace, registrace a vlastnictvi: ${snippets.join(" · ")}.`
    : "Kompaktni prehled identifikace, registrace a vlastnictvi.";
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

function compactPartyMeta(party) {
  return [
    party.ico ? `ICO ${party.ico}` : null,
    party.address || null,
    party.period || null,
    !party.period && party.since ? `od ${party.since}` : null
  ]
    .filter(Boolean)
    .join(" · ");
}

function ErrorState({ message, query, type }) {
  return (
    <Card className="border-white/10 bg-card/85 backdrop-blur-xl">
      <CardContent className="p-6 sm:p-8">
        <div className="flex flex-col gap-5 sm:flex-row sm:items-start sm:justify-between">
          <div className="space-y-4">
            <Badge variant="warning" className="w-fit">
              Bez vysledku
            </Badge>
            <div>
              <h2 className="text-3xl font-semibold tracking-[-0.04em] text-white sm:text-4xl">
                Pro tento identifikator zatim nic nemam
              </h2>
              <p className="mt-4 max-w-3xl text-sm leading-7 text-muted-foreground sm:text-base">
                {message}
              </p>
            </div>
          </div>

          <div className="rounded-[1.5rem] border border-white/10 bg-background/60 p-4 sm:min-w-[280px]">
            <div className="mb-3 flex items-center gap-2">
              <AlertCircle className="size-4 text-amber-200" />
              <p className="text-sm font-semibold text-white">Posledni dotaz</p>
            </div>
            <p className="text-sm leading-7 text-muted-foreground">
              {(type === "vin" ? "VIN" : type === "ico" ? "ICO" : "SPZ") + (query ? `: ${query}` : "")}
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
        {label}
      </p>
      <p className="mt-3 text-3xl font-semibold text-white">{value ?? "-"}</p>
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
          {label}
        </p>
        <p className="mt-1 text-sm leading-6 text-white">{value}</p>
      </div>
    </div>
  );
}

function detectType(value) {
  const compact = String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");

  if (/^\d{8}$/.test(compact) && isValidIco(compact)) {
    return "ico";
  }

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

function renderQuery(query) {
  if (!query) {
    return "Bez dotazu";
  }

  return `${query.type === "vin" ? "VIN" : query.type === "ico" ? "ICO" : "SPZ"} | ${query.raw || query.normalized || ""}`;
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

function formatDaysRemaining(value) {
  if (value < 0) {
    return `${Math.abs(value)} d po terminu`;
  }

  if (value === 0) {
    return "Konci dnes";
  }

  return `${value} d do konce`;
}

function inspectionTone(status) {
  const normalized = String(status || "").toLowerCase();

  if (normalized.includes("plat")) {
    return "success";
  }

  if (normalized.includes("konci") || normalized.includes("propad")) {
    return "warning";
  }

  return "muted";
}

function statusVariant(status) {
  const normalized = String(status || "").toLowerCase();

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
