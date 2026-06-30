# Info.exleasing.cz

Jednostrankova aplikace pro vyhledani vozidla podle `SPZ` nebo `VIN`.

## Co je hotove

- moderni responzivni UI s jednim vstupnim polem
- vlastni Node server bez externich zavislosti
- `/api/lookup` endpoint pro napojeni na produkcni zdroj podle `SPZ` nebo `VIN`
- demo dataset pro rychle predvedeni
- obohaceni firemnich subjektu z ARES podle `ICO`
- firemni subjekty z registru maji prioritu, dalsi verejny prehled se pouzije jen jako fallback
- technicke prohlidky z otevrene datove sady RSV napojene pres `PČV`

## Spusteni

```bash
npm start
```

Aplikace pobezi na `http://localhost:3000`.

## Demo dotazy

- `1AB2345`
- `TMBJJ7NE8L0123456`
- `5AC5678`

## Produkcni napojeni

Zkopirujte `.env.example` do `.env` a doplnte:

- `DATAOVOZIDLECH_API_KEY` pro oficialni verejnou VIN API
- `DATABASE_URL` pro vlastni Postgres index nad otevrenymi daty
- `TRANSPORT_CUBE_LOOKUP_URL`
- pripadne `TRANSPORT_CUBE_API_KEY`
- nazvy parametru pro identifikator a jeho typ
- volitelne `UNIQA_LOOKUP_ENABLED=true` a `UNIQA_PHONE` pro opt-in SPZ->VIN fallback pres verejny formular UNIQA; telefonni cislo musi byt pod kontrolou provozovatele, protoze UNIQA u formulare uvadi mozny nasledny kontakt

## ALPR cteni SPZ z fotky

Upload fotky SPZ jde nejdriv pres ALPR provider a az potom pada na lokalni Tesseract fallback. Cloud varianta pouziva Plate Recognizer kompatibilni endpoint:

```bash
ALPR_PLATE_LOOKUP_ENABLED=true
ALPR_PROVIDER=plate-recognizer
PLATE_RECOGNIZER_API_TOKEN=...
PLATE_RECOGNIZER_REGIONS=cz
```

Pro self-host variantu lze spustit lokalni FastALPR wrapper:

```bash
python -m pip install -r requirements-alpr.txt
npm run alpr:local
```

V druhem terminalu pak spustte web s:

```bash
ALPR_PROVIDER=local
LOCAL_ALPR_API_URL=http://127.0.0.1:8080/v1/plate-reader/
npm start
```

`ALPR_PROVIDER=auto` pouzije lokalni ALPR, pokud je nastaveny `LOCAL_ALPR_API_URL`, a Plate Recognizer cloud, pokud je nastaveny `PLATE_RECOGNIZER_API_TOKEN`. Kdyz ALPR nevrati bezpecny vysledek, backend zkusi puvodni OCR fallback.

Pokud je nastaveno `DATAOVOZIDLECH_API_KEY`, backend zkusi pro `VIN` nejdriv oficialni endpoint:

`https://api.dataovozidlech.cz/api/vehicletechnicaldata/v2/{vin}`

Tento zdroj typicky vrati technicke udaje a pocty vlastniku/provozovatelu, ale ne jmena subjektu.

Pokud produkcni napojeni vrati u pravnickych osob `ICO`, aplikace primarne doplni obchodni jmeno a adresu z ARES. Dalsi verejny prehled se pouzije az jako fallback, kdyz z registru neprijde pouzitelny subjekt.

Technicke prohlidky se doplnuji z otevrene datove sady `Technicke prohlidky k vozidlum`. Vazba probiha pres `PČV`, ktere se dohledava z hlavni otevrene sady `Vozidla - technicke udaje` podle `VIN`. Obe sady jsou velke, takze prvni dohledani muze byt citelne pomalejsi, dalsi dotazy uz vyuzivaji lokalni pametovou cache.

Fleet vypisy podle `ICO` ctou technicka data z DB fact tabulek `vehicle_fleet_facts` a `company_vehicle_facts`. Prvni sjednocuje `vehicles`, `vehicle_plate_links`/`plate_resolutions` a `vehicle_inspection_summaries`; druha predpocita vazby firem na vozidla, aby request pro flotilu nedelal opakovane velke joiny nad ownership tabulkami.

`npm run db:migrate` standardne obnovi rychlou SPZ projekci `vehicle_plate_summaries` a navazujici fact tabulky. Plny rebuild STK souhrnu nad desitkami milionu kontrol se spousti jen explicitne pres `MIGRATE_REFRESH_INSPECTIONS=true npm run db:migrate`; import otevrenych dat ho obnovuje po nacteni inspekci automaticky.

## Postgres index otevřených dat

Produkce má pro `IČO` vyhledávání používat Postgres index. Backend ho zapne automaticky, pokud je nastavené `DATABASE_URL`; runtime CSV sken velkých otevřených dat je kvůli Railway limitům vypnutý a zapíná se jen explicitně přes `ALLOW_RUNTIME_OPEN_DATA_ICO_SCAN=true`. Souborový `fleet-db` je jen fallback: `FLEET_DB_FALLBACK_ENABLED=auto` ho použije bez `DATABASE_URL`, `true` ho vynutí a `false` vypne.

```bash
npm run db:migrate
npm run import:open-data
npm run import:open-data:full
npm run db:index-company-names
npm run db:status
npm run db:audit-storage
npm run db:cleanup-import
npm run db:prune-cache
npm run benchmark:endpoints
```

Import načte poslední CSV z `download.dataovozidlech.cz`, uloží data do staging tabulek a po validaci přepne produkční tabulku každého zdroje samostatně. Core lookup používá normalizované tabulky `vehicles`, `vehicle_vins`, `ownership_relations` a `inspections`; menší sady `imports` a `deregistered` mají vlastní tabulky podle `PČV`. Široké doplňkové JSONB sady `equipment` a `manufacturer_reports` jsou vypnuté, dokud nenastavíte `OPEN_DATA_IMPORT_AUX_ENABLED=true`.

Oficiální dataset vlastníků/provozovatelů často obsahuje právnickou osobu jen ve sloupci `Název`, ale bez `IČO`. IČO lookup proto dělá dvě indexované cesty: přímé `ownership_relations.ico = IČO` a fallback `IČO -> ARES obchodní jméno -> lower(ownership_relations.name)`, pouze pro řádky s prázdným `ico`. Pro druhou cestu slouží partial indexy `ownership_relations_missing_ico_name_*`.

Výchozí profil je úsporný pro Railway Hobby: `OPEN_DATA_IMPORT_SOURCES=ownership,vehicles`, ale `vehicles` importuje jen malý `vehicle_vins` index (`OPEN_DATA_IMPORT_VEHICLES_VIN_INDEX_ONLY=true`). Technická data vozidla se berou přes veřejné VIN API, takže v DB není potřeba držet plný 16GB výpis vozidel ani STK tabulky. Vlastnické vazby berou celou historii zobrazitelných právnických osob (`OPEN_DATA_IMPORT_OWNERSHIP_HISTORY_SCOPE=legal-history`) a nevyžadují `IČO`, protože část právnických osob je ve zdroji jen podle názvu. Lean rebuild používá `OPEN_DATA_IMPORT_OWNERSHIP_DESTRUCTIVE_REPLACE=true`: zahodí aktuální `ownership_relations`, nahradí ji přímo a přeskočí velké fact tabulky.

Kontrola lean importních větví bez DB:

```bash
npm run test:lean-import
```

Lokální plná databáze se spouští přes:

```bash
npm run db:migrate
npm run import:open-data:full
```

Tento profil importuje core sady `ownership,vehicles,inspections,deregistered,imports`, nechává plnou historii vlastnictví a zapíná plné VIN mapování. Pokud chcete i JSONB doplňkové sady, přidejte `OPEN_DATA_IMPORT_AUX_ENABLED=true`. API pro `IČO` standardně vrací nejvýše `ICO_FLEET_MAX_RECORDS=200` vozidel a u větších flotil označí výsledek jako zkrácený. Pokud se filename od poslední aktivní verze nezměnil, import jen aktualizuje `last_checked_at` a skončí.

`npm run db:audit-storage` vypíše velikosti Postgres tabulek/indexů a lokální `.cache/open-data`. `npm run db:prune-cache` je dry-run; destruktivní mazání `.tmp`, starých datasetů a gzip kompresi spustíte až přes `npm run db:prune-cache -- --apply`. Importer umí číst i ponechané `.csv.gz` soubory.

`GET /api/health` a `npm run db:status` ukážou aktivní verze datasetů, počty řádků a čas posledního importu. Pro rychlé zobrazení právnických osob po dotazu musí být aktivní minimálně `ownership`; pro dohledání `PČV` podle `VIN` musí být aktivní i `vehicles`.

## Lookup audit a statistiky

Backend ukládá serverový audit lookup requestů do Postgres tabulky `lookup_events`, pokud je nastavené `DATABASE_URL` a `LOOKUP_AUDIT_ENABLED=true`. Záznam obsahuje endpoint, typ dotazu, stav odpovědi, dobu trvání, souhrn nalezeného vozidla nebo flotily a stav navazujících kontrol. Přesné identifikátory `SPZ/VIN/PČV/IČO` se ukládají, pokud je `LOOKUP_AUDIT_STORE_IDENTIFIERS=true`; při vypnutí zůstane jen maskovaná hodnota a hash.

Admin statistiky jsou dostupné na:

```bash
curl -H "Authorization: Bearer $LOOKUP_ADMIN_TOKEN" "https://vase-domena/api/admin/lookup-stats?days=30&limit=50"
```

Endpoint vrací souhrny podle dnů, typu dotazu, endpointu, nejčastějších dotazů/vozidel a poslední události. Bez `LOOKUP_ADMIN_TOKEN` endpoint data nevrací. Minimalní cookie banner ve frontendu ukládá pouze volbu souhlasu; anonymní `autoinfo_analytics_session` cookie vznikne jen po volbě analytiky.

Na Railway vytvořte samostatnou import service se stejným repozitářem. Pokud máte v Railway dostupný cron schedule, může service po jednom importu skončit se start command:

```bash
npm run cron:open-data
```

Alternativně může cron service použít stejné `npm start` jako web, pokud má nastaveno `SERVICE_MODE=open-data-cron`. Doporučený schedule je `30 2 * * *` UTC. Service musí po importu skončit; Railway jinak další cron běh přeskočí. Web service i cron service mají mít referenci na stejné Postgres `DATABASE_URL`.

Pokud cron schedule nejde nastavit přes CLI nebo API, jde použít dlouho běžící worker, ale na Railway Hobby je méně vhodný než nativní cron, protože běží pořád:

```bash
SERVICE_MODE=open-data-cron
OPEN_DATA_IMPORT_INTERVAL_HOURS=24
OPEN_DATA_IMPORT_RUN_ON_START=true
OPEN_DATA_IMPORT_EXIT_ON_ERROR=false
```

Worker spustí import při startu, potom čeká nastavený interval a při dalších bězích jen zkontroluje, jestli se na zdroji změnil filename. Pokud je zdroj stále stejný, jen aktualizuje `last_checked_at` a nestahuje velké CSV znovu.

Backend umi dve varianty:

1. `GET` provider: posle identifikator v query stringu
2. `POST` provider: posle JSON payload s identifikatorem

Pokud vas provider vraci uz normalizovanou strukturu s `hero`, `sections` a `ownership`, UI ji vezme rovnou. Jinak se pouzije genericky normalizer s mapovanim beznych nazvu poli.

### Import SPZ vazeb

Otevrena RSV data aktualne neobsahuji samotnou registracni znacku, jen typ/variantu RZ. Importer umi ulozit `SPZ`/`RZ` primo do `vehicles.plate`, pokud ji budouci nebo interni feed obsahuje; bez tohoto sloupce bere fleet vypis podle `ICO` SPZ z normalizovane tabulky `vehicle_plate_links`, ktera je navazana na `vehicles.pcv` a/nebo `vehicle_vins.vin`. Pokud mate interni nebo placeny feed `PCV/VIN -> SPZ`, nahrajte ho pres:

```bash
npm run db:import-plates -- --file ./plates.csv --source provider-name
```

Import lze rovnou svazat s coverage branou pro konkretni firmu. Pokud po importu zustane u aktualnich vozidel daneho `ICO` chybejici `SPZ` nebo STK, import se rollbackne:

```bash
npm run db:import-plates -- --file ./plates.csv --source provider-name --require-ico 06649114
```

Podporovane vstupy jsou CSV, JSON a JSONL. Rozpoznane sloupce jsou napr. `pcv`, `pcv vozidla`, `vin`, `spz`, `rz`, `plate`, `registrationPlateNumber`. Script validuje identifikatory, dohleda existujici `PCV` pres `vehicles`/`vehicle_vins`, ulozi vazbu do `vehicle_plate_links` i legacy cache `plate_resolutions`, obnovi `vehicle_plate_summaries` a `ICO` endpoint ji zacne vracet bez dalsi zmeny frontendu. Suchy beh:

```bash
npm run db:import-plates -- --file ./plates.csv --source provider-name --dry-run
```

### E2E kontrola SPZ a fotky

Zakladni E2E overi rychly `SPZ -> VIN` lookup, sdilenou stranku `/spz/...` a upload fotky SPZ:

```bash
npm run test:e2e:spz-photo
```

Vychozi cil je produkce `https://spz.up.railway.app`; lokalni server lze otestovat pres:

```bash
E2E_BASE_URL=http://127.0.0.1:3000 npm run test:e2e:spz-photo
```

Na Windows PowerShellu:

```powershell
$env:E2E_REQUIRE_ALL_VISIBLE_PLATES='1'; $env:E2E_REQUIRE_ALL_VISIBLE_STK='1'; npm run test:e2e:ico
```

Stejne coverage metriky jsou i v DB diagnostice. Pro konkretni firmu:

```powershell
$env:STATUS_ICO='06649114'; npm run db:status
```

Pokud diagnostika ukaze chybejici `SPZ`, lze vyexportovat sablonu pro doplneni realneho `VIN/PČV -> SPZ` zdroje. Vychozi export bere aktualni vozidla; historii lze pribrat pres `--include-history`:

```powershell
npm run db:missing-plates -- --ico 06649114 --output missing-plates.csv
```

Po doplneni sloupce `spz` se stejny soubor importuje pres `npm run db:import-plates -- --file missing-plates.csv --source provider-name --require-ico 06649114`.

Pokud ma prostredi nakonfigurovany externi zdroj pro dohledani SPZ podle VIN, lze nejdriv bezpecne vypsat kandidaty. Vychozi backfill bere aktualni vozidla; historii lze pribrat pres `--include-history`:

```powershell
npm run db:backfill-plates -- --ico 06649114 --dry-run
```

Skutecne externi dohledani se spousti jen s explicitnim potvrzenim, protoze odesila VIN/PČV do nakonfigurovanych zdroju a nalezene SPZ uklada do `vehicle_plate_links`, `plate_resolutions` a `vehicle_plate_summaries`:

```powershell
npm run db:backfill-plates -- --ico 06649114 --limit 20 --confirm-external-lookup
```

## Railway diagnostika

- `.env` se nedeployuje z GitHubu, takze vsechny potrebne promenne nastavte v `Railway > Variables`.
- `GET /api/health` ted vraci i bezpecny souhrn lookup konfigurace bez secretu.
- Pri neuspesnem `/api/lookup` se do odpovedi i logu vypise, jestli chybi `TRANSPORT_CUBE_LOOKUP_URL`, `DATAOVOZIDLECH_API_KEY`, nebo selhalo volani provideru.
- Startup log ted obsahuje radek `[startup] lookup runtime ...`, ktery hned po deployi ukaze, co je na serveru skutecne nakonfigurovano.
- Na Railway nema smysl nastavovat `PVZP_BROWSER_PATH`, pokud staci automaticky nalezeny Playwright nebo systemovy Chromium browser.

## Poznamka k datum

Verejne dostupna cast Datove kostky zjevne neresi cely pozadovany rozsah `SPZ + plne nazvy majitelu`, takze je aplikace pripravena na doplneni interniho nebo neverejneho zdroje bez zmeny frontendu.
