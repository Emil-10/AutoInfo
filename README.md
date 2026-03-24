# Info.exleasing.cz

Jednostrankova aplikace pro vyhledani vozidla podle `SPZ` nebo `VIN`.

## Co je hotove

- moderni responzivni UI s jednim vstupnim polem
- vlastni Node server bez externich zavislosti
- `/api/lookup` endpoint pro napojeni na produkcni zdroj
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
- `TRANSPORT_CUBE_LOOKUP_URL`
- pripadne `TRANSPORT_CUBE_API_KEY`
- nazvy parametru pro identifikator a jeho typ

Pokud je nastaveno `DATAOVOZIDLECH_API_KEY`, backend zkusi pro `VIN` nejdriv oficialni endpoint:

`https://api.dataovozidlech.cz/api/vehicletechnicaldata/v2/{vin}`

Tento zdroj typicky vrati technicke udaje a pocty vlastniku/provozovatelu, ale ne jmena subjektu.

Pokud produkcni napojeni vrati u pravnickych osob `ICO`, aplikace primarne doplni obchodni jmeno a adresu z ARES. Dalsi verejny prehled se pouzije az jako fallback, kdyz z registru neprijde pouzitelny subjekt.

Technicke prohlidky se doplnuji z otevrene datove sady `Technicke prohlidky k vozidlum`. Vazba probiha pres `PČV`, ktere se dohledava z hlavni otevrene sady `Vozidla - technicke udaje` podle `VIN`. Obe sady jsou velke, takze prvni dohledani muze byt citelne pomalejsi, dalsi dotazy uz vyuzivaji lokalni pametovou cache.

Backend umi dve varianty:

1. `GET` provider: posle identifikator v query stringu
2. `POST` provider: posle JSON payload s identifikatorem

Pokud vas provider vraci uz normalizovanou strukturu s `hero`, `sections` a `ownership`, UI ji vezme rovnou. Jinak se pouzije genericky normalizer s mapovanim beznych nazvu poli.

## Poznamka k datum

Verejne dostupna cast Datove kostky zjevne neresi cely pozadovany rozsah `SPZ + plne nazvy majitelu`, takze je aplikace pripravena na doplneni interniho nebo neverejneho zdroje bez zmeny frontendu.
