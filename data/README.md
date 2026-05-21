# Carpeta `data/` — convención de paths

Este documento define cómo se organiza la carpeta `data/` del repositorio.
Cualquier scraper o loader nuevo debe respetar esta convención para que el
pipeline siga siendo coherente.

## Estructura

```
data/
├── raw/                              ← JSON crudos producidos por los scrapers
│   └── <comp_slug>/                  ← p.ej. la_liga, premier_league
│       └── <season>/                 ← p.ej. 2024_2025
│           └── <source>/             ← transfermarkt, sofascore, ...
│               ├── stadiums/<team_slug>.json
│               ├── players/<team_slug>.json
│               └── injuries/<team_slug>.json
│
├── clean/                            ← CSV "DB-ready" que leen los loaders
│   └── <comp_slug>/<season>/<source>/
│       ├── stadiums.csv
│       ├── players.csv
│       ├── injuries.csv
│       └── matches.csv
│
├── reference/                        ← CSVs de referencia (manuales, IDs)
│
├── exports/                          ← Outputs generados para usuarios
│
├── logs/                             ← Logs de ejecución del wizard
│
├── .cache/                           ← Cachés globales de scrapers
│   └── transfermarkt_stadiums_last_scraped.json
│
└── _old/                             ← Backup automático tras una migración
```

## Convenciones de naming

| Campo         | Formato                          | Helper                                  |
|---------------|----------------------------------|-----------------------------------------|
| `comp_slug`   | `snake_case` sin tildes          | `utils.data_paths.slugify_competition()` |
| `season`      | `YYYY_YYYY` (start + end)        | `utils.data_paths.normalize_season()`    |
| `source`      | minúsculas, una sola palabra     | —                                        |
| `team_slug`   | el que devuelve Transfermarkt    | —                                        |

Ejemplos válidos:

- `data/raw/la_liga/2024_2025/transfermarkt/stadiums/fc-barcelona.json`
- `data/clean/segunda_division/2025_2026/transfermarkt/stadiums.csv`
- `data/clean/premier_league/2024_2025/sofascore/matches.csv`

## Por qué esta jerarquía

- **`comp_slug` primero** — al trabajar en el día a día casi siempre piensas
  en una competición concreta. Tener todas las temporadas y fuentes de una
  liga a un solo `cd` de distancia es el optimo.
- **`season` antes que `source`** — facilita archivar temporadas viejas con
  un único `mv` y permite ver de un vistazo qué fuentes tienen datos de una
  temporada concreta.
- **`raw/` separado de `clean/`** — el patrón medallion: los crudos son la
  fuente de verdad y nunca se modifican, los limpios son derivados. Si algo
  está mal en un CSV, se regenera desde el JSON sin volver a scrapear.

## Cómo construir paths desde el código

Siempre usa los helpers de `utils.data_paths` — nunca concatenes strings:

```python
from utils.data_paths import raw_dir, clean_dir

# Escribir crudos
out = raw_dir("La Liga", "2024/2025", "transfermarkt", "stadiums")
(out / "fc-barcelona.json").write_text(...)

# Escribir CSV DB-ready
csv_dir = clean_dir("La Liga", "2024/2025", "transfermarkt")
df.to_csv(csv_dir / "stadiums.csv", index=False)
```

Los helpers se encargan de:

1. Normalizar `competition` → `comp_slug`.
2. Normalizar `season` → `YYYY_YYYY`.
3. Crear las rutas con el separador correcto del SO.

## Migración desde estructura antigua

Si tu carpeta `data/raw/` tiene la estructura vieja
(`data/raw/<source>/<comp>/season=<label>/...`), lanza:

```bash
# Plan de movimientos sin tocar nada
python -m scripts.reorganize_data --dry-run

# Aplicar (deja backup en data/_old/)
python -m scripts.reorganize_data --apply

# Aplicar sin backup
python -m scripts.reorganize_data --apply --no-backup
```

El script es **idempotente**: si lo lanzas dos veces sobre la misma carpeta,
la segunda no hace nada.

## Reglas para nuevos scrapers

Cuando añadas un scraper nuevo:

1. Importa `from utils.data_paths import raw_dir, clean_dir`.
2. Escribe los JSON crudos en `raw_dir(comp, season, source, subdir)`.
3. Escribe el CSV final en `clean_dir(comp, season, source) / "<fact>.csv"`.
4. **No** crees carpetas `batch_id=...` — incluye `batch_id` como campo en
   cada JSON crudo.
5. **No** uses `data/raw/<source>/` como primer nivel; usa la nueva jerarquía.

## Mejoras pendientes

- [ ] Migrar `sofascore_scraper.py`, `understat_scraper.py`,
      `whoscored_scraper.py`, `statsbomb_scraper.py` y `transfermarkt_scraper.py`
      a la nueva convención (el script de migración deja los datos colocados,
      falta actualizar el código que escribe).
- [ ] Añadir un `_manifest.json` por `<comp>/<season>/` con qué fuentes están
      completas y la última fecha de actualización.
- [ ] Considerar formato Parquet en `clean/` para ahorrar espacio en
      temporadas grandes.
