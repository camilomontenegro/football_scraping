# Football Scraping Wizard

## 🐳 Inicio rápido con Docker

Requisitos: **Docker Desktop** instalado y corriendo.

```powershell
# 1. Clonar el repositorio
git clone -b noelia/docker https://github.com/camilomontenegro/football_scraping.git
cd football_scraping

# 2. Configurar variables de entorno
# Windows:
copy .env.example .env
notepad .env
# Mac/Linux:
# cp .env.example .env && nano .env
# Rellena DB_PASSWORD con tu contraseña de PostgreSQL

# 3. Restaurar la base de datos
docker compose up db
# Espera a ver: "database system is ready to accept connections"
# Abre otra terminal:
# Coloca el archivo .dump recibido dentro de db/migrations/ (puede estar en una subcarpeta)
# Sustituye <nombre_del_dump> por la ruta relativa al archivo (ej: football_db_final.dump o subcarpeta/archivo.dump)
docker cp "db/migrations/<nombre_del_dump>" football_postgres_db:/tmp/football_db_backup.dump
docker exec football_postgres_db pg_restore -U postgres -d football_db --clean /tmp/football_db_backup.dump
# Nota: es normal ver ~150 warnings "errors ignored on restore" la primera vez — no es un error

# 4. Levantar todo el proyecto
docker compose up
```

Accede al dashboard en: **http://localhost:8501**

> **Nota:** Las credenciales de Cloudinary solo son necesarias si se quieren volver a ejecutar los scrapers de fotos de jugadores.

---

Pipeline ETL de fútbol que combina varios scrapers (WhoScored, SofaScore, Understat, Transfermarkt, StatsBomb) en una única base de datos PostgreSQL, con un wizard interactivo y soporte multi‑competición (LaLiga, Bundesliga, Premier League, Champions, Mundial, etc.).

---

## ⚡ Inicio rápido — comandos en orden

Asumiendo Windows + PowerShell, Python 3.12 y PostgreSQL ya instalado y arrancado.

```powershell
# 1) Posicionarse en la raíz del proyecto
cd football_scraping

# 2) Crear y activar el entorno virtual
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 3) Instalar dependencias
pip install -r requirements.txt
# Si falta playwright/selenium driver, además:
pip install statsbombpy  # si vas a usar StatsBomb

# 4) Configurar variables de entorno (BD, etc.)
copy .env.example .env
notepad .env             # rellena DB_HOST/PORT/NAME/USER/PASSWORD

# 5) Crear/recrear el schema en PostgreSQL
python db/setup_db.py

# 6) Lanzar el wizard interactivo
python -m wizard.wizard

# 7) Lanzar el dashboard Streamlit
streamlit run dashboard/app.py
```

A partir del wizard se elige acción (descargar / actualizar), competición, temporada y fuente(s). Internamente lanza el scraping y la carga a BD.

> **Nota sobre compatibilidad:** Aunque el wizard principal se ha movido a `wizard/wizard.py`, el comando `python -m scripts.wizard` seguirá funcionando gracias a un *wrapper* de compatibilidad. Se recomienda usar la nueva ruta para mayor claridad.

---

## Reset completo de base de datos

Si hay datos mal insertados y quieres eliminar la base configurada en `.env`:

```powershell
# Borra la base completa indicada por DB_NAME
python -m db.drop_database --yes

# La vuelve a crear, recrea todas las tablas y siembra dim_competition
python db/setup_db.py
```

Si sólo quieres vaciar/recrear tablas sin borrar la base PostgreSQL, basta con:

```powershell
python db/setup_db.py
```

`setup_db.py` ejecuta `db/create_tables.sql`, que hace `DROP TABLE ... CASCADE`
de las tablas del proyecto antes de recrearlas.

---

## Dashboard Streamlit

El repositorio incluye un dashboard en `dashboard/` para explorar y monitorizar la BD:

```powershell
streamlit run dashboard/app.py
```

Pestañas principales:

- `Exploration`: resultados, jugadores, tiros por fuente y eventos.
- `Teams`, `Goalkeepers`, `Players`, `Injuries`: vistas analíticas por competición, temporada y equipo.
- `Shot Intelligence`: mapa de peligro, finishing y especialistas a balón parado.
- `Pipeline monitoring`: métricas de BD, coverage por fuente, cola de revisión de jugadores y últimos partidos.
- `Wizard`: ejecuta el pipeline desde Streamlit y guarda el log en `data/logs/wizard_latest_log.txt`.

Salvo la pestaña `Wizard`, el dashboard sólo lee de la base de datos.

---

## Estadios SCD2

La descarga de estadios de Transfermarkt se carga en `dim_stadium` como
modelo SCD2: re-scrapear una temporada actualiza o abre versiones según los
cambios detectados, pero no reconstruye por sí solo todo el historial. Para
materializar nombres anteriores publicados en el campo `Antes:` y compactar
versiones contiguas, usa:

```powershell
python -m scripts.bootstrap_dim_stadium
python -m scripts.compact_dim_stadium
```

El enriquecimiento Wikidata es opcional y se ejecuta bajo demanda:

```powershell
python -m scrapers.wikidata_stadium_enricher --limit 20
python -m wizard.pipeline_runner --enrich-wikidata
```

---

## 🛠 Comandos útiles habituales

```powershell
# Diagnóstico rápido del estado de la BD
python -m scripts.inspect_db
python -m scripts.inspect_db -c "Bundesliga" -s 2025/2026

# Normalizar formatos heterogéneos de season en BD
python -m scripts.normalize_db_seasons --dry-run     # preview
python -m scripts.normalize_db_seasons               # aplica

# Rellenar match_date de partidos cargados sin fecha (WhoScored)
python -m scripts.backfill_match_dates --limit 20    # prueba
python -m scripts.backfill_match_dates               # todos

# Cargar dimensiones / facts manualmente (sin scrape)
python -m scripts.load_dimensions --all
python -m scripts.load_facts --all

# Pipeline completo en una línea (sin wizard)
python -m wizard.pipeline_runner --scrape -c "Bundesliga" -s 2025/2026
python -m wizard.pipeline_runner --scrape -c "Bundesliga" -s 2025/2026 --source whoscored
python -m wizard.pipeline_runner --update  -c "Bundesliga" -s 2025/2026  # incremental

# Scraper suelto (sin pasar por el pipeline)
python -m scrapers.whoscored_scraper -c "FIFA World Cup" -s 2026
python -m scrapers.understat_scraper --competition Bundesliga --seasons 2025
```

---

## 📁 Estructura del proyecto

```
football_scraping_wizard/
├── db/
│   ├── setup_db.py              # crea BD y schema (DROP+CREATE idempotente)
│   ├── create_tables.sql        # schema PostgreSQL
│   └── normalize_seasons.sql    # script SQL de limpieza/normalización
├── scrapers/                    # uno por fuente externa
│   ├── whoscored_scraper.py     # multi-stage genérico (ligas + Mundial/EURO)
│   ├── sofascore_scraper.py     # API JSON (Osen)
│   ├── understat_scraper.py     # JSON + HTML, ligas europeas (Osen)
│   ├── statsbomb_scraper.py     # statsbombpy Open Data (Osen)
│   ├── transfermarkt_scraper.py # plantillas + lesiones (Osen)
│   └── base_extractor.py        # helpers JSON
├── loaders/                     # CSV → PostgreSQL
│   ├── team_loader.py           # dim_team
│   ├── player_loader.py         # dim_player + player_review
│   ├── match_loader.py          # dim_match
│   ├── fact_loader.py           # fact_shots / fact_events / fact_injuries
│   ├── team_loader_generico.py     # versión multi-liga (globs **/*teams*.csv)
│   ├── player_loader_generico.py   # versión multi-liga (globs **/*players*.csv)
│   ├── match_loader_generico.py    # versión multi-liga
│   ├── fact_loader_generico.py     # multi-liga + normaliza coordenadas a 0-1
│   ├── competition_loader.py       # dim_competition
│   ├── champions_loader.py         # orquestador Champions League
│   ├── la_liga_loader.py           # orquestador LaLiga
│   ├── ligue1_loader.py            # orquestador Ligue 1
│   └── premier_league_loader.py    # orquestador Premier League
├── wizard/                      # Componentes principales del wizard y pipeline
│   ├── wizard.py                # menú interactivo principal
│   ├── pipeline_runner.py       # orquestador no interactivo del pipeline
│   ├── competitions.py          # IDs de cada fuente por competición
│   └── __init__.py              # para que Python lo reconozca como paquete
├── scripts/                     # Scripts de utilidad y wrappers de compatibilidad
│   ├── wizard.py                # Wrapper para python -m scripts.wizard
│   ├── pipeline_runner.py       # Wrapper para python -m scripts.pipeline_runner
│   ├── competitions.py          # Wrapper para imports de scripts.competitions
│   ├── __init__.py              # Re-exporta componentes de wizard/ para compatibilidad
│   ├── load_dimensions.py       # carga manual de dim_*
│   ├── load_facts.py            # carga manual de fact_*
│   ├── inspect_db.py            # diagnóstico de BD
│   ├── normalize_db_seasons.py  # normalización retroactiva de season
│   └── backfill_match_dates.py  # rellena match_date desde WhoScored
├── utils/
│   ├── season_utils.py             # normalize_season() canónico
│   ├── canonical_teams.py          # alias de nombres de equipo (218+ aliases)
│   ├── coordinate_normalization.py # normaliza x,y al rango 0-1 (mezcla 0-100 / 0-1)
│   ├── mdm_engine.py               # resolve_player / resolve_or_create_player
│   └── ...
├── data/raw/<fuente>/...        # CSVs descargados por los scrapers
├── requirements.txt
├── .env.example
└── README.md
```

---

## 🌍 Competiciones soportadas

Definidas en `wizard/competitions.py`. Cada una mapea su ID interno por fuente.

**Ligas nacionales**
LaLiga · Segunda División · Premier League · Championship · Bundesliga ·
Serie A · Ligue 1 · Primeira Liga · Eredivisie

**Continentales (Europa)**
Champions · Europa League · Europa Conference League · European Championship ·
UEFA Women's EURO · UEFA Nations League A/B/C/D · World Cup Qualification UEFA

**Selecciones / Internacional**
FIFA World Cup · Copa America · Africa Cup of Nations · Asian Cup ·
FIFA Women's World Cup · FIFA Club World Cup · World Cup Qualification CONMEBOL · Int. Friendly

Para añadir una temporada nueva en WhoScored basta con añadir una entrada en
`WHOSCORED_STAGES` de `scrapers/whoscored_scraper.py` con `(season_id, [stage_ids])`.

---

## 🔁 Flujos típicos

### 1. Primera carga de una nueva temporada (Bundesliga 2025/26)

```powershell
python db/setup_db.py
python -m wizard.wizard
# → 1 (descargar) → Bundesliga → 2025/2026 → all → todos los partidos → S
```

El wizard ejecuta scraping + carga en BD. Tarda ~90 min para 306 partidos por
las pausas anti‑bot de WhoScored.

### 2. Actualizar con partidos nuevos

```powershell
python -m wizard.wizard
# → 2 (actualizar) → Bundesliga → 2025/2026 → ...
```

Lee la última `match_date` en BD y descarga solo lo posterior. Para que esto
funcione, los partidos en `dim_match` deben tener `match_date` rellenado
(usa `backfill_match_dates` si falta).

### 3. Solo cargar (los CSVs ya existen)

```powershell
python -m scripts.load_dimensions --all
python -m scripts.load_facts --all
```

### 4. Verificar qué hay en la BD

```powershell
python -m scripts.inspect_db -c "Bundesliga" -s 2025/2026
```

---

## 🧹 Convenciones internas

- **`season`** se guarda siempre como `\'YYYY/YYYY\'` (`\'2025/2026\'`).
  La función única de normalización es `utils.season_utils.normalize_season()`.
- **`competition`** se guarda con el `name` de `wizard/competitions.py`
  (`"LaLiga"`, `"Bundesliga"`, …).
- **CSVs por fuente y liga**: cada scraper escribe en `data/raw/<fuente>/`
  con sufijo de slug (`whoscored_teams_<slug>.csv` plano) o estructura
  jerárquica (`<fuente>/<comp_slug>/season=YYYY/...csv`).
- **Convención de PKs**: `dim_team.canonical_id`, `dim_player.canonical_id`,
  `dim_match.match_id`. Cada dim tiene además `id_<fuente>` para cruzar.

---

## 🩹 Troubleshooting

**`Understat` devuelve 404 para shotmaps**
→ Corregido: el scraper ahora omite los `404` de shotmaps inexistentes sin marcar el scraping como fallido. Esto ocurre cuando Understat aún no ha publicado los datos de tiros para un partido específico.

**`SofaScore` devuelve 403 Forbidden o un challenge anti-bot**
→ Corregido a nivel de código: el scraper de SofaScore ahora intenta primero una petición HTTP con *fingerprint* TLS de navegador (`curl_cffi`). Si falla, recurre a Selenium con un navegador Chrome real. Sin embargo, en algunos entornos (especialmente servidores o IPs con mala reputación), SofaScore puede seguir bloqueando el acceso.

  **Posibles soluciones si el bloqueo persiste:**
  *   **Usar un proxy residencial/sticky:** Define la variable de entorno `SOFASCORE_PROXY` (o `HTTPS_PROXY`/`HTTP_PROXY`) con la URL de un proxy de alta calidad antes de ejecutar el pipeline. Ejemplo: `SOFASCORE_PROXY="http://user:pass@ip:port" python -m wizard.wizard`.
  *   **Ejecutar en un entorno local con navegador:** El fallback a Selenium es más efectivo en máquinas con un navegador Chrome instalado y una IP residencial.

**"Error en el SQL: error de sintaxis en o cerca de «CREATE»" al lanzar `setup_db.py`**
→ Ya parcheado: el `create_tables.sql` lleva el `;` correcto y `DROP TABLE IF EXISTS`. Asegúrate de tener la última versión.

**"`\'list\' object has no attribute \'items\'`" al scrapear con WhoScored**
→ Reparado tras refactor multi-stage. Si vuelves a verlo, es que el archivo se quedó truncado: relanza `python -m py_compile scrapers/whoscored_scraper.py` para detectarlo.

**Wizard "Actualizar" dice "no se encontraron partidos en BD" pero sí los hay**
→ Causa habitual: `match_date` está NULL o `season` está en formato distinto a `\'YYYY/YYYY\'`. Solución:
```powershell
python -m scripts.normalize_db_seasons   # unifica seasons
python -m scripts.backfill_match_dates   # rellena fechas desde WhoScored
```

**`fact_events` cuelga con cientos de miles de filas**
→ Ya optimizado: el loader cachea las FKs en memoria (matches/players/teams) y hace inserts por lotes de 5000. Debería tardar pocos minutos para 500K eventos.

**Null bytes / archivos truncados**
→ Si compilas y sale `ValueError: source code string cannot contain null bytes`:
```powershell
python -c "
for f in [r\'loaders\\player_loader.py\', r\'loaders\\match_loader.py\', r\'loaders\\team_loader.py\', r\'loaders\\fact_loader.py\']:
    d = open(f,\'rb\').read()
    if b\'\\x00\' in d:
        open(f,\'wb\').write(d.replace(b\'\\x00\', b\'\'))
        print(f, \'limpiado\')
\"
```

**El backfill de fechas falla por "matchCentreData no encontrado"**
→ WhoScored detectó scraping. Espera 10‑15 min y reintenta; el scraper ya tiene anti‑bot (reinicio de driver + pausas exponenciales).

---

## ✏️ Notas de diseño

- **Whoscored multi‑stage**: torneos como el Mundial tienen 12 grupos + final. El scraper itera por todas las stages registradas en `WHOSCORED_STAGES[(comp, season)][\'stages\']` y agrega los eventos.
- **Sin Transfermarkt como master obligatorio**: `resolve_or_create_player()` crea jugadores canónicos desde Understat/WhoScored cuando TM no está disponible (antes todos iban a `player_review` y `dim_player` quedaba vacía).
- **Dos convenciones de output**: la antigua (`whoscored_teams_<slug>.csv` plano) y la nueva de Osen (`<fuente>/<comp_slug>/season=YYYY/...csv`). Los loaders soportan ambas con `glob` recursivo.
- **MDM (Master Data Management)** en `utils/mdm_engine.py`: `resolve_player()` busca por id de fuente → nombre exacto → fuzzy. Los no resueltos van a `player_review` para revisión manual.

---

## 📚 Documentos adicionales

- `DESCARGA_GUIA.md` — guía detallada del proceso de descarga.
- `FLUJO_GRANULAR.md` — flujo paso a paso del pipeline.
- `PLAYER_REVIEW_GUIDE.md` — cómo resolver entradas de `player_review`.

---

## 🔀 Notas del merge (rama database-loader)

Este repo `football_scraping_wizard` integra cambios del trabajo de Álvaro
(rama `database-loader`, snapshot 13-may) sobre la base `unified` (19-may):

- **Globs genéricos en los `*_loader_generico.py`** — antes los loaders solo
  reconocían `understat_players_laliga.csv` (hardcoded). Ahora usan
  `**/*players*.csv`, `**/*teams*.csv`, `**/*matches*.csv` y funcionan para
  cualquier liga sin renombrar ficheros.
- **`utils/coordinate_normalization.py`** — todas las coordenadas x,y de tiros
  y eventos se normalizan al rango 0-1 en el loader. SofaScore y WhoScored
  llegan en 0-100, Understat en 0-1; antes se mezclaban en BD.
- **Loaders por liga** — `la_liga_loader.py`, `ligue1_loader.py`,
  `premier_league_loader.py` (mismo patrón que `champions_loader.py`).
- **`champions_loader.py` con `engine.begin()` por operación** — cada carga
  de dimensión/hecho abre su propia transacción para mejor aislamiento.
- **+49 alias nuevos en `canonical_teams.py`** — equipos de Premier League
  (Bournemouth, Brentford, Wolves…) y Ligue 1 (Lyon, Marseille, Lens…).

⚠️ **Antes de recargar facts tras el merge**:

```sql
TRUNCATE fact_shots, fact_events RESTART IDENTITY CASCADE;
```

Las coordenadas viejas (escala 0-100) no son comparables con las nuevas (0-1).
