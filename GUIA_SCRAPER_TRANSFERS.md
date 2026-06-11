# Scraper de Fichajes y Valor de Mercado (Transfermarkt)

## Resumen

Este módulo extrae dos tipos de datos históricos de Transfermarkt para todos los jugadores que ya existen en `dim_player`:

- **Fichajes**: historial completo de traspasos, cesiones y agentes libres de cada jugador.
- **Valor de mercado**: serie temporal con la evolución del valor de mercado a lo largo de su carrera.

Los datos se obtienen de la **CEAPI** de Transfermarkt (endpoints JSON internos), no del HTML de la web. Esto los hace más rápidos, fiables y resistentes a cambios de diseño.

---

## Arquitectura

```
dim_player (id_transfermarkt)
        |
        v
scrapers/transfermarkt_transfers_scraper.py
        |
        |-- CEAPI /transferHistory/list/{id}     --> data/raw/transfers/{id}.json
        |-- CEAPI /marketValueDevelopment/graph/{id} --> data/raw/market_value/{id}.json
        |
        v
    Genera CSVs limpios (cada 50 jugadores + al final + al interrumpir)
        |-- data/clean/transfers/transfers.csv
        |-- data/clean/market_value/market_value.csv
        |
        v
loaders/transfers_loader.py
        |
        |-- fact_transfers      (INSERT ... ON CONFLICT DO NOTHING)
        |-- fact_market_value   (INSERT ... ON CONFLICT DO UPDATE)
```

---

## Endpoints CEAPI utilizados

| Dato | Endpoint | Dominio |
|------|----------|---------|
| Fichajes | `/ceapi/transferHistory/list/{player_id}` | `transfermarkt.co.uk` |
| Valor de mercado | `/ceapi/marketValueDevelopment/graph/{player_id}` | `transfermarkt.com` |

Ambos devuelven JSON puro. No requieren API key ni autenticación. El User-Agent es genérico.

---

## Tablas en la base de datos

### `fact_transfers`

Creada por la migración `db/migrations/add_transfers_and_market_value.sql`.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `transfer_id` | SERIAL PK | ID autoincremental |
| `player_id` | INTEGER FK | Referencia a `dim_player.canonical_id` |
| `season` | VARCHAR(20) | Temporada del fichaje (ej: "24/25") |
| `transfer_date` | DATE | Fecha del traspaso |
| `from_team_id` | INTEGER FK | Equipo origen (si existe en `dim_team`) |
| `from_team_name` | VARCHAR(200) | Nombre del equipo origen |
| `to_team_id` | INTEGER FK | Equipo destino (si existe en `dim_team`) |
| `to_team_name` | VARCHAR(200) | Nombre del equipo destino |
| `fee_raw` | VARCHAR(100) | Texto original: "25 mill. €", "Cesión", "Libre" |
| `fee_euros` | BIGINT | Valor numérico en euros (NULL si cesión/libre) |
| `transfer_type` | VARCHAR(50) | `transfer`, `loan`, `free`, `end_of_loan`, `retirement`, `unknown` |
| `is_loan` | BOOLEAN | True si es cesión |
| `id_tm_from_team` | INTEGER | ID de TM del equipo origen (para trazabilidad) |
| `id_tm_to_team` | INTEGER | ID de TM del equipo destino |

Clave única: `(player_id, season, transfer_date, COALESCE(id_tm_from_team, -1), COALESCE(id_tm_to_team, -1))`

### `fact_market_value`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `mv_id` | SERIAL PK | ID autoincremental |
| `player_id` | INTEGER FK | Referencia a `dim_player.canonical_id` |
| `value_date` | DATE | Fecha de la valoración |
| `market_value` | BIGINT | Valor en euros |
| `market_value_raw` | VARCHAR(100) | Texto original: "80 mill. €" |
| `club_id` | INTEGER FK | Club en ese momento (si existe en `dim_team`) |
| `club_name` | VARCHAR(200) | Nombre del club |
| `id_tm_club` | INTEGER | ID de TM del club |

Clave única: `(player_id, value_date)`

---

## Uso paso a paso

### 1. Crear las tablas (solo la primera vez)

```bash
psql -U postgres -d football_db -f db/migrations/add_transfers_and_market_value.sql
```

### 2. Ejecutar el scraper

```bash
# Scraping completo (todos los jugadores con id_transfermarkt en dim_player)
python -m scrapers.transfermarkt_transfers_scraper

# Probar con pocos jugadores primero
python -m scrapers.transfermarkt_transfers_scraper --limit 10

# Solo fichajes (sin valor de mercado)
python -m scrapers.transfermarkt_transfers_scraper --skip-market-value

# Solo valor de mercado (sin fichajes)
python -m scrapers.transfermarkt_transfers_scraper --skip-transfers

# Forzar re-descarga ignorando caché
python -m scrapers.transfermarkt_transfers_scraper --force

# Simulación sin descargar nada
python -m scrapers.transfermarkt_transfers_scraper --dry-run

# Regenerar CSVs desde los JSONs ya descargados (sin llamar a la API)
python -m scrapers.transfermarkt_transfers_scraper --transform-only
```

### 3. Cargar en la base de datos

```bash
# Carga ambas tablas
python -m loaders.transfers_loader

# Solo fichajes
python -m loaders.transfers_loader --only transfers

# Solo valor de mercado
python -m loaders.transfers_loader --only market_value

# Simulación
python -m loaders.transfers_loader --dry-run
```

---

## Sistema de persistencia incremental

El scraper está diseñado para poder interrumpirse en cualquier momento sin perder datos:

- **Cada 50 jugadores** procesados, guarda automáticamente la caché y regenera los CSVs limpios.
- **Al pulsar Ctrl+C**, guarda todo el progreso acumulado antes de salir.
- **Al reiniciar**, la caché (`data/.cache/transfermarkt_transfers_last_scraped.json`) evita re-descargar jugadores ya procesados.

Si quieres forzar la re-descarga de un jugador concreto, borra su entrada de la caché o usa `--force` para re-descargar todo.

---

## Estructura de ficheros generados

```
data/
├── raw/
│   ├── transfers/
│   │   ├── 28003.json          ← Messi: historial de fichajes
│   │   ├── 418560.json         ← Mbappé
│   │   └── ...
│   └── market_value/
│       ├── 28003.json          ← Messi: curva de valor
│       ├── 418560.json
│       └── ...
├── clean/
│   ├── transfers/
│   │   └── transfers.csv       ← CSV listo para el loader
│   └── market_value/
│       └── market_value.csv    ← CSV listo para el loader
└── .cache/
    └── transfermarkt_transfers_last_scraped.json
```

---

## Ejecución en paralelo con otros scrapers

Este scraper es **compatible con ejecución en paralelo** junto a los demás scrapers del proyecto. Usa endpoints de Transfermarkt distintos a los de `transfermarkt_scraper.py` (que usa las páginas HTML de kader/leistungsdaten) y a `transfermarkt_stadiums_scraper.py`.

Sin embargo, como todos van contra el mismo dominio (transfermarkt.com/co.uk/es), se recomienda **no ejecutar más de 2 scrapers de Transfermarkt a la vez** para evitar rate-limiting.

---

## Resolución de Foreign Keys

El loader resuelve automáticamente las FKs:

- `player_id`: busca en `dim_player.id_transfermarkt`
- `from_team_id` / `to_team_id` / `club_id`: busca en `dim_team.id_transfermarkt`

Si un equipo no existe en `dim_team` (por ejemplo, un club de una liga que no scrapeamos), la FK queda NULL pero el nombre del equipo se guarda en `from_team_name` / `to_team_name`.

---

## Ejemplo de consultas útiles

```sql
-- Top 10 fichajes más caros
SELECT p.canonical_name, t.from_team_name, t.to_team_name,
       t.fee_euros, t.fee_raw, t.transfer_date
FROM fact_transfers t
JOIN dim_player p ON t.player_id = p.canonical_id
WHERE t.fee_euros IS NOT NULL
ORDER BY t.fee_euros DESC
LIMIT 10;

-- Evolución de valor de mercado de un jugador
SELECT p.canonical_name, mv.value_date, mv.market_value,
       mv.market_value_raw, mv.club_name
FROM fact_market_value mv
JOIN dim_player p ON mv.player_id = p.canonical_id
WHERE p.canonical_name ILIKE '%messi%'
ORDER BY mv.value_date;

-- Jugadores con más fichajes
SELECT p.canonical_name, COUNT(*) AS num_transfers
FROM fact_transfers t
JOIN dim_player p ON t.player_id = p.canonical_id
GROUP BY p.canonical_name
ORDER BY num_transfers DESC
LIMIT 20;

-- Valor máximo alcanzado por cada jugador
SELECT p.canonical_name, MAX(mv.market_value) AS peak_value,
       mv.club_name
FROM fact_market_value mv
JOIN dim_player p ON mv.player_id = p.canonical_id
GROUP BY p.canonical_name, mv.club_name
ORDER BY peak_value DESC
LIMIT 20;
```
