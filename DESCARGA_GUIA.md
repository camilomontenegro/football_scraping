# GuÃ­a de Descarga - Temporadas 2020/21 a 2024/25

**Estado:** [OK] Production Ready  
**Temporadas:** 5 aÃ±os completos (2020/21 â†’ 2024/25)  
**Fuentes:** 5 scrapers configurados

---

## Quick Start

```bash
# 1. AsegÃºrate de que el .env estÃ¡ configurado
cat .env                # Verifica DB_PASSWORD, DB_NAME, etc.

# 2. Verifica que la BD estÃ¡ lista
python db/setup_db.py

# 3. Descarga datos (elige una o varias fuentes)
python -m scrapers.understat_scraper      # [OK] Async (rÃ¡pido)
python -m scrapers.sofascore_scraper      # [SLOW] Necesita navegador (lento)
python -m scrapers.transfermarkt_scraper  # [LIMITED] Rate-limited
python -m scrapers.statsbomb_scraper      # [OK] RÃ¡pido (Open Data)

# 4. Carga en la base de datos
python -m scripts. --load-only

# 5. Verifica que todo funciona
python -m scripts. --verbose
```

---

## Configuracion por Scraper

### [GREEN] Understat - RECOMENDADO
**Velocidad:** [FAST] Muy rÃ¡pido (async)  
**Cobertura:** Shots, Events (datos event-level)  
**ConfiguraciÃ³n:**
```python
LEAGUE      = "La liga"
SEASONS     = [2020, 2021, 2022, 2023, 2024]   # 20/21 â†’ 24/25
```
**Comando:**
```bash
python -m scrapers.understat_scraper
# Descarga 5 temporadas ~15-20 minutos
```

**Salida:** `data/raw/understat/`
```
â”œâ”€â”€ shots.json          â† Tiros con xG
â”œâ”€â”€ matches.json        â† Partidos
â”œâ”€â”€ teams.json          â† Equipos
â””â”€â”€ players.json        â† Jugadores
```

---

### [ORANGE] SofaScore
**Velocidad:** [SLOW] Lento (necesita Selenium)  
**Cobertura:** Matches, Shots, Events, Lineups  
**ConfiguraciÃ³n:**
```python
TOURNAMENT_ID = 8      # La Liga en SofaScore
SEASON_NAMES  = [
    "2020/2021",
    "2021/2022", 
    "2022/2023",
    "2023/2024",
    "2024/2025"
]
```
**Comando:**
```bash
python -m scrapers.sofascore_scraper
# Descarga 5 temporadas ~2-3 horas (con pausa entre requests)
```

**Salida:** `data/raw/sofascore/`
```
season=2020_2021/
â”œâ”€â”€ matches_clean.json
â”œâ”€â”€ shots_clean.json
â”œâ”€â”€ events_clean.json
â”œâ”€â”€ teams.json
â””â”€â”€ players.json
```

**[WARNING] Notas:**
- SofaScore tiene rate limiting estricto
- Necesita navegador Chrome/Chromium instalado
- Lento pero muy confiable

---

### [BLUE] StatsBomb - RECOMENDADO
**Velocidad:** [FAST] RÃ¡pido (Open Data)  
**Cobertura:** Events (datos granulares)  
**ConfiguraciÃ³n:**
```python
COMPETITION_ID = 11    # La Liga en StatsBomb
SEASON_IDS     = [90, 106, 113, 120, 127]
SEASON_LABELS  = ["2020/21", "2021/22", "2022/23", "2023/24", "2024/25"]
```
**Comando:**
```bash
python -m scrapers.statsbomb_scraper
# Descarga 5 temporadas ~5-10 minutos
```

**Salida:** `data/raw/statsbomb/`
```
competition_11/
â”œâ”€â”€ season_90/
â”‚   â”œâ”€â”€ matches_clean.json
â”‚   â”œâ”€â”€ events_clean.json
â”‚   â”œâ”€â”€ teams.json
â”‚   â””â”€â”€ players.json
â”œâ”€â”€ season_106/
â””â”€â”€ ...
```

**âœ… Ventajas:**
- Open Data (sin autenticaciÃ³n)
- Muy rÃ¡pido
- Eventos muy detallados

---

### ðŸŸ£ Transfermarkt - RECOMENDADO
**Velocidad:** [FAST] RÃ¡pido  
**Cobertura:** Jugadores canÃ³nicos, Lesiones  
**ConfiguraciÃ³n:**
```python
LEAGUE_CODE = "ES1"    # La Liga
SEASONS     = [2020, 2021, 2022, 2023, 2024]
```
**Comando:**
```bash
python -m scrapers.transfermarkt_scraper
# Descarga 5 temporadas ~10-15 minutos
```

**Salida:** `data/raw/transfermarkt/`
```
season=2020-2024/
â”œâ”€â”€ players_clean.json      â† Fuente CANÃ“NICA de dim_player
â”œâ”€â”€ injuries_clean.json
â””â”€â”€ (subdirectorios por equipo y temporada)
```

**âœ… Importante:**
- Es la fuente maestra de jugadores
- Incluye lesiones (fact_injuries)
- Datos de transferencias

---

### [YELLOW] WhoScored
**Velocidad:** [SLOW] Lento (necesita Selenium)  
**Cobertura:** Events (muy detallados)  
**ConfiguraciÃ³n:**
```python
# Necesita match_ids especÃ­ficos (no loop automÃ¡tico)
# Ver whoscored_scraper.py para mÃ¡s detalles
```
**Comando:**
```bash
python -m scrapers.whoscored_scraper --match-ids 1234567 1234568 ...
```

**[WARNING] Notas:**
- Requiere match IDs especÃ­ficos
- Datos ultra granulares pero lento
- Bloquea Chrome headless -> usa headless=False

---

## Estrategia de Descarga Recomendada

### Opcion 1 - Rapida (30-40 minutos)
```bash
# MÃ¡xima cobertura, mÃ­nimo tiempo
python -m scrapers.statsbomb_scraper      # 5-10 min
python -m scrapers.understat_scraper      # 15-20 min
python -m scrapers.transfermarkt_scraper  # 10-15 min
```
**Total:** 30-45 minutos  
**Cobertura:** Shots, Events, Players, Injuries

---

### Opcion 2 - Completa (3-4 horas)
```bash
# Toda la cobertura posible
python -m scrapers.statsbomb_scraper      # 5-10 min
python -m scrapers.understat_scraper      # 15-20 min
python -m scrapers.transfermarkt_scraper  # 10-15 min
python -m scrapers.sofascore_scraper      # 2-3 horas
```
**Total:** 2.5-3.5 horas  
**Cobertura:** MÃ¡xima (todos los datos, todas las fuentes)

---

### OpciÃ³n 3ï¸âƒ£ - Solo Base de Datos
```bash
# Si ya descargaste los datos, solo cargar en BD
python -m scripts. --load-only
```
**Total:** 5-15 minutos (segÃºn volumen)

---

## ðŸ“ˆ DespuÃ©s de Descargar

### Verificar Descargas
```bash
# âœ… Verificar archivos descargados
ls -la data/raw/understat/
ls -la data/raw/sofascore/
ls -la data/raw/statsbomb/
ls -la data/raw/transfermarkt/

# âœ… Contar registros
python -c "
import json
for f in ['data/raw/understat/shots.json']:
    with open(f) as fp:
        data = json.load(fp)
        print(f'{f}: {len(data)} registros')
"
```

### Cargar en Base de Datos
```bash
# OpciÃ³n 1: AutomÃ¡tico
python -m scripts. --load-only

# OpciÃ³n 2: Manual (por fuente)
python -c "
from loaders.understat_loader import load_understat_shots
load_understat_shots()
"
```

### Verificar Carga
```bash
# Conectar a PostgreSQL
psql -h localhost -U postgres -d football_db

# Consultas Ãºtiles
SELECT COUNT(*) FROM dim_team;         -- Equipos
SELECT COUNT(*) FROM dim_player;       -- Jugadores
SELECT COUNT(*) FROM dim_match;        -- Partidos
SELECT COUNT(*) FROM fact_shots;       -- Tiros
SELECT COUNT(*) FROM fact_events;      -- Eventos
SELECT COUNT(*) FROM fact_injuries;    -- Lesiones
```

---

## ðŸ› Troubleshooting

### Error: "Connection refused"
```bash
# Verifica que PostgreSQL estÃ¡ corriendo
sudo systemctl status postgresql
# o
psql -h localhost -U postgres -c "SELECT 1"
```

### Error: "DB_PASSWORD must be set"
```bash
# Verifica .env
cat .env
# Debe tener: DB_PASSWORD=tu_contraseÃ±a
```

### Error: "Chrome driver not found" (SofaScore)
```bash
# Instala dependencias de Selenium
pip install webdriver-manager
# webdriver-manager descargarÃ¡ Chrome automÃ¡ticamente
```

### Scraper muy lento
```bash
# Reduce DELAY_SEC en el scraper:
DELAY_SEC = 0.5   # MÃ¡s rÃ¡pido pero riesgo de bloqueo

# Mejores opciones:
# 1. Usa Understat (mÃ¡s rÃ¡pido)
# 2. Usa StatsBomb (Open Data, sin limites)
# 3. Ejecuta en paralelo (en mÃ¡quinas diferentes)
```

### Datos incompletos
```bash
# Verifica el log
tail -100 scraper.log

# Vuelve a intentar fase especÃ­fica
python -m scrapers.understat_scraper  # Reintenta
python -m scripts. --load-only  # Carga lo que se descargÃ³
```

---

## ðŸ“… Mapeo de Season IDs

| Scraper | ID | Temporada |
|---------|----|-----------| 
| Understat | 2020 | 2020/21 |
| Understat | 2021 | 2021/22 |
| Understat | 2022 | 2022/23 |
| Understat | 2023 | 2023/24 |
| Understat | 2024 | 2024/25 |
| StatsBomb | 90 | 2020/21 |
| StatsBomb | 106 | 2021/22 |
| StatsBomb | 113 | 2022/23 |
| StatsBomb | 120 | 2023/24 |
| StatsBomb | 127 | 2024/25 |
| SofaScore | "2020/2021" | 2020/21 |
| SofaScore | "2021/2022" | 2021/22 |
| SofaScore | "2022/2023" | 2022/23 |
| SofaScore | "2023/2024" | 2023/24 |
| SofaScore | "2024/2025" | 2024/25 |
| Transfermarkt | 2020 | 2020/21 |
| Transfermarkt | 2021 | 2021/22 |
| Transfermarkt | 2022 | 2022/23 |
| Transfermarkt | 2023 | 2023/24 |
| Transfermarkt | 2024 | 2024/25 |

---

## âœ… Checklist Final

- [ ] `.env` configurado con credenciales reales
- [ ] PostgreSQL corriendo (`psql -h localhost -U postgres`)
- [ ] Base de datos creada (`python db/setup_db.py`)
- [ ] Health check pasando (`python -m scripts. --verbose`)
- [ ] Tests pasando (`pytest tests/ -m unit -v`)
- [ ] Primer scraper ejecutado (`python -m scrapers.understat_scraper`)
- [ ] Datos cargados en BD (`python -m scripts. --load-only`)
- [ ] Verificar conteos en BD (SELECT COUNT(*) FROM dim_team)
- [ ] Listo para producciÃ³n

---

**Â¿Preguntas?** Consulta [README.md](README.md) o [SECURITY_SETUP.md](SECURITY_SETUP.md)

**Tiempo estimado:**
- RÃ¡pida: 30-40 minutos
- Completa: 3-4 horas
- En producciÃ³n: Ejecutar una vez al mes o al iniciar temporada
