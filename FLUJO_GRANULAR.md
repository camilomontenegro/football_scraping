# Flujo Granular: Descarga â†’ Dimensiones â†’ Facts

**Estado:** [PRODUCTION READY]  
**3 Scripts Nuevos:** `scrape_only.py`, `load_dimensions.py`, `load_facts.py`

---

## Flujo Recomendado (Paso a Paso)

### Paso 1 - DESCARGAR DATOS (30-45 minutos)
```bash
# Opcion A: Todos los scrapers (lento, ~3.5 horas)
python scrape_only.py --all

# Opcion B: Rápido (recomendado, ~35 minutos)
python scrape_only.py --understat       # ~15-20 min
python scrape_only.py --statsbomb       # ~5-10 min
python scrape_only.py --transfermarkt   # ~10-15 min

# Opcion C: Solo uno
python scrape_only.py --understat
```

**Salida:** `data/raw/` lleno de archivos JSON

---

### Paso 2 - CARGAR DIMENSIONES

#### 2a) Cargar Equipos (dim_team)
```bash
python load_dimensions.py --teams
```
**Tiempo:** ~1-2 minutos  
**Resultado:** 20-50 equipos en dim_team

#### 2b) Cargar Jugadores (dim_player)
```bash
python load_dimensions.py --players
```
**Tiempo:** ~2-5 minutos  
**Resultado:** 1000-2000 jugadores en dim_player

#### 2c) Cargar Partidos (dim_match)
```bash
python load_dimensions.py --matches
```
**Tiempo:** ~1-2 minutos  
**Resultado:** 380 partidos (5 temporadas) en dim_match

#### 2d) O Cargar TODAS las Dimensiones
```bash
python load_dimensions.py --all
```
**Tiempo:** ~5-10 minutos

---

### Paso 3 - CARGAR FACTS

#### 3a) Cargar Tiros (fact_shots)
```bash
python load_facts.py --shots
```
**Tiempo:** ~5-10 minutos  
**Resultado:** 10,000-30,000 registros en fact_shots

#### 3b) Cargar Eventos (fact_events)
```bash
python load_facts.py --events
```
**Tiempo:** ~10-15 minutos  
**Resultado:** 50,000-200,000 registros en fact_events

#### 3c) Cargar Lesiones (fact_injuries)
```bash
python load_facts.py --injuries
```
**Tiempo:** ~1-2 minutos  
**Resultado:** 1000-5000 registros en fact_injuries

#### 3d) O Cargar TODOS los Facts
```bash
python load_facts.py --all
```
**Tiempo:** ~15-30 minutos

---

## Timeline Completo (Recomendado)

```
Paso 1: Descargar (Understat + StatsBomb + Transfermarkt)
â”œâ”€ Understat       : 15-20 min
â”œâ”€ StatsBomb       : 5-10 min
â”œâ”€ Transfermarkt   : 10-15 min
â””â”€ SUBTOTAL        : ~30-45 min

Paso 2: Cargar Dimensiones
â”œâ”€ Teams           : 1-2 min
â”œâ”€ Players         : 2-5 min
â”œâ”€ Matches         : 1-2 min
â””â”€ SUBTOTAL        : ~5-10 min

Paso 3: Cargar Facts
â”œâ”€ Shots           : 5-10 min
â”œâ”€ Events          : 10-15 min
â”œâ”€ Injuries        : 1-2 min
â””â”€ SUBTOTAL        : ~15-30 min

TOTAL: ~50 min - 1.5 horas âœ…
```

---

## Variantes de Flujo

### Variante A: MUY RÃPIDA (45 minutos)
```bash
# Solo lo esencial
python scrape_only.py --understat
python load_dimensions.py --all
python load_facts.py --shots
# Total: ~40 minutos, bÃ¡sico pero funcional
```

### Variante B: RECOMENDADA (1 hora)
```bash
# Buen balance
python scrape_only.py --understat
python scrape_only.py --statsbomb
python scrape_only.py --transfermarkt
python load_dimensions.py --all
python load_facts.py --all
# Total: ~1 hora, muy completa
```

### Variante C: COMPLETA (3.5 horas)
```bash
# Toda la cobertura posible
python scrape_only.py --all           # Incluye SofaScore (2-3 horas)
python load_dimensions.py --all
python load_facts.py --all
# Total: ~3.5 horas, mÃ¡xima cobertura
```

### Variante D: SI YA DESCARGASTE
```bash
# Si ya tienes los datos en data/raw/
python load_dimensions.py --all       # 5-10 min
python load_facts.py --all            # 15-30 min
# Total: ~30 minutos
```

### Variante E: SOLO UNA DIMENSIÃ“N
```bash
# Ejemplo: Solo cargar equipos
python load_dimensions.py --teams     # 1-2 min
```

---

## Estructura de los Scripts

### scrape_only.py
**OpciÃ³n:** Descargar sin cargar en BD  
**Salida:** Archivos en `data/raw/`  
**Flags:**
- `--understat` - Understat
- `--sofascore` - SofaScore
- `--statsbomb` - StatsBomb
- `--transfermarkt` - Transfermarkt
- `--all` - Todos

### load_dimensions.py
**OpciÃ³n:** Cargar dimensiones individualmente  
**Entrada:** Archivos en `data/raw/`  
**Flags:**
- `--teams` - dim_team
- `--players` - dim_player
- `--matches` - dim_match
- `--all` - Todos

### load_facts.py
**OpciÃ³n:** Cargar facts individualmente  
**Entrada:** dim_team, dim_player, dim_match llenos  
**Flags:**
- `--shots` - fact_shots
- `--events` - fact_events
- `--injuries` - fact_injuries
- `--all` - Todos

---

## âš™ï¸ Manejo de Errores

### Si un scraper falla
```bash
# ContinÃºa con dimensiones (se cargarÃ¡n los datos parciales)
python scrape_only.py --understat    # Falla
python load_dimensions.py --all      # Carga lo que se descargÃ³

# O reintenta el scraper
python scrape_only.py --understat
```

### Si falla al cargar teams
```bash
# Retry individual
python load_dimensions.py --teams    # Reintenta solo teams

# Los otros no estÃ¡n afectados, pueden continuarse
python load_dimensions.py --players  # OK
```

### Si falla al cargar fact_shots
```bash
# Retry individual
python load_facts.py --shots         # Reintenta solo shots

# Los otros facts siguen funcionando
python load_facts.py --events        # OK
```

---

## ðŸ” VerificaciÃ³n (Entre Pasos)

```bash
# DespuÃ©s de descargar
ls -la data/raw/
ls -la data/raw/understat/
ls -la data/raw/statsbomb/

# DespuÃ©s de cargar dimensiones
psql -h localhost -U postgres -d football_db -c "SELECT COUNT(*) FROM dim_team;"
psql -h localhost -U postgres -d football_db -c "SELECT COUNT(*) FROM dim_player;"
psql -h localhost -U postgres -d football_db -c "SELECT COUNT(*) FROM dim_match;"

# DespuÃ©s de cargar facts
psql -h localhost -U postgres -d football_db -c "SELECT COUNT(*) FROM fact_shots;"
psql -h localhost -U postgres -d football_db -c "SELECT COUNT(*) FROM fact_events;"
psql -h localhost -U postgres -d football_db -c "SELECT COUNT(*) FROM fact_injuries;"
```

---

## âœ… Checklist

- [ ] `.env` configurado
- [ ] PostgreSQL corriendo
- [ ] Base de datos creada (`python db/setup_db.py`)
- [ ] `health_check.py --verbose` pasando
- [ ] Ejecutar `scrape_only.py --understat` (o tu opciÃ³n)
- [ ] Ejecutar `load_dimensions.py --all`
- [ ] Ejecutar `load_facts.py --all`
- [ ] Verificar conteos en BD
- [ ] Ejecutar `health_check.py --verbose` nuevamente
- [ ] âœ… LISTO PARA PRODUCCIÃ“N

---

## ðŸ†˜ Troubleshooting

### "ModuleNotFoundError: No module named 'scrapers'"
```bash
# AsegÃºrate de ejecutar desde el directorio del proyecto
cd d:\PRACTICAS\Proyecto_Mercanza\Football_scraping_v1
python scrape_only.py --understat
```

### "Connection refused to database"
```bash
# Verifica PostgreSQL
psql -h localhost -U postgres -c "SELECT 1"

# Verifica .env
cat .env
```

### "No se descargaron datos"
```bash
# Verifica data/raw/
ls -la data/raw/

# Si esta vacio, el scraper falló
# Revisa el error del scraper
python scrape_only.py --understat 2>&1 | head -50
```

### "FK violation on dim_match"
```bash
# Los equipos no estÃ¡n cargados
# Ejecuta primero: python load_dimensions.py --teams
```

---

## ðŸŽ¯ RecomendaciÃ³n Final

**Usar Variante B (Recomendada):**
```bash
# Paso 1: Descargar (35 minutos)
python scrape_only.py --understat
python scrape_only.py --statsbomb
python scrape_only.py --transfermarkt

# Paso 2: Cargar dimensiones (5-10 minutos)
python load_dimensions.py --all

# Paso 3: Cargar facts (15-30 minutos)
python load_facts.py --all

# Paso 4: Verificar (1 minuto)
python health_check.py --verbose

# TOTAL: ~1 hora
```

**¡Listo!**
