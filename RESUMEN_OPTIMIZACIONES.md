# 🚀 Optimización del Pipeline de Scraping (Abril 2026)

Este documento resume las mejoras implementadas para aumentar la velocidad, estabilidad y discreción de los scrapers de fútbol.

## 🛠 Cambios Principales

### 1. WhoScored: Navegación Optimizada
Se ha modificado el driver de Selenium para bloquear la carga de **imágenes y CSS**.
- **Beneficio:** Las páginas cargan hasta un 60% más rápido y consumen menos ancho de banda.
- **Archivo:** `scrapers/whoscored_scraper.py`

### 2. Transfermarkt: Caché Local e Inteligente
Se ha implementado un sistema de caché de 7 días para los jugadores.
- **Beneficio:** Si un jugador ya fue consultado recientemente, el script salta la petición HTTP. El tiempo total de ejecución baja de **45 minutos a segundos** en actualizaciones frecuentes.
- **Archivo:** `scrapers/transfermarkt_scraper.py`
- **Caché:** `data/raw/transfermarkt/last_scraped.json`

### 3. SofaScore: Modo Incremental vía Base de Datos
El scraper ahora consulta la tabla `fact_events` de PostgreSQL antes de descargar un partido.
- **Beneficio:** Si el partido ya tiene eventos en la BD, no se descarga de nuevo. Evita redundancia y riesgo de baneo.
- **Archivo:** `scrapers/sofascore_scraper.py`

### 4. Gestión Inteligente de Temporadas
Se ha eliminado el valor fijo "2024/2025" de los scripts.
- **Detección Automática:** El sistema ahora detecta la temporada actual (ej: 25/26) basándose en la fecha del sistema.
- **Nombres Estandarizados:** Las carpetas de datos ahora usan el formato `season=2025_2026` para evitar conflictos y caracteres especiales.

---

## 📖 Cómo usar los nuevos comandos

### A. Actualización Rápida (Modo por defecto)
Usa `--update` para que el sistema use toda la inteligencia (caché + BD) y solo baje lo que falte.
```bash
python -m scripts.pipeline_runner --competition "La Liga" --update
```

### B. Descarga Forzada (Limpieza total)
Usa `--full-refresh` si necesitas ignorar la caché y descargar todo de nuevo (útil para errores o cambios de estructura).
```bash
python -m scripts.pipeline_runner --competition "La Liga" --scrape --full-refresh
```

### C. Temporada Específica
Si quieres una temporada que no sea la actual, especifícala manualmente:
```bash
python -m scripts.pipeline_runner --competition "La Liga" --season "2023/2024" --update
```
