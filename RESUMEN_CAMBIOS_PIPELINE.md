# Resumen de Cambios en el Pipeline ETL de Fútbol

Este documento detalla todas las modificaciones y mejoras realizadas en el sistema de scraping y carga de datos para estandarizar el soporte multi-competición y multi-temporada.

## 1. Estandarización de Directorios
Se ha implementado una jerarquía de carpetas única para todas las fuentes, permitiendo una gestión clara de los datos crudos:
- **Estructura**: `data/raw/<fuente>/<competicion_slug>/season=<YYYY_YYYY>/`
- **Impacto**: Facilita la carga masiva y evita colisiones de nombres entre diferentes ligas o años.

## 2. Orquestador (`scripts/pipeline_runner.py`)
El antiguo `scrape_only.py` ha sido reemplazado por un orquestador robusto:
- **Soporte Multi-Temporada**: Ahora permite descargar varias temporadas en un solo comando (`--season 2020/2021 2021/2022 ...`).
- **Soporte Multi-Fuente**: Permite filtrar por varias fuentes simultáneamente (`--source sofascore whoscored ...`).
- **Modo Scrape-Only**: Nueva bandera `--scrape-only` para descargar datos sin enviarlos a la base de datos.
- **Detección Incremental**: Consulta la base de datos para obtener la fecha del último partido y descargar solo lo nuevo.
- **Correcciones Técnicas**: Se resolvieron errores de tipos en las llamadas a los cargadores y problemas de importación.

## 3. Scrapers Específicos

### Transfermarkt (`scrapers/transfermarkt_scraper.py`)
- **Integración de Lógica Externa**: Se fusionó el código del colaborador con las optimizaciones existentes.
- **Descubrimiento Dinámico**: Detecta automáticamente los equipos de la liga basándose en la temporada y el slug de la competición.
- **Resiliencia**:
    - Guardado de caché (`last_scraped.json`) tras procesar cada equipo.
    - Manejo de reintentos (3) con pausas aleatorias aumentadas (3-6s) para evitar bloqueos por IP.
- **Enriquecimiento**: Ahora descarga plantillas completas, fechas de nacimiento y registros de lesiones.

### WhoScored (`scrapers/whoscored_scraper.py`)
- **Guardado Incremental**: Se modificó para guardar los archivos CSV tras cada partido procesado, evitando la pérdida de datos en sesiones largas.
- **Mapeo de Competiciones**: Ahora utiliza `scripts/competitions.py` para resolver las URLs correctas de cualquier liga.
- **Carga de Progreso**: Al iniciar, lee los CSVs existentes para no repetir la descarga de partidos que ya están en el disco.

### SofaScore, Understat y StatsBomb
- Se han actualizado sus rutas de salida para cumplir con el nuevo estándar de directorios.
- Se ha unificado la forma en que obtienen el `comp_slug` desde la configuración centralizada.

## 4. Entorno y Dependencias
- **SQLAlchemy 2.0.49**: Se actualizó la librería para resolver un error crítico de compatibilidad (`AssertionError` con `TypingOnly`) presente en entornos con Python 3.14.
- **Organización de Código**: Se eliminaron archivos redundantes como `understat_generico.py` y `scrape_only.py` para limpiar el repositorio.

## 5. Utilidades (`scripts/competitions.py`)
- Se han añadido y verificado los IDs de WhoScored, SofaScore y Understat para las principales ligas europeas (La Liga, Premier League, Bundesliga, Serie A, etc.).

---
*Documento generado automáticamente el 05/05/2026 para documentar el estado actual del pipeline.*
