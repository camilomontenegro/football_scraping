# 📖 Guía de Uso: Filtros de Temporada y Fecha

Esta guía explica cómo utilizar las nuevas capacidades del pipeline para descargar datos de forma selectiva, ya sea por temporadas completas o por rangos de fechas específicos.

## 🚀 Comandos Principales

El orquestador principal es `scripts/pipeline_runner.py`. Todos los comandos deben ejecutarse desde la raíz del proyecto.

---

### 1. Filtrado por Temporada (`--season`)
Úsalo cuando necesites datos de una temporada específica, ya sea la actual o una histórica.

*   **Sintaxis:** `--season "YYYY/YYYY"`
*   **Ejemplo:**
    ```bash
    python -m scripts.pipeline_runner --competition "La Liga" --season "2023/2024" --scrape
    ```
*   **Qué hace:** Descarga todos los partidos y eventos asociados exclusivamente a esa temporada.

---

### 2. Filtrado por Fecha Manual (`--from-date`)
Úsalo si necesitas recuperar datos de un periodo específico o si el pipeline falló en una fecha determinada.

*   **Sintaxis:** `--from-date "YYYY-MM-DD"`
*   **Ejemplo:**
    ```bash
    python -m scripts.pipeline_runner --competition "La Liga" --from-date "2025-05-01" --scrape
    ```
*   **Qué hace:** Ignora los partidos anteriores a la fecha indicada y descarga todo lo posterior.

---

### 3. Modo Incremental Automático (`--update`) 🌟
Es la opción más eficiente para actualizaciones diarias. El sistema consulta la base de datos y decide qué falta.

*   **Sintaxis:** `--update`
*   **Ejemplo:**
    ```bash
    python -m scripts.pipeline_runner --competition "La Liga" --update
    ```
*   **Lógica interna:**
    1. Busca el partido más reciente en la tabla `dim_match`.
    2. Establece esa fecha como punto de inicio.
    3. Si detecta un cambio de temporada (ej. de Mayo a Agosto), crea automáticamente las nuevas carpetas correspondientes.

---

## 🛠️ Combinaciones Útiles

### Refresco Total de una Competición
Si quieres borrar caché y bajar todo de nuevo para una liga:
```bash
python -m scripts.pipeline_runner --competition "La Liga" --scrape
```

### Actualización de una sola fuente
Si solo quieres actualizar datos de SofaScore desde una fecha:
```bash
python -m scripts.pipeline_runner --competition "La Liga" --source sofascore --from-date "2025-05-15" --scrape
```

## ⚠️ Notas Importantes
- **Formato de Temporada:** Siempre usa `YYYY/YYYY` (ej: `2024/2025`). El sistema se encarga de convertirlo al formato interno de cada fuente (ej: `24/25` para SofaScore o `2024` para Understat).
- **Formato de Fecha:** Usa siempre el estándar ISO `YYYY-MM-DD`.
- **Capa de Carga:** Todos los comandos anteriores incluyen la fase de **CARGA** en la base de datos automáticamente después del scraping.
