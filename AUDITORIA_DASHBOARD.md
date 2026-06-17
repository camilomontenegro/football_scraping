# Auditoría del dashboard de Streamlit

Fecha: 2026-06-15 · Alcance: `dashboard/` (app.py, explore.py, analytics.py, player_detail.py, pass_network.py, db.py, scanner.py).

> **Nota de validación:** no tengo acceso a tu Postgres desde este entorno, así que todo está verificado por análisis estático y compilación (`py_compile` / `ast`), no ejecutando la app con datos. Los puntos marcados con 🔎 conviene confirmarlos en local.

---

## 1. Resumen ejecutivo

- **3 arreglos aplicados** (fuera de las pestañas de Match, como acordamos): standings, coverage del pipeline y rendimiento (cacheo de selectores).
- **Pestañas de Match Context**: auditadas, **sin tocar el código**. El problema de fondo no es que "no funcionen" sino que el tab se llama *Match Context* pero **todo son agregados de temporada**: no existe vista por partido. Además árbitros/mánagers dependen de columnas que casi siempre vienen vacías → muchas veces ves "no data" y parece roto.
- **Recomendación principal**: cachear todas las consultas y unificar el idioma (hay mucho texto en inglés pese a tener i18n).

---

## 2. Cambios aplicados (bugs críticos arreglados)

### 2.1 Standings: conteo de partidos incorrecto al filtrar por equipo
`dashboard/app.py` (Exploración → Standings, ~línea 274).

Antes: `total_matches = int(df["p"].sum()) // 2`. La división por 2 asume que la tabla trae **las dos** filas de cada partido (local + visitante). Cuando seleccionas **un equipo**, la consulta devuelve **una sola fila**, así que `//2` parte por la mitad los partidos y los KPIs *Goles/partido* y *xG/partido* salían **al doble**.

Ahora solo divide entre 2 cuando hay tabla completa:
```python
_p_sum = int(df["p"].sum())
total_matches = _p_sum if len(df) <= 1 else _p_sum // 2
```

### 2.2 Pipeline → Coverage: ignoraba la competición
`dashboard/db.py` · `get_coverage_by_source()`.

La consulta filtraba solo por `season`, **no por competición**, así que al elegir, p. ej., *Champions League* contaba partidos de **todas** las competiciones de esa temporada. Además aplicaba el total de referencia de La Liga (380) a cualquier liga, dando barras de progreso sin sentido.

Arreglado: la consulta ahora hace `JOIN dim_competition` y filtra por `c.canonical_name = :competition`; y el ratio `cargados / total` solo se muestra para La Liga (para el resto se enseña el conteo sin barra engañosa).

### 2.3 Rendimiento: cacheo de los selectores
`dashboard/explore.py` · `get_competitions`, `get_seasons_for_competition`, `get_teams_for_season` ahora llevan `@st.cache_data(ttl=300)`.

Estas tres se ejecutan **en cada rerun y en casi todas las pestañas** (cada vez que mueves un control). Cachearlas reduce mucho la latencia. Es seguro: son lecturas puras; el TTL de 300 s las refresca tras cargar datos nuevos.

---

## 3. Pestañas de Match Context (solo auditoría)

### 3.1 Conceptual (lo más importante)
- **No hay vista por partido.** Weather / Attendance / Referees / Managers son todos agregados de temporada. Sugerencia de rediseño: un **selector de partido** arriba con una ficha de contexto (resultado, estadio, clima, asistencia + % llenado, árbitro y tarjetas de ese partido, mánagers). Eso sí encaja con el nombre del tab.

### 3.2 Weather
- La tabla "Temperature by stadium" usa `df_venue` **completo y sin ordenar**, mientras el gráfico de barras usa el top-20 ya ordenado → tabla y gráfico no coinciden (`app.py` ~1177-1183).
- "Evolución por temporada" ignora competición y temporada por diseño (es cross-season); puede confundir porque está dentro de un tab ya filtrado por temporada.
- Riesgo bajo: las barras de error usan `min/max`; si `humidity` u otros vienen `NULL` no afecta al chart, pero conviene 🔎.

### 3.3 Attendance
- `fill_pct = attendance / capacity * 100` puede dar **inf** si `capacity = 0` y mostrarse como "inf%" (`explore.get_attendance_by_match`). Recomiendo `NULLIF(capacity,0)`.
- Textos hardcodeados en inglés en una UI en español: "Min", "Avg Fill %", "Fill %", "Empty seats".
- `_att_display` se reutiliza para dos tablas distintas (cosmético, no es bug).

### 3.4 Referees
- Las tarjetas se detectan con heurística `ILIKE '%yellow%'/'%red%'` sobre `event_type`/`outcome` (`explore.py` ~1027-1034). Es frágil entre fuentes: si una fuente codifica la tarjeta distinto, cuenta de más o de menos. 🔎
- Depende de `dim_match.referee_id` + `dim_referee` (solo WhoScored). Si está vacío, el tab muestra "no data" y parece roto. Conviene un aviso explícito tipo "esta fuente no trae árbitro".

### 3.5 Managers
- `get_manager_stats` agrupa **solo por nombre** de mánager (`GROUP BY manager`, `explore.py` ~1141). Dos técnicos con el mismo nombre se fusionan, y `MAX(team)` elige un equipo **arbitrario** si dirigió a varios. Debería agrupar por `(manager, equipo)` o por un id real.
- Depende de `manager_home/manager_away` (WhoScored) → a menudo escaso.

---

## 4. Resto del dashboard (hallazgos, no modificados)

### 4.1 Shot Intelligence — posible espejado de zonas 🔎
`analytics.get_heatmap_data` normaliza la X de **SofaScore** invirtiéndola (`(100 - x)`), pero la Y no, y el resto de fuentes **no** invierten la X (`analytics.py` ~66-79). Si mezclas fuentes en el mismo heatmap, los disparos de SofaScore pueden quedar en el lado contrario. En `player_detail.get_player_shots` la convención es distinta (ahí sí invierte SofaScore y deja el resto). Revisar para que todas las fuentes compartan criterio de "ataque hacia la derecha".

### 4.2 Pipeline / Scanner — cableado a La Liga
- `db.get_seasons_in_db()` devuelve tuplas `("La Liga", season)` **hardcodeadas**, y `scanner.py` fija liga/temporada (`UNDERSTAT_SEASON="2020"`, `SOFASCORE_SEASON_NAME="20/21"`). El escáner solo tiene sentido para La Liga 2020/21.
- `_TOTAL_BY_SOURCE = 380` es el total de La Liga (mitigado en el fix 2.2, pero el escáner sigue siendo La-Liga-céntrico).

### 4.3 Players → Player Detail
- Cálculo de edad: `(_date_cls.today() - _bd)` con `_bd` proveniente del DataFrame (puede ser `Timestamp`). Suele funcionar por el `__rsub__` de pandas, pero es frágil; mejor `pd.Timestamp(_bd).date()`. 🔎 (`app.py` ~467-471)
- Toda la pestaña está en inglés (no usa `t()`).

### 4.4 Stadiums
- `display_df.columns = [21 nombres]` es **posicional** (`app.py` ~1882). Si algún día cambia el `SELECT` de `get_stadiums`, las columnas se renombran mal y/o salta `Length mismatch`. Mejor renombrar por diccionario `{col_real: etiqueta}`.

### 4.5 Exploración
- Varios `st.info("No ... data")` están en inglés.
- En Standings, `xG For/Against` son **totales de temporada** (ya avisado en el caption) — correcto, pero suele malinterpretarse como por-partido.

---

## 5. Qué cambiaría (mejoras generales)

1. **Cacheo total**: extender `@st.cache_data(ttl=...)` a todas las consultas de `explore.py`, `analytics.py` y `player_detail.py`, no solo a los selectores. Es el mayor salto de rendimiento.
2. **Idioma**: unificar con `t()` todo el texto hardcodeado en inglés (Player Detail entero, "Sort order/Descending/Ascending", muchos captions y métricas).
3. **Refactor de gráficos**: hay ~10 bloques de `matplotlib` casi idénticos (barras horizontales con tema oscuro, `fig/ax`, `facecolor`, `invert_yaxis`, `st.pyplot`+`plt.close`). Extraer un helper `barh_dark(labels, values, ...)` o pasar a `st.bar_chart`/Plotly: quitarías 300+ líneas duplicadas y ganarías interactividad (tooltips/zoom).
4. **Robustez de datos**: usar `NULLIF(denominador,0)` en todos los ratios (fill %, conversiones) y `try/except` con mensaje claro en las consultas que aún no lo tienen.
5. **Deduplicar**: `analytics._resolve_team_id` y `explore._team_id` hacen lo mismo; unificar en `db.py`.
6. **Tests**: no hay pruebas del dashboard. Añadir smoke tests que ejecuten cada función de `explore.py`/`analytics.py` contra una BD de prueba (o mockeada) evitaría regresiones como las de §2.

---

## 6. Cómo verificar los arreglos

```bash
streamlit run dashboard/app.py
```
- **Standings** (Exploración → pestaña equipos): selecciona **un equipo** y comprueba que *Goles/partido* y *xG/partido* ahora son coherentes (antes salían al doble).
- **Pipeline → Coverage**: cambia la competición y verifica que los conteos cambian (antes eran iguales para todas) y que el ratio /total solo aparece en La Liga.
- **Velocidad**: mover los selectores debería notarse más ágil (selectores cacheados).
