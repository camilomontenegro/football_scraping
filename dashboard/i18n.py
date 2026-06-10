"""
dashboard/i18n.py
=================
Lightweight ES/EN translation system for the Streamlit dashboard.

Usage:
    from dashboard.i18n import t, get_lang

    st.header(t("exploration"))
"""

from __future__ import annotations

import streamlit as st

TRANSLATIONS: dict[str, dict[str, str]] = {
    # ── Tab names ────────────────────────────────────────────
    "tab_exploration":       {"es": "Exploración",           "en": "Exploration"},
    "tab_teams":             {"es": "Equipos",               "en": "Teams"},
    "tab_goalkeepers":       {"es": "Porteros",              "en": "Goalkeepers"},
    "tab_players":           {"es": "Jugadores",             "en": "Players"},
    "tab_injuries":          {"es": "Lesiones",              "en": "Injuries"},
    "tab_shot_intelligence": {"es": "Inteligencia de tiro",  "en": "Shot Intelligence"},
    "tab_pass_network":      {"es": "Red de pases",          "en": "Pass Network"},
    "tab_stadiums":          {"es": "Estadios",              "en": "Stadiums"},
    "tab_pipeline":          {"es": "Monitorización",        "en": "Pipeline monitoring"},
    "tab_wizard":            {"es": "Wizard",                "en": "Wizard"},

    # ── Shared selectors ────────────────────────────────────
    "competition":           {"es": "Competición",           "en": "Competition"},
    "season":                {"es": "Temporada",             "en": "Season"},
    "team":                  {"es": "Equipo",                "en": "Team"},
    "all_teams":             {"es": "Todos los equipos",     "en": "All teams"},
    "all_seasons":           {"es": "Todas las temporadas",  "en": "All seasons"},
    "all_competitions":      {"es": "Todas las competiciones", "en": "All competitions"},
    "all_countries":         {"es": "Todos los países",      "en": "All countries"},
    "sort_order":            {"es": "Orden",                 "en": "Sort order"},
    "descending":            {"es": "Descendente",           "en": "Descending"},
    "ascending":             {"es": "Ascendente",            "en": "Ascending"},

    # ── Exploration tab ─────────────────────────────────────
    "exploration":           {"es": "Exploración",           "en": "Exploration"},
    "matches":               {"es": "Partidos",              "en": "Matches"},
    "goals":                 {"es": "Goles",                 "en": "Goals"},
    "results":               {"es": "Resultados",            "en": "Results"},
    "player_stats":          {"es": "Estadísticas de jugador", "en": "Player stats"},
    "shots_by_source":       {"es": "Tiros por fuente",      "en": "Shots by source"},
    "events":                {"es": "Eventos",               "en": "Events"},
    "wins":                  {"es": "Victorias",             "en": "Wins"},
    "draws":                 {"es": "Empates",               "en": "Draws"},
    "losses":                {"es": "Derrotas",              "en": "Losses"},
    "no_data":               {"es": "No hay datos para esta selección. Comprueba la cobertura del pipeline en la pestaña de monitorización.",
                              "en": "No data found for this selection. Check pipeline coverage in the Pipeline monitoring tab."},
    "no_seasons":            {"es": "No hay temporadas en la BD. Ejecuta el pipeline para poblar la base de datos.",
                              "en": "No seasons in the database yet. Run the pipeline to populate the database."},

    # ── Teams tab ───────────────────────────────────────────
    "select_season":         {"es": "Selecciona una temporada para ver las estadísticas.",
                              "en": "Select a season to view statistics."},
    "total_goals":           {"es": "Goles totales",         "en": "Total goals"},
    "avg_goals_match":       {"es": "Media goles/partido",   "en": "Avg goals/match"},
    "avg_xg_match":          {"es": "Media xG/partido",      "en": "Avg xG/match"},

    # ── Goalkeepers tab ─────────────────────────────────────
    "goalkeepers":           {"es": "Porteros",              "en": "Goalkeepers"},
    "gk_tracked":            {"es": "Porteros registrados",  "en": "Goalkeepers tracked"},
    "total_saves":           {"es": "Paradas totales",       "en": "Total saves"},
    "avg_save_pct":          {"es": "% paradas medio",       "en": "Avg save %"},
    "clean_sheets":          {"es": "Porterías a cero",      "en": "Clean sheets"},

    # ── Players tab ─────────────────────────────────────────
    "players_tracked":       {"es": "Jugadores registrados", "en": "Players tracked"},
    "yellow_cards":          {"es": "Tarjetas amarillas",    "en": "Yellow cards"},
    "red_cards":             {"es": "Tarjetas rojas",        "en": "Red cards"},

    # ── Injuries tab ────────────────────────────────────────
    "total_injuries":        {"es": "Total lesiones",        "en": "Total injuries"},
    "total_days_absent":     {"es": "Días de baja totales",  "en": "Total days absent"},
    "total_matches_missed":  {"es": "Partidos perdidos totales", "en": "Total matches missed"},
    "ongoing_injuries":      {"es": "Lesiones en curso",     "en": "Ongoing injuries"},
    "top_injury_types":      {"es": "Tipos de lesión más frecuentes", "en": "Top injury types"},
    "season_trend":          {"es": "Tendencia por temporada", "en": "Season trend"},

    # ── Shot Intelligence tab ───────────────────────────────
    "shot_intelligence":     {"es": "Inteligencia de tiro",  "en": "Shot Intelligence"},
    "metric":                {"es": "Métrica",               "en": "Metric"},
    "avg_xg_per_shot":       {"es": "xG medio por tiro",     "en": "Average xG per shot"},
    "conversion_rate":       {"es": "Tasa de conversión",    "en": "Conversion rate"},
    "pitch_danger_heatmap":  {"es": "Mapa de peligro",       "en": "Pitch Danger Heatmap"},
    "player_finishing":      {"es": "Calidad de definición", "en": "Player Finishing Quality"},
    "setpiece_specialists":  {"es": "Especialistas a balón parado", "en": "Set-piece Specialists"},
    "player_drilldown":      {"es": "Detalle por jugador",   "en": "Player drill-down"},
    "all_players":           {"es": "Todos los jugadores",   "en": "All players"},
    "zone_data_table":       {"es": "Tabla de datos por zona", "en": "Zone data table"},

    # ── Pass Network tab ────────────────────────────────────
    "pass_network":          {"es": "Red de pases",          "en": "Pass Network"},
    "match":                 {"es": "Partido",               "en": "Match"},
    "min_passes":            {"es": "Mínimo de pases entre jugadores", "en": "Min passes between players"},
    "no_pass_matches":       {"es": "No hay partidos con datos de pases (WhoScored) para esta selección.",
                              "en": "No matches with pass data (WhoScored) for this selection."},
    "no_pass_data":          {"es": "No hay datos de pases para este equipo en este partido.",
                              "en": "No pass data for this team in this match."},
    "total_passes":          {"es": "Pases completados",     "en": "Completed passes"},
    "pass_pairs":            {"es": "Conexiones",            "en": "Connections"},
    "top_connection":        {"es": "Mejor conexión",        "en": "Top connection"},

    # ── Stadiums tab ────────────────────────────────────────
    "stadiums":              {"es": "Estadios",              "en": "Stadiums"},
    "total_capacity":        {"es": "Aforo total",           "en": "Total capacity"},
    "avg_capacity":          {"es": "Aforo medio",           "en": "Avg capacity"},
    "largest":               {"es": "Mayor",                 "en": "Largest"},
    "country":               {"es": "País",                  "en": "Country"},
    "search_stadium":        {"es": "Buscar (estadio / equipo / ciudad)",
                              "en": "Search (stadium / team / city)"},
    "top_15_capacity":       {"es": "Top 15 por aforo",      "en": "Top 15 by capacity"},
    "stadium_select_hint":   {"es": "Haz clic en una fila de la tabla para ver la ficha del estadio.",
                              "en": "Click a table row to open the stadium detail panel."},
    "stadium_detail":        {"es": "Ficha del estadio",     "en": "Stadium detail"},
    "stadium_no_photo":      {"es": "Sin foto en Wikidata. Ejecuta el enricher de estadios para intentar obtenerla.",
                              "en": "No Wikidata photo yet. Run the stadium enricher to fetch one."},
    "stadium_wikipedia":     {"es": "Wikipedia",             "en": "Wikipedia"},
    "stadium_wikidata":      {"es": "Wikidata",              "en": "Wikidata"},
    "stadium_map_fallback":  {"es": "Ubicación (sin foto disponible)", "en": "Location (no photo available)"},

    # ── Pipeline monitoring tab ─────────────────────────────
    "pipeline_monitoring":   {"es": "Monitorización del pipeline", "en": "Pipeline monitoring"},
    "season_scanner":        {"es": "Escáner de temporadas", "en": "Season scanner"},
    "scan_all_sources":      {"es": "Escanear todas las fuentes", "en": "Scan all sources"},
    "coverage_by_source":    {"es": "Cobertura por fuente",  "en": "Coverage by source"},
    "player_review_queue":   {"es": "Cola de revisión de jugadores", "en": "Player review queue"},
    "recent_matches":        {"es": "Partidos recientes",    "en": "Recent matches"},
    "total":                 {"es": "Total",                 "en": "Total"},
    "unresolved":            {"es": "Sin resolver",          "en": "Unresolved"},
    "resolved":              {"es": "Resueltos",             "en": "Resolved"},
    "avg_similarity":        {"es": "Similitud media",       "en": "Avg similarity"},

    # ── Wizard tab ──────────────────────────────────────────
    "wizard":                {"es": "Wizard",                "en": "Wizard"},
    "wizard_warning":        {"es": "Esta pestaña escribe en la base de datos vía el pipeline de scraping. El resto de pestañas son de solo lectura.",
                              "en": "This tab writes to the database via the scraping pipeline. Every other tab is read-only."},
    "what_to_do":            {"es": "¿Qué quieres hacer?",   "en": "What do you want to do?"},
    "download_full_season":  {"es": "Descargar temporada completa",       "en": "Download full season"},
    "update_new_games":      {"es": "Actualizar datos con juegos nuevos", "en": "Update with new games"},
    "download_stadiums":     {"es": "Descargar estadios por temporada",   "en": "Download stadiums by season"},
    "data_sources":          {"es": "Fuente(s) de datos",    "en": "Data source(s)"},
    "match_filter":          {"es": "¿Cómo filtrar los partidos descargados?",
                              "en": "How to filter downloaded matches?"},
    "all_matches":           {"es": "Todos los partidos",    "en": "All matches"},
    "team_only":             {"es": "Sólo de un equipo",     "en": "Single team only"},
    "from_date":             {"es": "Desde una fecha",       "en": "From a date"},
    "start_date":            {"es": "Fecha de inicio",       "en": "Start date"},
    "operation_summary":     {"es": "Resumen de la operación", "en": "Operation summary"},
    "action":                {"es": "Acción",                "en": "Action"},
    "full_download":         {"es": "Descarga completa",     "en": "Full download"},
    "incremental_update":    {"es": "Actualización incremental", "en": "Incremental update"},
    "filter":                {"es": "Filtro",                "en": "Filter"},
    "run_pipeline":          {"es": "Ejecutar pipeline",     "en": "Run pipeline"},
    "pipeline_log":          {"es": "Log del pipeline",      "en": "Pipeline log"},
    "pipeline_completed":    {"es": "Pipeline completado correctamente.",
                              "en": "Pipeline completed successfully."},
    "pipeline_failed":       {"es": "Pipeline fallido",      "en": "Pipeline failed"},

    # ── Weather columns ─────────────────────────────────────
    "temperature":           {"es": "Temperatura (°C)",      "en": "Temperature (°C)"},
    "humidity":              {"es": "Humedad (%)",           "en": "Humidity (%)"},
    "precipitation":         {"es": "Precipitación (mm)",    "en": "Precipitation (mm)"},
    "wind_speed":            {"es": "Viento (km/h)",         "en": "Wind speed (km/h)"},
    "attendance":            {"es": "Asistencia",            "en": "Attendance"},

    # ── DB connection error ─────────────────────────────────
    "db_error":              {"es": "No se puede conectar a la base de datos. Revisa tu archivo .env.",
                              "en": "Cannot connect to the database. Check your .env file."},
}

LANGUAGES = {"Español": "es", "English": "en"}
DEFAULT_LANG = "es"


def get_lang() -> str:
    return st.session_state.get("app_language", DEFAULT_LANG)


def t(key: str, lang: str | None = None) -> str:
    lang = lang or get_lang()
    entry = TRANSLATIONS.get(key)
    if entry is None:
        return key
    return entry.get(lang, entry.get("es", key))
