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
    "tab_player_detail":     {"es": "Ficha del jugador",     "en": "Player Detail"},
    "tab_market_value":      {"es": "Valor de mercado",      "en": "Market Value"},
    "tab_transfer_history":  {"es": "Historial de fichajes", "en": "Transfer History"},
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

    # ── Player detail tab ─────────────────────────────────
    "player_detail":         {"es": "Ficha del jugador",     "en": "Player Detail"},
    "search_player":         {"es": "Buscar jugador",        "en": "Search player"},
    "search_player_ph":      {"es": "Escribe un nombre…",    "en": "Type a name…"},
    "select_player":         {"es": "Seleccionar jugador",   "en": "Select player"},
    "no_player_match":       {"es": "(sin coincidencias)",   "en": "(no match)"},
    "position":              {"es": "Posición",              "en": "Position"},
    "nationality":           {"es": "Nacionalidad",          "en": "Nationality"},
    "born":                  {"es": "Nacimiento",            "en": "Born"},
    "player_no_photo":       {"es": "Sin foto disponible para este jugador.",
                              "en": "No photo available for this player."},
    "source_identity_mdm":   {"es": "Identidad en fuentes (MDM)", "en": "Source Identity (MDM)"},
    "no_source_aliases":     {"es": "No hay alias de fuentes registrados para este jugador.",
                              "en": "No source aliases recorded for this player."},
    "mdm_source":            {"es": "Fuente",                "en": "Source"},
    "mdm_name_used":         {"es": "Nombre usado",          "en": "Name used"},
    "mdm_source_id":         {"es": "ID en fuente",          "en": "Source ID"},
    "mdm_score":             {"es": "Puntuación",            "en": "Score"},
    "mdm_resolved":          {"es": "Resuelto",              "en": "Resolved"},
    "shot_map":              {"es": "Mapa de tiros",         "en": "Shot Map"},
    "pd_match_filter":       {"es": "Partido",               "en": "Match"},
    "all_matches_filter":    {"es": "Todos",                 "en": "All"},
    "no_shot_data_selection":{"es": "No hay tiros para esta selección.",
                              "en": "No shot data found for this selection."},
    "no_shot_data_player":   {"es": "No hay datos de tiros para este jugador.",
                              "en": "No shot data available for this player."},
    "shots_metric":          {"es": "Tiros",                 "en": "Shots"},
    "goals_minus_xg":       {"es": "Goles − xG",            "en": "Goals − xG"},
    "shot_map_no_goal":      {"es": "Sin gol",               "en": "No goal"},
    "shot_map_goal":         {"es": "Gol",                   "en": "Goal"},
    "seasonal_stats":        {"es": "Estadísticas por temporada", "en": "Seasonal Stats"},
    "col_competition":       {"es": "Competición",           "en": "Competition"},
    "injury_history":        {"es": "Historial de lesiones", "en": "Injury History"},
    "no_injury_records":     {"es": "No hay registros de lesiones para este jugador.",
                              "en": "No injury records found for this player."},
    "injury_type":           {"es": "Tipo de lesión",        "en": "Injury type"},
    "date_from":             {"es": "Desde",                 "en": "Date from"},
    "date_until":            {"es": "Hasta",                 "en": "Date until"},
    "days_absent":           {"es": "Días de baja",          "en": "Days absent"},
    "matches_missed":        {"es": "Partidos perdidos",     "en": "Matches missed"},
    "install_mplsoccer":     {"es": "Instala mplsoccer: pip install mplsoccer",
                              "en": "Install mplsoccer: pip install mplsoccer"},

    "yellow_cards":          {"es": "Tarjetas amarillas",    "en": "Yellow cards"},
    "red_cards":             {"es": "Tarjetas rojas",        "en": "Red cards"},

    # ── Market Value tab ────────────────────────────────────

    "mv_title":              {"es": "Historial de valor de mercado", "en": "Market Value History"},
    "mv_current_value":      {"es": "Valor actual",             "en": "Current Value"},
    "mv_peak_value":         {"es": "Valor pico",               "en": "Peak Value"},
    "mv_from_peak":          {"es": "Desde el pico",            "en": "From Peak"},
    "mv_last_year_change":   {"es": "Cambio último año",        "en": "Last Year Change"},
    "mv_transfers":          {"es": "Fichajes",                 "en": "Transfers"},
    "mv_compare_player":     {"es": "Comparar con otro jugador (opcional)", "en": "Compare with another player (optional)"},
    "mv_select_comparison":  {"es": "Seleccionar jugador de comparación", "en": "Select comparison player"},
    
    # Market value chart 
    "mv_show_benchmark":     {"es": "Mostrar banda de referencia por posición", "en": "Show position benchmark band"},
    "mv_no_data":            {"es": "No hay datos de valor de mercado para este jugador.", "en": "No market value data found for this player."},
    "mv_caption":            {"es": "Gráfico de escalones: el valor se mantiene fijo hasta la siguiente tasación", "en": "Step chart: value stays flat until next valuation"},
    "mv_benchmark_explain":  {"es": "**🟢 Línea discontinua:** valor típico para un jugador de la misma posición y edad *(la mitad de jugadores similares valen más, la mitad menos)*\n\n**🟩 Banda verde:** el rango normal para esa edad y posición\n- Por encima → excepcionalmente valioso\n- Por debajo → por debajo de la media para su perfil",
                            "en": "**🟢 Dashed line:** typical market value for a player of the same position and age *(half of similar players are worth more, half less)*\n\n**🟩 Green band:** the normal range for that age and position\n- Above the band → exceptionally valuable\n- Below the band → below average for their profile"},
    # Legend 
    "mv_legend_transfer":    {"es": "Traspaso",          "en": "Transfer"},
    "mv_legend_loan":        {"es": "Cesión",             "en": "Loan"},
    "mv_legend_end_of_loan": {"es": "Fin de cesión",      "en": "End of loan"},
    "mv_legend_free":        {"es": "Libre",              "en": "Free transfer"},
    "mv_legend_injury":      {"es": "Lesión",             "en": "Injury"},

    # Hover information 
    "mv_hover_title":    {"es": "Valor de mercado",  "en": "Market value"},
    "mv_hover_club": {"es": "Club", "en": "Club"},
    "mv_hover_value":    {"es": "Valor",             "en": "Value"},
    "mv_hover_from_team":  {"es": "Equipo origen",  "en": "From"},
    "mv_hover_to_team":    {"es": "Equipo destino", "en": "To"},     
    "mv_hover_season":  {"es": "Temporada", "en": "Season"},
    "mv_hover_absent":  {"es": "Días de baja", "en": "Absent"},
    "mv_hover_injury":  {"es": "Lesión",    "en": "Injury"},



    # ── Career History ──────────────────────────────────────
    "tab_career_history":    {"es": "Historial de equipos",     "en": "Career History"},
    "career_date_from":      {"es": "Fecha llegada",            "en": "Date From"},
    "career_date_to":        {"es": "Fecha salida",             "en": "Date To"},
    "career_team":           {"es": "Equipo",                   "en": "Team"},
    "career_no_data":        {"es": "No hay datos de fichajes para este jugador.",
                              "en": "No transfer data found for this player."},


    # ── Injuries tab ────────────────────────────────────────
    "total_injuries":        {"es": "Total lesiones",        "en": "Total injuries"},
    "total_days_absent":     {"es": "Días de baja totales",  "en": "Total days absent"},
    "total_matches_missed":  {"es": "Partidos perdidos totales", "en": "Total matches missed"},
    "ongoing_injuries":      {"es": "Lesiones en curso",     "en": "Ongoing injuries"},
    "top_injury_types":      {"es": "Tipos de lesión más frecuentes", "en": "Top injury types"},
    "season_trend":          {"es": "Tendencia por temporada", "en": "Season trend"},

    # ── Transfer History tab ────────────────────────────────
    "transfer_total_fees":    {"es": "Coste total de fichajes",  "en": "Total transfer fees"},
    "transfer_most_expensive":{"es": "Fichaje más caro",         "en": "Most expensive transfer"},
    "transfer_current_team":  {"es": "Último equipo conocido",   "en": "Last known team"},
    "transfer_num_teams":     {"es": "Equipos distintos",        "en": "Teams"},

    "transfer_no_data":       {"es": "No hay datos de fichajes para este jugador.",
                            "en": "No transfer data found for this player."},
    "transfer_col_season":    {"es": "Temporada",                "en": "Season"},
    "transfer_col_date":      {"es": "Fecha",                    "en": "Date"},
    "transfer_col_from":      {"es": "Equipo origen",            "en": "From"},
    "transfer_col_to":        {"es": "Equipo destino",           "en": "To"},
    "transfer_col_fee":       {"es": "Coste",                    "en": "Fee"},
    "transfer_col_type":      {"es": "Tipo",                     "en": "Type"},
    "transfer_caption":       {"es": "Fuente: fact_transfers (Transfermarkt) · Coste solo para traspasos permanentes · Cesiones y libres excluidos del total",
                            "en": "Source: fact_transfers (Transfermarkt) · Fee shown for permanent transfers only · Loans and free transfers excluded from total fees"},
    "transfer_type_transfer":  {"es": "Traspaso",      "en": "Transfer"},
    "transfer_type_loan":      {"es": "Cesión",         "en": "Loan"},
    "transfer_type_end_of_loan":{"es": "Fin de cesión", "en": "End of loan"},
    "transfer_type_free":      {"es": "Libre",          "en": "Free"},
    "transfer_type_unknown":   {"es": "Desconocido",    "en": "Unknown"},

    # transfer history charts 
    "transfer_career_timeline":     {"es": "Línea temporal de equipos",     "en": "Career Timeline"},
    "transfer_fees_chart":          {"es": "Costes de fichajes",             "en": "Transfer & Loan Fees"},
    "transfer_no_career_data":      {"es": "No hay datos de carrera para este jugador.",
                                    "en": "No career data found for this player."},
    "transfer_no_fee_data":         {"es": "No hay fichajes con coste conocido para este jugador.",
                                    "en": "No transfers with known fee for this player."},
    "transfer_hover_from":          {"es": "Desde",        "en": "From"},
    "transfer_hover_to":            {"es": "Hasta",        "en": "To"},
    "transfer_hover_duration":      {"es": "Duración",     "en": "Duration"},
    "transfer_hover_fee":           {"es": "Coste",        "en": "Fee"},
    "transfer_hover_date":          {"es": "Fecha",        "en": "Date"},
    "transfer_hover_type":          {"es": "Tipo",         "en": "Type"},
    "transfer_present":             {"es": "Presente",     "en": "Present"},
        


    # ── Shot Intelligence tab ───────────────────────────────
    "shot_intelligence":     {"es": "Inteligencia de tiro",  "en": "Shot Intelligence"},
    "metric":                {"es": "Métrica",               "en": "Metric"},
    "avg_xg_per_shot":       {"es": "xG medio por tiro",     "en": "Average xG per shot"},
    "conversion_rate":       {"es": "Tasa de conversión",    "en": "Conversion rate"},
    "pitch_danger_heatmap":  {"es": "Mapa de peligro",       "en": "Pitch Danger Heatmap"},
    "pitch_view":            {"es": "Visualización",         "en": "Visualization"},
    "goal_mouth":            {"es": "Vista de portería",     "en": "Goal Mouth"},
    "no_shot_data":          {"es": "No hay tiros (WhoScored) para este equipo en este partido.",
                              "en": "No shots (WhoScored) for this team in this match."},
    "no_goalmouth_data":     {"es": "No hay coordenadas de portería (goalMouthY/goalMouthZ) para los tiros de este equipo en este partido.",
                              "en": "No goal-mouth coordinates (goalMouthY/goalMouthZ) for this team's shots in this match."},
    "decided_on_penalties":  {"es": "Decidido en la tanda de penaltis",
                              "en": "Decided on penalties"},
    "penalty_shootout":      {"es": "Tanda de penaltis",       "en": "Penalty shootout"},
    "no_shootout_data":      {"es": "No hay datos de la tanda de penaltis para este equipo.",
                              "en": "No penalty-shootout data for this team."},
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
    "stadium_select_hint":   {"es": "Haz clic en una fila de la tabla o elige un estadio abajo para ver su ficha.",
                              "en": "Click a table row or pick a stadium below to open the detail panel."},
    "stadium_view_select":   {"es": "Estadio a visualizar",  "en": "Stadium to view"},
    "stadium_detail":        {"es": "Ficha del estadio",     "en": "Stadium detail"},
    "stadium_no_photo":      {"es": "Sin foto disponible (ni Cloudinary ni Wikidata).",
                              "en": "No photo available (neither Cloudinary nor Wikidata)."},
    "stadium_wikipedia":     {"es": "Wikipedia",             "en": "Wikipedia"},
    "stadium_wikidata":      {"es": "Wikidata",              "en": "Wikidata"},
    "stadium_map_fallback":  {"es": "Ubicación (sin foto disponible)", "en": "Location (no photo available)"},
    "stadium_name_history":  {"es": "Historial de nombres", "en": "Name history"},
    "stadium_name_current":  {"es": "actual",                "en": "current"},
    "stadium_name_from":     {"es": "desde",                 "en": "from"},
    "stadium_name_until":    {"es": "hasta",                 "en": "until"},
    "stadium_name_history_none": {
        "es": "Sin cambios de nombre documentados.",
        "en": "No documented name changes.",
    },
    "stadium_location":      {"es": "Ubicación",             "en": "Location"},
    "stadium_no_coords":     {
        "es": "Sin coordenadas geográficas en la base de datos.",
        "en": "No geographic coordinates in the database.",
    },
    "stadium_open_maps":     {"es": "Abrir en Google Maps",  "en": "Open in Google Maps"},

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
    "load_to_db":            {"es": "Cargar datos en la base de datos",   "en": "Load data into database"},
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

    # ── Match Context tab ───────────────────────────────────
    "tab_match_context":     {"es": "Contexto de partido",   "en": "Match Context"},
    "temperature":           {"es": "Temperatura (°C)",      "en": "Temperature (°C)"},
    "humidity":              {"es": "Humedad (%)",           "en": "Humidity (%)"},
    "precipitation":         {"es": "Precipitación (mm)",    "en": "Precipitation (mm)"},
    "wind_speed":            {"es": "Viento (km/h)",         "en": "Wind speed (km/h)"},
    "attendance":            {"es": "Asistencia",            "en": "Attendance"},
    "weather_section":       {"es": "Clima por partido",     "en": "Weather by match"},
    "attendance_section":    {"es": "Asistencia por partido", "en": "Attendance by match"},
    "referees_section":      {"es": "Árbitros",              "en": "Referees"},
    "managers_section":      {"es": "Managers / Entrenadores", "en": "Managers"},
    "avg_temp":              {"es": "Temp. media",           "en": "Avg temperature"},
    "min_temp":              {"es": "Temp. mín.",            "en": "Min temperature"},
    "max_temp":              {"es": "Temp. máx.",            "en": "Max temperature"},
    "rainy_matches":         {"es": "Partidos con lluvia",   "en": "Rainy matches"},
    "matches_with_weather":  {"es": "Partidos con meteo",    "en": "Matches with weather"},
    "avg_attendance":        {"es": "Asistencia media",      "en": "Avg attendance"},
    "max_attendance":        {"es": "Máx. asistencia",       "en": "Max attendance"},
    "total_attendance":      {"es": "Asistencia total",      "en": "Total attendance"},
    "matches_officiated":    {"es": "Partidos dirigidos",    "en": "Matches officiated"},
    "manager_record":        {"es": "Registro del manager",  "en": "Manager record"},
    "points_pct":            {"es": "% de puntos",           "en": "Points %"},
    "temp_over_season":      {"es": "Temperatura a lo largo de la temporada", "en": "Temperature over the season"},
    "attendance_by_team":    {"es": "Asistencia media por equipo (local)", "en": "Avg home attendance by team"},
    "no_weather_data":       {"es": "No hay datos meteorológicos para esta selección.", "en": "No weather data for this selection."},
    "no_attendance_data":    {"es": "No hay datos de asistencia para esta selección.", "en": "No attendance data for this selection."},
    "stadium_fill_viz":      {"es": "Llenado del estadio",   "en": "Stadium fill"},
    "stadium_fill_select":   {"es": "Partido / vista",       "en": "Match / view"},
    "stadium_fill_avg":      {"es": "Media de la temporada", "en": "Season average"},
    "stadium_fill_caption":  {
        "es": "Las gradas se rellenan de abajo arriba; el color (rojo → verde) indica el % de aforo.",
        "en": "Stands fill bottom-up; color (red → green) reflects occupancy %.",
    },
    "stadium_fill_no_cap":   {
        "es": "Sin capacidad en dim_stadium para calcular el llenado visual.",
        "en": "No dim_stadium capacity — visual fill unavailable.",
    },
    "no_referee_data":       {"es": "No hay datos de árbitros. Ejecuta la migración add_dim_referee.sql y carga datos.", "en": "No referee data. Run add_dim_referee.sql migration and load data."},
    "no_manager_data":       {"es": "No hay datos de managers. Ejecuta add_whoscored_stats.sql y el extractor.", "en": "No manager data. Run add_whoscored_stats.sql and the extractor."},
    "players_tracked":       {"es": "Jugadores registrados", "en": "Players tracked"},

    # ── Match (per-match context) ───────────────────────────
    "match_detail_section":  {"es": "Partido",               "en": "Match"},
    "match_select":          {"es": "Selecciona un partido", "en": "Select a match"},
    "no_matches_found":      {"es": "No hay partidos para esta selección.", "en": "No matches for this selection."},
    "match_officials":       {"es": "Arbitraje",             "en": "Officials"},
    "referee":               {"es": "Árbitro",               "en": "Referee"},
    "humidity":              {"es": "Humedad",               "en": "Humidity"},
    "precipitation":         {"es": "Precipitación",         "en": "Precipitation"},
    "wind":                  {"es": "Viento",                "en": "Wind"},
    "home_label":            {"es": "Local",                 "en": "Home"},
    "away_label":            {"es": "Visitante",             "en": "Away"},

    # ── Cards & fouls / chalkboard / diagnostics ────────────
    "cards_fouls_section":   {"es": "Tarjetas y faltas",     "en": "Cards & fouls"},
    "chalkboard_section":    {"es": "Pizarra",               "en": "Chalkboard"},
    "event_diagnostics":     {"es": "Diagnóstico de eventos", "en": "Event diagnostics"},
    "action_heatmap":        {"es": "Mapa de calor de acciones", "en": "Action heatmap"},
    "fouls":                 {"es": "Faltas",                "en": "Fouls"},
    "total_cards":           {"es": "Tarjetas totales",      "en": "Total cards"},
    "cards_per_match":       {"es": "Tarjetas/partido",      "en": "Cards/match"},
    "fouls_per_match":       {"es": "Faltas/partido",        "en": "Fouls/match"},
    "min_matches":           {"es": "Mínimo de partidos",    "en": "Min. matches"},
    "action_type":           {"es": "Tipo de acción",        "en": "Action type"},
    "passes":                {"es": "Pases",                 "en": "Passes"},
    "tackles":               {"es": "Entradas",              "en": "Tackles"},
    "shots":                 {"es": "Tiros",                 "en": "Shots"},
    "no_event_data":         {"es": "No hay eventos WhoScored para esta selección.",
                              "en": "No WhoScored events for this selection."},

    # ── DB connection error ─────────────────────────────────
    "db_error":              {"es": "No se puede conectar a la base de datos. Revisa tu archivo .env.",
                              "en": "Cannot connect to the database. Check your .env file."},

    # ── Page / sidebar ──────────────────────────────────────
    "page_title":            {"es": "Dashboard de fútbol",     "en": "Football Scraping Dashboard"},
    "lang_label":            {"es": "🌐 Idioma",               "en": "🌐 Language / Idioma"},
    "filter_all":            {"es": "Todos",                   "en": "All"},
    "no_seasons_in_db":      {"es": "(sin temporadas en BD)",  "en": "(no seasons in DB)"},
    "no_seasons_paren":      {"es": "(sin temporadas)",        "en": "(no seasons)"},
    "none_option":           {"es": "(ninguna)",               "en": "(none)"},
    "count":                 {"es": "Recuento",                "en": "Count"},
    "source":                {"es": "Fuente",                  "en": "Source"},
    "metric_label":          {"es": "Métrica",                 "en": "Metric"},
    "overall":               {"es": "Global",                  "en": "Overall"},
    "ongoing":               {"es": "En curso",                "en": "Ongoing"},
    "unknown_date":          {"es": "Fecha desconocida",       "en": "Unknown date"},
    "shots_xg_metric":       {"es": "Tiros (xG)",              "en": "Shots (xG)"},
    "scanning_spinner":      {"es": "Escaneando todas las fuentes…", "en": "Scanning all sources..."},
    "scanner_errors":        {"es": "Errores del escáner",     "en": "Scanner errors"},
    "load_missing_cli":      {
        "es": "Para cargar temporadas faltantes, ejecuta:\n\n    python pipeline_runner.py --sources <fuente>\n\nLa carga es solo por CLI en este dashboard.",
        "en": "To load missing seasons, run:\n\n    python pipeline_runner.py --sources <source>\n\nLoading is intentionally CLI-only in this dashboard.",
    },
    "all_sources_up_to_date": {
        "es": "Todas las fuentes escaneadas están al día — no hay temporadas pendientes.",
        "en": "All scanned sources are up-to-date — no missing seasons.",
    },
    "resolve_player_cli":    {
        "es": "Para resolver un caso, ejecuta:\n\n    python -m scripts.review_players --unresolved",
        "en": "To resolve a case, run:\n\n    python -m scripts.review_players --unresolved",
    },
    "no_unresolved_review":  {
        "es": "No hay entradas sin resolver en `player_review`.",
        "en": "No unresolved entries in `player_review`.",
    },
    "no_matches_dim":        {
        "es": "No hay partidos en `dim_match` todavía.",
        "en": "No matches in `dim_match` yet.",
    },
    "sofascore_incident_caption": {
        "es": "Los eventos SofaScore son solo incidencias. Las coordenadas son NULL por diseño.",
        "en": "SofaScore events are incident-only. Coordinates are NULL by design.",
    },
    "whoscored_events_season": {"es": "Eventos WhoScored por temporada", "en": "WhoScored events by season"},
    "event_types_xy":        {"es": "event_type disponibles (con coordenadas)", "en": "event_type available (with coordinates)"},
    "events_col":            {"es": "Eventos",                 "en": "Events"},
    "with_xy":               {"es": "Con x/y",                 "en": "With x/y"},
    "event_diag_caption":    {
        "es": "Úsalo para confirmar qué temporadas tienen eventos y los nombres exactos de event_type (pases, entradas, faltas, tarjetas…).",
        "en": "Use this to confirm which seasons have events and the exact event_type names (passes, tackles, fouls, cards…).",
    },

    # ── Exploration captions / messages ─────────────────────
    "no_shot_data_pipeline": {
        "es": "No hay tiros para esta selección. Revisa la cobertura del pipeline en la pestaña de monitorización.",
        "en": "No shot data found for this selection. Check pipeline coverage in the monitoring tab.",
    },
    "no_event_data_selection": {
        "es": "No hay eventos para esta selección.",
        "en": "No event data found for this selection.",
    },
    "no_match_data_pipeline": {
        "es": "No hay datos de partidos. Ejecuta pipeline_runner.py para poblar dim_match.",
        "en": "No match data found. Run pipeline_runner.py to populate dim_match.",
    },
    "caption_player_stats":  {
        "es": "Fuente: fact_shots (todas las fuentes — StatsBomb, Understat, SofaScore).",
        "en": "Source: fact_shots (all sources combined — StatsBomb, Understat, SofaScore).",
    },
    "caption_shots_by_source": {
        "es": "Cada fuente cubre distintos tipos de evento. Understat y StatsBomb incluyen xG. Los tiros SofaScore pueden tener coordenadas NULL.",
        "en": "Each source covers different event types. Understat and StatsBomb include xG. SofaScore shots may have NULL coordinates.",
    },
    "caption_events_summary": {
        "es": "Los eventos SofaScore son solo incidencias (tarjetas, sustituciones, VAR) — coordenadas NULL por diseño. WhoScored y StatsBomb incluyen coordenadas x/y.",
        "en": "SofaScore events are incident-only (cards, substitutions, VAR) — coordinates are NULL by design. WhoScored and StatsBomb events include x/y coordinates.",
    },
    "caption_standings":     {
        "es": "Fuente: dim_match (todas las fuentes) · xG y tiros: fact_shots · xG a favor/en contra = total de temporada (suma de partidos, no por tiro)",
        "en": "Source: dim_match (all sources combined) · xG and shots: fact_shots · xG For/Against = season-total expected goals (sum across all matches, not per-shot)",
    },

    # ── Standings columns ───────────────────────────────────
    "col_played":            {"es": "Jugados",                 "en": "Played"},
    "col_won":               {"es": "Ganados",                 "en": "Won"},
    "col_drawn":             {"es": "Empatados",               "en": "Drawn"},
    "col_lost":              {"es": "Perdidos",                "en": "Lost"},
    "col_gf":                {"es": "Goles a favor",           "en": "Goals For"},
    "col_ga":                {"es": "Goles en contra",         "en": "Goals Against"},
    "col_gd":                {"es": "Dif. goles",              "en": "Goal Diff"},
    "col_xg_for":            {"es": "xG a favor (temporada)",  "en": "xG For (season total)"},
    "col_xg_against":        {"es": "xG en contra (temporada)", "en": "xG Against (season total)"},
    "col_shots_for":         {"es": "Tiros a favor",           "en": "Shots For"},
    "col_shots_against":     {"es": "Tiros en contra",         "en": "Shots Against"},

    # ── Players / discipline ──────────────────────────────────
    "no_player_data":        {"es": "No hay datos de jugadores para esta selección.", "en": "No player data found for this selection."},
    "caption_discipline":    {
        "es": "Goles y xG: fact_shots (todas las fuentes) · Tarjetas: fact_events (incidencias SofaScore + StatsBomb)",
        "en": "Goals and xG: fact_shots (all sources) · Cards: fact_events (SofaScore incidents + StatsBomb)",
    },
    "col_player":            {"es": "Jugador",                 "en": "Player"},
    "col_matches":           {"es": "Partidos",                "en": "Matches"},
    "top_scorers":           {"es": "Máximos goleadores",      "en": "Top scorers"},

    # ── Goalkeepers ─────────────────────────────────────────
    "no_gk_data":            {"es": "No hay datos de porteros para esta selección.", "en": "No goalkeeper data found for this selection."},
    "col_goals_allowed":     {"es": "Goles encajados",         "en": "Goals Allowed"},
    "col_saves":             {"es": "Paradas",                 "en": "Saves"},
    "col_save_pct":          {"es": "% paradas",               "en": "Save %"},
    "col_clean_sheets":      {"es": "Porterías a cero",        "en": "Clean Sheets"},
    "col_gsae":              {"es": "Goles salvados sobre lo esperado", "en": "Goals Saved Above Expected"},
    "caption_gsae":          {
        "es": "Goles salvados sobre lo esperado = paradas − xG encajado (positivo = por encima de lo esperado)",
        "en": "Goals Saved Above Expected = saves − xG conceded (positive = outperforming)",
    },

    # ── Player detail extras ──────────────────────────────────
    "search_rival":          {"es": "Buscar rival",            "en": "Search rival"},
    "select_player_compare": {"es": "Jugador a comparar",      "en": "Select player to compare"},
    "compare_mode_player":   {"es": "Otro jugador",            "en": "Another player"},
    "caption_radar_player":  {"es": "Medias por partido. El % de conversión es por tiro.", "en": "Per-match averages. Conversion % is per-shot."},
    "caption_radar_league":  {
        "es": "Medias por partido vs liga (excluyendo este jugador). El % de conversión es por tiro.",
        "en": "Per-match averages vs league (excluding this player). Conversion % is per-shot.",
    },
    "no_shot_cmp_player":    {
        "es": "No hay tiros de este jugador en la misma competición/temporada.",
        "en": "No shot data for this player in the same competition/season.",
    },
    "not_enough_league":     {"es": "No hay suficientes datos de liga para calcular la media.", "en": "Not enough league data to compute average."},
    "col_season_short":      {"es": "Temporada",               "en": "Season"},
    "col_shots":             {"es": "Tiros",                   "en": "Shots"},
    "col_xg":                {"es": "xG",                      "en": "xG"},
    "goals_per_match":       {"es": "Goles/partido",           "en": "Goals/Match"},
    "caption_action_heatmap": {
        "es": "{n} acciones localizadas · fact_events (WhoScored) · ataque hacia la derecha.",
        "en": "{n} located actions · fact_events (WhoScored) · attack towards the right.",
    },
    "mdm_expander":          {"es": "Identidad en fuentes (MDM)", "en": "Source Identity (MDM)"},
    "no_injury_data":        {"es": "No hay datos de lesiones para esta selección.", "en": "No injury data found for this selection."},
    "caption_injuries":      {
        "es": "Fuente: fact_injuries (Transfermarkt)\ndate_until = NULL significa que el jugador seguía lesionado al recoger los datos.",
        "en": "Source: fact_injuries (Transfermarkt)\ndate_until = NULL means the player was still injured at time of data collection.",
    },

    # ── Shot intelligence ─────────────────────────────────────
    "si_caption_coords":     {
        "es": "Todas las fuentes · Coordenadas del campo: 105 m × 68 m · Normalizadas a metros",
        "en": "All sources · Pitch coordinates: 105 m × 68 m · Coordinates normalised to metres",
    },
    "si_metric_avg_xg":      {"es": "xG medio por tiro",       "en": "Average xG per shot"},
    "si_metric_conversion":  {"es": "Tasa de conversión",      "en": "Conversion rate"},
    "si_avg_xg_label":       {"es": "xG medio",                "en": "Avg xG"},
    "si_conversion_label":   {"es": "Tasa de conversión",      "en": "Conversion Rate"},
    "no_shots_coords":       {
        "es": "No hay tiros con coordenadas para esta selección.",
        "en": "No shot data with coordinates for this selection.",
    },
    "hm_title":              {"es": "{metric} por zona — {season} · {scope}", "en": "{metric} by zone — {season} · {scope}"},
    "si_finishing_caption":  {
        "es": "Mín. 20 tiros para clasificar · Goles − xG: positivo = por encima de lo esperado",
        "en": "Min. 20 shots to qualify · Goals − xG: positive = overperforming",
    },
    "no_players_20_shots":   {
        "es": "No hay jugadores con 20+ tiros para esta selección.",
        "en": "No players with 20+ shots for this selection.",
    },
    "no_setpiece_data":      {"es": "No hay goles a balón parado para esta selección.", "en": "No set-piece goal data for this selection."},
    "col_penalty_goals":     {"es": "Goles de penalti",        "en": "Penalty Goals"},
    "col_freekick_goals":    {"es": "Goles de falta",          "en": "Free Kick Goals"},
    "col_openplay_goals":    {"es": "Goles en juego abierto",  "en": "Open Play Goals"},
    "col_setpiece_other":    {"es": "Balón parado / Otros",    "en": "Set Piece / Other"},
    "col_total_goals":       {"es": "Goles totales",           "en": "Total Goals"},
    "si_setpiece_caption":   {
        "es": "Fuente: fact_shots (todas las fuentes) · Penalti = situación 'penalty' · Falta = 'direct freekick' / 'free-kick'",
        "en": "Source: fact_shots (all sources) · Penalty = situation 'penalty' · Free Kick = 'direct freekick' / 'free-kick'",
    },
    "zone_data_expander":    {"es": "Tabla de datos por zona", "en": "Zone data table"},

    # ── Pass network ──────────────────────────────────────────
    "pn_caption":            {
        "es": "Fuente: fact_events (WhoScored) · Solo pases completados cuyo siguiente evento es del mismo equipo · Grosor/opacidad ∝ pases entre la pareja · Tamaño del nodo ∝ pases realizados",
        "en": "Source: fact_events (WhoScored) · Only successful passes where the next event belongs to the same team · Edge width/opacity ∝ passes between the pair (both directions combined) · Node size ∝ passes made",
    },

    # ── Match context ─────────────────────────────────────────
    "col_stadium":           {"es": "Estadio",                 "en": "Stadium"},
    "col_home_team":         {"es": "Equipo local",            "en": "Home Team"},
    "col_away_team":         {"es": "Equipo visitante",        "en": "Away Team"},
    "stadium_venue_select":  {"es": "Estadio / sede",          "en": "Stadium / Venue"},
    "fill_pct":              {"es": "% ocupación",             "en": "Fill %"},
    "empty_seats":           {"es": "Asientos vacíos",         "en": "Empty seats"},
    "col_home_matches":      {"es": "Partidos en casa",        "en": "Home Matches"},
    "col_stadium_capacity":  {"es": "Aforo del estadio",       "en": "Stadium Capacity"},
    "col_raw_venue":         {"es": "Sede bruta",              "en": "Raw Venue"},
    "col_capacity":          {"es": "Aforo",                   "en": "Capacity"},
    "select_stadium_team":   {
        "es": "Selecciona un estadio o equipo para ver la evolución por temporada.",
        "en": "Select a stadium or team to see the season trend.",
    },

    # ── Stadiums tab ────────────────────────────────────────
    "stadium_caption":       {
        "es": "Estadios por equipo — fuentes: Transfermarkt + enriquecimiento Wikidata. Modelo SCD2: una fila por estado del estadio. Los partidos usan match_stadium_id (sedes neutrales incluidas).",
        "en": "Stadiums per team — sources: Transfermarkt + Wikidata enrichment. SCD2 model: one row per stadium state. Matches use match_stadium_id (neutral venues included).",
    },
    "stadium_table_missing": {
        "es": "La tabla `dim_stadium` no existe todavía. Aplica la migración:\n\n    psql -U postgres -d football_db -f db/add_dim_stadium.sql\n\nY luego carga datos desde el wizard (\"Descargar estadios por temporada\").",
        "en": "Table `dim_stadium` does not exist yet. Apply migration:\n\n    psql -U postgres -d football_db -f db/add_dim_stadium.sql\n\nThen load data from the wizard (\"Download stadiums by season\").",
    },
    "stadium_include_venues": {
        "es": "Incluir sedes solo de partido (match-venue)",
        "en": "Include match-only venues (match-venue)",
    },
    "stadium_no_results":    {
        "es": "No hay estadios para esta combinación de filtros. Si acabas de migrar la tabla, lanza desde el wizard \"Descargar estadios por temporada\" para poblarla.",
        "en": "No stadiums for this filter combination. If you just migrated the table, run \"Download stadiums by season\" from the wizard to populate it.",
    },
    "col_seats":             {"es": "Asientos",                "en": "Seats"},
    "col_built":             {"es": "Inauguración",            "en": "Built"},
    "col_owner":             {"es": "Propietario",             "en": "Owner"},
    "col_city":              {"es": "Ciudad",                  "en": "City"},
    "col_surface":           {"es": "Superficie",              "en": "Surface"},
    "col_architect":         {"es": "Arquitecto",              "en": "Architect"},
    "col_lat":               {"es": "Lat",                     "en": "Lat"},
    "col_lon":               {"es": "Lon",                     "en": "Lon"},
    "col_altitude":          {"es": "Altitud m",               "en": "Altitude m"},
    "col_timezone":          {"es": "Zona horaria",            "en": "Timezone"},
    "col_tm_url":            {"es": "URL Transfermarkt",       "en": "Transfermarkt URL"},
    "tm_link_label":         {"es": "Transfermarkt",           "en": "Transfermarkt"},
    "tm_open":               {"es": "abrir",                   "en": "open"},
    "stadium_footer_caption": {
        "es": "Fuente: dim_stadium (Transfermarkt + Wikidata, SCD2). Partidos enlazan vía match_stadium_id; el % de asistencia usa la capacidad del estadio real del partido.",
        "en": "Source: dim_stadium (Transfermarkt + Wikidata, SCD2). Matches link via match_stadium_id; attendance fill % uses the actual match stadium capacity.",
    },

    # ── Player detail header / stats ──────────────────────────
    "born_fmt":              {"es": "**Nacimiento:** {date} ({age} años)", "en": "**Born:** {date} ({age} yrs)"},
    "years_old":             {"es": "años",                  "en": "yrs"},
    "statistics_title":      {"es": "Estadísticas — {season}", "en": "Statistics — {season}"},
    "penalties":             {"es": "Penaltis",                "en": "Penalties"},
    "penalty_goals":         {"es": "Goles de penalti",        "en": "Penalty Goals"},
    "conversion_pct":        {"es": "% conversión",            "en": "Conversion %"},
    "xg_per_shot":           {"es": "xG/tiro",                 "en": "xG/Shot"},
    "compare_with":          {"es": "Comparar con",            "en": "Compare with"},
    "compare_mode_league":   {"es": "Media de {comp}",         "en": "{comp} average"},
    "col_goalkeeper":        {"es": "Portero",                 "en": "Goalkeeper"},
    "col_shots_faced":       {"es": "Tiros a puerta recibidos", "en": "Shots On Target Faced"},
    "col_xg_conceded":       {"es": "xG encajado",             "en": "xG Conceded"},
    "col_save_pct_formula":  {"es": "% paradas (paradas/tiros×100)", "en": "Save % (saves/shots×100)"},
    "caption_gk_stats":      {
        "es": "Estadísticas limitadas a partidos donde el portero aparece en eventos (sustituciones, tarjetas…) — proxy de partidos jugados. Tiros a puerta = goles + paradas · % paradas = paradas ÷ tiros a puerta × 100 · xG encajado = xG total de tiros recibidos · Goles salvados sobre lo esperado = paradas − xG encajado (positivo = por encima)",
        "en": "Stats are scoped to matches where each GK appeared in event data (substitutions, cards, etc.) — used as a proxy for matches played. Shots On Target Faced = goals + saves (blocked/missed excluded) · Save % = saves ÷ shots on target × 100 · xG Conceded = total expected-goal value of shots faced · Goals Saved Above Expected = saves − xG conceded (positive = outperforming)",
    },
    "caption_discipline_rows": {
        "es": "Goles y xG: fact_shots (todas las fuentes) · Tarjetas: fact_events (SofaScore + StatsBomb)\nFilas acumuladas por temporada cuando se elige Todas las temporadas.",
        "en": "Goals and xG: fact_shots (all sources) · Cards: fact_events (SofaScore incidents + StatsBomb)\nRows show per-season accumulation when All seasons is selected.",
    },
    "caption_cards_fouls":   {
        "es": "Tarjetas: fact_events (todas las fuentes). Faltas: fact_events (WhoScored, heurística por event_type). No se muestran 'faltas recibidas' porque WhoScored atribuye la falta al infractor. Partidos = partidos con eventos del jugador (proxy).",
        "en": "Cards: fact_events (all sources). Fouls: fact_events (WhoScored, event_type heuristic). 'Fouls suffered' not shown because WhoScored attributes fouls to the offender. Matches = matches with player events (proxy).",
    },
    "col_metric":            {"es": "Métrica",                 "en": "Metric"},

    # ── Match context extras ──────────────────────────────────
    "venue_weather_caption": {
        "es": "Sedes desde match_stadium_id (dim_stadium), no solo venue_name. Color: rojo > 25°C · naranja 10–25°C · azul < 10°C. Barras de error = rango mín–máx entre partidos en esa sede.",
        "en": "Venues from match_stadium_id (dim_stadium), not only venue_name. Color: red > 25°C · orange 10–25°C · blue < 10°C. Error bars show min–max range across matches at that venue.",
    },
    "weather_trend_caption": {
        "es": "Barras de error = rango mín–máx por temporada.",
        "en": "Error bars show min–max range per season.",
    },
    "attendance_fill_caption": {
        "es": "% naranja = ocupación media (asistencia / aforo × 100). Fuente: aforo en dim_stadium.",
        "en": "Orange % = avg fill rate (avg attendance / stadium capacity × 100). Source: dim_stadium capacity.",
    },
    "col_home_matches_short": {"es": "PJ local",               "en": "Home Matches"},
    "col_avg_short":         {"es": "Media",                   "en": "Avg"},
    "col_max_short":         {"es": "Máx",                     "en": "Max"},
    "col_min_short":         {"es": "Mín",                     "en": "Min"},
    "col_total_short":       {"es": "Total",                   "en": "Total"},
    "col_date":              {"es": "Fecha",                   "en": "Date"},
    "col_hg":                {"es": "GL",                      "en": "HG"},
    "col_ag":                {"es": "GV",                      "en": "AG"},
    "avg_cards_match":       {"es": "Media tarjetas/partido",  "en": "Avg Cards/Match"},
    "cards_match_vs_team":   {"es": "Tarjetas/partido vs {team}", "en": "Cards/Match vs {team}"},
    "cards_match_all":       {"es": "Tarjetas/partido (todos)", "en": "Cards/Match (all teams)"},
    "yellows_per_match":     {"es": "Amarillas/partido",       "en": "Yellows/Match"},
    "reds_per_match":        {"es": "Rojas/partido",           "en": "Reds/Match"},
    "cards_per_match_label": {"es": "Tarjetas/partido",        "en": "Cards per match"},
    "referee_caption":       {
        "es": "Fuente: dim_referee + dim_match + fact_events. {scope}Mín. {min_m} partidos para el gráfico. Rojas = rojas directas; las 2ª amarillas se incluyen como amarillas y se muestran aparte. Tarjetas/partido = (amarillas + rojas directas) / partidos.",
        "en": "Source: dim_referee + dim_match + fact_events. {scope}Min. {min_m} matches for chart. Reds = direct reds; 2nd yellows are included as yellows and shown separately. Cards/Match = (yellows + direct reds) / matches.",
    },
    "ref_scope_team":        {"es": "Tarjetas limitadas a {team}. ", "en": "Cards scoped to {team}. "},
    "pn_caption_header":     {
        "es": "WhoScored · Campo: 105 m × 68 m · Local ataca →, visitante ← · Nodo = origen medio del pase (partido completo) · Receptor = siguiente evento del mismo equipo",
        "en": "WhoScored · Pitch: 105 m × 68 m · Home attacks →, away attacks ← · Node = avg pass-origin location (full match, subs included) · Receiver = next same-team event",
    },
    "no_matches_paren":      {"es": "(sin partidos)",            "en": "(no matches)"},
    "both_teams":            {"es": "Ambos",                     "en": "Both"},
    "chalkboard_caption":    {
        "es": "Fuente: fact_events (WhoScored). Pases: verde = completado, rojo = fallido · entradas (azul) · tiros (estrella) · coordenadas 0-1 escaladas a 105×68.",
        "en": "Source: fact_events (WhoScored). Passes: green = completed, red = failed · tackles (blue) · shots (star) · 0-1 coords scaled to 105×68.",
    },
    "col_referee":           {"es": "Árbitro",                   "en": "Referee"},
    "col_yellows_scope":     {"es": "Amarillas{scope}",          "en": "Yellows{scope}"},
    "col_reds_scope":        {"es": "Rojas{scope}",              "en": "Reds{scope}"},
    "col_second_yellow_reds": {"es": "2ª amarilla",              "en": "2nd yellow reds"},
    "col_total_cards_scope": {"es": "Tarjetas totales{scope}",   "en": "Total Cards{scope}"},
    "col_avg_temp":          {"es": "Media °C",                "en": "Avg °C"},
    "col_min_temp_c":        {"es": "Mín °C",                  "en": "Min °C"},
    "col_max_temp_c":        {"es": "Máx °C",                  "en": "Max °C"},
    "col_avg_humidity":      {"es": "Humedad media %",         "en": "Avg Humidity %"},
    "col_rainy_matches":     {"es": "Partidos con lluvia",     "en": "Rainy Matches"},
    "weather_trend_color_caption": {
        "es": "Color: rojo > 25°C · naranja 10–25°C · azul < 10°C. Barras de error = rango mín–máx por temporada.",
        "en": "Color: red > 25°C · orange 10–25°C · blue < 10°C. Error bars show min–max range per season.",
    },
    "manager_caption":       {
        "es": "Fuente: dim_match (manager_home / manager_away, WhoScored). % puntos = puntos obtenidos / máximo posible × 100.",
        "en": "Source: dim_match (manager_home / manager_away, WhoScored). Points % = points won / max possible × 100.",
    },
    "col_manager":           {"es": "Entrenador",              "en": "Manager"},
    "col_draws_short":       {"es": "E",                       "en": "D"},
    "col_wins_short":        {"es": "V",                       "en": "W"},
    "col_losses_short":      {"es": "D",                       "en": "L"},
    "col_cards_match_scope": {"es": "Tarjetas/partido{scope}",   "en": "Cards/Match{scope}"},
    "temp_axis_label":       {"es": "°C (media, rango mín–máx)", "en": "°C (avg, min–max range)"},
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
