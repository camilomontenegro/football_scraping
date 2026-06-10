# scripts/__init__.py
# Redirigir componentes movidos a la carpeta wizard/ para compatibilidad de imports.

from wizard.competitions import (
    COMPETITIONS,
    get_competition,
    get_source_ids,
    get_source_config,
    get_season_start_year,
    get_available_seasons,
    list_competitions,
)
from wizard.pipeline_runner import (
    run_pipeline,
    list_available_competitions,
    get_last_match_date,
    get_current_season,
)
