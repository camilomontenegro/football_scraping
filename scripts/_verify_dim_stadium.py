"""Verificación integral de calidad en dim_stadium. Solo lectura."""
from __future__ import annotations

import re
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from dotenv import load_dotenv
from sqlalchemy import text

_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_ROOT / ".env", encoding="utf-8")

from loaders.common import engine  # noqa: E402
from wizard.competitions import WORKING_COMPETITION_NAMES  # noqa: E402

_ISSUES: list[str] = []
_OK: list[str] = []

_STADIUM_WORDS = re.compile(
    r"\b(stadium|estadio|arena|park|field|ground|stadion|stade|stadio|"
    r"metropolitano|nou|olimpico|olympic)\b",
    re.I,
)
_TEAM_PREFIX = re.compile(r"^(fc |sc |ac |cd |ud |rcd |real |sl |ss )", re.I)
_BAD_WIKI = re.compile(r"\b(politician|railway station|fútbol\)|\(fútbol)\b", re.I)


def _issue(msg: str) -> None:
    _ISSUES.append(msg)


def _ok(msg: str) -> None:
    _OK.append(msg)


def main() -> int:
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM dim_stadium")).scalar()

        # ── 1. Rangos de temporada ───────────────────────────────────────
        bad_seasons = conn.execute(text("""
            SELECT COUNT(*) FROM dim_stadium
            WHERE valid_to_season >= '2090' OR valid_from_season < '1990'
               OR valid_to_season > '2035' OR valid_from_season > valid_to_season
        """)).scalar()
        if bad_seasons:
            _issue(f"Rangos de temporada inválidos: {bad_seasons}")
        else:
            _ok("Rangos de temporada: todos dentro de límites razonables")

        inverted = conn.execute(text("""
            SELECT COUNT(*) FROM dim_stadium
            WHERE valid_from_season > valid_to_season
        """)).scalar()
        if inverted:
            _issue(f"valid_from > valid_to: {inverted}")

        outside_match_range = conn.execute(text("""
            WITH bounds AS (
              SELECT MIN(season) AS gmin, MAX(season) AS gmax
              FROM dim_match WHERE season IS NOT NULL
            )
            SELECT COUNT(*) FROM dim_stadium ds, bounds b
            WHERE ds.valid_from_season < b.gmin OR ds.valid_to_season > b.gmax
        """)).scalar()
        if outside_match_range:
            _issue(
                f"Estadios con rango fuera de partidos cargados (global): {outside_match_range}"
            )
        else:
            _ok("Rangos contenidos en temporadas con partidos (global)")

        # ── 2. FK y equipos ──────────────────────────────────────────────
        no_fk = conn.execute(text(
            "SELECT COUNT(*) FROM dim_stadium WHERE canonical_team_id IS NULL"
        )).scalar()
        if no_fk:
            _issue(f"Sin canonical_team_id: {no_fk}")
        else:
            _ok("Todos los estadios tienen canonical_team_id")

        orphan_fk = conn.execute(text("""
            SELECT COUNT(*) FROM dim_stadium ds
            LEFT JOIN dim_team t ON t.canonical_id = ds.canonical_team_id
            WHERE ds.canonical_team_id IS NOT NULL AND t.canonical_id IS NULL
        """)).scalar()
        if orphan_fk:
            _issue(f"canonical_team_id sin fila en dim_team: {orphan_fk}")
        else:
            _ok("FK canonical_team_id válida en dim_team")

        not_in_working = conn.execute(text("""
            WITH working AS (
              SELECT DISTINCT t.canonical_id
              FROM dim_match m
              JOIN dim_competition c ON c.canonical_id = m.competition_id
              JOIN dim_team t ON t.canonical_id IN (m.home_team_id, m.away_team_id)
              WHERE c.canonical_name = ANY(:names)
            )
            SELECT COUNT(*) FROM dim_stadium ds
            WHERE ds.canonical_team_id IS NOT NULL
              AND ds.canonical_team_id NOT IN (SELECT canonical_id FROM working)
        """), {"names": sorted(WORKING_COMPETITION_NAMES)}).scalar()
        if not_in_working:
            _issue(f"Equipos sin partidos en WORKING_COMPETITIONS: {not_in_working}")
        else:
            _ok("Todos los equipos tienen partidos en competiciones activas")

        # ── 3. Duplicados / solapamientos ────────────────────────────────
        dup_hash = conn.execute(text("""
            SELECT COALESCE(SUM(n-1),0) FROM (
              SELECT COUNT(*) n FROM dim_stadium
              GROUP BY id_transfermarkt_team, data_hash HAVING COUNT(*)>1
            ) s
        """)).scalar()
        if dup_hash:
            _issue(f"Duplicados mismo equipo+hash: {dup_hash}")
        else:
            _ok("Sin duplicados exactos (equipo + data_hash)")

        overlap = conn.execute(text("""
            SELECT COUNT(*) FROM dim_stadium a
            JOIN dim_stadium b ON a.id_transfermarkt_team = b.id_transfermarkt_team
              AND a.stadium_id < b.stadium_id
              AND a.valid_from_season <= b.valid_to_season
              AND b.valid_from_season <= a.valid_to_season
        """)).scalar()
        if overlap:
            _issue(f"Pares de rangos solapados (mismo equipo): {overlap}")
        else:
            _ok("Sin solapamientos temporales por equipo")

        dup_from = conn.execute(text("""
            SELECT COUNT(*) FROM (
              SELECT id_transfermarkt_team, valid_from_season
              FROM dim_stadium GROUP BY 1,2 HAVING COUNT(*)>1
            ) s
        """)).scalar()
        if dup_from:
            _issue(f"Violaciones índice único (equipo, valid_from): {dup_from}")
        else:
            _ok("Índice único (equipo, valid_from_season) respetado")

        # ── 4. Coordenadas ───────────────────────────────────────────────
        bad_coords = conn.execute(text("""
            SELECT COUNT(*) FROM dim_stadium
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
              AND (latitude < -90 OR latitude > 90
                   OR longitude < -180 OR longitude > 180
                   OR (latitude = 0 AND longitude = 0))
        """)).scalar()
        no_coords = conn.execute(text("""
            SELECT COUNT(*) FROM dim_stadium
            WHERE latitude IS NULL OR longitude IS NULL
        """)).scalar()
        if bad_coords:
            _issue(f"Coordenadas inválidas: {bad_coords}")
        else:
            _ok("Coordenadas dentro de rangos válidos")
        if no_coords:
            _issue(f"Sin lat/lon: {no_coords}")

        # ── 5. Nombres sospechosos ───────────────────────────────────────
        rows = conn.execute(text("""
            SELECT stadium_id, team_slug, stadium_name, wikipedia_url, data_source
            FROM dim_stadium WHERE stadium_name IS NOT NULL
        """)).mappings().all()
        bad_names = []
        for r in rows:
            name = (r["stadium_name"] or "").strip()
            if _BAD_WIKI.search(name):
                bad_names.append(r)
                continue
            if _TEAM_PREFIX.match(name) and not _STADIUM_WORDS.search(name):
                bad_names.append(r)
                continue
            slug = (r["team_slug"] or "").replace("-", " ").lower()
            if slug and slug in name.lower() and not _STADIUM_WORDS.search(name):
                if r["data_source"] != "synthetic-geocode":
                    bad_names.append(r)
        if bad_names:
            _issue(f"Nombres que parecen equipo, no estadio: {len(bad_names)}")
            for r in bad_names[:10]:
                print(f"    ! {r['team_slug']}: {r['stadium_name']!r}")
        else:
            _ok("Nombres de estadio: sin patrones de nombre de club")

        # ── 6. Cobertura partidos ────────────────────────────────────────
        orphan_stadiums = conn.execute(text("""
            SELECT COUNT(*) FROM dim_stadium ds
            WHERE NOT EXISTS (
              SELECT 1 FROM dim_match m WHERE m.stadium_id = ds.stadium_id
            )
        """)).scalar()
        matches_no_stadium = conn.execute(text("""
            SELECT COUNT(*) FROM dim_match WHERE stadium_id IS NULL
        """)).scalar()
        total_matches = conn.execute(text("SELECT COUNT(*) FROM dim_match")).scalar()
        pct = 100.0 * (total_matches - matches_no_stadium) / total_matches if total_matches else 0

        if orphan_stadiums:
            _issue(f"Estadios sin ningún partido vinculado: {orphan_stadiums}")
        else:
            _ok("Todos los estadios tienen al menos un partido")

        if matches_no_stadium:
            _issue(f"Partidos sin stadium_id: {matches_no_stadium} ({100-pct:.2f}% sin cubrir)")
        else:
            _ok(f"100% partidos con stadium_id ({total_matches})")

        # Partidos cuyo home no tiene estadio para esa temporada
        missing_season = conn.execute(text("""
            SELECT COUNT(*) FROM dim_match m
            JOIN dim_team t ON t.canonical_id = m.home_team_id
            WHERE m.season IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM dim_stadium s
                WHERE s.canonical_team_id = m.home_team_id
                  AND m.season BETWEEN s.valid_from_season AND s.valid_to_season
              )
        """)).scalar()
        if missing_season:
            _issue(f"Partidos cuyo local no tiene estadio para esa temporada: {missing_season}")
        else:
            _ok("Cada partido tiene estadio del local para su temporada")

        # ── 7. data_source ───────────────────────────────────────────────
        sources = conn.execute(text("""
            SELECT data_source, COUNT(*) FROM dim_stadium GROUP BY 1 ORDER BY 2 DESC
        """)).fetchall()
        print("\n── Por data_source ──")
        for s, n in sources:
            print(f"  {s or '(null)'}: {n}")

        # ── 8. Multi-fila SCD2 ───────────────────────────────────────────
        multi = conn.execute(text("""
            SELECT team_slug, COUNT(*) n,
                   array_agg(stadium_name) names,
                   array_agg(valid_from_season || '..' || valid_to_season) ranges
            FROM dim_stadium GROUP BY team_slug, id_transfermarkt_team
            HAVING COUNT(*) > 1 ORDER BY n DESC
        """)).fetchall()
        print(f"\n── Equipos con >1 fila SCD2: {len(multi)} ──")
        for r in multi:
            print(f"  {r.team_slug}: {r.n} filas — {list(r.names)} — {list(r.ranges)}")

        # ── Resumen ──────────────────────────────────────────────────────
        print("\n" + "=" * 60)
        print(f"  VERIFICACIÓN dim_stadium — {total} filas")
        print("=" * 60)
        print("\n✓ OK:")
        for m in _OK:
            print(f"  • {m}")
        if _ISSUES:
            print("\n✗ PROBLEMAS:")
            for m in _ISSUES:
                print(f"  • {m}")
        else:
            print("\n✓ Sin problemas detectados.")
        print()

    return 1 if _ISSUES else 0


if __name__ == "__main__":
    sys.exit(main())
