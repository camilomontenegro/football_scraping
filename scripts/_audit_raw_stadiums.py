"""
Auditoría de calidad de JSON crudos de estadios (Transfermarkt).

    python -m scripts._audit_raw_stadiums --root "C:/Users/Ivan/Desktop/stadiums"
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

_STADIUM_WORDS = re.compile(
    r"\b(stadium|estadio|arena|park|field|ground|stadion|stade|stadio|"
    r"camp|campo|complex|kompleks|ullev|velodrom|coliseum)\b",
    re.I,
)
_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _loose(s: str) -> str:
    return _SLUG_RE.sub("", (s or "").lower())


def _season_from_path(path: Path, root: Path) -> str | None:
    # raw/<comp>/<season>/transfermarkt/stadiums/<file>.json
    parts = path.relative_to(root / "raw").parts
    return parts[1] if len(parts) >= 2 else None


def _comp_from_path(path: Path, root: Path) -> str | None:
    parts = path.relative_to(root / "raw").parts
    return parts[0] if parts else None


def _path_season_year(season_folder: str) -> int | None:
    m = re.match(r"(\d{4})_\d{4}", season_folder or "")
    return int(m.group(1)) if m else None


@dataclass
class Issue:
    severity: str  # error | warn | info
    code: str
    path: str
    detail: str


@dataclass
class AuditResult:
    files: int = 0
    parse_errors: int = 0
    issues: list[Issue] = field(default_factory=list)
    field_fill: Counter = field(default_factory=Counter)
    by_comp: Counter = field(default_factory=Counter)

    def add(self, severity: str, code: str, path: Path, detail: str) -> None:
        self.issues.append(Issue(severity, code, str(path), detail))


def audit_file(path: Path, root: Path, res: AuditResult) -> dict | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        res.parse_errors += 1
        res.add("error", "json_parse", path, str(exc))
        return None

    if not isinstance(data, dict):
        res.add("error", "not_object", path, type(data).__name__)
        return None

    comp = _comp_from_path(path, root)
    season_folder = _season_from_path(path, root)
    slug_file = path.stem

    # Campos obligatorios
    for key in ("team_slug", "team_id_tm", "season", "stadium_name"):
        val = data.get(key)
        if val is None or (isinstance(val, str) and not val.strip()):
            res.add("error", "missing_required", path, key)

    team_slug = (data.get("team_slug") or "").strip()
    team_id = data.get("team_id_tm")
    season_json = data.get("season")
    name = (data.get("stadium_name") or "").strip()

    if team_slug and slug_file and _loose(team_slug) != _loose(slug_file):
        res.add("warn", "slug_mismatch", path, f"file={slug_file} json={team_slug}")

    sy = _path_season_year(season_folder or "")
    if sy is not None and season_json is not None:
        try:
            if int(season_json) != sy:
                res.add("warn", "season_mismatch", path, f"path={sy} json={season_json}")
        except (TypeError, ValueError):
            res.add("error", "bad_season", path, repr(season_json))

    if name:
        if _loose(name) == _loose(team_slug):
            res.add("error", "name_is_team", path, name)
        elif not _STADIUM_WORDS.search(name) and len(name) < 25:
            # nombres cortos sin palabra-estadio: revisar
            if _loose(name) in (_loose(team_slug), _loose(slug_file)):
                res.add("error", "name_is_team", path, name)
            else:
                res.add("warn", "name_no_stadium_word", path, name)

    cap = data.get("capacity")
    seats = data.get("seats_total")
    if cap is not None:
        try:
            cap_i = int(cap)
            if cap_i < 500 or cap_i > 150_000:
                res.add("warn", "capacity_range", path, str(cap_i))
        except (TypeError, ValueError):
            res.add("error", "bad_capacity", path, repr(cap))
    if cap is not None and seats is not None:
        try:
            if int(seats) < int(cap):
                res.add("warn", "seats_lt_capacity", path, f"seats={seats} cap={cap}")
        except (TypeError, ValueError):
            pass

    for yfield in ("built_year", "inaugurated_year", "refurbished_year"):
        yv = data.get(yfield)
        if yv is None:
            continue
        try:
            y = int(yv)
            if y < 1850 or y > 2035:
                res.add("warn", "year_range", path, f"{yfield}={y}")
        except (TypeError, ValueError):
            res.add("error", "bad_year", path, f"{yfield}={yv!r}")

    pl, pw = data.get("pitch_length_m"), data.get("pitch_width_m")
    if pl is not None or pw is not None:
        try:
            pl_i, pw_i = int(pl), int(pw)
            if not (90 <= pl_i <= 120 and 45 <= pw_i <= 90):
                res.add("warn", "pitch_dims", path, f"{pl_i}x{pw_i}")
        except (TypeError, ValueError):
            res.add("error", "bad_pitch", path, f"{pl!r}x{pw!r}")

    url = data.get("tm_url") or ""
    if url and team_id is not None and str(team_id) not in url:
        res.add("warn", "tm_url_team_id", path, url[:80])

    addr = (data.get("address") or "").strip()
    if addr and name and _loose(addr) == _loose(name) and not _STADIUM_WORDS.search(name):
        res.add("info", "address_equals_name", path, addr)

    # Cobertura de campos
    for k, v in data.items():
        if v is not None and v != "" and v is not False:
            res.field_fill[k] += 1

    if comp:
        res.by_comp[comp] += 1
    return data


def compare_clean_csv(root: Path, res: AuditResult) -> None:
    clean = root / "clean"
    if not clean.exists():
        return
    for csv_path in sorted(clean.rglob("stadiums.csv")):
        rel = csv_path.relative_to(clean)
        comp, season = rel.parts[0], rel.parts[1]
        raw_dir = root / "raw" / comp / season / "transfermarkt" / "stadiums"
        n_raw = len(list(raw_dir.glob("*.json"))) if raw_dir.exists() else 0
        import csv
        with csv_path.open(encoding="utf-8") as f:
            n_csv = sum(1 for _ in csv.DictReader(f))
        if n_raw != n_csv:
            res.add(
                "warn", "raw_csv_count",
                csv_path,
                f"raw={n_raw} csv={n_csv}",
            )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=r"C:\Users\Ivan\Desktop\stadiums")
    ap.add_argument("--report", default=None, help="Ruta del informe txt")
    args = ap.parse_args()

    root = Path(args.root)
    raw = root / "raw"
    if not raw.exists():
        print(f"No existe {raw}")
        return 1

    res = AuditResult()
    by_comp_season: dict[tuple[str, str], list[dict]] = defaultdict(list)
    dup_keys: dict[tuple[str, str, int], list[str]] = defaultdict(list)

    for path in sorted(raw.rglob("*.json")):
        if path.parent.name != "stadiums":
            continue
        res.files += 1
        data = audit_file(path, root, res)
        if not data:
            continue
        comp = _comp_from_path(path, root) or "?"
        season = _season_from_path(path, root) or "?"
        by_comp_season[(comp, season)].append(data)
        tid = data.get("team_id_tm")
        if tid is not None:
            dup_keys[(comp, season, int(tid))].append(path.name)

    for key, files in dup_keys.items():
        if len(files) > 1:
            res.add("error", "duplicate_team_id", Path(key[0]), f"{key[2]} in {files}")

    # Consistencia inter-temporadas: mismo team_id_tm debe tener mismo team_slug
    by_tid: dict[int, set[str]] = defaultdict(set)
    for data in (d for rows in by_comp_season.values() for d in rows):
        tid = data.get("team_id_tm")
        slug = data.get("team_slug")
        if tid is not None and slug:
            by_tid[int(tid)].add(slug)
    for tid, slugs in by_tid.items():
        if len(slugs) > 1:
            res.add("warn", "slug_drift", Path("global"), f"tm_id={tid} slugs={sorted(slugs)}")

    compare_clean_csv(root, res)

    errors = [i for i in res.issues if i.severity == "error"]
    warns = [i for i in res.issues if i.severity == "warn"]

    lines = [
        "=" * 70,
        f"  AUDITORÍA RAW ESTADIOS — {root}",
        "=" * 70,
        f"Archivos JSON: {res.files}",
        f"Errores parseo: {res.parse_errors}",
        f"Incidencias: {len(errors)} errores, {len(warns)} avisos",
        "",
        "── Por competición (archivos) ──",
    ]
    for comp, n in res.by_comp.most_common():
        lines.append(f"  {comp}: {n}")

    lines += ["", "── Cobertura de campos (filas con valor) ──"]
    for k, n in res.field_fill.most_common():
        pct = 100 * n / res.files if res.files else 0
        lines.append(f"  {k:<22} {n:>5} ({pct:5.1f}%)")

    lines += ["", "── Combos comp/temporada ──"]
    for (comp, season), rows in sorted(by_comp_season.items()):
        missing_name = sum(1 for r in rows if not (r.get("stadium_name") or "").strip())
        no_cap = sum(1 for r in rows if r.get("capacity") is None)
        lines.append(
            f"  {comp}/{season}: {len(rows)} equipos"
            + (f" | sin nombre: {missing_name}" if missing_name else "")
            + (f" | sin aforo: {no_cap}" if no_cap else "")
        )

    if errors:
        lines += ["", "── ERRORES ──"]
        for i in errors[:80]:
            lines.append(f"  [{i.code}] {i.path}")
            lines.append(f"    {i.detail}")
        if len(errors) > 80:
            lines.append(f"  ... y {len(errors) - 80} más")

    if warns:
        lines += ["", "── AVISOS (muestra) ──"]
        by_code = Counter(i.code for i in warns)
        for code, n in by_code.most_common():
            lines.append(f"  {code}: {n}")
        for i in warns[:40]:
            lines.append(f"  [{i.code}] {Path(i.path).name}: {i.detail}")
        if len(warns) > 40:
            lines.append(f"  ... y {len(warns) - 40} avisos más")

    text = "\n".join(lines)
    print(text)

    report_path = Path(args.report) if args.report else root.parent / "football_scraping" / "reports" / "raw_stadiums_audit.txt"
    if not report_path.parent.exists():
        report_path = Path(__file__).resolve().parents[1] / "reports" / "raw_stadiums_audit.txt"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(text, encoding="utf-8")
    print(f"\nInforme: {report_path}")

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
