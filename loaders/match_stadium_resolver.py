"""
Resuelve venues de partido → dim_stadium.stadium_id (sin depender del equipo local).
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from sqlalchemy import text

log = logging.getLogger(__name__)

DEFAULT_DATA_ROOT = Path(r"C:\Users\Ivan\Desktop\football_scraping_backup")
VENUE_SEASON_FROM = "2020/2021"
VENUE_SEASON_TO = "2025/2026"
COORD_EPS = 0.015


@dataclass
class VenueInfo:
    name: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    city: Optional[str] = None
    country: Optional[str] = None
    capacity: Optional[int] = None
    sofascore_venue_id: Optional[int] = None
    slug: Optional[str] = None


def normalize_name(name: str) -> str:
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    cleaned = re.sub(r"[^\w\s]", "", ascii_str.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def name_similarity(a: str, b: str) -> float:
    wa, wb = set(a.split()), set(b.split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / len(wa | wb)


def _coord_dist(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    return math.hypot(lat1 - lat2, lon1 - lon2)


def _venue_tm_id(venue: VenueInfo) -> int:
    if venue.sofascore_venue_id:
        return -int(venue.sofascore_venue_id)
    digest = hashlib.md5(normalize_name(venue.name).encode()).hexdigest()
    # id_transfermarkt_team es INTEGER (±2e9); evitar overflow en hashes.
    return -(int(digest[:7], 16) % 900_000_000 + 1)


class MatchStadiumResolver:
    """Índice dim_stadium + altas de venues neutros."""

    def __init__(self, conn) -> None:
        self.conn = conn
        self.by_norm: dict[str, list[dict]] = {}
        self.with_coords: list[dict] = []
        self._load_stadiums()

    def _load_stadiums(self) -> None:
        rows = self.conn.execute(text("""
            SELECT stadium_id, stadium_name, canonical_team_id,
                   latitude, longitude, city, country, capacity,
                   valid_from_season, valid_to_season
            FROM dim_stadium
            WHERE stadium_name IS NOT NULL AND TRIM(stadium_name) <> ''
        """)).mappings().all()
        for row in rows:
            d = dict(row)
            norm = normalize_name(d["stadium_name"])
            self.by_norm.setdefault(norm, []).append(d)
            if d["latitude"] is not None and d["longitude"] is not None:
                self.with_coords.append(d)

    def find_by_name(self, venue_name: str) -> Optional[int]:
        norm = normalize_name(venue_name)
        if not norm:
            return None
        if norm in self.by_norm:
            return self.by_norm[norm][0]["stadium_id"]
        best_score, best_id = 0.0, None
        for sn, rows in self.by_norm.items():
            score = name_similarity(norm, sn)
            if score > best_score:
                best_score, best_id = score, rows[0]["stadium_id"]
        if best_score >= 0.55:
            return best_id
        return None

    def find_by_coords(self, lat: float, lon: float) -> Optional[int]:
        best_id, best_d = None, COORD_EPS
        for row in self.with_coords:
            d = _coord_dist(lat, lon, float(row["latitude"]), float(row["longitude"]))
            if d < best_d:
                best_d, best_id = d, row["stadium_id"]
        return best_id

    def resolve(self, venue: VenueInfo, *, dry_run: bool) -> Optional[int]:
        if not venue.name or not venue.name.strip():
            return None
        sid = self.find_by_name(venue.name)
        if sid is None and venue.latitude is not None and venue.longitude is not None:
            sid = self.find_by_coords(venue.latitude, venue.longitude)
        if sid is not None:
            return sid
        if dry_run:
            return -1
        return self._insert_venue(venue)

    def _insert_venue(self, venue: VenueInfo) -> int:
        tm_id = _venue_tm_id(venue)
        slug = venue.slug or normalize_name(venue.name).replace(" ", "-")[:120] or "venue"
        existing = self.conn.execute(
            text("SELECT stadium_id FROM dim_stadium WHERE id_transfermarkt_team = :tid LIMIT 1"),
            {"tid": tm_id},
        ).scalar()
        if existing:
            return int(existing)

        row = self.conn.execute(
            text("""
                INSERT INTO dim_stadium (
                    canonical_team_id, id_transfermarkt_team, team_slug,
                    valid_from_season, valid_to_season,
                    stadium_name, capacity, city, country,
                    latitude, longitude, data_source, updated_at
                ) VALUES (
                    NULL, :tm_id, :slug,
                    :vf, :vt,
                    :name, :cap, :city, :country,
                    :lat, :lon, 'match-venue', NOW()
                )
                RETURNING stadium_id
            """),
            {
                "tm_id": tm_id,
                "slug": slug,
                "vf": VENUE_SEASON_FROM,
                "vt": VENUE_SEASON_TO,
                "name": venue.name.strip(),
                "cap": venue.capacity,
                "city": venue.city,
                "country": venue.country,
                "lat": venue.latitude,
                "lon": venue.longitude,
            },
        ).scalar()
        d = {
            "stadium_id": row,
            "stadium_name": venue.name.strip(),
            "latitude": venue.latitude,
            "longitude": venue.longitude,
        }
        norm = normalize_name(venue.name)
        self.by_norm.setdefault(norm, []).append(d)
        if venue.latitude is not None and venue.longitude is not None:
            self.with_coords.append(d)
        log.info("Nuevo match-venue: %r -> stadium_id=%s", venue.name, row)
        return int(row)


def parse_sofascore_venue(data: dict) -> Optional[VenueInfo]:
    venue = data.get("venue") or {}
    name = (venue.get("name") or "").strip()
    if not name:
        return None
    coords = venue.get("venueCoordinates") or {}
    city_obj = venue.get("city") or {}
    country_obj = venue.get("country") or city_obj.get("country") or {}
    cap = venue.get("capacity")
    try:
        cap_i = int(cap) if cap is not None else None
    except (TypeError, ValueError):
        cap_i = None
    return VenueInfo(
        name=name,
        latitude=coords.get("latitude"),
        longitude=coords.get("longitude"),
        city=(city_obj.get("name") or "").strip() or None,
        country=(country_obj.get("name") or "").strip() or None,
        capacity=cap_i,
        sofascore_venue_id=venue.get("id"),
        slug=venue.get("slug"),
    )


def load_sofascore_venues(data_root: Path) -> dict[str, VenueInfo]:
    att_dir = data_root / "data" / "raw" / "attendance"
    out: dict[str, VenueInfo] = {}
    if not att_dir.is_dir():
        log.warning("No existe %s", att_dir)
        return out
    for path in att_dir.glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        venue = parse_sofascore_venue(data)
        if venue:
            out[path.stem] = venue
    log.info("SofaScore venues: %d archivos", len(out))
    return out


def load_whoscored_venues(data_root: Path) -> dict[int, str]:
    """whoscored_match_id -> venue_name"""
    out: dict[int, str] = {}
    clean = data_root / "data" / "clean"
    if not clean.is_dir():
        return out
    for path in clean.rglob("match_enrichment.csv"):
        if "whoscored" not in {p.lower() for p in path.parts}:
            continue
        try:
            df = pd.read_csv(path, usecols=["whoscored_match_id", "venue_name"])
        except (ValueError, pd.errors.EmptyDataError, OSError):
            try:
                df = pd.read_csv(path)
            except Exception:
                continue
            if "whoscored_match_id" not in df.columns or "venue_name" not in df.columns:
                continue
        for _, row in df.iterrows():
            ws_id = row.get("whoscored_match_id")
            name = row.get("venue_name")
            if pd.isna(ws_id) or pd.isna(name) or not str(name).strip():
                continue
            try:
                out[int(float(ws_id))] = str(name).strip()
            except (ValueError, TypeError):
                continue
    log.info("WhoScored venues: %d partidos", len(out))
    return out
