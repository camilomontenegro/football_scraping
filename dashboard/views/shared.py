"""
dashboard/views/shared.py
=========================
Helpers shared by several views (moved verbatim from app.py).
"""
from __future__ import annotations

import pandas as pd
import streamlit as st
<<<<<<< HEAD
# used to create custom legend entries for scatter markers in market value chart
from matplotlib.lines import Line2D  # noqa: F401 — re-exported for players.py
 
from dashboard import explore
from dashboard.i18n import t
 

def _fmt(n) -> str:
    return f"{int(n):,}".replace(",", ".")
def _fmt_eur(euros) -> str:
    """
    Formats a euro value as a human-readable string.
    Used throughout the market value and transfer history tabs.
 
    Examples:
        _fmt_eur(38000000) → "€38.0M"
        _fmt_eur(500000)   → "€500K"
        _fmt_eur(None)     → "—"
    """
    if euros is None:
        return "—"
    if isinstance(euros, float) and pd.isna(euros):
        return "—"
    if abs(euros) >= 1_000_000:
        return f"€{euros/1_000_000:.1f}M"
    if abs(euros) >= 1_000:
        return f"€{euros/1_000:.0f}K"
    return f"€{euros:,}"
 
 
def _fmt_team_history_date_to(row: pd.Series) -> str:
    """
    Formats the date_to column for the Career History table.
 
    - If date_to is NULL and the team is 'Retirado', the player retired
      at that club so we show 'Retired' instead of a date.
    - If date_to is NULL and the team is not 'Retirado', the player is
      still at that club so we show 'Present'.
    - Otherwise, format the date normally as dd/mm/yyyy.
    """
    if pd.isna(row["date_to"]):
        return "Retired" if row["team"] == "Retirado" else "Present"
    return pd.Timestamp(row["date_to"]).strftime("%d/%m/%Y")
=======

from dashboard import explore
from dashboard.i18n import t


def _fmt(n) -> str:
    return f"{int(n):,}".replace(",", ".")

>>>>>>> f879e66061c859d3375dbc3b2a982db4093cf724

def _render_stadium_detail(row: pd.Series) -> None:
    """Detail panel: photo, metadata and external links for one stadium row."""
    st.subheader(t("stadium_detail"))
    img_col, info_col = st.columns([1, 1.4])

    with img_col:
        image_url = row.get("image_url")
        lat, lon = row.get("latitude"), row.get("longitude")
        if pd.notna(image_url) and str(image_url).strip():
            st.image(str(image_url), caption=str(row.get("stadium_name") or ""), width="stretch")
        elif pd.notna(lat) and pd.notna(lon):
            st.caption(t("stadium_map_fallback"))
            st.map(pd.DataFrame({"lat": [float(lat)], "lon": [float(lon)]}), zoom=12)
        else:
            st.info(t("stadium_no_photo"))

    with info_col:
        title = str(row.get("stadium_name") or "-")
        team = str(row.get("team") or "-")
        st.markdown(f"**{title}** — {team}")
        meta = []
        if pd.notna(row.get("season")):
            meta.append(f"**{t('season')}:** {row['season']}")
        if pd.notna(row.get("capacity")):
            meta.append(f"**{t('total_capacity')}:** {_fmt(row['capacity'])}")
        if pd.notna(row.get("built_year")):
            meta.append(f"**Built:** {int(row['built_year'])}")
        if pd.notna(row.get("city")) or pd.notna(row.get("country")):
            city = str(row.get("city") or "")
            country = str(row.get("country") or "")
            loc = ", ".join(p for p in (city, country) if p)
            meta.append(f"**{t('country')}:** {loc}")
        if pd.notna(row.get("surface")):
            meta.append(f"**Surface:** {row['surface']}")
        if pd.notna(row.get("architect")):
            meta.append(f"**Architect:** {row['architect']}")
        if pd.notna(row.get("owner")):
            meta.append(f"**Owner:** {row['owner']}")
        if pd.notna(lat) and pd.notna(lon):
            meta.append(f"**Coords:** {float(lat):.4f}, {float(lon):.4f}")
        if meta:
            st.markdown("  \n".join(meta))

        link_cols = st.columns(3)
        wiki = row.get("wikipedia_url")
        if pd.notna(wiki) and str(wiki).strip():
            link_cols[0].link_button(t("stadium_wikipedia"), str(wiki))
        qid = row.get("wikidata_qid")
        if pd.notna(qid) and str(qid).strip():
            link_cols[1].link_button(
                t("stadium_wikidata"),
                f"https://www.wikidata.org/wiki/{qid}",
            )
        tm = row.get("tm_url")
        if pd.notna(tm) and str(tm).strip():
            link_cols[2].link_button("Transfermarkt", str(tm))

# ════════════════════════════════════════════════════════════════════
# SHARED HELPER — 3-column selector row (reused across tabs)
# ════════════════════════════════════════════════════════════════════
def _tab_selectors(key_prefix: str, all_seasons: bool = False):
    """Return (competition, season_or_none, team_or_none) for a tab."""
    _comps = explore.get_competitions()
    sc1, sc2, sc3 = st.columns(3)
    with sc1:
        _comp = st.selectbox(t("competition"), _comps, key=f"{key_prefix}_comp")
    _seasons = explore.get_seasons_for_competition(_comp)
    season_opts = ([t("all_seasons")] + _seasons) if all_seasons else (_seasons or ["(no seasons)"])
    with sc2:
        _season_sel = st.selectbox(
            t("season"), season_opts,
            key=f"{key_prefix}_season",
            disabled=not _seasons,
        )
    _season = None if (_season_sel in (t("all_seasons"), "(no seasons)") or not _seasons) else _season_sel
    _teams = explore.get_teams_for_season(_season or (_seasons[0] if _seasons else ""), _comp) if _seasons else []
    with sc3:
        _team_sel = st.selectbox(
            t("team"), [t("all_teams")] + _teams,
            key=f"{key_prefix}_team",
            disabled=not _teams,
        )
    _team = None if _team_sel == t("all_teams") else _team_sel
    return _comp, _season, _team

def _empty_info(message: str | None = None):
    st.info(message or t("no_data"))
