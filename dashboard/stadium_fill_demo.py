"""
dashboard/stadium_fill_demo.py
==============================
Standalone playground for the semi-transparent glass-dome stadium widget.

Run from the project root:

    streamlit run dashboard/stadium_fill_demo.py

It tries to load real stadiums from ``dim_stadium`` (if the DB is reachable),
and otherwise falls back to a small built-in sample list, so it works even
with no database configured.
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st

from dashboard.stadium_fill_svg import render_stadium_fill_svg

st.set_page_config(page_title="Stadium fill demo", page_icon="🏟️", layout="wide")

# Built-in fallback (name, capacity, city, country).
SAMPLE: list[dict] = [
    {"name": "Camp Nou", "capacity": 99354, "city": "Barcelona", "country": "España"},
    {"name": "Santiago Bernabéu", "capacity": 81044, "city": "Madrid", "country": "España"},
    {"name": "Wembley Stadium", "capacity": 90000, "city": "London", "country": "England"},
    {"name": "Signal Iduna Park", "capacity": 81365, "city": "Dortmund", "country": "Germany"},
    {"name": "San Siro", "capacity": 75923, "city": "Milano", "country": "Italy"},
    {"name": "Allianz Arena", "capacity": 75024, "city": "München", "country": "Germany"},
    {"name": "Old Trafford", "capacity": 74310, "city": "Manchester", "country": "England"},
    {"name": "Metropolitano", "capacity": 70460, "city": "Madrid", "country": "España"},
]


@st.cache_data(show_spinner=False)
def load_stadiums() -> tuple[list[dict], str]:
    """Return (stadiums, source_label). Falls back to SAMPLE if no DB."""
    try:
        import pandas as pd
        from sqlalchemy import text

        from dashboard import db

        eng = db.get_engine()
        with eng.connect() as conn:
            df = pd.read_sql(
                text(
                    """
                    SELECT stadium_name, capacity, city, country
                    FROM dim_stadium
                    WHERE capacity IS NOT NULL AND capacity > 0
                      AND stadium_name IS NOT NULL
                    ORDER BY capacity DESC
                    """
                ),
                conn,
            )
        seen: dict[str, dict] = {}
        for r in df.itertuples(index=False):
            name = str(r.stadium_name).strip()
            if name and name not in seen:
                seen[name] = {
                    "name": name,
                    "capacity": int(r.capacity),
                    "city": str(getattr(r, "city", "") or ""),
                    "country": str(getattr(r, "country", "") or ""),
                }
        if seen:
            return list(seen.values()), "base de datos (dim_stadium)"
    except Exception:
        pass
    return SAMPLE, "lista de ejemplo (BD no disponible)"


def _fmt(n) -> str:
    return f"{int(n):,}".replace(",", ".")


st.title("🏟️ Llenado de estadio — demo")
st.caption(
    "Estadio aéreo semitransparente tipo domo de cristal: las gradas se llenan "
    "de abajo a arriba y el color va de rojo → amarillo → verde según la ocupación."
)

stadiums, source = load_stadiums()
names = [s["name"] for s in stadiums]

left, right = st.columns([1, 2], gap="large")

with left:
    st.caption(f"Fuente: {source} · {len(stadiums)} estadios")
    idx = st.selectbox("Estadio", range(len(names)), format_func=lambda i: names[i])
    s = stadiums[idx]
    cap = max(int(s["capacity"]), 1)

    mode = st.radio("Definir ocupación por", ["Asistencia", "Porcentaje"], horizontal=True)
    if mode == "Asistencia":
        att = st.slider("Asistencia", 0, cap, min(int(cap * 0.75), cap), step=max(1, cap // 200))
        pct = att / cap * 100.0
    else:
        pct = st.slider("Ocupación (%)", 0.0, 100.0, 75.0, 0.5)
        att = int(round(cap * pct / 100.0))

    animate = st.checkbox("Animar llenado", value=True)

    st.metric("Aforo", _fmt(cap))
    st.metric("Asistencia", _fmt(att))
    st.metric("Ocupación", f"{pct:.1f}%")
    st.metric("Asientos vacíos", _fmt(max(cap - att, 0)))

with right:
    subtitle = " · ".join(x for x in [s.get("city", ""), s.get("country", "")] if x)
    render_stadium_fill_svg(
        pct,
        attendance=att,
        capacity=cap,
        title=s["name"],
        subtitle=subtitle,
        animate=animate,
        height=520,
    )
