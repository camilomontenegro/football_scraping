"""
dashboard/views/pass_network.py
===============================
Pass Network — WhoScored pass maps per match.

Split out of app.py so st.navigation only executes the selected page
(the old st.tabs layout ran every tab's queries on every rerun).
"""
from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from dashboard import explore, pass_network
from dashboard.i18n import t
from dashboard.views.shared import _fmt


def render() -> None:
    st.header(t("pass_network"))
    st.caption(
        "WhoScored · Pitch: 105 m × 68 m · Home attacks →, away attacks ← · "
        "Node = avg pass-origin location (full match, subs included) · "
        "Receiver = next same-team event"
    )

    try:
        from mplsoccer import Pitch as _PnPitch
    except ImportError:
        st.error("Install mplsoccer: pip install mplsoccer")
        st.stop()

    _pn_comps = explore.get_competitions()
    pnf1, pnf2, pnf3 = st.columns([1, 1, 2])
    with pnf1:
        pn_comp = st.selectbox(
            t("competition"), _pn_comps or ["(none)"],
            key="pn_comp", disabled=not _pn_comps,
        )
    _pn_seasons = explore.get_seasons_for_competition(pn_comp) if _pn_comps else []
    with pnf2:
        pn_season = st.selectbox(
            t("season"), _pn_seasons or ["(no seasons)"],
            key="pn_season", disabled=not _pn_seasons,
        )

    _pn_matches = (
        pass_network.get_matches_with_passes(pn_comp, pn_season)
        if _pn_seasons else pd.DataFrame()
    )
    with pnf3:
        pn_match_label = st.selectbox(
            t("match"),
            _pn_matches["label"].tolist() if not _pn_matches.empty else ["(no matches)"],
            key="pn_match", disabled=_pn_matches.empty,
        )

    if _pn_matches.empty:
        st.info(t("no_pass_matches"))
    else:
        _pn_row = _pn_matches[_pn_matches["label"] == pn_match_label].iloc[0]
        _pn_mid = int(_pn_row["match_id"])

        pn_min_passes = st.slider(t("min_passes"), 1, 10, 3, key="pn_min_passes")

        def _draw_pass_network(
            team_name: str, team_id: int, node_color: str, flip: bool = False,
        ) -> None:
            nodes, edges = pass_network.get_pass_network(_pn_mid, team_id)
            if nodes.empty:
                st.info(t("no_pass_data"))
                return

            # Scale normalised 0-1 coords to the 105x68 custom pitch.
            # The away team is mirrored (180° rotation) so the two teams
            # face each other: home attacks →, away attacks ←.
            _nx = (1 - nodes["x"]) if flip else nodes["x"]
            _ny = (1 - nodes["y"]) if flip else nodes["y"]
            nodes = nodes.assign(px=_nx * 105, py=_ny * 68)
            pos = nodes.set_index("player_id")[["px", "py"]]

            edges_f = edges[edges["pass_count"] >= pn_min_passes]

            pitch = _PnPitch(
                pitch_type="custom", pitch_length=105, pitch_width=68,
                pitch_color="grass", stripe=True, line_color="white", line_zorder=2,
            )
            fig, ax = pitch.draw(figsize=(8, 5.5))

            max_count = int(edges_f["pass_count"].max()) if not edges_f.empty else 1
            for r in edges_f.itertuples():
                a, b = pos.loc[r.passer_id], pos.loc[r.receiver_id]
                pitch.lines(
                    a["px"], a["py"], b["px"], b["py"],
                    lw=0.5 + 4.5 * (r.pass_count / max_count),
                    color="white", alpha=0.25 + 0.55 * (r.pass_count / max_count),
                    zorder=2, ax=ax,
                )

            sizes = 200 + 900 * (nodes["passes"] / nodes["passes"].max())
            pitch.scatter(
                nodes["px"], nodes["py"], s=sizes,
                color=node_color, edgecolors="white", linewidths=1.2,
                alpha=0.95, zorder=3, ax=ax,
            )
            for r in nodes.itertuples():
                surname = str(r.player).split()[-1]
                pitch.annotate(
                    surname, xy=(r.px, r.py), ax=ax,
                    ha="center", va="center", color="white",
                    fontsize=7, fontweight="bold", zorder=4,
                )
            ax.set_title(team_name, fontsize=12, pad=10)
            st.pyplot(fig)
            plt.close(fig)

            id2name = nodes.set_index("player_id")["player"]
            m1, m2, m3 = st.columns(3)
            m1.metric(t("total_passes"), _fmt(nodes["passes"].sum()))
            m2.metric(t("pass_pairs"), _fmt(len(edges)))
            if not edges.empty:
                top = edges.loc[edges["pass_count"].idxmax()]
                m3.metric(
                    t("top_connection"),
                    f"{int(top['pass_count'])}",
                    delta=(
                        f"{id2name.get(int(top['passer_id']), '?')} ↔ "
                        f"{id2name.get(int(top['receiver_id']), '?')}"
                    ),
                    delta_color="off",
                )

        pn_home, pn_away = st.columns(2)
        with pn_home:
            _draw_pass_network(str(_pn_row["home"]), int(_pn_row["home_team_id"]), "#3498db")
        with pn_away:
            _draw_pass_network(str(_pn_row["away"]), int(_pn_row["away_team_id"]), "#e74c3c", flip=True)

        st.caption(
            "Source: fact_events (WhoScored) · Only successful passes where the next "
            "event belongs to the same team · Edge width/opacity ∝ passes between the "
            "pair (both directions combined) · Node size ∝ passes made"
        )
