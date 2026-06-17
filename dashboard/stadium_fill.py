"""
dashboard/stadium_fill.py
=========================
Illustration of stadium occupancy: tiered stands fill from bottom to top
with a color that shifts red → yellow → green as fill % increases.
"""
from __future__ import annotations

import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

BG = "#0e1117"
EMPTY_COLOR = "#1a1f2e"
EMPTY_EDGE = "#3d4654"
PITCH_COLOR = "#27ae60"
PITCH_EDGE = "#ecf0f1"


def fill_level_color(pct: float) -> str:
    """Map 0–100 % occupancy to a red → yellow → green hex color."""
    pct = max(0.0, min(100.0, float(pct)))
    rgba = plt.cm.RdYlGn(pct / 100.0)
    return mcolors.to_hex(rgba)


def _draw_side_stand(
    ax,
    *,
    side: str,
    y_base: float,
    row_h: float,
    n_rows: int,
    filled_rows: int,
    crowd_color: str,
) -> None:
    """Draw one side stand as stacked tiers (wider rows toward the back)."""
    for i in range(n_rows):
        frac = (i + 1) / n_rows
        tier_y = y_base + i * row_h
        tier_h = row_h - 0.12
        if side == "left":
            x = 6.0 + (1.0 - frac) * 5.0
            w = 14.0 + frac * 10.0
        else:
            x = 80.0 - (14.0 + frac * 10.0) - (1.0 - frac) * 5.0
            w = 14.0 + frac * 10.0

        filled = i < filled_rows
        fc = crowd_color if filled else EMPTY_COLOR
        ec = crowd_color if filled else EMPTY_EDGE
        ax.add_patch(
            mpatches.Rectangle(
                (x, tier_y), w, tier_h,
                facecolor=fc, edgecolor=ec, linewidth=0.25,
            )
        )


def _draw_back_stand(
    ax,
    *,
    y_base: float,
    row_h: float,
    n_rows: int,
    filled_rows: int,
    crowd_color: str,
) -> None:
    """Draw the main stand behind the pitch."""
    for i in range(n_rows):
        frac = (i + 1) / n_rows
        tier_y = y_base + i * row_h
        tier_h = row_h - 0.12
        x = 18.0 - frac * 4.0
        w = 64.0 + frac * 12.0
        filled = i < filled_rows
        fc = crowd_color if filled else EMPTY_COLOR
        ec = crowd_color if filled else EMPTY_EDGE
        ax.add_patch(
            mpatches.Rectangle(
                (x, tier_y), w, tier_h,
                facecolor=fc, edgecolor=ec, linewidth=0.25,
            )
        )


def _draw_pitch(ax) -> None:
    pitch = mpatches.FancyBboxPatch(
        (30.0, 3.5), 40.0, 13.0,
        boxstyle="round,pad=0.35",
        facecolor=PITCH_COLOR,
        edgecolor=PITCH_EDGE,
        linewidth=1.2,
    )
    ax.add_patch(pitch)
    ax.plot([50.0, 50.0], [3.5, 16.5], color=PITCH_EDGE, linewidth=0.7, alpha=0.55)
    ax.add_patch(
        mpatches.Circle(
            (50.0, 10.0), 3.5,
            fill=False, edgecolor=PITCH_EDGE, linewidth=0.7, alpha=0.55,
        )
    )


def _draw_color_legend(ax, y: float) -> None:
    """Small horizontal gradient bar explaining the color scale."""
    n = 40
    for i in range(n):
        pct = i / (n - 1) * 100.0
        ax.add_patch(
            mpatches.Rectangle(
                (22.0 + i * 1.4, y), 1.35, 2.0,
                facecolor=fill_level_color(pct),
                edgecolor="none",
            )
        )
    ax.text(18.5, y + 1.0, "0%", ha="right", va="center", fontsize=7, color="#95a5a6")
    ax.text(80.5, y + 1.0, "100%", ha="left", va="center", fontsize=7, color="#95a5a6")


def render_stadium_fill(
    fill_pct: float,
    *,
    attendance: int | float | None = None,
    capacity: int | float | None = None,
    title: str = "",
    subtitle: str = "",
) -> plt.Figure:
    """
    Draw a stylised stadium whose stands fill bottom-up to ``fill_pct``.

    Stand tiers below the fill line use a crowd color keyed to the overall
    percentage; tiers above stay dark (empty seats).
    """
    fill_pct = max(0.0, min(100.0, float(fill_pct)))
    n_rows = 22
    filled_rows = int(round(n_rows * fill_pct / 100.0))
    crowd_color = fill_level_color(fill_pct)

    fig, ax = plt.subplots(figsize=(7, 5.8))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 78)
    ax.set_aspect("equal")
    ax.axis("off")

    _draw_pitch(ax)

    stand_base = 19.0
    stand_height = 46.0
    row_h = stand_height / n_rows

    _draw_side_stand(
        ax, side="left",
        y_base=stand_base, row_h=row_h,
        n_rows=n_rows, filled_rows=filled_rows,
        crowd_color=crowd_color,
    )
    _draw_side_stand(
        ax, side="right",
        y_base=stand_base, row_h=row_h,
        n_rows=n_rows, filled_rows=filled_rows,
        crowd_color=crowd_color,
    )
    _draw_back_stand(
        ax,
        y_base=stand_base, row_h=row_h,
        n_rows=n_rows, filled_rows=filled_rows,
        crowd_color=crowd_color,
    )

    if 0 < filled_rows < n_rows:
        level_y = stand_base + filled_rows * row_h
        ax.axhline(
            level_y, xmin=0.04, xmax=0.96,
            color=crowd_color, linestyle="--", linewidth=1.0, alpha=0.75,
        )

    ax.text(
        50.0, 70.5, f"{fill_pct:.1f}%",
        ha="center", va="center",
        fontsize=24, fontweight="bold", color=crowd_color,
    )
    if title:
        ax.text(
            50.0, 74.5, title,
            ha="center", va="center",
            fontsize=11, color="white", fontweight="600",
        )

    caption_parts: list[str] = []
    if attendance is not None and capacity is not None:
        att_i = int(attendance)
        cap_i = int(capacity)
        caption_parts.append(f"{att_i:,} / {cap_i:,}".replace(",", "."))
    if subtitle:
        caption_parts.append(subtitle)
    if caption_parts:
        ax.text(
            50.0, 1.2, " · ".join(caption_parts),
            ha="center", va="center", fontsize=9, color="#95a5a6",
        )

    _draw_color_legend(ax, y=66.0)
    plt.tight_layout(pad=0.4)
    return fig
