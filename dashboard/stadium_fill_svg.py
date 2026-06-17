"""
dashboard/stadium_fill_svg.py
=============================
Semi-transparent stadium occupancy widget (aerial, rectangular layout).

A stadium seen from above with its real shape: two long side stands (top and
bottom), two goal-end stands (left and right), and a striped rectangular pitch
in the centre. Seats are drawn as chunky rounded squares that light up tier by
tier from the pitch outward (lower deck first) as occupancy rises, glowing in a
colour that shifts red -> amber -> green with the fill percentage. A translucent
glass shell with a specular highlight sits on top, with an ambient colour halo.

    stadium_fill_svg_html(...)  -> str   (pure, no Streamlit dependency)
    render_stadium_fill_svg(...) -> None  (Streamlit st.iframe wrapper)
"""
from __future__ import annotations

__all__ = ["stadium_fill_svg_html", "render_stadium_fill_svg", "fill_level_color"]

# ----------------------------------------------------------------- colours ---

_RDYLGN = (
    (0.00, (0xC0, 0x14, 0x2B)),
    (0.12, (0xE0, 0x36, 0x30)),
    (0.25, (0xF4, 0x6D, 0x43)),
    (0.38, (0xFD, 0xAE, 0x4F)),
    (0.50, (0xFA, 0xD6, 0x4A)),
    (0.62, (0xCC, 0xE0, 0x55)),
    (0.75, (0x88, 0xCC, 0x55)),
    (0.88, (0x39, 0xB5, 0x4A)),
    (1.00, (0x12, 0x96, 0x4E)),
)


def _lerp(a, b, t):
    return a + (b - a) * t


def _rdylgn_rgb(pct):
    x = max(0.0, min(1.0, float(pct) / 100.0))
    for i in range(len(_RDYLGN) - 1):
        lo_x, lo_c = _RDYLGN[i]
        hi_x, hi_c = _RDYLGN[i + 1]
        if x <= hi_x:
            span = hi_x - lo_x
            t = 0.0 if span == 0 else (x - lo_x) / span
            return (round(_lerp(lo_c[0], hi_c[0], t)),
                    round(_lerp(lo_c[1], hi_c[1], t)),
                    round(_lerp(lo_c[2], hi_c[2], t)))
    return _RDYLGN[-1][1]


def _hex(rgb):
    return "#%02x%02x%02x" % rgb


def _lighten(rgb, f):
    return (round(rgb[0] + (255 - rgb[0]) * f),
            round(rgb[1] + (255 - rgb[1]) * f),
            round(rgb[2] + (255 - rgb[2]) * f))


def fill_level_color(pct):
    return _hex(_rdylgn_rgb(pct))


def _esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _fmt_int(v):
    try:
        return format(int(round(float(v))), ",d").replace(",", ".")
    except (TypeError, ValueError):
        return _esc(v)


# ---------------------------------------------------------------- geometry ---

_W, _H = 820, 662
_CX, _CY = 410, 322
_PHW, _PHH = 150, 95              # pitch half-extents
_PL, _PR = _CX - _PHW, _CX + _PHW
_PT, _PB = _CY - _PHH, _CY + _PHH
_SQ, _CG, _CELL = 26, 4, 30       # seat square, gap, cell pitch
_GAP = 16                         # track gap between pitch and stands
_TIERS = 5                        # seat rows (depth) per stand
_LONG, _SHORT = 14, 6            # columns: long stands / goal ends
_PAD = 16                         # shell padding around the stands

_SX0 = _PL - _GAP - _SQ - (_TIERS - 1) * _CELL - _PAD
_SX1 = _PR + _GAP + (_TIERS - 1) * _CELL + _SQ + _PAD
_SY0 = _PT - _GAP - _SQ - (_TIERS - 1) * _CELL - _PAD
_SY1 = _PB + _GAP + (_TIERS - 1) * _CELL + _SQ + _PAD


def _i(v):
    return "%.1f" % v


def _cells():
    """Yield (x, y, row, col, cols) for every seat square in all four stands."""
    out = []
    start_x = _CX - (_LONG * _CELL - _CG) / 2.0
    for col in range(_LONG):
        cx = start_x + col * _CELL
        for r in range(_TIERS):
            out.append((cx, _PT - _GAP - _SQ - r * _CELL, r, col, _LONG))
            out.append((cx, _PB + _GAP + r * _CELL, r, col, _LONG))
    start_y = _CY - (_SHORT * _CELL - _CG) / 2.0
    for col in range(_SHORT):
        cy = start_y + col * _CELL
        for r in range(_TIERS):
            out.append((_PL - _GAP - _SQ - r * _CELL, cy, r, col, _SHORT))
            out.append((_PR + _GAP + r * _CELL, cy, r, col, _SHORT))
    return out


def _center_order(cols):
    mid = (cols - 1) / 2.0
    return sorted(range(cols), key=lambda c: abs(c - mid))


def _pitch_svg():
    n = 10
    sw = (2 * _PHW) / n
    stripes = []
    for i in range(n):
        col = "#2fa85b" if i % 2 == 0 else "#279150"
        stripes.append('<rect x="' + _i(_PL + i * sw) + '" y="' + _i(_PT) +
                       '" width="' + _i(sw + 0.6) + '" height="' + _i(2 * _PHH) +
                       '" fill="' + col + '"/>')
    band = '<g clip-path="url(#pitchClip)">' + "".join(stripes) + "</g>"
    wl = 'stroke="#eafff3" stroke-opacity="0.6" stroke-width="1.5" fill="none"'
    marks = (
        '<rect x="' + _i(_PL) + '" y="' + _i(_PT) + '" width="' + _i(2 * _PHW) +
        '" height="' + _i(2 * _PHH) + '" rx="10" ' + wl + '/>'
        '<line x1="' + str(_CX) + '" y1="' + _i(_PT) + '" x2="' + str(_CX) +
        '" y2="' + _i(_PB) + '" ' + wl + '/>'
        '<ellipse cx="' + str(_CX) + '" cy="' + str(_CY) + '" rx="33" ry="30" ' + wl + '/>'
        '<circle cx="' + str(_CX) + '" cy="' + str(_CY) + '" r="2.6" fill="#eafff3" fill-opacity="0.7"/>'
        '<rect x="' + _i(_PL) + '" y="' + _i(_CY - 55) + '" width="44" height="110" ' + wl + '/>'
        '<rect x="' + _i(_PL) + '" y="' + _i(_CY - 26) + '" width="16" height="52" ' + wl + '/>'
        '<rect x="' + _i(_PR - 44) + '" y="' + _i(_CY - 55) + '" width="44" height="110" ' + wl + '/>'
        '<rect x="' + _i(_PR - 16) + '" y="' + _i(_CY - 26) + '" width="16" height="52" ' + wl + '/>'
        '<circle cx="' + _i(_PL + 30) + '" cy="' + str(_CY) + '" r="2.2" fill="#eafff3" fill-opacity="0.7"/>'
        '<circle cx="' + _i(_PR - 30) + '" cy="' + str(_CY) + '" r="2.2" fill="#eafff3" fill-opacity="0.7"/>'
    )
    surround = ('<rect x="' + _i(_PL - 10) + '" y="' + _i(_PT - 10) + '" width="' +
                _i(2 * _PHW + 20) + '" height="' + _i(2 * _PHH + 20) +
                '" rx="16" fill="#11202b" fill-opacity="0.65"/>')
    grass = ('<rect x="' + _i(_PL) + '" y="' + _i(_PT) + '" width="' + _i(2 * _PHW) +
             '" height="' + _i(2 * _PHH) + '" rx="10" fill="#239050"/>')
    return surround + grass + band + marks


# ------------------------------------------------------------------ render ---

def stadium_fill_svg_html(fill_pct, *, attendance=None, capacity=None, title="",
                          subtitle="", animate=True, height=470, embed_doc=True):
    pct = max(0.0, min(100.0, float(fill_pct)))
    rgb = _rdylgn_rgb(pct)
    crowd = _hex(rgb)
    light = _hex(_lighten(rgb, 0.35))

    f = pct / 100.0
    filled_rows = int(f * _TIERS)
    partial = f * _TIERS - filled_rows
    co = {_LONG: _center_order(_LONG), _SHORT: _center_order(_SHORT)}
    kpart = {c: int(round(len(co[c]) * partial)) for c in co}

    def lit(row, col, cols):
        if row < filled_rows:
            return True
        if row == filled_rows and partial > 0:
            return col in set(co[cols][:kpart[cols]])
        return False

    filled_by_row = {}
    empty = []
    for (x, y, r, col, cols) in _cells():
        rect = ('<rect x="' + _i(x) + '" y="' + _i(y) + '" width="' + str(_SQ) +
                '" height="' + str(_SQ) + '" rx="3"/>')
        if lit(r, col, cols):
            filled_by_row.setdefault(r, []).append(rect)
        else:
            empty.append(rect)

    groups = ""
    for r in sorted(filled_by_row):
        op = 0.97 - 0.12 * (r / max(_TIERS - 1, 1))
        anim = ""
        if animate:
            anim = ('<animate attributeName="opacity" from="0" to="1" begin="' +
                    ("%.2f" % (0.09 * r)) + 's" dur="0.5s" fill="freeze" '
                    'calcMode="spline" keyTimes="0;1" keySplines="0.2 0.8 0.2 1"/>')
        groups += ('<g fill="' + crowd + '" fill-opacity="' + ("%.2f" % op) +
                   '" stroke="' + light + '" stroke-opacity="0.45" stroke-width="0.8">' +
                   "".join(filled_by_row[r]) + anim + "</g>")
    empty_grp = ('<g fill="#9fb4d6" fill-opacity="0.07" stroke="#ffffff" '
                 'stroke-opacity="0.06" stroke-width="0.8">' + "".join(empty) + "</g>")
    seats = '<g filter="url(#crowdGlow)">' + groups + "</g>" + empty_grp

    legend_stops = "".join('<stop offset="' + ("%.1f" % (s / 10)) + '" stop-color="' +
                           _hex(_rdylgn_rgb(s * 10)) + '"/>' for s in range(11))
    sw_, sh_ = _SX1 - _SX0, _SY1 - _SY0
    defs = (
        "<defs>"
        '<clipPath id="pitchClip"><rect x="' + _i(_PL) + '" y="' + _i(_PT) +
        '" width="' + _i(2 * _PHW) + '" height="' + _i(2 * _PHH) + '" rx="10"/></clipPath>'
        '<clipPath id="shellClip"><rect x="' + _i(_SX0) + '" y="' + _i(_SY0) +
        '" width="' + _i(sw_) + '" height="' + _i(sh_) + '" rx="64"/></clipPath>'
        '<linearGradient id="roof" x1="0" y1="0" x2="0.4" y2="1">'
        '<stop offset="0" stop-color="#3a4760" stop-opacity="0.40"/>'
        '<stop offset="0.6" stop-color="#222c3e" stop-opacity="0.52"/>'
        '<stop offset="1" stop-color="#161d2b" stop-opacity="0.66"/></linearGradient>'
        '<radialGradient id="spec" cx="0.5" cy="0.5" r="0.5">'
        '<stop offset="0" stop-color="#ffffff" stop-opacity="0.28"/>'
        '<stop offset="0.55" stop-color="#ffffff" stop-opacity="0.05"/>'
        '<stop offset="1" stop-color="#ffffff" stop-opacity="0"/></radialGradient>'
        '<radialGradient id="ambient" cx="0.5" cy="0.5" r="0.5">'
        '<stop offset="0" stop-color="' + crowd + '" stop-opacity="0.18"/>'
        '<stop offset="1" stop-color="' + crowd + '" stop-opacity="0"/></radialGradient>'
        '<linearGradient id="legend" x1="0" y1="0" x2="1" y2="0">' + legend_stops +
        "</linearGradient>"
        '<filter id="crowdGlow" x="-20%" y="-20%" width="140%" height="140%">'
        '<feDropShadow dx="0" dy="0" stdDeviation="3" flood-color="' + crowd +
        '" flood-opacity="0.5"/></filter>'
        '<filter id="softShadow" x="-30%" y="-30%" width="160%" height="160%">'
        '<feDropShadow dx="0" dy="10" stdDeviation="16" flood-color="#000" flood-opacity="0.5"/></filter>'
        '<filter id="tshadow" x="-60%" y="-60%" width="220%" height="220%">'
        '<feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#000" flood-opacity="0.8"/></filter>'
        "</defs>"
    )

    ambient = ('<rect x="' + _i(_SX0 - 10) + '" y="' + _i(_SY0 - 10) + '" width="' +
               _i(sw_ + 20) + '" height="' + _i(sh_ + 20) + '" rx="74" fill="url(#ambient)"/>')
    shell = ('<g filter="url(#softShadow)"><rect x="' + _i(_SX0) + '" y="' + _i(_SY0) +
             '" width="' + _i(sw_) + '" height="' + _i(sh_) + '" rx="64" fill="url(#roof)" '
             'stroke="#ffffff" stroke-opacity="0.16" stroke-width="1.5"/></g>')
    sheen = ('<g clip-path="url(#shellClip)"><ellipse cx="' + str(_CX - 110) + '" cy="' +
             str(_SY0 + 70) + '" rx="' + ("%.0f" % (sw_ * 0.55)) + '" ry="' +
             ("%.0f" % (sh_ * 0.42)) + '" fill="url(#spec)"/></g>')

    body = ambient + shell + sheen + _pitch_svg() + seats

    big = ('<g filter="url(#tshadow)" text-anchor="middle"><text x="' + str(_CX) +
           '" y="' + str(_CY + 8) + '" font-size="54" font-weight="800" fill="#ffffff" '
           'letter-spacing="-1">' + ("%.0f" % pct) +
           '<tspan font-size="31" font-weight="700" dx="1">%</tspan></text>'
           '<rect x="' + str(_CX - 36) + '" y="' + str(_CY + 24) +
           '" width="72" height="5" rx="2.5" fill="' + crowd + '"/></g>')

    title_markup = ""
    if title:
        title_markup = ('<text x="' + str(_CX) + '" y="34" text-anchor="middle" '
                        'font-size="19" font-weight="700" fill="#ffffff" '
                        'fill-opacity="0.95" letter-spacing="0.3">' + _esc(title) + "</text>")

    cap_parts = []
    if attendance is not None and capacity is not None:
        cap_parts.append(_fmt_int(attendance) + " / " + _fmt_int(capacity))
    if subtitle:
        cap_parts.append(_esc(subtitle))
    caption = "  ·  ".join(cap_parts)
    caption_markup = ""
    if caption:
        caption_markup = ('<text x="' + str(_CX) + '" y="' + str(_H - 50) +
                          '" text-anchor="middle" font-size="13.5" fill="#cfd6e4" '
                          'fill-opacity="0.9">' + caption + "</text>")

    lx, lw, ly = _CX - 130, 260, _H - 30
    mx = lx + f * lw
    legend = (
        '<rect x="' + str(lx) + '" y="' + str(ly) + '" width="' + str(lw) +
        '" height="10" rx="5" fill="url(#legend)" stroke="#ffffff" stroke-opacity="0.18"/>'
        '<text x="' + str(lx - 10) + '" y="' + str(ly + 9) + '" text-anchor="end" '
        'font-size="11.5" fill="#8a93a6">0%</text>'
        '<text x="' + str(lx + lw + 10) + '" y="' + str(ly + 9) + '" text-anchor="start" '
        'font-size="11.5" fill="#8a93a6">100%</text>'
        '<path d="M ' + ("%.1f" % (mx - 7)) + ' ' + str(ly - 6) + ' L ' +
        ("%.1f" % (mx + 7)) + ' ' + str(ly - 6) + ' L ' + ("%.1f" % mx) + ' ' +
        str(ly + 2) + ' Z" fill="#ffffff"/>'
    )

    svg = ('<svg viewBox="0 0 ' + str(_W) + ' ' + str(_H) + '" '
           'xmlns="http://www.w3.org/2000/svg" width="100%" height="100%" '
           'preserveAspectRatio="xMidYMid meet" '
           'font-family="Inter, Segoe UI, system-ui, sans-serif" role="img" '
           'aria-label="Stadium occupancy ' + ("%.0f" % pct) + ' percent">' +
           defs + body + big + title_markup + caption_markup + legend + "</svg>")

    if not embed_doc:
        return svg

    style = ("html,body{margin:0;padding:0;background:transparent;}"
             ".wrap{display:flex;align-items:center;justify-content:center;width:100%;}"
             ".wrap svg{height:100%;width:auto;max-width:100%;}")
    return ("<!doctype html><html><head><meta charset='utf-8'><style>" + style +
            "</style></head><body><div class='wrap' style='height:" +
            str(int(height)) + "px'>" + svg + "</div></body></html>")


def render_stadium_fill_svg(fill_pct, *, attendance=None, capacity=None, title="",
                            subtitle="", animate=True, height=470):
    import streamlit as st
    html = stadium_fill_svg_html(fill_pct, attendance=attendance, capacity=capacity,
                                 title=title, subtitle=subtitle, animate=animate,
                                 height=height)
    # Streamlit >= 1.56 deprecates components.html in favour of st.iframe; use the
    # new API when available and fall back for older Streamlit versions.
    if hasattr(st, "iframe"):
        st.iframe(html, height=height + 6)
    else:
        import streamlit.components.v1 as components
        components.html(html, height=height + 6, scrolling=False)
