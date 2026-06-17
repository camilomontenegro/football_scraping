"""Normalización y emparejado de nombres de estadio (Cloudinary ↔ BD)."""
from __future__ import annotations

import re
import unicodedata

# Palabras genéricas que no aportan al match (evitan falsos positivos tipo *Stadion*)
GENERIC_STADIUM_WORDS = frozenset(
    {
        "stadion",
        "stadium",
        "stadio",
        "estadio",
        "stade",
        "stady",
        "stadi",
        "arena",
        "field",
        "ground",
        "park",
        "complex",
        "municipal",
        "municipale",
        "city",
        "ciudad",
        "national",
        "nacional",
        "home",
        "de",
        "du",
        "des",
        "the",
        "del",
        "da",
        "do",
        "dos",
        "the",
        "van",
        "und",
        "y",
        "e",
        "i",
    }
)


def normalize_stadium_name(s: str, *, drop_generic: bool = True) -> str:
    """Minúsculas sin acentos; opcionalmente quita tokens genéricos."""
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9 ]", " ", s.lower())
    s = re.sub(r"\s+", " ", s).strip()
    if not drop_generic:
        return s
    tokens = [t for t in s.split() if t not in GENERIC_STADIUM_WORDS]
    return " ".join(tokens).strip()


def stadium_keywords(name: str, *, min_len: int = 3) -> set[str]:
    """Palabras distintivas para fuzzy match."""
    n = normalize_stadium_name(name, drop_generic=True)
    return {p for p in n.split() if len(p) >= min_len and p not in GENERIC_STADIUM_WORDS}


def _compact_stadium_name(name: str) -> str:
    """Forma compacta para alias (conserva city/ground, quita artículos)."""
    s = normalize_stadium_name(name, drop_generic=False)
    for prefix in ("the ",):
        if s.startswith(prefix):
            s = s[len(prefix):]
    return s.strip()


# Grupos de nombres equivalentes (verificados en MD Osen / Transfermarkt).
_ALIAS_GROUPS: tuple[frozenset[str], ...] = (
    frozenset({"francois coty", "michel moretti", "stade francois coty", "stade michel moretti"}),
    frozenset({"tapiolan urheilupuisto", "tapiolan urheilukeskus"}),
    frozenset({"estadio dos arcos", "estadio do rio ave"}),
    frozenset({"estadio municipal de botosani", "stadionul municipal"}),
    frozenset({"tony bezzina stadium", "hibernians stadium"}),
    frozenset({"ecolog arena", "renova stadium dzepciste", "renova stadium"}),
    frozenset({"stadion tresnjica", "stadion pod golubom"}),
    frozenset({"m scores stadion", "stadion de vliert", "matchoholic stadion"}),
    frozenset({"de kuip", "stadion feijenoord"}),
    frozenset({"tele2 arena", "3arena"}),
    frozenset({"dacia arena", "bluenergy stadium"}),
    frozenset({"besiktas park", "vodafone park", "tupras stadyumu", "turkas stadyumu"}),
    frozenset({"bingoal stadion", "werktalent stadion", "cars jeans stadion"}),
    frozenset({"bravida arena", "nordic wellness arena"}),
    frozenset({"ozon arena", "stadion krasnodar"}),
    frozenset({"zondacrypto arena", "miejski stadion pilkarski rakow"}),
    frozenset({"limassol arena", "alphamega stadium", "tsirio stadium"}),
    frozenset({"ud almeria stadium", "power horse stadium", "estadio de los juegos mediterraneos"}),
    frozenset({"brianteo", "u power stadium", "stadio brianteo"}),
    frozenset({"arena nationala", "national arena"}),
    frozenset({"stadium municipal", "stadium de toulouse"}),
    frozenset({"dean court", "vitality stadium"}),
    frozenset({"falmer stadium", "amex stadium", "american express community stadium"}),
    frozenset({"letzigrund", "stadion letzigrund"}),
    frozenset({"anoeta", "reale arena", "estadio de anoeta"}),
    frozenset({"azrsun arena", "azersun arena", "ask arena"}),
)


def _alias_bucket(name: str) -> frozenset[str] | None:
    n = _compact_stadium_name(name)
    if not n:
        return None
    for group in _ALIAS_GROUPS:
        if n in group:
            return group
    return None


def stadium_names_match(a: str, b: str) -> bool:
    """¿Encajan dos nombres de estadio?"""
    ga, gb = _alias_bucket(a), _alias_bucket(b)
    if ga and gb and ga == gb:
        return True

    na = normalize_stadium_name(a)
    nb = normalize_stadium_name(b)
    if not na or not nb:
        ca, cb = _compact_stadium_name(a), _compact_stadium_name(b)
        return bool(ca and cb and ca == cb)
    if na == nb:
        return True
    if na in nb or nb in na:
        return True
    ka, kb = stadium_keywords(a), stadium_keywords(b)
    if not ka or not kb:
        return False
    overlap = ka & kb
    if len(overlap) >= 2:
        return True
    if len(overlap) == 1 and min(len(na), len(nb)) <= 16:
        return True
    return False


def stadium_match_score(a: str, b: str) -> int:
    """Puntuación para desambiguar (mayor = mejor)."""
    na = normalize_stadium_name(a)
    nb = normalize_stadium_name(b)
    if not na or not nb:
        return 0
    if na == nb:
        return 10
    if na in nb or nb in na:
        return 8
    overlap = stadium_keywords(a) & stadium_keywords(b)
    if len(overlap) >= 2:
        return 6
    if len(overlap) == 1:
        return 4
    return 0
