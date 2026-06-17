"""
País del club (dim_team.country), no de la liga doméstica.

Transfermarkt suele exponer la bandera de la liga en la ficha del equipo; este módulo
centraliza overrides y normalización para corregir casos transfronterizos.
"""
from __future__ import annotations

import re
import unicodedata

# canonical_name dim_team → país (español TM)
TEAM_COUNTRY_OVERRIDES: dict[str, str] = {
    "AS Monaco": "Mónaco",
    "Cardiff City": "Gales",
    "Swansea City": "Gales",
    "Newport County": "Gales",
    "Wrexham AFC": "Gales",
    "Connah's Quay Nomads": "Gales",
    "The New Saints": "Gales",
    "Barry Town": "Gales",
    "Bala Town": "Gales",
    "Derry City": "Irlanda",
    "Bohemian FC": "Irlanda",
    "Shamrock Rovers": "Irlanda",
    "Dundalk FC": "Irlanda",
    "Shelbourne": "Irlanda",
    "St Patrick's Athletic": "Irlanda",
    "Sligo Rovers": "Irlanda",
    "Linfield FC": "Irlanda del Norte",
    "Glentoran FC": "Irlanda del Norte",
    "Coleraine FC": "Irlanda del Norte",
    "Larne FC": "Irlanda del Norte",
    "FC Vaduz": "Liechtenstein",
    "Lincoln Red Imps": "Gibraltar",
    "Europa FC": "Gibraltar",
    "St Joseph's FC": "Gibraltar",
    "SP Tre Penne": "San Marino",
    "La Fiorita": "San Marino",
    "SP Tre Fiori": "San Marino",
    "SS Folgore / Falciano": "San Marino",
    "SS Virtus": "San Marino",
    "UE Santa Coloma": "Andorra",
    "Inter Club d'Escaldes": "Andorra",
    "FC Santa Coloma": "Andorra",
    "Atlètic Club Escaldes": "Andorra",
    "FC Prishtina": "Kosovo",
    "FC Drita": "Kosovo",
    "KF Ballkani": "Kosovo",
    "KF Llapi": "Kosovo",
    "SC Gjilani": "Kosovo",
    "Celtic FC": "Escocia",
    "Rangers": "Escocia",
    "Aberdeen": "Escocia",
    "Hibernian": "Escocia",
    "Heart of Midlothian": "Escocia",
    "SK Sturm Graz": "Austria",
}

# id_transfermarkt → país (cuando el nombre aún no está en dim_team)
TM_ID_COUNTRY_OVERRIDES: dict[int, str] = {
    162: "Mónaco",   # AS Monaco
    268: "Gales",    # Cardiff
    2288: "Gales",   # Swansea
    1039: "Gales",   # Newport County
    1198: "Gales",   # Wrexham
    923: "Liechtenstein",  # Vaduz
}

COUNTRY_ALIASES: dict[str, str] = {
    "england": "Inglaterra",
    "inglaterra": "Inglaterra",
    "scotland": "Escocia",
    "escocia": "Escocia",
    "wales": "Gales",
    "gales": "Gales",
    "northern ireland": "Irlanda del Norte",
    "irlanda del norte": "Irlanda del Norte",
    "ireland": "Irlanda",
    "irlanda": "Irlanda",
    "eire / ireland": "Irlanda",
    "eire ireland": "Irlanda",
    "spain": "España",
    "espana": "España",
    "españa": "España",
    "france": "Francia",
    "francia": "Francia",
    "monaco": "Mónaco",
    "mónaco": "Mónaco",
    "germany": "Alemania",
    "alemania": "Alemania",
    "italy": "Italia",
    "italia": "Italia",
    "slovakia": "Eslovaquia",
    "eslovaquia": "Eslovaquia",
    "netherlands": "Países Bajos",
    "paises bajos": "Países Bajos",
    "países bajos": "Países Bajos",
    "portugal": "Portugal",
    "belgium": "Bélgica",
    "belgica": "Bélgica",
    "bélgica": "Bélgica",
    "switzerland": "Suiza",
    "suiza": "Suiza",
    "austria": "Austria",
    "poland": "Polonia",
    "polonia": "Polonia",
    "turkey": "Turquía",
    "turkiye": "Turquía",
    "turquía": "Turquía",
    "czechia": "República Checa",
    "czech republic": "República Checa",
    "republica checa": "República Checa",
    "república checa": "República Checa",
    "denmark": "Dinamarca",
    "dinamarca": "Dinamarca",
    "norway": "Noruega",
    "noruega": "Noruega",
    "sweden": "Suecia",
    "suecia": "Suecia",
    "finland": "Finlandia",
    "finlandia": "Finlandia",
    "iceland": "Islandia",
    "islandia": "Islandia",
    "greece": "Grecia",
    "grecia": "Grecia",
    "romania": "Rumania",
    "rumania": "Rumania",
    "bulgaria": "Bulgaria",
    "serbia": "Serbia",
    "croatia": "Croacia",
    "croacia": "Croacia",
    "slovenia": "Eslovenia",
    "eslovenia": "Eslovenia",
    "bosnia & herzegovina": "Bosnia y Herzegovina",
    "bosnia y herzegovina": "Bosnia y Herzegovina",
    "north macedonia": "Macedonia del Norte",
    "macedonia del norte": "Macedonia del Norte",
    "montenegro": "Montenegro",
    "albania": "Albania",
    "hungary": "Hungría",
    "hungría": "Hungría",
    "israel": "Israel",
    "cyprus": "Chipre",
    "chipre": "Chipre",
    "malta": "Malta",
    "luxembourg": "Luxemburgo",
    "luxemburgo": "Luxemburgo",
    "estonia": "Estonia",
    "latvia": "Letonia",
    "letonia": "Letonia",
    "lithuania": "Lituania",
    "lituania": "Lituania",
    "belarus": "Bielorrusia",
    "bielorrusia": "Bielorrusia",
    "moldova": "Moldavia",
    "moldavia": "Moldavia",
    "georgia": "Georgia",
    "armenia": "Armenia",
    "azerbaijan": "Azerbaiyán",
    "azerbaiyán": "Azerbaiyán",
    "kazakhstan": "Kazajistán",
    "kazajistán": "Kazajistán",
    "russia": "Rusia",
    "rusia": "Rusia",
    "ukraine": "Ucrania",
    "ucrania": "Ucrania",
    "andorra": "Andorra",
    "gibraltar": "Gibraltar",
    "liechtenstein": "Liechtenstein",
    "san marino": "San Marino",
    "kosovo": "Kosovo",
    "faroe islands": "Islas Feroe",
    "islas feroe": "Islas Feroe",
}

# País del estadio que no indica nacionalidad del club
_UNRELIABLE_STADIUM_COUNTRIES = {
    "reino unido",
    "united kingdom",
    "estados unidos",
    "australia",
}


def _norm(s: str | None) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9 ]", " ", s.lower()).strip()


def canonical_country(raw: str | None) -> str | None:
    if not raw:
        return None
    key = _norm(raw)
    return COUNTRY_ALIASES.get(key, raw.strip())


def country_for_team(canonical_name: str, tm_id: int | None = None) -> str | None:
    if canonical_name in TEAM_COUNTRY_OVERRIDES:
        return TEAM_COUNTRY_OVERRIDES[canonical_name]
    if tm_id and tm_id in TM_ID_COUNTRY_OVERRIDES:
        return TM_ID_COUNTRY_OVERRIDES[tm_id]
    return None


def country_from_stadium(stadium_country: str | None) -> str | None:
    c = canonical_country(stadium_country)
    if not c or _norm(c) in _UNRELIABLE_STADIUM_COUNTRIES:
        return None
    return c


def resolve_team_country(
    canonical_name: str,
    *,
    tm_country: str | None = None,
    tm_id: int | None = None,
    stadium_country: str | None = None,
    existing: str | None = None,
) -> str | None:
    """Prioridad: override > existente > estadio > TM (liga)."""
    override = country_for_team(canonical_name, tm_id)
    if override:
        return override
    if existing:
        return canonical_country(existing)
    from_stadium = country_from_stadium(stadium_country)
    if from_stadium:
        return from_stadium
    if tm_country:
        return canonical_country(tm_country)
    return None
