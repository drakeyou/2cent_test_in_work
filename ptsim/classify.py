"""Разбор slug/question рынка: дисциплина, уровень, тип, номер сегмента.

Классификация эвристическая и будет ошибаться на новых формулировках. Поэтому
сырые slug и question сохраняются целиком: любую таксономию можно переразобрать
офлайн, не пересобирая данные.
"""
from __future__ import annotations

import re

_DOTA = re.compile(r"\b(dota\s*2|dota2|dota|the-international|ti\d{1,2})\b", re.I)
_CS2 = re.compile(r"\b(cs2|cs-2|csgo|cs-go|counter[\s-]?strike)\b", re.I)

_SEGMENT = re.compile(r"\b(?:map|game|match)[\s\-_]*(\d{1,2})\b", re.I)
_ROUND_NO = re.compile(r"\bround[\s\-_]*(\d{1,2})\b", re.I)

_KINDS: list[tuple[str, re.Pattern]] = [
    ("first_blood", re.compile(r"first[\s\-_]?blood", re.I)),
    ("first_to_n", re.compile(r"first[\s\-_]to[\s\-_]\d+", re.I)),
    ("first_kill", re.compile(r"first[\s\-_]?(kill|frag)", re.I)),
    ("first_roshan", re.compile(r"roshan", re.I)),
    ("first_tower", re.compile(r"first[\s\-_]?tower", re.I)),
    ("pistol_round", re.compile(r"pistol", re.I)),
    ("handicap", re.compile(r"handicap|[+-]\d+\.\d\b|spread", re.I)),
    ("total_rounds", re.compile(r"total[\s\-_]?rounds?|rounds?[\s\-_]?(over|under)", re.I)),
    ("total_maps", re.compile(r"total[\s\-_]?(maps?|games?)", re.I)),
    ("total_kills", re.compile(r"total[\s\-_]?kills?", re.I)),
    ("correct_score", re.compile(r"correct[\s\-_]?score|\b2[\s\-_]?0\b|\b2[\s\-_]?1\b", re.I)),
    ("over_under", re.compile(r"\b(over|under)\b[\s\-_]*\d", re.I)),
    ("map_winner", re.compile(r"(map|game)[\s\-_]*\d+.{0,20}(win|winner)|win.{0,20}(map|game)[\s\-_]*\d+", re.I)),
    ("winner", re.compile(r"\bwin(s|ner)?\b|\bbeat(s)?\b|\bto[\s\-_]win\b", re.I)),
    # "vs" встречается практически в каждом киберспортивном слаге, включая
    # проп-рынки. Если считать его признаком winner, колонка kind схлопнется в
    # одно значение и разрез по типам подрынка перестанет что-либо значить.
    # Очная пара без явного признака исхода — это matchup, а не winner.
    ("matchup", re.compile(r"\bvs\.?\b|\b-v-\b", re.I)),
]


def detect_sport(*texts: str | None) -> str | None:
    blob = " ".join(t for t in texts if t)
    if _CS2.search(blob):
        return "cs2"
    if _DOTA.search(blob):
        return "dota2"
    return None


def detect_kind(*texts: str | None) -> str:
    blob = " ".join(t for t in texts if t)
    for name, rx in _KINDS:
        if rx.search(blob):
            return name
    return "other"


def detect_segment(*texts: str | None) -> int | None:
    blob = " ".join(t for t in texts if t)
    m = _SEGMENT.search(blob)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    m = _ROUND_NO.search(blob)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def detect_level(kind: str, segment_no: int | None, *texts: str | None) -> str:
    """series (весь матч) | map (конкретная карта/игра) | round | prop."""
    blob = " ".join(t for t in texts if t)
    if re.search(r"\bround[\s\-_]*\d", blob, re.I) or kind == "pistol_round":
        return "round"
    if segment_no is not None or kind in ("map_winner", "total_rounds"):
        return "map"
    if kind in ("winner", "matchup", "handicap", "total_maps", "correct_score"):
        return "series"
    return "prop"


def classify(slug: str | None, question: str | None) -> dict:
    sport = detect_sport(slug, question)
    kind = detect_kind(slug, question)
    segment_no = detect_segment(slug, question)
    level = detect_level(kind, segment_no, slug, question)
    return {
        "sport": sport,
        "kind": kind,
        "segment_no": segment_no,
        "market_level": level,
    }
