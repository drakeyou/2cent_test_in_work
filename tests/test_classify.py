from ptsim.classify import classify, detect_sport


def test_sport_detection():
    assert detect_sport("cs2-navi-vs-faze-map-2", None) == "cs2"
    assert detect_sport(None, "Will Team Spirit win Map 1 (Dota 2)?") == "dota2"
    assert detect_sport("lakers-vs-celtics", "NBA game") is None
    assert detect_sport("counter-strike-vitality-vs-g2", None) == "cs2"


def test_map_level_market():
    c = classify("cs2-navi-vs-faze-map-2-winner", "Will NAVI win Map 2?")
    assert c["sport"] == "cs2" and c["segment_no"] == 2
    assert c["market_level"] == "map"


def test_series_level_market():
    c = classify("dota2-og-vs-spirit", "Will OG beat Team Spirit?")
    assert c["sport"] == "dota2" and c["market_level"] == "series"
    assert c["kind"] == "winner" and c["segment_no"] is None


def test_prop_markets():
    c = classify("dota2-og-vs-spirit-first-blood-game-1", "First blood in game 1")
    assert c["kind"] == "first_blood" and c["segment_no"] == 1
    c = classify("cs2-total-rounds-over-24-5-map-1", None)
    assert c["kind"] == "total_rounds" and c["market_level"] == "map"


def test_bare_matchup_is_not_promoted_to_winner():
    """`vs` есть почти везде; если он значит winner, колонка kind бесполезна."""
    c = classify("cs2-navi-vs-faze-some-new-format", "Something novel")
    assert c["kind"] == "matchup" and c["sport"] == "cs2"
    assert c["market_level"] == "series"


def test_unknown_stays_other_not_a_guess():
    c = classify("cs2-some-new-market-format", "Something novel")
    assert c["kind"] == "other" and c["market_level"] == "prop"
