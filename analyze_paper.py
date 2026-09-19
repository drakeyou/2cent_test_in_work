#!/usr/bin/env python3
"""Анализ собранных данных paper-trade симулятора.

    python analyze_paper.py --db state/paper.db

Порядок разделов не случаен. Матрица ошибок классификатора идёт ПЕРВОЙ: она
говорит, насколько WS-классификация «сделка против отмены» сходится с
ончейн-лентой. Если сходимость низкая, все последующие числа описывают филлы,
которых не было, и читать их нельзя.

Второй по важности — частота события, главное неизвестное проекта. Она даётся
в трёх нормировках, потому что «как часто бывает обвал» и «как часто бывает
обвал там, где мы стояли бы» — разные вопросы.
"""
from __future__ import annotations

import argparse
import math
import random
import sqlite3
import statistics
from collections import defaultdict

H1 = "=" * 78
H2 = "-" * 78


def pct(x: float | None, digits: int = 1) -> str:
    return "н/д" if x is None else f"{100 * x:.{digits}f}%"


def num(x: float | None, digits: int = 2) -> str:
    return "н/д" if x is None else f"{x:.{digits}f}"


def head(title: str) -> None:
    print(f"\n{H1}\n{title}\n{H1}")


def sub(title: str) -> None:
    print(f"\n{title}\n{H2}")


def bootstrap_ci(values: list[float], iters: int = 2000, alpha: float = 0.05):
    """Доверительный интервал среднего бутстрапом.

    Нужен не для красоты. Распределение хвостовое: по историческим данным 74%
    позиций в ноль, максимум 44x. На десятках событий среднее не
    стабилизируется вообще, и без интервала его прочтут как оценку.
    """
    if len(values) < 2:
        return (None, None)
    rnd = random.Random(12345)
    means = []
    n = len(values)
    for _ in range(iters):
        means.append(sum(rnd.choice(values) for _ in range(n)) / n)
    means.sort()
    lo = means[int(alpha / 2 * iters)]
    hi = means[min(iters - 1, int((1 - alpha / 2) * iters))]
    return (lo, hi)


def describe(values: list[float], label: str) -> None:
    if not values:
        print(f"  {label:34s} нет данных")
        return
    lo, hi = bootstrap_ci(values)
    zero = sum(1 for v in values if v <= 1e-9) / len(values)
    over4 = sum(1 for v in values if v >= 4.0) / len(values)
    print(
        f"  {label:34s} n={len(values):5d}  среднее={num(statistics.fmean(values))}"
        f"  медиана={num(statistics.median(values))}"
        f"  ДИ95=[{num(lo)}, {num(hi)}]"
        f"  в ноль={pct(zero, 0):>6s}  >=4x={pct(over4, 0):>6s}"
    )


# ---------------------------------------------------------------------------


def section_classifier(conn) -> None:
    head("1. МАТРИЦА ОШИБОК КЛАССИФИКАТОРА (читать до всего остального)")
    rows = conn.execute(
        "SELECT verdict, COUNT(*) n FROM reconcile_log GROUP BY verdict"
    ).fetchall()
    total = sum(r["n"] for r in rows)
    if not total:
        print("\n  Сверка с ончейн-лентой ещё не проводилась.")
        print("  БЕЗ НЕЁ ОСТАЛЬНЫЕ РАЗДЕЛЫ НЕ ИМЕЮТ ПОДТВЕРЖДЁННОЙ ОСНОВЫ:")
        print("  неизвестно, какая доля зафиксированных входов — реальные сделки.")
        return
    by = {r["verdict"]: r["n"] for r in rows}
    confirmed = by.get("confirmed", 0)
    phantom = by.get("phantom_trade", 0)
    missed = by.get("missed_trade", 0)
    checked = confirmed + phantom
    print(f"\n  подтверждено лентой        {confirmed:6d}")
    print(f"  фантом (WS видел, цепь нет) {phantom:6d}")
    print(f"  пропущено (цепь видела, WS нет) {missed:4d}")
    if checked:
        acc = confirmed / checked
        print(f"\n  СХОДИМОСТЬ WS С ЛЕНТОЙ: {pct(acc)}")
        if acc >= 0.95:
            print("  -> классификации можно верить, числа ниже осмысленны.")
        elif acc >= 0.85:
            print("  -> заметная доля входов не подтверждается; PnL завышен.")
        else:
            print("  -> НИЗКАЯ. Проект надо строить на ленте, а не на WS.")
            print("     Числа ниже описывают филлы, которых в значительной части не было.")

    ev = conn.execute(
        "SELECT evidence, COUNT(*) n FROM book_vanished GROUP BY evidence"
    ).fetchall()
    if ev:
        sub("Отмены на дне (нас бы НЕ залило), по типу свидетельства")
        for r in ev:
            print(f"  {str(r['evidence']):24s} {r['n']:6d}")
        v = conn.execute("SELECT COUNT(*) n FROM book_vanished").fetchone()["n"]
        e = conn.execute(
            "SELECT COUNT(*) n FROM paper_events WHERE is_suppressed=0"
        ).fetchone()["n"]
        if v + e:
            print(f"\n  Доля исчезновений книги, которые были ОТМЕНОЙ, а не сделкой: "
                  f"{pct(v / (v + e))}")
            print("  Ровно на эту долю ошибся бы триггер «биды резко исчезли».")


def section_frequency(conn) -> None:
    head("2. ЧАСТОТА СОБЫТИЯ — главное неизвестное проекта")
    cov = conn.execute(
        "SELECT sport, SUM(observed_seconds) obs, SUM(gap_seconds) gap, "
        "SUM(eligible_seconds) elig, SUM(resting_seconds) rest, "
        "SUM(n_events) ev, AVG(sampling_rate) rate, SUM(dropped_markets) dropped "
        "FROM coverage GROUP BY sport"
    ).fetchall()
    if not cov:
        print("\n  Нет данных о покрытии.")
        return
    print(f"\n  {'дисц.':8s} {'рынко-ч':>9s} {'минус gap':>10s} {'с предусл.':>11s} "
          f"{'с заявкой':>10s} {'событий':>8s} {'на рынко-час':>13s}")
    for r in cov:
        obs_h = (r["obs"] or 0) / 3600.0
        net_h = max(0.0, ((r["obs"] or 0) - (r["gap"] or 0))) / 3600.0
        elig_h = (r["elig"] or 0) / 3600.0
        rest_h = (r["rest"] or 0) / 3600.0
        n = r["ev"] or 0
        print(f"  {r['sport']:8s} {obs_h:9.1f} {net_h:10.1f} {elig_h:11.1f} "
              f"{rest_h:10.1f} {n:8d} {(n / net_h if net_h else 0):13.3f}")
        if elig_h > 0:
            print(f"  {'':8s} {'':9s} {'':10s} {'':11s} {'':10s} {'':8s} "
                  f"{n / elig_h:13.3f}  <- на час С ПРЕДУСЛОВИЕМ")
        if r["dropped"]:
            print(f"  {'':8s} внимание: отброшено рынков потолком подписки "
                  f"{r['dropped']}, выборка {num(r['rate'], 3)}")
            print(f"  {'':8s} частоту экстраполировать делением на sampling_rate")

    print("\n  Два знаменателя отвечают на разные вопросы. Первый — как часто")
    print("  случается обвал вообще. Второй — как часто он случается там, где мы")
    print("  вообще стояли бы. Публиковать один без другого нельзя.")

    sub("Разрез по дисциплине и типу подрынка")
    rows = conn.execute(
        "SELECT sport, market_level, kind, COUNT(*) n, "
        "AVG(our_fill) fill, AVG(prior_size_at_002) prior "
        "FROM paper_events WHERE is_suppressed=0 "
        "GROUP BY sport, market_level, kind ORDER BY n DESC LIMIT 20"
    ).fetchall()
    if rows:
        print(f"  {'дисц.':8s} {'уровень':10s} {'тип':16s} {'событий':>8s} "
              f"{'ср. филл':>9s} {'ср. очередь':>12s}")
        for r in rows:
            print(f"  {r['sport']:8s} {r['market_level'] or '':10s} {r['kind'] or '':16s} "
                  f"{r['n']:8d} {num(r['fill'], 0):>9s} {num(r['prior'], 0):>12s}")

    sup = conn.execute(
        "SELECT suppressed_reason, COUNT(*) n FROM paper_events "
        "WHERE is_suppressed=1 GROUP BY suppressed_reason"
    ).fetchall()
    if sup:
        sub("Подавленные возможности (теневая заявка без кулдауна)")
        for r in sup:
            print(f"  {str(r['suppressed_reason']):20s} {r['n']:6d}")
        print("\n  Это события, которых у нас бы НЕ было из-за кулдауна и лимитов.")
        print("  Прибавив их к основным, получаем частоту при кулдауне = 0.")


def section_queue(conn) -> None:
    head("3. МОДЕЛЬ ОЧЕРЕДИ")
    row = conn.execute(
        "SELECT COUNT(*) n, SUM(CASE WHEN our_fill<=0 THEN 1 ELSE 0 END) zero, "
        "SUM(CASE WHEN prior_size_at_002<=0 THEN 1 ELSE 0 END) empty, "
        "AVG(our_fill) fill, AVG(prior_size_at_002) prior, "
        "AVG(prior_size_staleness_ms) stale "
        "FROM paper_events WHERE is_suppressed=0"
    ).fetchone()
    n = row["n"] or 0
    if not n:
        print("\n  Событий нет.")
        return
    print(f"\n  событий                                  {n}")
    print(f"  из них our_fill = 0 из-за очереди        {row['zero']} ({pct((row['zero'] or 0)/n)})")
    print(f"  уровень 0.02 был ПУСТ                    {row['empty']} ({pct((row['empty'] or 0)/n)})")
    print(f"  средний филл / средняя очередь впереди   {num(row['fill'],0)} / {num(row['prior'],0)}")
    print(f"  возраст последнего изменения книги, мс   {num(row['stale'],0)}")
    print("    (не мера ошибки: книга ведётся пособытийно, состояние точное;")
    print("     это просто время, которое книга простояла без изменений)")
    print("\n  Доля пустого уровня — прямая проверка исходного наблюдения")
    print("  (по истории объекта на 2 центах пусто в 52-59% случаев).")

    alt = conn.execute(
        "SELECT AVG(queue_ahead_at_placement) a, AVG(queue_ahead_est) b "
        "FROM paper_events WHERE is_suppressed=0"
    ).fetchone()
    print("\n  Альтернативные модели очереди (все три в данных, выбор офлайн):")
    print(f"    prior_size_at_002 (по ТЗ, консервативная)  {num(row['prior'],0)}")
    print(f"    очередь на МОМЕНТ ПОСТАНОВКИ заявки        {num(alt['a'],0)}")
    print(f"    она же минус учтённые отмены впереди       {num(alt['b'],0)}")
    print("    Разница между первой и второй — заявки, пришедшие ПОСЛЕ нашей.")
    print("    По price-time они стоят позади, и ТЗ их ошибочно засчитывает")
    print("    впереди. Консервативная оценка занижает филл, оптимистичная —")
    print("    завышает; истина между ними, и обе лежат в данных.")

    snap = conn.execute(
        "SELECT COUNT(*) n, SUM(CASE WHEN ABS(COALESCE(prior_size_at_002_snapshot,-1) "
        "- prior_size_at_002) > 1 THEN 1 ELSE 0 END) diff FROM paper_events "
        "WHERE is_suppressed=0 AND prior_size_at_002_snapshot IS NOT NULL"
    ).fetchone()
    if snap and snap["n"]:
        print(f"\n  Проверка подхода ТЗ «prior_size из снапшота раз в 2 с»:")
        print(f"    расходится с точным пособытийным состоянием в "
              f"{snap['diff']} из {snap['n']} событий "
              f"({pct((snap['diff'] or 0) / snap['n'])}).")
        print("    Каждое такое расхождение — прямая ошибка в множителе филла.")


def _tape(conn, event_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT price, size, side_hit, seconds_from_fill FROM paper_trades "
        "WHERE event_id = ? AND side_hit = 'ASK' ORDER BY ts_ms", (event_id,)
    ).fetchall()


def _reached(tape, target: float, horizon_s: float | None) -> bool:
    for t in tape:
        if horizon_s is not None and t["seconds_from_fill"] > horizon_s:
            break
        if (t["price"] or 0) >= target:
            return True
    return False


def section_policies(conn) -> None:
    head("4. СРАВНЕНИЕ ПОЛИТИК ВЫХОДА НА ОДНИХ ДАННЫХ")
    events = conn.execute(
        "SELECT e.event_id, e.our_entry_price, e.our_fill, e.fair_lower_bound, "
        "e.paired_ask, e.paired_ask_size, p.exit_vwap, p.exit_size, p.closed_by, "
        "p.resolution_payout "
        "FROM paper_events e LEFT JOIN paper_positions p ON p.event_id = e.event_id "
        "WHERE e.is_suppressed = 0 AND e.our_fill > 0"
    ).fetchall()
    if not events:
        print("\n  Позиций нет.")
        return

    res: dict[str, list[float]] = defaultdict(list)
    n_resolved = 0
    for e in events:
        entry = e["our_entry_price"] or 0.02
        fill = e["our_fill"] or 0.0
        payout = e["resolution_payout"]
        if payout is not None:
            n_resolved += 1
        tape = _tape(conn, e["event_id"])

        # undercut: фактический результат симуляции + остаток по резолюции
        sold = (e["exit_size"] or 0.0)
        proceeds = (e["exit_vwap"] or 0.0) * sold
        if payout is not None:
            proceeds += max(0.0, fill - sold) * payout
            res["undercut (симуляция)"].append(proceeds / (entry * fill) if fill else 0.0)

        if payout is not None:
            res["держать до резолюции"].append(payout / entry)

            t4 = entry * 4.0
            hit4 = _reached(tape, t4, None)
            res["статичный аск 4x"].append(4.0 if hit4 else payout / entry)
            res["половина 4x + остаток до конца"].append(
                (0.5 * 4.0 + 0.5 * payout / entry) if hit4 else payout / entry
            )

            flb = e["fair_lower_bound"]
            if flb:
                half = 0.5 * flb
                res["аск 0.5 x fair_lower_bound"].append(
                    half / entry if _reached(tape, half, None) else payout / entry
                )
                # Хедж: покупаем близнеца, получаем комплект ценой ровно $1 на
                # резолюции. Единственная политика, которой не нужен покупатель.
                pask = e["paired_ask"]
                if pask and 0 < pask < 1:
                    avail = min(fill, e["paired_ask_size"] or 0.0)
                    share = avail / fill if fill else 0.0
                    hedged = share * (1.0 / (entry + pask))
                    rest = (1 - share) * (payout / entry)
                    res["хедж по аску близнеца"].append(hedged + rest)

    print(f"\n  событий с известной резолюцией: {n_resolved} из {len(events)}")
    print("  множитель = итог / затраты входа\n")
    for label in ("undercut (симуляция)", "статичный аск 4x",
                  "аск 0.5 x fair_lower_bound", "держать до резолюции",
                  "половина 4x + остаток до конца", "хедж по аску близнеца"):
        describe(res.get(label, []), label)

    print("\n  ЧИТАТЬ ТАК. Результат undercut — ВЕРХНЯЯ ГРАНИЦА: симуляция не")
    print("  реагирует на нас, а в реальности мейкер, чей аск мы подрезали,")
    print("  подрежет нас в ответ. Статичный аск на 4x от реакции контрагента")
    print("  почти не зависит и является более доверенной оценкой.")
    print("  Политики, кроме undercut, посчитаны без модели очереди: на уровне")
    print("  сильно выше рынка после обвала он обычно пуст, но это допущение.")
    print("  Хедж по близнецу добавлен сверх ТЗ: он не требует покупателя на")
    print("  нашей стороне, а именно её отсутствие и есть проблема выхода.")

    sub("Закрытие позиций undercut")
    rows = conn.execute(
        "SELECT closed_by, COUNT(*) n, AVG(hold_seconds) hold FROM paper_positions "
        "WHERE is_suppressed=0 GROUP BY closed_by"
    ).fetchall()
    for r in rows:
        print(f"  {str(r['closed_by']):16s} {r['n']:6d}  средний холд "
              f"{num(r['hold'], 1)} c")


def section_dislocation(conn) -> None:
    head("5. РАЗРЕЗ ПО РАСХОЖДЕНИЮ С ПОЛОМ ПО БЛИЗНЕЦУ")
    rows = conn.execute(
        "SELECT e.dislocation_vs_entry d, e.internal_dislocation di, e.bid_after, "
        "e.event_id, e.our_entry_price entry, p.resolution_payout payout "
        "FROM paper_events e LEFT JOIN paper_positions p ON p.event_id = e.event_id "
        "WHERE e.is_suppressed = 0"
    ).fetchall()
    if not rows:
        print("\n  Событий нет.")
        return

    buckets = [("< 1 (покупаем выше пола)", -math.inf, 1.0),
               ("1 - 2", 1.0, 2.0), ("2 - 5", 2.0, 5.0),
               ("5 - 10", 5.0, 10.0), (">= 10", 10.0, math.inf)]
    print(f"\n  По dislocation_vs_entry = fair_lower_bound / цена входа")
    print(f"  (делим на НАШУ цену: платим мы 0.02, а не bid_after)\n")
    print(f"  {'корзина':28s} {'n':>5s} {'4x за 5 мин':>12s} {'ср. множитель':>14s}")
    for label, lo, hi in buckets:
        sel = [r for r in rows if r["d"] is not None and lo <= r["d"] < hi]
        if not sel:
            continue
        hit = sum(1 for r in sel if _reached(_tape(conn, r["event_id"]),
                                             (r["entry"] or 0.02) * 4, 300))
        mult = [r["payout"] / (r["entry"] or 0.02) for r in sel if r["payout"] is not None]
        print(f"  {label:28s} {len(sel):5d} {pct(hit/len(sel), 0):>12s} "
              f"{(num(statistics.fmean(mult)) if mult else 'н/д'):>14s}")

    n_null = sum(1 for r in rows if r["d"] is None)
    # `r["bid_after"] or -1` дал бы -1 для нуля: 0.0 в Python ложно.
    # Это обнулило бы ровно ту категорию, ради которой ноль и NULL разведены.
    n_empty = sum(1 for r in rows if r["bid_after"] is not None and r["bid_after"] == 0)
    print(f"\n  без пола по близнецу (нет paired_ask): {n_null}")
    print(f"  bid_after = 0 (книга выметена целиком):  {n_empty}")
    print("  internal_dislocation в этих событиях NULL по построению: делить")
    print("  на ноль нельзя, а они самые интересные. Использовать колонку")
    print("  dislocation_vs_entry, она определена всегда, когда есть близнец.")


def section_target(conn) -> None:
    head("6. СВЕРКА С ОБЪЕКТОМ ИССЛЕДОВАНИЯ")
    # Считаем JOIN-ом по ленте активности, а не по денормализованному флагу:
    # флаг проставляется поздним бэкфиллом, и если он не успел пройти, все
    # события молча уедут в третью группу — ту самую, которую хочется видеть
    # большой. Тихая ошибка в пользу гипотезы недопустима.
    both = conn.execute(
        "SELECT COUNT(*) n FROM paper_events e WHERE e.is_suppressed=0 "
        "AND EXISTS (SELECT 1 FROM target_activity t "
        "            WHERE t.condition_id = e.condition_id)"
    ).fetchone()["n"]
    only_us = conn.execute(
        "SELECT COUNT(*) n FROM paper_events e WHERE e.is_suppressed=0 "
        "AND NOT EXISTS (SELECT 1 FROM target_activity t "
        "                WHERE t.condition_id = e.condition_id)"
    ).fetchone()["n"]
    stale_flag = conn.execute(
        "SELECT COUNT(*) n FROM paper_events e WHERE e.is_suppressed=0 "
        "AND e.target_wallet_traded_here = 0 AND EXISTS "
        "(SELECT 1 FROM target_activity t WHERE t.condition_id = e.condition_id)"
    ).fetchone()["n"]
    only_him = conn.execute(
        "SELECT COUNT(DISTINCT t.condition_id) n FROM target_activity t "
        "JOIN markets m ON m.condition_id = t.condition_id "
        "WHERE t.condition_id NOT IN (SELECT DISTINCT condition_id FROM paper_events)"
    ).fetchone()["n"]

    print(f"\n  он зашёл и мы бы зашли        {both:6d}")
    print(f"  он зашёл, мы бы нет (рынков)  {only_him:6d}")
    print(f"  он не зашёл, мы бы да         {only_us:6d}   <- самая интересная группа")
    if stale_flag:
        print(f"\n  (колонка target_wallet_traded_here отстаёт на {stale_flag} строк: "
              f"бэкфилл ещё не прошёл;")
        print("   в CSV она будет неточной, пока не отработает опрос активности)")

    gap = conn.execute("SELECT value FROM kv WHERE key='target_activity_gap_seconds'").fetchone()
    print("\n  ОСТОРОЖНО С ТРЕТЬЕЙ ГРУППОЙ. Она держится на полноте ленты активности")
    print("  по четырём адресам: любой пропуск его сделки перекладывает событие из")
    print("  первой группы в третью, то есть ошибка сбора работает В ПОЛЬЗУ гипотезы.")
    if gap:
        print(f"  Накопленный пропуск опроса активности: {gap['value']} c.")
        print("  Пока он не близок к нулю, третью группу публиковать как число нельзя.")
    else:
        print("  Пропусков опроса не зафиксировано.")


def section_quality(conn) -> None:
    head("7. КРИТЕРИИ ГОТОВНОСТИ")

    def one(q, params=()):
        r = conn.execute(q, params).fetchone()
        return (r[0] or 0) if r else 0

    checks = []
    m_total = one("SELECT COUNT(*) FROM markets")
    if m_total:
        checks.append(("рынков с observed_during_game=1",
                       one("SELECT COUNT(*) FROM markets WHERE observed_during_game=1") / m_total,
                       0.80))
        known = one("SELECT COUNT(*) FROM markets WHERE game_start_source='gamma'")
        if known:
            checks.append(("  то же, только с известным gameStartTime",
                           one("SELECT COUNT(*) FROM markets WHERE observed_during_game=1 "
                               "AND game_start_source='gamma'") / known, 0.80))
    e_total = one("SELECT COUNT(*) FROM paper_events WHERE is_suppressed=0")
    if e_total:
        checks.append(("событий с непустым fair_lower_bound",
                       one("SELECT COUNT(*) FROM paper_events WHERE is_suppressed=0 "
                           "AND fair_lower_bound IS NOT NULL") / e_total, 0.90))
        checks.append(("событий с paired_stale_seconds < 30",
                       one("SELECT COUNT(*) FROM paper_events WHERE is_suppressed=0 "
                           "AND paired_stale_seconds < 30") / e_total, 0.70))
        checks.append(("событий с полным плотным окном (76 узлов)",
                       one("SELECT COUNT(*) FROM paper_events WHERE is_suppressed=0 "
                           "AND window_complete=1") / e_total, 0.80))
    p_total = one("SELECT COUNT(*) FROM paper_positions WHERE is_suppressed=0")
    if p_total:
        checks.append(("позиций с известной резолюцией",
                       one("SELECT COUNT(*) FROM paper_positions WHERE is_suppressed=0 "
                           "AND resolution_payout IS NOT NULL") / p_total, 0.95))

    print()
    for label, value, threshold in checks:
        ok = "ДА " if value >= threshold else "НЕТ"
        print(f"  [{ok}] {label:44s} {pct(value):>7s}  (порог {pct(threshold, 0)})")

    has_gap = conn.execute("PRAGMA table_info(coverage)").fetchall()
    print(f"  [{'ДА ' if any(c[1] == 'gap_seconds' for c in has_gap) else 'НЕТ'}] "
          f"{'coverage: есть колонка gap_seconds':44s}")

    page = conn.execute("PRAGMA page_count").fetchone()[0]
    psize = conn.execute("PRAGMA page_size").fetchone()[0]
    mb = page * psize / 1e6
    print(f"  [{'ДА ' if mb < 50 else 'НЕТ'}] {'суммарный объём базы':44s} {mb:6.1f} МБ  (порог 50)")

    unknown = one("SELECT SUM(classify_unknown) FROM coverage")
    print(f"\n  Не поддаётся классификации (снапшот посреди события): {num(unknown, 0)} долей.")
    print("  Это метрика качества, а не мусор: чем выше, тем меньше веса у")
    print("  разделения «сделка против отмены».")

    print("\n  ЧТО ЭТИ КРИТЕРИИ НЕ ПРОВЕРЯЮТ. Все семь — про качество сбора, и это")
    print("  правильно. Но по суткам нельзя читать PnL: распределение хвостовое,")
    print("  среднее по десяткам событий не стабилизируется. Смотреть на ДИ95 в")
    print("  разделе 4; если он шире самого среднего, числа рано интерпретировать.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="state/paper.db")
    a = ap.parse_args()
    conn = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    print(H1)
    print("PAPER-TRADE POLYMARKET — ОТЧЁТ")
    print(f"база: {a.db}")
    print(H1)
    section_classifier(conn)
    section_frequency(conn)
    section_queue(conn)
    section_policies(conn)
    section_dislocation(conn)
    section_target(conn)
    section_quality(conn)
    print()
    conn.close()


if __name__ == "__main__":
    main()
