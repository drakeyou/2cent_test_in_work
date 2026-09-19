#!/usr/bin/env python3
"""Экспорт SQLite -> CSV со схемой из ТЗ, один в один.

Экспорт — тупой дамп. Ни одно поле здесь не вычисляется: всё посчитано в
момент записи и лежит в базе. Колонки сверх ТЗ идут после спецификационных,
чтобы файл читался как в ТЗ и при этом ничего не терялось.

    python export_csv.py --db state/paper.db --out data/
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

SPEC: dict[str, tuple[str, list[str], str]] = {
    "paper-events.csv": ("paper_events", [
        "event_id", "ts", "condition_id", "asset_id", "sport", "market_level", "kind",
        "trigger", "bid_before", "bid_after", "best_ask_before",
        "level_traded_size", "prior_size_at_002", "our_fill", "our_entry_price",
        "prior_size_at_001", "prior_size_at_003", "prior_size_at_005",
        "depth_before", "n_bid_levels_before",
        "paired_bid", "paired_ask", "paired_ask_size", "paired_stale_seconds",
        "fair_lower_bound", "fair_upper_bound", "book_sum", "internal_dislocation",
        "market_volume", "market_liquidity", "minutes_from_game_start",
        "target_wallet_traded_here",
    ], "ORDER BY ts_ms"),
    "paper-book.csv": ("paper_book", [
        "event_id", "seconds_from_fill", "best_bid", "best_ask", "mid",
        "size_at_001", "size_at_002", "size_at_003", "size_at_005",
        "depth_bid_total", "n_bid_levels", "paired_bid", "paired_ask", "is_checkpoint",
    ], "ORDER BY event_id, seconds_from_fill"),
    "paper-quotes.csv": ("paper_quotes", [
        "event_id", "ts", "seconds_from_fill", "action", "our_ask",
        "best_ask_at_moment", "prior_size_at_our_ask", "filled_size", "reason",
    ], "ORDER BY event_id, ts_ms"),
    "paper-positions.csv": ("paper_positions", [
        "event_id", "entry_price", "entry_size", "exit_vwap", "exit_size",
        "hold_seconds", "n_partial_fills", "pnl", "multiple", "closed_by",
        "resolution_winner", "resolution_payout",
    ], "ORDER BY event_id"),
    "markets.csv": ("markets", [
        "condition_id", "asset_id_a", "asset_id_b", "question", "slug", "sport",
        "market_level", "kind", "segment_no", "volume", "liquidity",
        "game_start_time", "end_date", "first_seen", "last_seen",
        "observed_during_game", "release_reason",
    ], "ORDER BY first_seen"),
    "coverage.csv": ("coverage", [
        "hour", "sport", "observed_seconds", "gap_seconds",
    ], "ORDER BY hour, sport"),
    "gaps.csv": ("gaps", [
        "started_at", "ended_at", "duration_ms", "reason", "assets_resubscribed",
    ], "ORDER BY started_ms"),
}

# Дополнительные таблицы целиком: сверка, отмены на дне, лента окна, рассинхрон.
EXTRA_TABLES = ["reconcile_log", "book_vanished", "paper_trades", "book_desync",
                "resolutions", "target_activity"]


def columns_of(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def dump(conn: sqlite3.Connection, table: str, cols: list[str], order: str,
         path: Path, spec_only: bool = False) -> int:
    available = columns_of(conn, table)
    missing = [c for c in cols if c not in available]
    if missing:
        raise SystemExit(f"{table}: в базе нет колонок {missing}")
    extra = [] if spec_only else [c for c in available if c not in cols]
    all_cols = cols + extra
    rows = conn.execute(
        f"SELECT {','.join(all_cols)} FROM {table} {order}"
    ).fetchall()
    with path.open("w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(all_cols)
        w.writerows(rows)
    return len(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="state/paper.db")
    ap.add_argument("--out", default="data")
    ap.add_argument("--spec-only", action="store_true",
                    help="только колонки из ТЗ, без дополнительных")
    a = ap.parse_args()

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(f"file:{a.db}?mode=ro", uri=True)

    total = 0
    for fname, (table, cols, order) in SPEC.items():
        n = dump(conn, table, cols, order, out / fname, spec_only=a.spec_only)
        total += n
        print(f"{fname:24s} {n:8d} строк")
    if not a.spec_only:
        for table in EXTRA_TABLES:
            cols = columns_of(conn, table)
            if not cols:
                continue
            n = dump(conn, table, cols, "", out / f"{table.replace('_', '-')}.csv")
            total += n
            print(f"{table.replace('_', '-') + '.csv':24s} {n:8d} строк")

    size_mb = sum(f.stat().st_size for f in out.glob("*.csv")) / 1e6
    print(f"\nвсего строк: {total}, объём CSV: {size_mb:.1f} МБ")
    conn.close()


if __name__ == "__main__":
    main()
