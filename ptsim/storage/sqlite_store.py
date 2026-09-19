"""Хранилище на SQLite с WAL.

Буферизованная запись: операции копятся в памяти и применяются одной
транзакцией раз в commit_interval_s. При событийной нагрузке (~сотни событий в
сутки) это микросекунды, поэтому поток не нужен — блокировки цикла не будет.

Все операции идемпотентны по ключам, чтобы рестарт посреди записи не создавал
дублей: реестр рынков и кэш резолюций переживают перезапуск процесса.
"""
from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Sequence

log = logging.getLogger("store")

_SCHEMA = Path(__file__).with_name("schema.sql")


class SqliteStore:
    def __init__(self, db_path: str, commit_interval_s: float = 1.0) -> None:
        self.db_path = db_path
        self.commit_interval_s = commit_interval_s
        self._conn: sqlite3.Connection | None = None
        self._ops: list[tuple] = []
        self._task: asyncio.Task | None = None
        self._closing = False
        self.rows_written = 0

    # ------------------------------------------------------------ lifecycle

    def open(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA.read_text())
        log.info("хранилище открыто: %s", self.db_path)

    async def start(self) -> None:
        if self._conn is None:
            self.open()
        self._task = asyncio.create_task(self._loop(), name="store-flush")

    async def _loop(self) -> None:
        try:
            while not self._closing:
                await asyncio.sleep(self.commit_interval_s)
                self.flush()
        except asyncio.CancelledError:
            self.flush()
            raise

    async def close(self) -> None:
        self._closing = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.flush()
        if self._conn:
            self._conn.close()
            self._conn = None

    # --------------------------------------------------------------- запись

    def insert(self, table: str, row: dict[str, Any]) -> None:
        self._ops.append(("insert", table, row))

    def upsert(self, table: str, row: dict[str, Any], keys: Sequence[str]) -> None:
        self._ops.append(("upsert", table, row, tuple(keys)))

    def update(self, table: str, keys: dict[str, Any], values: dict[str, Any]) -> None:
        """Поздняя запись в уже написанную строку.

        Ради этого и выбран SQLite: чекпоинт +60 минут, резолюция,
        trigger_confirmed и target_wallet_traded_here пишутся часами позже
        самой строки события.
        """
        if values:
            self._ops.append(("update", table, keys, values))

    def accumulate(self, table: str, keys: dict[str, Any], adds: dict[str, float]) -> None:
        """Прибавление к счётчикам (coverage). Создаёт строку, если её нет."""
        self._ops.append(("accumulate", table, keys, adds))

    def set_kv(self, key: str, value: Any) -> None:
        self.upsert("kv", {"key": key, "value": json.dumps(value)}, ["key"])

    def get_kv(self, key: str, default: Any = None) -> Any:
        row = self.query_one("SELECT value FROM kv WHERE key = ?", (key,))
        if row is None:
            return default
        try:
            return json.loads(row["value"])
        except (TypeError, ValueError):
            return default

    def flush(self) -> None:
        if not self._ops or self._conn is None:
            return
        ops, self._ops = self._ops, []
        cur = self._conn.cursor()
        try:
            cur.execute("BEGIN")
            for op in ops:
                self._apply(cur, op)
            cur.execute("COMMIT")
            self.rows_written += len(ops)
        except Exception:
            try:
                cur.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            log.exception("сброс на диск не удался, потеряно операций: %d", len(ops))

    def _apply(self, cur: sqlite3.Cursor, op: tuple) -> None:
        kind = op[0]
        if kind == "insert":
            _, table, row = op
            cols = ",".join(row)
            marks = ",".join("?" * len(row))
            cur.execute(
                f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({marks})",
                tuple(row.values()),
            )
        elif kind == "upsert":
            _, table, row, keys = op
            cols = ",".join(row)
            marks = ",".join("?" * len(row))
            upd = ",".join(f"{c}=excluded.{c}" for c in row if c not in keys)
            conflict = ",".join(keys)
            sql = f"INSERT INTO {table} ({cols}) VALUES ({marks}) ON CONFLICT({conflict}) DO "
            sql += f"UPDATE SET {upd}" if upd else "NOTHING"
            cur.execute(sql, tuple(row.values()))
        elif kind == "update":
            _, table, keys, values = op
            sets = ",".join(f"{c}=?" for c in values)
            where = " AND ".join(f"{c}=?" for c in keys)
            cur.execute(
                f"UPDATE {table} SET {sets} WHERE {where}",
                (*values.values(), *keys.values()),
            )
        elif kind == "accumulate":
            _, table, keys, adds = op
            kcols = ",".join(keys)
            kmarks = ",".join("?" * len(keys))
            cur.execute(
                f"INSERT OR IGNORE INTO {table} ({kcols}) VALUES ({kmarks})",
                tuple(keys.values()),
            )
            sets = ",".join(f"{c}=COALESCE({c},0)+?" for c in adds)
            where = " AND ".join(f"{c}=?" for c in keys)
            cur.execute(
                f"UPDATE {table} SET {sets} WHERE {where}",
                (*adds.values(), *keys.values()),
            )

    # --------------------------------------------------------------- чтение

    def execute_raw(self, sql: str, params: Iterable = ()) -> int:
        """Прямой UPDATE по множеству строк (массовый бэкфилл).

        Сбрасывает буфер перед выполнением, чтобы не обогнать отложенные
        вставки: иначе бэкфилл прошёл бы мимо строк, которые ещё в очереди.
        """
        if self._conn is None:
            raise RuntimeError("SqliteStore: обращение к базе до open()/start()")
        self.flush()
        cur = self._conn.execute(sql, tuple(params))
        return cur.rowcount

    def query(self, sql: str, params: Iterable = ()) -> list[sqlite3.Row]:
        if self._conn is None:
            raise RuntimeError("SqliteStore: обращение к базе до open()/start()")
        self.flush()
        return list(self._conn.execute(sql, tuple(params)))

    def query_one(self, sql: str, params: Iterable = ()) -> sqlite3.Row | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None


def read_only(db_path: str) -> sqlite3.Connection:
    """Соединение для анализатора и экспортёра: читает базу под живой записью."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn
