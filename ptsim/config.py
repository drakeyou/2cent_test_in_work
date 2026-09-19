"""Загрузка и валидация config.yaml.

Без pydantic: dataclasses + явные проверки. У долгоживущего процесса каждая
лишняя зависимость — это лишний способ не стартовать после рестарта.

Валидация строгая и падает на старте, а не на первом событии через четыре
часа наблюдения.
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .util import MAX_TICK, MIN_TICK, dense_grid, to_tick


class ConfigError(ValueError):
    pass


@dataclass
class EntryCfg:
    price: float = 0.02
    size_shares: float = 1000.0
    min_best_bid: float = 0.08
    cancel_when_precondition_breaks: bool = False
    cooldown_s: int = 300
    max_events_per_asset_per_hour: int = 12
    log_suppressed: bool = True
    price_tick: int = field(init=False, default=20)
    min_best_bid_tick: int = field(init=False, default=80)

    def __post_init__(self) -> None:
        pt = to_tick(self.price)
        bt = to_tick(self.min_best_bid)
        if pt is None:
            raise ConfigError(f"entry.price {self.price} вне [0.001, 0.999]")
        if bt is None:
            raise ConfigError(f"entry.min_best_bid {self.min_best_bid} вне [0.001, 0.999]")
        if bt <= pt:
            raise ConfigError(
                f"entry.min_best_bid ({self.min_best_bid}) должен быть строго выше "
                f"entry.price ({self.price}); иначе предусловие бессмысленно"
            )
        if self.size_shares <= 0:
            raise ConfigError("entry.size_shares должен быть положительным")
        object.__setattr__(self, "price_tick", pt)
        object.__setattr__(self, "min_best_bid_tick", bt)


@dataclass
class ExitCfg:
    policy: str = "undercut"
    tick: float = 0.001
    follow_up: bool = False
    follow_down_floor: float = 0.001
    max_quote_moves: int = 200
    min_clip_shares: float = 5.0
    max_participation: float = 1.0
    simulate_until: str = "resolution"
    static_multiple: float = 4.0
    floor_tick: int = field(init=False, default=1)

    def __post_init__(self) -> None:
        if self.policy not in ("undercut", "static_multiple", "none"):
            raise ConfigError(f"exit.policy неизвестна: {self.policy}")
        if self.simulate_until not in ("resolution", "window_end"):
            raise ConfigError(f"exit.simulate_until неизвестно: {self.simulate_until}")
        if abs(self.tick - 0.001) > 1e-9:
            raise ConfigError("exit.tick на Polymarket равен 0.001; иное не поддержано")
        if not 0.0 < self.max_participation <= 1.0:
            raise ConfigError("exit.max_participation должен быть в (0, 1]")
        ft = to_tick(self.follow_down_floor)
        if ft is None:
            raise ConfigError("exit.follow_down_floor вне диапазона тиков")
        object.__setattr__(self, "floor_tick", ft)


@dataclass
class WindowCfg:
    dense_before_s: int = 30
    dense_after_s: int = 120
    snapshot_interval_s: int = 2
    checkpoints_min: list[int] = field(default_factory=lambda: [5, 10, 20, 40, 60])
    ring_buffer_s: int = 60
    grid: list[int] = field(init=False, default_factory=list)

    def __post_init__(self) -> None:
        if self.snapshot_interval_s <= 0:
            raise ConfigError("window.snapshot_interval_s должен быть положительным")
        if self.ring_buffer_s < self.dense_before_s:
            raise ConfigError(
                f"window.ring_buffer_s ({self.ring_buffer_s}) меньше dense_before_s "
                f"({self.dense_before_s}): предсобытийная часть окна будет неполной всегда"
            )
        g = dense_grid(self.dense_before_s, self.dense_after_s, self.snapshot_interval_s)
        object.__setattr__(self, "grid", g)

    @property
    def expected_dense_rows(self) -> int:
        """76 при -30..+120/2, а не 75. Критерий готовности читать отсюда."""
        return len(self.grid)


@dataclass
class ClassifyCfg:
    window_ms: int = 1500
    credit_ttl_ms: int = 3000
    sweep_inference: bool = True
    side_calibration_trades: int = 200
    log_book_vanished: bool = True

    def __post_init__(self) -> None:
        if self.credit_ttl_ms < self.window_ms:
            raise ConfigError(
                "classify.credit_ttl_ms не может быть меньше window_ms: кредиты "
                "истекут раньше, чем истечёт окно корреляции, и сделки будут "
                "систематически классифицироваться как отмены"
            )


@dataclass
class DiscoveryCfg:
    poll_s: int = 45
    gamma_url: str = "https://gamma-api.polymarket.com/markets"
    tags_url: str = "https://gamma-api.polymarket.com/tags/slug"
    # Gamma игнорирует limit выше 100; держим реальное значение, иначе
    # пагинация «страница меньше limit — значит последняя» остановится сразу.
    limit: int = 100
    max_pages: int = 40
    tag_slugs: list[str] = field(default_factory=lambda: ["dota", "cs2", "counter-strike"])
    tag_sport_map: dict = field(default_factory=lambda: {
        "dota": "dota2", "cs2": "cs2", "counter-strike": "cs2"})
    scan_all_markets: bool = False
    max_horizon_days: float = 7.0
    subscribe_before_game_s: int = 600
    release_after_end_s: int = 1800
    max_subscription_hours: int = 12
    subscribe_when_start_unknown: bool = True

    def __post_init__(self) -> None:
        if self.limit > 100:
            raise ConfigError(
                "market_discovery.limit > 100 бессмысленно: Gamma отдаёт не более "
                "100 записей за запрос, а пагинация по признаку «страница короче "
                "limit» остановится на первой же странице"
            )


@dataclass
class WsCfg:
    url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    max_assets_per_conn: int = 100
    max_total_assets: int = 1200
    overflow_policy: str = "random_sample"
    ping_interval_s: int = 10
    backoff_initial_s: float = 1.0
    backoff_max_s: float = 60.0
    resubscribe_debounce_s: float = 5.0

    def __post_init__(self) -> None:
        if self.overflow_policy not in ("random_sample", "by_liquidity", "reject_new"):
            raise ConfigError(f"ws.overflow_policy неизвестна: {self.overflow_policy}")


@dataclass
class IntegrityCfg:
    rest_book_url: str = "https://clob.polymarket.com/book"
    check_interval_s: int = 300
    max_checks_per_minute: int = 20
    enabled: bool = True


@dataclass
class ResolutionCfg:
    url: str = "https://clob.polymarket.com/markets"
    poll_s: int = 3600


@dataclass
class ReconcileCfg:
    trades_url: str = "https://data-api.polymarket.com/trades"
    poll_s: int = 60
    delayed_pass_s: int = 300
    match_tolerance_s: int = 3


@dataclass
class TargetWalletCfg:
    activity_url: str = "https://data-api.polymarket.com/activity"
    poll_s: int = 300
    addresses: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        object.__setattr__(self, "addresses", [a.lower() for a in self.addresses])


@dataclass
class StorageCfg:
    backend: str = "sqlite"
    db_path: str = "state/paper.db"
    csv_dir: str = "data"
    commit_interval_s: float = 1.0
    keep_raw_frames_days: int = 3
    raw_frames_dir: str = "captures"
    raw_frames_for_events_only: bool = True

    def __post_init__(self) -> None:
        if self.backend not in ("sqlite", "csv"):
            raise ConfigError(f"storage.backend неизвестен: {self.backend}")


@dataclass
class HttpCfg:
    timeout_s: float = 20.0
    max_retries: int = 4


@dataclass
class Config:
    disciplines: list[str] = field(default_factory=lambda: ["dota2", "cs2"])
    entry: EntryCfg = field(default_factory=EntryCfg)
    exit: ExitCfg = field(default_factory=ExitCfg)
    window: WindowCfg = field(default_factory=WindowCfg)
    classify: ClassifyCfg = field(default_factory=ClassifyCfg)
    market_discovery: DiscoveryCfg = field(default_factory=DiscoveryCfg)
    ws: WsCfg = field(default_factory=WsCfg)
    integrity: IntegrityCfg = field(default_factory=IntegrityCfg)
    resolution: ResolutionCfg = field(default_factory=ResolutionCfg)
    reconcile: ReconcileCfg = field(default_factory=ReconcileCfg)
    target_wallet: TargetWalletCfg = field(default_factory=TargetWalletCfg)
    storage: StorageCfg = field(default_factory=StorageCfg)
    http: HttpCfg = field(default_factory=HttpCfg)
    log_level: str = "INFO"


_SECTIONS: dict[str, type] = {
    "entry": EntryCfg,
    "exit": ExitCfg,
    "window": WindowCfg,
    "classify": ClassifyCfg,
    "market_discovery": DiscoveryCfg,
    "ws": WsCfg,
    "integrity": IntegrityCfg,
    "resolution": ResolutionCfg,
    "reconcile": ReconcileCfg,
    "target_wallet": TargetWalletCfg,
    "storage": StorageCfg,
    "http": HttpCfg,
}


def _build(cls: type, raw: dict[str, Any], section: str) -> Any:
    known = {f.name for f in dataclasses.fields(cls) if f.init}
    unknown = set(raw) - known
    if unknown:
        raise ConfigError(f"{section}: неизвестные ключи {sorted(unknown)}")
    return cls(**{k: v for k, v in raw.items() if k in known})


def load(path: str | Path) -> Config:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    if not isinstance(raw, dict):
        raise ConfigError("конфиг должен быть YAML-словарём")

    # Алиас из ТЗ: entry.min_bid_above -> entry.min_best_bid.
    entry_raw = dict(raw.get("entry") or {})
    alias = entry_raw.pop("min_bid_above", None)
    if alias is not None:
        entry_raw.setdefault("min_best_bid", alias)
    raw = dict(raw)
    raw["entry"] = entry_raw

    kwargs: dict[str, Any] = {}
    for name, cls in _SECTIONS.items():
        kwargs[name] = _build(cls, dict(raw.get(name) or {}), name)

    disciplines = raw.get("disciplines") or ["dota2", "cs2"]
    bad = [d for d in disciplines if d not in ("dota2", "cs2")]
    if bad:
        raise ConfigError(f"disciplines: поддержаны только dota2 и cs2, получено {bad}")

    unknown_top = set(raw) - set(_SECTIONS) - {"disciplines", "log_level"}
    if unknown_top:
        raise ConfigError(f"неизвестные секции верхнего уровня: {sorted(unknown_top)}")

    return Config(
        disciplines=list(disciplines),
        log_level=raw.get("log_level", "INFO"),
        **kwargs,
    )
