-- Схема хранилища paper-trade симулятора.
--
-- SQLite, а не CSV, по одной причине: схема из ТЗ сама требует ПОЗДНИХ
-- ЗАПИСЕЙ в уже написанные строки — чекпоинт +60 минут, резолюция,
-- trigger_confirmed из сверки с лентой, target_wallet_traded_here (объект
-- может зайти в рынок после нашего события). CSV этого не умеет: получился бы
-- append-only с патч-строками и грязным merge на выходе. Плюс атомарность при
-- падении и чтение анализатором на живых данных.
--
-- export_csv.py выдаёт ровно те 7 файлов, что в ТЗ, без пересчёта полей.

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS markets (
  condition_id          TEXT PRIMARY KEY,
  asset_id_a            TEXT,
  asset_id_b            TEXT,
  question              TEXT,
  slug                  TEXT,
  sport                 TEXT,
  market_level          TEXT,
  kind                  TEXT,
  segment_no            INTEGER,
  volume                REAL,
  liquidity             REAL,
  game_start_time       TEXT,
  game_start_ms         INTEGER,
  -- gamma | inferred | none. Без этого поля критерий observed_during_game
  -- непроверяем: непонятно, провал это сбора или отсутствие метаданных.
  game_start_source     TEXT,
  end_date              TEXT,
  end_date_ms           INTEGER,
  first_seen            TEXT,
  last_seen             TEXT,
  subscribed_at         TEXT,
  released_at           TEXT,
  observed_during_game  INTEGER DEFAULT 0,
  release_reason        TEXT,
  -- отброшен потолком подписок; без этой колонки знаменатель частоты завышен
  dropped_by_overflow   INTEGER DEFAULT 0,
  resolved              INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_markets_sport ON markets(sport, resolved);

CREATE TABLE IF NOT EXISTS paper_events (
  event_id                 TEXT PRIMARY KEY,
  ts                       TEXT,
  ts_ms                    INTEGER,
  condition_id             TEXT,
  asset_id                 TEXT,
  sport                    TEXT,
  market_level             TEXT,
  kind                     TEXT,
  trigger                  TEXT,
  bid_before               REAL,
  bid_after                REAL,
  best_ask_before          REAL,
  level_traded_size        REAL,
  prior_size_at_002        REAL,
  our_fill                 REAL,
  our_fill_cum             REAL,
  our_entry_price          REAL,
  prior_size_at_001        REAL,
  prior_size_at_003        REAL,
  prior_size_at_005        REAL,
  depth_before             REAL,
  n_bid_levels_before      INTEGER,
  paired_bid               REAL,
  paired_ask               REAL,
  paired_ask_size          REAL,
  paired_stale_seconds     REAL,
  fair_lower_bound         REAL,
  fair_upper_bound         REAL,
  book_sum                 REAL,
  internal_dislocation     REAL,
  market_volume            REAL,
  market_liquidity         REAL,
  minutes_from_game_start  REAL,
  target_wallet_traded_here INTEGER DEFAULT 0,

  -- --- сверх ТЗ, всё считается при записи ---
  -- вторая трактовка entry.min_bid_above (нотионал вместо цены)
  bid_notional_above_002   REAL,
  bid_shares_above_002     REAL,
  -- три модели очереди вместо одной
  queue_ahead_at_placement REAL,
  queue_ahead_est          REAL,
  -- насколько книга «непосредственно до» действительно непосредственно до
  prior_size_staleness_ms  INTEGER,
  -- Что дал бы подход ТЗ: prior_size из снапшота с шагом 2 с вместо точного
  -- пособытийного состояния. Пишется рядом, чтобы расхождение двух подходов
  -- было ИЗМЕРЕНО на собранных данных, а не осталось моим утверждением.
  prior_size_at_002_snapshot REAL,
  precondition_held_at_fill INTEGER,
  -- делим на нашу цену входа, а не на bid_after: платим мы 0.02
  dislocation_vs_entry     REAL,
  -- сторона last_trade_price ещё не откалибрована на момент события
  calibration_pending      INTEGER DEFAULT 0,
  n_partial                INTEGER,
  market_slug              TEXT,
  game_start_source        TEXT,
  -- теневая заявка без кулдауна: событие, которого у нас бы не было
  is_suppressed            INTEGER DEFAULT 0,
  suppressed_reason        TEXT,
  -- --- поздние записи (сверка с ончейн-лентой) ---
  trigger_confirmed        INTEGER,
  confirmed_level_traded_size REAL,
  reconcile_verdict        TEXT,
  reconciled_at            TEXT,
  window_complete          INTEGER,
  window_truncated_reason  TEXT,
  dense_rows               INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON paper_events(ts_ms);
CREATE INDEX IF NOT EXISTS idx_events_cond ON paper_events(condition_id);
CREATE INDEX IF NOT EXISTS idx_events_sup ON paper_events(is_suppressed);

CREATE TABLE IF NOT EXISTS paper_book (
  event_id          TEXT,
  seconds_from_fill REAL,
  ts_ms             INTEGER,
  best_bid          REAL,
  best_ask          REAL,
  mid               REAL,
  size_at_001       REAL,
  size_at_002       REAL,
  size_at_003       REAL,
  size_at_005       REAL,
  depth_bid_total   REAL,
  n_bid_levels      INTEGER,
  paired_bid        REAL,
  paired_ask        REAL,
  is_checkpoint     INTEGER DEFAULT 0,
  PRIMARY KEY (event_id, seconds_from_fill, is_checkpoint)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS paper_quotes (
  event_id             TEXT,
  ts                   TEXT,
  ts_ms                INTEGER,
  seconds_from_fill    REAL,
  action               TEXT,
  our_ask              REAL,
  best_ask_at_moment   REAL,
  prior_size_at_our_ask REAL,
  filled_size          REAL,
  reason               TEXT
);
CREATE INDEX IF NOT EXISTS idx_quotes_event ON paper_quotes(event_id);

CREATE TABLE IF NOT EXISTS paper_positions (
  event_id          TEXT PRIMARY KEY,
  asset_id          TEXT,
  condition_id      TEXT,
  entry_price       REAL,
  entry_size        REAL,
  exit_vwap         REAL,
  exit_size         REAL,
  hold_seconds      REAL,
  n_partial_fills   INTEGER,
  pnl               REAL,
  multiple          REAL,
  closed_by         TEXT,
  resolution_winner INTEGER,
  resolution_payout REAL,
  -- политика «120 секунд» считается без пересбора данных
  exit_vwap_at_window_end   REAL,
  filled_size_at_window_end REAL,
  is_suppressed     INTEGER DEFAULT 0,
  updated_at        TEXT
);

-- Лента сделок внутри окна события. Без неё альтернативные политики выхода
-- (статический аск на 4x, 0.5 * fair_lower_bound, хедж) офлайн не считаются:
-- по снапшотам книги видно только «доходил ли аск до цены», а это другой и
-- более слабый вопрос, чем «была ли по ней сделка».
CREATE TABLE IF NOT EXISTS paper_trades (
  event_id          TEXT,
  ts_ms             INTEGER,
  seconds_from_fill REAL,
  asset_id          TEXT,
  price             REAL,
  size              REAL,
  side_hit          TEXT,
  source            TEXT,
  -- из last_trade_price; даёт точную сверку с лентой по хэшу
  tx_hash           TEXT
);
CREATE INDEX IF NOT EXISTS idx_ptrades_event ON paper_trades(event_id);

-- Отдельный журнал «биды исчезли, но сделки не было». Пишется ТОЛЬКО когда у
-- нас лежала виртуальная заявка: отмены на 0.02 идут непрерывно, без гейта
-- эта отладочная таблица была бы на порядки больше основной.
CREATE TABLE IF NOT EXISTS book_vanished (
  ts_ms            INTEGER,
  asset_id         TEXT,
  condition_id     TEXT,
  tick             INTEGER,
  reduced          REAL,
  traded           REAL,
  cancelled        REAL,
  unknown          REAL,
  evidence         TEXT,
  bid_before       REAL,
  opposite_side_changed_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_vanished_ts ON book_vanished(ts_ms);

CREATE TABLE IF NOT EXISTS coverage (
  hour             TEXT,
  sport            TEXT,
  observed_seconds REAL DEFAULT 0,
  gap_seconds      REAL DEFAULT 0,
  eligible_seconds REAL DEFAULT 0,
  resting_seconds  REAL DEFAULT 0,
  n_markets        INTEGER DEFAULT 0,
  n_assets         INTEGER DEFAULT 0,
  dropped_markets  INTEGER DEFAULT 0,
  sampling_rate    REAL DEFAULT 1.0,
  classify_unknown REAL DEFAULT 0,
  n_events         INTEGER DEFAULT 0,
  PRIMARY KEY (hour, sport)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS gaps (
  started_at          TEXT,
  ended_at            TEXT,
  started_ms          INTEGER,
  ended_ms            INTEGER,
  duration_ms         INTEGER,
  reason              TEXT,
  assets_resubscribed INTEGER,
  shard               INTEGER
);
CREATE INDEX IF NOT EXISTS idx_gaps_start ON gaps(started_ms);

CREATE TABLE IF NOT EXISTS resolutions (
  condition_id     TEXT PRIMARY KEY,
  question         TEXT,
  market_slug      TEXT,
  game_start_time  TEXT,
  end_date_iso     TEXT,
  closed           INTEGER,
  token_a          TEXT,
  token_b          TEXT,
  winner_a         INTEGER,
  winner_b         INTEGER,
  price_a          REAL,
  price_b          REAL,
  fetched_at       TEXT
);

-- Матрица ошибок WS-классификатора против ончейн-ленты. Заголовочная метрика
-- всего датасета: без неё PnL из симулятора нечем поверить.
CREATE TABLE IF NOT EXISTS reconcile_log (
  event_id       TEXT,
  checked_at     TEXT,
  verdict        TEXT,   -- confirmed | phantom_trade | missed_trade | no_data
  ws_size        REAL,
  chain_size     REAL,
  n_chain_trades INTEGER,
  detail         TEXT
);
CREATE INDEX IF NOT EXISTS idx_reconcile_event ON reconcile_log(event_id);

CREATE TABLE IF NOT EXISTS book_desync (
  ts             TEXT,
  asset_id       TEXT,
  ws_best_bid    REAL,
  rest_best_bid  REAL,
  ws_best_ask    REAL,
  rest_best_ask  REAL,
  ws_levels      INTEGER,
  rest_levels    INTEGER,
  max_level_diff REAL,
  severity       TEXT
);

CREATE TABLE IF NOT EXISTS target_activity (
  address      TEXT,
  condition_id TEXT,
  asset_id     TEXT,
  ts_ms        INTEGER,
  side         TEXT,
  price        REAL,
  size         REAL,
  tx_hash      TEXT,
  PRIMARY KEY (address, tx_hash, asset_id, ts_ms)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_target_cond ON target_activity(condition_id);

CREATE TABLE IF NOT EXISTS kv (
  key   TEXT PRIMARY KEY,
  value TEXT
);
