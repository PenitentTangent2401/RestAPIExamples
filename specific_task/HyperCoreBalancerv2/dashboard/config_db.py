"""
config_db.py — Live-tunable configuration store
================================================
SQLite-backed key/value store for runtime-tunable settings.
The Dashboard seeds and owns the database; Balancer and Collector
poll it each loop iteration for live changes without restarting.

Shared Docker volume: sc-config → /config  (all three containers)
Database file:        /config/tunables.db

On first run, values are seeded from environment variables (falling
back to hardcoded defaults).  After that, env vars are ignored —
the database wins.
"""

import os
import sqlite3
import threading
from typing import Any

DB_PATH = os.getenv('SC_CONFIG_DB_PATH', '/config/tunables.db')

_lock = threading.Lock()

# Registry: key → (env_var_name, hardcoded_default, type_hint)
TUNABLES = {
    # ── Balancer: Reactive ──────────────────────────────────────────────────
    'DRY_RUN':                      ('SC_DRY_RUN',                      True,   'bool'),
    'VERBOSITY':                    ('SC_VERBOSITY',                     3,      'int'),
    'AVG_WINDOW_MINUTES':           ('SC_AVG_WINDOW_MINUTES',            5,      'int'),
    'SAMPLE_INTERVAL_SECONDS':      ('SC_SAMPLE_INTERVAL_SECONDS',       30,     'int'),
    'RAM_LIMIT_PERCENT':            ('SC_RAM_LIMIT_PERCENT',             85.0,   'float'),
    'RAM_UPPER_THRESHOLD_PERCENT':  ('SC_RAM_UPPER_THRESHOLD_PERCENT',   80.0,   'float'),
    'CPU_UPPER_THRESHOLD_PERCENT':  ('SC_CPU_UPPER_THRESHOLD_PERCENT',   50.0,   'float'),
    'MAX_VCPU_RATIO':               ('SC_MAX_VCPU_RATIO',                2.0,    'float'),
    'VM_MOVE_COOLDOWN_MINUTES':     ('SC_VM_MOVE_COOLDOWN_MINUTES',      30,     'int'),
    'MIGRATION_COOLDOWN_MINUTES':   ('SC_MIGRATION_COOLDOWN_MINUTES',    5,      'int'),
    'EXCLUDE_NODE_IPS':             ('SC_EXCLUDE_NODE_IPS',              '',     'list'),
    # ── Balancer: Predictive ────────────────────────────────────────────────
    'PREDICTIVE_BALANCING_ENABLED': ('SC_PREDICTIVE_BALANCING_ENABLED',  False,  'bool'),
    'PREDICTIVE_INTERVAL_SECONDS':  ('SC_PREDICTIVE_INTERVAL_SECONDS',   43200,  'int'),
    'PREDICTIVE_LEAD_TIME_HOURS':   ('SC_PREDICTIVE_LEAD_TIME_HOURS',    1,      'int'),
    'PREDICTIVE_THRESHOLD':         ('SC_PREDICTIVE_THRESHOLD',          80.0,   'float'),
    'PREDICTIVE_MIN_HISTORY_HOURS': ('SC_PREDICTIVE_MIN_HISTORY_HOURS',  336,    'int'),
    'PREDICTIVE_LOOKBACK_DAYS':     ('SC_PREDICTIVE_LOOKBACK_DAYS',      90,     'int'),
    'PREDICTIVE_MAX_WORKERS':       ('SC_PREDICTIVE_MAX_WORKERS',        8,      'int'),
    'PREDICTIVE_RESERVATION_MINUTES': ('SC_PREDICTIVE_RESERVATION_MINUTES', 5,   'int'),
    # ── Collector ───────────────────────────────────────────────────────────
    'POLL_INTERVAL':                ('SC_POLL_INTERVAL',                 30,     'int'),
    'SLOW_POLL_INTERVAL':           ('SC_SLOW_POLL_INTERVAL',            300,    'int'),
    'INFLUX_WRITE_RETRIES':         ('INFLUX_WRITE_RETRIES',             3,      'int'),
    'INFLUX_RETRY_DELAY':           ('INFLUX_RETRY_DELAY',               5,      'int'),
    # ── Dashboard ───────────────────────────────────────────────────────────
    'STALE_SECONDS':                ('SC_DASHBOARD_STALE_SECONDS',       120,    'int'),
}


def _cast(value: str, type_hint: str) -> Any:
    if type_hint == 'bool':
        return str(value).lower() in ('true', '1', 'yes', 'y')
    if type_hint == 'int':
        return int(float(value))  # float() first handles "30.0" edge case
    if type_hint == 'float':
        return float(value)
    if type_hint == 'list':
        return [ip.strip() for ip in str(value).split(',') if ip.strip()]
    return str(value)


def _serialize(value: Any, type_hint: str) -> str:
    if type_hint == 'list':
        return ','.join(str(v) for v in value) if isinstance(value, list) else str(value)
    if type_hint == 'bool':
        return 'true' if value else 'false'
    return str(value)


def _connect() -> sqlite3.Connection:
    dirpath = os.path.dirname(DB_PATH)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS tunables (
            key        TEXT PRIMARY KEY,
            value      TEXT NOT NULL,
            type       TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    ''')
    conn.commit()
    return conn


def seed_from_env() -> None:
    """
    Called once at service startup.
    - Missing keys are inserted from env var (or hardcoded default).
    - Existing keys are updated from env var only if their stored value still
      matches the hardcoded default (i.e. never touched by the user via the UI).
      This lets env vars fill in values that were seeded empty on a previous run
      before the env var was available, without clobbering user changes.
    """
    with _lock:
        conn = _connect()
        try:
            for key, (env_var, default, type_hint) in TUNABLES.items():
                raw_env = os.getenv(env_var) or None  # treat empty string same as unset
                row = conn.execute(
                    'SELECT value FROM tunables WHERE key = ?', (key,)
                ).fetchone()
                if not row:
                    value = _cast(raw_env, type_hint) if raw_env is not None else default
                    conn.execute(
                        'INSERT INTO tunables (key, value, type) VALUES (?, ?, ?)',
                        (key, _serialize(value, type_hint), type_hint)
                    )
                elif raw_env is not None and row[0] == _serialize(default, type_hint):
                    # Still at hardcoded default — env var now available, apply it
                    conn.execute(
                        'UPDATE tunables SET value = ?, updated_at = datetime(\'now\') WHERE key = ?',
                        (_serialize(_cast(raw_env, type_hint), type_hint), key)
                    )
            conn.commit()
        finally:
            conn.close()


def get(key: str) -> Any:
    """Return a single tunable cast to its Python type."""
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                'SELECT value, type FROM tunables WHERE key = ?', (key,)
            ).fetchone()
        finally:
            conn.close()
    if row is None:
        entry = TUNABLES.get(key)
        return entry[1] if entry else None
    return _cast(row[0], row[1])


def get_all() -> dict:
    """Return all tunables as {key: typed_value}."""
    with _lock:
        conn = _connect()
        try:
            rows = conn.execute(
                'SELECT key, value, type FROM tunables'
            ).fetchall()
        finally:
            conn.close()
    return {r[0]: _cast(r[1], r[2]) for r in rows}


def save(key: str, value: Any) -> None:
    """Upsert a single tunable."""
    if key not in TUNABLES:
        raise ValueError(f'Unknown tunable: {key}')
    _, _, type_hint = TUNABLES[key]
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                '''INSERT INTO tunables (key, value, type, updated_at)
                   VALUES (?, ?, ?, datetime('now'))
                   ON CONFLICT(key) DO UPDATE SET
                       value      = excluded.value,
                       updated_at = excluded.updated_at''',
                (key, _serialize(value, type_hint), type_hint)
            )
            conn.commit()
        finally:
            conn.close()


def save_many(updates: dict) -> None:
    """Upsert multiple tunables in one transaction. Unknown keys are silently skipped."""
    with _lock:
        conn = _connect()
        try:
            for key, value in updates.items():
                if key not in TUNABLES:
                    continue
                _, _, type_hint = TUNABLES[key]
                conn.execute(
                    '''INSERT INTO tunables (key, value, type, updated_at)
                       VALUES (?, ?, ?, datetime('now'))
                       ON CONFLICT(key) DO UPDATE SET
                           value      = excluded.value,
                           updated_at = excluded.updated_at''',
                    (key, _serialize(value, type_hint), type_hint)
                )
            conn.commit()
        finally:
            conn.close()


def get_with_meta() -> list:
    """Return all tunables with type and timestamp — for the settings API."""
    with _lock:
        conn = _connect()
        try:
            rows = conn.execute(
                'SELECT key, value, type, updated_at FROM tunables ORDER BY key'
            ).fetchall()
        finally:
            conn.close()
    return [
        {'key': r[0], 'value': _cast(r[1], r[2]), 'type': r[2], 'updated_at': r[3]}
        for r in rows
    ]
