from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import base64
import hashlib
import hmac
import json
import os
import secrets
import sqlite3

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel, EmailStr

app = FastAPI(title="Signal Agent API", version="7.0.0")


APP_ENV = os.getenv("APP_ENV", "development").strip().lower()


def _read_secret(name: str, dev_fallback: str) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    if APP_ENV == "production":
        raise RuntimeError(f"{name} must be set in production")
    return dev_fallback


def _read_cors_origins() -> List[str]:
    raw = os.getenv("CORS_ALLOW_ORIGINS", "").strip()
    if raw:
        return [item.strip() for item in raw.split(",") if item.strip()]
    if APP_ENV == "production":
        raise RuntimeError("CORS_ALLOW_ORIGINS must be set in production")
    return ["*"]


CORS_ALLOW_ORIGINS = _read_cors_origins()

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials="*" not in CORS_ALLOW_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

SECRET_KEY = _read_secret("SECRET_KEY", "dev-secret-change-me")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
TV_API_KEY = _read_secret("TV_API_KEY", "dev-tv-key-change-me")
HEARTBEAT_TIMEOUT_SEC = int(os.getenv("HEARTBEAT_TIMEOUT_SEC", "90"))
ACCOUNT_SNAPSHOT_TIMEOUT_SEC = int(os.getenv("ACCOUNT_SNAPSHOT_TIMEOUT_SEC", "90"))
DB_PATH = os.getenv("DB_PATH", "signal_agent.db")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")


SEED_USERS: Dict[str, Dict[str, Any]] = {
    "test@test.com": {
        "password": "123456",
        "role": "customer",
        "customer_id": 1,
        "display_name": "Test Customer",
        "access_status": "active",
        "trading_status": "enabled",
        "subscription_status": "active",
    },
    "admin@claus.digital": {
        "password": "123456",
        "role": "master",
        "customer_id": None,
        "display_name": "Master Admin",
        "access_status": "active",
        "trading_status": "enabled",
        "subscription_status": "active",
    },
}

SEED_CUSTOMERS: Dict[int, Dict[str, Any]] = {
    1: {
        "id": 1,
        "display_name": "Test Customer",
        "access_start_at": None,
        "access_end_at": None,
        "access_status": "active",
        "trading_status": "enabled",
        "subscription_status": "active",
        "grace_until": None,
    },
}

SEED_EXPERT_ADVISORS: List[Dict[str, Any]] = [
    {
        "id": 1,
        "ea_name": "Gold Core EA",
        "ea_code": "gold_core_ea",
        "version": "1.0.0",
        "default_symbol": "XAUUSD",
        "default_magic": "61001",
        "download_url": "",
        "file_name": "gold_core_ea.ex5",
        "is_active": True,
    },
    {
        "id": 2,
        "ea_name": "BTC Core EA",
        "ea_code": "btc_core_ea",
        "version": "1.0.0",
        "default_symbol": "BTCUSD",
        "default_magic": "61002",
        "download_url": "",
        "file_name": "btc_core_ea.ex5",
        "is_active": True,
    },
]

SEED_CUSTOMER_ACCOUNTS: Dict[str, List[Dict[str, Any]]] = {
    "test@test.com": [
        {
            "id": 1,
            "account_number": "10001",
            "broker": "IC Markets",
            "broker_name": "IC Markets",
            "account_label": "IC Markets • 10001",
            "is_active": True,
        },
        {
            "id": 2,
            "account_number": "10002",
            "broker": "FTMO",
            "broker_name": "FTMO",
            "account_label": "FTMO • 10002",
            "is_active": True,
        },
    ],
    "admin@claus.digital": [
        {
            "id": 10,
            "account_number": "90001",
            "broker": "Master View",
            "broker_name": "Master View",
            "account_label": "Master View • 90001",
            "is_active": True,
        },
    ],
}

SEED_ACCOUNT_STRATEGIES: Dict[int, List[Dict[str, Any]]] = {
    1: [
        {
            "id": 1,
            "account_id": 1,
            "symbol": "XAUUSD",
            "name": "Gold Core",
            "strategy_name": "Gold Core",
            "strategy_code": "xau_core",
            "magic": "61001",
            "risk_tier": "balanced",
            "is_enabled": True,
            "ea_id": 1,
        },
        {
            "id": 2,
            "account_id": 1,
            "symbol": "BTCUSD",
            "name": "BTC Core",
            "strategy_name": "BTC Core",
            "strategy_code": "btc_core",
            "magic": "61002",
            "risk_tier": "balanced",
            "is_enabled": True,
            "ea_id": 2,
        },
    ],
    2: [
        {
            "id": 3,
            "account_id": 2,
            "symbol": "XAUUSD",
            "name": "Gold Core",
            "strategy_name": "Gold Core",
            "strategy_code": "xau_core",
            "magic": "61001",
            "risk_tier": "balanced",
            "is_enabled": True,
            "ea_id": 1,
        },
        {
            "id": 4,
            "account_id": 2,
            "symbol": "BTCUSD",
            "name": "BTC Core",
            "strategy_name": "BTC Core",
            "strategy_code": "btc_core",
            "magic": "61002",
            "risk_tier": "balanced",
            "is_enabled": True,
            "ea_id": 2,
        },
    ],
    10: [
        {
            "id": 5,
            "account_id": 10,
            "symbol": "XAUUSD",
            "name": "Gold Master",
            "strategy_name": "Gold Master",
            "strategy_code": "xau_core",
            "magic": "777",
            "risk_tier": "balanced",
            "is_enabled": True,
            "ea_id": 1,
        },
        {
            "id": 6,
            "account_id": 10,
            "symbol": "BTCUSD",
            "name": "BTC Master",
            "strategy_name": "BTC Master",
            "strategy_code": "btc_core",
            "magic": "62001",
            "risk_tier": "balanced",
            "is_enabled": True,
            "ea_id": 2,
        },
    ],
}

SEED_CUSTOMER_SETUP: Dict[str, Dict[int, Dict[str, Dict[str, Any]]]] = {
    "test@test.com": {
        1: {
            "XAUUSD": {"enabled": True, "risk_tier": "balanced"},
            "BTCUSD": {"enabled": True, "risk_tier": "balanced"},
        },
        2: {
            "XAUUSD": {"enabled": True, "risk_tier": "balanced"},
            "BTCUSD": {"enabled": True, "risk_tier": "balanced"},
        },
    },
    "admin@claus.digital": {
        10: {
            "XAUUSD": {"enabled": True, "risk_tier": "balanced"},
            "BTCUSD": {"enabled": True, "risk_tier": "balanced"},
        },
    },
}


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str


class StrategySetupIn(BaseModel):
    enabled: bool
    risk_tier: str


class CustomerAccountCreate(BaseModel):
    broker_name: str
    account_number: str
    account_label: str
    is_active: bool = True


class CustomerAccountUpdate(BaseModel):
    broker_name: str
    account_number: str
    account_label: str
    is_active: bool = True


class CustomerStrategyCreate(BaseModel):
    account_id: Optional[int] = None
    symbol: str
    strategy_code: str
    strategy_name: str
    magic: int
    risk_tier: str = "balanced"
    is_enabled: bool = True
    ea_id: Optional[int] = None


class CustomerStrategyUpdate(BaseModel):
    account_id: Optional[int] = None
    symbol: str
    strategy_code: str
    strategy_name: str
    magic: int
    risk_tier: str = "balanced"
    is_enabled: bool = True
    ea_id: Optional[int] = None


class MasterCustomerCreate(BaseModel):
    display_name: str
    access_start_at: Optional[str] = None
    access_end_at: Optional[str] = None
    access_status: str = "active"
    trading_status: str = "enabled"
    subscription_status: str = "active"
    grace_until: Optional[str] = None


class MasterCustomerUpdate(BaseModel):
    display_name: str
    access_start_at: Optional[str] = None
    access_end_at: Optional[str] = None
    access_status: str = "active"
    trading_status: str = "enabled"
    subscription_status: str = "active"
    grace_until: Optional[str] = None


class MasterUserCreate(BaseModel):
    email: EmailStr
    password: str
    display_name: str
    customer_id: int


class MasterCustomerAccountCreate(BaseModel):
    broker_name: str
    account_number: str
    account_label: str
    is_active: bool = True


class MasterCustomerAccountUpdate(BaseModel):
    broker_name: str
    account_number: str
    account_label: str
    is_active: bool = True


class MasterCustomerStrategyCreate(BaseModel):
    account_id: int
    symbol: str
    strategy_code: str
    strategy_name: str
    magic: int
    risk_tier: str = "balanced"
    is_enabled: bool = True
    ea_id: Optional[int] = None


class MasterCustomerStrategyUpdate(BaseModel):
    account_id: int
    symbol: str
    strategy_code: str
    strategy_name: str
    magic: int
    risk_tier: str = "balanced"
    is_enabled: bool = True
    ea_id: Optional[int] = None


class ExpertAdvisorCreate(BaseModel):
    ea_name: str
    ea_code: str
    version: Optional[str] = None
    default_symbol: Optional[str] = None
    default_magic: Optional[int] = None
    download_url: Optional[str] = None
    file_name: Optional[str] = None
    is_active: bool = True


class ExpertAdvisorUpdate(BaseModel):
    ea_name: str
    ea_code: str
    version: Optional[str] = None
    default_symbol: Optional[str] = None
    default_magic: Optional[int] = None
    download_url: Optional[str] = None
    file_name: Optional[str] = None
    is_active: bool = True


class TVSignalIn(BaseModel):
    key: Optional[str] = None
    symbol: str
    side: Optional[str] = None
    action: Optional[str] = None
    score: Optional[float] = 1.0
    payload: Optional[Dict[str, Any]] = None


class AckIn(BaseModel):
    key: Optional[str] = None
    symbol: str
    updated_utc: str
    account: str
    magic: Optional[str] = None
    ticket: Optional[str] = None


class HeartbeatPing(BaseModel):
    key: Optional[str] = None
    symbol: str
    account: str
    magic: Optional[str] = None
    ea_name: Optional[str] = None
    version: Optional[str] = None
    status: Optional[str] = "alive"
    comment: Optional[str] = None
    owner_name: Optional[str] = None


class AccountSnapshotIn(BaseModel):
    key: Optional[str] = None
    account: str
    broker_name: Optional[str] = None
    balance: float = 0.0
    equity: float = 0.0
    margin: float = 0.0
    free_margin: float = 0.0
    margin_level: float = 0.0
    currency: Optional[str] = "USD"


class DealIn(BaseModel):
    key: Optional[str] = None
    account: str
    magic: str
    symbol: str
    side: Optional[str] = None
    ticket: Optional[str] = None
    volume: Optional[float] = None
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    pnl: Optional[float] = 0.0
    commission: Optional[float] = 0.0
    swap: Optional[float] = 0.0
    r_multiple: Optional[float] = 0.0
    strategy_code: Optional[str] = None
    deal_time_utc: Optional[str] = None


class RiskIn(BaseModel):
    key: Optional[str] = None
    account: str
    magic: str
    symbol: str
    risk_level: str = "GREEN"
    allow_new_entries: bool = True
    daily_pnl: float = 0.0
    daily_r: float = 0.0
    daily_trades: int = 0
    reasons: Optional[List[str]] = None
    limits: Optional[Dict[str, Any]] = None


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    return dict(row) if row is not None else None


def rows_to_dicts(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_utc_iso() -> str:
    return now_utc().isoformat()


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


PASSWORD_HASH_SCHEME = "pbkdf2_sha256"
PASSWORD_HASH_ITERATIONS = int(os.getenv("PASSWORD_HASH_ITERATIONS", "390000"))


def hash_password(password: str, *, salt: Optional[str] = None) -> str:
    if not password:
        raise ValueError("password must not be empty")

    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt.encode("utf-8"),
        PASSWORD_HASH_ITERATIONS,
    )
    encoded = base64.b64encode(digest).decode("utf-8")
    return f"{PASSWORD_HASH_SCHEME}${PASSWORD_HASH_ITERATIONS}${salt}${encoded}"


def verify_password(password: str, stored_value: str) -> bool:
    if not stored_value:
        return False

    prefix = f"{PASSWORD_HASH_SCHEME}$"
    if stored_value.startswith(prefix):
        try:
            _, iterations_text, salt, expected = stored_value.split("$", 3)
            digest = hashlib.pbkdf2_hmac(
                "sha256",
                password.encode("utf-8"),
                salt.encode("utf-8"),
                int(iterations_text),
            )
            candidate = base64.b64encode(digest).decode("utf-8")
            return hmac.compare_digest(candidate, expected)
        except Exception:
            return False

    return hmac.compare_digest(password, stored_value)


def maybe_upgrade_password_hash(email: str, raw_password: str) -> None:
    user = db_get_user(email)
    if not user:
        return

    stored_value = str(user.get("password") or "")
    if stored_value.startswith(f"{PASSWORD_HASH_SCHEME}$"):
        return
    if not hmac.compare_digest(stored_value, raw_password):
        return

    with get_db() as conn:
        conn.execute(
            "UPDATE users SET password = ? WHERE email = ?",
            (hash_password(raw_password), email),
        )


def require_machine_api_key(
    header_key: Optional[str] = None,
    body_key: Optional[str] = None,
) -> None:
    provided = (header_key or body_key or "").strip()
    if not provided or not hmac.compare_digest(provided, TV_API_KEY):
        raise HTTPException(status_code=401, detail="Invalid API key")


def build_public_ea_download_url(ea_id: int) -> Optional[str]:
    if not PUBLIC_BASE_URL:
        return None
    return f"{PUBLIC_BASE_URL}/public/eas/{ea_id}/download"


def init_db() -> None:
    with get_db() as conn:
        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY,
                display_name TEXT NOT NULL,
                access_start_at TEXT,
                access_end_at TEXT,
                access_status TEXT NOT NULL DEFAULT 'active',
                trading_status TEXT NOT NULL DEFAULT 'enabled',
                subscription_status TEXT NOT NULL DEFAULT 'active',
                grace_until TEXT
            );

            CREATE TABLE IF NOT EXISTS users (
                email TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                role TEXT NOT NULL,
                customer_id INTEGER,
                display_name TEXT NOT NULL,
                access_status TEXT NOT NULL DEFAULT 'active',
                trading_status TEXT NOT NULL DEFAULT 'enabled',
                subscription_status TEXT NOT NULL DEFAULT 'active',
                FOREIGN KEY(customer_id) REFERENCES customers(id)
            );

            CREATE TABLE IF NOT EXISTS expert_advisors (
                id INTEGER PRIMARY KEY,
                ea_name TEXT NOT NULL,
                ea_code TEXT NOT NULL UNIQUE,
                version TEXT,
                default_symbol TEXT,
                default_magic TEXT,
                download_url TEXT,
                file_name TEXT,
                is_active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS customer_accounts (
                id INTEGER PRIMARY KEY,
                user_email TEXT NOT NULL,
                account_number TEXT NOT NULL,
                broker TEXT,
                broker_name TEXT,
                account_label TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                UNIQUE(user_email, account_number),
                FOREIGN KEY(user_email) REFERENCES users(email) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS customer_strategies (
                id INTEGER PRIMARY KEY,
                account_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT,
                strategy_name TEXT NOT NULL,
                strategy_code TEXT NOT NULL,
                magic TEXT NOT NULL,
                risk_tier TEXT NOT NULL DEFAULT 'balanced',
                is_enabled INTEGER NOT NULL DEFAULT 1,
                ea_id INTEGER,
                UNIQUE(account_id, symbol, magic),
                FOREIGN KEY(account_id) REFERENCES customer_accounts(id) ON DELETE CASCADE,
                FOREIGN KEY(ea_id) REFERENCES expert_advisors(id)
            );

            CREATE TABLE IF NOT EXISTS customer_strategy_setup (
                user_email TEXT NOT NULL,
                account_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                risk_tier TEXT NOT NULL DEFAULT 'balanced',
                PRIMARY KEY(user_email, account_id, symbol),
                FOREIGN KEY(user_email) REFERENCES users(email) ON DELETE CASCADE,
                FOREIGN KEY(account_id) REFERENCES customer_accounts(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_utc TEXT NOT NULL,
                actor_email TEXT NOT NULL,
                action_type TEXT NOT NULL,
                message TEXT NOT NULL,
                target_customer_id INTEGER,
                target_user_email TEXT,
                target_account_id INTEGER,
                target_strategy_id INTEGER
            );

            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                score REAL,
                payload_json TEXT,
                created_utc TEXT NOT NULL,
                updated_utc TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
            );

            CREATE TABLE IF NOT EXISTS signal_acks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                account TEXT NOT NULL,
                magic TEXT NOT NULL,
                ack_utc TEXT NOT NULL,
                ticket TEXT,
                UNIQUE(signal_id, account, magic)
            );

            CREATE TABLE IF NOT EXISTS heartbeats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account TEXT NOT NULL,
                magic TEXT,
                symbol TEXT NOT NULL,
                ea_name TEXT,
                version TEXT,
                last_seen_utc TEXT NOT NULL,
                status TEXT,
                comment TEXT,
                owner_name TEXT
            );

            CREATE TABLE IF NOT EXISTS account_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account TEXT NOT NULL,
                broker_name TEXT,
                balance REAL NOT NULL DEFAULT 0,
                equity REAL NOT NULL DEFAULT 0,
                margin REAL NOT NULL DEFAULT 0,
                free_margin REAL NOT NULL DEFAULT 0,
                margin_level REAL NOT NULL DEFAULT 0,
                currency TEXT,
                created_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account TEXT NOT NULL,
                magic TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT,
                ticket TEXT,
                volume REAL,
                entry_price REAL,
                exit_price REAL,
                sl REAL,
                tp REAL,
                pnl REAL,
                commission REAL,
                swap REAL,
                r_multiple REAL,
                strategy_code TEXT,
                deal_time_utc TEXT NOT NULL,
                created_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS risk_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account TEXT NOT NULL,
                magic TEXT NOT NULL,
                symbol TEXT NOT NULL,
                risk_level TEXT NOT NULL,
                allow_new_entries INTEGER NOT NULL DEFAULT 1,
                daily_pnl REAL NOT NULL DEFAULT 0,
                daily_r REAL NOT NULL DEFAULT 0,
                daily_trades INTEGER NOT NULL DEFAULT 0,
                reasons_json TEXT,
                limits_json TEXT,
                created_utc TEXT NOT NULL
            );
            '''
        )


def run_db_migrations() -> None:
    with get_db() as conn:
        ea_cols = [r["name"] for r in conn.execute("PRAGMA table_info(expert_advisors)").fetchall()]
        if "download_url" not in ea_cols:
            conn.execute("ALTER TABLE expert_advisors ADD COLUMN download_url TEXT")
        if "file_name" not in ea_cols:
            conn.execute("ALTER TABLE expert_advisors ADD COLUMN file_name TEXT")

        strategy_cols = [r["name"] for r in conn.execute("PRAGMA table_info(customer_strategies)").fetchall()]
        if "ea_id" not in strategy_cols:
            conn.execute("ALTER TABLE customer_strategies ADD COLUMN ea_id INTEGER")

        conn.executescript(
            '''
            CREATE TABLE IF NOT EXISTS account_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                account TEXT NOT NULL,
                broker_name TEXT,
                balance REAL NOT NULL DEFAULT 0,
                equity REAL NOT NULL DEFAULT 0,
                margin REAL NOT NULL DEFAULT 0,
                free_margin REAL NOT NULL DEFAULT 0,
                margin_level REAL NOT NULL DEFAULT 0,
                currency TEXT,
                created_utc TEXT NOT NULL
            );
            '''
        )


def seed_db_if_empty() -> None:
    with get_db() as conn:
        existing = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        if existing > 0:
            return

        for customer in SEED_CUSTOMERS.values():
            conn.execute(
                '''
                INSERT INTO customers (
                    id, display_name, access_start_at, access_end_at,
                    access_status, trading_status, subscription_status, grace_until
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    customer["id"],
                    customer["display_name"],
                    customer.get("access_start_at"),
                    customer.get("access_end_at"),
                    customer.get("access_status", "active"),
                    customer.get("trading_status", "enabled"),
                    customer.get("subscription_status", "active"),
                    customer.get("grace_until"),
                ),
            )

        for email, user in SEED_USERS.items():
            conn.execute(
                '''
                INSERT INTO users (
                    email, password, role, customer_id, display_name,
                    access_status, trading_status, subscription_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    email,
                    hash_password(user["password"]),
                    user["role"],
                    user.get("customer_id"),
                    user.get("display_name", email),
                    user.get("access_status", "active"),
                    user.get("trading_status", "enabled"),
                    user.get("subscription_status", "active"),
                ),
            )

        for ea in SEED_EXPERT_ADVISORS:
            conn.execute(
                '''
                INSERT INTO expert_advisors (
                    id, ea_name, ea_code, version, default_symbol, default_magic,
                    download_url, file_name, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    ea["id"],
                    ea["ea_name"],
                    ea["ea_code"],
                    ea.get("version"),
                    ea.get("default_symbol"),
                    ea.get("default_magic"),
                    ea.get("download_url"),
                    ea.get("file_name"),
                    1 if ea.get("is_active", True) else 0,
                ),
            )

        for email, accounts in SEED_CUSTOMER_ACCOUNTS.items():
            for account in accounts:
                conn.execute(
                    '''
                    INSERT INTO customer_accounts (
                        id, user_email, account_number, broker, broker_name,
                        account_label, is_active
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        account["id"],
                        email,
                        account["account_number"],
                        account.get("broker"),
                        account.get("broker_name"),
                        account.get("account_label"),
                        1 if account.get("is_active", True) else 0,
                    ),
                )

        for account_id, strategies in SEED_ACCOUNT_STRATEGIES.items():
            for strategy in strategies:
                conn.execute(
                    '''
                    INSERT INTO customer_strategies (
                        id, account_id, symbol, name, strategy_name,
                        strategy_code, magic, risk_tier, is_enabled, ea_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (
                        strategy["id"],
                        account_id,
                        strategy["symbol"].upper(),
                        strategy.get("name"),
                        strategy.get("strategy_name") or strategy.get("name"),
                        strategy["strategy_code"],
                        str(strategy["magic"]),
                        strategy.get("risk_tier", "balanced"),
                        1 if strategy.get("is_enabled", True) else 0,
                        strategy.get("ea_id"),
                    ),
                )

        for email, account_map in SEED_CUSTOMER_SETUP.items():
            for account_id, symbol_map in account_map.items():
                for symbol, setup in symbol_map.items():
                    conn.execute(
                        '''
                        INSERT INTO customer_strategy_setup (
                            user_email, account_id, symbol, enabled, risk_tier
                        ) VALUES (?, ?, ?, ?, ?)
                        ''',
                        (
                            email,
                            account_id,
                            symbol.upper(),
                            1 if setup.get("enabled", True) else 0,
                            setup.get("risk_tier", "balanced"),
                        ),
                    )


def force_seed_defaults() -> Dict[str, Any]:
    init_db()
    run_db_migrations()
    with get_db() as conn:
        conn.execute("DELETE FROM customer_strategy_setup")
        conn.execute("DELETE FROM customer_strategies")
        conn.execute("DELETE FROM customer_accounts")
        conn.execute("DELETE FROM expert_advisors")
        conn.execute("DELETE FROM users")
        conn.execute("DELETE FROM customers")
    seed_db_if_empty()
    return {
        "ok": True,
        "message": "Defaults seeded",
        "db_path": DB_PATH,
        "available_logins": [
            {"email": "admin@claus.digital", "password": "123456", "role": "master"},
            {"email": "test@test.com", "password": "123456", "role": "customer"},
        ],
    }


@app.on_event("startup")
def startup_event() -> None:
    init_db()
    run_db_migrations()
    seed_db_if_empty()


def create_token(email: str, role: str) -> str:
    expire = now_utc() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    payload = {"sub": email, "role": role, "exp": expire}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def db_get_user(email: str) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    return row_to_dict(row)


def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict[str, Any]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email = payload.get("sub")
        if not email:
            raise HTTPException(status_code=401, detail="Invalid token")
        user = db_get_user(email)
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        return {
            "email": user["email"],
            "role": user["role"],
            "customer_id": user.get("customer_id"),
            "display_name": user.get("display_name") or user["email"],
            "access_status": user.get("access_status", "active"),
            "trading_status": user.get("trading_status", "enabled"),
            "subscription_status": user.get("subscription_status", "active"),
        }
    except JWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc


def require_customer(current_user: Dict[str, Any]) -> None:
    if current_user["role"] != "customer":
        raise HTTPException(status_code=403, detail="Not allowed")


def require_master(current_user: Dict[str, Any]) -> None:
    if current_user["role"] != "master":
        raise HTTPException(status_code=403, detail="Not allowed")


def normalize_risk_tier(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in ("conservative", "balanced", "dynamic", "aggressive"):
        raise HTTPException(status_code=422, detail="Invalid risk_tier")
    return normalized


def normalize_access_status(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in ("active", "disabled", "expired", "paused"):
        raise HTTPException(status_code=422, detail="Invalid access_status")
    return normalized


def normalize_trading_status(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in ("enabled", "disabled", "paused"):
        raise HTTPException(status_code=422, detail="Invalid trading_status")
    return normalized


def normalize_subscription_status(value: str) -> str:
    normalized = value.strip().lower()
    if normalized not in ("active", "trial", "expired", "cancelled", "grace"):
        raise HTTPException(status_code=422, detail="Invalid subscription_status")
    return normalized


def normalize_side(value: Optional[str]) -> str:
    text = (value or "").strip().upper()
    if text == "LONG":
        return "BUY"
    if text == "SHORT":
        return "SELL"
    return text


def next_id(table_name: str) -> int:
    with get_db() as conn:
        row = conn.execute(f"SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM {table_name}").fetchone()
    return int(row["next_id"])


def next_customer_id() -> int:
    return next_id("customers")


def next_account_id() -> int:
    return next_id("customer_accounts")


def next_strategy_id() -> int:
    return next_id("customer_strategies")


def next_ea_id() -> int:
    return next_id("expert_advisors")


def write_audit_log(
    actor_email: str,
    action_type: str,
    message: str,
    target_customer_id: Optional[int] = None,
    target_user_email: Optional[str] = None,
    target_account_id: Optional[int] = None,
    target_strategy_id: Optional[int] = None,
) -> None:
    with get_db() as conn:
        conn.execute(
            '''
            INSERT INTO audit_logs (
                created_utc, actor_email, action_type, message,
                target_customer_id, target_user_email, target_account_id, target_strategy_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                now_utc_iso(),
                actor_email,
                action_type,
                message,
                target_customer_id,
                target_user_email,
                target_account_id,
                target_strategy_id,
            ),
        )


def find_customer(customer_id: int) -> Dict[str, Any]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM customers WHERE id = ?", (customer_id,)).fetchone()
    customer = row_to_dict(row)
    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")
    return customer


def get_customer_user_emails(customer_id: int) -> List[str]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT email FROM users WHERE customer_id = ? AND role = 'customer' ORDER BY email",
            (customer_id,),
        ).fetchall()
    return [row["email"] for row in rows]


def get_primary_customer_email(customer_id: int) -> Optional[str]:
    emails = get_customer_user_emails(customer_id)
    return emails[0] if emails else None


def require_customer_owner_email(customer_id: int) -> str:
    email = get_primary_customer_email(customer_id)
    if not email:
        raise HTTPException(status_code=400, detail="Customer has no customer-user login yet")
    return email


def sync_customer_status_to_users(customer_id: int) -> None:
    customer = find_customer(customer_id)
    with get_db() as conn:
        conn.execute(
            '''
            UPDATE users
            SET access_status = ?, trading_status = ?, subscription_status = ?
            WHERE customer_id = ?
            ''',
            (
                customer.get("access_status", "active"),
                customer.get("trading_status", "enabled"),
                customer.get("subscription_status", "active"),
                customer_id,
            ),
        )


def format_customer_payload(customer: Dict[str, Any]) -> Dict[str, Any]:
    customer_id = int(customer["id"])
    user_emails = get_customer_user_emails(customer_id)
    return {
        "id": customer_id,
        "display_name": customer["display_name"],
        "access_start_at": customer.get("access_start_at"),
        "access_end_at": customer.get("access_end_at"),
        "access_status": customer.get("access_status", "active"),
        "trading_status": customer.get("trading_status", "enabled"),
        "subscription_status": customer.get("subscription_status", "active"),
        "grace_until": customer.get("grace_until"),
        "user_count": len(user_emails),
        "user_emails": user_emails,
    }


def find_ea(ea_id: int) -> Dict[str, Any]:
    with get_db() as conn:
        row = conn.execute(
            '''
            SELECT id, ea_name, ea_code, version, default_symbol, default_magic,
                   download_url, file_name, is_active
            FROM expert_advisors
            WHERE id = ?
            ''',
            (ea_id,),
        ).fetchone()
    ea = row_to_dict(row)
    if not ea:
        raise HTTPException(status_code=404, detail="Expert advisor not found")
    ea["is_active"] = bool(ea.get("is_active", 1))
    return ea


def list_eas() -> List[Dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            '''
            SELECT id, ea_name, ea_code, version, default_symbol, default_magic,
                   download_url, file_name, is_active
            FROM expert_advisors
            ORDER BY ea_name, id
            '''
        ).fetchall()
    result = rows_to_dicts(rows)
    for row in result:
        row["is_active"] = bool(row.get("is_active", 1))
    return result


def format_ea_payload(ea: Dict[str, Any]) -> Dict[str, Any]:
    ea_id = int(ea["id"])
    return {
        "id": ea_id,
        "ea_name": ea["ea_name"],
        "ea_code": ea["ea_code"],
        "version": ea.get("version"),
        "default_symbol": ea.get("default_symbol"),
        "default_magic": str(ea.get("default_magic")) if ea.get("default_magic") is not None else None,
        "download_url": ea.get("download_url"),
        "file_name": ea.get("file_name"),
        "public_download_path": f"/public/eas/{ea_id}/download",
        "public_download_url": build_public_ea_download_url(ea_id),
        "is_active": bool(ea.get("is_active", True)),
    }


def get_ea_payload_or_none(ea_id: Optional[int]) -> Optional[Dict[str, Any]]:
    if ea_id is None:
        return None
    try:
        return format_ea_payload(find_ea(int(ea_id)))
    except HTTPException:
        return None


def get_user_accounts(email: str) -> List[Dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            '''
            SELECT id, user_email, account_number, broker, broker_name, account_label, is_active
            FROM customer_accounts
            WHERE user_email = ?
            ORDER BY id
            ''',
            (email,),
        ).fetchall()
    result = rows_to_dicts(rows)
    for row in result:
        row["is_active"] = bool(row.get("is_active", 1))
    return result


def format_account_payload(account: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(account["id"]),
        "account_number": account["account_number"],
        "broker": account.get("broker_name") or account.get("broker"),
        "broker_name": account.get("broker_name") or account.get("broker"),
        "account_label": account.get("account_label") or f'{account.get("broker_name") or account.get("broker")} • {account["account_number"]}',
        "is_active": bool(account.get("is_active", True)),
    }


def find_account_for_user(email: str, account_id: int) -> Dict[str, Any]:
    with get_db() as conn:
        row = conn.execute(
            '''
            SELECT id, user_email, account_number, broker, broker_name, account_label, is_active
            FROM customer_accounts
            WHERE user_email = ? AND id = ?
            ''',
            (email, account_id),
        ).fetchone()
    account = row_to_dict(row)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    account["is_active"] = bool(account.get("is_active", 1))
    return account


def ensure_account_access(email: str, account_id: int) -> None:
    _ = find_account_for_user(email, account_id)


def get_account_strategies(account_id: int) -> List[Dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            '''
            SELECT id, account_id, symbol, name, strategy_name, strategy_code,
                   magic, risk_tier, is_enabled, ea_id
            FROM customer_strategies
            WHERE account_id = ?
            ORDER BY id
            ''',
            (account_id,),
        ).fetchall()
    result = rows_to_dicts(rows)
    for row in result:
        row["is_enabled"] = bool(row.get("is_enabled", 1))
    return result


def find_strategy_for_user(email: str, strategy_id: int) -> Dict[str, Any]:
    with get_db() as conn:
        row = conn.execute(
            '''
            SELECT cs.id, cs.account_id, cs.symbol, cs.name, cs.strategy_name,
                   cs.strategy_code, cs.magic, cs.risk_tier, cs.is_enabled, cs.ea_id
            FROM customer_strategies cs
            JOIN customer_accounts ca ON ca.id = cs.account_id
            WHERE ca.user_email = ? AND cs.id = ?
            ''',
            (email, strategy_id),
        ).fetchone()
    strategy = row_to_dict(row)
    if not strategy:
        raise HTTPException(status_code=404, detail="Strategy not found")
    strategy["is_enabled"] = bool(strategy.get("is_enabled", 1))
    return strategy


def format_strategy_payload(strategy: Dict[str, Any]) -> Dict[str, Any]:
    ea_id = strategy.get("ea_id")
    return {
        "id": int(strategy["id"]),
        "account_id": int(strategy["account_id"]),
        "symbol": str(strategy["symbol"]).upper(),
        "strategy_code": strategy.get("strategy_code"),
        "strategy_name": strategy.get("strategy_name") or strategy.get("name") or str(strategy["symbol"]).upper(),
        "name": strategy.get("strategy_name") or strategy.get("name") or str(strategy["symbol"]).upper(),
        "magic": str(strategy.get("magic", "")),
        "risk_tier": strategy.get("risk_tier", "balanced"),
        "is_enabled": bool(strategy.get("is_enabled", True)),
        "ea_id": ea_id,
        "ea": get_ea_payload_or_none(ea_id),
    }


def get_strategy_setup(email: str, account_id: int, symbol: str) -> Dict[str, Any]:
    symbol_upper = symbol.upper()
    with get_db() as conn:
        row = conn.execute(
            '''
            SELECT enabled, risk_tier
            FROM customer_strategy_setup
            WHERE user_email = ? AND account_id = ? AND symbol = ?
            ''',
            (email, account_id, symbol_upper),
        ).fetchone()
        if row:
            return {"enabled": bool(row["enabled"]), "risk_tier": row["risk_tier"]}
        conn.execute(
            '''
            INSERT INTO customer_strategy_setup (user_email, account_id, symbol, enabled, risk_tier)
            VALUES (?, ?, ?, ?, ?)
            ''',
            (email, account_id, symbol_upper, 1, "balanced"),
        )
    return {"enabled": True, "risk_tier": "balanced"}


def set_strategy_setup(email: str, account_id: int, symbol: str, enabled: bool, risk_tier: str) -> None:
    symbol_upper = symbol.upper()
    with get_db() as conn:
        conn.execute(
            '''
            INSERT INTO customer_strategy_setup (user_email, account_id, symbol, enabled, risk_tier)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(user_email, account_id, symbol)
            DO UPDATE SET enabled = excluded.enabled, risk_tier = excluded.risk_tier
            ''',
            (email, account_id, symbol_upper, 1 if enabled else 0, risk_tier),
        )


def get_customer_accounts_with_setup(email: str) -> List[Dict[str, Any]]:
    accounts = get_user_accounts(email)
    result: List[Dict[str, Any]] = []
    for account in accounts:
        account_id = int(account["id"])
        base_strategies = get_account_strategies(account_id)
        symbols: List[Dict[str, Any]] = []
        for strategy in base_strategies:
            symbol = str(strategy["symbol"]).upper()
            setup = get_strategy_setup(email, account_id, symbol)
            enabled = bool(strategy.get("is_enabled", True)) and bool(setup["enabled"])
            strategy_name = strategy.get("strategy_name") or strategy.get("name") or symbol
            ea_id = strategy.get("ea_id")
            symbols.append(
                {
                    "id": int(strategy["id"]),
                    "account_id": account_id,
                    "symbol": symbol,
                    "displayName": strategy_name,
                    "name": strategy_name,
                    "strategy_name": strategy_name,
                    "magic": str(strategy.get("magic", "")),
                    "enabled": enabled,
                    "is_enabled": bool(strategy.get("is_enabled", True)),
                    "riskTier": setup["risk_tier"],
                    "risk_tier": setup["risk_tier"],
                    "strategyCode": strategy.get("strategy_code"),
                    "strategy_code": strategy.get("strategy_code"),
                    "sortOrder": 2 if symbol == "BTCUSD" else 1,
                    "sort_order": 2 if symbol == "BTCUSD" else 1,
                    "baseLot": 0.01,
                    "base_lot": 0.01,
                    "maxLot": 1.0,
                    "max_lot": 1.0,
                    "color": "#F7931A" if symbol == "BTCUSD" else "#D4AF37",
                    "ea_id": ea_id,
                    "ea": get_ea_payload_or_none(ea_id),
                }
            )
        result.append(
            {
                "id": account_id,
                "account_number": account["account_number"],
                "label": f'{account.get("broker_name") or account.get("broker")} • {account["account_number"]}',
                "broker": account.get("broker_name") or account.get("broker"),
                "broker_name": account.get("broker_name") or account.get("broker"),
                "account_label": account.get("account_label") or f'{account.get("broker_name") or account.get("broker")} • {account["account_number"]}',
                "enabled": bool(account.get("is_active", True)),
                "is_active": bool(account.get("is_active", True)),
                "symbols": symbols,
            }
        )
    return result


def get_accounts_for_customer(customer_id: int) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for email in get_customer_user_emails(customer_id):
        for account in get_user_accounts(email):
            items.append(format_account_payload(account))
    items.sort(key=lambda x: int(x["id"]))
    return items


def get_strategies_for_customer(customer_id: int) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    seen: set[int] = set()
    for email in get_customer_user_emails(customer_id):
        for account in get_user_accounts(email):
            for strategy in get_account_strategies(int(account["id"])):
                sid = int(strategy["id"])
                if sid in seen:
                    continue
                seen.add(sid)
                items.append(format_strategy_payload(strategy))
    items.sort(key=lambda x: (int(x["account_id"]), x["symbol"], x["magic"]))
    return items


def find_account_for_customer(customer_id: int, account_id: int) -> Tuple[str, Dict[str, Any]]:
    for email in get_customer_user_emails(customer_id):
        for account in get_user_accounts(email):
            if int(account["id"]) == account_id:
                return email, account
    raise HTTPException(status_code=404, detail="Account not found for customer")


def find_strategy_for_customer(customer_id: int, strategy_id: int) -> Tuple[str, Dict[str, Any]]:
    for email in get_customer_user_emails(customer_id):
        for account in get_user_accounts(email):
            for strategy in get_account_strategies(int(account["id"])):
                if int(strategy["id"]) == strategy_id:
                    return email, strategy
    raise HTTPException(status_code=404, detail="Strategy not found for customer")


def ensure_account_belongs_to_customer(customer_id: int, account_id: int) -> Tuple[str, Dict[str, Any]]:
    return find_account_for_customer(customer_id, account_id)


def risk_multiplier_for_tier(risk_tier: str) -> float:
    rt = risk_tier.strip().lower()
    if rt == "conservative":
        return 0.5
    if rt == "balanced":
        return 1.0
    if rt == "dynamic":
        return 1.25
    if rt == "aggressive":
        return 1.5
    return 1.0


def build_controls(enabled: bool, symbol: str, risk_tier: str) -> Dict[str, Any]:
    return {
        "paused": False,
        "allow_new_entries": enabled,
        "risk_multiplier": risk_multiplier_for_tier(risk_tier) if enabled else 0.0,
        "symbol": symbol.upper(),
        "source": "customer_setup",
    }


def get_latest_risk_snapshot(account: str, magic: str, symbol: str) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        row = conn.execute(
            '''
            SELECT *
            FROM risk_snapshots
            WHERE account = ? AND magic = ? AND symbol = ?
            ORDER BY id DESC
            LIMIT 1
            ''',
            (account, magic, symbol.upper()),
        ).fetchone()
    item = row_to_dict(row)
    if not item:
        return None
    return item


def build_risk_engine(enabled: bool, account: str, magic: str, symbol: str) -> Dict[str, Any]:
    snap = get_latest_risk_snapshot(account, magic, symbol)
    if snap:
        reasons = []
        limits = {}
        try:
            reasons = json.loads(snap.get("reasons_json") or "[]")
        except Exception:
            reasons = []
        try:
            limits = json.loads(snap.get("limits_json") or "{}")
        except Exception:
            limits = {}

        allow_new_entries = bool(snap.get("allow_new_entries", 1)) and enabled
        risk_level = str(snap.get("risk_level") or "GREEN").upper()
        if not enabled:
            allow_new_entries = False
            risk_level = "RED"
            if "STRATEGY_DISABLED" not in reasons:
                reasons.append("STRATEGY_DISABLED")

        return {
            "enabled": True,
            "allow_new_entries": allow_new_entries,
            "risk_level": risk_level,
            "daily_pnl": safe_float(snap.get("daily_pnl")),
            "daily_r": safe_float(snap.get("daily_r")),
            "daily_trades": safe_int(snap.get("daily_trades")),
            "limits": limits or {
                "daily_loss_cap_usd": 250.0,
                "daily_r_cap": -5.0,
                "daily_max_trades": 10,
            },
            "reasons": reasons or ["NORMAL"],
            "updated_utc": snap.get("created_utc"),
        }

    level = "GREEN" if enabled else "RED"
    return {
        "enabled": True,
        "allow_new_entries": enabled,
        "risk_level": level,
        "daily_pnl": 0.0,
        "daily_r": 0.0,
        "daily_trades": 0,
        "limits": {
            "daily_loss_cap_usd": 250.0,
            "daily_r_cap": -5.0,
            "daily_max_trades": 10,
        },
        "reasons": ["NORMAL" if enabled else "STRATEGY_DISABLED"],
        "updated_utc": None,
    }


def build_gate_combo_payload(symbol: str, enabled: bool, risk_tier: str, risk_engine: Dict[str, Any]) -> Dict[str, Any]:
    allow_new_entries = enabled and bool(risk_engine.get("allow_new_entries", True))
    if not enabled:
        gate_level = "RED"
    else:
        gate_level = str(risk_engine.get("risk_level") or "GREEN").upper()

    multiplier = risk_multiplier_for_tier(risk_tier) if allow_new_entries else 0.0

    return {
        "ok": True,
        "symbol": symbol.upper(),
        "gate_level": gate_level,
        "allow_new_entries": allow_new_entries,
        "risk_multiplier": multiplier,
        "paused": False,
        "controls": build_controls(allow_new_entries, symbol, risk_tier),
        "auto_gate": {
            "gate_level": gate_level,
            "allow_new_entries": allow_new_entries,
            "risk_multiplier": multiplier,
            "reasons": risk_engine.get("reasons", ["NORMAL"]),
        },
        "risk_engine": risk_engine,
        "reasons": risk_engine.get("reasons", ["NORMAL"]),
    }


def build_mock_heartbeat_item(symbol: str) -> Dict[str, Any]:
    return {
        "account": "connected",
        "magic": "n/a",
        "symbol": symbol.upper(),
        "ea_name": f"{symbol.upper()} Core EA",
        "version": "1.0.0",
        "last_seen_utc": now_utc_iso(),
        "connected": True,
        "status": "alive",
        "comment": "mock heartbeat",
        "owner_name": "system",
    }


def find_strategy_for_account_symbol_magic(account_number: str, symbol: str, magic: str) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            '''
            SELECT
                ca.id AS account_id,
                ca.account_number,
                cs.id AS strategy_id,
                cs.symbol,
                cs.name,
                cs.strategy_name,
                cs.strategy_code,
                cs.magic,
                cs.risk_tier,
                cs.is_enabled,
                cs.ea_id,
                u.email AS user_email
            FROM customer_accounts ca
            JOIN customer_strategies cs ON cs.account_id = ca.id
            JOIN users u ON u.email = ca.user_email
            WHERE ca.account_number = ?
              AND UPPER(cs.symbol) = ?
              AND TRIM(cs.magic) = ?
            ORDER BY cs.id DESC
            ''',
            (account_number.strip(), symbol.upper(), str(magic).strip()),
        ).fetchall()

    for row in rows:
        item = dict(row)
        setup = get_strategy_setup(item["user_email"], int(item["account_id"]), symbol.upper())
        enabled = bool(item["is_enabled"]) and bool(setup["enabled"])
        return {
            "id": int(item["strategy_id"]),
            "account_id": int(item["account_id"]),
            "symbol": symbol.upper(),
            "strategy_name": item.get("strategy_name") or item.get("name") or symbol.upper(),
            "name": item.get("strategy_name") or item.get("name") or symbol.upper(),
            "strategy_code": item.get("strategy_code"),
            "magic": str(item["magic"]),
            "risk_tier": setup["risk_tier"],
            "enabled": enabled,
            "is_enabled": bool(item["is_enabled"]),
            "ea_id": item.get("ea_id"),
        }
    return None


def get_latest_signal(symbol: str) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        row = conn.execute(
            '''
            SELECT *
            FROM signals
            WHERE symbol = ? AND status = 'pending'
            ORDER BY id DESC
            LIMIT 1
            ''',
            (symbol.upper(),),
        ).fetchone()
    signal = row_to_dict(row)
    if not signal:
        return None
    try:
        signal["payload"] = json.loads(signal.get("payload_json") or "{}")
    except Exception:
        signal["payload"] = {}
    return signal


def is_signal_acked(signal_id: int, account: str, magic: str) -> bool:
    with get_db() as conn:
        row = conn.execute(
            '''
            SELECT id
            FROM signal_acks
            WHERE signal_id = ? AND account = ? AND magic = ?
            LIMIT 1
            ''',
            (signal_id, account, str(magic).strip()),
        ).fetchone()
    return row is not None


def get_recent_signals(symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
    with get_db() as conn:
        rows = conn.execute(
            '''
            SELECT *
            FROM signals
            WHERE symbol = ?
            ORDER BY id DESC
            LIMIT ?
            ''',
            (symbol.upper(), limit),
        ).fetchall()
    items = rows_to_dicts(rows)
    for item in items:
        try:
            item["payload"] = json.loads(item.get("payload_json") or "{}")
        except Exception:
            item["payload"] = {}
    return items


def get_recent_acks(symbol: Optional[str] = None, account: Optional[str] = None, magic: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM signal_acks WHERE 1=1"
    params: List[Any] = []
    if symbol:
        sql += " AND symbol = ?"
        params.append(symbol.upper())
    if account:
        sql += " AND account = ?"
        params.append(account)
    if magic:
        sql += " AND magic = ?"
        params.append(str(magic).strip())
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)

    with get_db() as conn:
        rows = conn.execute(sql, params).fetchall()
    return rows_to_dicts(rows)


def build_heartbeat_status(symbol: str) -> Dict[str, Any]:
    cutoff = now_utc() - timedelta(seconds=HEARTBEAT_TIMEOUT_SEC)
    with get_db() as conn:
        rows = conn.execute(
            '''
            SELECT *
            FROM heartbeats
            WHERE symbol = ?
            ORDER BY id DESC
            LIMIT 300
            ''',
            (symbol.upper(),),
        ).fetchall()

    latest_map: Dict[str, Dict[str, Any]] = {}
    for row in rows_to_dicts(rows):
        key = f'{row.get("account","")}|{row.get("magic","")}|{row.get("symbol","")}'
        if key not in latest_map:
            latest_map[key] = row

    items: List[Dict[str, Any]] = []
    connected_count = 0
    for row in latest_map.values():
        last_seen = parse_dt(row.get("last_seen_utc"))
        connected = bool(last_seen and last_seen >= cutoff)
        if connected:
            connected_count += 1
        row["connected"] = connected
        items.append(row)

    if not items:
        items = [build_mock_heartbeat_item(symbol)]
        connected_count = 1

    return {
        "ok": True,
        "timeout_sec": HEARTBEAT_TIMEOUT_SEC,
        "connected_count": connected_count,
        "items": items,
    }


def get_latest_account_snapshot(account: str) -> Optional[Dict[str, Any]]:
    with get_db() as conn:
        row = conn.execute(
            '''
            SELECT *
            FROM account_snapshots
            WHERE account = ?
            ORDER BY id DESC
            LIMIT 1
            ''',
            (account.strip(),),
        ).fetchone()
    return row_to_dict(row)


def build_account_snapshot(account: str) -> Dict[str, Any]:
    snap = get_latest_account_snapshot(account)
    account_clean = account.strip()

    if not snap:
        return {
            "ok": True,
            "account": account_clean,
            "has_live_data": False,
            "is_live": False,
            "age_seconds": None,
            "timeout_sec": ACCOUNT_SNAPSHOT_TIMEOUT_SEC,
            "broker_name": None,
            "balance": None,
            "equity": None,
            "margin": None,
            "free_margin": None,
            "margin_level": None,
            "currency": "USD",
            "updated_utc": None,
        }

    updated_utc = snap.get("created_utc")
    updated_dt = parse_dt(updated_utc)
    age_seconds: Optional[int] = None
    is_live = False

    if updated_dt is not None:
        age_seconds = max(0, int((now_utc() - updated_dt).total_seconds()))
        is_live = age_seconds <= ACCOUNT_SNAPSHOT_TIMEOUT_SEC

    return {
        "ok": True,
        "account": snap["account"],
        "has_live_data": True,
        "is_live": is_live,
        "age_seconds": age_seconds,
        "timeout_sec": ACCOUNT_SNAPSHOT_TIMEOUT_SEC,
        "broker_name": snap.get("broker_name"),
        "balance": round(safe_float(snap.get("balance")), 2),
        "equity": round(safe_float(snap.get("equity")), 2),
        "margin": round(safe_float(snap.get("margin")), 2),
        "free_margin": round(safe_float(snap.get("free_margin")), 2),
        "margin_level": round(safe_float(snap.get("margin_level")), 2),
        "currency": snap.get("currency") or "USD",
        "updated_utc": updated_utc,
    }


def get_filtered_deals(symbol: Optional[str] = None, account: Optional[str] = None, magic: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM deals WHERE 1=1"
    params: List[Any] = []
    if symbol:
        sql += " AND symbol = ?"
        params.append(symbol.upper())
    if account:
        sql += " AND account = ?"
        params.append(account)
    if magic:
        sql += " AND magic = ?"
        params.append(str(magic).strip())
    sql += " ORDER BY deal_time_utc DESC, id DESC LIMIT ?"
    params.append(limit)

    with get_db() as conn:
        rows = conn.execute(sql, params).fetchall()

    items = list(reversed(rows_to_dicts(rows)))
    return items


def calc_equity_curve_from_pnl(rows: List[Dict[str, Any]]) -> List[float]:
    curve = [0.0]
    running = 0.0
    for row in rows:
        running += safe_float(row.get("pnl"))
        curve.append(running)
    return curve


def calc_max_drawdown_abs(curve: List[float]) -> float:
    peak = None
    max_dd = 0.0
    for x in curve:
        peak = x if peak is None else max(peak, x)
        max_dd = max(max_dd, peak - x)
    return max_dd


def calc_max_drawdown_pct(curve: List[float]) -> float:
    peak = None
    max_dd_pct = 0.0
    for x in curve:
        peak = x if peak is None else max(peak, x)
        if peak and peak > 0:
            max_dd_pct = max(max_dd_pct, ((peak - x) / peak) * 100.0)
    return max_dd_pct


def calc_max_loss_streak(rows: List[Dict[str, Any]]) -> int:
    streak = 0
    max_streak = 0
    for row in rows:
        pnl = safe_float(row.get("pnl"))
        if pnl < 0:
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return max_streak


def calc_current_loss_streak(rows: List[Dict[str, Any]]) -> int:
    streak = 0
    for row in reversed(rows):
        pnl = safe_float(row.get("pnl"))
        if pnl < 0:
            streak += 1
        else:
            break
    return streak


def summarize_kpis(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total_trades = len(rows)
    wins = 0
    losses = 0
    breakeven = 0
    gross_profit = 0.0
    gross_loss = 0.0
    net_pnl = 0.0
    sum_r = 0.0

    for row in rows:
        pnl = safe_float(row.get("pnl"))
        r_mult = safe_float(row.get("r_multiple"))
        net_pnl += pnl
        sum_r += r_mult

        if pnl > 0:
            wins += 1
            gross_profit += pnl
        elif pnl < 0:
            losses += 1
            gross_loss += abs(pnl)
        else:
            breakeven += 1

    winrate_pct = (wins / total_trades * 100.0) if total_trades > 0 else 0.0
    avg_pnl = (net_pnl / total_trades) if total_trades > 0 else 0.0
    avg_r = (sum_r / total_trades) if total_trades > 0 else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
    curve = calc_equity_curve_from_pnl(rows)

    return {
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "winrate_pct": round(winrate_pct, 2),
        "gross_profit": round(gross_profit, 2),
        "gross_loss": round(gross_loss, 2),
        "net_pnl": round(net_pnl, 2),
        "avg_pnl": round(avg_pnl, 2),
        "sum_r": round(sum_r, 2),
        "avg_r": round(avg_r, 2),
        "profit_factor": round(profit_factor, 2),
        "max_drawdown_abs": round(calc_max_drawdown_abs(curve), 2),
        "max_drawdown_pct": round(calc_max_drawdown_pct(curve), 2),
        "max_loss_streak": calc_max_loss_streak(rows),
        "current_loss_streak": calc_current_loss_streak(rows),
        "last_trade_time_utc": rows[-1].get("deal_time_utc") if rows else None,
    }


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "status": "ok",
        "service": "signal-agent-api",
        "version": "7.0.0",
        "server_time_utc": now_utc_iso(),
        "db_path": DB_PATH,
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    return {"status": "ok", "time_utc": now_utc_iso()}


@app.post("/login", response_model=LoginResponse)
def login(data: LoginRequest) -> Dict[str, str]:
    email = data.email.strip().lower()
    password = data.password.strip()
    user = db_get_user(email)

    if not user or not verify_password(password, str(user["password"])):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if user["role"] == "customer" and user.get("access_status", "active") != "active":
        raise HTTPException(status_code=403, detail=f'Customer access is {user.get("access_status")}')

    maybe_upgrade_password_hash(email, password)

    token = create_token(email=email, role=user["role"])
    return {"access_token": token, "token_type": "bearer"}


@app.get("/me")
def me(current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    return current_user


@app.get("/accounts")
def get_accounts(current_user: Dict[str, Any] = Depends(get_current_user)) -> List[Dict[str, Any]]:
    return get_user_accounts(current_user["email"])


@app.get("/accounts/{account_id}/strategies")
def get_strategies(account_id: int, current_user: Dict[str, Any] = Depends(get_current_user)) -> List[Dict[str, Any]]:
    ensure_account_access(current_user["email"], account_id)
    return get_account_strategies(account_id)


@app.get("/customer/accounts")
def get_customer_accounts(current_user: Dict[str, Any] = Depends(get_current_user)) -> List[Dict[str, Any]]:
    require_customer(current_user)
    return [format_account_payload(item) for item in get_user_accounts(current_user["email"])]


@app.post("/customer/accounts")
def create_customer_account(data: CustomerAccountCreate, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_customer(current_user)
    email = current_user["email"]
    broker_name = data.broker_name.strip()
    account_number = data.account_number.strip()
    account_label = data.account_label.strip()

    if not broker_name or not account_number or not account_label:
        raise HTTPException(status_code=422, detail="broker_name, account_number and account_label are required")

    if any(item["account_number"] == account_number for item in get_user_accounts(email)):
        raise HTTPException(status_code=400, detail="Account number already exists")

    account_id = next_account_id()
    with get_db() as conn:
        conn.execute(
            '''
            INSERT INTO customer_accounts (
                id, user_email, account_number, broker, broker_name, account_label, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (account_id, email, account_number, broker_name, broker_name, account_label, 1 if data.is_active else 0),
        )

    write_audit_log(email, "customer_account_created", f"Created account {account_number}", target_customer_id=current_user.get("customer_id"), target_account_id=account_id)
    return format_account_payload(find_account_for_user(email, account_id))


@app.put("/customer/accounts/{account_id}")
def update_customer_account(account_id: int, data: CustomerAccountUpdate, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_customer(current_user)
    email = current_user["email"]
    _ = find_account_for_user(email, account_id)

    broker_name = data.broker_name.strip()
    account_number = data.account_number.strip()
    account_label = data.account_label.strip()

    if not broker_name or not account_number or not account_label:
        raise HTTPException(status_code=422, detail="broker_name, account_number and account_label are required")

    for item in get_user_accounts(email):
        if int(item["id"]) != account_id and item["account_number"] == account_number:
            raise HTTPException(status_code=400, detail="Account number already exists")

    with get_db() as conn:
        conn.execute(
            '''
            UPDATE customer_accounts
            SET broker = ?, broker_name = ?, account_number = ?, account_label = ?, is_active = ?
            WHERE id = ? AND user_email = ?
            ''',
            (broker_name, broker_name, account_number, account_label, 1 if data.is_active else 0, account_id, email),
        )

    write_audit_log(email, "customer_account_updated", f"Updated account {account_number}", target_customer_id=current_user.get("customer_id"), target_account_id=account_id)
    return format_account_payload(find_account_for_user(email, account_id))


@app.delete("/customer/accounts/{account_id}")
def disable_customer_account(account_id: int, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_customer(current_user)
    email = current_user["email"]
    account = find_account_for_user(email, account_id)
    with get_db() as conn:
        conn.execute("UPDATE customer_accounts SET is_active = 0 WHERE id = ? AND user_email = ?", (account_id, email))
    write_audit_log(email, "customer_account_disabled", f'Disabled account {account["account_number"]}', target_customer_id=current_user.get("customer_id"), target_account_id=account_id)
    return {"ok": True, "message": "Account disabled", "account_id": account_id}


@app.get("/customer/strategies")
def get_customer_strategies(current_user: Dict[str, Any] = Depends(get_current_user)) -> List[Dict[str, Any]]:
    require_customer(current_user)
    items: List[Dict[str, Any]] = []
    for account in get_user_accounts(current_user["email"]):
        for strategy in get_account_strategies(int(account["id"])):
            items.append(format_strategy_payload(strategy))
    items.sort(key=lambda x: (int(x["account_id"]), x["symbol"], x["magic"]))
    return items


@app.post("/customer/strategies")
def create_customer_strategy(data: CustomerStrategyCreate, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_customer(current_user)
    email = current_user["email"]

    if data.account_id is None:
        raise HTTPException(status_code=422, detail="account_id is required")
    ensure_account_access(email, data.account_id)

    symbol = data.symbol.strip().upper()
    strategy_code = data.strategy_code.strip()
    strategy_name = data.strategy_name.strip()
    magic = str(data.magic).strip()
    risk_tier = normalize_risk_tier(data.risk_tier)
    ea_id = data.ea_id

    if ea_id is not None:
        _ = find_ea(int(ea_id))

    if not symbol or not strategy_code or not strategy_name or not magic:
        raise HTTPException(status_code=422, detail="symbol, strategy_code, strategy_name and magic are required")

    for item in get_account_strategies(data.account_id):
        if item["symbol"].upper() == symbol and str(item["magic"]).strip() == magic:
            raise HTTPException(status_code=400, detail="Strategy with symbol and magic already exists for this account")

    strategy_id = next_strategy_id()
    with get_db() as conn:
        conn.execute(
            '''
            INSERT INTO customer_strategies (
                id, account_id, symbol, name, strategy_name, strategy_code, magic, risk_tier, is_enabled, ea_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (strategy_id, data.account_id, symbol, strategy_name, strategy_name, strategy_code, magic, risk_tier, 1 if data.is_enabled else 0, ea_id),
        )

    set_strategy_setup(email, data.account_id, symbol, bool(data.is_enabled), risk_tier)
    write_audit_log(email, "customer_strategy_created", f"Created strategy {strategy_code} for {symbol}", target_customer_id=current_user.get("customer_id"), target_account_id=data.account_id, target_strategy_id=strategy_id)
    return format_strategy_payload(find_strategy_for_user(email, strategy_id))


@app.put("/customer/strategies/{strategy_id}")
def update_customer_strategy(strategy_id: int, data: CustomerStrategyUpdate, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_customer(current_user)
    email = current_user["email"]
    current_strategy = find_strategy_for_user(email, strategy_id)

    target_account_id = int(data.account_id if data.account_id is not None else current_strategy["account_id"])
    ensure_account_access(email, target_account_id)

    symbol = data.symbol.strip().upper()
    strategy_code = data.strategy_code.strip()
    strategy_name = data.strategy_name.strip()
    magic = str(data.magic).strip()
    risk_tier = normalize_risk_tier(data.risk_tier)
    ea_id = data.ea_id

    if ea_id is not None:
        _ = find_ea(int(ea_id))

    if not symbol or not strategy_code or not strategy_name or not magic:
        raise HTTPException(status_code=422, detail="symbol, strategy_code, strategy_name and magic are required")

    for item in get_account_strategies(target_account_id):
        if int(item["id"]) == strategy_id:
            continue
        if item["symbol"].upper() == symbol and str(item["magic"]).strip() == magic:
            raise HTTPException(status_code=400, detail="Strategy with symbol and magic already exists for this account")

    with get_db() as conn:
        conn.execute(
            '''
            UPDATE customer_strategies
            SET account_id = ?, symbol = ?, name = ?, strategy_name = ?, strategy_code = ?, magic = ?, risk_tier = ?, is_enabled = ?, ea_id = ?
            WHERE id = ?
            ''',
            (target_account_id, symbol, strategy_name, strategy_name, strategy_code, magic, risk_tier, 1 if data.is_enabled else 0, ea_id, strategy_id),
        )

    set_strategy_setup(email, target_account_id, symbol, bool(data.is_enabled), risk_tier)
    write_audit_log(email, "customer_strategy_updated", f"Updated strategy {strategy_code} for {symbol}", target_customer_id=current_user.get("customer_id"), target_account_id=target_account_id, target_strategy_id=strategy_id)
    return format_strategy_payload(find_strategy_for_user(email, strategy_id))


@app.delete("/customer/strategies/{strategy_id}")
def disable_customer_strategy(strategy_id: int, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_customer(current_user)
    email = current_user["email"]
    strategy = find_strategy_for_user(email, strategy_id)

    with get_db() as conn:
        conn.execute("UPDATE customer_strategies SET is_enabled = 0 WHERE id = ?", (strategy_id,))

    set_strategy_setup(email, int(strategy["account_id"]), strategy["symbol"], False, strategy.get("risk_tier", "balanced"))
    write_audit_log(email, "customer_strategy_disabled", f'Disabled strategy {strategy.get("strategy_code")}', target_customer_id=current_user.get("customer_id"), target_account_id=int(strategy["account_id"]), target_strategy_id=strategy_id)
    return {"ok": True, "message": "Strategy disabled", "strategy_id": strategy_id}


@app.get("/customer/setup")
def customer_setup(current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    return {"ok": True, "items": get_customer_accounts_with_setup(current_user["email"])}


@app.post("/accounts/{account_id}/strategies/{symbol}/setup")
def update_strategy_setup(account_id: int, symbol: str, data: StrategySetupIn, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    ensure_account_access(current_user["email"], account_id)

    normalized_risk = normalize_risk_tier(data.risk_tier)
    symbol_upper = symbol.strip().upper()
    valid_symbols = {str(item["symbol"]).upper() for item in get_account_strategies(account_id)}
    if symbol_upper not in valid_symbols:
        raise HTTPException(status_code=404, detail="Strategy not found")

    set_strategy_setup(current_user["email"], account_id, symbol_upper, bool(data.enabled), normalized_risk)

    with get_db() as conn:
        conn.execute(
            '''
            UPDATE customer_strategies
            SET risk_tier = ?, is_enabled = ?
            WHERE account_id = ? AND UPPER(symbol) = ?
            ''',
            (normalized_risk, 1 if data.enabled else 0, account_id, symbol_upper),
        )

    return {"ok": True, "account_id": account_id, "symbol": symbol_upper, "enabled": bool(data.enabled), "risk_tier": normalized_risk}


@app.get("/master/eas")
def master_get_eas(current_user: Dict[str, Any] = Depends(get_current_user)) -> List[Dict[str, Any]]:
    require_master(current_user)
    return [format_ea_payload(item) for item in list_eas()]


@app.post("/master/eas")
def master_create_ea(data: ExpertAdvisorCreate, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)

    ea_name = data.ea_name.strip()
    ea_code = data.ea_code.strip().lower()
    version = data.version.strip() if data.version else None
    default_symbol = data.default_symbol.strip().upper() if data.default_symbol else None
    default_magic = str(data.default_magic).strip() if data.default_magic is not None else None
    download_url = data.download_url.strip() if data.download_url and data.download_url.strip() else ""
    file_name = data.file_name.strip() if data.file_name and data.file_name.strip() else None

    if not ea_name or not ea_code:
        raise HTTPException(status_code=422, detail="ea_name and ea_code are required")

    with get_db() as conn:
        existing = conn.execute("SELECT id FROM expert_advisors WHERE ea_code = ?", (ea_code,)).fetchone()
        if existing:
            raise HTTPException(status_code=400, detail="EA code already exists")
        ea_id = next_ea_id()
        conn.execute(
            '''
            INSERT INTO expert_advisors (
                id, ea_name, ea_code, version, default_symbol, default_magic, download_url, file_name, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (ea_id, ea_name, ea_code, version, default_symbol, default_magic, download_url, file_name, 1 if data.is_active else 0),
        )

    write_audit_log(current_user["email"], "master_ea_created", f"Created EA {ea_name}")
    return format_ea_payload(find_ea(ea_id))


@app.put("/master/eas/{ea_id}")
def master_update_ea(ea_id: int, data: ExpertAdvisorUpdate, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)
    _ = find_ea(ea_id)

    ea_name = data.ea_name.strip()
    ea_code = data.ea_code.strip().lower()
    version = data.version.strip() if data.version else None
    default_symbol = data.default_symbol.strip().upper() if data.default_symbol else None
    default_magic = str(data.default_magic).strip() if data.default_magic is not None else None
    download_url = data.download_url.strip() if data.download_url and data.download_url.strip() else ""
    file_name = data.file_name.strip() if data.file_name and data.file_name.strip() else None

    if not ea_name or not ea_code:
        raise HTTPException(status_code=422, detail="ea_name and ea_code are required")

    with get_db() as conn:
        existing = conn.execute("SELECT id FROM expert_advisors WHERE ea_code = ? AND id != ?", (ea_code, ea_id)).fetchone()
        if existing:
            raise HTTPException(status_code=400, detail="EA code already exists")
        conn.execute(
            '''
            UPDATE expert_advisors
            SET ea_name = ?, ea_code = ?, version = ?, default_symbol = ?, default_magic = ?, download_url = ?, file_name = ?, is_active = ?
            WHERE id = ?
            ''',
            (ea_name, ea_code, version, default_symbol, default_magic, download_url, file_name, 1 if data.is_active else 0, ea_id),
        )

    write_audit_log(current_user["email"], "master_ea_updated", f"Updated EA {ea_name}")
    return format_ea_payload(find_ea(ea_id))


@app.delete("/master/eas/{ea_id}")
def master_disable_ea(ea_id: int, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)
    ea = find_ea(ea_id)
    with get_db() as conn:
        conn.execute("UPDATE expert_advisors SET is_active = 0 WHERE id = ?", (ea_id,))
    write_audit_log(current_user["email"], "master_ea_disabled", f'Disabled EA {ea["ea_name"]}')
    return {"ok": True, "message": "EA disabled", "ea_id": ea_id}


@app.get("/public/eas/{ea_id}/download")
def public_ea_download(ea_id: int) -> RedirectResponse:
    ea = find_ea(ea_id)
    if not bool(ea.get("is_active", True)):
        raise HTTPException(status_code=404, detail="EA is inactive")
    download_url = (ea.get("download_url") or "").strip()
    if not download_url:
        raise HTTPException(status_code=404, detail="EA download URL not configured")
    return RedirectResponse(url=download_url, status_code=307)


@app.get("/master/customers")
def master_get_customers(current_user: Dict[str, Any] = Depends(get_current_user)) -> List[Dict[str, Any]]:
    require_master(current_user)
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM customers ORDER BY id").fetchall()
    return [format_customer_payload(dict(row)) for row in rows]


@app.post("/master/customers")
def master_create_customer(data: MasterCustomerCreate, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)
    display_name = data.display_name.strip()
    if not display_name:
        raise HTTPException(status_code=422, detail="display_name is required")

    customer_id = next_customer_id()
    with get_db() as conn:
        conn.execute(
            '''
            INSERT INTO customers (
                id, display_name, access_start_at, access_end_at, access_status, trading_status, subscription_status, grace_until
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                customer_id,
                display_name,
                data.access_start_at,
                data.access_end_at,
                normalize_access_status(data.access_status),
                normalize_trading_status(data.trading_status),
                normalize_subscription_status(data.subscription_status),
                data.grace_until,
            ),
        )

    write_audit_log(current_user["email"], "master_customer_created", f"Created customer {display_name}", target_customer_id=customer_id)
    return format_customer_payload(find_customer(customer_id))


@app.get("/master/customers/{customer_id}")
def master_get_customer(customer_id: int, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)
    return format_customer_payload(find_customer(customer_id))


@app.put("/master/customers/{customer_id}")
def master_update_customer(customer_id: int, data: MasterCustomerUpdate, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)
    _ = find_customer(customer_id)
    display_name = data.display_name.strip()
    if not display_name:
        raise HTTPException(status_code=422, detail="display_name is required")

    with get_db() as conn:
        conn.execute(
            '''
            UPDATE customers
            SET display_name = ?, access_start_at = ?, access_end_at = ?, access_status = ?, trading_status = ?, subscription_status = ?, grace_until = ?
            WHERE id = ?
            ''',
            (
                display_name,
                data.access_start_at,
                data.access_end_at,
                normalize_access_status(data.access_status),
                normalize_trading_status(data.trading_status),
                normalize_subscription_status(data.subscription_status),
                data.grace_until,
                customer_id,
            ),
        )
    sync_customer_status_to_users(customer_id)
    write_audit_log(current_user["email"], "master_customer_updated", f"Updated customer {display_name}", target_customer_id=customer_id)
    return format_customer_payload(find_customer(customer_id))


@app.post("/master/users")
def master_create_customer_user(data: MasterUserCreate, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)
    email = data.email.strip().lower()
    password = data.password.strip()
    display_name = data.display_name.strip()
    customer_id = int(data.customer_id)

    if not password or not display_name:
        raise HTTPException(status_code=422, detail="password and display_name are required")
    if db_get_user(email):
        raise HTTPException(status_code=400, detail="User email already exists")

    customer = find_customer(customer_id)
    with get_db() as conn:
        conn.execute(
            '''
            INSERT INTO users (
                email, password, role, customer_id, display_name, access_status, trading_status, subscription_status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                email,
                hash_password(password),
                "customer",
                customer_id,
                display_name,
                customer.get("access_status", "active"),
                customer.get("trading_status", "enabled"),
                customer.get("subscription_status", "active"),
            ),
        )
    write_audit_log(current_user["email"], "master_customer_user_created", f"Created customer user {email}", target_customer_id=customer_id, target_user_email=email)
    return {"ok": True, "email": email, "role": "customer", "customer_id": customer_id, "display_name": display_name}


@app.get("/master/customers/{customer_id}/accounts")
def master_get_customer_accounts(customer_id: int, current_user: Dict[str, Any] = Depends(get_current_user)) -> List[Dict[str, Any]]:
    require_master(current_user)
    _ = find_customer(customer_id)
    return get_accounts_for_customer(customer_id)


@app.post("/master/customers/{customer_id}/accounts")
def master_create_customer_account(customer_id: int, data: MasterCustomerAccountCreate, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)
    _ = find_customer(customer_id)
    owner_email = require_customer_owner_email(customer_id)

    broker_name = data.broker_name.strip()
    account_number = data.account_number.strip()
    account_label = data.account_label.strip()

    if not broker_name or not account_number or not account_label:
        raise HTTPException(status_code=422, detail="broker_name, account_number and account_label are required")
    if any(item["account_number"] == account_number for item in get_user_accounts(owner_email)):
        raise HTTPException(status_code=400, detail="Account number already exists")

    account_id = next_account_id()
    with get_db() as conn:
        conn.execute(
            '''
            INSERT INTO customer_accounts (
                id, user_email, account_number, broker, broker_name, account_label, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            (account_id, owner_email, account_number, broker_name, broker_name, account_label, 1 if data.is_active else 0),
        )
    write_audit_log(current_user["email"], "master_customer_account_created", f"Created customer account {account_number}", target_customer_id=customer_id, target_user_email=owner_email, target_account_id=account_id)
    return format_account_payload(find_account_for_user(owner_email, account_id))


@app.put("/master/customers/{customer_id}/accounts/{account_id}")
def master_update_customer_account(customer_id: int, account_id: int, data: MasterCustomerAccountUpdate, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)
    _ = find_customer(customer_id)
    owner_email, _account = find_account_for_customer(customer_id, account_id)

    broker_name = data.broker_name.strip()
    account_number = data.account_number.strip()
    account_label = data.account_label.strip()

    if not broker_name or not account_number or not account_label:
        raise HTTPException(status_code=422, detail="broker_name, account_number and account_label are required")
    for item in get_user_accounts(owner_email):
        if int(item["id"]) != account_id and item["account_number"] == account_number:
            raise HTTPException(status_code=400, detail="Account number already exists")

    with get_db() as conn:
        conn.execute(
            '''
            UPDATE customer_accounts
            SET broker = ?, broker_name = ?, account_number = ?, account_label = ?, is_active = ?
            WHERE id = ?
            ''',
            (broker_name, broker_name, account_number, account_label, 1 if data.is_active else 0, account_id),
        )
    write_audit_log(current_user["email"], "master_customer_account_updated", f"Updated customer account {account_number}", target_customer_id=customer_id, target_user_email=owner_email, target_account_id=account_id)
    return format_account_payload(find_account_for_user(owner_email, account_id))


@app.delete("/master/customers/{customer_id}/accounts/{account_id}")
def master_disable_customer_account(customer_id: int, account_id: int, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)
    _ = find_customer(customer_id)
    owner_email, account = find_account_for_customer(customer_id, account_id)
    with get_db() as conn:
        conn.execute("UPDATE customer_accounts SET is_active = 0 WHERE id = ?", (account_id,))
    write_audit_log(current_user["email"], "master_customer_account_disabled", f'Disabled customer account {account["account_number"]}', target_customer_id=customer_id, target_user_email=owner_email, target_account_id=account_id)
    return {"ok": True, "message": "Account disabled", "account_id": account_id}


@app.get("/master/customers/{customer_id}/strategies")
def master_get_customer_strategies(customer_id: int, current_user: Dict[str, Any] = Depends(get_current_user)) -> List[Dict[str, Any]]:
    require_master(current_user)
    _ = find_customer(customer_id)
    return get_strategies_for_customer(customer_id)


@app.post("/master/customers/{customer_id}/strategies")
def master_create_customer_strategy(customer_id: int, data: MasterCustomerStrategyCreate, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)
    _ = find_customer(customer_id)
    owner_email, _account = ensure_account_belongs_to_customer(customer_id, data.account_id)

    symbol = data.symbol.strip().upper()
    strategy_code = data.strategy_code.strip()
    strategy_name = data.strategy_name.strip()
    magic = str(data.magic).strip()
    risk_tier = normalize_risk_tier(data.risk_tier)
    ea_id = data.ea_id

    if ea_id is not None:
        _ = find_ea(int(ea_id))
    if not symbol or not strategy_code or not strategy_name or not magic:
        raise HTTPException(status_code=422, detail="symbol, strategy_code, strategy_name and magic are required")
    for item in get_account_strategies(data.account_id):
        if item["symbol"].upper() == symbol and str(item["magic"]).strip() == magic:
            raise HTTPException(status_code=400, detail="Strategy with symbol and magic already exists for this account")

    strategy_id = next_strategy_id()
    with get_db() as conn:
        conn.execute(
            '''
            INSERT INTO customer_strategies (
                id, account_id, symbol, name, strategy_name, strategy_code, magic, risk_tier, is_enabled, ea_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (strategy_id, data.account_id, symbol, strategy_name, strategy_name, strategy_code, magic, risk_tier, 1 if data.is_enabled else 0, ea_id),
        )
    set_strategy_setup(owner_email, data.account_id, symbol, bool(data.is_enabled), risk_tier)
    write_audit_log(current_user["email"], "master_customer_strategy_created", f"Created strategy {strategy_code} for {symbol}", target_customer_id=customer_id, target_user_email=owner_email, target_account_id=data.account_id, target_strategy_id=strategy_id)
    return format_strategy_payload(find_strategy_for_user(owner_email, strategy_id))


@app.put("/master/customers/{customer_id}/strategies/{strategy_id}")
def master_update_customer_strategy(customer_id: int, strategy_id: int, data: MasterCustomerStrategyUpdate, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)
    _ = find_customer(customer_id)
    owner_email, _old = find_strategy_for_customer(customer_id, strategy_id)
    target_owner_email, _target_account = ensure_account_belongs_to_customer(customer_id, data.account_id)

    symbol = data.symbol.strip().upper()
    strategy_code = data.strategy_code.strip()
    strategy_name = data.strategy_name.strip()
    magic = str(data.magic).strip()
    risk_tier = normalize_risk_tier(data.risk_tier)
    ea_id = data.ea_id

    if ea_id is not None:
        _ = find_ea(int(ea_id))
    if not symbol or not strategy_code or not strategy_name or not magic:
        raise HTTPException(status_code=422, detail="symbol, strategy_code, strategy_name and magic are required")
    for item in get_account_strategies(data.account_id):
        if int(item["id"]) == strategy_id:
            continue
        if item["symbol"].upper() == symbol and str(item["magic"]).strip() == magic:
            raise HTTPException(status_code=400, detail="Strategy with symbol and magic already exists for this account")

    with get_db() as conn:
        conn.execute(
            '''
            UPDATE customer_strategies
            SET account_id = ?, symbol = ?, name = ?, strategy_name = ?, strategy_code = ?, magic = ?, risk_tier = ?, is_enabled = ?, ea_id = ?
            WHERE id = ?
            ''',
            (data.account_id, symbol, strategy_name, strategy_name, strategy_code, magic, risk_tier, 1 if data.is_enabled else 0, ea_id, strategy_id),
        )
    set_strategy_setup(target_owner_email, data.account_id, symbol, bool(data.is_enabled), risk_tier)
    write_audit_log(current_user["email"], "master_customer_strategy_updated", f"Updated strategy {strategy_code} for {symbol}", target_customer_id=customer_id, target_user_email=owner_email, target_account_id=data.account_id, target_strategy_id=strategy_id)
    return format_strategy_payload(find_strategy_for_user(target_owner_email, strategy_id))


@app.delete("/master/customers/{customer_id}/strategies/{strategy_id}")
def master_disable_customer_strategy(customer_id: int, strategy_id: int, current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)
    _ = find_customer(customer_id)
    owner_email, strategy = find_strategy_for_customer(customer_id, strategy_id)

    with get_db() as conn:
        conn.execute("UPDATE customer_strategies SET is_enabled = 0 WHERE id = ?", (strategy_id,))
    set_strategy_setup(owner_email, int(strategy["account_id"]), strategy["symbol"], False, strategy.get("risk_tier", "balanced"))
    write_audit_log(current_user["email"], "master_customer_strategy_disabled", f'Disabled strategy {strategy.get("strategy_code")}', target_customer_id=customer_id, target_user_email=owner_email, target_account_id=int(strategy["account_id"]), target_strategy_id=strategy_id)
    return {"ok": True, "message": "Strategy disabled", "strategy_id": strategy_id}


@app.get("/master/audit_logs")
def master_get_audit_logs(limit: int = Query(default=100, ge=1, le=500), current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    require_master(current_user)
    with get_db() as conn:
        rows = conn.execute(
            '''
            SELECT created_utc, actor_email, action_type, message, target_customer_id, target_user_email, target_account_id, target_strategy_id
            FROM audit_logs
            ORDER BY id DESC
            LIMIT ?
            ''',
            (limit,),
        ).fetchall()
    return {"ok": True, "count": len(rows), "items": rows_to_dicts(rows)}


@app.post("/tv")
def tv_signal(
    data: TVSignalIn,
    x_api_key: Optional[str] = Header(default=None, alias="x-api-key"),
) -> Dict[str, Any]:
    require_machine_api_key(x_api_key, data.key)

    symbol = data.symbol.strip().upper()
    side = normalize_side(data.side or data.action)
    if side not in ("BUY", "SELL"):
        raise HTTPException(status_code=422, detail="side/action must be BUY or SELL")

    payload = dict(data.payload or {})
    payload["score"] = data.score if data.score is not None else 1.0
    now_iso = now_utc_iso()

    with get_db() as conn:
        cur = conn.execute(
            '''
            INSERT INTO signals (symbol, side, score, payload_json, created_utc, updated_utc, status)
            VALUES (?, ?, ?, ?, ?, ?, 'pending')
            ''',
            (symbol, side, safe_float(payload.get("score"), 1.0), json.dumps(payload, ensure_ascii=False), now_iso, now_iso),
        )
        signal_id = cur.lastrowid

    return {"ok": True, "signal_id": signal_id, "symbol": symbol, "side": side, "created_utc": now_iso}


@app.get("/latest")
def latest_signal(symbol: str = Query(...), account: str = Query(...), magic: str = Query(...)) -> Dict[str, Any]:
    symbol_upper = symbol.upper()
    strategy = find_strategy_for_account_symbol_magic(account, symbol_upper, magic)

    if strategy is None:
        return {
            "ok": True,
            "has_signal": False,
            "blocked": True,
            "reason": "STRATEGY_NOT_ASSIGNED",
            "symbol": symbol_upper,
            "controls": {"paused": False, "allow_new_entries": False, "risk_multiplier": 0.0},
            "gate": {"gate_level": "RED", "allow_new_entries": False, "risk_multiplier": 0.0},
            "signal": None,
        }

    enabled = bool(strategy["enabled"])
    risk_tier = strategy.get("risk_tier", "balanced")
    risk_engine = build_risk_engine(enabled, account, magic, symbol_upper)
    gate_payload = build_gate_combo_payload(symbol_upper, enabled, risk_tier, risk_engine)

    if not gate_payload["allow_new_entries"]:
        return {
            "ok": True,
            "has_signal": False,
            "blocked": True,
            "reason": "STRATEGY_DISABLED_OR_RISK_BLOCKED",
            "symbol": symbol_upper,
            "controls": gate_payload["controls"],
            "gate": gate_payload,
            "signal": None,
        }

    signal = get_latest_signal(symbol_upper)
    if signal is None:
        return {
            "ok": True,
            "has_signal": False,
            "blocked": False,
            "reason": None,
            "symbol": symbol_upper,
            "controls": gate_payload["controls"],
            "gate": gate_payload,
            "signal": None,
        }

    if is_signal_acked(int(signal["id"]), account, magic):
        return {
            "ok": True,
            "has_signal": False,
            "blocked": False,
            "reason": "ALREADY_ACKED",
            "symbol": symbol_upper,
            "controls": gate_payload["controls"],
            "gate": gate_payload,
            "signal": None,
        }

    return {
        "ok": True,
        "has_signal": True,
        "blocked": False,
        "reason": None,
        "symbol": symbol_upper,
        "controls": gate_payload["controls"],
        "gate": gate_payload,
        "execution_engine": {
            "mode": "customer_setup",
            "score_to_risk_enabled": True,
            "score": safe_float(signal.get("score"), 1.0),
            "priority": "NORMAL",
            "risk_multiplier": gate_payload["risk_multiplier"],
            "approved": True,
            "reasons": gate_payload["reasons"],
        },
        "effective_risk_multiplier": gate_payload["risk_multiplier"],
        "delivery": {
            "delivery_id": int(signal["id"]),
            "signal_id": int(signal["id"]),
            "delivery_status": "pending",
            "first_seen_utc": signal["created_utc"],
            "ack_utc": None,
        },
        "signal": signal,
    }


@app.post("/ack")
def ack_signal(
    data: AckIn,
    x_api_key: Optional[str] = Header(default=None, alias="x-api-key"),
) -> Dict[str, Any]:
    require_machine_api_key(x_api_key, data.key)

    symbol_upper = data.symbol.strip().upper()
    magic = (data.magic or "").strip()

    with get_db() as conn:
        row = conn.execute(
            '''
            SELECT *
            FROM signals
            WHERE symbol = ? AND updated_utc = ? AND status = 'pending'
            ORDER BY id DESC
            LIMIT 1
            ''',
            (symbol_upper, data.updated_utc),
        ).fetchone()
        signal = row_to_dict(row)
        if not signal:
            raise HTTPException(status_code=404, detail="Signal not found")

        conn.execute(
            '''
            INSERT OR IGNORE INTO signal_acks (signal_id, symbol, account, magic, ack_utc, ticket)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            (int(signal["id"]), symbol_upper, data.account, magic, now_utc_iso(), data.ticket),
        )

    return {"ok": True, "signal_id": int(signal["id"]), "symbol": symbol_upper, "account": data.account, "magic": magic}


@app.post("/hb")
def heartbeat(
    data: HeartbeatPing,
    x_api_key: Optional[str] = Header(default=None, alias="x-api-key"),
) -> Dict[str, Any]:
    require_machine_api_key(x_api_key, data.key)

    with get_db() as conn:
        conn.execute(
            '''
            INSERT INTO heartbeats (account, magic, symbol, ea_name, version, last_seen_utc, status, comment, owner_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (data.account, data.magic, data.symbol.upper(), data.ea_name, data.version, now_utc_iso(), data.status, data.comment, data.owner_name),
        )
    return {"ok": True, "server_time_utc": now_utc_iso()}


@app.post("/account_snapshot")
def post_account_snapshot(
    data: AccountSnapshotIn,
    x_api_key: Optional[str] = Header(default=None, alias="x-api-key"),
) -> Dict[str, Any]:
    require_machine_api_key(x_api_key, data.key)

    created_utc = now_utc_iso()

    with get_db() as conn:
        conn.execute(
            '''
            INSERT INTO account_snapshots (
                account,
                broker_name,
                balance,
                equity,
                margin,
                free_margin,
                margin_level,
                currency,
                created_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                data.account.strip(),
                data.broker_name.strip() if data.broker_name else None,
                safe_float(data.balance),
                safe_float(data.equity),
                safe_float(data.margin),
                safe_float(data.free_margin),
                safe_float(data.margin_level),
                (data.currency or "USD").strip().upper(),
                created_utc,
            ),
        )

    return {
        "ok": True,
        "account": data.account.strip(),
        "updated_utc": created_utc,
    }


@app.get("/status/heartbeat")
def heartbeat_status(symbol: str = Query(...)) -> Dict[str, Any]:
    return build_heartbeat_status(symbol)


@app.get("/status/account_snapshot")
def status_account_snapshot(account: str = Query(...)) -> Dict[str, Any]:
    return build_account_snapshot(account)


@app.post("/deal")
def post_deal(
    data: DealIn,
    x_api_key: Optional[str] = Header(default=None, alias="x-api-key"),
) -> Dict[str, Any]:
    require_machine_api_key(x_api_key, data.key)

    deal_time = data.deal_time_utc or now_utc_iso()
    with get_db() as conn:
        conn.execute(
            '''
            INSERT INTO deals (
                account, magic, symbol, side, ticket, volume, entry_price, exit_price, sl, tp,
                pnl, commission, swap, r_multiple, strategy_code, deal_time_utc, created_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                data.account.strip(),
                str(data.magic).strip(),
                data.symbol.upper(),
                data.side,
                data.ticket,
                data.volume,
                data.entry_price,
                data.exit_price,
                data.sl,
                data.tp,
                safe_float(data.pnl),
                safe_float(data.commission),
                safe_float(data.swap),
                safe_float(data.r_multiple),
                data.strategy_code,
                deal_time,
                now_utc_iso(),
            ),
        )
    return {"ok": True}


@app.post("/risk")
def post_risk(
    data: RiskIn,
    x_api_key: Optional[str] = Header(default=None, alias="x-api-key"),
) -> Dict[str, Any]:
    require_machine_api_key(x_api_key, data.key)

    with get_db() as conn:
        conn.execute(
            '''
            INSERT INTO risk_snapshots (
                account, magic, symbol, risk_level, allow_new_entries, daily_pnl, daily_r, daily_trades,
                reasons_json, limits_json, created_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                data.account.strip(),
                str(data.magic).strip(),
                data.symbol.upper(),
                data.risk_level.strip().upper(),
                1 if data.allow_new_entries else 0,
                safe_float(data.daily_pnl),
                safe_float(data.daily_r),
                safe_int(data.daily_trades),
                json.dumps(data.reasons or [], ensure_ascii=False),
                json.dumps(data.limits or {
                    "daily_loss_cap_usd": 250.0,
                    "daily_r_cap": -5.0,
                    "daily_max_trades": 10,
                }, ensure_ascii=False),
                now_utc_iso(),
            ),
        )
    return {"ok": True}


@app.get("/status/system_overview")
def system_overview(symbol: str = Query(...), account: str = Query(...), magic: str = Query(...)) -> Dict[str, Any]:
    symbol_upper = symbol.upper()
    strategy = find_strategy_for_account_symbol_magic(account, symbol_upper, magic)
    enabled = bool(strategy["enabled"]) if strategy else False
    risk_tier = strategy.get("risk_tier", "balanced") if strategy else "balanced"
    risk_engine = build_risk_engine(enabled, account, magic, symbol_upper)
    gate_payload = build_gate_combo_payload(symbol_upper, enabled, risk_tier, risk_engine)
    kpis = summarize_kpis(get_filtered_deals(symbol_upper, account, magic, limit=50))

    return {
        "ok": True,
        "server_time_utc": now_utc_iso(),
        "filters": {"symbol": symbol_upper, "account": account, "magic": magic},
        "heartbeat": build_heartbeat_status(symbol_upper),
        "account_snapshot": build_account_snapshot(account),
        "controls": gate_payload["controls"],
        "kpis": kpis,
        "gate": gate_payload,
        "risk_engine": risk_engine,
    }


@app.get("/status/risk_engine")
def status_risk_engine(symbol: str = Query(...), account: str = Query(...), magic: str = Query(...)) -> Dict[str, Any]:
    strategy = find_strategy_for_account_symbol_magic(account, symbol.upper(), magic)
    enabled = bool(strategy["enabled"]) if strategy else False
    engine = build_risk_engine(enabled, account, magic, symbol.upper())
    payload = {"ok": True, "filters": {"symbol": symbol.upper(), "account": account, "magic": magic}, "risk_engine": engine}
    payload.update(engine)
    return payload


@app.get("/status/gate_combo")
def gate_combo(symbol: str = Query(...), account: str = Query(...), magic: str = Query(...)) -> Dict[str, Any]:
    strategy = find_strategy_for_account_symbol_magic(account, symbol.upper(), magic)
    enabled = bool(strategy["enabled"]) if strategy else False
    risk_tier = strategy.get("risk_tier", "balanced") if strategy else "balanced"
    risk_engine = build_risk_engine(enabled, account, magic, symbol.upper())
    return build_gate_combo_payload(symbol, enabled, risk_tier, risk_engine)


@app.get("/debug/state")
def debug_state(symbol: str = Query(...)) -> Dict[str, Any]:
    signals = get_recent_signals(symbol.upper(), limit=50)
    return {"ok": True, "symbol": symbol.upper(), "signals": signals[:10], "deliveries": signals}


@app.get("/debug/recent_acks")
def debug_recent_acks(symbol: Optional[str] = Query(default=None), account: Optional[str] = Query(default=None), magic: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    items = get_recent_acks(symbol=symbol, account=account, magic=magic, limit=100)
    return {
        "ok": True,
        "count": len(items),
        "items": items,
        "acks": items,
        "filters": {"symbol": symbol.upper() if symbol else None, "account": account, "magic": magic},
    }


@app.get("/debug/delivery_status")
def debug_delivery_status(signal_id: int = Query(...)) -> Dict[str, Any]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM signals WHERE id = ?", (signal_id,)).fetchone()
    signal = row_to_dict(row)
    if signal:
        try:
            signal["payload"] = json.loads(signal.get("payload_json") or "{}")
        except Exception:
            signal["payload"] = {}
    return {"ok": True, "signal": signal, "delivery_count": 0, "deliveries": []}


@app.get("/debug/pending_by_consumer")
def debug_pending_by_consumer(account: str = Query(...), magic: str = Query(...), symbol: str = Query(...)) -> Dict[str, Any]:
    latest = get_latest_signal(symbol.upper())
    items: List[Dict[str, Any]] = []
    if latest and not is_signal_acked(int(latest["id"]), account, magic):
        items.append(
            {
                "signal_id": int(latest["id"]),
                "symbol": symbol.upper(),
                "account": account,
                "magic": str(magic).strip(),
                "delivery_status": "pending",
                "payload": latest.get("payload", {}),
                "signal_created_utc": latest.get("created_utc"),
                "signal_updated_utc": latest.get("updated_utc"),
            }
        )
    return {"ok": True, "count": len(items), "items": items, "filters": {"account": account, "magic": magic, "symbol": symbol.upper()}}


@app.post("/debug/seed_users")
def debug_seed_users(
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> Dict[str, Any]:
    require_master(current_user)
    if APP_ENV == "production":
        raise HTTPException(status_code=403, detail="Not allowed in production")
    return force_seed_defaults()


@app.get("/debug/users")
def debug_users(
    current_user: Dict[str, Any] = Depends(get_current_user),
) -> Dict[str, Any]:
    require_master(current_user)

    with get_db() as conn:
        rows = conn.execute(
            '''
            SELECT email, role, customer_id, display_name, access_status, trading_status, subscription_status
            FROM users
            ORDER BY email
            '''
        ).fetchall()
    return {"ok": True, "db_path": DB_PATH, "count": len(rows), "items": rows_to_dicts(rows)}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=10000, reload=True)
