#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
⚡ ULTRA-FAST PREMIUM TELEGRAM AI CONTROL CENTER
================================================

All‑in‑one Telegram Bot for:
  • Channel member management (official Bot API only)
  • AI‑assisted appeal generation (Instagram, WhatsApp, Telegram)
  • Case management with evidence
  • User approval & admin control
  • Notifications, analytics, audit logs, language support

Run:
    export BOT_TOKEN="123456:ABC..."
    export ADMIN_ID="123456789"
    python bot.py

Optional environment variables:
    OPENAI_API_KEY   if provided, enhances appeal generation with GPT (else template-based)
    DB_PATH          SQLite file path                     (default: bot_data.sqlite3)
    MAX_WORKERS      hard concurrency ceiling             (default: 12)
    QUEUE_SIZE       bounded queue size                   (default: 100)
    PORT             if set, health endpoint is bound (Render Web Service)

Security:
    Only ADMIN_ID (owner) may approve/block users.
    All other users must request access; the owner approves them.
    Callback data is cryptographically signed to prevent forgery.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import hashlib
import hmac
import html
import json
import logging
import os
import re
import sqlite3
import sys
import tempfile
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import (
    BadRequest,
    Forbidden,
    NetworkError,
    RetryAfter,
    TelegramError,
    TimedOut,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# --------------------------------------------------------------------------- #
# Configuration                                                                #
# --------------------------------------------------------------------------- #

BOT_TOKEN = (os.environ.get("BOT_TOKEN") or "").strip()
ADMIN_ID = int((os.environ.get("ADMIN_ID") or "0").strip())
OPENAI_API_KEY = (os.environ.get("OPENAI_API_KEY") or "").strip()
DB_PATH = (os.environ.get("DB_PATH") or "bot_data.sqlite3").strip()
PORT = (os.environ.get("PORT") or "").strip()


def _env_int(name: str, default: int, low: int, high: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    value = int(raw) if raw.lstrip("-").isdigit() else default
    return max(low, min(high, value))


MIN_WORKERS = 2
MAX_WORKERS = _env_int("MAX_WORKERS", 12, 2, 24)
QUEUE_SIZE = _env_int("QUEUE_SIZE", 100, 20, 1000)

LOG_KEEP = 5000
ADMIN_CACHE_TTL = 90.0
ADMIN_CACHE_MAX = 256
SPEED_WINDOW = 256
DEDUPE_CAP = 200_000
TMP_DIR = os.path.join(tempfile.gettempdir(), "tg_member_manager")

logging.basicConfig(
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.ext.Application").setLevel(logging.WARNING)
log = logging.getLogger("manager")

# --------------------------------------------------------------------------- #
# Database schema                                                              #
# --------------------------------------------------------------------------- #

SCHEMA = """
-- Original tables
CREATE TABLE IF NOT EXISTS channels (
    chat_id       INTEGER PRIMARY KEY,
    title         TEXT    NOT NULL DEFAULT '',
    username      TEXT,
    registered_by INTEGER NOT NULL DEFAULT 0,
    registered_at REAL    NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS members (
    chat_id    INTEGER NOT NULL,
    user_id    INTEGER NOT NULL,
    username   TEXT,
    full_name  TEXT,
    status     TEXT    NOT NULL DEFAULT 'member',
    updated_at REAL    NOT NULL DEFAULT 0,
    PRIMARY KEY (chat_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_members_status   ON members(chat_id, status);
CREATE INDEX IF NOT EXISTS idx_members_username ON members(chat_id, username);
CREATE TABLE IF NOT EXISTS logs (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        REAL    NOT NULL,
    chat_id   INTEGER NOT NULL,
    admin_id  INTEGER NOT NULL,
    op        TEXT    NOT NULL,
    target_id INTEGER NOT NULL DEFAULT 0,
    ok        INTEGER NOT NULL DEFAULT 0,
    detail    TEXT
);
CREATE INDEX IF NOT EXISTS idx_logs_chat ON logs(chat_id, id DESC);
CREATE TABLE IF NOT EXISTS settings (
    chat_id  INTEGER PRIMARY KEY,
    workers  INTEGER NOT NULL DEFAULT 0,
    interval REAL    NOT NULL DEFAULT 2.0,
    mode     TEXT    NOT NULL DEFAULT 'safe',
    action   TEXT    NOT NULL DEFAULT 'remove'
);

-- New tables for user management, cases, appeals, evidence, notifications
CREATE TABLE IF NOT EXISTS users (
    user_id      INTEGER PRIMARY KEY,
    username     TEXT,
    full_name    TEXT,
    approved     INTEGER NOT NULL DEFAULT 0,
    blocked      INTEGER NOT NULL DEFAULT 0,
    language     TEXT    NOT NULL DEFAULT 'en',
    registered_at REAL   NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS cases (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER NOT NULL,
    platform     TEXT    NOT NULL,
    issue_type   TEXT    NOT NULL,
    description  TEXT,
    status       TEXT    NOT NULL DEFAULT 'draft',
    appeal_text  TEXT,
    created_at   REAL    NOT NULL,
    updated_at   REAL    NOT NULL
);
CREATE TABLE IF NOT EXISTS evidence (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id      INTEGER NOT NULL,
    type         TEXT    NOT NULL,
    content      TEXT    NOT NULL,
    uploaded_at  REAL    NOT NULL
);
CREATE TABLE IF NOT EXISTS notifications (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER NOT NULL,
    type         TEXT    NOT NULL,
    content      TEXT    NOT NULL,
    read         INTEGER NOT NULL DEFAULT 0,
    created_at   REAL    NOT NULL
);
CREATE TABLE IF NOT EXISTS appeals (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id      INTEGER NOT NULL,
    platform     TEXT    NOT NULL,
    text         TEXT    NOT NULL,
    generated_at REAL    NOT NULL
);
CREATE TABLE IF NOT EXISTS user_actions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_id     INTEGER NOT NULL,
    target_id    INTEGER NOT NULL,
    action       TEXT    NOT NULL,
    created_at   REAL    NOT NULL
);
"""

# --------------------------------------------------------------------------- #
# Database wrapper                                                             #
# --------------------------------------------------------------------------- #

class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()

    def _open(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-4000")
        conn.executescript(SCHEMA)
        conn.commit()
        return conn

    async def open(self) -> None:
        self._conn = await asyncio.to_thread(self._open)

    async def close(self) -> None:
        if self._conn is not None:
            conn, self._conn = self._conn, None
            await asyncio.to_thread(conn.close)

    def _run(self, sql: str, params: Sequence[Any], many: bool, fetch: str) -> Any:
        assert self._conn is not None
        cur = self._conn.executemany(sql, params) if many else self._conn.execute(sql, params)
        try:
            if fetch == "all":
                return cur.fetchall()
            if fetch == "one":
                return cur.fetchone()
            self._conn.commit()
            return cur.rowcount
        finally:
            cur.close()

    async def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        async with self._lock:
            return await asyncio.to_thread(self._run, sql, params, False, "none")

    async def executemany(self, sql: str, rows: Sequence[Sequence[Any]]) -> int:
        async with self._lock:
            return await asyncio.to_thread(self._run, sql, rows, True, "none")

    async def query(self, sql: str, params: Sequence[Any] = ()) -> List[sqlite3.Row]:
        async with self._lock:
            return await asyncio.to_thread(self._run, sql, params, False, "all")

    async def query_one(self, sql: str, params: Sequence[Any] = ()) -> Optional[sqlite3.Row]:
        async with self._lock:
            return await asyncio.to_thread(self._run, sql, params, False, "one")

    async def scalar(self, sql: str, params: Sequence[Any] = (), default: Any = 0) -> Any:
        row = await self.query_one(sql, params)
        if row is None:
            return default
        value = row[0]
        return default if value is None else value


DB = Database(DB_PATH)

# --------------------------------------------------------------------------- #
# Helpers & Utilities                                                          #
# --------------------------------------------------------------------------- #

BAR_FULL = "█"
BAR_EMPTY = "░"

def esc(text: Any) -> str:
    return html.escape(str(text if text is not None else ""), quote=False)

def bar(fraction: float, width: int = 10) -> str:
    fraction = 0.0 if fraction < 0 else (1.0 if fraction > 1 else fraction)
    filled = int(round(fraction * width))
    return BAR_FULL * filled + BAR_EMPTY * (width - filled)

def fmt_int(value: float) -> str:
    return f"{int(value):,}"

def fmt_duration(seconds: float) -> str:
    seconds = int(max(0, seconds))
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"

def kb(*rows: Sequence[Tuple[str, str]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(label, callback_data=data) for label, data in row] for row in rows]
    )

def parse_ids(text: str) -> Iterable[int]:
    for token in re.split(r"[^\d-]+", text or ""):
        if token and token.lstrip("-").isdigit():
            value = int(token)
            if value != 0:
                yield value

# --------------------------------------------------------------------------- #
# Signed callback data                                                         #
# --------------------------------------------------------------------------- #

_SECRET = hashlib.sha256(("cb-sign:" + BOT_TOKEN).encode("utf-8")).digest()

def cb(action: str, *args: Any) -> str:
    payload = ":".join([action, *(str(a) for a in args)])
    sig = hmac.new(_SECRET, payload.encode("utf-8"), hashlib.sha256).hexdigest()[:8]
    return f"{payload}|{sig}"

def cb_parse(data: str) -> Optional[List[str]]:
    if not data or "|" not in data:
        return None
    payload, _, sig = data.rpartition("|")
    expected = hmac.new(_SECRET, payload.encode("utf-8"), hashlib.sha256).hexdigest()[:8]
    if not hmac.compare_digest(sig, expected):
        return None
    return payload.split(":")

CB_NOOP = cb("nop")

# --------------------------------------------------------------------------- #
# LogWriter                                                                    #
# --------------------------------------------------------------------------- #

class LogWriter:
    def __init__(self, db: Database, flush_interval: float = 2.0, batch: int = 250) -> None:
        self.db = db
        self.flush_interval = flush_interval
        self.batch = batch
        self._buffer: deque = deque(maxlen=10_000)
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    def add(self, chat_id: int, admin_id: int, op: str, target_id: int, ok: bool, detail: str = "") -> None:
        self._buffer.append((time.time(), chat_id, admin_id, op, target_id, 1 if ok else 0, detail[:120]))

    async def flush(self) -> None:
        if not self._buffer:
            return
        rows = []
        while self._buffer and len(rows) < 2000:
            rows.append(self._buffer.popleft())
        await self.db.executemany(
            "INSERT INTO logs (ts, chat_id, admin_id, op, target_id, ok, detail) VALUES (?,?,?,?,?,?,?)",
            rows,
        )
        await self.db.execute(
            "DELETE FROM logs WHERE id <= (SELECT MAX(id) FROM logs) - ?",
            (LOG_KEEP,),
        )
        rows.clear()

    async def _loop(self) -> None:
        while not self._stop.is_set():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self.flush_interval)
            try:
                await self.flush()
            except Exception as exc:
                log.warning("log flush failed: %s", exc)

    def start(self) -> None:
        if self._task is None:
            self._stop.clear()
            self._task = asyncio.create_task(self._loop(), name="log-writer")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        with contextlib.suppress(Exception):
            await self.flush()


LOGS = LogWriter(DB)

# --------------------------------------------------------------------------- #
# Permission & user management helpers                                         #
# --------------------------------------------------------------------------- #

_admin_cache: Dict[int, Tuple[float, Set[int]]] = {}

def _prune_admin_cache() -> None:
    if len(_admin_cache) <= ADMIN_CACHE_MAX:
        return
    now = time.monotonic()
    stale = [k for k, (ts, _) in _admin_cache.items() if now - ts > ADMIN_CACHE_TTL]
    for key in stale:
        _admin_cache.pop(key, None)
    while len(_admin_cache) > ADMIN_CACHE_MAX:
        _admin_cache.pop(next(iter(_admin_cache)))

async def channel_admin_ids(bot, chat_id: int, *, force: bool = False) -> Set[int]:
    cached = _admin_cache.get(chat_id)
    now = time.monotonic()
    if cached and not force and now - cached[0] < ADMIN_CACHE_TTL:
        return cached[1]
    try:
        admins = await bot.get_chat_administrators(chat_id)
        ids = {member.user.id for member in admins}
    except TelegramError:
        return cached[1] if cached else set()
    _admin_cache[chat_id] = (now, ids)
    _prune_admin_cache()
    return ids

async def bot_rights(bot, chat_id: int) -> Tuple[bool, bool]:
    try:
        member = await bot.get_chat_member(chat_id, bot.id)
    except TelegramError:
        return False, False
    is_admin = str(member.status) == "administrator"
    return is_admin, bool(is_admin and getattr(member, "can_restrict_members", False))

async def is_authorized(bot, chat_id: int, user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    row = await DB.query_one("SELECT approved FROM users WHERE user_id=?", (user_id,))
    if row is None or not row["approved"]:
        return False
    return user_id in await channel_admin_ids(bot, chat_id)

async def register_user(user_id: int, username: str = "", full_name: str = "") -> None:
    await DB.execute(
        "INSERT OR IGNORE INTO users (user_id, username, full_name, registered_at) VALUES (?,?,?,?)",
        (user_id, username[:64], full_name[:128], time.time())
    )
    await DB.execute(
        "UPDATE users SET username=?, full_name=? WHERE user_id=?",
        (username[:64], full_name[:128], user_id)
    )

async def is_user_approved(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    row = await DB.query_one("SELECT approved, blocked FROM users WHERE user_id=?", (user_id,))
    if row is None:
        return False
    return bool(row["approved"]) and not bool(row["blocked"])

async def is_user_blocked(user_id: int) -> bool:
    row = await DB.query_one("SELECT blocked FROM users WHERE user_id=?", (user_id,))
    return bool(row["blocked"]) if row else False

async def get_user_language(user_id: int) -> str:
    row = await DB.query_one("SELECT language FROM users WHERE user_id=?", (user_id,))
    return row["language"] if row else "en"

async def set_user_language(user_id: int, lang: str) -> None:
    await DB.execute("UPDATE users SET language=? WHERE user_id=?", (lang, user_id))

# --------------------------------------------------------------------------- #
# Translation / Language support                                               #
# --------------------------------------------------------------------------- #

LANGUAGES = {
    "en": {
        "lang_name": "English",
        "welcome": "Welcome to the AI Control Center!",
        "not_authorized": "🔒 Access Denied\n\nYour access request has been sent to the administrator.",
        "request_sent": "✅ Access request sent. You will be notified when approved.",
        "admin_panel": "👑 Admin Panel",
        "users": "👥 Users",
        "pending": "⏳ Pending",
        "blocked": "🚫 Blocked",
        "cases": "📂 Cases",
        "analytics": "📊 Analytics",
        "settings": "⚙️ Settings",
        "back": "◀️ Back",
        "refresh": "🔄 Refresh",
        "confirm": "✅ Confirm",
        "cancel": "❌ Cancel",
        "stop": "⛔ Stop",
    },
    "hi": {
        "lang_name": "हिन्दी",
        "welcome": "एआई कंट्रोल सेंटर में आपका स्वागत है!",
        "not_authorized": "🔒 पहुंच अस्वीकृत\n\nआपका एक्सेस अनुरोध व्यवस्थापक को भेज दिया गया है।",
        "request_sent": "✅ एक्सेस अनुरोध भेजा गया। अनुमोदित होने पर आपको सूचित किया जाएगा।",
        "admin_panel": "👑 व्यवस्थापक पैनल",
        "users": "👥 उपयोगकर्ता",
        "pending": "⏳ लंबित",
        "blocked": "🚫 अवरुद्ध",
        "cases": "📂 मामले",
        "analytics": "📊 विश्लेषण",
        "settings": "⚙️ सेटिंग्स",
        "back": "◀️ वापस",
        "refresh": "🔄 ताज़ा करें",
        "confirm": "✅ पुष्टि करें",
        "cancel": "❌ रद्द करें",
        "stop": "⛔ रोकें",
    }
}

async def get_text(user_id: int, key: str, *args) -> str:
    lang = await get_user_language(user_id)
    template = LANGUAGES.get(lang, LANGUAGES["en"]).get(key, key)
    return template.format(*args)

# --------------------------------------------------------------------------- #
# AI Appeal Generator (modern OpenAI v1.x)                                     #
# --------------------------------------------------------------------------- #

# Check if openai is available; fallback to template if not
_OPENAI_AVAILABLE = False
_openai_client = None

if OPENAI_API_KEY:
    try:
        from openai import AsyncOpenAI
        _openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        _OPENAI_AVAILABLE = True
        log.info("OpenAI client initialized with API key.")
    except ImportError:
        log.warning("openai package not installed. Using template-based appeals.")
    except Exception as e:
        log.warning("OpenAI initialization failed: %s", e)

async def generate_appeal(
    platform: str,
    issue_type: str,
    description: str,
    user_details: Optional[Dict[str, str]] = None
) -> str:
    """Generate a professional appeal using OpenAI (if available) or template."""
    if _OPENAI_AVAILABLE and _openai_client:
        try:
            prompt = (
                f"Write a professional appeal for a {platform} account recovery. "
                f"Issue: {issue_type}. Details: {description}. "
                "The appeal should be polite, clear, and include all necessary information "
                "to request reinstatement or resolution. Do not include false information."
            )
            response = await _openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=300,
                temperature=0.7
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            log.warning("OpenAI generation failed: %s", e)

    # Template fallback
    templates = {
        ("instagram", "disabled"): (
            "Dear Instagram Support Team,\n\n"
            "My Instagram account (@{username}) has been disabled. I believe this was a mistake. "
            "I have always followed the Community Guidelines and Terms of Service. "
            "I kindly request a review of my account and ask that you reinstate it. "
            "I can provide any additional information if needed.\n\n"
            "Thank you for your attention.\n\nSincerely,\n{full_name}"
        ),
        ("instagram", "hacked"): (
            "Dear Instagram Support Team,\n\n"
            "My Instagram account (@{username}) was hacked. The attacker changed the email and phone number. "
            "I have proof of ownership and can verify my identity. Please help me recover my account. "
            "I can provide previous email addresses, phone numbers, and any other verification details.\n\n"
            "Thank you.\n\nSincerely,\n{full_name}"
        ),
        ("whatsapp", "banned"): (
            "Dear WhatsApp Support,\n\n"
            "My WhatsApp number {phone} has been banned. I have not violated any terms of service. "
            "I request a review of the ban and ask that you reactivate my account. "
            "I am willing to provide any further information needed.\n\n"
            "Thank you.\n\nSincerely,\n{full_name}"
        ),
        ("telegram", "spam"): (
            "Dear Telegram Support,\n\n"
            "My Telegram account (username @{username}) has been limited due to spam-related issues. "
            "I believe this is a false positive. I use Telegram for legitimate communication and always follow the rules. "
            "Please review my account and remove the restriction.\n\n"
            "Thank you.\n\nSincerely,\n{full_name}"
        ),
    }
    key = (platform.lower(), issue_type.lower().replace(" ", "_"))
    template = templates.get(key, "Please write a professional appeal regarding your {platform} issue: {description}.")
    user = user_details or {}
    return template.format(
        username=user.get("username", "username"),
        full_name=user.get("full_name", "User"),
        phone=user.get("phone", "your phone number"),
        platform=platform.capitalize(),
        description=description
    )

# --------------------------------------------------------------------------- #
# Bulk operation engine (unchanged – works as before)                         #
# --------------------------------------------------------------------------- #

# Error categories
PERMANENT = "permanent"
TEMPORARY = "temporary"
PERMISSION = "permission"
RATELIMIT = "ratelimit"
NETWORK = "network"
SKIP = "skipped"

class AdaptiveLimit:
    def __init__(self, start: int, minimum: int = MIN_WORKERS, maximum: int = MAX_WORKERS) -> None:
        self.minimum = max(1, minimum)
        self.maximum = max(self.minimum, maximum)
        self._limit = max(self.minimum, min(start, self.maximum))
        self._active = 0
        self._cond = asyncio.Condition()

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def active(self) -> int:
        return self._active

    async def acquire(self) -> None:
        async with self._cond:
            while self._active >= self._limit:
                await self._cond.wait()
            self._active += 1

    async def release(self) -> None:
        async with self._cond:
            if self._active > 0:
                self._active -= 1
            self._cond.notify()

    async def grow(self, step: int = 1) -> int:
        async with self._cond:
            new = min(self.maximum, self._limit + step)
            if new != self._limit:
                self._limit = new
                self._cond.notify(step)
            return self._limit

    async def shrink(self) -> int:
        async with self._cond:
            self._limit = max(self.minimum, self._limit // 2)
            return self._limit


@dataclass
class Stats:
    total: int = 0
    processed: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    retries: int = 0
    floodwaits: int = 0
    started: float = 0.0
    finished: float = 0.0
    errors: Counter = field(default_factory=Counter)
    window: deque = field(default_factory=lambda: deque(maxlen=SPEED_WINDOW))

    def mark(self) -> None:
        self.window.append(time.monotonic())

    @property
    def elapsed(self) -> float:
        end = self.finished or time.monotonic()
        return max(0.0, end - self.started) if self.started else 0.0

    @property
    def speed(self) -> float:
        if len(self.window) < 2:
            return 0.0
        span = self.window[-1] - self.window[0]
        return (len(self.window) - 1) / span if span > 0 else 0.0

    @property
    def avg_speed(self) -> float:
        return self.processed / self.elapsed if self.elapsed > 0.5 else 0.0

    @property
    def remaining(self) -> int:
        return max(0, self.total - self.processed) if self.total else 0

    @property
    def eta(self) -> float:
        rate = self.speed or self.avg_speed
        return self.remaining / rate if (rate > 0 and self.total) else 0.0

    @property
    def fraction(self) -> float:
        return (self.processed / self.total) if self.total else 0.0


class Operation:
    ACTIONS = {"remove": "🗑 Remove (kick)", "ban": "⛔ Ban"}

    def __init__(
        self,
        app: Application,
        *,
        chat_id: int,
        title: str,
        admin_id: int,
        dm_chat_id: int,
        source: str,
        source_path: Optional[str],
        action: str,
        workers: int,
        interval: float,
        mode: str,
        verify: bool,
        total: int,
    ) -> None:
        self.app = app
        self.chat_id = chat_id
        self.title = title
        self.admin_id = admin_id
        self.dm_chat_id = dm_chat_id
        self.source = source
        self.source_path = source_path
        self.action = action if action in self.ACTIONS else "remove"
        self.interval = max(1.0, min(5.0, interval))
        self.mode = mode
        self.verify = verify

        start = workers if workers else (4 if mode == "fast" else MIN_WORKERS)
        ceiling = MAX_WORKERS if mode == "fast" else max(MIN_WORKERS, MAX_WORKERS // 2)
        self.limiter = AdaptiveLimit(start, MIN_WORKERS, min(ceiling, MAX_WORKERS))

        self.queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_SIZE)
        self.stop_event = asyncio.Event()
        self.stats = Stats(total=total)
        self.message_id: Optional[int] = None
        self.abort_reason: str = ""
        self.stopped_by_user = False

        self._seen: Set[int] = set()
        self._producer_done = False
        self._resume_at = 0.0
        self._streak = 0
        self._last_flood = 0.0
        self._permission_errors = 0
        self._tasks: List[asyncio.Task] = []
        self._task: Optional[asyncio.Task] = None
        self._grow_after = 20 if mode == "fast" else 40

    def start(self, message_id: int) -> None:
        self.message_id = message_id
        self._task = asyncio.create_task(self._run(), name=f"op-{self.chat_id}")

    async def wait(self) -> None:
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    def request_stop(self, by_user: bool = True) -> None:
        self.stopped_by_user = by_user
        self.stop_event.set()
        drained = 0
        while True:
            try:
                self.queue.get_nowait()
                drained += 1
            except asyncio.QueueEmpty:
                break
        if drained:
            log.info("stop: dropped %d queued targets", drained)

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def _run(self) -> None:
        self.stats.started = time.monotonic()
        producer = asyncio.create_task(self._produce(), name="producer")
        progress = asyncio.create_task(self._progress_loop(), name="progress")
        workers = [
            asyncio.create_task(self._worker(), name=f"worker-{i}")
            for i in range(self.limiter.maximum)
        ]
        self._tasks = [producer, progress, *workers]
        try:
            await asyncio.gather(*workers, return_exceptions=True)
        finally:
            for task in (producer, progress):
                task.cancel()
            for task in (producer, progress):
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            await self._finalize()

    async def _produce(self) -> None:
        try:
            async for user_id in self._iter_targets():
                if self.stop_event.is_set():
                    break
                if user_id == self.app.bot.id:
                    continue
                if len(self._seen) < DEDUPE_CAP:
                    if user_id in self._seen:
                        continue
                    self._seen.add(user_id)
                await self.queue.put(user_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("producer stopped: %s", exc)
            self.abort_reason = self.abort_reason or f"Source error: {exc}"
        finally:
            self._producer_done = True

    async def _iter_targets(self) -> AsyncIterator[int]:
        if self.source == "db":
            async for uid in self._iter_db():
                yield uid
        else:
            async for uid in self._iter_file():
                yield uid

    async def _iter_db(self) -> AsyncIterator[int]:
        last = 0
        while not self.stop_event.is_set():
            rows = await DB.query(
                "SELECT user_id FROM members "
                "WHERE chat_id=? AND user_id>? AND status IN ('member','restricted') "
                "ORDER BY user_id LIMIT 500",
                (self.chat_id, last),
            )
            if not rows:
                return
            for row in rows:
                last = int(row["user_id"])
                yield last

    async def _iter_file(self) -> AsyncIterator[int]:
        path = self.source_path
        if not path or not os.path.exists(path):
            return

        def read_chunk(handle, limit: int) -> List[int]:
            out: List[int] = []
            for line in handle:
                out.extend(parse_ids(line))
                if len(out) >= limit:
                    break
            return out

        handle = await asyncio.to_thread(open, path, "r", encoding="utf-8", errors="ignore")
        try:
            while not self.stop_event.is_set():
                chunk = await asyncio.to_thread(read_chunk, handle, 200)
                if not chunk:
                    return
                for uid in chunk:
                    yield uid
        finally:
            await asyncio.to_thread(handle.close)

    async def _worker(self) -> None:
        while True:
            if self.stop_event.is_set():
                return
            try:
                user_id = await asyncio.wait_for(self.queue.get(), timeout=0.4)
            except asyncio.TimeoutError:
                if self._producer_done and self.queue.empty():
                    return
                continue
            if self.stop_event.is_set():
                return
            await self._pause_gate()
            if self.stop_event.is_set():
                return
            await self.limiter.acquire()
            try:
                await self._process(user_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._record(user_id, False, TEMPORARY, str(exc))
            finally:
                await self.limiter.release()

    async def _pause_gate(self) -> None:
        while not self.stop_event.is_set():
            remaining = self._resume_at - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, 0.5))

    async def _flood(self, seconds: float) -> None:
        seconds = max(1.0, min(float(seconds) + 0.5, 300.0))
        self.stats.floodwaits += 1
        self._last_flood = time.monotonic()
        self._streak = 0
        await self.limiter.shrink()
        self._resume_at = max(self._resume_at, time.monotonic() + seconds)
        await self._pause_gate()

    async def _process(self, user_id: int) -> None:
        bot = self.app.bot
        attempts = 0
        while attempts < 3 and not self.stop_event.is_set():
            attempts += 1
            try:
                if self.verify:
                    member = await bot.get_chat_member(self.chat_id, user_id)
                    status = str(member.status)
                    if status in ("creator", "administrator"):
                        await self._record(user_id, False, SKIP, "administrator")
                        return
                    if status in ("left", "kicked"):
                        await self._record(user_id, False, SKIP, "not a member")
                        return

                await bot.ban_chat_member(self.chat_id, user_id)
                if self.action == "remove":
                    await bot.unban_chat_member(self.chat_id, user_id, only_if_banned=True)
                await self._record(user_id, True, "", "")
                return

            except RetryAfter as exc:
                self.stats.retries += 1
                await self._flood(getattr(exc, "retry_after", 5) or 5)
                continue
            except (TimedOut, NetworkError) as exc:
                self.stats.retries += 1
                if attempts >= 3:
                    await self._record(user_id, False, NETWORK, str(exc))
                    return
                await asyncio.sleep(min(1.5 * attempts, 4.0))
                continue
            except Forbidden as exc:
                self._permission_errors += 1
                await self._record(user_id, False, PERMISSION, str(exc))
                if self._permission_errors >= 5 and self.stats.success == 0:
                    self.abort_reason = "The bot lacks the required permission in this channel."
                    self.request_stop(by_user=False)
                return
            except BadRequest as exc:
                await self._record(user_id, False, PERMANENT, str(exc))
                return
            except TelegramError as exc:
                await self._record(user_id, False, TEMPORARY, str(exc))
                return
        if attempts >= 3:
            await self._record(user_id, False, RATELIMIT, "retry limit reached")

    async def _record(self, user_id: int, ok: bool, category: str, detail: str) -> None:
        self.stats.processed += 1
        self.stats.mark()
        if ok:
            self.stats.success += 1
            self._streak += 1
            LOGS.add(self.chat_id, self.admin_id, self.action, user_id, True, "")
            if (
                self._streak >= self._grow_after
                and time.monotonic() - self._last_flood > 20
                and self.limiter.limit < self.limiter.maximum
            ):
                self._streak = 0
                await self.limiter.grow()
        elif category == SKIP:
            self.stats.skipped += 1
            self.stats.errors[SKIP] += 1
        else:
            self.stats.failed += 1
            self.stats.errors[category or TEMPORARY] += 1
            LOGS.add(self.chat_id, self.admin_id, self.action, user_id, False, detail)

    def status_line(self) -> str:
        if self.stop_event.is_set():
            return "🔴 STOPPING"
        if self._resume_at > time.monotonic():
            return "⏳ RATE LIMIT"
        return "🟢 RUNNING"

    def render_progress(self) -> str:
        s = self.stats
        total_line = f"┃ Remaining: {fmt_int(s.remaining)}\n" if s.total else ""
        eta_line = f"┃ ETA: {fmt_duration(s.eta)}\n" if (s.total and s.eta) else ""
        pct = int(round(s.fraction * 100)) if s.total else 0
        return (
            "╭━━〔 ⚡ FAST CLEANUP 〕━━╮\n"
            "┃\n"
            f"┃ 📡 {esc(self.title)}\n"
            f"┃ Progress: {bar(s.fraction)} {pct}%\n"
            "┃\n"
            f"┃ Processed: {fmt_int(s.processed)}\n"
            f"┃ Success: {fmt_int(s.success)}\n"
            f"┃ Failed: {fmt_int(s.failed)}\n"
            f"┃ Skipped: {fmt_int(s.skipped)}\n"
            f"{total_line}"
            "┃\n"
            f"┃ 🚀 Speed: {s.speed:.1f}/sec\n"
            f"┃ ⚡ Avg: {s.avg_speed:.1f}/sec\n"
            f"┃ ⏱ Elapsed: {fmt_duration(s.elapsed)}\n"
            f"{eta_line}"
            f"┃ 👷 Workers: {self.limiter.limit}\n"
            f"┃ ⏳ FloodWaits: {s.floodwaits}\n"
            f"┃ Status: {self.status_line()}\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━━╯"
        )

    def progress_keyboard(self) -> InlineKeyboardMarkup:
        return kb([("⛔ STOP NOW", cb("stp", self.chat_id))])

    async def _progress_loop(self) -> None:
        last = ""
        while True:
            await asyncio.sleep(self.interval)
            text = self.render_progress()
            if text == last or self.message_id is None:
                continue
            try:
                await self.app.bot.edit_message_text(
                    chat_id=self.dm_chat_id,
                    message_id=self.message_id,
                    text=text,
                    reply_markup=self.progress_keyboard(),
                    parse_mode=ParseMode.HTML,
                )
                last = text
            except RetryAfter as exc:
                await asyncio.sleep(float(getattr(exc, "retry_after", 3) or 3))
            except (BadRequest, TelegramError):
                pass

    def render_report(self) -> str:
        s = self.stats
        header = "⛔ CLEANUP STOPPED" if (self.stopped_by_user or self.abort_reason) else "✅ CLEANUP COMPLETE"
        note = f"\n⚠️ {esc(self.abort_reason)}\n" if self.abort_reason else ""
        return (
            f"╭━━〔 {header} 〕━━╮\n"
            "┃\n"
            f"┃ 📡 {esc(self.title)}\n"
            f"┃ Action: {esc(self.ACTIONS[self.action])}\n"
            "┃\n"
            f"┃ Total Processed: {fmt_int(s.processed)}\n"
            f"┃ Successful: {fmt_int(s.success)}\n"
            f"┃ Failed: {fmt_int(s.failed)}\n"
            f"┃ Skipped: {fmt_int(s.skipped)}\n"
            "┃\n"
            f"┃ Average Speed: {s.avg_speed:.1f}/sec\n"
            f"┃ Duration: {fmt_duration(s.elapsed)}\n"
            f"┃ FloodWaits: {s.floodwaits}\n"
            f"┃ Retries: {s.retries}\n"
            "┃\n"
            "┃ Pending tasks cancelled.\n"
            "┃ Temporary memory cleared.\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━━━━╯"
            f"{note}"
        )

    def render_details(self) -> str:
        s = self.stats
        if not s.errors:
            body = "No failures recorded."
        else:
            labels = {
                PERMANENT: "Permanent failure",
                TEMPORARY: "Temporary failure",
                PERMISSION: "Permission failure",
                RATELIMIT: "Rate limit",
                NETWORK: "Network failure",
                SKIP: "Skipped (admin / not a member)",
            }
            body = "\n".join(
                f"• {labels.get(cat, cat)}: {fmt_int(count)}" for cat, count in s.errors.most_common()
            )
        return (
            "📋 <b>OPERATION DETAILS</b>\n\n"
            f"Channel: {esc(self.title)}\n"
            f"Source: {'Known-members database' if self.source == 'db' else 'Supplied ID list'}\n"
            f"Action: {esc(self.ACTIONS[self.action])}\n"
            f"Mode: {esc(self.mode.upper())} · verify: {'on' if self.verify else 'off'}\n"
            f"Peak workers: {self.limiter.limit}\n\n"
            f"{body}"
        )

    async def _finalize(self) -> None:
        self.stats.finished = time.monotonic()
        while True:
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self._seen.clear()
        self._tasks.clear()
        if self.source_path:
            with contextlib.suppress(OSError):
                os.remove(self.source_path)
            self.source_path = None

        with contextlib.suppress(Exception):
            await LOGS.flush()

        text = self.render_report()
        keyboard = kb(
            [("📋 DETAILS", cb("det", self.chat_id)), ("📝 LOGS", cb("logs", self.chat_id, 0))],
            [("🔄 NEW OPERATION", cb("cln", self.chat_id)), ("◀️ DASHBOARD", cb("ch", self.chat_id))],
        )
        if self.message_id is not None:
            with contextlib.suppress(TelegramError):
                await self.app.bot.edit_message_text(
                    chat_id=self.dm_chat_id,
                    message_id=self.message_id,
                    text=text,
                    reply_markup=keyboard,
                    parse_mode=ParseMode.HTML,
                )
        REPORTS[self.chat_id] = self
        while len(REPORTS) > 20:
            REPORTS.pop(next(iter(REPORTS)), None)
        JOBS.pop(self.chat_id, None)
        gc.collect()


JOBS: Dict[int, Operation] = {}
REPORTS: Dict[int, Operation] = {}

# --------------------------------------------------------------------------- #
# Settings helpers                                                             #
# --------------------------------------------------------------------------- #

DEFAULT_SETTINGS = {"workers": 0, "interval": 2.0, "mode": "safe", "action": "remove"}

async def get_settings(chat_id: int) -> Dict[str, Any]:
    row = await DB.query_one(
        "SELECT workers, interval, mode, action FROM settings WHERE chat_id=?", (chat_id,)
    )
    if row is None:
        return dict(DEFAULT_SETTINGS)
    return {
        "workers": int(row["workers"]),
        "interval": float(row["interval"]),
        "mode": str(row["mode"]),
        "action": str(row["action"]),
    }

async def save_setting(chat_id: int, key: str, value: Any) -> None:
    if key not in DEFAULT_SETTINGS:
        return
    current = await get_settings(chat_id)
    current[key] = value
    await DB.execute(
        "INSERT INTO settings (chat_id, workers, interval, mode, action) VALUES (?,?,?,?,?) "
        "ON CONFLICT(chat_id) DO UPDATE SET workers=excluded.workers, interval=excluded.interval, "
        "mode=excluded.mode, action=excluded.action",
        (chat_id, int(current["workers"]), float(current["interval"]), current["mode"], current["action"]),
    )

# --------------------------------------------------------------------------- #
# Screen functions (Channel, Member, Admin, AI, Cases, etc.)                   #
# --------------------------------------------------------------------------- #

API_LIMITATION = (
    "⚠️ <b>TELEGRAM API LIMITATION</b>\n\n"
    "The official Bot API does not expose a channel's subscriber list, so no bot "
    "can enumerate and remove every member of a channel.\n\n"
    "This bot works only with data Telegram actually provides:\n"
    "• members observed through official membership updates since the bot became "
    "administrator (stored locally);\n"
    "• a list of user IDs you supply yourself.\n\n"
    "No unofficial bypass is used."
)

async def channels_for(bot, user_id: int) -> List[sqlite3.Row]:
    rows = await DB.query("SELECT * FROM channels ORDER BY registered_at DESC LIMIT 20")
    allowed: List[sqlite3.Row] = []
    for row in rows:
        chat_id = int(row["chat_id"])
        if user_id == ADMIN_ID or int(row["registered_by"]) == user_id:
            allowed.append(row)
            continue
        if user_id in await channel_admin_ids(bot, chat_id):
            allowed.append(row)
    return allowed

async def home_screen(bot, user_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    rows = await channels_for(bot, user_id)
    if not rows:
        text = (
            "╭━━━〔 ✦ AI CONTROL CENTER ✦ 〕━━━╮\n"
            "┃\n"
            "┃ 🤖 AI Recovery & Management\n"
            "┃ 🟢 System: ONLINE\n"
            "┃ 🔐 Access: AUTHORIZED\n"
            "┃ ⚡ Performance: OPTIMIZED\n"
            "┃\n"
            "╰━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
            "🟡 No channel connected yet\n\n"
            "<b>Setup</b>\n"
            "1. Add this bot to your channel.\n"
            "2. Promote it to administrator with the "
            "<b>Ban users</b> permission.\n"
            "3. The channel is detected automatically.\n\n"
            + API_LIMITATION
        )
        return text, kb([("🔄 REFRESH", cb("home"))])

    text = (
        "╭━━━〔 ✦ AI CONTROL CENTER ✦ 〕━━━╮\n"
        "┃\n"
        "┃ 🤖 AI Recovery & Management\n"
        "┃ 🟢 System: ONLINE\n"
        "┃ 🔐 Access: AUTHORIZED\n"
        "┃ ⚡ Performance: OPTIMIZED\n"
        "┃\n"
        f"┃ 📡 Connected channels: {len(rows)}\n"
        "┃\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
        "Select a channel to manage:"
    )
    buttons = [[(f"📡 {row['title'] or row['chat_id']}", cb("ch", int(row["chat_id"])))] for row in rows]
    if user_id == ADMIN_ID:
        buttons.append([("👑 ADMIN PANEL", cb("admin_panel"))])
    buttons.append([("🔄 REFRESH", cb("home"))])
    return text, kb(*buttons)

async def channel_screen(bot, chat_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    row = await DB.query_one("SELECT * FROM channels WHERE chat_id=?", (chat_id,))
    title = (row["title"] if row else "") or str(chat_id)
    is_admin, can_restrict = await bot_rights(bot, chat_id)
    known = await DB.scalar("SELECT COUNT(*) FROM members WHERE chat_id=?", (chat_id,))
    job = JOBS.get(chat_id)

    text = (
        "╭━━━〔 📡 CHANNEL CENTER 〕━━━╮\n"
        "┃\n"
        f"┃ 📡 Channel: {esc(title)}\n"
        f"┃ 🆔 ID: <code>{chat_id}</code>\n"
        f"┃ 🛡 Bot admin: {'✅' if is_admin else '❌'}\n"
        f"┃ 🗑 Ban permission: {'✅' if can_restrict else '❌'}\n"
        f"┃ 👥 Known members: {fmt_int(known)}\n"
        f"┃ {'⚡ Operation running' if job else '🟢 Status: Connected'}\n"
        "┃\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╯"
    )
    rows = [
        [("👥 MEMBERS", cb("mem", chat_id)), ("⚡ FAST CLEANER", cb("cln", chat_id))],
        [("🔎 SEARCH", cb("srch", chat_id)), ("📊 STATISTICS", cb("stat", chat_id))],
        [("📝 LOGS", cb("logs", chat_id, 0)), ("⚙️ SETTINGS", cb("set", chat_id))],
        [("◀️ BACK", cb("home"))],
    ]
    if job:
        rows.insert(0, [("⛔ STOP NOW", cb("stp", chat_id))])
    return text, kb(*rows)

async def cleaner_screen(bot, chat_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    row = await DB.query_one("SELECT title FROM channels WHERE chat_id=?", (chat_id,))
    title = (row["title"] if row else "") or str(chat_id)
    cfg = await get_settings(chat_id)
    known = await DB.scalar(
        "SELECT COUNT(*) FROM members WHERE chat_id=? AND status IN ('member','restricted')", (chat_id,)
    )
    workers = "Auto" if not cfg["workers"] else cfg["workers"]
    text = (
        "╭━━━〔 ⚡ FAST CLEANER 〕━━━╮\n"
        "┃\n"
        f"┃ 📡 Channel: {esc(title)}\n"
        "┃\n"
        f"┃ ⚡ Mode: {cfg['mode'].capitalize()}\n"
        f"┃ 🎯 Action: {esc(Operation.ACTIONS[cfg['action']])}\n"
        "┃ 🧠 Memory: Optimized\n"
        f"┃ 🚀 Workers: {workers}\n"
        f"┃ ⏱ Updates: {cfg['interval']:.0f}s\n"
        "┃\n"
        "╰━━━━━━━━━━━━━━━━━━━━━━━━━━━━╯\n\n"
        "Choose the target source:\n"
        f"• Known-members database — {fmt_int(known)} eligible entries\n"
        "• ID list — paste IDs or upload a .txt file\n\n"
        + API_LIMITATION
    )
    return text, kb(
        [("🗄 KNOWN MEMBERS", cb("src", chat_id, "db"))],
        [("📄 ID LIST", cb("src", chat_id, "list"))],
        [("⚙️ SPEED SETTINGS", cb("set", chat_id))],
        [("◀️ BACK", cb("ch", chat_id))],
    )

def confirm_screen(chat_id: int, title: str, total: int, action: str, source: str) -> Tuple[str, InlineKeyboardMarkup]:
    text = (
        "⚠️ <b>CONFIRM FAST CLEANUP</b>\n\n"
        f"📡 Channel: {esc(title)}\n"
        f"🎯 Action: {esc(Operation.ACTIONS[action])}\n"
        f"🗂 Source: {'Known-members database' if source == 'db' else 'Supplied ID list'}\n"
        f"🔢 Targets: {fmt_int(total)}\n\n"
        "This will perform the selected member-management operation using the "
        "fastest safe API-supported method. Administrators are skipped.\n\n"
        "Removed users can rejoin; banned users cannot."
    )
    return text, kb(
        [("✅ START", cb("go", chat_id))],
        [("❌ CANCEL", cb("cln", chat_id))],
    )

async def settings_screen(chat_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    cfg = await get_settings(chat_id)
    mark = lambda cond: "🔘" if cond else "⚪"
    text = (
        "⚙️ <b>SPEED SETTINGS</b>\n\n"
        f"Workers: {'AUTO' if not cfg['workers'] else cfg['workers']}\n"
        f"Progress updates: {cfg['interval']:.0f}s\n"
        f"Mode: {cfg['mode'].upper()}\n"
        f"Action: {esc(Operation.ACTIONS[cfg['action']])}\n\n"
        f"Concurrency ceiling on this host: {MAX_WORKERS}\n"
        "AUTO starts conservatively and adapts to Telegram's responses. "
        "Rate limits always reduce concurrency automatically."
    )
    return text, kb(
        [
            (f"{mark(cfg['workers'] == 2)} 2", cb("sw", chat_id, 2)),
            (f"{mark(cfg['workers'] == 4)} 4", cb("sw", chat_id, 4)),
            (f"{mark(cfg['workers'] == 8)} 8", cb("sw", chat_id, 8)),
            (f"{mark(cfg['workers'] == 0)} AUTO", cb("sw", chat_id, 0)),
        ],
        [
            (f"{mark(cfg['interval'] == 1)} 1s", cb("si", chat_id, 1)),
            (f"{mark(cfg['interval'] == 2)} 2s", cb("si", chat_id, 2)),
            (f"{mark(cfg['interval'] == 5)} 5s", cb("si", chat_id, 5)),
        ],
        [
            (f"{mark(cfg['mode'] == 'fast')} 🚀 FAST", cb("sm", chat_id, "fast")),
            (f"{mark(cfg['mode'] == 'safe')} 🛡 SAFE", cb("sm", chat_id, "safe")),
        ],
        [
            (f"{mark(cfg['action'] == 'remove')} 🗑 REMOVE", cb("sa", chat_id, "remove")),
            (f"{mark(cfg['action'] == 'ban')} ⛔ BAN", cb("sa", chat_id, "ban")),
        ],
        [("◀️ BACK", cb("ch", chat_id))],
    )

async def stats_screen(bot, chat_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    row = await DB.query_one("SELECT title FROM channels WHERE chat_id=?", (chat_id,))
    title = (row["title"] if row else "") or str(chat_id)
    try:
        subscribers = await bot.get_chat_member_count(chat_id)
    except TelegramError:
        subscribers = None
    admins = len(await channel_admin_ids(bot, chat_id, force=True))
    known = await DB.scalar("SELECT COUNT(*) FROM members WHERE chat_id=?", (chat_id,))
    active = await DB.scalar(
        "SELECT COUNT(*) FROM members WHERE chat_id=? AND status IN ('member','restricted')", (chat_id,)
    )
    removed = await DB.scalar(
        "SELECT COUNT(*) FROM logs WHERE chat_id=? AND ok=1", (chat_id,)
    )
    text = (
        "📊 <b>STATISTICS</b>\n\n"
        f"📡 {esc(title)}\n"
        f"👥 Subscribers (Telegram): {fmt_int(subscribers) if subscribers is not None else 'unavailable'}\n"
        f"🛡 Administrators: {fmt_int(admins)}\n"
        f"🗄 Known members recorded: {fmt_int(known)}\n"
        f"✅ Known and active: {fmt_int(active)}\n"
        f"🗑 Successful operations logged: {fmt_int(removed)}\n\n"
        "Only figures Telegram actually returns are shown."
    )
    return text, kb([("🔄 REFRESH", cb("stat", chat_id))], [("◀️ BACK", cb("ch", chat_id))])

async def logs_screen(chat_id: int, page: int) -> Tuple[str, InlineKeyboardMarkup]:
    await LOGS.flush()
    per_page = 8
    total = await DB.scalar("SELECT COUNT(*) FROM logs WHERE chat_id=?", (chat_id,))
    pages = max(1, (int(total) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    rows = await DB.query(
        "SELECT ts, admin_id, op, target_id, ok, detail FROM logs WHERE chat_id=? "
        "ORDER BY id DESC LIMIT ? OFFSET ?",
        (chat_id, per_page, page * per_page),
    )
    if rows:
        lines = []
        for row in rows:
            stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(row["ts"])))
            icon = "✅" if int(row["ok"]) else "❌"
            detail = f" — {esc(row['detail'])}" if row["detail"] else ""
            lines.append(
                f"{icon} <code>{stamp}</code>\n"
                f"   {esc(row['op'])} · target <code>{row['target_id']}</code> · admin <code>{row['admin_id']}</code>{detail}"
            )
        body = "\n".join(lines)
    else:
        body = "No entries yet."
    text = f"📝 <b>LOGS</b> — page {page + 1}/{pages} ({fmt_int(total)} entries)\n\n{body}"

    nav: List[Tuple[str, str]] = []
    if page > 0:
        nav.append(("⬅️ PREV", cb("logs", chat_id, page - 1)))
    if page < pages - 1:
        nav.append(("NEXT ➡️", cb("logs", chat_id, page + 1)))
    rows_kb = [nav] if nav else []
    rows_kb.append([("🧹 CLEAR LOGS", cb("lgcl", chat_id))])
    rows_kb.append([("◀️ BACK", cb("ch", chat_id))])
    return text, kb(*rows_kb)

def members_screen(chat_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    text = (
        "👥 <b>MEMBER MANAGEMENT</b>\n\n"
        "Look a member up by numeric ID, or by @username if that user was "
        "recorded by the bot. Every action is verified against Telegram before "
        "it runs."
    )
    return text, kb(
        [("🔎 SEARCH MEMBER", cb("srch", chat_id))],
        [("⚡ BULK ACTION", cb("cln", chat_id))],
        [("◀️ BACK", cb("ch", chat_id))],
    )

# --------------------------------------------------------------------------- #
# Admin Panel screens                                                          #
# --------------------------------------------------------------------------- #

async def admin_panel_screen(user_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    if user_id != ADMIN_ID:
        return "🔒 Access Denied.", kb([("◀️ BACK", cb("home"))])
    pending = await DB.scalar("SELECT COUNT(*) FROM users WHERE approved=0 AND blocked=0")
    total_users = await DB.scalar("SELECT COUNT(*) FROM users")
    blocked = await DB.scalar("SELECT COUNT(*) FROM users WHERE blocked=1")
    total_cases = await DB.scalar("SELECT COUNT(*) FROM cases")
    text = (
        "👑 <b>ADMIN PANEL</b>\n\n"
        f"👥 Total Users: {fmt_int(total_users)}\n"
        f"⏳ Pending Approvals: {fmt_int(pending)}\n"
        f"🚫 Blocked Users: {fmt_int(blocked)}\n"
        f"📂 Total Cases: {fmt_int(total_cases)}\n"
    )
    return text, kb(
        [("👥 USERS", cb("admin_users"))],
        [("⏳ PENDING", cb("admin_pending"))],
        [("🚫 BLOCKED", cb("admin_blocked"))],
        [("📂 CASES", cb("admin_cases"))],
        [("📊 ANALYTICS", cb("admin_analytics"))],
        [("⚙️ SETTINGS", cb("admin_settings"))],
        [("◀️ BACK", cb("home"))],
    )

async def admin_users_screen(page: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    per_page = 10
    offset = page * per_page
    rows = await DB.query(
        "SELECT user_id, username, full_name, approved, blocked FROM users ORDER BY registered_at DESC LIMIT ? OFFSET ?",
        (per_page, offset)
    )
    total = await DB.scalar("SELECT COUNT(*) FROM users")
    pages = max(1, (int(total) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))

    lines = []
    for row in rows:
        status = "✅ Approved" if row["approved"] else ("🚫 Blocked" if row["blocked"] else "⏳ Pending")
        lines.append(f"• <code>{row['user_id']}</code> {esc(row['full_name'] or '')} — {status}")
    body = "\n".join(lines) or "No users."
    text = f"👥 <b>USERS</b> — page {page+1}/{pages}\n\n{body}"
    nav = []
    if page > 0:
        nav.append(("⬅️ PREV", cb("admin_users", page-1)))
    if page < pages - 1:
        nav.append(("NEXT ➡️", cb("admin_users", page+1)))
    buttons = []
    if nav:
        buttons.append(nav)
    buttons.append([("◀️ BACK", cb("admin_panel"))])
    return text, kb(*buttons)

async def admin_pending_screen() -> Tuple[str, InlineKeyboardMarkup]:
    rows = await DB.query("SELECT user_id, username, full_name FROM users WHERE approved=0 AND blocked=0 ORDER BY registered_at ASC LIMIT 20")
    if not rows:
        text = "⏳ No pending users."
        return text, kb([("◀️ BACK", cb("admin_panel"))])
    lines = []
    for row in rows:
        lines.append(f"• <code>{row['user_id']}</code> {esc(row['full_name'] or '')} (@{row['username'] or 'no_username'})")
    body = "\n".join(lines)
    text = f"⏳ <b>PENDING USERS</b>\n\n{body}"
    buttons = []
    for row in rows:
        uid = row['user_id']
        buttons.append([(f"✅ Approve {uid}", cb("admin_approve", uid)), (f"🚫 Block {uid}", cb("admin_block", uid))])
    buttons.append([("◀️ BACK", cb("admin_panel"))])
    return text, kb(*buttons)

async def admin_blocked_screen() -> Tuple[str, InlineKeyboardMarkup]:
    rows = await DB.query("SELECT user_id, username, full_name FROM users WHERE blocked=1 ORDER BY registered_at DESC LIMIT 20")
    if not rows:
        text = "🚫 No blocked users."
        return text, kb([("◀️ BACK", cb("admin_panel"))])
    lines = []
    for row in rows:
        lines.append(f"• <code>{row['user_id']}</code> {esc(row['full_name'] or '')} (@{row['username'] or 'no_username'})")
    body = "\n".join(lines)
    text = f"🚫 <b>BLOCKED USERS</b>\n\n{body}"
    buttons = []
    for row in rows:
        uid = row['user_id']
        buttons.append([(f"♻️ Unblock {uid}", cb("admin_unblock", uid))])
    buttons.append([("◀️ BACK", cb("admin_panel"))])
    return text, kb(*buttons)

async def admin_analytics_screen() -> Tuple[str, InlineKeyboardMarkup]:
    total_users = await DB.scalar("SELECT COUNT(*) FROM users")
    approved = await DB.scalar("SELECT COUNT(*) FROM users WHERE approved=1")
    pending = await DB.scalar("SELECT COUNT(*) FROM users WHERE approved=0 AND blocked=0")
    blocked = await DB.scalar("SELECT COUNT(*) FROM users WHERE blocked=1")
    total_cases = await DB.scalar("SELECT COUNT(*) FROM cases")
    active_cases = await DB.scalar("SELECT COUNT(*) FROM cases WHERE status NOT IN ('closed','rejected')")
    total_ops = await DB.scalar("SELECT COUNT(*) FROM logs")
    success_ops = await DB.scalar("SELECT COUNT(*) FROM logs WHERE ok=1")
    text = (
        "📊 <b>ANALYTICS</b>\n\n"
        f"👥 Users: {fmt_int(total_users)}\n"
        f"   ✅ Approved: {fmt_int(approved)}\n"
        f"   ⏳ Pending: {fmt_int(pending)}\n"
        f"   🚫 Blocked: {fmt_int(blocked)}\n"
        f"📂 Cases: {fmt_int(total_cases)} (active: {fmt_int(active_cases)})\n"
        f"📝 Operations: {fmt_int(total_ops)} (success: {fmt_int(success_ops)})\n"
    )
    return text, kb([("◀️ BACK", cb("admin_panel"))])

async def admin_settings_screen() -> Tuple[str, InlineKeyboardMarkup]:
    text = "⚙️ <b>ADMIN SETTINGS</b>\n\nSet bot-wide preferences."
    return text, kb(
        [("🌐 Language Default", cb("admin_lang"))],
        [("◀️ BACK", cb("admin_panel"))],
    )

# --------------------------------------------------------------------------- #
# Case Management Screens                                                      #
# --------------------------------------------------------------------------- #

async def cases_screen(user_id: int, page: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    per_page = 8
    offset = page * per_page
    rows = await DB.query(
        "SELECT id, platform, issue_type, status, updated_at FROM cases WHERE user_id=? ORDER BY updated_at DESC LIMIT ? OFFSET ?",
        (user_id, per_page, offset)
    )
    total = await DB.scalar("SELECT COUNT(*) FROM cases WHERE user_id=?", (user_id,))
    pages = max(1, (int(total) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    if rows:
        lines = []
        for row in rows:
            status_emoji = {"draft": "🟡", "ready": "🔵", "submitted": "🟢", "waiting": "⏳", "resolved": "✅", "rejected": "❌", "closed": "🔴"}.get(row["status"], "⚪")
            lines.append(f"{status_emoji} <b>#{row['id']}</b> {esc(row['platform'])} — {esc(row['issue_type'])} (updated: {time.strftime('%Y-%m-%d', time.localtime(row['updated_at']))})")
        body = "\n".join(lines)
    else:
        body = "No cases yet."
    text = f"📂 <b>YOUR CASES</b> — page {page+1}/{pages}\n\n{body}"
    nav = []
    if page > 0:
        nav.append(("⬅️ PREV", cb("cases", page-1)))
    if page < pages - 1:
        nav.append(("NEXT ➡️", cb("cases", page+1)))
    buttons = []
    if nav:
        buttons.append(nav)
    buttons.append([("🆕 NEW CASE", cb("new_case")), ("◀️ BACK", cb("home"))])
    return text, kb(*buttons)

async def new_case_screen() -> Tuple[str, InlineKeyboardMarkup]:
    text = "🆕 <b>NEW CASE</b>\n\nSelect the platform:"
    return text, kb(
        [("📸 Instagram", cb("case_platform", "instagram"))],
        [("💬 WhatsApp", cb("case_platform", "whatsapp"))],
        [("✈️ Telegram", cb("case_platform", "telegram"))],
        [("◀️ BACK", cb("cases", 0))],
    )

async def case_platform_screen(platform: str) -> Tuple[str, InlineKeyboardMarkup]:
    issues = {
        "instagram": ["Disabled", "Hacked", "Login Issue", "Content Issue"],
        "whatsapp": ["Banned", "Business Issue", "Access Issue", "Review"],
        "telegram": ["Spam Limit", "Account Access", "Channel Issue", "Group Issue", "Other"],
    }
    buttons = []
    for issue in issues.get(platform, []):
        buttons.append([(issue, cb("case_issue", platform, issue.lower().replace(" ", "_")))])
    buttons.append([("◀️ BACK", cb("new_case"))])
    text = f"📸 <b>{platform.capitalize()}</b>\n\nSelect the issue type:"
    return text, kb(*buttons)

# --------------------------------------------------------------------------- #
# Guard function for channel actions                                           #
# --------------------------------------------------------------------------- #

async def guard(update: Update, chat_id: int) -> bool:
    user = update.effective_user
    query = update.callback_query
    if user is None:
        return False
    if user.id != ADMIN_ID and not await is_user_approved(user.id):
        if query:
            await query.answer("Not authorized.", show_alert=True)
        return False
    exists = await DB.query_one("SELECT chat_id FROM channels WHERE chat_id=?", (chat_id,))
    if exists is None:
        if query:
            await query.answer("This channel is no longer connected.", show_alert=True)
        return False
    if not await is_authorized(update.get_bot(), chat_id, user.id):
        if query:
            await query.answer("You are not an administrator of this channel.", show_alert=True)
        return False
    return True

async def show(update: Update, text: str, markup: InlineKeyboardMarkup) -> None:
    query = update.callback_query
    if query is not None:
        with contextlib.suppress(BadRequest):
            await query.edit_message_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        return
    if update.effective_message is not None:
        await update.effective_message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)

# --------------------------------------------------------------------------- #
# Commands                                                                     #
# --------------------------------------------------------------------------- #

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if user is None:
        return
    await register_user(user.id, user.username or "", user.full_name or "")
    if user.id == ADMIN_ID or await is_user_approved(user.id):
        if user.id == ADMIN_ID:
            await DB.execute("UPDATE users SET approved=1 WHERE user_id=?", (user.id,))
        context.user_data.pop("await", None)
        text, markup = await home_screen(context.bot, user.id)
        await update.effective_message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
    else:
        await DB.execute("UPDATE users SET approved=0, blocked=0 WHERE user_id=?", (user.id,))
        text = await get_text(user.id, "not_authorized")
        await update.effective_message.reply_text(text)
        if ADMIN_ID:
            try:
                admin_text = (
                    f"╭━━〔 🔔 ACCESS REQUEST 〕━━╮\n"
                    f"┃\n"
                    f"┃ 👤 Name: {esc(user.full_name or '')}\n"
                    f"┃ 🔗 Username: @{user.username or 'no_username'}\n"
                    f"┃ 🆔 User ID: <code>{user.id}</code>\n"
                    f"┃ 🕐 Time: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                    "╰━━━━━━━━━━━━━━━━━━━━━━━╯"
                )
                await context.bot.send_message(
                    ADMIN_ID,
                    admin_text,
                    reply_markup=kb(
                        [("✅ APPROVE", cb("admin_approve", user.id))],
                        [("❌ REJECT", cb("admin_reject", user.id))],
                        [("🚫 BLOCK", cb("admin_block", user.id))],
                    ),
                    parse_mode=ParseMode.HTML,
                )
            except TelegramError:
                pass
        await DB.execute(
            "INSERT INTO notifications (user_id, type, content, created_at) VALUES (?,?,?,?)",
            (ADMIN_ID, "access_request", f"User {user.id} requested access.", time.time())
        )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        "<b>Commands</b>\n"
        "/start — dashboard\n"
        "/channels — refresh connected channels\n"
        "/cancel — cancel the current input\n"
        "/id — show your user ID\n\n"
        + API_LIMITATION,
        parse_mode=ParseMode.HTML,
    )

async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    await update.effective_message.reply_text(f"🆔 Your user ID: <code>{user.id}</code>", parse_mode=ParseMode.HTML)

async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = context.user_data.pop("await", None)
    path = context.user_data.pop("targets_path", None)
    if path:
        with contextlib.suppress(OSError):
            os.remove(path)
    await update.effective_message.reply_text("❎ Cancelled." if state else "Nothing to cancel.")

async def cmd_channels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_start(update, context)

# --------------------------------------------------------------------------- #
# Channel detection handlers                                                   #
# --------------------------------------------------------------------------- #

async def on_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    event = update.my_chat_member
    if event is None or str(event.chat.type) != "channel":
        return
    chat = event.chat
    status = str(event.new_chat_member.status)
    actor = event.from_user.id if event.from_user else 0

    if status in ("left", "kicked"):
        await DB.execute("DELETE FROM channels WHERE chat_id=?", (chat.id,))
        _admin_cache.pop(chat.id, None)
        job = JOBS.get(chat.id)
        if job:
            job.request_stop(by_user=False)
        log.info("removed from channel %s", chat.id)
        return

    if status != "administrator":
        return

    is_admin, can_restrict = await bot_rights(context.bot, chat.id)
    await DB.execute(
        "INSERT INTO channels (chat_id, title, username, registered_by, registered_at) VALUES (?,?,?,?,?) "
        "ON CONFLICT(chat_id) DO UPDATE SET title=excluded.title, username=excluded.username",
        (chat.id, chat.title or "", chat.username, actor, time.time()),
    )
    await channel_admin_ids(context.bot, chat.id, force=True)

    text = (
        "🟢 <b>CHANNEL CONNECTED</b>\n\n"
        f"📡 {esc(chat.title or chat.id)}\n"
        f"🆔 <code>{chat.id}</code>\n\n"
        f"🛡 Administrator: {'YES' if is_admin else 'NO'}\n"
        f"🗑 Ban-users permission: {'YES' if can_restrict else 'NO'}\n\n"
        + ("" if can_restrict else "Grant the <b>Ban users</b> permission to enable member removal.\n\n")
        + API_LIMITATION
    )
    markup = kb([("⚙️ MANAGE", cb("ch", chat.id))])
    if actor:
        with contextlib.suppress(TelegramError):
            await context.bot.send_message(actor, text, reply_markup=markup, parse_mode=ParseMode.HTML)

async def on_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    event = update.chat_member
    if event is None or str(event.chat.type) != "channel":
        return
    known = await DB.query_one("SELECT chat_id FROM channels WHERE chat_id=?", (event.chat.id,))
    if known is None:
        return
    member = event.new_chat_member
    user = member.user
    if user.is_bot:
        return
    full_name = (user.full_name or "")[:64]
    await DB.execute(
        "INSERT INTO members (chat_id, user_id, username, full_name, status, updated_at) VALUES (?,?,?,?,?,?) "
        "ON CONFLICT(chat_id, user_id) DO UPDATE SET username=excluded.username, "
        "full_name=excluded.full_name, status=excluded.status, updated_at=excluded.updated_at",
        (event.chat.id, user.id, (user.username or "").lower() or None, full_name, str(member.status), time.time()),
    )

# --------------------------------------------------------------------------- #
# Member lookup                                                                #
# --------------------------------------------------------------------------- #

async def member_card(bot, chat_id: int, user_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except BadRequest as exc:
        return (
            f"❌ <b>NOT FOUND</b>\n\nTelegram could not return that user for this channel.\n<code>{esc(exc)}</code>",
            kb([("◀️ BACK", cb("mem", chat_id))]),
        )
    except Forbidden:
        return (
            "❌ <b>NO ACCESS</b>\n\nThe bot cannot read members of this channel.",
            kb([("◀️ BACK", cb("mem", chat_id))]),
        )
    except TelegramError as exc:
        return (
            f"⚠️ <b>TELEGRAM ERROR</b>\n\n<code>{esc(exc)}</code>",
            kb([("◀️ BACK", cb("mem", chat_id))]),
        )

    user = member.user
    status = str(member.status)
    labels = {
        "creator": "Owner",
        "administrator": "Administrator",
        "member": "Member",
        "restricted": "Restricted",
        "left": "Not a member",
        "kicked": "Banned",
    }
    text = (
        "👤 <b>MEMBER</b>\n\n"
        f"Name: {esc(user.full_name)}\n"
        f"Username: {('@' + user.username) if user.username else '—'}\n"
        f"ID: <code>{user.id}</code>\n"
        f"Status: {labels.get(status, status)}\n"
    )
    rows: List[List[Tuple[str, str]]] = []
    if status in ("member", "restricted"):
        rows.append([("🗑 REMOVE", cb("mrm", chat_id, user.id, "remove"))])
        rows.append([("⛔ BAN", cb("mrm", chat_id, user.id, "ban"))])
    elif status == "kicked":
        rows.append([("♻️ UNBAN", cb("mrm", chat_id, user.id, "unban"))])
    else:
        text += "\nNo removal action is available for this status."
    rows.append([("◀️ BACK", cb("mem", chat_id))])
    return text, kb(*rows)

async def resolve_username(chat_id: int, username: str) -> Optional[int]:
    row = await DB.query_one(
        "SELECT user_id FROM members WHERE chat_id=? AND username=? LIMIT 1",
        (chat_id, username.lower().lstrip("@")),
    )
    return int(row["user_id"]) if row else None

# --------------------------------------------------------------------------- #
# Operation launch                                                             #
# --------------------------------------------------------------------------- #

def pending_key(chat_id: int) -> str:
    return f"pending:{chat_id}"

async def launch(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    query = update.callback_query
    user = update.effective_user
    pending = context.user_data.get(pending_key(chat_id))
    if not pending:
        await query.answer("Nothing prepared — choose a source first.", show_alert=True)
        return
    if chat_id in JOBS:
        await query.answer("An operation is already running for this channel.", show_alert=True)
        return

    is_admin, can_restrict = await bot_rights(context.bot, chat_id)
    if not (is_admin and can_restrict):
        await show(
            update,
            "❌ <b>MISSING PERMISSION</b>\n\nThe bot needs administrator rights with the "
            "<b>Ban users</b> permission in this channel.",
            kb([("◀️ BACK", cb("ch", chat_id))]),
        )
        return

    cfg = await get_settings(chat_id)
    row = await DB.query_one("SELECT title FROM channels WHERE chat_id=?", (chat_id,))
    title = (row["title"] if row else "") or str(chat_id)

    operation = Operation(
        context.application,
        chat_id=chat_id,
        title=title,
        admin_id=user.id,
        dm_chat_id=update.effective_chat.id,
        source=pending["source"],
        source_path=pending.get("path"),
        action=cfg["action"],
        workers=int(cfg["workers"]),
        interval=float(cfg["interval"]),
        mode=str(cfg["mode"]),
        verify=True,
        total=int(pending.get("total") or 0),
    )
    context.user_data.pop(pending_key(chat_id), None)
    JOBS[chat_id] = operation

    await query.edit_message_text(
        operation.render_progress(),
        reply_markup=operation.progress_keyboard(),
        parse_mode=ParseMode.HTML,
    )
    operation.start(query.message.message_id)
    LOGS.add(chat_id, user.id, f"{cfg['action']}:start", 0, True, pending["source"])

async def prepare_db_source(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int) -> None:
    total = int(
        await DB.scalar(
            "SELECT COUNT(*) FROM members WHERE chat_id=? AND status IN ('member','restricted')",
            (chat_id,),
        )
    )
    if total == 0:
        await show(
            update,
            "🗄 <b>NO RECORDED MEMBERS</b>\n\n"
            "No membership updates have been received for this channel yet. Telegram only "
            "reports joins and leaves that happen while the bot is administrator.\n\n"
            "Use an ID list instead.\n\n" + API_LIMITATION,
            kb([("📄 ID LIST", cb("src", chat_id, "list"))], [("◀️ BACK", cb("cln", chat_id))]),
        )
        return
    cfg = await get_settings(chat_id)
    row = await DB.query_one("SELECT title FROM channels WHERE chat_id=?", (chat_id,))
    title = (row["title"] if row else "") or str(chat_id)
    context.user_data[pending_key(chat_id)] = {"source": "db", "path": None, "total": total}
    text, markup = confirm_screen(chat_id, title, total, cfg["action"], "db")
    await show(update, text, markup)

def count_ids_in_file(path: str) -> int:
    count = 0
    with open(path, "r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            count += sum(1 for _ in parse_ids(line))
    return count

def write_ids_file(path: str, text: str) -> int:
    count = 0
    with open(path, "w", encoding="utf-8") as handle:
        for uid in parse_ids(text):
            handle.write(f"{uid}\n")
            count += 1
    return count

async def prepared_list(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int, path: str, total: int) -> None:
    if total == 0:
        with contextlib.suppress(OSError):
            os.remove(path)
        await update.effective_message.reply_text("❌ No valid user IDs found. Send numeric IDs, one per line.")
        return
    cfg = await get_settings(chat_id)
    row = await DB.query_one("SELECT title FROM channels WHERE chat_id=?", (chat_id,))
    title = (row["title"] if row else "") or str(chat_id)
    previous = context.user_data.get(pending_key(chat_id))
    if previous and previous.get("path") and previous["path"] != path:
        with contextlib.suppress(OSError):
            os.remove(previous["path"])
    context.user_data[pending_key(chat_id)] = {"source": "list", "path": path, "total": total}
    context.user_data.pop("await", None)
    text, markup = confirm_screen(chat_id, title, total, cfg["action"], "list")
    await update.effective_message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)

# --------------------------------------------------------------------------- #
# Input handlers                                                               #
# --------------------------------------------------------------------------- #

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.effective_message
    if user is None or message is None:
        return
    if user.id != ADMIN_ID and not await is_user_approved(user.id):
        await message.reply_text("🔒 You are not authorized.")
        return

    state = context.user_data.get("await")
    if not state:
        if context.user_data.get("case_platform"):
            platform = context.user_data["case_platform"]
            issue = context.user_data["case_issue"]
            description = message.text
            await DB.execute(
                "INSERT INTO cases (user_id, platform, issue_type, description, status, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
                (user.id, platform, issue, description, "draft", time.time(), time.time())
            )
            case_id = await DB.scalar("SELECT last_insert_rowid()")
            context.user_data.pop("case_platform", None)
            context.user_data.pop("case_issue", None)
            context.user_data.pop("await", None)
            await message.reply_text(
                f"✅ Case #{case_id} created. You can now generate an appeal.",
                reply_markup=kb([("📝 GENERATE APPEAL", cb("gen_appeal", case_id))], [("📂 VIEW CASE", cb("view_case", case_id))], [("◀️ BACK", cb("cases", 0))])
            )
            return
        return

    kind, chat_id = state[0], int(state[1])
    if not await guard(update, chat_id):
        context.user_data.pop("await", None)
        await message.reply_text("⛔ Not authorized for that channel.")
        return

    if kind == "search":
        context.user_data.pop("await", None)
        raw = (message.text or "").strip()
        target: Optional[int] = None
        if raw.lstrip("-").isdigit():
            target = int(raw)
        else:
            target = await resolve_username(chat_id, raw)
        if target is None:
            await message.reply_text(
                "❌ Unknown username. The Bot API cannot resolve arbitrary usernames — "
                "send a numeric user ID, or use a username the bot has already recorded.",
                reply_markup=kb([("◀️ BACK", cb("mem", chat_id))]),
            )
            return
        text, markup = await member_card(context.bot, chat_id, target)
        await message.reply_text(text, reply_markup=markup, parse_mode=ParseMode.HTML)
        return

    if kind == "targets":
        os.makedirs(TMP_DIR, exist_ok=True)
        path = os.path.join(TMP_DIR, f"targets_{user.id}_{int(time.time())}.txt")
        total = await asyncio.to_thread(write_ids_file, path, message.text or "")
        await prepared_list(update, context, chat_id, path, total)

async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.effective_message
    if user is None or message is None or (user.id != ADMIN_ID and not await is_user_approved(user.id)):
        return
    state = context.user_data.get("await")
    if not state or state[0] != "targets":
        return
    chat_id = int(state[1])
    if not await guard(update, chat_id):
        context.user_data.pop("await", None)
        return
    document = message.document
    if document is None:
        return
    if (document.file_size or 0) > 18 * 1024 * 1024:
        await message.reply_text("❌ File too large. The Bot API allows downloads up to 20 MB.")
        return

    os.makedirs(TMP_DIR, exist_ok=True)
    path = os.path.join(TMP_DIR, f"targets_{user.id}_{int(time.time())}.txt")
    try:
        telegram_file = await document.get_file()
        await telegram_file.download_to_drive(path)
    except TelegramError as exc:
        await message.reply_text(f"❌ Download failed: <code>{esc(exc)}</code>", parse_mode=ParseMode.HTML)
        return
    total = await asyncio.to_thread(count_ids_in_file, path)
    await prepared_list(update, context, chat_id, path, total)

# --------------------------------------------------------------------------- #
# Callback router                                                              #
# --------------------------------------------------------------------------- #

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if query is None or user is None:
        return
    parts = cb_parse(query.data or "")
    if parts is None:
        await query.answer("Invalid or expired button.", show_alert=True)
        return
    if user.id != ADMIN_ID and not await is_user_approved(user.id):
        await query.answer("Not authorized.", show_alert=True)
        return

    action, args = parts[0], parts[1:]
    if action == "nop":
        await query.answer()
        return

    # Admin actions
    if action in ("admin_panel", "admin_users", "admin_pending", "admin_blocked", "admin_analytics", "admin_settings"):
        await query.answer()
        if user.id != ADMIN_ID:
            await query.answer("Admin only.", show_alert=True)
            return
        if action == "admin_panel":
            text, markup = await admin_panel_screen(user.id)
            await show(update, text, markup)
        elif action == "admin_users":
            page = int(args[0]) if args and args[0].isdigit() else 0
            text, markup = await admin_users_screen(page)
            await show(update, text, markup)
        elif action == "admin_pending":
            text, markup = await admin_pending_screen()
            await show(update, text, markup)
        elif action == "admin_blocked":
            text, markup = await admin_blocked_screen()
            await show(update, text, markup)
        elif action == "admin_analytics":
            text, markup = await admin_analytics_screen()
            await show(update, text, markup)
        elif action == "admin_settings":
            text, markup = await admin_settings_screen()
            await show(update, text, markup)
        return

    if action in ("admin_approve", "admin_reject", "admin_block", "admin_unblock") and len(args) > 0:
        await query.answer()
        if user.id != ADMIN_ID:
            await query.answer("Admin only.", show_alert=True)
            return
        target = int(args[0])
        if action == "admin_approve":
            await DB.execute("UPDATE users SET approved=1, blocked=0 WHERE user_id=?", (target,))
            await DB.execute("INSERT INTO user_actions (admin_id, target_id, action, created_at) VALUES (?,?,?,?)",
                             (user.id, target, "approve", time.time()))
            try:
                await context.bot.send_message(target, "✅ Your access request has been approved. You can now use the bot.\n/start")
            except TelegramError:
                pass
            await query.answer("User approved.")
        elif action == "admin_reject":
            await DB.execute("DELETE FROM users WHERE user_id=?", (target,))
            await DB.execute("INSERT INTO user_actions (admin_id, target_id, action, created_at) VALUES (?,?,?,?)",
                             (user.id, target, "reject", time.time()))
            await query.answer("User rejected and removed.")
        elif action == "admin_block":
            await DB.execute("UPDATE users SET blocked=1, approved=0 WHERE user_id=?", (target,))
            await DB.execute("INSERT INTO user_actions (admin_id, target_id, action, created_at) VALUES (?,?,?,?)",
                             (user.id, target, "block", time.time()))
            await query.answer("User blocked.")
        elif action == "admin_unblock":
            await DB.execute("UPDATE users SET blocked=0 WHERE user_id=?", (target,))
            await DB.execute("INSERT INTO user_actions (admin_id, target_id, action, created_at) VALUES (?,?,?,?)",
                             (user.id, target, "unblock", time.time()))
            await query.answer("User unblocked.")
        text, markup = await admin_panel_screen(user.id)
        await show(update, text, markup)
        return

    # Home
    if action == "home":
        await query.answer()
        text, markup = await home_screen(context.bot, user.id)
        await show(update, text, markup)
        return

    # Channel actions
    if not args or not args[0].lstrip("-").isdigit():
        await query.answer("Malformed action.", show_alert=True)
        return
    chat_id = int(args[0])
    if not await guard(update, chat_id):
        return
    await query.answer()

    if action == "ch":
        text, markup = await channel_screen(context.bot, chat_id)
        await show(update, text, markup)
    elif action == "mem":
        text, markup = members_screen(chat_id)
        await show(update, text, markup)
    elif action == "srch":
        context.user_data["await"] = ("search", chat_id)
        await show(
            update,
            "🔎 <b>SEARCH MEMBER</b>\n\nSend a numeric user ID, or an @username the bot has "
            "already recorded.\n\n/cancel to abort.",
            kb([("◀️ BACK", cb("mem", chat_id))]),
        )
    elif action == "stat":
        text, markup = await stats_screen(context.bot, chat_id)
        await show(update, text, markup)
    elif action == "logs":
        page = int(args[1]) if len(args) > 1 and args[1].isdigit() else 0
        text, markup = await logs_screen(chat_id, page)
        await show(update, text, markup)
    elif action == "lgcl":
        await show(
            update,
            "🧹 <b>CLEAR LOGS</b>\n\nDelete every stored log entry for this channel?",
            kb([("✅ CLEAR", cb("lgcy", chat_id))], [("❌ CANCEL", cb("logs", chat_id, 0))]),
        )
    elif action == "lgcy":
        await LOGS.flush()
        await DB.execute("DELETE FROM logs WHERE chat_id=?", (chat_id,))
        text, markup = await logs_screen(chat_id, 0)
        await show(update, text, markup)
    elif action == "set":
        text, markup = await settings_screen(chat_id)
        await show(update, text, markup)
    elif action in ("sw", "si", "sm", "sa") and len(args) > 1:
        value = args[1]
        if action == "sw":
            await save_setting(chat_id, "workers", max(0, min(MAX_WORKERS, int(value))) if value.isdigit() else 0)
        elif action == "si":
            await save_setting(chat_id, "interval", float(value) if value.replace(".", "", 1).isdigit() else 2.0)
        elif action == "sm":
            await save_setting(chat_id, "mode", "fast" if value == "fast" else "safe")
        else:
            await save_setting(chat_id, "action", "ban" if value == "ban" else "remove")
        text, markup = await settings_screen(chat_id)
        await show(update, text, markup)
    elif action == "cln":
        text, markup = await cleaner_screen(context.bot, chat_id)
        await show(update, text, markup)
    elif action == "src" and len(args) > 1:
        if args[1] == "db":
            await prepare_db_source(update, context, chat_id)
        else:
            context.user_data["await"] = ("targets", chat_id)
            await show(
                update,
                "📄 <b>ID LIST</b>\n\nSend the target user IDs — paste them (one per line or "
                "comma separated) or upload a .txt file.\n\nOnly numeric Telegram user IDs are "
                "accepted; the Bot API cannot resolve usernames of users it has never seen.\n\n"
                "/cancel to abort.",
                kb([("◀️ BACK", cb("cln", chat_id))]),
            )
    elif action == "go":
        await launch(update, context, chat_id)
    elif action == "stp":
        job = JOBS.get(chat_id)
        if job is None:
            await show(update, "No operation is running.", kb([("◀️ DASHBOARD", cb("ch", chat_id))]))
        else:
            job.request_stop(by_user=True)
            with contextlib.suppress(BadRequest):
                await query.edit_message_text(
                    "⛔ <b>STOPPING</b>\n\nCancelling pending tasks and clearing the queue…",
                    parse_mode=ParseMode.HTML,
                )
    elif action == "det":
        job = REPORTS.get(chat_id) or JOBS.get(chat_id)
        if job is None:
            await show(update, "No report available yet.", kb([("◀️ DASHBOARD", cb("ch", chat_id))]))
        else:
            await show(update, job.render_details(), kb([("◀️ DASHBOARD", cb("ch", chat_id))]))
    elif action == "rep":
        job = REPORTS.get(chat_id)
        if job is None:
            await show(update, "No report available yet.", kb([("◀️ DASHBOARD", cb("ch", chat_id))]))
        else:
            await show(
                update,
                job.render_report(),
                kb([("📋 DETAILS", cb("det", chat_id))], [("◀️ DASHBOARD", cb("ch", chat_id))]),
            )
    elif action == "mrm" and len(args) > 2:
        target, mode = args[1], args[2]
        verbs = {"remove": "remove (kick)", "ban": "ban", "unban": "unban"}
        await show(
            update,
            f"⚠️ <b>CONFIRM</b>\n\nAction: {esc(verbs.get(mode, mode))}\nUser: <code>{esc(target)}</code>",
            kb(
                [("✅ CONFIRM", cb("mrmy", chat_id, target, mode))],
                [("❌ CANCEL", cb("mem", chat_id))],
            ),
        )
    elif action == "mrmy" and len(args) > 2:
        target_raw, mode = args[1], args[2]
        if not target_raw.lstrip("-").isdigit():
            await show(update, "Malformed target.", kb([("◀️ BACK", cb("mem", chat_id))]))
            return
        target = int(target_raw)
        is_admin, can_restrict = await bot_rights(context.bot, chat_id)
        if not can_restrict:
            await show(
                update,
                "❌ <b>MISSING PERMISSION</b>\n\nThe bot needs the <b>Ban users</b> permission.",
                kb([("◀️ BACK", cb("ch", chat_id))]),
            )
            return
        try:
            if mode == "unban":
                await context.bot.unban_chat_member(chat_id, target, only_if_banned=True)
            else:
                await context.bot.ban_chat_member(chat_id, target)
                if mode == "remove":
                    await context.bot.unban_chat_member(chat_id, target, only_if_banned=True)
            LOGS.add(chat_id, user.id, mode, target, True, "single")
            await DB.execute(
                "UPDATE members SET status=?, updated_at=? WHERE chat_id=? AND user_id=?",
                ("kicked" if mode == "ban" else "left", time.time(), chat_id, target),
            )
            await show(
                update,
                f"✅ <b>DONE</b>\n\nUser <code>{target}</code> — {esc(mode)} succeeded.",
                kb([("👥 MEMBERS", cb("mem", chat_id))], [("◀️ DASHBOARD", cb("ch", chat_id))]),
            )
        except TelegramError as exc:
            LOGS.add(chat_id, user.id, mode, target, False, str(exc))
            await show(
                update,
                f"❌ <b>FAILED</b>\n\n<code>{esc(exc)}</code>",
                kb([("◀️ BACK", cb("mem", chat_id))]),
            )
    else:
        # Case management callbacks
        if action == "cases":
            page = int(args[0]) if args and args[0].isdigit() else 0
            text, markup = await cases_screen(user.id, page)
            await show(update, text, markup)
        elif action == "new_case":
            text, markup = await new_case_screen()
            await show(update, text, markup)
        elif action == "case_platform" and len(args) > 0:
            platform = args[0]
            text, markup = await case_platform_screen(platform)
            await show(update, text, markup)
        elif action == "case_issue" and len(args) > 1:
            platform = args[0]
            issue = args[1]
            context.user_data["case_platform"] = platform
            context.user_data["case_issue"] = issue
            context.user_data["await"] = ("case_description", 0)
            await show(
                update,
                f"📝 <b>DESCRIBE YOUR ISSUE</b>\n\nPlease describe your {platform} issue in detail. "
                "This will be used to generate your appeal.\n\n/cancel to abort.",
                kb([("◀️ BACK", cb("new_case"))]),
            )
        elif action == "gen_appeal" and len(args) > 0:
            case_id = int(args[0])
            row = await DB.query_one("SELECT platform, issue_type, description, user_id FROM cases WHERE id=?", (case_id,))
            if not row or row["user_id"] != user.id:
                await query.answer("Case not found.", show_alert=True)
                return
            user_row = await DB.query_one("SELECT username, full_name FROM users WHERE user_id=?", (user.id,))
            user_details = {"username": user_row["username"] if user_row else "", "full_name": user_row["full_name"] if user_row else ""}
            appeal = await generate_appeal(row["platform"], row["issue_type"], row["description"], user_details)
            await DB.execute("UPDATE cases SET appeal_text=?, status='ready', updated_at=? WHERE id=?", (appeal, time.time(), case_id))
            await DB.execute("INSERT INTO appeals (case_id, platform, text, generated_at) VALUES (?,?,?,?)",
                             (case_id, row["platform"], appeal, time.time()))
            await show(
                update,
                f"📝 <b>APPEAL GENERATED</b>\n\n{appeal}\n\nYou can copy this text and submit it via the official support channel.",
                kb([("📋 COPY", cb("copy_appeal", case_id))],
                   [("📎 ADD EVIDENCE", cb("add_evidence", case_id))],
                   [("📂 VIEW CASE", cb("view_case", case_id))],
                   [("◀️ BACK", cb("cases", 0))])
            )
        elif action == "view_case" and len(args) > 0:
            case_id = int(args[0])
            row = await DB.query_one("SELECT * FROM cases WHERE id=? AND user_id=?", (case_id, user.id))
            if not row:
                await query.answer("Case not found.", show_alert=True)
                return
            evidence_rows = await DB.query("SELECT type, content FROM evidence WHERE case_id=?", (case_id,))
            evidence_text = "\n".join([f"• {row['type']}: {row['content'][:50]}" for row in evidence_rows]) or "None"
            status_emoji = {"draft": "🟡", "ready": "🔵", "submitted": "🟢", "waiting": "⏳", "resolved": "✅", "rejected": "❌", "closed": "🔴"}.get(row["status"], "⚪")
            text = (
                f"📂 <b>CASE #{row['id']}</b>\n\n"
                f"Platform: {esc(row['platform'])}\n"
                f"Issue: {esc(row['issue_type'])}\n"
                f"Status: {status_emoji} {esc(row['status'])}\n"
                f"Description: {esc(row['description'][:200])}\n"
                f"Appeal: {esc((row['appeal_text'] or '')[:200])}...\n"
                f"Evidence: {evidence_text}\n"
            )
            buttons = []
            if row["appeal_text"]:
                buttons.append([("📝 COPY APPEAL", cb("copy_appeal", case_id))])
            buttons.append([("📎 ADD EVIDENCE", cb("add_evidence", case_id))])
            buttons.append([("🔄 UPDATE STATUS", cb("update_status", case_id))])
            buttons.append([("◀️ BACK", cb("cases", 0))])
            await show(update, text, kb(*buttons))
        elif action == "copy_appeal" and len(args) > 0:
            case_id = int(args[0])
            row = await DB.query_one("SELECT appeal_text FROM cases WHERE id=? AND user_id=?", (case_id, user.id))
            if not row or not row["appeal_text"]:
                await query.answer("No appeal text.", show_alert=True)
                return
            await query.answer("Appeal copied to clipboard (select text manually).")
            await query.edit_message_text(
                f"<b>APPEAL TEXT</b>\n\n{row['appeal_text']}\n\n(Select and copy this text.)",
                reply_markup=kb([("◀️ BACK", cb("view_case", case_id))]),
                parse_mode=ParseMode.HTML,
            )
        elif action == "add_evidence" and len(args) > 0:
            case_id = int(args[0])
            context.user_data["await"] = ("evidence", case_id)
            await show(
                update,
                "📎 <b>ADD EVIDENCE</b>\n\nSend a text description, screenshot, or file. "
                "You can send multiple items.\n\n/cancel to abort.",
                kb([("◀️ BACK", cb("view_case", case_id))]),
            )
        elif action == "update_status" and len(args) > 0:
            case_id = int(args[0])
            await show(
                update,
                "🔄 <b>UPDATE STATUS</b>\n\nSelect new status:",
                kb(
                    [("🔵 READY", cb("set_status", case_id, "ready"))],
                    [("🟢 SUBMITTED", cb("set_status", case_id, "submitted"))],
                    [("⏳ WAITING", cb("set_status", case_id, "waiting"))],
                    [("✅ RESOLVED", cb("set_status", case_id, "resolved"))],
                    [("❌ REJECTED", cb("set_status", case_id, "rejected"))],
                    [("🔴 CLOSED", cb("set_status", case_id, "closed"))],
                    [("◀️ BACK", cb("view_case", case_id))],
                )
            )
        elif action == "set_status" and len(args) > 1:
            case_id = int(args[0])
            status = args[1]
            await DB.execute("UPDATE cases SET status=?, updated_at=? WHERE id=? AND user_id=?", (status, time.time(), case_id, user.id))
            await query.answer("Status updated.")
            text, markup = await cases_screen(user.id, 0)
            await show(update, text, markup)
        else:
            await show(update, "Unknown action.", kb([("◀️ DASHBOARD", cb("home"))]))


# --------------------------------------------------------------------------- #
# Error handler                                                                #
# --------------------------------------------------------------------------- #

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    error = context.error
    if isinstance(error, (TimedOut, NetworkError)):
        log.warning("network issue: %s", error)
        return
    log.exception("unhandled error: %s", error)

# --------------------------------------------------------------------------- #
# Health endpoint (Render)                                                     #
# --------------------------------------------------------------------------- #

async def _health(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(reader.read(1024), timeout=2.0)
        body = b"ok"
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
            b"Content-Length: " + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body
        )
        with contextlib.suppress(Exception):
            await writer.drain()
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()

# --------------------------------------------------------------------------- #
# Lifecycle                                                                    #
# --------------------------------------------------------------------------- #

async def post_init(app: Application) -> None:
    os.makedirs(TMP_DIR, exist_ok=True)
    await DB.open()
    LOGS.start()
    if PORT.isdigit():
        server = await asyncio.start_server(_health, "0.0.0.0", int(PORT))
        app.bot_data["health_server"] = server
        log.info("health endpoint listening on port %s", PORT)
    with contextlib.suppress(TelegramError):
        await app.bot.set_my_commands(
            [
                ("start", "Open the dashboard"),
                ("channels", "Refresh connected channels"),
                ("cancel", "Cancel the current input"),
                ("id", "Show your user ID"),
                ("help", "Help"),
            ]
        )
    gc.collect()
    gc.freeze()
    me = await app.bot.get_me()
    log.info("started as @%s (id=%s), workers<=%s, queue=%s", me.username, me.id, MAX_WORKERS, QUEUE_SIZE)

async def post_shutdown(app: Application) -> None:
    for job in list(JOBS.values()):
        job.request_stop(by_user=False)
    for job in list(JOBS.values()):
        with contextlib.suppress(Exception):
            await asyncio.wait_for(job.wait(), timeout=10)
    JOBS.clear()
    REPORTS.clear()
    await LOGS.stop()
    await DB.close()
    server = app.bot_data.pop("health_server", None)
    if server is not None:
        server.close()
        with contextlib.suppress(Exception):
            await server.wait_closed()
    gc.collect()
    log.info("shutdown complete")

# --------------------------------------------------------------------------- #
# Main                                                                         #
# --------------------------------------------------------------------------- #

def main() -> None:
    if not BOT_TOKEN:
        print("BOT_TOKEN environment variable is not set.", file=sys.stderr)
        raise SystemExit(1)
    if not ADMIN_ID:
        print("ADMIN_ID environment variable is not set.", file=sys.stderr)
        raise SystemExit(1)
    if sys.version_info < (3, 10):
        print("Python 3.10 or newer is required.", file=sys.stderr)
        raise SystemExit(1)

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(True)
        .connection_pool_size(MAX_WORKERS + 8)
        .pool_timeout(30.0)
        .connect_timeout(20.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .get_updates_read_timeout(40.0)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", cmd_start, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("help", cmd_help, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("channels", cmd_channels, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("cancel", cmd_cancel, filters=filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("id", cmd_id, filters=filters.ChatType.PRIVATE))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(ChatMemberHandler(on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(ChatMemberHandler(on_chat_member, ChatMemberHandler.CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.Document.ALL, on_document))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(on_error)

    app.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
        poll_interval=0.0,
        timeout=30,
    )

if __name__ == "__main__":
    main()
