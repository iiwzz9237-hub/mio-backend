import os
import re
import json
import time
import math
import uuid
import hmac
import secrets
import hashlib

from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from functools import wraps
from urllib.parse import parse_qsl

import psycopg2
from psycopg2.extras import RealDictCursor, Json

from flask import Flask, request, jsonify
from flask_cors import CORS


# ================= CONFIG =================

DATABASE_URL = os.environ.get("DATABASE_URL")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
BOT_USERNAME = (os.environ.get("BOT_USERNAME") or "").lstrip("@")

GAME_ENABLED = (
    os.environ.get("GAME_ENABLED", "true").strip().lower()
    in {"true", "1", "yes", "on"}
)

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL تنظیم نشده است.")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN تنظیم نشده است.")

app = Flask(__name__)
CORS(app)

IRAN_TZ = timezone(timedelta(hours=3, minutes=30))

BALANCE_CAP = 500000
DAILY_AMOUNT = 340000
DAILY_BALANCE_CAP = BALANCE_CAP

NORMAL_TRANSFER_DELAY = 35
BANK_TRANSFER_DELAY = 300

GAME_COOLDOWN = timedelta(hours=48)
GAME_SYMBOLS = ("seven", "grape", "lemon", "blank")

GAME_PAYOUTS = {
    "seven": [0, 100000, 200000, 400000],
    "grape": [0, 30000, 60000, 80000],
    "lemon": [0, 10000, 20000, 30000],
    "blank": [0, 0, 0, 0],
}


# ================= DATABASE =================

@contextmanager
def db_transaction():
    connection = psycopg2.connect(
        DATABASE_URL,
        connect_timeout=10,
    )

    try:
        with connection:
            with connection.cursor(
                cursor_factory=RealDictCursor
            ) as cur:
                yield cur
    finally:
        connection.close()


def init_database():
    with db_transaction() as cur:
        cur.execute(
            "SELECT pg_advisory_xact_lock(%s)",
            (748219306,),
        )

        cur.execute("""
            CREATE TABLE IF NOT EXISTS withdraw_queue (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                target_username TEXT,
                amount BIGINT,
                withdraw_type TEXT,
                status INTEGER DEFAULT 0,
                created_time DOUBLE PRECISION,
                message_id BIGINT,
                completed_time DOUBLE PRECISION
            )
        """)

        cur.execute("""
            ALTER TABLE withdraw_queue
            ADD COLUMN IF NOT EXISTS completed_time DOUBLE PRECISION
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS receipt_request (
                user_id BIGINT PRIMARY KEY,
                request INTEGER DEFAULT 0
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS mio_game_rounds (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                request_id UUID NOT NULL,
                symbols JSONB NOT NULL,
                reward BIGINT NOT NULL CHECK (reward >= 0),
                balance_after BIGINT NOT NULL,
                played_at TIMESTAMPTZ NOT NULL,
                next_play_at TIMESTAMPTZ NOT NULL,
                UNIQUE (user_id, request_id),
                CHECK (next_play_at > played_at)
            )
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS
                mio_game_rounds_user_latest_idx
            ON mio_game_rounds (user_id, id DESC)
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS
                withdraw_queue_user_completed_idx
            ON withdraw_queue (user_id, completed_time)
            WHERE status=2
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS withdraw_transfer_limits (
                method TEXT PRIMARY KEY,
                delay_seconds INTEGER NOT NULL
                    CHECK (delay_seconds > 0),
                next_allowed_at DOUBLE PRECISION NOT NULL DEFAULT 0,
                updated_at DOUBLE PRECISION NOT NULL DEFAULT 0
            )
        """)

        for method, delay in (
            ("normal", NORMAL_TRANSFER_DELAY),
            ("card", BANK_TRANSFER_DELAY),
        ):
            cur.execute("""
                INSERT INTO withdraw_transfer_limits (
                    method,
                    delay_seconds,
                    next_allowed_at,
                    updated_at
                )
                VALUES (%s, %s, 0, 0)
                ON CONFLICT (method)
                DO UPDATE SET
                    delay_seconds=EXCLUDED.delay_seconds,
                    next_allowed_at=GREATEST(
                        withdraw_transfer_limits.next_allowed_at,
                        CASE
                            WHEN withdraw_transfer_limits.updated_at > 0
                            THEN withdraw_transfer_limits.updated_at
                                + EXCLUDED.delay_seconds
                            ELSE 0
                        END
                    )
            """, (method, delay))


@app.errorhandler(psycopg2.Error)
def database_error(error):
    app.logger.exception("Database operation failed")

    return jsonify({
        "ok": False,
        "error": "database_unavailable",
    }), 503


# ================= AUTH =================

def verify_init_data(init_data):
    if not init_data or len(init_data) > 20000:
        return None

    try:
        items = parse_qsl(
            init_data,
            keep_blank_values=True,
            strict_parsing=True,
        )

        pairs = dict(items)

        if len(items) != len(pairs):
            return None

        received_hash = pairs.pop("hash", None)

        if not received_hash:
            return None

        if not re.fullmatch(r"[0-9a-fA-F]{64}", received_hash):
            return None

        data_check_string = "\n".join(
            f"{key}={value}"
            for key, value in sorted(pairs.items())
        )

        secret_key = hmac.new(
            b"WebAppData",
            BOT_TOKEN.encode(),
            hashlib.sha256,
        ).digest()

        computed_hash = hmac.new(
            secret_key,
            data_check_string.encode(),
            hashlib.sha256,
        ).hexdigest()

        if not hmac.compare_digest(
            computed_hash,
            received_hash.lower(),
        ):
            return None

        auth_date = int(pairs.get("auth_date", "0"))
        now = time.time()

        if auth_date <= 0:
            return None

        if now - auth_date > 86400 or auth_date > now + 30:
            return None

        user = json.loads(pairs.get("user", ""))

        if not isinstance(user, dict):
            return None

        user_id = user.get("id")

        if (
            not isinstance(user_id, int)
            or isinstance(user_id, bool)
            or user_id <= 0
        ):
            return None

        return {
            "id": user_id,
            "username": user.get("username"),
            "first_name": user.get("first_name"),
        }

    except (ValueError, TypeError):
        return None


def get_authenticated_user():
    header = request.headers.get("Authorization", "")

    if not header.startswith("tma "):
        return None

    return verify_init_data(header[4:])


def authenticated(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        user = get_authenticated_user()

        if not user:
            return jsonify({
                "ok": False,
                "error": "unauthorized",
            }), 401

        return view(user, *args, **kwargs)

    return wrapped


def json_body():
    data = request.get_json(silent=True)
    return data if isinstance(data, dict) else {}


# ================= HELPERS =================

def database_now(cur):
    cur.execute("SELECT clock_timestamp() AS now")
    return cur.fetchone()["now"]


def get_user_row(cur, user_id, lock=False):
    query = """
        SELECT user_id, balance, daily_mio
        FROM users
        WHERE user_id=%s
    """

    if lock:
        query += " FOR UPDATE"

    cur.execute(query, (user_id,))
    return cur.fetchone()


def user_balance(row):
    return int(row["balance"] or 0)


def to_iran_time_str(timestamp):
    if not timestamp:
        return ""

    return datetime.fromtimestamp(
        float(timestamp),
        tz=IRAN_TZ,
    ).strftime("%Y-%m-%d %H:%M")


def format_queue_duration(seconds):
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    parts = []

    if hours:
        parts.append(f"{hours} ساعت")
    if minutes:
        parts.append(f"{minutes} دقیقه")
    if seconds:
        parts.append(f"{seconds} ثانیه")

    return " و ".join(parts) if parts else "چند لحظه"


# ================= QUEUE =================

def get_pending_withdrawal(cur, user_id):
    cur.execute("""
        SELECT id, user_id, amount, status, withdraw_type
        FROM withdraw_queue
        WHERE user_id=%s AND status IN (0, 1)
        ORDER BY id ASC
        LIMIT 1
    """, (user_id,))

    return cur.fetchone()


def get_pending_withdrawals(cur, user_id):
    cur.execute("""
        SELECT id, user_id, amount, status, withdraw_type
        FROM withdraw_queue
        WHERE user_id=%s AND status IN (0, 1)
        ORDER BY id ASC
    """, (user_id,))

    rows = cur.fetchall()

    return [
        withdrawal_payload(cur, row)
        for row in rows
    ]


def withdrawal_payload(cur, row):
    if not row:
        return {"status": "none"}

    status_code = row["status"]
    is_bank = row["withdraw_type"] == "card"
    method = "card" if is_bank else "normal"

    result = {
        "withdraw_id": row["id"],
        "amount": int(row["amount"] or 0),
        "withdraw_type": row["withdraw_type"],
        "queue_type": method,
        "processing": status_code == 1,
    }

    if status_code == 2:
        result["status"] = "success"
        return result

    if status_code not in (0, 1):
        result["status"] = "failed"
        return result

    if status_code == 1:
        result.update({
            "status": "pending",
            "position": 0,
            "wait_seconds": 0,
            "wait_text": (
                "در حال انجام مراحل بانک میویی"
                if is_bank
                else "در حال پردازش انتقال"
            ),
        })

        return result

    cur.execute("""
        SELECT COUNT(*) AS position
        FROM withdraw_queue
        WHERE
            status=0
            AND id <= %s
            AND (
                CASE
                    WHEN withdraw_type='card' THEN 'card'
                    ELSE 'normal'
                END
            ) = %s
    """, (row["id"], method))

    position = max(1, int(cur.fetchone()["position"]))

    cur.execute("""
        SELECT
            method,
            delay_seconds,
            next_allowed_at,
            EXTRACT(EPOCH FROM clock_timestamp())::double precision
                AS server_now
        FROM withdraw_transfer_limits
        WHERE method IN ('normal', 'card')
    """)

    limits = {
        item["method"]: item
        for item in cur.fetchall()
    }

    own_limit = limits.get(method)
    normal_limit = limits.get("normal")

    delay_seconds = (
        int(own_limit["delay_seconds"])
        if own_limit
        else BANK_TRANSFER_DELAY if is_bank else NORMAL_TRANSFER_DELAY
    )

    if own_limit:
        ready_at = float(own_limit["next_allowed_at"])

        if is_bank and normal_limit:
            ready_at = max(
                ready_at,
                float(normal_limit["next_allowed_at"]),
            )

        cooldown_remaining = max(
            0,
            math.ceil(ready_at - float(own_limit["server_now"])),
        )
    else:
        cooldown_remaining = 0

    wait_seconds = (
        cooldown_remaining
        + (position - 1) * delay_seconds
    )

    if wait_seconds > 0:
        label = "بانک میویی" if is_bank else "انتقال میویی"

        wait_text = (
            f"{label}: حدود "
            f"{format_queue_duration(wait_seconds)} "
            "تا نوبت شروع پردازش"
        )
    else:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM withdraw_queue
                WHERE status=1
            ) AS processing
        """)

        if cur.fetchone()["processing"]:
            wait_text = "در انتظار پایان پردازش درخواست جاری"
        else:
            wait_text = "نوبت شما رسیده؛ در انتظار شروع پردازش"

    result.update({
        "status": "pending",
        "position": position,
        "wait_seconds": wait_seconds,
        "wait_text": wait_text,
    })

    return result


# ================= DAILY WITHDRAW TOTAL / NOTIFICATIONS =================

def get_withdrawn_today(cur, user_id):
    now = database_now(cur).astimezone(IRAN_TZ)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)

    cur.execute("""
        SELECT COALESCE(SUM(amount), 0) AS total
        FROM withdraw_queue
        WHERE
            user_id=%s
            AND status=2
            AND completed_time >= %s
            AND completed_time < %s
    """, (
        user_id,
        day_start.timestamp(),
        day_end.timestamp(),
    ))

    return int(cur.fetchone()["total"])


def get_recent_notifications(cur, user_id):
    cur.execute("""
        SELECT
            id,
            amount,
            status,
            COALESCE(completed_time, created_time) AS event_time
        FROM withdraw_queue
        WHERE user_id=%s AND status IN (2, 3, 4)
        ORDER BY
            COALESCE(completed_time, created_time) DESC NULLS LAST,
            id DESC
        LIMIT 10
    """, (user_id,))

    notifications = []

    for row in cur.fetchall():
        amount = int(row["amount"] or 0)
        success = row["status"] == 2

        notifications.append({
            "id": f"withdraw:{row['id']}:{row['status']}",
            "type": "success" if success else "fail",
            "text": (
                f"🎉 برداشت {amount:,} میو با موفقیت ثبت شد."
                if success
                else f"❌ برداشت {amount:,} میو ناموفق بود."
            ),
            "time": to_iran_time_str(row["event_time"]),
        })

    return notifications


# ================= GAME =================

def calculate_game_reward(symbols):
    counts = Counter(symbols)

    return sum(
        GAME_PAYOUTS[symbol][counts[symbol]]
        for symbol in GAME_SYMBOLS
    )


def public_round(row):
    if not row:
        return None

    credited_reward = int(row["reward"])
    nominal_reward = calculate_game_reward(row["symbols"])

    return {
        "id": row["id"],
        "request_id": str(row["request_id"]),
        "symbols": row["symbols"],
        # مبلغی که واقعاً به موجودی اضافه شده است.
        "reward": credited_reward,
        "nominal_reward": nominal_reward,
        "reward_capped": credited_reward < nominal_reward,
        "balance_after": int(row["balance_after"]),
        "played_at": row["played_at"].timestamp(),
        "next_play_at": row["next_play_at"].timestamp(),
    }


def get_game_state(cur, user_id, balance, now=None):
    if now is None:
        now = database_now(cur)

    cur.execute("""
        SELECT *
        FROM mio_game_rounds
        WHERE user_id=%s
        ORDER BY id DESC
        LIMIT 1
    """, (user_id,))

    latest = cur.fetchone()
    pending = get_pending_withdrawal(cur, user_id)

    remaining_seconds = 0
    next_play_at = None
    balance = int(balance)
    balance_limit_reached = balance >= BALANCE_CAP

    if latest:
        next_play_at = latest["next_play_at"]
        remaining_seconds = max(
            0,
            math.ceil((next_play_at - now).total_seconds()),
        )

    return {
        "enabled": GAME_ENABLED,
        "can_play": (
            GAME_ENABLED
            and remaining_seconds == 0
            and pending is None
            and not balance_limit_reached
        ),
        "balance_cap": BALANCE_CAP,
        "balance_limit_reached": balance_limit_reached,
        "withdrawal_pending": pending is not None,
        "remaining_seconds": remaining_seconds,
        "next_play_at": (
            next_play_at.timestamp()
            if next_play_at
            else None
        ),
        "server_time": now.timestamp(),
        "balance": balance,
        "last_round": public_round(latest),
        "payouts": GAME_PAYOUTS,
    }


# ================= ROUTES =================

@app.route("/")
def health():
    return jsonify({
        "status": "ok",
        "service": "mio-backend",
    })


@app.route("/api/user")
@authenticated
def api_user(user):
    with db_transaction() as cur:
        row = get_user_row(cur, user["id"])

        if not row:
            return jsonify({
                "balance": 0,
                "daily_enabled": False,
                "notifications": [],
                "not_started": True,
                "withdrawn_today": 0,
                "pending_withdrawal": None,
                "pending_withdrawals": [],
            })

        pending = get_pending_withdrawals(cur, user["id"])

        return jsonify({
            "balance": user_balance(row),
            "daily_enabled": row["daily_mio"] == 1,
            "notifications": get_recent_notifications(cur, user["id"]),
            "not_started": False,
            "withdrawn_today": get_withdrawn_today(cur, user["id"]),
            "pending_withdrawal": pending[0] if pending else None,
            "pending_withdrawals": pending,
        })


@app.route("/api/invite-link")
@authenticated
def api_invite_link(user):
    if not BOT_USERNAME:
        return jsonify({
            "ok": False,
            "error": "bot_username_not_configured",
        }), 500

    return jsonify({
        "link": f"https://t.me/{BOT_USERNAME}?start={user['id']}"
    })


def create_withdrawal(user, target, withdraw_type):
    with db_transaction() as cur:
        row = get_user_row(cur, user["id"], lock=True)

        if not row:
            return jsonify({
                "ok": False,
                "error": "not_started",
            }), 403

        pending = get_pending_withdrawal(cur, user["id"])

        if pending:
            result = withdrawal_payload(cur, pending)
            result.update({
                "ok": False,
                "error": "already_pending",
            })

            return jsonify(result), 409

        balance = user_balance(row)

        if balance <= 0:
            return jsonify({
                "ok": False,
                "error": "zero_balance",
            }), 400

        cur.execute("""
            INSERT INTO withdraw_queue (
                user_id,
                target_username,
                amount,
                withdraw_type,
                status,
                created_time
            )
            VALUES (%s, %s, %s, %s, 0, %s)
            RETURNING id, user_id, amount, status, withdraw_type
        """, (
            user["id"],
            target,
            balance,
            withdraw_type,
            time.time(),
        ))

        result = withdrawal_payload(cur, cur.fetchone())
        result["ok"] = True

    return jsonify(result)


@app.route("/api/withdraw", methods=["POST"])
@authenticated
def api_withdraw(user):
    target = json_body().get("target")

    if not isinstance(target, str):
        return jsonify({"ok": False, "error": "invalid_target"}), 400

    target = target.strip()

    if not re.fullmatch(r"@[A-Za-z0-9_]{1,32}", target):
        return jsonify({"ok": False, "error": "invalid_target"}), 400

    return create_withdrawal(user, target, "id")


@app.route("/api/withdraw-bank", methods=["POST"])
@authenticated
def api_withdraw_bank(user):
    card = json_body().get("card")

    if not isinstance(card, str):
        return jsonify({"ok": False, "error": "invalid_card"}), 400

    card = card.strip()

    if not re.fullmatch(r"[0-9]{10,20}", card):
        return jsonify({"ok": False, "error": "invalid_card"}), 400

    return create_withdrawal(user, card, "card")


@app.route("/api/withdraw-status")
@authenticated
def api_withdraw_status(user):
    withdraw_id = request.args.get("withdraw_id")

    if withdraw_id is not None:
        if not re.fullmatch(r"[0-9]{1,18}", withdraw_id):
            return jsonify({
                "ok": False,
                "error": "invalid_withdraw_id",
            }), 400

        withdraw_id = int(withdraw_id)

    with db_transaction() as cur:
        if withdraw_id is None:
            cur.execute("""
                SELECT id, user_id, amount, status, withdraw_type
                FROM withdraw_queue
                WHERE user_id=%s
                ORDER BY id DESC
                LIMIT 1
            """, (user["id"],))
        else:
            cur.execute("""
                SELECT id, user_id, amount, status, withdraw_type
                FROM withdraw_queue
                WHERE user_id=%s AND id=%s
            """, (user["id"], withdraw_id))

        return jsonify(
            withdrawal_payload(cur, cur.fetchone())
        )


@app.route("/api/receipt", methods=["POST"])
@authenticated
def api_receipt(user):
    with db_transaction() as cur:
        cur.execute("""
            INSERT INTO receipt_request (user_id, request)
            VALUES (%s, 1)
            ON CONFLICT (user_id)
            DO UPDATE SET request=EXCLUDED.request
        """, (user["id"],))

    return jsonify({"ok": True})


@app.route("/api/daily/enable", methods=["POST"])
@authenticated
def api_daily_enable(user):
    with db_transaction() as cur:
        row = get_user_row(cur, user["id"], lock=True)

        if not row:
            return jsonify({"ok": False, "error": "not_started"}), 403

        current_balance = user_balance(row)

        if row["daily_mio"] == 1:
            return jsonify({
                "ok": True,
                "balance": current_balance,
            })

        if get_pending_withdrawal(cur, user["id"]):
            return jsonify({
                "ok": False,
                "error": "withdrawal_pending",
            }), 409

        daily_credit = max(
            0,
            min(DAILY_AMOUNT, DAILY_BALANCE_CAP - current_balance),
        )

        cur.execute("""
            UPDATE users
            SET
                daily_mio=1,
                balance=COALESCE(balance, 0) + %s
            WHERE user_id=%s
            RETURNING balance
        """, (daily_credit, user["id"]))

        balance = int(cur.fetchone()["balance"])

    return jsonify({"ok": True, "balance": balance})


@app.route("/api/game/status")
@authenticated
def api_game_status(user):
    with db_transaction() as cur:
        row = get_user_row(cur, user["id"], lock=True)

        if not row:
            return jsonify({"ok": False, "error": "not_started"}), 403

        result = get_game_state(
            cur,
            user["id"],
            user_balance(row),
        )
        result["ok"] = True

    return jsonify(result)


@app.route("/api/game/play", methods=["POST"])
@authenticated
def api_game_play(user):
    data = json_body()

    try:
        request_id = str(uuid.UUID(str(data.get("request_id", ""))))
    except (ValueError, TypeError, AttributeError):
        return jsonify({
            "ok": False,
            "error": "invalid_request_id",
        }), 400

    with db_transaction() as cur:
        # بررسی سقف و واریز جایزه زیر همان قفل کاربر انجام می‌شوند.
        row = get_user_row(cur, user["id"], lock=True)

        if not row:
            return jsonify({"ok": False, "error": "not_started"}), 403

        balance = user_balance(row)

        cur.execute("""
            SELECT *
            FROM mio_game_rounds
            WHERE user_id=%s AND request_id=%s
        """, (user["id"], request_id))

        previous_round = cur.fetchone()

        # بازیابی نتیجه حتی در موجودی ۵۰۰ هزار باید ممکن باشد.
        # این مسیر هیچ جایزه یا فرصت بازی جدیدی مصرف نمی‌کند.
        if previous_round:
            result = get_game_state(cur, user["id"], balance)
            result.update({
                "ok": True,
                "replayed": True,
                "round": public_round(previous_round),
            })
            return jsonify(result)

        now = database_now(cur)
        game_state = get_game_state(
            cur,
            user["id"],
            balance,
            now=now,
        )

        if not GAME_ENABLED:
            return jsonify({
                **game_state,
                "ok": False,
                "error": "game_disabled",
            }), 403

        if game_state["withdrawal_pending"]:
            return jsonify({
                **game_state,
                "ok": False,
                "error": "withdrawal_pending",
            }), 409

        # پیش از تولید نمادها، ثبت دور و ایجاد محدودیت ۴۸ ساعته.
        if balance >= BALANCE_CAP:
            return jsonify({
                **game_state,
                "ok": False,
                "error": "balance_limit_reached",
            }), 409

        if game_state["remaining_seconds"] > 0:
            return jsonify({
                **game_state,
                "ok": False,
                "error": "cooldown",
            }), 429

        symbols = [
            secrets.choice(GAME_SYMBOLS)
            for _ in range(3)
        ]

        nominal_reward = calculate_game_reward(symbols)

        # مثال: موجودی ۴۰۰ هزار و جایزه ۴۰۰ هزار
        # فقط ۱۰۰ هزار اعتبار می‌گیرد.
        reward = min(
            nominal_reward,
            max(0, BALANCE_CAP - balance),
        )

        next_play_at = now + GAME_COOLDOWN

        cur.execute("""
            UPDATE users
            SET balance=COALESCE(balance, 0) + %s
            WHERE user_id=%s
            RETURNING balance
        """, (reward, user["id"]))

        new_balance = int(cur.fetchone()["balance"])

        cur.execute("""
            INSERT INTO mio_game_rounds (
                user_id,
                request_id,
                symbols,
                reward,
                balance_after,
                played_at,
                next_play_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
        """, (
            user["id"],
            request_id,
            Json(symbols),
            reward,
            new_balance,
            now,
            next_play_at,
        ))

        round_row = cur.fetchone()

        result = get_game_state(
            cur,
            user["id"],
            new_balance,
            now=now,
        )

        result.update({
            "ok": True,
            "replayed": False,
            "round": public_round(round_row),
        })

    return jsonify(result)


# ================= STARTUP =================

init_database()

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
    )
