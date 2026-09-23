import os
import time
import hmac
import hashlib
from urllib.parse import parse_qsl

import psycopg2
from flask import Flask, request, jsonify
from flask_cors import CORS

# ================= CONFIG (از Environment Variables رندر) =================

DATABASE_URL = os.environ.get("DATABASE_URL")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
BOT_USERNAME = os.environ.get("BOT_USERNAME")  # بدون @  مثلا: MioPointBot

if not DATABASE_URL:
    raise RuntimeError("متغیر محیطی DATABASE_URL تنظیم نشده است.")
if not BOT_TOKEN:
    raise RuntimeError("متغیر محیطی BOT_TOKEN تنظیم نشده است.")

app = Flask(__name__)
CORS(app)

# ================= DATABASE =================

db = psycopg2.connect(DATABASE_URL)
db.autocommit = False
cursor = db.cursor()


def db_execute(query, params=None):
    """
    اجرای امن کوئری با تلاش مجدد در صورت قطعی کانکشن
    """
    global db, cursor
    try:
        cursor.execute(query, params or ())
    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        db = psycopg2.connect(DATABASE_URL)
        db.autocommit = False
        cursor = db.cursor()
        cursor.execute(query, params or ())


# ================= TELEGRAM INIT DATA VALIDATION =================

def verify_init_data(init_data: str):
    """
    اعتبارسنجی initData ارسالی از مینی‌اپ تلگرام
    طبق مستندات: https://core.telegram.org/bots/webapps#validating-data-received-via-the-mini-app
    در موفقیت، دیکشنری کاربر (user_id, username, first_name) برمی‌گرداند، وگرنه None.
    """
    if not init_data:
        return None

    try:
        pairs = dict(parse_qsl(init_data, strict_parsing=True))
    except ValueError:
        return None

    received_hash = pairs.pop("hash", None)
    if not received_hash:
        return None

    data_check_string = "\n".join(
        f"{k}={v}" for k, v in sorted(pairs.items())
    )

    secret_key = hmac.new(
        key=b"WebAppData",
        msg=BOT_TOKEN.encode(),
        digestmod=hashlib.sha256
    ).digest()

    computed_hash = hmac.new(
        key=secret_key,
        msg=data_check_string.encode(),
        digestmod=hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(computed_hash, received_hash):
        return None

    # auth_date نباید خیلی قدیمی باشه (بیشتر از ۲۴ ساعت)
    auth_date = int(pairs.get("auth_date", "0"))
    if time.time() - auth_date > 86400:
        return None

    import json
    user_raw = pairs.get("user")
    if not user_raw:
        return None

    user = json.loads(user_raw)
    return {
        "id": user.get("id"),
        "username": user.get("username"),
        "first_name": user.get("first_name"),
    }


def get_authenticated_user():
    """
    از هدر Authorization: tma <initData> کاربر رو استخراج و اعتبارسنجی می‌کنه.
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("tma "):
        return None

    init_data = auth_header[4:]
    return verify_init_data(init_data)


# ================= DB HELPERS (مشترک با بات) =================

MAX_BALANCE = 500000


def get_balance(user_id):
    db_execute("SELECT balance FROM users WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()
    return row[0] if row else 0


def is_daily_enabled(user_id):
    db_execute("SELECT daily_mio FROM users WHERE user_id=%s", (user_id,))
    row = cursor.fetchone()
    return bool(row and row[0] == 1)


def get_recent_notifications(user_id, limit=10):
    """
    آخرین رویدادهای برداشت این کاربر رو به شکل اعلان برمی‌گردونه.
    """
    db_execute(
        """
        SELECT amount, status, created_time
        FROM withdraw_queue
        WHERE user_id=%s AND status IN (2,3,4)
        ORDER BY id DESC
        LIMIT %s
        """,
        (user_id, limit)
    )
    rows = cursor.fetchall()

    notifications = []
    for amount, status, created_time in rows:
        t = time.strftime("%Y-%m-%d %H:%M", time.localtime(created_time)) if created_time else ""
        if status == 2:
            notifications.append({
                "type": "success",
                "text": f"🎉 برداشت {amount:,} میو با موفقیت انجام شد.",
                "time": t
            })
        else:
            notifications.append({
                "type": "fail",
                "text": f"❌ برداشت {amount:,} میو ناموفق بود.",
                "time": t
            })

    return notifications


# ================= ROUTES =================

@app.route("/")
def health():
    return jsonify({"status": "ok", "service": "mio-backend"})


@app.route("/api/user")
def api_user():
    user = get_authenticated_user()
    if not user:
        return jsonify({"error": "unauthorized"}), 401

    user_id = user["id"]

    db_execute("SELECT user_id FROM users WHERE user_id=%s", (user_id,))
    if not cursor.fetchone():
        return jsonify({
            "balance": 0,
            "daily_enabled": False,
            "notifications": [],
            "not_started": True
        })

    return jsonify({
        "balance": get_balance(user_id),
        "daily_enabled": is_daily_enabled(user_id),
        "notifications": get_recent_notifications(user_id)
    })


@app.route("/api/invite-link")
def api_invite_link():
    user = get_authenticated_user()
    if not user:
        return jsonify({"error": "unauthorized"}), 401

    if not BOT_USERNAME:
        return jsonify({"error": "bot_username_not_configured"}), 500

    link = f"https://t.me/{BOT_USERNAME}?start={user['id']}"
    return jsonify({"link": link})


@app.route("/api/withdraw", methods=["POST"])
def api_withdraw():
    user = get_authenticated_user()
    if not user:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    user_id = user["id"]
    data = request.get_json(silent=True) or {}
    target = (data.get("target") or "").strip()

    if not target.startswith("@") or len(target) < 2:
        return jsonify({"ok": False, "error": "invalid_target"}), 400

    balance = get_balance(user_id)
    if balance <= 0:
        return jsonify({"ok": False, "error": "zero_balance"}), 400

    # جلوگیری از ثبت چند درخواست هم‌زمان
    db_execute(
        """
        SELECT id FROM withdraw_queue
        WHERE user_id=%s AND status IN (0,1)
        """,
        (user_id,)
    )
    if cursor.fetchone():
        return jsonify({"ok": False, "error": "already_pending"}), 400

    db_execute(
        """
        INSERT INTO withdraw_queue
        (user_id, target_username, amount, withdraw_type, status, created_time)
        VALUES (%s,%s,%s,'id',0,%s)
        """,
        (user_id, target, balance, time.time())
    )
    db.commit()

    return jsonify({"ok": True})


@app.route("/api/withdraw-status")
def api_withdraw_status():
    user = get_authenticated_user()
    if not user:
        return jsonify({"status": "failed", "error": "unauthorized"}), 401

    user_id = user["id"]

    db_execute(
        """
        SELECT status
        FROM withdraw_queue
        WHERE user_id=%s
        ORDER BY id DESC
        LIMIT 1
        """,
        (user_id,)
    )
    row = cursor.fetchone()

    if not row:
        return jsonify({"status": "failed"})

    status_code = row[0]

    if status_code in (0, 1):
        return jsonify({"status": "pending"})
    elif status_code == 2:
        return jsonify({"status": "success"})
    else:
        return jsonify({"status": "failed"})


@app.route("/api/receipt", methods=["POST"])
def api_receipt():
    user = get_authenticated_user()
    if not user:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    user_id = user["id"]

    db_execute(
        """
        INSERT INTO receipt_request (user_id, request)
        VALUES (%s,1)
        ON CONFLICT (user_id) DO UPDATE SET request=EXCLUDED.request
        """,
        (user_id,)
    )
    db.commit()

    return jsonify({"ok": True})


@app.route("/api/daily/enable", methods=["POST"])
def api_daily_enable():
    user = get_authenticated_user()
    if not user:
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    user_id = user["id"]

    if is_daily_enabled(user_id):
        return jsonify({"ok": True, "balance": get_balance(user_id)})

    db_execute(
        "UPDATE users SET daily_mio=1 WHERE user_id=%s",
        (user_id,)
    )
    db.commit()

    current = get_balance(user_id)
    new_balance = min(current + 340000, MAX_BALANCE)

    db_execute(
        "UPDATE users SET balance=%s WHERE user_id=%s",
        (new_balance, user_id)
    )
    db.commit()

    return jsonify({"ok": True, "balance": new_balance})


# ================= RUN (فقط برای تست لوکال) =================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
