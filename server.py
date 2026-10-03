from telethon import TelegramClient, events
import psycopg2
import asyncio
import time

from config import DATABASE_URL_TABLET

# ================= API =================

API_ID = 36849785
API_HASH = "fcfe769c08575c9bbeb92e87fd340983"

client = TelegramClient(
    "mio_account",
    API_ID,
    API_HASH
)

# ================= DATABASE =================

db = psycopg2.connect(DATABASE_URL_TABLET)
db.autocommit = False

cursor = db.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS withdraw_success(
    user_id BIGINT PRIMARY KEY,
    status INTEGER DEFAULT 1
)
""")

# جدول صف برداشت
cursor.execute("""
CREATE TABLE IF NOT EXISTS withdraw_queue(
    id SERIAL PRIMARY KEY,
    user_id BIGINT,
    target_username TEXT,
    amount BIGINT,
    withdraw_type TEXT,
    status INTEGER DEFAULT 0,
    created_time DOUBLE PRECISION,
    message_id BIGINT
)
""")

# جدول رسیدها (با نوع برداشت)
cursor.execute("""
CREATE TABLE IF NOT EXISTS withdraw_receipts(
    user_id BIGINT PRIMARY KEY,
    message_id BIGINT,
    withdraw_type TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS receipt_request(
    user_id BIGINT PRIMARY KEY,
    request INTEGER DEFAULT 0
)
""")

db.commit()

# ================= DB HELPER =================

def db_execute(query, params=None):
    """
    اجرای امن کوئری با تلاش مجدد در صورت قطعی کانکشن
    """
    global db, cursor
    try:
        cursor.execute(query, params or ())
    except (psycopg2.InterfaceError, psycopg2.OperationalError):
        db = psycopg2.connect(DATABASE_URL_TABLET)
        db.autocommit = False
        cursor = db.cursor()
        cursor.execute(query, params or ())

# ================= BALANCE =================

def get_balance(user_id):
    db_execute(
        """
        SELECT balance
        FROM users
        WHERE user_id=%s
        """,
        (user_id,)
    )
    result = cursor.fetchone()
    if result:
        return result[0]
    return 0

def reset_balance(user_id):
    db_execute(
        """
        UPDATE users
        SET balance=0
        WHERE user_id=%s
        """,
        (user_id,)
    )
    db.commit()

# ================= CONFIG =================

WITHDRAW_GROUP = "panke_saghfe"
ID_GROUP = "themaws_gap"
MEOWIE = "MeowieQIVBot"

ACTIVE_WITHDRAW = None
LAST_TRANSFER_TIME = 0
TRANSFER_DELAY = 35
CONFIRM_TIMEOUT = 30
CONFIRM_RETRY = 3

# ================= QUEUE FUNCTIONS =================

def add_withdraw_queue(user_id, target_username, amount, withdraw_type, message_id):
    db_execute(
        """
        INSERT INTO withdraw_queue
        (user_id, target_username, amount, withdraw_type, status, created_time, message_id)
        VALUES (%s,%s,%s,%s,0,%s,%s)
        """,
        (user_id, target_username, amount, withdraw_type, time.time(), message_id)
    )
    db.commit()

def set_withdraw_status(request_id, status):
    db_execute(
        """
        UPDATE withdraw_queue
        SET status=%s
        WHERE id=%s
        """,
        (status, request_id)
    )
    db.commit()

def get_next_withdraw():
    db_execute(
        """
        SELECT id, user_id, target_username, amount, withdraw_type, message_id
        FROM withdraw_queue
        WHERE status=0
        ORDER BY id ASC
        LIMIT 1
        """
    )
    return cursor.fetchone()

def fail_active_withdraw():
    global ACTIVE_WITHDRAW

    if not ACTIVE_WITHDRAW:
        return

    db_execute(
        """
        UPDATE withdraw_queue
        SET status=3
        WHERE id=%s
        """,
        (ACTIVE_WITHDRAW["id"],)
    )
    db.commit()

    print("❌ انتقال ناموفق ثبت شد")

    ACTIVE_WITHDRAW = None

MEOWIE_ID = None


async def get_meowie_id():
    """
    آیدی عددی Meowie رو یه بار می‌گیره و کش می‌کنه (برای فیلتر دقیق‌تر پیام‌ها).
    """
    global MEOWIE_ID
    if MEOWIE_ID is None:
        entity = await client.get_entity(MEOWIE)
        MEOWIE_ID = entity.id
    return MEOWIE_ID


async def find_meowie_reply(reply_to_id, must_contain=None, button_text=None, timeout=30):
    """
    پیام Meowie رو پیدا می‌کنه که:
      - دقیقاً ریپلای روی پیام ما (reply_to_id) باشه
      - (اختیاری) متنش شامل یکی از عبارت‌های must_contain باشه
      - (اختیاری) دکمه‌ای شامل button_text داشته باشه
    گپ شلوغه و Meowie به بقیه هم جواب میده؛ این فیلتر جلوی اشتباه گرفتن رو می‌گیره.
    """
    meowie_id = await get_meowie_id()
    deadline = time.time() + timeout

    while time.time() < deadline:
        msgs = await client.get_messages(WITHDRAW_GROUP, limit=40)

        for m in msgs:
            if m.sender_id != meowie_id:
                continue
            if not m.reply_to or m.reply_to.reply_to_msg_id != reply_to_id:
                continue

            text = m.raw_text or ""

            if must_contain and not any(t in text for t in must_contain):
                continue

            if button_text:
                found = False
                for row in (m.buttons or []):
                    for btn in row:
                        if btn.text and button_text in btn.text:
                            found = True
                if not found:
                    continue

            return m

        await asyncio.sleep(1)

    return None


async def find_confirm_panel(card, min_id, timeout=30):
    """
    پنل تایید نهایی (شامل شماره‌حساب ما و دکمه‌ی «تایید تراکنش») رو پیدا می‌کنه.
    Meowie ممکنه پیام قبلی رو edit کنه یا پیام جدید بفرسته؛ هر دو حالت پوشش داده میشه.
    """
    meowie_id = await get_meowie_id()
    deadline = time.time() + timeout

    while time.time() < deadline:
        msgs = await client.get_messages(WITHDRAW_GROUP, limit=40)

        for m in msgs:
            if m.sender_id != meowie_id or m.id < min_id:
                continue
            if card not in (m.raw_text or ""):
                continue
            if not m.buttons:
                continue

            for row in m.buttons:
                for btn in row:
                    if btn.text and "تایید تراکنش" in btn.text:
                        return m

        await asyncio.sleep(1)

    return None


async def click_button_containing(msg, text):
    """
    تو دکمه‌های شیشه‌ای یه پیام دنبال دکمه‌ای می‌گرده که شامل متن داده‌شده باشه
    و روش کلیک می‌کنه. اگه پیدا نشد False برمی‌گردونه.
    """
    if not msg.buttons:
        return False

    for row in msg.buttons:
        for btn in row:
            if btn.text and text in btn.text:
                await btn.click()
                return True

    return False


async def process_bank_card_withdraw(request_id, user_id, card, amount):
    """
    پردازش برداشت با بانک میویی (کارت به کارت):
    ۱) ارسال پیام «بانک میویی»
    ۲) پیدا کردن پنل «کارت به کارت میویی» که Meowie روی پیام ما ریپلای کرده
    ۳) ریپلای دقیق روی همون پنل با «مبلغ شماره‌حساب»
    ۴) پیدا کردن پنل تایید و کلیک روی «تایید تراکنش»
    """
    global LAST_TRANSFER_TIME

    amount_text = f"{amount}"

    try:
        # ۱) ارسال «بانک میویی»
        sent = await client.send_message(WITHDRAW_GROUP, "بانک میویی")

        # ۲) منوی Meowie (ریپلای روی پیام ما) که دکمه‌ی «کارت به کارت میویی» داره
        menu_msg = await find_meowie_reply(
            sent.id,
            button_text="کارت به کارت",
            timeout=30
        )

        if not menu_msg:
            print("❌ منوی بانک میویی (با دکمه کارت به کارت) از Meowie نیومد")
            set_withdraw_status(request_id, 3)
            return

        if not await click_button_containing(menu_msg, "کارت به کارت"):
            print("❌ دکمه «کارت به کارت میویی» پیدا نشد")
            set_withdraw_status(request_id, 3)
            return

        # ۳) بعد از کلیک، پنل «لطفا مبلغ و شماره حساب رو در جواب همین پنل وارد کنید» میاد
        #    (Meowie ممکنه همون پیام منو رو edit کنه یا پیام جدید بفرسته)
        prompt_msg = await find_meowie_reply(
            sent.id,
            must_contain=["در جواب همین پنل", "تعیین مبلغ"],
            timeout=30
        )

        if not prompt_msg:
            print("❌ پنل «مبلغ و شماره حساب» از Meowie نیومد")
            set_withdraw_status(request_id, 3)
            return

        # ۴) ریپلای دقیق روی همون پنل با «مبلغ شماره‌حساب»
        await client.send_message(
            WITHDRAW_GROUP,
            f"{amount_text} {card}",
            reply_to=prompt_msg.id
        )

        # ۵) پنل تایید (با شماره‌حساب ما و دکمه‌ی تایید تراکنش)
        confirm_msg = await find_confirm_panel(
            card,
            min_id=prompt_msg.id,
            timeout=30
        )

        if not confirm_msg:
            print("❌ پنل تایید تراکنش از Meowie نیومد")
            set_withdraw_status(request_id, 3)
            return

        if not await click_button_containing(confirm_msg, "تایید تراکنش"):
            print("❌ دکمه «تایید تراکنش» پیدا نشد")
            set_withdraw_status(request_id, 3)
            return

        # کمی صبر تا Meowie تراکنش رو نهایی کنه
        await asyncio.sleep(3)

        final_msg = await client.get_messages(WITHDRAW_GROUP, ids=confirm_msg.id)
        final_text = final_msg.raw_text if final_msg else ""
        print(f"📩 وضعیت نهایی بانک میویی برای {user_id}: {final_text}")

        # همه‌ی مراحل بدون خطا طی شد -> موفق ثبت کن
        # (پیام موفقیت و ریست موجودی همون‌طور که برای روش «آیدی» هست،
        #  توسط حلقه‌ی check_withdraw_success تو bot.py انجام می‌شود)
        set_withdraw_status(request_id, 2)

        db_execute(
            """
            INSERT INTO withdraw_receipts (user_id, message_id, withdraw_type)
            VALUES (%s,%s,'card')
            ON CONFLICT (user_id) DO UPDATE SET
                message_id=EXCLUDED.message_id,
                withdraw_type=EXCLUDED.withdraw_type
            """,
            (user_id, confirm_msg.id)
        )

        db_execute(
            """
            INSERT INTO withdraw_success (user_id, status)
            VALUES (%s,1)
            ON CONFLICT (user_id) DO UPDATE SET status=EXCLUDED.status
            """,
            (user_id,)
        )
        db.commit()

        print(f"🎉 برداشت با بانک میویی موفق: {user_id} مقدار {amount_text}")

    except Exception as e:
        print("❌ خطا در پردازش بانک میویی:", e)
        set_withdraw_status(request_id, 3)

    finally:
        LAST_TRANSFER_TIME = time.time()


# ================= PROCESS WITHDRAW QUEUE =================

async def process_withdraw_queue():
    global ACTIVE_WITHDRAW
    global LAST_TRANSFER_TIME

    while True:
        try:
            if ACTIVE_WITHDRAW:
                if time.time() - ACTIVE_WITHDRAW["time"] > CONFIRM_TIMEOUT:
                    print("⏰ تایم اوت انتقال")
                    fail_active_withdraw()

                await asyncio.sleep(3)
                continue

            if time.time() - LAST_TRANSFER_TIME < TRANSFER_DELAY:
                await asyncio.sleep(3)
                continue

            request = get_next_withdraw()

            if not request:
                await asyncio.sleep(3)
                continue

            request_id = request[0]
            user_id = request[1]
            target = request[2]
            amount = request[3]
            withdraw_type = request[4]
            message_id = request[5]

            amount_text = f"{amount:,}"

            # برداشت با بانک میویی -> جریان مکالمه‌ای جدا (بلاک‌کننده تا پایان)
            if withdraw_type == "card":
                set_withdraw_status(request_id, 1)
                print(f"🏦 شروع برداشت بانک میویی {user_id} مقدار {amount_text}")
                await process_bank_card_withdraw(request_id, user_id, target, amount)
                continue

            try:
                # برداشت با آیدی
                if withdraw_type == "id":
                    msg = await client.send_message(
                        ID_GROUP,
                        f"انتقال میویی {amount_text} {target}"
                    )
                # برداشت گپی با ریپلای
                else:
                    msg = await client.send_message(
                        WITHDRAW_GROUP,
                        f"انتقال میویی {amount_text}",
                        reply_to=message_id
                    )

            except Exception as e:
                print("❌ خطا در ارسال انتقال:", e)
                set_withdraw_status(request_id, 3)
                await asyncio.sleep(5)
                continue

            ACTIVE_WITHDRAW = {
                "id": request_id,
                "user_id": user_id,
                "amount": amount,
                "type": withdraw_type,
                "message_id": msg.id,
                "time": time.time()
            }

            set_withdraw_status(request_id, 1)

            LAST_TRANSFER_TIME = time.time()

            print(f"🚀 شروع انتقال {user_id} مقدار {amount_text}")

        except Exception as e:
            # قطعی موقت اینترنت/دیتابیس؛ کل یوزربات نباید کرش کنه
            print("خطا در حلقه‌ی process_withdraw_queue (نادیده گرفته شد):", e)
            await asyncio.sleep(5)

# ================= AUTO CONFIRM =================

@client.on(events.NewMessage(from_users=MEOWIE))
async def meowie_handler(event):
    global ACTIVE_WITHDRAW

    if not ACTIVE_WITHDRAW:
        return

    text = event.raw_text

    if "آیا از انتقال" not in text:
        return

    print("📩 پیام تایید Meowie دریافت شد")

    confirmed = False

    for attempt in range(CONFIRM_RETRY):
        try:
            await event.click(0, 0)
            confirmed = True
            print("✅ دکمه تایید زده شد")
            break

        except Exception as e:
            print(f"❌ تلاش {attempt+1} ناموفق:", e)
            await asyncio.sleep(2)

    if not confirmed:
        return

    try:
        await asyncio.sleep(3)

        user_id = ACTIVE_WITHDRAW["user_id"]
        request_id = ACTIVE_WITHDRAW["id"]
        withdraw_type = ACTIVE_WITHDRAW["type"]

        db_execute(
            """
            INSERT INTO withdraw_receipts (user_id, message_id, withdraw_type)
            VALUES (%s,%s,%s)
            ON CONFLICT (user_id) DO UPDATE SET
                message_id=EXCLUDED.message_id,
                withdraw_type=EXCLUDED.withdraw_type
            """,
            (user_id, event.id, withdraw_type)
        )

        db_execute(
            """
            INSERT INTO withdraw_success (user_id, status)
            VALUES (%s,1)
            ON CONFLICT (user_id) DO UPDATE SET status=EXCLUDED.status
            """,
            (user_id,)
        )

        db_execute(
            """
            UPDATE withdraw_queue
            SET status=2
            WHERE id=%s
            """,
            (request_id,)
        )

        db.commit()

        reset_balance(user_id)

        print(f"🎉 انتقال موفق: {user_id}")

        # رسید دیگه اینجا خودکار فرستاده نمی‌شه؛
        # فقط وقتی کاربر دکمه «می‌خواهم» رو بزنه و پیام
        # «رسیدم رو بده» رو بفرسته، از طریق receipt_handler ارسال میشه.

        ACTIVE_WITHDRAW = None

    except Exception as e:
        print("❌ خطا:", e)
        fail_active_withdraw()

# ================= OLD WITHDRAW =================

@client.on(events.NewMessage(chats=WITHDRAW_GROUP))
async def old_withdraw_handler(event):
    text = event.raw_text.strip()

    if text not in ["میوهام رو بده", "میو هام رو بده"]:
        return

    user = await event.get_sender()

    bal = get_balance(user.id)

    if bal <= 0:
        await event.reply("❌ موجودی میویی ندارید")
        return

    add_withdraw_queue(user.id, "", bal, "group", event.id)

    await event.reply(
        f"""

⏳ درخواست برداشت ثبت شد ✅

💎 مقدار:

{bal:,} میو

صف انتقال فعال شد.

لطفاً منتظر بمانید.
"""
    )

# ================= WITHDRAW BY ID =================

async def add_id_withdraw(user_id, target):
    amount = get_balance(user_id)

    if amount <= 0:
        return False

    add_withdraw_queue(user_id, target, amount, "id", 0)

    return True

# ================= RECEIPT HANDLER =================

@client.on(events.NewMessage(incoming=True))
async def receipt_handler(event):
    if not event.is_private:
        return

    if event.raw_text.strip() != "رسیدم رو بده":
        return

    user = await event.get_sender()

    db_execute(
        """
        SELECT request
        FROM receipt_request
        WHERE user_id=%s
        """,
        (user.id,)
    )

    request = cursor.fetchone()

    if not request or request[0] != 1:
        await event.reply("❌ درخواست رسیدی ثبت نشده.")
        return

    db_execute(
        """
        SELECT message_id, withdraw_type
        FROM withdraw_receipts
        WHERE user_id=%s
        """,
        (user.id,)
    )

    receipt = cursor.fetchone()

    if not receipt:
        await event.reply("❌ رسید پیدا نشد.")
        return

    receipt_message_id, withdraw_type = receipt

    # اگه برداشت از گپ بوده، رسید همونجا هست - فقط راهنمایی کن
    if withdraw_type == "group":
        await event.reply("برو تو گپ رسیدت هست 😐🤣")
        return

    # برداشت با آیدی -> از ID_GROUP فوروارد کن
    # برداشت با بانک میویی -> از WITHDRAW_GROUP فوروارد کن
    source_chat = WITHDRAW_GROUP if withdraw_type == "card" else ID_GROUP

    await client.forward_messages(user.id, receipt_message_id, source_chat)

    db_execute(
        """
        UPDATE receipt_request
        SET request=0
        WHERE user_id=%s
        """,
        (user.id,)
    )

    db.commit()

    print(f"✅ رسید ارسال شد: {user.id}")

# ================= RUN =================

async def main():
    print("Mio Userbot Started 🚀")

    await client.start()

    print("✅ Userbot Online")

    await asyncio.gather(
        client.run_until_disconnected(),
        process_withdraw_queue()
    )

if __name__ == "__main__":
    asyncio.run(main())
