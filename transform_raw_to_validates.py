import os
import csv
import hashlib
from datetime import datetime, date
from dotenv import load_dotenv
import mysql.connector
from decimal import Decimal

# ==========================================
# LOAD ENV
# ==========================================
load_dotenv(".env")

DB_HOST = os.getenv("DB_HOSTNAME")
DB_PORT = int(os.getenv("DB_PORT"))
DB_USER = os.getenv("DB_USERNAME")
DB_PASS = os.getenv("DB_PASSWORD")
DB_STAGING = os.getenv("DB_STAGING")
DB_CONTROLLER = os.getenv("DB_CONTROLLER")

MAPPING_FILE = "field_mapping.csv"


# ==========================================
# DB CONNECTION
# ==========================================
def db_staging():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS, database=DB_STAGING
    )


def db_controller():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS, database=DB_CONTROLLER
    )


# ==========================================
# PROCESS LOGGING
# ==========================================
def log_process_start(step):
    conn = db_controller()
    cur = conn.cursor()
    now = datetime.now()

    cur.execute("""
        INSERT INTO process_log(step, status, start_time)
        VALUES (%s, %s, %s)
    """, (step, "RUNNING", now))

    conn.commit()
    log_id = cur.lastrowid
    cur.close()
    conn.close()
    return log_id, now


def log_process_end(log_id, status, note=None):
    conn = db_controller()
    cur = conn.cursor()

    cur.execute("""
        UPDATE process_log
        SET status=%s, end_time=%s, note=%s
        WHERE id=%s
    """, (status, datetime.now(), note, log_id))

    conn.commit()
    cur.close()
    conn.close()


# ==========================================
# NORMALIZE FOR COMPARISON
# ==========================================
def normalize(v):
    if v is None:
        return None
    if isinstance(v, (int, float, Decimal)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        return s if s != "" else None
    return v


# ==========================================
# ENSURE TABLE STRUCTURES
# ==========================================
def ensure_dim_date():
    conn = db_staging()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id       INT PRIMARY KEY,
            date_value    DATE NOT NULL,
            year_num      SMALLINT NOT NULL,
            month_num     TINYINT NOT NULL,
            day_of_month  TINYINT NOT NULL,
            day_of_week   TINYINT NOT NULL,
            is_weekend    TINYINT NOT NULL,
            week_of_year  TINYINT NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    conn.commit()
    cur.close()
    conn.close()


def ensure_phones_validated():
    conn = db_staging()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS phones_validated (
            id INT AUTO_INCREMENT PRIMARY KEY,
            source VARCHAR(100),
            product_url VARCHAR(1000),
            bk_hash CHAR(32),

            product_name VARCHAR(300),
            image_url VARCHAR(1000),
            price_current DECIMAL(12,0),
            price_original DECIMAL(12,0),
            price_gift DECIMAL(12,0),
            discount_percent INT,
            promotion TEXT,
            installment TEXT,
            screen_size VARCHAR(100),
            screen_resolution VARCHAR(100),
            ram VARCHAR(50),
            rom VARCHAR(50),
            variants TEXT,
            rating DECIMAL(3,2),
            sold_quantity VARCHAR(100),
            data_id VARCHAR(100),

            effective_from DATETIME NOT NULL,
            effective_to DATETIME,
            is_current TINYINT NOT NULL DEFAULT 1,

            INDEX idx_bk (bk_hash),
            INDEX idx_current (is_current)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    conn.commit()
    cur.close()
    conn.close()


# ==========================================
# READ MAPPING
# ==========================================
def load_mapping():
    mapping = []
    with open(MAPPING_FILE, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["active_flag"].strip() == "1":
                mapping.append((row["source_field"], row["target_field"], row["target_datatype"]))
    return mapping


# ==========================================
# CLEAN VALUE HELPERS
# ==========================================
def clean_price(value):
    if not value:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return int(digits) if digits else None


def clean_rating(value):
    if not value:
        return None

    s = str(value).strip()
    if s in ("", "null", "-", "None"):
        return None

    # e.g. "4/5"
    if "/" in s:
        try:
            num, den = s.split("/")
            return round(float(num) / float(den) * 5, 2)
        except:
            return None

    # e.g. "98%"
    if s.endswith("%"):
        try:
            return round(float(s[:-1]) / 20, 2)
        except:
            return None

    s = s.replace(",", ".")
    try:
        r = float(s)
        return max(0, min(5, round(r, 2)))
    except:
        return None


# ==========================================
# CAST VALUE
# ==========================================
def cast_value(value, dtype):
    if value is None or value == "":
        return None

    dtype = dtype.upper()

    if dtype == "DECIMAL(3,2)":
        return clean_rating(value)

    if dtype.startswith("DECIMAL") or dtype.startswith("INT"):
        return clean_price(value)

    return value


# ==========================================
# BUSINESS KEY HASH
# ==========================================
def build_bk_hash(source, product_url):
    return hashlib.md5(f"{source}||{product_url}".encode("utf-8")).hexdigest()


# ==========================================
# UPSERT dim_date
# ==========================================
def upsert_dim_date(d: date):
    conn = db_staging()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO dim_date (date_id, date_value, year_num, month_num, day_of_month, day_of_week, is_weekend, week_of_year)
        VALUES (%s,%s, YEAR(%s), MONTH(%s), DAY(%s), WEEKDAY(%s)+1, IF(WEEKDAY(%s)>=5,1,0), WEEK(%s))
        ON DUPLICATE KEY UPDATE date_value=VALUES(date_value)
    """, (int(d.strftime("%Y%m%d")), d, d, d, d, d, d, d))

    conn.commit()
    cur.close()
    conn.close()


# ==========================================
# INSERT NEW SCD2 VERSION
# ==========================================
def insert_new(cursor, row):
    cursor.execute("""
        INSERT INTO phones_validated (
            bk_hash, effective_from, effective_to, is_current,
            source, product_url, product_name, image_url,
            price_current, price_original, price_gift,
            discount_percent, promotion, installment,
            screen_size, screen_resolution, ram, rom, variants,
            rating, sold_quantity, data_id
        )
        VALUES (
            %(bk_hash)s, %(effective_from)s, %(effective_to)s, %(is_current)s,
            %(source)s, %(product_url)s, %(product_name)s, %(image_url)s,
            %(price_current)s, %(price_original)s, %(price_gift)s,
            %(discount_percent)s, %(promotion)s, %(installment)s,
            %(screen_size)s, %(screen_resolution)s, %(ram)s, %(rom)s, %(variants)s,
            %(rating)s, %(sold_quantity)s, %(data_id)s
        )
    """, row)


# ==========================================
# MAIN PROCESS (TRANSFORM + SCD2 + LOG)
# ==========================================
def process():
    log_id, _ = log_process_start("T1")

    try:
        ensure_dim_date()
        ensure_phones_validated()

        mapping = load_mapping()

        conn = db_staging()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM phones_raw")
        rows = cursor.fetchall()

        if not rows:
            log_process_end(log_id, "NO_DATA", "phones_raw empty")
            return

        now = datetime.now()
        upsert_dim_date(now.date())

        inserted = 0
        updated = 0

        for r in rows:
            bk = build_bk_hash(r["source"], r["product_url"])

            cursor.execute("""
                SELECT * FROM phones_validated
                WHERE bk_hash=%s AND is_current=1
                LIMIT 1
            """, (bk,))
            existing = cursor.fetchone()

            new_row = {
                "bk_hash": bk,
                "source": r.get("source"),
                "product_url": r.get("product_url"),
                "effective_from": now,
                "effective_to": None,
                "is_current": 1
            }

            for src, tgt, dtype in mapping:
                new_row[tgt] = cast_value(r.get(src), dtype)

            # FIRST TIME
            if existing is None:
                insert_new(cursor, new_row)
                inserted += 1
                conn.commit()
                continue

            # CHECK CHANGES
            changed = any(
                normalize(existing.get(tgt)) != normalize(new_row.get(tgt))
                for _, tgt, _ in mapping
            )

            if changed:
                cursor.execute("""
                    UPDATE phones_validated
                    SET effective_to=%s, is_current=0
                    WHERE id=%s
                """, (now, existing["id"]))

                insert_new(cursor, new_row)
                updated += 1
                conn.commit()

        cursor.close()
        conn.close()

        note = f"Inserted={inserted}, Updated={updated}, TotalRows={len(rows)}"
        log_process_end(log_id, "COMPLETED", note)

        print("Transform + SCD2 completed")

    except Exception as e:
        log_process_end(log_id, "FAILED", str(e))
        raise e


# ==========================================
# RUN
# ==========================================
if __name__ == "__main__":
    process()
