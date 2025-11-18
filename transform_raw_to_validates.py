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
# CONNECT DB
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
# CHECK L1 COMPLETED BEFORE RUN
# ==========================================
def check_L1_completed():
    conn = db_controller()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT * FROM process_log
        WHERE step = 'L1' AND status = 'COMPLETED'
        ORDER BY end_time DESC
        LIMIT 1
    """)

    row = cur.fetchone()
    cur.close()
    conn.close()

    return row is not None  # True = ok to run


# ==========================================
# PROCESS LOGGING
# ==========================================
def log_start(step):
    conn = db_controller()
    cur = conn.cursor()
    now = datetime.now()

    cur.execute("""
        INSERT INTO process_log(step, status, start_time)
        VALUES (%s, 'RUNNING', %s)
    """, (step, now))

    conn.commit()
    log_id = cur.lastrowid
    cur.close()
    conn.close()
    return log_id, now


def log_end(log_id, status, note=None):
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
# VALUE HELPERS
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

    if s in ("", "-", "null", "None"):
        return None

    if "/" in s:
        try:
            num, den = s.split("/")
            return round(float(num) / float(den) * 5, 2)
        except:
            return None

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


def cast_value(value, dtype):
    if value is None:
        return None

    dtype = dtype.upper()

    if dtype == "DECIMAL(3,2)":
        return clean_rating(value)

    if dtype.startswith("DECIMAL") or dtype.startswith("INT"):
        return clean_price(value)

    return value


# ==========================================
# LOAD MAPPING
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

            INDEX idx_bk (bk_hash)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    conn.commit()
    cur.close()
    conn.close()


# ==========================================
# TRUNCATE VALIDATED
# ==========================================
def truncate_validated():
    conn = db_staging()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE phones_validated")
    conn.commit()
    cur.close()
    conn.close()


# ==========================================
# INSERT NEW ROW
# ==========================================
def insert_row(cursor, row):
    cursor.execute("""
        INSERT INTO phones_validated (
            bk_hash, source, product_url,
            product_name, image_url, price_current, price_original,
            price_gift, discount_percent, promotion, installment,
            screen_size, screen_resolution, ram, rom, variants,
            rating, sold_quantity, data_id
        )
        VALUES (
            %(bk_hash)s, %(source)s, %(product_url)s,
            %(product_name)s, %(image_url)s, %(price_current)s, %(price_original)s,
            %(price_gift)s, %(discount_percent)s, %(promotion)s, %(installment)s,
            %(screen_size)s, %(screen_resolution)s, %(ram)s, %(rom)s, %(variants)s,
            %(rating)s, %(sold_quantity)s, %(data_id)s
        )
    """, row)


# ==========================================
# MAIN T1 PROCESS (FULL REFRESH)
# ==========================================
def process_t1():
    log_id, _ = log_start("T1")

    try:
        # Step 0: Check if L1 Completed
        if not check_L1_completed():
            print("L1 NOT COMPLETED → STOP T1")
            log_end(log_id, "SKIPPED", "L1 not completed")
            return

        ensure_dim_date()
        ensure_phones_validated()  # <-- Ensure tables exist before truncating

        mapping = load_mapping()

        conn = db_staging()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM phones_raw")
        rows = cursor.fetchall()

        if not rows:
            log_end(log_id, "NO_DATA", "phones_raw empty")
            return

        # FULL REFRESH
        truncate_validated()

        inserted = 0

        for r in rows:
            row = {
                "source": r.get("source"),
                "product_url": r.get("product_url"),
                "bk_hash": hashlib.md5(f"{r.get('source')}||{r.get('product_url')}".encode()).hexdigest()
            }

            for src, tgt, dtype in mapping:
                row[tgt] = cast_value(r.get(src), dtype)

            insert_row(cursor, row)
            inserted += 1

        conn.commit()
        cursor.close()
        conn.close()

        log_end(log_id, "COMPLETED", f"Inserted={inserted}")

        print(f"T1 Transform completed: {inserted} rows inserted")

    except Exception as e:
        log_end(log_id, "FAILED", str(e))
        raise e


if __name__ == "__main__":
    process_t1()