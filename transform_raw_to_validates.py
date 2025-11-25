import os
import csv
import hashlib
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector

load_dotenv(".env")

DB_HOST = os.getenv("DB_HOSTNAME")
DB_PORT = int(os.getenv("DB_PORT"))
DB_USER = os.getenv("DB_USERNAME")
DB_PASS = os.getenv("DB_PASSWORD")
DB_STAGING = os.getenv("DB_STAGING")
DB_CONTROLLER = os.getenv("DB_CONTROLLER")

MAPPING_FILE = "field_mapping.csv"


def db_staging():
    """Kết nối DB staging (phones_raw, phones_validated)."""
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS, database=DB_STAGING
    )


def db_controller():
    """Kết nối DB controller (process_log)."""
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS, database=DB_CONTROLLER
    )


def check_L1_completed():
    """Bước 1: kiểm tra L1 gần nhất đã COMPLETED hay chưa."""
    conn = db_controller()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT end_time FROM process_log
        WHERE step='L1' AND status='COMPLETED'
        ORDER BY end_time DESC LIMIT 1
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row is not None


def get_L1_time():
    """Lấy end_time của L1 gần nhất (dùng cho load_staging_time)."""
    conn = db_controller()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT end_time FROM process_log
        WHERE step='L1' AND status='COMPLETED'
        ORDER BY end_time DESC LIMIT 1
    """)
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row["end_time"] if row else None


def log_start(step):
    """Bước 0: ghi log bắt đầu step T1 (status = RUNNING)."""
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
    """Bước 8/9: cập nhật trạng thái kết thúc step T1."""
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


def clean_price(value):
    """Chuẩn hóa giá: giữ lại chữ số, trả về INT hoặc None."""
    if not value:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return int(digits) if digits else None


def clean_rating(value):
    """Chuẩn hóa rating về thang 5.0, DECIMAL(3,2)."""
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


def cast_value(value, dtype, target_field=None):
    """Ép kiểu theo datatype & field đích (Bước 6.2)."""
    if value is None:
        return None

    dtype = dtype.upper()

    # Giữ nguyên % cho discount_percent
    if target_field == "discount_percent":
        return str(value).strip()

    # Rating
    if dtype == "DECIMAL(3,2)":
        return clean_rating(value)

    # Giá / số
    if dtype.startswith("DECIMAL") or dtype.startswith("INT"):
        return clean_price(value)

    # Mặc định trả về nguyên giá trị
    return value


def load_mapping():
    """Bước 2.2: đọc field_mapping.csv (source_field → target_field, datatype)."""
    mapping = []
    with open(MAPPING_FILE, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["active_flag"].strip() == "1":
                mapping.append((row["source_field"], row["target_field"], row["target_datatype"]))
    return mapping


def ensure_phones_validated():
    """Bước 2.1: đảm bảo tồn tại bảng phones_validated."""
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
            discount_percent VARCHAR(50),
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

            execute_at DATETIME,
            load_staging_time DATETIME,

            INDEX idx_bk (bk_hash)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    conn.commit()
    cur.close()
    conn.close()


def truncate_validated():
    """Bước 5: xoá dữ liệu cũ trong phones_validated."""
    conn = db_staging()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE phones_validated")
    conn.commit()
    cur.close()
    conn.close()


def insert_row(cursor, row):
    """Bước 6.3: insert 1 dòng đã chuẩn hoá vào phones_validated."""
    cursor.execute("""
        INSERT INTO phones_validated (
            bk_hash, source, product_url,
            product_name, image_url, price_current, price_original,
            price_gift, discount_percent, promotion, installment,
            screen_size, screen_resolution, ram, rom, variants,
            rating, sold_quantity, data_id,
            execute_at, load_staging_time
        )
        VALUES (
            %(bk_hash)s, %(source)s, %(product_url)s,
            %(product_name)s, %(image_url)s, %(price_current)s, %(price_original)s,
            %(price_gift)s, %(discount_percent)s, %(promotion)s, %(installment)s,
            %(screen_size)s, %(screen_resolution)s, %(ram)s, %(rom)s, %(variants)s,
            %(rating)s, %(sold_quantity)s, %(data_id)s,
            %(execute_at)s, %(load_staging_time)s
        )
    """, row)


def process_t1():
    """
    THỨ TỰ CÁC BƯỚC TRONG WORKFLOW T1 (phones_raw → phones_validated):

    Bước 0: Ghi log bắt đầu step T1.
    Bước 1: Kiểm tra L1 đã COMPLETED hay chưa.
    Bước 2: Chuẩn bị môi trường (bảng validated + mapping).
    Bước 3: Lấy thời gian thực thi & thời điểm kết thúc L1.
    Bước 4: Đọc dữ liệu nguồn từ phones_raw.
    Bước 5: Xoá dữ liệu cũ trong phones_validated.
    Bước 6: Chuẩn hoá & insert từng dòng vào phones_validated.
    Bước 7: Commit & đóng kết nối.
    Bước 8: Ghi log COMPLETED / NO_DATA.
    Bước 9: Nếu có exception → ghi log FAILED.
    """
    # Bước 0: log start T1
    log_id, _ = log_start("T1")

    try:
        # Bước 1: kiểm tra điều kiện tiền đề – L1 phải COMPLETED
        if not check_L1_completed():
            print("L1 NOT COMPLETED → STOP T1")
            log_end(log_id, "SKIPPED", "L1 not completed")
            return

        # Bước 2: chuẩn bị môi trường T1
        ensure_phones_validated()      # 2.1: đảm bảo bảng validated
        mapping = load_mapping()       # 2.2: load cấu hình mapping

        # Bước 3: lấy thời gian thực thi & thời điểm L1
        execute_time = datetime.now()
        l1_time = get_L1_time()

        # Bước 4: đọc dữ liệu nguồn từ phones_raw
        conn = db_staging()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM phones_raw")
        rows = cursor.fetchall()

        if not rows:
            # Không có dữ liệu → kết thúc với NO_DATA
            cursor.close()
            conn.close()
            log_end(log_id, "NO_DATA", "phones_raw empty")
            return

        # Bước 5: xoá dữ liệu cũ trong phones_validated
        truncate_validated()

        # Bước 6: chuẩn hoá & insert từng dòng
        inserted = 0
        for r in rows:
            # 6.1: build row cơ bản (source, url, hash, thời gian)
            row = {
                "source": r.get("source"),
                "product_url": r.get("product_url"),
                "bk_hash": hashlib.md5(
                    f"{r.get('source')}||{r.get('product_url')}".encode()
                ).hexdigest(),
                "execute_at": execute_time,
                "load_staging_time": l1_time
            }

            # 6.2: apply mapping + cast datatype
            for src, tgt, dtype in mapping:
                row[tgt] = cast_value(r.get(src), dtype, target_field=tgt)

            # 6.3: insert dòng đã chuẩn hoá
            insert_row(cursor, row)
            inserted += 1

        # Bước 7: commit & đóng kết nối
        conn.commit()
        cursor.close()
        conn.close()

        # Bước 8: log COMPLETED
        log_end(log_id, "COMPLETED", f"Inserted={inserted}")
        print(f"T1 COMPLETED — {inserted} rows inserted")

    except Exception as e:
        # Bước 9: lỗi → log FAILED
        log_end(log_id, "FAILED", str(e))
        raise e


if __name__ == "__main__":
    process_t1()
