import os
import csv
import hashlib
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector

load_dotenv(".env")

DB_HOST = os.getenv("DB_HOSTNAME")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USERNAME")
DB_PASS = os.getenv("DB_PASSWORD")
DB_STAGING = os.getenv("DB_STAGING")
DB_CONTROLLER = os.getenv("DB_CONTROLLER")

MAPPING_FILE = "field_mapping.csv"


def db_staging():
    """Kết nối DB staging (phones_raw, phones_validated)."""
    return mysql.connector.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        user=DB_USER,
        password=DB_PASS,
        database=DB_STAGING
    )


def db_controller():
    """Kết nối DB controller (process_log)."""
    return mysql.connector.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        user=DB_USER,
        password=DB_PASS,
        database=DB_CONTROLLER
    )


def validate_env():
    """
    Kiểm tra các biến môi trường bắt buộc trong .env đã được cấu hình đầy đủ chưa.
    Nếu thiếu -> raise RuntimeError để process_t1() ghi FAILED.
    """
    required_keys = [
        "DB_HOSTNAME",
        "DB_PORT",
        "DB_USERNAME",
        "DB_PASSWORD",
        "DB_STAGING",
        "DB_CONTROLLER",
    ]
    missing = [k for k in required_keys if os.getenv(k) in (None, "")]
    if missing:
        raise RuntimeError(f"Missing ENV config: {', '.join(missing)}")


def get_last_step_time(step_name, statuses=("COMPLETED",)):
    """
    Lấy end_time mới nhất của 1 step (L1, T1, ...) với các trạng thái cho trước.
    Dùng để so sánh L1 vs T1.
    """
    conn = db_controller()
    cur = conn.cursor(dictionary=True)
    placeholders = ",".join(["%s"] * len(statuses))
    cur.execute(
        f"""
        SELECT MAX(end_time) AS last_time
        FROM process_log
        WHERE step = %s
          AND status IN ({placeholders})
        """,
        (step_name, *statuses),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row["last_time"] if row and row["last_time"] else None


def can_run_T1():

    last_L1 = get_last_step_time("L1", ("COMPLETED",))
    if last_L1 is None:
        # 2.1: Chưa có L1 COMPLETED → không chạy T1
        return False, None

    last_T1 = get_last_step_time("T1", ("COMPLETED",))

    # 2.2: Đánh giá điều kiện L1 mới hơn T1
    if last_T1 is None or last_L1 > last_T1:
        return True, last_L1
    else:
        return False, last_L1


def log_start(step):
    """0.x: ghi log bắt đầu 1 step (status = RUNNING)."""
    conn = db_controller()
    cur = conn.cursor()
    now = datetime.now()

    cur.execute(
        """
        INSERT INTO process_log(step, status, start_time)
        VALUES (%s, 'RUNNING', %s)
        """,
        (step, now),
    )

    conn.commit()
    log_id = cur.lastrowid
    cur.close()
    conn.close()
    return log_id, now


def log_end(log_id, status, note=None):
    """Ghi trạng thái kết thúc step (COMPLETED / SKIPPED / NO_DATA / FAILED)."""
    conn = db_controller()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE process_log
        SET status=%s, end_time=%s, note=%s
        WHERE id=%s
        """,
        (status, datetime.now(), note, log_id),
    )
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
    """Dùng trong 9.2 – Ép kiểu theo datatype & field đích."""
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
    """3.2 Đọc field_mapping.csv (source_field → target_field, datatype)."""
    mapping = []
    with open(MAPPING_FILE, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["active_flag"].strip() == "1":
                mapping.append(
                    (
                        row["source_field"],
                        row["target_field"],
                        row["target_datatype"],
                    )
                )
    return mapping


def ensure_phones_validated():
    """3.1 Đảm bảo tồn tại bảng phones_validated."""
    conn = db_staging()
    cur = conn.cursor()
    cur.execute(
        """
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
        """
    )
    conn.commit()
    cur.close()
    conn.close()


def truncate_validated():
    """8. Xoá dữ liệu cũ trong phones_validated trước khi insert batch mới."""
    conn = db_staging()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE phones_validated")
    conn.commit()
    cur.close()
    conn.close()


def insert_row(cursor, row):
    """9.3 Insert 1 dòng đã chuẩn hoá vào phones_validated."""
    cursor.execute(
        """
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
        """,
        row,
    )


def process_t1():
    # 0. Ghi log bắt đầu T1
    log_id, _ = log_start("T1")

    try:
        # 1. Kiểm tra .env trước (nếu thiếu config → raise, sẽ vào except)
        validate_env()

        # 2. Kiểm tra điều kiện tiền đề (L1 mới hơn T1?)
        can_run, l1_time = can_run_T1()
        if not can_run:
            print("L1 NOT COMPLETED → STOP T1")
            log_end(log_id, "SKIPPED", "L1 NOT COMPLETED")
            return

        # 3. Chuẩn bị môi trường T1
        # 3.1 Đảm bảo bảng phones_validated tồn tại
        ensure_phones_validated()
        # 3.2 Load cấu hình mapping từ CSV
        mapping = load_mapping()

        # 4. Kiểm tra mapping hợp lệ (không rỗng)
        if not mapping:
            print("INVALID MAPPING → STOP T1")
            log_end(log_id, "FAILED", "Invalid or empty field_mapping.csv")
            return

        # 5. Lấy thời gian thực thi & thời điểm L1
        execute_time = datetime.now()
        load_staging_time = l1_time  # dùng làm load_staging_time

        # 6. Kết nối DB staging & đọc dữ liệu từ phones_raw
        try:
            # 6.1 Mở kết nối & tạo cursor dictionary
            conn = db_staging()
            cursor = conn.cursor(dictionary=True)
            # 6.2 Đọc toàn bộ dữ liệu từ phones_raw
            cursor.execute("SELECT * FROM phones_raw")
            rows = cursor.fetchall()
        except Exception as db_err:
            # Lỗi kết nối / truy vấn DB staging
            log_end(log_id, "FAILED", f"DB_STAGING error: {db_err}")
            return

        # 7. Kiểm tra có dữ liệu hay không
        if not rows:
            cursor.close()
            conn.close()
            log_end(log_id, "NO_DATA", "phones_raw empty")
            return

        # 8. Xoá dữ liệu cũ trong phones_validated
        truncate_validated()

        # 9. Chuẩn hoá & insert từng dòng
        inserted = 0
        for r in rows:
            # 9.1 Xây dựng row cơ bản (source, url, hash, thời gian)
            row = {
                "source": r.get("source"),
                "product_url": r.get("product_url"),
                "bk_hash": hashlib.md5(
                    f"{r.get('source')}||{r.get('product_url')}".encode()
                ).hexdigest(),
                "execute_at": execute_time,
                "load_staging_time": load_staging_time,
            }

            # 9.2 Áp dụng mapping + ép kiểu datatype
            for src, tgt, dtype in mapping:
                row[tgt] = cast_value(r.get(src), dtype, target_field=tgt)

            # 9.3 Insert dòng đã chuẩn hoá vào phones_validated
            insert_row(cursor, row)
            inserted += 1

        # 10. Commit & đóng kết nối
        conn.commit()
        cursor.close()
        conn.close()

        # 11. Ghi log COMPLETED
        log_end(log_id, "COMPLETED", f"Inserted={inserted}")
        print(f"T1 COMPLETED — {inserted} rows inserted")

    except Exception as e:
        # Ngoại lệ tổng → FAILED
        log_end(log_id, "FAILED", str(e))
        raise e


if __name__ == "__main__":
    process_t1()
