import mysql.connector
import csv
import os
from dotenv import load_dotenv
import datetime


# ===========================================
# LOAD .env
# ===========================================
load_dotenv("Phones_Sources_WH/.env")

DB_HOST = os.getenv("DB_HOSTNAME")
DB_USER = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT"))

DB_CONTROLLER = os.getenv("DB_CONTROLLER")
DB_STAGING = os.getenv("DB_STAGING")

FOLDER = os.getenv("FOLDER")   # ví dụ: data_scraping


# ===========================================
# CONNECT
# ===========================================
def get_connection(database):
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        database=database
    )


# ===========================================
# QUERY FILE MỚI NHẤT TỪ DB
# ===========================================
def get_latest_file():
    conn = get_connection(DB_CONTROLLER)
    cursor = conn.cursor()

    sql = """
        SELECT fl.file_path, fl.file_name
        FROM process_log AS pl
        JOIN file_log AS fl 
            ON fl.process_id = pl.id
        WHERE pl.step = 'E'
          AND pl.status = 'COMPLETED'
        ORDER BY pl.end_time DESC
        LIMIT 1;
    """

    cursor.execute(sql)
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if not row:
        raise Exception("❌ Không tìm thấy file mới nhất trong DB")

    return row[0], row[1]   # file_path, file_name


# ===========================================
# CLEAN VALUE
# ===========================================
def clean_value(v):
    if v is None:
        return None
    v = v.strip()
    if v == "" or v.lower() in ("null", "none", "-"):
        return None
    return v


# ===========================================
# CLEAR OLD DATA
# ===========================================
def clear_table():
    conn = get_connection(DB_STAGING)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM phones_raw")
    cursor.execute("ALTER TABLE phones_raw AUTO_INCREMENT = 1")

    conn.commit()
    cursor.close()
    conn.close()
    print("🗑️  Đã xoá dữ liệu cũ trong phones_raw")


# ===========================================
# LOAD CSV → phones_raw
# ===========================================
def load_csv_to_phones_raw(file_path):
    conn = get_connection(DB_STAGING)
    cursor = conn.cursor()

    sql = """
        INSERT INTO phones_raw (
            product_name, product_url, image_url,
            price_current, price_original, price_gift,
            discount_percent, promotion, installment,
            screen_size, screen_resolution, ram, rom,
            variants, rating, sold_quantity,
            source, data_id, collected_at
        ) VALUES (
            %(product_name)s, %(product_url)s, %(image_url)s,
            %(price_current)s, %(price_original)s, %(price_gift)s,
            %(discount_percent)s, %(promotion)s, %(installment)s,
            %(screen_size)s, %(screen_resolution)s, %(ram)s, %(rom)s,
            %(variants)s, %(rating)s, %(sold_quantity)s,
            %(source)s, %(data_id)s, %(collected_at)s
        )
    """

    inserted = 0

    # FIX UTF-8 BOM
    with open(file_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        # FIX header BOM
        reader.fieldnames = [h.strip().lstrip('\ufeff') for h in reader.fieldnames]

        print("== Header Keys ==", reader.fieldnames)

        for row in reader:
            data = {
                "product_name": clean_value(row.get("product_name")),
                "product_url": clean_value(row.get("product_url")),
                "image_url": clean_value(row.get("image_url")),

                "price_current": clean_value(row.get("price_current")),
                "price_original": clean_value(row.get("price_original")),
                "price_gift": clean_value(row.get("price_gift")),

                "discount_percent": clean_value(row.get("discount_percent")),
                "promotion": clean_value(row.get("promotion")),
                "installment": clean_value(row.get("installment")),

                "screen_size": clean_value(row.get("screen_size")),
                "screen_resolution": clean_value(row.get("screen_resolution")),
                "ram": clean_value(row.get("ram")),
                "rom": clean_value(row.get("rom")),

                "variants": clean_value(row.get("variants")),
                "rating": clean_value(row.get("rating")),
                "sold_quantity": clean_value(row.get("sold_quantity")),

                "source": clean_value(row.get("source")),
                "data_id": clean_value(row.get("data_id")),
                "collected_at": clean_value(row.get("collected_at")),
            }

            cursor.execute(sql, data)
            inserted += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Đã import {inserted} dòng vào phones_raw")

def insert_process_log(step="L1", status="COMPLETED", note=""):
    conn = get_connection(DB_CONTROLLER)  # process_log nằm ở DB_CONTROLLER
    cursor = conn.cursor()

    now = datetime.datetime.now()

    sql = """
        INSERT INTO process_log (step, status, start_time, end_time, note)
        VALUES (%s, %s, %s, %s, %s)
    """

    # start_time = 10 giây trước (hoặc bạn có thể dùng thời gian bắt đầu script)
    start_time = now - datetime.timedelta(seconds=10)
    end_time = now

    cursor.execute(sql, (step, status, start_time, end_time, note))
    conn.commit()
    cursor.close()
    conn.close()

    print(f"📝 Đã ghi log: step={step}, status={status}, note={note}")

# ===========================================
# MAIN
# ===========================================
if __name__ == "__main__":
    # 1. Lấy file mới nhất
    file_path_db, file_name = get_latest_file()
    print(f"📄 File name từ DB: {file_name}")
    print(f"⚠️ File_path từ DB bỏ qua (Windows path): {file_path_db}")

    # 2. Build đường dẫn chính xác theo thư mục làm việc hiện tại
    local_path = os.path.join(os.getcwd(), FOLDER, file_name)

    print(f"📂 File thực tế trên Linux: {local_path}")

    if not os.path.exists(local_path):
        raise Exception(f"❌ File không tồn tại: {local_path}")

    clear_table()
    load_csv_to_phones_raw(local_path)
    note_text = "Imported to rows into phones_raw"
    insert_process_log(step="L1", status="COMPLETED", note=note_text)
