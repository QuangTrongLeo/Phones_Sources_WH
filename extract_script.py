import os
import re
import csv
import requests
from datetime import datetime
import pytz
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import schedule
import time
import mysql.connector
from mysql.connector import Error

load_dotenv()
FOLDER = os.getenv("FOLDER")
URLS = {}

# Hàm tiện ích
def get_html(url):
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")

def clean_price(text):
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None

def clean_percent(text):
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None

# Kết nối đến database controller
def get_db_controller_connection():
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOSTNAME"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_CONTROLLER")
        )
        if connection.is_connected():
            print(f"Đã kết nối đến database: {os.getenv('DB_CONTROLLER')}")
        return connection
    except Error as e:
        print(f"Lỗi kết nối database: {e}")
        return None
    
# Kiểm tra xem các bảng cần thiết đã tồn tại chưa
def check_tables_exist(connection):
    try:
        cursor = connection.cursor()
        required_tables = ["config_source", "process_log", "file_log"]
        all_exist = True

        for table in required_tables:
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = '{table}';
            """)
            exists = cursor.fetchone()[0]
            if exists == 0:
                all_exist = False
                break

        cursor.close()
        return all_exist

    except Error as e:
        print(f"Lỗi khi kiểm tra bảng: {e}")
        return False

# Tạo các bảng nếu chưa tồn tại
def create_tables(connection):
    try:
        cursor = connection.cursor()

        tables = {
            "config_source": """
                CREATE TABLE config_source (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    source VARCHAR(255) UNIQUE,
                    endtrypoint TEXT,
                    enabled VARCHAR(10),
                    fetch_type VARCHAR(50),
                    parse_format VARCHAR(50),
                    schedule_cron VARCHAR(50),
                    pagination VARCHAR(255),
                    output_table_stagging VARCHAR(255),
                    note TEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """,
            "file_log": """
                CREATE TABLE file_log (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    process_id INT,
                    file_name VARCHAR(255),
                    file_path VARCHAR(255),
                    source VARCHAR(255),
                    status VARCHAR(50),
                    rows_count INT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    note TEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """,
            "process_log": """
                CREATE TABLE process_log (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    step VARCHAR(255),
                    status VARCHAR(50),
                    start_time DATETIME,
                    end_time DATETIME,
                    note TEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """
        }

        for table_name, create_sql in tables.items():
            cursor.execute(f"""
                SELECT COUNT(*)
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = '{table_name}';
            """)
            exists = cursor.fetchone()[0]

            if exists == 0:
                cursor.execute(create_sql)
                connection.commit()
                print(f"Bảng `{table_name}` đã được tạo.")

        cursor.close()

    except Error as e:
        print(f"Lỗi khi tạo bảng: {e}")

# Nạp file config.csv vào bảng config_source trong DB (chỉ khi bảng rỗng)
def load_config_to_db(connection, config_path):
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM config_source")
        count = cursor.fetchone()[0]

        db_name = os.getenv("DB_CONTROLLER") or connection.database

        if count > 0:
            cursor.close()
            return

        try:
            df = pd.read_csv(config_path, encoding='utf-8-sig')
        except UnicodeDecodeError:
            df = pd.read_csv(config_path, encoding='latin1')

        df.columns = df.columns.str.replace('\ufeff', '').str.strip().str.lower()
        df = df.dropna(subset=['id', 'source'])
        df['id'] = df['id'].astype(int)

        for index, row in df.iterrows():
            cursor.execute("""
                INSERT INTO config_source (
                    id, source, endtrypoint, enabled, fetch_type, parse_format,
                    schedule_cron, pagination, output_table_stagging, note
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    source=VALUES(source),
                    endtrypoint=VALUES(endtrypoint),
                    enabled=VALUES(enabled),
                    fetch_type=VALUES(fetch_type),
                    parse_format=VALUES(parse_format),
                    schedule_cron=VALUES(schedule_cron),
                    pagination=VALUES(pagination),
                    output_table_stagging=VALUES(output_table_stagging),
                    note=VALUES(note)
            """, tuple(row.fillna('').values))

        connection.commit()
        cursor.close()
        print(f"Đã nạp {len(df)} dòng từ `{config_path}` vào bảng `{db_name}.config_source`.")

    except Exception as e:
        print(f"Lỗi khi nạp config.csv vào DB: {e}")

# Đọc entrypoints từ file config.csv
def get_entrypoints(config_path):
    entrypoints = []
    try:
        with open(config_path, mode='r', encoding='cp1258', newline='') as file:
            sample = file.read(2048)
            file.seek(0)
            dialect = csv.Sniffer().sniff(sample)
            reader = csv.DictReader(file, dialect=dialect)

            for row in reader:
                if row['enabled'].strip().upper() == 'TRUE':
                    entrypoints.append({
                        'source': row['source'].strip(),
                        'url': row['endtrypoint'].strip(),
                    })
    except UnicodeDecodeError:
        print("Lỗi đọc file: hãy thử lưu lại file CSV dạng 'CSV UTF-8 (Comma delimited)'.")
    except FileNotFoundError:
        print(f"Không tìm thấy file: {config_path}")
    except KeyError as e:
        print(f"Thiếu cột trong CSV: {e}")
    except Exception as e:
        print(f"Lỗi khác: {e}")

    return entrypoints

# Crawl dữ liệu từ trang Cellphones
def crawl_cellphones():
    print("Crawling Cellphones...")
    soup = get_html(URLS["cellphones"])
    products = soup.select("div.product-info-container.product-item")
    data = []

    for p in products:
        name = p.select_one(".product__name h3")
        product_url = p.select_one(".product__link")
        image_url = p.select_one(".product__img")
        price_current = p.select_one(".product__price--show")
        price_original = p.select_one(".product__price--through")
        discount_percent = p.select_one(".product__price--percent-detail span")
        screen_size = p.select_one(".product__more-info__item:nth-of-type(1)")
        ram = p.select_one(".product__more-info__item:nth-of-type(2)")
        rom = p.select_one(".product__more-info__item:nth-of-type(3)")
        promotion = p.select_one(".promotion .coupon-price")
        installment = p.select_one(".box-info__installment")
        rating = p.select_one(".product__box-rating")

        data.append({
            "product__name": name.get_text(strip=True) if name else None,
            "product_url": product_url["href"] if product_url else None,
            "image_url": image_url["src"] if image_url else None,
            "price_current": clean_price(price_current.get_text(strip=True) if price_current else None),
            "price_original": clean_price(price_original.get_text(strip=True) if price_original else None),
            "discount_percent": clean_percent(discount_percent.get_text(strip=True) if discount_percent else None),
            "screen_size": screen_size.get_text(strip=True) if screen_size else None,
            "ram": ram.get_text(strip=True) if ram else None,
            "rom": rom.get_text(strip=True) if rom else None,
            "promotion": promotion.get_text(strip=True) if promotion else None,
            "installment": installment.get_text(strip=True) if installment else None,
            "rating": rating.get_text(strip=True) if rating else None,
        })

    return pd.DataFrame(data)

# Crawl dữ liệu từ trang Thegioididong
def crawl_tgdd():
    print("Crawling Thegioididong...")
    soup = get_html(URLS["tgdd"])
    products = soup.select("li.item.ajaxed")
    data = []

    for p in products:
        img_tag = p.select_one(".thumb")
        image_url = None
        if img_tag:
            image_url = img_tag.get("src") or img_tag.get("data-original") or img_tag.get("data-src")

        name_tag = p.select_one("h3")
        product_name = name_tag.get_text(strip=True) if name_tag else None

        price_current = p.select_one("strong.price")
        price_original = p.select_one(".price-old")
        discount_percent = p.select_one(".percent")

        price_gift_tag = p.select_one(".item-gift b")
        price_gift = clean_price(price_gift_tag.get_text(strip=True)) if price_gift_tag else None

        data.append({
            "data_id": p.get("data-id"),
            "product_name": product_name,
            "product_url": "https://www.thegioididong.com" + p.select_one("a")["href"] if p.select_one("a") else None,
            "image_url": image_url,
            "price_current": clean_price(price_current.get_text(strip=True) if price_current else None),
            "price_original": clean_price(price_original.get_text(strip=True) if price_original else None),
            "discount_percent": clean_percent(discount_percent.get_text(strip=True) if discount_percent else None),
            "price_gift": price_gift,
            "screen_resolution": p.select_one(".item-compare span:nth-of-type(1)").get_text(strip=True)
                if p.select_one(".item-compare span:nth-of-type(1)") else None,
            "screen_size": p.select_one(".item-compare span:nth-of-type(2)").get_text(strip=True)
                if p.select_one(".item-compare span:nth-of-type(2)") else None,
            "variants": ", ".join([li.get_text(strip=True) for li in p.select(".merge__item")]),
            "installment_info": p.select_one(".shiping")["aria-label"] if p.select_one(".shiping") else None,
            "rating": p.select_one(".vote-txt b").get_text(strip=True) if p.select_one(".vote-txt b") else None,
            "sold_quantity": p.select_one(".rating_Compare span").get_text(strip=True)
                .replace("•", "").strip() if p.select_one(".rating_Compare span") else None
        })

    return pd.DataFrame(data)

# Gộp cột trùng lặp giữa hai nguồn dữ liệu
def merge_duplicate_columns(df: pd.DataFrame):
    equivalent_cols = {
        "product_name": ["product__name"],
        "installment": ["installment_info"],
        "screen_resolution": ["screen_res"],
    }

    for main_col, duplicates in equivalent_cols.items():
        if main_col not in df.columns:
            df[main_col] = None

        for dup in duplicates:
            if dup in df.columns:
                df[main_col] = df[main_col].combine_first(df[dup])
                df.drop(columns=[dup], inplace=True, errors="ignore")

    return df

# Ghi log bắt đầu process ETL
def log_process_start(connection, step="E", note="Data extraction"):
    try:
        cursor = connection.cursor()
        vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
        start_time = datetime.now(vn_tz).strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute("""
            INSERT INTO process_log (step, status, start_time, note)
            VALUES (%s, %s, %s, %s)
        """, (step, "PENDING", start_time, note))

        connection.commit()
        process_id = cursor.lastrowid
        cursor.close()
        return process_id

    except Error as e:
        print(f"Lỗi khi ghi log process: {e}")
        return None

# Cập nhật trạng thái khi kết thúc process ETL
def log_process_end(connection, process_id, status="COMPLETED"):
    try:
        cursor = connection.cursor()
        vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
        end_time = datetime.now(vn_tz).strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute("""
            UPDATE process_log
            SET status=%s, end_time=%s
            WHERE id=%s
        """, (status, end_time, process_id))

        connection.commit()
        cursor.close()

    except Error as e:
        print(f"Lỗi khi cập nhật log process: {e}")

# Ghi log thông tin file sau khi xuất CSV
def log_file(connection, process_id, file_path, status="SUCCESS", note="Exported ETL data file"):
    try:
        cursor = connection.cursor()
        vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
        created_at = datetime.now(vn_tz).strftime("%Y-%m-%d %H:%M:%S")

        file_name = os.path.basename(file_path)

        cursor.execute("SELECT source FROM config_source")
        sources = [row[0] for row in cursor.fetchall()]
        source_str = ", ".join(sources)

        rows_count = 0
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8-sig") as f:
                rows_count = sum(1 for line in f) - 1

        cursor.execute("""
            INSERT INTO file_log (process_id, file_name, file_path, source, status, rows_count, created_at, note)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (process_id, file_name, file_path, source_str, status, rows_count, created_at, note))

        connection.commit()
        cursor.close()

    except Error as e:
        print(f"Lỗi khi ghi log file: {e}")        

# =============================
def init():
    print("Bắt đầu tiến trình ETL(Extract)...")

    # 3. Kết nối DB_CONTROLLER
    db_conn = get_db_controller_connection()
    process_id = None
    output_path = ""

    # 3.1 Kết nối DB OK?
    if not db_conn:
        # 3.2 Lỗi kết nối DB
        print("Không kết nối được DB_CONTROLLER.")
        return

    # 4. Kiểm tra bảng DB
    tables_exist = check_tables_exist(db_conn)

    # 4.1 Bảng tồn tại?
    if not tables_exist:
        # 4.2 Tạo bảng mới
        create_tables(db_conn)

    # 5. Ghi process_log START
    process_id = log_process_start(db_conn)

    # 6. Load config.csv & lấy entrypoints
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "config.csv")
    load_config_to_db(db_conn, config_path)
    configs = get_entrypoints(config_path)

    # 6.1 Có entrypoints?
    if not configs:
        # 6.2 Không có entrypoints
        print("Không tìm thấy entrypoints hợp lệ trong config.csv")
        log_process_end(db_conn, process_id, status="FAILED")
        db_conn.close()
        return

    # 7. Chuẩn bị URL theo entrypoints
    for item in configs:
        URLS[item['source']] = item['url']

    try:
        # 8. Crawl dữ liệu từng nguồn
        df1 = crawl_cellphones()
        df1["source"] = "cellphones"

        df2 = crawl_tgdd()
        df2["source"] = "thegioididong"

        # 8.1 Crawl thành công?

        # 9. Xử lý dữ liệu trước khi xuất CSV
        df = pd.concat([df1, df2], ignore_index=True)
        df = merge_duplicate_columns(df)

        vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
        now = datetime.now(vn_tz)
        timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
        df["collected_at"] = timestamp

        desired_order = [
            "product_name", "product_url", "image_url",
            "price_current", "price_original", "price_gift", "discount_percent",
            "smember_discount", "sstudent_discount", "promotion", "installment",
            "screen_size", "screen_resolution", "ram", "rom", "variants",
            "rating", "sold_quantity", "source", "data_id", "collected_at"
        ]
        existing_cols = [c for c in df.columns if c not in desired_order]
        final_cols = [c for c in desired_order if c in df.columns] + existing_cols
        df = df[final_cols]

        # 10. Kiểm tra folder lưu CSV
        if not os.path.exists(FOLDER):
            # 10.2 Tạo folder mới
            os.makedirs(FOLDER, exist_ok=True)

        # 11. Xuất CSV vào folder
        filename = f"phones_source_{timestamp}.csv"
        output_path = os.path.join(FOLDER, filename)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"SUCCESS — Đã xuất file: {output_path}")

        # 12. Ghi file_log vào DB (SUCCESS)
        log_file(db_conn, process_id, output_path, status="SUCCESS")

        # 13. Ghi process_log END (COMPLETED)
        log_process_end(db_conn, process_id)

    except Exception as e:
        # 8.2 Crawl lỗi
        print(f"Lỗi ETL: {e}")
        log_file(db_conn, process_id, output_path, status="FAIL", note=str(e))
        log_process_end(db_conn, process_id, status="FAILED")

    finally:
        # 14. Đóng kết nối DB
        if db_conn:
            db_conn.close()

    print("Đang chạy chế độ tự động... (Nhấn Ctrl + C để dừng)\n")

def schedule_jobs():
    # 15. Bật chế độ tự động + schedule
    #schedule.every(30).seconds.do(init)     # chạy mỗi 30 giây
    schedule.every(1).minutes.do(init)      # chạy mỗi 1 phút
    #schedule.every(1).hours.do(init)        # chạy mỗi 1 giờ
    #schedule.every().day.at("00:00").do(init)  # chạy vào 00:00 mỗi ngày

    try:
        while True:
            schedule.run_pending()
            time.sleep(10)

    except KeyboardInterrupt:
        # 16. Thoát chương trình khi KeyboardInterrupt
        print("Đã dừng chương trình.")

if __name__ == "__main__":
    init()
    schedule_jobs()
