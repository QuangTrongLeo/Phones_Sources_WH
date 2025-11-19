import mysql.connector
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import os
import csv

load_dotenv(".env")

DB_HOST = os.getenv("DB_HOSTNAME")
DB_PORT = int(os.getenv("DB_PORT"))
DB_USER = os.getenv("DB_USERNAME")
DB_PASS = os.getenv("DB_PASSWORD")
DB_STAGING = os.getenv("DB_STAGING")
DB_CONTROLLER = os.getenv("DB_CONTROLLER")

OUTPUT_DIR = "/home/userserver/Phones_Sources_WH/dim_date"


# ==========================
# CONNECT DB
# ==========================
def db_staging():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS,
        database=DB_STAGING
    )

def db_controller():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS,
        database=DB_CONTROLLER
    )


# ==========================
# CHECK E COMPLETED
# ==========================
def check_E_completed():
    conn = db_controller()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT * FROM process_log
        WHERE step='E' AND status='COMPLETED'
        ORDER BY end_time DESC
        LIMIT 1
    """)

    row = cur.fetchone()
    cur.close()
    conn.close()

    return row is not None


# ==========================
# PROCESS LOG
# ==========================
def log_start():
    conn = db_controller()
    cur = conn.cursor()
    now = datetime.now()

    cur.execute("""
        INSERT INTO process_log(step, status, start_time)
        VALUES ('T0', 'RUNNING', %s)
    """, (now,))
    conn.commit()
    pid = cur.lastrowid

    cur.close()
    conn.close()
    return pid


def log_end(process_id, status, note=None):
    conn = db_controller()
    cur = conn.cursor()

    cur.execute("""
        UPDATE process_log
        SET status=%s, end_time=%s, note=%s
        WHERE id=%s
    """, (status, datetime.now(), note, process_id))

    conn.commit()
    cur.close()
    conn.close()


def log_file(process_id, file_path, file_name, status, rows_count, note=None):
    conn = db_controller()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO file_log(process_id, file_name, file_path, source, status, rows_count, note)
        VALUES (%s,%s,%s,'dim_date',%s,%s,%s)
    """, (process_id, file_name, file_path, status, rows_count, note))

    conn.commit()
    cur.close()
    conn.close()


# ==========================
# TABLE INIT
# ==========================
def ensure_dim_date():
    conn = db_staging()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id INT PRIMARY KEY,
            date_value DATE NOT NULL,
            year_num SMALLINT NOT NULL,
            month_num TINYINT NOT NULL,
            day_of_month TINYINT NOT NULL,
            day_of_week TINYINT NOT NULL,
            is_weekend TINYINT NOT NULL,
            week_of_year TINYINT NOT NULL,

            quarter_num TINYINT,
            quarter_label VARCHAR(10),

            year_week_sunday VARCHAR(10),
            year_week_monday VARCHAR(10),

            holiday_flag TINYINT,
            day_type VARCHAR(20),

            day_since_2025 INT,
            month_since_2025 INT,

            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    conn.commit()
    cur.close()
    conn.close()


# ==========================
# HOLIDAY VN (simple list)
# ==========================
def vietnam_holidays(year):
    return {
        f"{year}-01-01",
        f"{year}-04-30",
        f"{year}-05-01",
        f"{year}-09-02"
    }


# ==========================
# GENERATE TODAY RECORD
# ==========================
def generate_record(today):

    date_id = int(today.strftime("%Y%m%d"))
    year_num = today.year
    month_num = today.month
    day_of_month = today.day
    day_of_week = today.weekday() + 1
    is_weekend = 1 if day_of_week >= 6 else 0

    week_of_year = int(today.strftime("%U"))
    quarter_num = (month_num - 1) // 3 + 1
    quarter_label = f"{year_num}-Q{quarter_num}"

    # Week Sunday
    week_sunday = today - timedelta(days=today.weekday() + 1)
    year_week_sunday = f"{week_sunday.year}-W{week_sunday.strftime('%U')}"

    # Week Monday
    week_monday = today - timedelta(days=today.weekday())
    year_week_monday = f"{week_monday.year}-W{week_monday.strftime('%U')}"

    holidays = vietnam_holidays(year_num)
    is_holiday = 1 if str(today) in holidays else 0
    day_type = "Holiday" if is_holiday else ("Weekend" if is_weekend else "Weekday")

    since_2025 = date(2025, 1, 1)
    day_since_2025 = (today - since_2025).days + 1
    month_since_2025 = (year_num - 2025) * 12 + month_num

    return {
        "date_id": date_id,
        "date_value": today,
        "year_num": year_num,
        "month_num": month_num,
        "day_of_month": day_of_month,
        "day_of_week": day_of_week,
        "is_weekend": is_weekend,
        "week_of_year": week_of_year,
        "quarter_num": quarter_num,
        "quarter_label": quarter_label,
        "year_week_sunday": year_week_sunday,
        "year_week_monday": year_week_monday,
        "holiday_flag": is_holiday,
        "day_type": day_type,
        "day_since_2025": day_since_2025,
        "month_since_2025": month_since_2025
    }


# ==========================
# WRITE CSV
# ==========================
def write_csv(record):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f"dim_date_{record['date_id']}.csv"
    filepath = os.path.join(OUTPUT_DIR, filename)

    if os.path.exists(filepath):
        return filepath, 1

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(record.keys())
        writer.writerow(record.values())

    return filepath, 1


# ==========================
# INSERT INTO DB
# ==========================
def insert_into_db(record):
    conn = db_staging()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO dim_date (
            date_id, date_value, year_num, month_num,
            day_of_month, day_of_week, is_weekend, week_of_year,
            quarter_num, quarter_label,
            year_week_sunday, year_week_monday,
            holiday_flag, day_type,
            day_since_2025, month_since_2025
        )
        SELECT %s,%s,%s,%s,%s,%s,%s,%s,
               %s,%s,%s,%s,%s,%s,%s,%s
        FROM DUAL
        WHERE NOT EXISTS (SELECT 1 FROM dim_date WHERE date_id=%s)
    """, (
        record["date_id"], record["date_value"], record["year_num"], record["month_num"],
        record["day_of_month"], record["day_of_week"], record["is_weekend"], record["week_of_year"],
        record["quarter_num"], record["quarter_label"],
        record["year_week_sunday"], record["year_week_monday"],
        record["holiday_flag"], record["day_type"],
        record["day_since_2025"], record["month_since_2025"],
        record["date_id"]
    ))

    conn.commit()
    inserted = cur.rowcount
    cur.close()
    conn.close()
    return inserted


# ==========================
# MAIN PROCESS
# ==========================
def run():
    process_id = log_start()

    # NEW: Check Step E must be COMPLETED
    if not check_E_completed():
        print("Step E NOT COMPLETED → STOP T0")
        log_end(process_id, "SKIPPED", "E not completed")
        return

    try:
        ensure_dim_date()

        today = date.today()
        record = generate_record(today)

        filepath, rows_count = write_csv(record)
        inserted = insert_into_db(record)

        log_file(process_id, filepath, os.path.basename(filepath),
                 "COMPLETED", rows_count, note=f"Inserted={inserted}")

        log_end(process_id, "COMPLETED", f"Inserted={inserted}")

        print(f"T0 Done — {inserted} rows inserted")

    except Exception as e:
        log_end(process_id, "FAILED", str(e))
        print("ERROR:", e)


if __name__ == "__main__":
    run()
