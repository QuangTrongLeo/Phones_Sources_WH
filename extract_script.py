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

# --- Load biến môi trường từ file .env ---
load_dotenv()
FOLDER_LOCAL = os.getenv("FOLDER_LOCAL")

# --- URL nguồn ---
URLS = {}

# ===== LẤY DATA CỦA ENTRYPOINTS TRONG FILE CONFIG.CSV =====
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


# --- Hàm tiện ích ---
def get_html(url):
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


# --- Hàm làm sạch dữ liệu ---
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


# ===== CÀO DỮ LIỆU CELLPHONES =====
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


# ===== CÀO DỮ LIỆU TGDD =====
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


# ===== GỘP CỘT =====
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


# ===== CHẠY CHÍNH =====
def init():
    print("Bắt đầu cào dữ liệu...")

    # --- Đảm bảo luôn đọc đúng file config.csv trong cùng thư mục script ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "config.csv")
    configs = get_entrypoints(config_path)

    # --- Nếu không có config hợp lệ thì dừng ---
    if not configs:
        print("Không tìm thấy entrypoints hợp lệ trong config.csv, dừng chương trình.")
        return

    # --- Gán URL vào dict ---
    for item in configs:
        URLS[item['source']] = item['url']

    # --- Cào dữ liệu ---
    df1 = crawl_cellphones()
    df1["source"] = "cellphones"

    df2 = crawl_tgdd()
    df2["source"] = "thegioididong"

    df = pd.concat([df1, df2], ignore_index=True)
    df = merge_duplicate_columns(df)

    # --- Thêm cột thời gian cào ---
    vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    now = datetime.now(vn_tz)
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")

    df["collected_at"] = timestamp

    # --- Sắp xếp cột ---
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

    # --- Xuất file ---
    if not os.path.exists(FOLDER_LOCAL):
        os.makedirs(FOLDER_LOCAL, exist_ok=True)

    filename = f"phones_source_{timestamp}.csv"
    output_path = os.path.join(FOLDER_LOCAL, filename)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"SUCCESS — Đã xuất file: {output_path}\n")



# ===== LỊCH TRÌNH TỰ ĐỘNG =====
def schedule_jobs():
    #schedule.every(30).seconds.do(init)
    schedule.every(1).minutes.do(init)
    # schedule.every().hour.do(init)
    # schedule.every().day.at("00:00").do(init)

    print("Đang chạy chế độ tự động... (Nhấn Ctrl + C để dừng)")

    try:
        while True:
            schedule.run_pending()
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nĐã dừng chương trình.")

# --- Main ---
if __name__ == "__main__":
    init()
    schedule_jobs()
