import os
import re
import csv
import requests
from datetime import datetime
import pytz
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# --- Load biến môi trường từ file .env ---
load_dotenv()
FOLDER_LOCAL = os.getenv("FOLDER_LOCAL")

# --- URL nguồn ---
URLS = {}

# ===== LẤY DATA CỦA ENTRYPOINTS TRONG FILE CONFIG.CSV =====
def get_entrypoints(config_path):
    entrypoints = []

    try:
        # Đọc file CSV (MS-DOS hoặc UTF-8 đều ok)
        with open(config_path, mode='r', encoding='cp1258', newline='') as file:
            sample = file.read(2048)
            file.seek(0)
            dialect = csv.Sniffer().sniff(sample)
            reader = csv.DictReader(file, dialect=dialect)

            for row in reader:
                # Lấy những dòng có enabled = TRUE
                if row['enabled'].strip().upper() == 'TRUE':
                    entrypoints.append({
                        # 'id': row['ID'].strip(),
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

# ===== CÀO DATA TỪ CELLPHONES =====
import re
import pandas as pd

def crawl_cellphones():
    print("Crawling Cellphones...")
    soup = get_html(URLS["cellphones"])
    products = soup.select("div.product-info-container.product-item")
    data = []

    # --- Hàm làm sạch giá và % ---
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
        smember = p.select_one(".block-smem-price span")
        sstudent = p.select_one(".block-smem-price.edu span")
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
            "smember_discount": clean_price(smember.get_text(strip=True) if smember else None),
            "sstudent_discount": clean_price(sstudent.get_text(strip=True) if sstudent else None),
            "promotion": promotion.get_text(strip=True) if promotion else None,
            "installment": installment.get_text(strip=True) if installment else None,
            "rating": rating.get_text(strip=True) if rating else None,
        })

    return pd.DataFrame(data)

# ===== CÀO DATA TỪ THEGIOIDIDONG =====
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

        data.append({
            "data_id": p.get("data-id"),
            "product_name": p.get("data-name"),
            "product_url": "https://www.thegioididong.com" + p.select_one("a")["href"] if p.select_one("a") else None,
            "image_url": image_url,
            "price_current": p.select_one("strong.price").get_text(strip=True) if p.select_one("strong.price") else None,
            "price_original": p.select_one(".price-old").get_text(strip=True) if p.select_one(".price-old") else None,
            "discount_percent": p.select_one(".percent").get_text(strip=True) if p.select_one(".percent") else None,
            "brand": p.get("data-brand"),
            "screen_resolution": p.select_one(".item-compare span:nth-of-type(1)").get_text(strip=True) if p.select_one(".item-compare span:nth-of-type(1)") else None,
            "screen_size": p.select_one(".item-compare span:nth-of-type(2)").get_text(strip=True) if p.select_one(".item-compare span:nth-of-type(2)") else None,
            "variants": ", ".join([li.get_text(strip=True) for li in p.select(".merge__item")]),
            "installment_info": p.select_one(".shiping")["aria-label"] if p.select_one(".shiping") else None,
            "rating": p.select_one(".vote-txt b").get_text(strip=True) if p.select_one(".vote-txt b") else None,
            "sold_quantity": p.select_one(".rating_Compare span").get_text(strip=True).replace("•", "").strip() if p.select_one(".rating_Compare span") else None
        })

    return pd.DataFrame(data)

# --- Main ---
if __name__ == "__main__":
    configs = get_entrypoints("config.csv")

    for item in configs:
        URLS[item['source']] = item['url']

    df1 = crawl_cellphones()
    df1["source"] = "cellphones"

    df2 = crawl_tgdd()
    df2["source"] = "thegioididong"

    # Gộp 2 DataFrame lại
    df = pd.concat([df1, df2], ignore_index=True)

    # === Thời gian hiện tại theo múi giờ Việt Nam ===
    vn_tz = pytz.timezone("Asia/Ho_Chi_Minh")
    now = datetime.now(vn_tz)
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")

    # === Tạo folder nếu chưa có ===
    if not os.path.exists(FOLDER_LOCAL):
        os.makedirs(FOLDER_LOCAL, exist_ok=True)
        print(f"Đã tạo thư mục: {FOLDER_LOCAL}")

    # === Tạo tên file đẹp ===
    filename = f"phones_source_{timestamp}.csv"
    output_path = os.path.join(FOLDER_LOCAL, filename)

    # === Xuất ra file CSV ===
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print("SUCCESS")


