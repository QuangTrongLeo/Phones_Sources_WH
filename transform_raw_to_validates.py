import argparse
import os
from datetime import datetime, date
from typing import List, Dict

import pandas as pd
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv
import hashlib

# =============== ENV ===============
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"))

TARGET_SCHEMA = "dw_staging"
TARGET_TABLE  = "phones_validated"
RAW_TABLE     = "phones_raw"
DEFAULT_MAPPING_FILENAME = "field_mapping.csv"

# BUSINESS KEY mặc định: source + product_url
BUSINESS_KEY = [k.strip() for k in (os.getenv("BUSINESS_KEY") or "source,product_url").split(",") if k.strip()]

def build_db_url_from_env() -> str | None:
    url = os.getenv("DW_DB_URL")
    if url:
        return url
    host = os.getenv("MYSQL_HOST") or os.getenv("HOST") or "localhost"
    port = (os.getenv("MYSQL_PORT") or os.getenv("PORT") or "3306").strip()
    db   = os.getenv("MYSQL_DB")   or os.getenv("DB")   or "dw_staging"
    user = os.getenv("MYSQL_USER") or os.getenv("USER")
    pwd  = os.getenv("MYSQL_PASSWORD") or os.getenv("PASS")
    if user and pwd:
        return f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{db}"
    return None

# =============== IO HELPERS ===============
def resolve_mapping_path(path_arg: str | None) -> str:
    if path_arg:
        p = os.path.abspath(path_arg)
        if os.path.exists(p):
            return p
    p = os.path.join(BASE_DIR, DEFAULT_MAPPING_FILENAME)
    if not os.path.exists(p):
        raise FileNotFoundError(f"Không thấy {DEFAULT_MAPPING_FILENAME} tại: {p}")
    return p

def read_csv_robust(path: str) -> pd.DataFrame:
    # chịu được DECIMAL(12,0) không bọc nháy
    import io, re
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        raw = f.read()
    def _swap(m):
        txt = m.group(0)
        return re.sub(r'(?<=\()(\s*\d+)\s*,\s*(\d+\s*)(?=\))', r'\1|\2', txt)
    raw2 = re.sub(r'\b(DECIMAL|NUMERIC|DOUBLE|FLOAT)\s*\(\s*\d+\s*,\s*\d+\s*\)', _swap, raw, flags=re.I)
    df = pd.read_csv(io.StringIO(raw2), dtype=str).fillna("")
    if "target_datatype" in df.columns:
        df["target_datatype"] = df["target_datatype"].str.replace("|", ",", regex=False)
    return df

# =============== LOAD MAPPING ===============
REQUIRED_COLS = [
    "target_field", "source_field", "target_datatype",
    "tgt_nullable", "is_primary_key", "pk_position",
    "active_flag", "target_table"
]

def load_mapping(mapping_path: str) -> pd.DataFrame:
    df = read_csv_robust(mapping_path)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Thiếu cột bắt buộc trong mapping: {missing}")
    df = df[df["active_flag"].astype(str).str.upper().isin(["1","TRUE","Y","YES","ON","T"])]
    df = df[df["target_table"] == TARGET_TABLE]
    for c in ["target_field","source_field","target_datatype","tgt_nullable","is_primary_key","pk_position"]:
        df[c] = df[c].astype(str).str.strip()
    df["pk_position"] = pd.to_numeric(df["pk_position"], errors="coerce")
    df = df.drop_duplicates(subset=["target_field"], keep="first")
    if df.empty:
        raise ValueError("Mapping rỗng sau khi lọc (active_flag/target_table).")
    return df

# =============== CAST & CANONICALIZATION ===============
def to_decimal(s: pd.Series) -> pd.Series:
    s = s.fillna("").astype(str).str.replace(r"[^\d,.\-]", "", regex=True)
    s = s.str.replace(",", "", regex=False)  # bỏ dấu ngăn nghìn
    return pd.to_numeric(s, errors="coerce")

def to_int(s: pd.Series) -> pd.Series:
    s = s.fillna("").astype(str).str.replace(r"[^\d\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def to_date(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
    return dt.dt.date

def cast_by_datatype(series: pd.Series, target_datatype: str) -> pd.Series:
    dt = (target_datatype or "").upper().strip()
    if dt.startswith(("DECIMAL","NUMERIC","DOUBLE","FLOAT")):
        return to_decimal(series)
    if dt.startswith(("TINYINT","SMALLINT","INT","BIGINT")):
        return to_int(series)
    if dt.startswith("DATE"):
        return to_date(series)
    # VARCHAR/TEXT/... giữ nguyên text
    return series.fillna("").astype(str)

def canonicalize_scalar(v, dtype_str: str) -> str:
    """Chuẩn hoá 1 giá trị theo datatype để hash. Không dùng lower-case, giữ nguyên chữ hoa/thường."""
    if pd.isna(v):
        return ""
    dt = (dtype_str or "").upper().strip()
    # số: biểu diễn ổn định (không để '12.0' vs '12')
    if isinstance(v, (int, float)) or dt.startswith(("DECIMAL","NUMERIC","DOUBLE","FLOAT","TINYINT","SMALLINT","INT","BIGINT")):
        try:
            # ưu tiên làm float rồi loại .0 nếu là số nguyên
            fv = float(v)
            if fv.is_integer():
                return str(int(fv))
            else:
                # cắt đuôi zero không cần thiết
                s = f"{fv:.10f}".rstrip("0").rstrip(".")
                return s
        except Exception:
            return str(v).strip()
    # date: YYYY-MM-DD
    if dt.startswith("DATE"):
        try:
            dv = pd.to_datetime(v, errors="coerce")
            return "" if pd.isna(dv) else dv.strftime("%Y-%m-%d")
        except Exception:
            return ""
    # text
    return str(v).strip()

def compute_row_hash_typed(df: pd.DataFrame, typing: Dict[str, str], cols: List[str]) -> pd.Series:
    """Băm theo thứ tự cột + kiểu dữ liệu mapping, bỏ qua execute/load time."""
    def _hash_row(row):
        parts = []
        for c in cols:
            parts.append(canonicalize_scalar(row[c], typing.get(c, "")))
        payload = "||".join(parts)
        return hashlib.md5(payload.encode("utf-8")).hexdigest()
    return df.apply(_hash_row, axis=1)

# =============== DDL: CREATE TABLE IF NOT EXISTS ===============
def ensure_validated_table(engine, mapping: pd.DataFrame):
    insp = inspect(engine)
    if insp.has_table(TARGET_TABLE, schema=TARGET_SCHEMA):
        return
    if "pk_position" in mapping.columns:
        mapping = mapping.sort_values(by=["pk_position","target_field"], na_position="last")
    col_defs: List[str] = []
    pk_cols: List[str]  = []
    for _, r in mapping.iterrows():
        name = r["target_field"]
        dtype = r["target_datatype"] or "VARCHAR(255)"
        nullable = (r["tgt_nullable"] or "1").upper()
        null_sql = "NULL" if nullable in ["1","TRUE","Y","YES","ON","T"] else "NOT NULL"
        col_defs.append(f"`{name}` {dtype} {null_sql}")
        if (r["is_primary_key"] or "0").upper() in ["1","TRUE","Y","YES","ON","T"]:
            pk_cols.append(f"`{name}`")
    col_defs.append("`execute_time` DATETIME NOT NULL")
    col_defs.append("`load_stagging_time` DATETIME NULL")
    ddl = f"""
    CREATE TABLE {TARGET_SCHEMA}.{TARGET_TABLE} (
      {',\n      '.join(col_defs)}
      {(',' if pk_cols else '')}
      {('PRIMARY KEY (' + ','.join(pk_cols) + ')' ) if pk_cols else ''}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """.strip()
    with engine.begin() as conn:
        conn.execute(text(ddl))

# =============== TRANSFORM RAW ===============
def transform_raw(engine, mapping: pd.DataFrame) -> pd.DataFrame:
    raw = pd.read_sql(f"SELECT * FROM {TARGET_SCHEMA}.{RAW_TABLE}", con=engine, dtype=str).fillna("")
    if raw.empty:
        return pd.DataFrame()
    out = pd.DataFrame(index=raw.index)
    for _, r in mapping.iterrows():
        s_col = r["source_field"]
        t_col = r["target_field"]
        t_dt  = r["target_datatype"]
        series = raw[s_col] if s_col in raw.columns else pd.Series([""] * len(raw), index=raw.index)
        out[t_col] = cast_by_datatype(series, t_dt)
    now = datetime.now()
    out["execute_time"] = now
    out["load_stagging_time"] = now
    return out

# =============== LẤY BẢN “MỚI NHẤT” TỪ VALIDATED ===============
def load_latest_validated(engine, need_cols: List[str]) -> pd.DataFrame:
    cols = list(set(need_cols + BUSINESS_KEY + ["execute_time"]))
    cols_sql = ", ".join([f"`{c}`" for c in cols if c != "execute_time"]) + ", `execute_time`"
    # Fallback 2 bước cho MySQL không window:
    base = pd.read_sql(f"SELECT {cols_sql} FROM {TARGET_SCHEMA}.{TARGET_TABLE}", con=engine)
    if base.empty:
        return base
    base = base.sort_values(by=["execute_time"], ascending=False)
    dedup = base.drop_duplicates(subset=BUSINESS_KEY, keep="first").reset_index(drop=True)
    return dedup

# =============== DIM_DATE UPSERT ===============
DIM_DATE_UPSERT_SQL = """
INSERT INTO dw_staging.dim_date
(date_id, date_value, year_num, quarter_num, month_num, month_name, day_of_month,
 day_of_week, day_name, week_of_year, iso_year, iso_week)
VALUES (
  :date_id,
  :date_value,
  YEAR(:date_value),
  QUARTER(:date_value),
  MONTH(:date_value),
  DATE_FORMAT(:date_value, '%M'),
  DAY(:date_value),
  WEEKDAY(:date_value)+1,
  DATE_FORMAT(:date_value, '%W'),
  WEEK(:date_value, 0),
  YEARWEEK(:date_value, 3) DIV 100,
  YEARWEEK(:date_value, 3) MOD 100
)
ON DUPLICATE KEY UPDATE
  date_value = VALUES(date_value)
"""

def ensure_dim_date(engine, day: date):
    date_id = int(day.strftime("%Y%m%d"))
    with engine.begin() as conn:
        conn.execute(text(DIM_DATE_UPSERT_SQL), {"date_id": date_id, "date_value": day})

# =============== MAIN ===============
def main():
    ap = argparse.ArgumentParser(description="Transform + change-detect (no DB schema change)")
    ap.add_argument("--db-url", default=None, help="Override DB URL (nếu bỏ trống sẽ lấy từ .env)")
    ap.add_argument("--mapping-csv", default=None, help="Đường dẫn field_mapping.csv (mặc định: cạnh script)")
    args = ap.parse_args()

    db_url = args.db_url or build_db_url_from_env()
    if not db_url:
        raise SystemExit("Thiếu cấu hình DB. Thiết lập .env (DW_DB_URL hoặc MYSQL_*), hoặc truyền --db-url.")

    engine = create_engine(db_url, pool_pre_ping=True)

    # 1) mapping + ensure table
    mapping_path = resolve_mapping_path(args.mapping_csv)
    mapping = load_mapping(mapping_path)
    ensure_validated_table(engine, mapping)

    # cột nghiệp vụ để hash (KHÔNG gồm execute/load time)
    target_cols = mapping["target_field"].tolist()
    # map kiểu dữ liệu theo từng cột
    dtype_map: Dict[str, str] = dict(zip(mapping["target_field"], mapping["target_datatype"]))

    # 2) transform từ RAW
    df_new = transform_raw(engine, mapping)
    if df_new.empty:
        print("Không có dữ liệu trong phones_raw.")
        return

    # 3) load bản mới nhất hiện có từ validated
    df_latest = load_latest_validated(engine, target_cols)

    # 4) tính hash typed cho NEW và LATEST (bỏ 2 cột thời gian)
    df_new_hash = df_new.copy()
    df_new_hash["row_hash"] = compute_row_hash_typed(df_new_hash, dtype_map, target_cols)

    if df_latest.empty:
        to_insert = df_new_hash.copy()
    else:
        df_latest_hash = df_latest.copy()
        df_latest_hash["row_hash"] = compute_row_hash_typed(df_latest_hash, dtype_map, target_cols)

        # 5) so sánh theo BUSINESS_KEY + row_hash
        merged = df_new_hash.merge(
            df_latest_hash[BUSINESS_KEY + ["row_hash"]],
            on=BUSINESS_KEY, how="left", suffixes=("", "_old")
        )
        # cần insert khi: chưa có (_old NaN) hoặc hash khác nhau
        candidates = merged[(merged["row_hash_old"].isna()) | (merged["row_hash"] != merged["row_hash_old"])].drop(columns=["row_hash_old"])

        # 6) Safety check lần nữa: nếu vì lý do format nào đó hash khác nhưng giá trị cột bằng nhau -> loại
        if not candidates.empty:
            latest_lookup = df_latest_hash.set_index(BUSINESS_KEY)
            def is_really_changed(row):
                key = tuple(row[k] for k in BUSINESS_KEY)
                if key not in latest_lookup.index:
                    return True
                old_row = latest_lookup.loc[key]
                # so sánh từng cột target: đưa về chuỗi chuẩn theo datatype rồi so
                for c in target_cols:
                    new_v = canonicalize_scalar(row[c], dtype_map.get(c, ""))
                    old_v = canonicalize_scalar(old_row[c], dtype_map.get(c, ""))
                    if new_v != old_v:
                        return True
                return False
            mask = candidates.apply(is_really_changed, axis=1)
            to_insert = candidates[mask].copy()
        else:
            to_insert = candidates.copy()

    if to_insert.empty:
        print("Dữ liệu không thay đổi; không insert thêm.")
        return

    # 7) Ghi dữ liệu cần insert (bỏ cột row_hash vì ta không đổi schema DB)
    write_cols = target_cols + ["execute_time","load_stagging_time"]
    to_insert[write_cols].to_sql(
        TARGET_TABLE, con=engine, schema=TARGET_SCHEMA,
        if_exists="append", index=False, chunksize=5000, method="multi"
    )
    print(f"Đã ghi {len(to_insert)} bản ghi (mới/thay đổi) vào {TARGET_SCHEMA}.{TARGET_TABLE}")

    # 8) đảm bảo dim_date
    exec_day = to_insert["execute_time"].iloc[0].date()
    ensure_dim_date(engine, exec_day)
    print(f"Đã đảm bảo dim_date có ngày {exec_day}")

if __name__ == "__main__":
    main()
