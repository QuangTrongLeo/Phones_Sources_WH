import argparse
import os
from datetime import datetime, date
from typing import List
import pandas as pd
from sqlalchemy import create_engine, text, inspect

# CONFIG 
DEFAULT_DB_URL = os.getenv(
    "DW_DB_URL",
    "mysql+mysqlconnector://root:170804@localhost:3306/dw_staging"
)
TARGET_SCHEMA = "dw_staging"
TARGET_TABLE  = "phones_validated"
DEFAULT_MAPPING_FILENAME = "field_mapping.csv"


#  IO HELPERS 
def resolve_mapping_path(path_arg: str | None) -> str:
    """Ưu tiên đường dẫn do người dùng truyền; nếu không, lấy file cạnh script."""
    if path_arg:
        p = os.path.abspath(path_arg)
        if os.path.exists(p):
            return p
    base_dir = os.path.dirname(os.path.abspath(__file__))
    p = os.path.join(base_dir, DEFAULT_MAPPING_FILENAME)
    if not os.path.exists(p):
        raise FileNotFoundError(f"Không thấy {DEFAULT_MAPPING_FILENAME} tại: {p}")
    return p


def read_csv_robust(path: str) -> pd.DataFrame:
    """Đọc CSV với các encoding hay gặp."""
    for enc in ("utf-8", "utf-8-sig", "cp1258"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc).fillna("")
        except Exception:
            pass
    return pd.read_csv(path, dtype=str).fillna("")


# LOAD MAPPING (ĐÚNG FORMAT MỚI)
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

    # chỉ lấy active + đúng bảng đích
    df = df[df["active_flag"].astype(str).str.upper().isin(["1","TRUE","Y","YES","ON","T"])]
    df = df[df["target_table"] == TARGET_TABLE]

    # chuẩn hoá kiểu & pk_position
    df["target_field"]   = df["target_field"].astype(str).str.strip()
    df["source_field"]   = df["source_field"].astype(str).str.strip()
    df["target_datatype"]= df["target_datatype"].astype(str).str.strip()
    df["tgt_nullable"]   = df["tgt_nullable"].astype(str).str.strip()
    df["is_primary_key"] = df["is_primary_key"].astype(str).str.strip()
    df["pk_position"]    = pd.to_numeric(df["pk_position"], errors="coerce")

    # bỏ trùng theo target_field
    df = df.drop_duplicates(subset=["target_field"], keep="first")

    if df.empty:
        raise ValueError("Mapping rỗng sau khi lọc (active_flag/target_table).")

    return df


# CAST HELPERS 
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
    if dt.startswith(("DECIMAL", "NUMERIC", "DOUBLE", "FLOAT")):
        return to_decimal(series)
    if dt.startswith(("TINYINT", "SMALLINT", "INT", "BIGINT")):
        return to_int(series)
    if dt.startswith("DATE"):
        return to_date(series)
    # VARCHAR/TEXT/... giữ nguyên text
    return series.fillna("").astype(str)


#  DDL: TẠO BẢNG ĐÍCH NẾU CHƯA CÓ 
def ensure_validated_table(engine, mapping: pd.DataFrame):
    insp = inspect(engine)
    if insp.has_table(TARGET_TABLE, schema=TARGET_SCHEMA):
        return  # đã tồn tại

    # sắp xếp theo pk_position nếu có
    if "pk_position" in mapping.columns:
        mapping = mapping.sort_values(by=["pk_position", "target_field"], na_position="last")

    col_defs: List[str] = []
    pk_cols: List[str]  = []

    for _, r in mapping.iterrows():
        name = r["target_field"]
        dtype = r["target_datatype"] or "VARCHAR(255)"
        nullable = (r["tgt_nullable"] or "1").upper()
        null_sql = "NULL" if nullable in ["1", "TRUE", "Y", "YES", "ON", "T"] else "NOT NULL"
        col_defs.append(f"`{name}` {dtype} {null_sql}")
        if (r["is_primary_key"] or "0").upper() in ["1", "TRUE", "Y", "YES", "ON", "T"]:
            pk_cols.append(f"`{name}`")

    # + 2 cột thời gian theo yêu cầu
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


# TRANSFORM
def transform_raw(engine, mapping: pd.DataFrame) -> pd.DataFrame:
    raw = pd.read_sql(f"SELECT * FROM {TARGET_SCHEMA}.phones_raw", con=engine, dtype=str).fillna("")
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


# DIM_DATE UPSERT 
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


# MAIN 
def main():
    ap = argparse.ArgumentParser(description="Transform dw_staging.phones_raw -> dw_staging.phones_validated (no-drop, append)")
    ap.add_argument("--db-url", default=DEFAULT_DB_URL, help="VD: mysql+mysqlconnector://user:pass@host:3306/dw_staging")
    ap.add_argument("--mapping-csv", default=None, help="(tuỳ chọn) Đường dẫn field_mapping.csv; mặc định: cùng thư mục script")
    args = ap.parse_args()

    engine = create_engine(args.db_url, pool_pre_ping=True)

    mapping_path = resolve_mapping_path(args.mapping_csv)
    mapping = load_mapping(mapping_path)

    ensure_validated_table(engine, mapping)

    df_out = transform_raw(engine, mapping)
    if df_out.empty:
        print("Không có dữ liệu trong dw_staging.phones_raw để transform.")
        return

    df_out.to_sql(
        TARGET_TABLE, con=engine, schema=TARGET_SCHEMA,
        if_exists="append", index=False, chunksize=5000, method="multi"
    )
    print(f"Đã ghi {len(df_out)} bản ghi vào {TARGET_SCHEMA}.{TARGET_TABLE}")

    exec_day = df_out["execute_time"].iloc[0].date()
    ensure_dim_date(engine, exec_day)
    print(f"Đã đảm bảo dim_date có ngày {exec_day}")


if __name__ == "__main__":
    main()
