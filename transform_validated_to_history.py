import os
import sys
import hashlib
import traceback
from datetime import datetime, timedelta
import pytz
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# ---------- Configuration ----------
load_dotenv()

DB_HOST = os.getenv("DB_HOSTNAME", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USERNAME")
DB_PASS = os.getenv("DB_PASSWORD")
DB_STAGING = os.getenv("DB_STAGING", "phones_staging")          # where phones_validated & phones_history live
DB_CONTROLLER = os.getenv("DB_CONTROLLER", "phones_controller")# where process_log lives

if not DB_USER or not DB_PASS:
    raise RuntimeError("DB_USERNAME and DB_PASSWORD must be set in .env")

PROCESS_LOG_TABLE = f"{DB_CONTROLLER}.process_log"

TZ = pytz.timezone("Asia/Ho_Chi_Minh")
STEP_NAME = "T2"
LOCK_NAME = "scd2_phones_lock"
MAX_LOCK_WAIT = 5  # seconds to wait for GET_LOCK

# ---------- DB engine (connect to staging DB; controller table referenced fully-qualified) ----------
url = URL.create(
    "mysql+pymysql",
    username=DB_USER,
    password=DB_PASS,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_STAGING,
    query={"charset": "utf8mb4"}
)
engine = create_engine(url, future=True)


# ---------- Helpers ----------
def compute_md5_for_row(row: pd.Series, ignore_cols=None) -> str:
    if ignore_cols is None:
        ignore_cols = set()
    keys = [c for c in row.index if c not in ignore_cols]
    keys.sort()
    parts = []
    for k in keys:
        v = row.get(k)
        if pd.isna(v):
            parts.append("<NULL>")
        else:
            parts.append(str(v))
    payload = "||".join(parts)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()


def yesterday_end_of_day() -> datetime:
    now = datetime.now(TZ)
    y = now.date() - timedelta(days=1)
    return datetime(y.year, y.month, y.day, 23, 59, 59)


def acquire_lock(conn, lock_name: str, timeout: int) -> bool:
    try:
        r = conn.execute(text("SELECT GET_LOCK(:k, :t)"), {"k": lock_name, "t": timeout})
        return r.scalar() == 1
    except Exception:
        return False


def release_lock(conn, lock_name: str) -> bool:
    try:
        r = conn.execute(text("SELECT RELEASE_LOCK(:k)"), {"k": lock_name})
        return r.scalar() == 1
    except Exception:
        return False


def sanitize_value(v):
    if pd.isna(v):
        return None
    if hasattr(v, "to_pydatetime"):
        return v.to_pydatetime()
    return v


def get_history_columns(conn) -> set:
    # read column names of phones_history once (safe in production)
    r = conn.execute(text("SELECT * FROM phones_history LIMIT 0"))
    return set(r.keys())


# ---------- New: process_log helpers & L1 check ----------
def check_L1_completed(conn) -> bool:
    """
    Return True if there exists a row in phones_controller.process_log
    with step='L1' AND status='COMPLETED' (most recent).
    """
    q = text(f"""
        SELECT end_time FROM {PROCESS_LOG_TABLE}
        WHERE step = 'L1' AND status = 'COMPLETED'
        ORDER BY end_time DESC LIMIT 1
    """)
    r = conn.execute(q).first()
    return r is not None


def log_start_controller(conn, step):
    """
    Insert RUNNING into phones_controller.process_log and return inserted id and start_time.
    conn: a SQLAlchemy Connection (connected to DB_STAGING host)
    """
    start_time = datetime.now(TZ).replace(tzinfo=None)
    q_ins = text(f"INSERT INTO {PROCESS_LOG_TABLE} (step, status, start_time, note) VALUES (:step, 'RUNNING', :st, NULL)")
    conn.execute(q_ins, {"step": step, "st": start_time})
    last_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
    return last_id, start_time


def log_end_controller(conn, log_id, status, note=None):
    end_time = datetime.now(TZ).replace(tzinfo=None)
    q_upd = text(f"UPDATE {PROCESS_LOG_TABLE} SET status = :status, end_time = :et, note = :note WHERE id = :id")
    conn.execute(q_upd, {"status": status, "et": end_time, "note": note, "id": log_id})


def log_skipped_controller(conn, reason):
    """
    Insert a SKIPPED record for T2 (useful when L1 not completed).
    We set start_time and end_time both to now and status 'SKIPPED'.
    """
    now = datetime.now(TZ).replace(tzinfo=None)
    q = text(f"INSERT INTO {PROCESS_LOG_TABLE} (step, status, start_time, end_time, note) VALUES (:s, 'SKIPPED', :st, :et, :n)")
    conn.execute(q, {"s": STEP_NAME, "st": now, "et": now, "n": reason})


# ---------- Core SCD2 process ----------
def scd2_t2(batch_limit=None, debug=False, force_update=False):
    conn_main = engine.connect()
    last_log_id = None

    try:
        # --- Pre-check: L1 completed?
        if not check_L1_completed(conn_main):
            reason = "L1 not completed yet; skipping T2"
            print(f"[{STEP_NAME}] SKIPPED: {reason}")
            # Insert SKIPPED into controller log so you can see why it didn't run
            try:
                log_skipped_controller(conn_main, reason)
            except Exception as e:
                if debug:
                    print("[T2] Could not insert SKIPPED in process_log:", e)
            return

        # --- Acquire job-level lock
        got = acquire_lock(conn_main, LOCK_NAME, timeout=MAX_LOCK_WAIT)
        if not got:
            print(f"[{STEP_NAME}] Could not acquire lock '{LOCK_NAME}' - exiting")
            return

        if debug:
            # debug info about connection
            try:
                db_name = conn_main.execute(text("SELECT DATABASE()")).scalar()
            except Exception:
                db_name = None
            print(f"[{STEP_NAME}] Lock acquired. Engine DB: {db_name}; process_log target: {PROCESS_LOG_TABLE}")

        # Insert RUNNING into controller.process_log and get id
        last_log_id, start_time = log_start_controller(conn_main, STEP_NAME)
        if debug:
            print(f"[{STEP_NAME}] process_log RUNNING inserted id={last_log_id}")

        # Load validated (optionally limited)
        sql_valid = "SELECT * FROM phones_validated"
        if batch_limit:
            sql_valid += f" LIMIT {int(batch_limit)}"
        df_valid = pd.read_sql(sql_valid, conn_main)

        if df_valid.empty:
            note = "No rows in phones_validated"
            log_end_controller(conn_main, last_log_id, "COMPLETED", note)
            release_lock(conn_main, LOCK_NAME)
            print(f"[{STEP_NAME}] COMPLETED - {note}")
            return

        # Ensure BK columns
        if "source" not in df_valid.columns or "product_url" not in df_valid.columns:
            raise RuntimeError("phones_validated must contain 'source' and 'product_url'")

        df_valid["business_key"] = df_valid["source"].astype(str).str.strip() + "||" + df_valid["product_url"].astype(str).str.strip()

        ignore_cols = {"SID", "dataTimeExpired", "is_current", "runtime_md5", "execute_at", "load_staging_time", "bk_hash"}
        history_cols = get_history_columns(conn_main)  # production: stable schema, cheap call once

        inserted = 0
        closed = 0

        # process per business key; pick last row per BK in batch
        for bk, group in df_valid.groupby("business_key", sort=False):
            row = group.iloc[-1]
            new_md5 = compute_md5_for_row(row, ignore_cols=ignore_cols)
            if debug:
                print(f"[{STEP_NAME}] BK={bk} md5={new_md5}")

            # per-BK transaction (separate transaction)
            with engine.begin() as tx:
                q = text("""SELECT SID, runtime_md5 FROM phones_history
                            WHERE source = :source AND product_url = :product_url AND is_current = 1
                            FOR UPDATE
                        """)
                cur = tx.execute(q, {"source": row["source"], "product_url": row["product_url"]}).mappings().fetchone()

                if cur is None:
                    # NEW -> insert
                    payload = row.to_dict()
                    payload["dataTimeExpired"] = datetime(2100, 12, 31, 23, 59, 59)
                    payload["is_current"] = 1
                    payload["runtime_md5"] = new_md5

                    params = {}
                    for k, v in payload.items():
                        if k in {"id", "business_key"}:
                            continue
                        if k not in history_cols:
                            continue
                        params[k] = sanitize_value(v)

                    if not params:
                        if debug:
                            print(f"[{STEP_NAME}] BK={bk} nothing to insert (no matching history cols)")
                        continue

                    cols = ", ".join(f"`{c}`" for c in params.keys())
                    vals = ", ".join(f":{c}" for c in params.keys())
                    tx.execute(text(f"INSERT INTO phones_history ({cols}) VALUES ({vals})"), params)
                    inserted += 1
                    if debug:
                        print(f"[{STEP_NAME}] BK={bk} inserted (NEW)")

                else:
                    sid_old = cur["SID"]
                    old_md5 = cur["runtime_md5"]

                    if not force_update and old_md5 and old_md5 == new_md5:
                        if debug:
                            print(f"[{STEP_NAME}] BK={bk} duplicate -> no action")
                        continue

                    # CHANGED -> close old then insert new
                    yend = yesterday_end_of_day().strftime("%Y-%m-%d %H:%M:%S")
                    tx.execute(text("UPDATE phones_history SET dataTimeExpired = :dte, is_current = 0 WHERE SID = :sid AND is_current = 1"),
                               {"dte": yend, "sid": sid_old})
                    closed += 1
                    if debug:
                        print(f"[{STEP_NAME}] BK={bk} closed SID={sid_old}")

                    payload = row.to_dict()
                    payload["dataTimeExpired"] = datetime(2100, 12, 31, 23, 59, 59)
                    payload["is_current"] = 1
                    payload["runtime_md5"] = new_md5

                    params = {}
                    for k, v in payload.items():
                        if k in {"id", "business_key"}:
                            continue
                        if k not in history_cols:
                            continue
                        params[k] = sanitize_value(v)

                    if not params:
                        if debug:
                            print(f"[{STEP_NAME}] BK={bk} nothing to insert after change")
                        continue

                    cols = ", ".join(f"`{c}`" for c in params.keys())
                    vals = ", ".join(f":{c}" for c in params.keys())
                    tx.execute(text(f"INSERT INTO phones_history ({cols}) VALUES ({vals})"), params)
                    inserted += 1
                    if debug:
                        print(f"[{STEP_NAME}] BK={bk} inserted (CHANGED)")

        # finalize process_log -> COMPLETED
        note = f"Inserted={inserted};Closed={closed}"
        log_end_controller(conn_main, last_log_id, "COMPLETED", note)
        release_lock(conn_main, LOCK_NAME)
        print(f"[{STEP_NAME}] COMPLETED — {note}")

    except Exception as e:
        # on any failure, update process_log to FAILED
        err = str(e)[:2000]
        try:
            if last_log_id:
                log_end_controller(conn_main, last_log_id, "FAILED", err)
            else:
                # attempt insert FAILED record
                conn_main.execute(text(f"INSERT INTO {PROCESS_LOG_TABLE} (step, status, start_time, end_time, note) VALUES (:s, 'FAILED', :st, :et, :n)"),
                                  {"s": STEP_NAME, "st": datetime.now(TZ).replace(tzinfo=None), "et": datetime.now(TZ).replace(tzinfo=None), "n": err})
        except Exception:
            pass

        release_lock(conn_main, LOCK_NAME)
        print(f"[{STEP_NAME}] FAILED: {err}")
        traceback.print_exc()
        raise

    finally:
        try:
            conn_main.close()
        except Exception:
            pass


# ---------- CLI parsing ----------
def parse_cli(argv):
    debug = False
    force = False
    batch = None
    args = [a for a in argv[1:]]
    for a in args:
        if a in ("--debug", "-d"):
            debug = True
        elif a in ("--force", "-f"):
            force = True
        else:
            try:
                if batch is None:
                    batch = int(a)
            except Exception:
                pass
    return batch, debug, force


if __name__ == "__main__":
    batch_limit, debug_flag, force_flag = parse_cli(sys.argv)
    if debug_flag:
        print("Starting T2", {"batch_limit": batch_limit, "debug": debug_flag, "force_update": force_flag, "proc_log": PROCESS_LOG_TABLE})
    scd2_t2(batch_limit, debug=debug_flag, force_update=force_flag)
