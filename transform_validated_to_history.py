#!/usr/bin/env python3
"""
transform_validated_to_history.py

SCD Type 2 loader from phones_validated -> phones_history.

- Uses mysql.connector (no SQLAlchemy).
- Writes process_log rows into phones_controller.process_log (controller DB).
- Uses GET_LOCK on controller DB to avoid concurrent runs.
- Uses buffered cursors or fetchall() to avoid "Unread result found".
- CLI flags: --debug / -d ; --force / -f ; optional batch size (int)

Save and run:
    python transform_validated_to_history.py --debug
"""

import os
import sys
import hashlib
import traceback
from datetime import datetime, timedelta
from dotenv import load_dotenv
import mysql.connector

# ---------------------------
# 0. CONFIG / ENV
# ---------------------------
load_dotenv(".env")  # uses .env in working dir

DB_HOST = os.getenv("DB_HOSTNAME", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USERNAME")
DB_PASS = os.getenv("DB_PASSWORD")
DB_STAGING = os.getenv("DB_STAGING", "phones_staging")          # where phones_validated & phones_history live
DB_CONTROLLER = os.getenv("DB_CONTROLLER", "phones_controller")# where process_log lives

if not DB_USER or not DB_PASS:
    raise RuntimeError("DB_USERNAME and DB_PASSWORD must be set in .env")

PROCESS_STEP = "T2"
LOCK_NAME = "scd2_phones_lock"
LOCK_TIMEOUT = 5  # seconds
IGNORE_COLS = {"SID", "dataTimeExpired", "is_current", "runtime_md5", "execute_at", "load_staging_time", "bk_hash"}

# ---------------------------
# 1. DB CONNECTION HELPERS
# ---------------------------
def conn_staging():
    """Connection to staging DB (phones_staging)"""
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_STAGING,
        autocommit=False
    )

def conn_controller():
    """Connection to controller DB (phones_controller)"""
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_CONTROLLER,
        autocommit=True  # for GET_LOCK management and logging we can autocommit
    )

# ---------------------------
# 2. UTIL FUNCTIONS
# ---------------------------
def compute_md5_for_row_dict(d: dict, ignore_cols=None) -> str:
    """Compute MD5 deterministically over columns (sorted keys), represent None as <NULL>."""
    if ignore_cols is None:
        ignore_cols = set()
    keys = [k for k in d.keys() if k not in ignore_cols]
    keys.sort()
    parts = []
    for k in keys:
        v = d.get(k)
        if v is None:
            parts.append("<NULL>")
        else:
            # Normalize types: decimals, datetimes -> string
            parts.append(str(v))
    payload = "||".join(parts)
    return hashlib.md5(payload.encode("utf-8")).hexdigest()

def yesterday_end_of_day() -> datetime:
    y = (datetime.now() - timedelta(days=1)).date()
    return datetime(y.year, y.month, y.day, 23, 59, 59)

def sanitize_param(v):
    """Return Python-MySQL friendly value. mysql.connector will manage types."""
    if v is None:
        return None
    # If it's a datetime-like with tzinfo, remove tzinfo
    if hasattr(v, "tzinfo") and v.tzinfo is not None:
        return v.replace(tzinfo=None)
    return v

# ---------------------------
# 3. PROCESS LOG HELPERS (phones_controller.process_log)
# ---------------------------
def acquire_job_lock(ctrl_conn, lock_name=LOCK_NAME, timeout=LOCK_TIMEOUT):
    """
    Acquire named lock (MySQL GET_LOCK) using controller connection.
    Returns True if acquired.
    We use controller connection so lock is visible across jobs.
    """
    cur = ctrl_conn.cursor(buffered=True)
    try:
        cur.execute("SELECT GET_LOCK(%s, %s)", (lock_name, timeout))
        r = cur.fetchone()
        # r could be (1,) or (0,) or (NULL,)
        cur.fetchall()  # ensure we consumed (safe)
        acquired = (r is not None and r[0] == 1)
        return acquired
    finally:
        cur.close()

def release_job_lock(ctrl_conn, lock_name=LOCK_NAME):
    cur = ctrl_conn.cursor(buffered=True)
    try:
        cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
        cur.fetchone()
        cur.fetchall()
    finally:
        cur.close()

def log_insert_running(ctrl_conn, step):
    """
    Insert a RUNNING row into controller.process_log and return the inserted id.
    Uses autocommit connection.
    """
    cur = ctrl_conn.cursor(buffered=True)
    try:
        now = datetime.now()
        cur.execute("INSERT INTO process_log (step, status, start_time) VALUES (%s, 'RUNNING', %s)", (step, now))
        # mysql.connector with autocommit True commits automatically
        cur.execute("SELECT LAST_INSERT_ID()")
        lid = cur.fetchone()[0]
        # consume
        cur.fetchall()
        return lid, now
    finally:
        cur.close()

def log_end(ctrl_conn, log_id, status, note=None):
    cur = ctrl_conn.cursor(buffered=True)
    try:
        now = datetime.now()
        cur.execute("UPDATE process_log SET status=%s, end_time=%s, note=%s WHERE id=%s", (status, now, note, log_id))
        cur.fetchall()
    finally:
        cur.close()

def log_insert_skipped(ctrl_conn, step, reason):
    cur = ctrl_conn.cursor(buffered=True)
    try:
        now = datetime.now()
        cur.execute("INSERT INTO process_log (step, status, start_time, end_time, note) VALUES (%s, 'SKIPPED', %s, %s, %s)", (step, now, now, reason))
        cur.fetchall()
    finally:
        cur.close()

def check_L1_completed():
    """
    Return True if there exists a row in phones_controller.process_log
    with step='L1' AND status='COMPLETED'.
    """
    ctrl = conn_controller()
    cur = ctrl.cursor(dictionary=True, buffered=True)
    try:
        cur.execute("SELECT end_time FROM process_log WHERE step='L1' AND status='COMPLETED' ORDER BY end_time DESC LIMIT 1")
        row = cur.fetchone()
        cur.fetchall()
        return bool(row)
    finally:
        cur.close()
        ctrl.close()

# ---------------------------
# 4. METADATA helpers
# ---------------------------
def get_history_columns(st_conn):
    """
    Read columns of phones_history table (returns set of column names).
    Use buffered cursor and fetchall to avoid unread-results problem.
    """
    cur = st_conn.cursor(buffered=True)
    try:
        cur.execute("SELECT * FROM phones_history LIMIT 0")
        # fetchall to clear any internal buffers (returns [])
        cur.fetchall()
        cols = [c[0] for c in cur.description]
        return set(cols)
    finally:
        cur.close()

# ---------------------------
# 5. CORE SCD2 FLOW (numbered explanation + implementation)
# ---------------------------
def scd2_t2(batch_limit=None, debug=False, force=False):
    """
    Main flow — numbered:
    1) Pre-check L1 completed
      1.1) If L1 not completed -> insert SKIPPED into controller.process_log and exit
    2) Acquire job-level lock (GET_LOCK) on controller DB
    3) Insert RUNNING row into controller.process_log
    4) Load phones_validated from staging
      4.1) If no rows -> mark COMPLETE and exit
    5) For each business key (source||product_url) process:
      5.1) compute MD5 of row (ignore control cols)
      5.2) SELECT current phones_history row FOR UPDATE (to lock)
      5.3) If none -> INSERT new record (dataTimeExpired = 2100..; is_current=1; runtime_md5)
      5.4) If exists:
         - if md5 equal and not force -> do nothing (DUPLICATE)
         - else md5 different or force -> UPDATE old: set dataTimeExpired = yesterday 23:59:59 and is_current=0, then INSERT new
    6) On success -> update controller.process_log to COMPLETED with summary note
    7) On error -> update controller.process_log to FAILED with error note and release lock
    8) Always release job lock
    """
    # 1) Check L1
    if debug:
        print(f"[{PROCESS_STEP}] START — batch={batch_limit}, debug={debug}, force={force}")
    if not check_L1_completed():
        reason = "L1 not completed; skipping T2"
        if debug:
            print(f"[{PROCESS_STEP}] SKIPPED: {reason}")
        ctrl = conn_controller()
        try:
            log_insert_skipped(ctrl, PROCESS_STEP, reason)
        finally:
            ctrl.close()
        return

    # 2) Acquire job lock (on controller)
    ctrl_conn = conn_controller()
    got_lock = acquire_job_lock(ctrl_conn, LOCK_NAME, LOCK_TIMEOUT)
    if not got_lock:
        if debug:
            print(f"[{PROCESS_STEP}] Could not acquire lock '{LOCK_NAME}', exiting")
        ctrl_conn.close()
        return
    if debug:
        print(f"[{PROCESS_STEP}] Lock acquired on controller DB")

    last_log_id = None
    st_conn = None
    try:
        # 3) Insert RUNNING into controller.process_log
        last_log_id, start_time = log_insert_running(ctrl_conn, PROCESS_STEP)
        if debug:
            print(f"[{PROCESS_STEP}] Inserted RUNNING id={last_log_id}")

        # 4) Load validated rows from staging (buffered)
        st_conn = conn_staging()
        cur = st_conn.cursor(dictionary=True, buffered=True)
        try:
            sql = "SELECT * FROM phones_validated"
            if batch_limit:
                sql += f" LIMIT {int(batch_limit)}"
            cur.execute(sql)
            rows = cur.fetchall()  # read all -> avoid unread-result
            # rows is list of dicts
        finally:
            cur.close()

        if not rows:
            note = "No rows in phones_validated"
            log_end(ctrl_conn, last_log_id, "COMPLETED", note)
            if debug:
                print(f"[{PROCESS_STEP}] COMPLETED — {note}")
            return

        # ensure business key available & history columns
        # add business_key to every row (source||product_url)
        for r in rows:
            # ensure keys exist
            r["source"] = r.get("source")
            r["product_url"] = r.get("product_url")
            r["business_key"] = (str(r.get("source") or "").strip() + "||" + str(r.get("product_url") or "").strip())

        history_cols = get_history_columns(st_conn)

        inserted = 0
        closed = 0

        # group by business_key: we will take the last occurrence if multiple
        # simple approach: build dict mapping bk->row (last wins)
        latest_by_bk = {}
        for r in rows:
            latest_by_bk[r["business_key"]] = r

        # 5) Process each BK
        for bk, row in latest_by_bk.items():
            # 5.1 compute md5
            new_md5 = compute_md5_for_row_dict(row, IGNORE_COLS)
            if debug:
                print(f"[{PROCESS_STEP}] BK={bk} md5={new_md5}")

            # 5.2 Start a DB transaction on staging to do SELECT ... FOR UPDATE then updates/inserts
            tx_cursor = st_conn.cursor(dictionary=True, buffered=True)
            try:
                
                # SELECT current row (is_current=1) FOR UPDATE
                tx_cursor.execute(
                    "SELECT SID, runtime_md5 FROM phones_history WHERE source=%s AND product_url=%s AND is_current=1 FOR UPDATE",
                    (row.get("source"), row.get("product_url"))
                )
                currow = tx_cursor.fetchone()  # dict or None
                tx_cursor.fetchall()  # consume any remaining (safe)
                # currow may be None

                if currow is None:
                    # 5.3 NEW -> insert
                    # prepare payload dictionary containing only columns that exist in phones_history
                    payload = {}
                    for c, v in row.items():
                        if c in {"id", "business_key"}:
                            continue
                        if c not in history_cols:
                            continue
                        payload[c] = sanitize_param(v)
                    # add SCD control columns
                    if "dataTimeExpired" in history_cols:
                        payload["dataTimeExpired"] = datetime(2100, 12, 31, 23, 59, 59)
                    if "is_current" in history_cols:
                        payload["is_current"] = 1
                    if "runtime_md5" in history_cols:
                        payload["runtime_md5"] = new_md5

                    if not payload:
                        # nothing to insert (no matching cols)
                        st_conn.rollback()
                        if debug:
                            print(f"[{PROCESS_STEP}] BK={bk} nothing to insert (no matching history columns)")
                        continue

                    cols = ", ".join(f"`{c}`" for c in payload.keys())
                    vals = ", ".join("%s" for _ in payload.keys())
                    params = tuple(payload.values())
                    tx_cursor.execute(f"INSERT INTO phones_history ({cols}) VALUES ({vals})", params)
                    st_conn.commit()
                    inserted += 1
                    if debug:
                        print(f"[{PROCESS_STEP}] BK={bk} inserted (NEW)")

                else:
                    # 5.4 Exists -> compare md5s
                    sid_old = currow.get("SID")
                    old_md5 = currow.get("runtime_md5")

                    if (not force) and old_md5 and (old_md5 == new_md5):
                        # duplicate -> no action
                        st_conn.rollback()
                        if debug:
                            print(f"[{PROCESS_STEP}] BK={bk} duplicate -> no action")
                        continue

                    # changed -> close old
                    yend = yesterday_end_of_day()
                    tx_cursor.execute("UPDATE phones_history SET dataTimeExpired=%s, is_current=0 WHERE SID=%s AND is_current=1", (yend, sid_old))
                    # insert new
                    payload = {}
                    for c, v in row.items():
                        if c in {"id", "business_key"}:
                            continue
                        if c not in history_cols:
                            continue
                        payload[c] = sanitize_param(v)
                    if "dataTimeExpired" in history_cols:
                        payload["dataTimeExpired"] = datetime(2100, 12, 31, 23, 59, 59)
                    if "is_current" in history_cols:
                        payload["is_current"] = 1
                    if "runtime_md5" in history_cols:
                        payload["runtime_md5"] = new_md5

                    if payload:
                        cols = ", ".join(f"`{c}`" for c in payload.keys())
                        vals = ", ".join("%s" for _ in payload.keys())
                        params = tuple(payload.values())
                        tx_cursor.execute(f"INSERT INTO phones_history ({cols}) VALUES ({vals})", params)
                        st_conn.commit()
                        inserted += 1
                        closed += 1
                        if debug:
                            print(f"[{PROCESS_STEP}] BK={bk} closed SID={sid_old} and inserted new")
                    else:
                        # nothing to insert -> rollback the update too
                        st_conn.rollback()
                        if debug:
                            print(f"[{PROCESS_STEP}] BK={bk} had change but nothing to insert (no matching cols)")

            except Exception as e:
                st_conn.rollback()
                raise
            finally:
                tx_cursor.close()

        # 6) finalize: mark controller process_log as COMPLETED
        note = f"Inserted={inserted};Closed={closed}"
        log_end(ctrl_conn, last_log_id, "COMPLETED", note)
        if debug:
            print(f"[{PROCESS_STEP}] COMPLETED — {note}")
            print(" T2 PROCESS COMPLETED SUCCESSFULLY")
            print(" Inserted:", inserted)
            print(" Closed  :", closed)
            print(" Status  : COMPLETED (logged in phones_controller.process_log)")
            print(" Step    : T2")

    except Exception as e:
        
        # 7) On any failure, set FAILED
        err = str(e)[:2000]
        try:
            if last_log_id:
                log_end(ctrl_conn, last_log_id, "FAILED", err)
            else:
                # insert quick FAILED record
                log = conn_controller()
                try:
                    cur = log.cursor(buffered=True)
                    now = datetime.now()
                    cur.execute("INSERT INTO process_log (step, status, start_time, end_time, note) VALUES (%s, 'FAILED', %s, %s, %s)", (PROCESS_STEP, now, now, err))
                    cur.fetchall()
                finally:
                    cur.close()
                    log.close()
        except Exception:
            pass
        print(f"[{PROCESS_STEP}] FAILED: {err}")
        traceback.print_exc()
        raise
    finally:
        # 8) Always release lock and close conns
        try:
            release_job_lock(ctrl_conn, LOCK_NAME)
        except Exception:
            pass
        try:
            ctrl_conn.close()
        except Exception:
            pass
        try:
            if st_conn:
                st_conn.close()
        except Exception:
            pass
        

# ---------------------------
# 6. CLI parsing
# ---------------------------
def parse_cli(argv):
    debug = False
    force = False
    batch = None
    for a in argv[1:]:
        if a in ("--debug", "-d"):
            debug = True
        elif a in ("--force", "-f"):
            force = True
        else:
            try:
                batch = int(a)
            except Exception:
                pass
    return batch, debug, force

# ---------------------------
# 7. ENTRYPOINT
# ---------------------------
if __name__ == "__main__":
    batch_limit, debug_flag, force_flag = parse_cli(sys.argv)
    try:
        scd2_t2(batch_limit=batch_limit, debug=debug_flag, force=force_flag)
    except Exception as e:
        # already logged inside
        sys.exit(1)
