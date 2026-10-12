# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [PostgreSQL]
# Ver: 02.f.20261001    By: IWATA, Y.'s Agent AI
# Mod: モジュール名を tri_edge_XX_* から tri_sXX_* 形式に変更（tri_edge_11_find_rdb → tri_s11_find_rdb_postgres）
########################################
f_table_diff_bulk_trigger  # PostgreSQL テーブルの差分を定期取得してキューに流す

動作概要
    1. table_key_list で指定したスキーマ・テーブル・キー列を対象に差分を取得
    2. SQLite の table_state DB に前回の最終タイムスタンプを保存・復元
    3. ポーリングごとに PostgreSQL に接続し、前回タイムスタンプ以降の新規行を取得
    4. 取得結果をメタ情報（テーブル名・列名・dt_fr/dt_to 等）と合わせて queue_out に送信
    5. 接続断時は再接続を試みる（最大 pg_reconnect_max 回）

接続設定
    pg_host          : PostgreSQL ホスト（環境変数 PG_HOST / デフォルト: "localhost"）
    pg_port          : PostgreSQL ポート（環境変数 PG_PORT / デフォルト: 5432）
    pg_dbname        : データベース名（環境変数 PG_DB / デフォルト: "postgres"）
    pg_user          : ユーザ名（環境変数 PG_USER / デフォルト: "postgres"）
    pg_password      : パスワード（環境変数 PG_PASS / デフォルト: "postgres"）
    table_key_list   : 監視対象テーブルリスト [{schema, table, key}, ...]（必須）
    initial_lookback_hours: 初回起動時の遡及時間（デフォルト: 24 時間 / max_past_hours でも指定可）
    interval_sec     : ポーリング間隔秒（デフォルト: 10）
    max_row          : 1回の差分取得上限行数（デフォルト: 1000）
    reset_state      : SQLite 状態 DB をリセットするか（デフォルト: false）
    pg_reconnect_max : 再接続試行最大回数（デフォルト: 3）
    pg_reconnect_wait: 再接続待機時間秒（デフォルト: 5.0）
"""
MODULE_NAME = "FIND_RDB"

"""standard library"""
from   datetime import datetime  as dt
from   datetime import timedelta as td
import json
import os
import queue
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Tuple, Sequence, Optional

"""external library"""
import pg8000.dbapi as pg_dbapi

"""[Bootstrap] Resolve Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base    
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()
    
except ModuleNotFoundError:
    try: # Check and Add Path
        dir_valid = 'it_rdb'  ## (system_root)/src/.
        cur_dir  = os.path.dirname(os.path.abspath(__file__))
        dir_name = os.path.basename(cur_dir)
        if dir_name != dir_valid:
            raise FileNotFoundError(f".py MUST be in {dir_valid} . Current: {dir_name}")
        
        root_dir = os.path.abspath(os.path.join(cur_dir, "..", ".."))
        if root_dir not in sys.path:
            sys.path.append(root_dir)
        
        for i in range(10):
            if cur_dir in sys.path:
                sys.path.remove(cur_dir)
            else:
                break
        
        import src.tri_s00_supervise_base as base
        sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully after Path Adjusted.\n")
        sys.stdout.flush()
        
    except Exception as e:
        msg = f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]    {e}\n{traceback.format_exc()}"
        sys.stdout.write(f"{msg}\n")
        sys.stdout.flush()
        with open(f"{MODULE_NAME}_Import_Error.log", "a", encoding="utf-8-sig") as f:
            f.write(f"{msg}\n")
        sys.exit(1)

"""local module"""
import src.tri_s01_error_if as err
from   src.tri_s01_error_if import ErrorIF, SIGNAL_STOP
import src.it_rdb.tri_s19_rdb_postgres as rdb
import src.tri_s19_util as utl

"""system dir, file"""
system_root = base.get_system_root()
log_dir  = base.get_log_dir()
log_file = base.get_log_path()
work_dir       = base.get_work_dir()
work_db_dir    = base.get_work_sub_dirs().get("db"   , os.path.join(work_dir, "db"))
work_run_dir   = base.get_work_sub_dirs().get("run"  , os.path.join(work_dir, "run"))
work_spool_dir = base.get_work_sub_dirs().get("spool", os.path.join(work_dir, "spool"))

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__=='__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# Find Trigger Functions
########################################

def _any_dt_to_str(ts_val):  ##:def
    """文字列でもdtでも 20260221_100000_000 にする"""
    if not ts_val:
        return "00000000_000000_000"
    if isinstance(ts_val, str):
        return ts_val.replace("-", "").replace(":", "").replace(" ", "_").replace(".", "_")
    else:
        return ts_val.strftime("%Y%m%d_%H%M%S_%f")[:-3]


def f_table_diff_bulk_trigger(
    queue_out: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    Postgreから差分を取得しキューに流す
    Mod: 2026.05.12    By: IWATA,Y.
    """
    TRIGGER_NAME = "f_table_diff_bulk_trigger"
    
    try:
        """ SETTING """
        config = rdb.pg_merge_env(config) # os.env

        table_key_list = config.get("table_key_list", None)
        if table_key_list is None:
            raise ValueError("Config Error: 'table_key_list' is missing or empty.")
        
        required_keys = {"schema", "table", "key"}
        for d in table_key_list:
            if not required_keys.issubset(d.keys()):
                raise ValueError(f"Config Error: Missing required keys {required_keys}. Your config: {d}")
        
        initial_lookback_hours = int(config.get("initial_lookback_hours", config.get("max_past_hours", 24)))
        default_start          = (dt.now() - td(hours=initial_lookback_hours)).strftime("%Y-%m-%d %H:%M:%S")
        base.output_log(MODULE_NAME, f"initial_lookback_hours={initial_lookback_hours}  default_start={default_start}", log_file)
        interval_sec      = config.get("interval_sec", 10)
        max_row           = config.get("max_row", 1000)
        reset_state       = config.get("reset_state", False)
        pg_reconnect_max  = int(config.get("pg_reconnect_max" , 3))
        pg_reconnect_wait = float(config.get("pg_reconnect_wait", 5.0))

        # --- SQLite 状態DB の準備 ---
        db_path     = os.path.join(work_db_dir, f"{TRIGGER_NAME}_state.db")
        conn_sqlite = None
        conn_sqlite = utl.check_and_reconnect_db(conn_sqlite, db_path)
        cur_sqlite  = conn_sqlite.cursor()
        
        cur_sqlite.execute('''
            CREATE TABLE IF NOT EXISTS table_state (
                target_table TEXT PRIMARY KEY,
                last_ts TEXT
            )
        ''')
        if reset_state:
            cur_sqlite.execute('DELETE FROM table_state')
            base.output_log(MODULE_NAME, f"!!! WARNING !!! State DB Reset by config 'reset_state': True")
        conn_sqlite.commit()
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Initialized. DB: {db_path}")
        
        # --- 各テーブルの設定・初期状態ロード ---
        target_tables = []
        for d in table_key_list:
            _sch = utl.sanitize_str(d["schema"])
            _tab = utl.sanitize_str(d["table"])
            
            _sch_tab    = f"{_sch}_{_tab}"
            _sch_tab_pg = f'"{_sch}"."{_tab}"'
            
            _key    = utl.sanitize_str(d["key"])
            _key_pg = f'"{_key}"'
            
            cur_sqlite.execute('SELECT last_ts FROM table_state WHERE target_table = ?', (_sch_tab,))
            res    = cur_sqlite.fetchone()
            _state = res[0] if res else default_start
            
            # ターゲットリスト： 0=_sch_tab, 1=_sch_tab_pg, 2=_key, 3=_key_pg, 4=_state
            target_tables.append([_sch_tab, _sch_tab_pg, _key, _key_pg, _state])
            base.output_log(MODULE_NAME, f"target_tables={target_tables}")
        ##:for

        # --- PostgreSQL 初期接続 ---
        conn_pg = None
        conn_pg = rdb.pg_connect_if_needed(conn_pg, config, TRIGGER_NAME, log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None
    ##:try

    """======== LOOP START ========"""
    count = 0
    while not stop_event.is_set():
        try:
            # SQLite 接続チェック＆再接続
            conn_sqlite = utl.check_and_reconnect_db(conn_sqlite, db_path)
            cur_sqlite  = conn_sqlite.cursor()

            # PostgreSQL 接続チェック＆再接続
            for _retry in range(pg_reconnect_max + 1):
                try:
                    conn_pg = rdb.pg_connect_if_needed(conn_pg, config, TRIGGER_NAME, log_file)
                    break
                except pg_dbapi.DatabaseError as e_conn:
                    base.output_log(MODULE_NAME,
                        f"PostgreSQL 接続失敗 ({_retry+1}/{pg_reconnect_max}): {e_conn}", log_file)
                    if _retry >= pg_reconnect_max:
                        raise
                    time.sleep(pg_reconnect_wait)
                    conn_pg = None
            ##:for

            for t_info in target_tables:
                _sch_tab, _sch_tab_pg, _key, _key_pg, _state = t_info
                
                # 1. 差分データを取得
                rows = rdb.pg_fetch_diff(conn_pg, _sch_tab_pg, _key_pg, _state, limit=max_row)
                if not rows:
                    continue
                
                # 2. データの最終行のタイムスタンプを取得
                new_last_ts = rows[-1].get(_key)
                if not new_last_ts:
                    new_last_ts = rows[-1].get(_key.lower())
                t_info[4] = new_last_ts

                # 3. メタ情報付きでキューに送信
                dt_exec_str = dt.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
                dt_fr_str   = _any_dt_to_str(_state)[:15]
                dt_to_str   = _any_dt_to_str(new_last_ts)[:15]
                payload = [{
                    "col_def"   : col_def,
                    "table_name": _sch_tab,
                    "columns"   : list(rows[0].keys()),
                    "rows"      : rows,
                    "dt_fr": _state     , "dt_fr_str": dt_fr_str,
                    "dt_to": new_last_ts, "dt_to_str": dt_to_str,
                    "dt_fr_to_str": f"[{dt_fr_str}_to_{dt_to_str}]",
                    "dt_exec_str_yyyymm"             : dt_exec_str[:6],
                    "dt_exec_str_yyyymmdd"           : dt_exec_str[:8],
                    "dt_exec_str_yyyymmdd_hhmmss"    : dt_exec_str[:15],
                    "dt_exec_str_yyyymmdd_hhmmss_000": dt_exec_str,
                }]
                queue_out.put((count, payload))
                
                # 4. SQLiteに状態保存
                cur_sqlite.execute('''
                    INSERT INTO table_state (target_table, last_ts) 
                    VALUES (?, ?) 
                    ON CONFLICT(target_table) DO UPDATE SET last_ts=excluded.last_ts
                ''', (_sch_tab, str(new_last_ts)))
                conn_sqlite.commit()
                
                time.sleep(0.5)
                queue_out.put((count, ['____']))
                base.output_log(MODULE_NAME, f"Queued {len(rows)} rows from {_sch_tab_pg} between {_state} ~ {new_last_ts}.")
                
                count += 1
                time.sleep(0.5)
            ##:for
            
            time.sleep(interval_sec)

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            time.sleep(0.5)
            continue
        ##:try
    ##:while
    """======== LOOP END ========"""

    if conn_sqlite:
        conn_sqlite.close()
    if conn_pg is not None:
        try:
            conn_pg.close()
        except Exception:
            pass
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Worker Stop.", log_file)
##:def


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, f"test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
