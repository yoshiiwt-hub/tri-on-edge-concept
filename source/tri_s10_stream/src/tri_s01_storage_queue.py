# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Storage Queue Module
# Ver: 01.a.20261107    By: IWATA, Y.'s Agent AI
# Mod: 新規作成（tri_s00_supervise_base にあった Storage Queue の処理を移した。動作は変えない）
########################################
StreamCore から Act へ、セグメントを SQLite のテーブル経由で渡す Storage Queue（stq）の部品

主要関数:
get_storage_queue_path / get_storage_queue_tab  # Storage Queue の DB ファイル（work/spool/_storage_queue.db）とテーブル名。
coldef_to_table_col                             # col_def から、テーブルの列定義を作る。
stq_initialize_db                               # DB を初期化する（テーブルとエラーテーブルを作り直す。StreamCore が起動時に呼ぶ）。
stq_prepare_sql                                 # Storage Queue 用の SQL 文を一括で作る。
stq_open                                        # Act が DB への接続を開く。
stq_insert_new_seg / stq_select_new_seg         # セグメントを登録する（StreamCore）/ 取り出して取得済みにする（Act）。
stq_update_seg / stq_update_rec                 # セグメント・レコードの状態（seg_status）を更新する。
stq_trans_seg_to_error / stq_trans_rec_to_error # エラーのセグメント・レコードをエラーテーブルへ移す。
stq_cleanup                                     # 処理済みの行を消し、エラーテーブルが増えすぎたら空にする。

位置づけ: StreamCore（tri_s10_stream_core）が登録し、QMODE = "stq" の Act（例: tri_s12_act_tmpl_queue の a_template_stq_trigger）が取り出す。
設計意図: Storage Queue の定義（パス・テーブル名・SQL・登録・取得・掃除）を1か所にまとめ、基盤（tri_s00_supervise_base）は
          パス・設定・ログ・状態の置き場という役割だけにする。
前提・制約: Storage Queue はストリームデータの一時保持で、起動のたびに初期化する。seg_status は 0 = 未取得 / 1 = 取得済み / 2 = 処理済み・エラー。
            base はこのモジュールを import しない（base は最下層。このモジュールが base を使う）。
"""
MODULE_NAME = "STORAGE_QUEUE"

"""standard library"""
from   datetime import datetime as dt
import os
import sqlite3
import sys
import time
import traceback
from   typing   import Dict

"""external library"""
## NOT Use

"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()

except ModuleNotFoundError:
    try: # Check and Add Path
        cur_dir  = os.path.dirname(os.path.abspath(__file__))
        dir_name = os.path.basename(cur_dir)
        if dir_name != 'src':
            raise FileNotFoundError(f".py MUST be in 'src' dir. Current: {dir_name}")

        root_dir = os.path.dirname(cur_dir)
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
## NOT Use（base のみ）

"""Storage Queue の DB ファイル・テーブル名"""
_STORAGE_QUEUE_FILE = "_storage_queue.db"   # work/spool の下に置く
_STORAGE_QUEUE_TAB  = "storage_queue"
STQ_ERROR_MAX       = 100_000               # エラーテーブルの行数の上限（超えたら stq_cleanup で空にする）


########################################
# Storage Queue Paths
########################################

def get_storage_queue_path() -> str:
    """Accessor for Storage Queue File（work/spool/_storage_queue.db）"""
    return os.path.join(base.get_work_sub_dirs()["spool"], _STORAGE_QUEUE_FILE)


def get_storage_queue_tab() -> str:
    """Accessor for Storage Queue Table Name"""
    return _STORAGE_QUEUE_TAB


########################################
# Storage Queue Functions
########################################

def coldef_to_table_col(
    col_def : Dict
) -> dict:
    """col_def から、Storage Queue のテーブルの列定義 {番号: [列名, SQLite の型, ""]} を作る"""
    _table_col = {}
    type_map = {
        "time" : "TEXT"   , "timestamp": "TEXT"   ,
        "str"  : "TEXT"   , "string"   : "TEXT"   ,
        "int"  : "INTEGER", "integer"  : "INTEGER",
        "float": "REAL"   , "real"     : "REAL"   ,
    }
    for key, val in sorted(col_def.items()):
        col_name = val[0]
        d_type = type_map.get(val[1].lower(), "TEXT")
        _table_col[key] = [col_name, d_type, ""]
    return _table_col


def stq_initialize_db(
    db_path : str,
    tab_name: str,
    col_def : Dict,
) -> sqlite3.Connection:
    """Storage Queue の DB を初期化する（テーブルとエラーテーブルを作り直す。StreamCore が起動時に1回呼ぶ）"""
    conn, cur = None, None
    try:
        """
        Define DB
        - STQはストリームデータの一時保持。次回起動時に初期化される
        - キュー用途であり、トランザクション処理はない（→journalは不要）
        - SSD寿命を考慮し、ファイル書込み処理は必要最低限にする
        """
        db_dir = os.path.dirname(db_path)
        if db_dir: os.makedirs(db_dir, exist_ok=True)
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        cur.execute("PRAGMA journal_mode = MEMORY;") # journal自体は不要だが、ロックの緩和のため設定
        cur.execute("PRAGMA synchronous  = OFF;") # ファイル書込みはOSに委ねる
        cur.execute("PRAGMA temp_store = MEMORY;") # 一時ファイルをRAMに置く
        cur.execute("PRAGMA page_size = 4096;") # 書込みサイズをSSDブロックサイズに合わせる（4096 or 8192）
        
        """Create Table"""
        _l = []
        for v in col_def.values(): _l.append(f"{v[0]} {v[1]} {v[2]}")
        _sql_base = ", ".join(_l)
        cur.execute(f"DROP TABLE IF EXISTS {tab_name}")
        cur.execute(f"CREATE TABLE {tab_name} ({_sql_base})")
        cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{tab_name}_main ON {tab_name} (seg_id, seg_ind)")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{tab_name}_status ON {tab_name} (seg_id, seg_status)")
        conn.commit()
        
        error_tab = f"{tab_name}_error"
        _l = []
        for v in col_def.values(): _l.append(f"{v[0]} {v[1]} {v[2]}")
        _l.append("error_ts  TEXT  DEFAULT (datetime('now','localtime'))")
        _l.append("error_msg TEXT")
        cur.execute(f"DROP TABLE IF EXISTS {error_tab}")
        _sql_base = ', '.join(_l)
        cur.execute(f"CREATE TABLE {error_tab} ({_sql_base})")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{error_tab}_seg_id ON {error_tab} (seg_id)")
        conn.commit()
        
    except Exception as e:
        base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", base.get_log_path())

    finally:
        if cur is not None: cur.close()
    return conn


def stq_prepare_sql(
    tab_name: str,
    tab_col: dict
) -> dict:
    """
    Storage Queue用のSQL文を一括生成する
    tab_col: coldef_to_table_col(col_def) の戻り値（列数のみ使用。呼び出し側はこの型で渡すこと。B-4）
    """
    _placeholders = ', '.join(['?'] * len(tab_col))
    error_tab = f"{tab_name}_error"
    _sql = {
    "insert_new_seg"     : f"INSERT INTO {tab_name} VALUES ({_placeholders})",
    "select_new_seg"     : f"SELECT * FROM {tab_name} WHERE seg_id = ? AND seg_status = 0 ORDER BY seg_ind ASC",
    "update_seg_status"  : f"UPDATE {tab_name} SET seg_status = ? WHERE seg_id = ?",
    "update_rec_status"  : f"UPDATE {tab_name} SET seg_status = ? WHERE seg_id = ? AND seg_ind = ?",
    "delete_by_status"   : f"DELETE FROM {tab_name} WHERE seg_status = ?",
    "trans_seg_to_error" : f"INSERT INTO {error_tab} SELECT *, ?, ? FROM {tab_name} WHERE seg_id = ?",
    "trans_rec_to_error" : f"INSERT INTO {error_tab} SELECT *, ?, ? FROM {tab_name} WHERE seg_id = ? AND seg_ind = ?",
    "count_error" : f"SELECT count(*) FROM {error_tab}",
    "clear_error" : f"DELETE FROM {error_tab}",
}
    return _sql


def stq_open(
    stq_path: str
) -> sqlite3.Connection:
    """Storage Queue の SQLite 接続を開く"""
    conn = sqlite3.connect(
        stq_path,
        timeout=10.0,
        check_same_thread=False
    )
    conn.execute("PRAGMA journal_mode = MEMORY;")
    conn.execute("PRAGMA synchronous  = OFF;")
    conn.execute("PRAGMA temp_store   = MEMORY;")
    return conn


def _to_sqlite_val(v):
    """datetimeをTEXTに変換"""
    if isinstance(v, dt):
        return v.strftime("%Y-%m-%d %H:%M:%S.%f")
    return v


def stq_insert_new_seg(
    conn : sqlite3.Connection,
    sql_i: str,
    rows : list
) -> None:
    """新規segmentを登録"""
    safe_rows = [
        [_to_sqlite_val(v) for v in row] for row in rows
    ]
    conn.executemany(sql_i, safe_rows)
    conn.commit()
    return None


def stq_select_new_seg(
    conn: sqlite3.Connection,
    sql_r: str,
    sql_u_seg: str,
    seg_id: str
) -> list:
    """新規segmentを取得し、seg_statusを更新"""
    rows = conn.execute(sql_r, (seg_id,)).fetchall()
    if not rows: return None
    
    conn.execute(sql_u_seg, (1, seg_id)) # 取得成功
    conn.commit()
    return rows


def stq_update_seg(
    conn: sqlite3.Connection,
    sql_u_seg: str,
    seg_id: str,
    status: int
) -> None:
    """seg全体のseg_statusを更新"""
    conn.execute(sql_u_seg, (status, seg_id))
    conn.commit()
    return None


def stq_update_rec(
    conn: sqlite3.Connection,
    sql_u_rec: str,
    seg_id: str,
    seg_ind: int,
    status: int
) -> None:
    """rec単位でseg_statusを更新"""
    conn.execute(sql_u_rec, (status, seg_id, seg_ind))
    conn.commit()
    return None


def stq_trans_seg_to_error(
    conn : sqlite3.Connection,
    sql_t : str,
    sql_u : str,
    seg_id : int,
    error_msg: str = ""
) -> None:
    """エラーsegmentをエラーテーブルに移動"""
    error_ts = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    conn.execute(sql_t, (error_ts, error_msg, seg_id))
    conn.execute(sql_u, (2, seg_id))
    conn.commit()
    return None


def stq_trans_rec_to_error(
    conn : sqlite3.Connection,
    sql_t : str,
    sql_u : str,
    seg_id : int,
    seg_ind : int,
    error_msg: str = ""
) -> None:
    """エラーrecordをエラーテーブルに移動"""
    error_ts = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    conn.execute(sql_t, (error_ts, error_msg, seg_id, seg_ind))
    conn.execute(sql_u, (2, seg_id, seg_ind))
    conn.commit()
    return None


def stq_cleanup(
    conn: sqlite3.Connection,
    sql_d: str,
    status_to_del: int,
    sql_err_cnt: str,
    sql_err_clr: str,
    cleanup_last_time: float,
    cleanup_proc_count: int,
    interval_sec: float,
    interval_proc: int
) -> tuple:
    clear_error = False

    """Storage Queue Table Clean-up"""
    should_cleanup = (
        time.time() - cleanup_last_time >= interval_sec or
        cleanup_proc_count >= interval_proc
    )
    if not should_cleanup:
        return cleanup_last_time, cleanup_proc_count, 0, clear_error

    conn.execute(sql_d, (status_to_del,))
    deleted = conn.execute("SELECT changes()").fetchone()[0]
    conn.commit()
    
    """Error Queue Table Clean-up"""
    n_error = conn.execute(sql_err_cnt).fetchone()[0]
    if n_error > STQ_ERROR_MAX:
        conn.execute(sql_err_clr)
        conn.commit()
        clear_error = True
    
    return time.time(), 0, deleted, clear_error
