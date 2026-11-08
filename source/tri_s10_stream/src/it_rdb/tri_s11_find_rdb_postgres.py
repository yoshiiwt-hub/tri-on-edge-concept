# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [PostgreSQL]
# Ver: 02.i.20261108    By: IWATA, Y.'s Agent AI
# Mod: pyflakes の指摘を整理（使っていない import・変数、置き換えの無い f-string を削除。動作は変えない）
########################################
上位 DB（PostgreSQL）に追記されたデータを、Stream へ取り込む入口

主要関数:
f_table_diff_bulk_trigger  # テーブルの差分を定期的に取得し、キューに流す。

位置づけ: 他システムが書き込むテーブルのデータを、エッジ側の後続処理（SMB・FTP 送信等）へ渡す。
設計意図: 前回の取得位置を SQLite に保存し、再起動しても取りこぼし・重複を抑える。1 回の取得件数を max_row で制限し、大量の追記は複数周期に分けて追いつく。接続に失敗したり途中で切れたりしたら、つなぎ直して読み直し（リトライ）、続けて失敗したら接続できない（not_connectable）とする。接続できない状態の記録・再接続の間隔は tri_s01_connect_state に任せる。
前提・制約: key 列は単調増加すること（更新時刻など）。値が過去に戻る行は取得されない。
"""
MODULE_NAME = "FIND_RDB"

"""standard library"""
from   datetime import datetime  as dt
from   datetime import timedelta as td
import os
import queue
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, Optional

"""external library"""
## pg8000（PostgreSQL）は tri_s19_rdb_postgres が使う

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
from   src.tri_s01_error_if import ErrorIF, SIGNAL_STOP
import src.it_rdb.tri_s19_rdb_postgres as rdb
from   src.tri_s01_connect_state import ConnectState
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

    [接続の扱い]
        リトライ: 接続の失敗（つながらない・途中で切れた）は、retry_wait_sec ごとにつなぎ直して読み直す（このトリガーの中だけ）
            連続 retry_max 回失敗したら、接続できない（not_connectable）と判定して ConnectState へ報告する（読めたら回数を 0 に戻す）
            接続は生きているのに失敗したとき（SQL の誤り等）は、今までどおり ErrorIF へ報告して次の周期に進む
        再接続: 接続できない間は reconnect_interval_sec ごとに試み、結果を ConnectState へ報告する
        接続の状態（ConnectState が登録。FIND_RDB）: connect_state / not_connectable_since / reconnect_fail_count / total_not_connectable
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
        retry_max              = int(config.get("retry_max", 3))
        retry_wait_sec         = float(config.get("retry_wait_sec", 5.0))
        reconnect_interval_sec = float(config.get("reconnect_interval_sec", 10.0))
        if retry_max < 1 or retry_wait_sec < 0:
            raise ValueError(f"config['retry_max'] must be >= 1 and config['retry_wait_sec'] >= 0. Got: {retry_max} / {retry_wait_sec}")
        pg_target = f"{config.get('pg_host')}:{config.get('pg_port')}/{config.get('pg_dbname')}"
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} pg={pg_target}  retry_max={retry_max}  retry_wait_sec={retry_wait_sec}  "
            f"reconnect_interval_sec={reconnect_interval_sec}", log_file)

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
            base.output_log(MODULE_NAME, "!!! WARNING !!! State DB Reset by config 'reset_state': True")
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

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None
    ##:try

    """======== 接続の状態（判定・リトライはこのトリガー。記録・再接続の間隔・通知は ConnectState） ========"""
    conn_pg    = None
    conn_state = ConnectState(TRIGGER_NAME, pg_target, reconnect_interval_sec, MODULE_NAME, log_file)
    fail_count = 0      # 連続失敗の回数（判定に使う。このトリガーの中だけで数える）

    def _responsive_wait(seconds: float) -> None:
        """stop_event を見ながら細かく待つ"""
        _end = time.time() + seconds
        while time.time() < _end and not stop_event.is_set():
            time.sleep(min(0.05, max(0.0, _end - time.time())))

    def _drop_pg() -> None:
        """接続を閉じる"""
        nonlocal conn_pg
        if conn_pg is not None:
            try: conn_pg.close()
            except Exception: pass
        conn_pg = None

    def _conn_lost() -> bool:
        """失敗のあと、接続が切れたかを確かめる（ロールバックしてから SELECT 1）。切れていれば True"""
        if conn_pg is None:
            return True
        try:
            conn_pg.rollback()
        except Exception:
            return True
        return not rdb.pg_ping(conn_pg)

    def _try_pg() -> Optional[BaseException]:
        """接続を確かめ、切れていればつなぎ直す（1回）。成功なら None、失敗ならその例外"""
        nonlocal conn_pg
        try:
            conn_pg = rdb.pg_connect_if_needed(conn_pg, config, TRIGGER_NAME, log_file)
            return None
        except Exception as e:
            _drop_pg()
            return e

    def _count_fail(e: BaseException, what: str) -> bool:
        """
        接続の失敗を1回数える。連続 retry_max 回で not_connectable と判定して ConnectState へ報告し、False を返す
        （続けるなら retry_wait_sec 待って True。リトライは画面のみに出し、ConnectState・ErrorIF へは報告しない）
        """
        nonlocal fail_count
        _drop_pg()
        fail_count += 1
        if fail_count >= retry_max:
            conn_state.set_not_connectable(e)   # 判定：連続 retry_max 回の失敗
            return False
        if stop_event.is_set():
            return False
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Retry ({fail_count}/{retry_max}). {what}  {pg_target}  ({type(e).__name__}: {e})")
        _responsive_wait(retry_wait_sec)
        return True

    def _run_with_retry(work, what: str) -> bool:
        """
        work() を実行する。接続の失敗（つながらない・途中で切れた）は retry_wait_sec ごとにつなぎ直して、最初からやり直す（リトライ）。
        接続は生きているのに失敗したとき（データのエラー）は、例外のまま返す。not_connectable になったら False
        """
        nonlocal fail_count
        while True:
            e = _try_pg()
            if e is None:
                try:
                    work()
                    fail_count = 0
                    return True
                except Exception as ex:
                    if not _conn_lost():
                        raise
                    e = ex
            if not _count_fail(e, what):
                return False

    def _reconnect_if_due() -> None:
        """接続できない間、再接続の間隔が過ぎていれば再接続を試み、結果を ConnectState へ報告する"""
        nonlocal fail_count
        if conn_state.should_try_reconnect():
            e = _try_pg()
            if e is None:
                fail_count = 0
                conn_state.set_connectable()
            else:
                conn_state.reconnect_failed(e)

    """初回接続（retry_wait_sec ごとに retry_max 回まで試みる）"""
    _err: Optional[BaseException] = None
    for _try in range(1, retry_max + 1):
        _err = _try_pg()
        if _err is None:
            break
        if _try < retry_max and not stop_event.is_set():
            base.output_log(MODULE_NAME,       # リトライは画面のみ
                f"{TRIGGER_NAME} Connect Retry ({_try}/{retry_max}). {pg_target}  ({type(_err).__name__}: {_err})")
            _responsive_wait(retry_wait_sec)
    if _err is not None:
        _sig = ErrorIF.report_exception(_err, phase="startup", category="comm",  # 初回接続の失敗を報告（A-21）
                                         trigger_id=TRIGGER_NAME, log_file=log_file)
        if _sig == SIGNAL_STOP:
            stop_event.set()
            return None
        conn_state.set_not_connectable(_err, report=False)   # ErrorIF へは上で phase=startup として通知済み

    count = 0

    def _fetch_tables() -> None:
        """全テーブルの差分を取得してキューに流し、SQLite に取得位置を保存する（接続の失敗は例外のまま返す）"""
        nonlocal count
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

    """======== LOOP START ========"""
    while not stop_event.is_set():
        try:
            # SQLite 接続チェック＆再接続
            conn_sqlite = utl.check_and_reconnect_db(conn_sqlite, db_path)
            cur_sqlite  = conn_sqlite.cursor()

            # 接続できない間：再接続の間隔ごとに再接続を試みる（読まない）
            if not conn_state.is_connectable():
                _reconnect_if_due()
                if not conn_state.is_connectable():
                    ErrorIF.emit_summary_if_needed()
                    _responsive_wait(min(2.0, reconnect_interval_sec))
                    continue

            # PostgreSQL から差分を取得（接続の失敗はリトライ。連続 retry_max 回で not_connectable）
            if not _run_with_retry(_fetch_tables, "Fetch Diff."):
                continue
            _responsive_wait(interval_sec)

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
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
