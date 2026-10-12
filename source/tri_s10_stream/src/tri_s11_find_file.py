# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [File]
# Ver: 07.d.20261001    By: IWATA, Y.'s Agent AI
# Mod: モジュール名を tri_edge_XX_* から tri_sXX_* 形式に変更（tri_edge_11_find_file → tri_s11_find_file）
########################################
f_find_file_trigger   # フォルダを監視し、条件に該当したファイル情報をキューに流す

動作概要
    1. f_find_file_trigger: monitor_dir をポーリングし、未処理ファイルを発見したらファイルメタ情報をキュー送信
       - SQLite の seen_files DB で重複処理を防止
       - pfx_sfx フィルタ・stable_check・ACK チェック・古ファイル除外に対応
    2. stop_event が立つまでループを継続

出力列（f_find_file_trigger）
    0: file_path
    1: dir_path
    2: dir_base_name
    3: file_name
    4: base_name
    5: ext
    6: size_kb
    7: updated_time
    8: yyyymmdd
    9: yyyy
    10: mm
    11: dd
    12: hh

接続設定（f_find_file_trigger）
    monitor_dir    : 監視対象ディレクトリ（デフォルト: "."）
    search_subdir  : サブディレクトリを再帰的に検索するか（デフォルト: false）
    pfx_sfx        : ファイル名フィルタ [[接頭語], [接尾語]]（デフォルト: ["", ".json"]）
    stable_check_n : 安定チェック回数（デフォルト: 1）
    check_ack      : ACK ファイルチェックを行うか（デフォルト: false）
    ack_file_ext   : ACK ファイル拡張子（デフォルト: ".done"）
    ignore_old     : 古いファイルを無視するか（デフォルト: true）
    ignore_old_hour: 無視する閾値時間（デフォルト: 168 = 7日）
    copy_found     : 発見ファイルをローカルにコピーするか（デフォルト: false）
    copy_to        : コピー先ディレクトリ（デフォルト: "work/spool"）
    interval_sec   : ポーリング間隔秒（デフォルト: 2）
"""
MODULE_NAME = "FIND_File"

"""standard library"""
import csv
from   datetime import datetime as dt
import json
import os
import queue
import random
import shutil # copy file
import socket
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Tuple, Sequence, Optional

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
import src.tri_s01_error_if       as err
from   src.tri_s01_error_if       import ErrorIF, SIGNAL_STOP
import src.tri_s19_util        as utl

"""system dir, file"""
system_root = base.get_system_root()
log_dir  = base.get_log_dir()
log_file = base.get_log_path()
work_dir = base.get_work_dir()
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

def f_find_file_trigger(
    queue_out: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
    ) -> None:
    """
    フォルダを監視し、条件に該当したファイル名をキューに流す
    Mod: 2026.03.31    By: IWATA,Y.
    """
    TRIGGER_NAME = "f_find_file_trigger"
    
    try:
        """ SETTING """
        monitor_dir = os.path.join(system_root, config.get("monitor_dir", "."))
        os.makedirs(monitor_dir, exist_ok=True)
        search_subdir = config.get("search_subdir", False)
        pfx_sfx = config.get("pfx_sfx", ("", ".json"))
        
        stable_check_n = int(config.get("stable_check_n", "1"))
        
        check_ack = config.get("check_ack", False)
        if check_ack:
            ack_ext = config.get("ack_file_ext", ".done").replace(".", "")

        ignore_old = config.get("ignore_old", True)
        if ignore_old:
            ignore_old_hour = config.get("ignore_old_hour", 7*24)
        
        copy_found = config.get("copy_found", False)
        if copy_found:
            copy_to = base.get_absolute_path(config.get("copy_to", "work/spool"))
            os.makedirs(copy_to, exist_ok=True)
        
        interval_sec = config.get("interval_sec", 2)
        
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} monitor_dir={monitor_dir} search_subdir={search_subdir} pfx_sfx={pfx_sfx} stable_check_n={stable_check_n} check_ack={check_ack} ignore_old={ignore_old} copy_found={copy_found} interval_sec={interval_sec}", log_file)
        
        """発見済みファイル名格納用DB"""
        db_path = os.path.join(work_db_dir, f"{TRIGGER_NAME}_seen_files.db")
        table_name = "seen_files"
        table_col = {
            0: ["path" , "TEXT", "PRIMARY KEY"],
            1: ["mdate", "REAL", ""]
        }
        conn = utl.initialize_db(db_path, None, table_name, table_col)
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} seen_files={db_path}", log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None
    
    """======== LOOP START ========"""
    n_col_def = len(col_def)
    count = 0
    while not stop_event.is_set():
        _now = time.time()
        try:
            files = utl.list_files(monitor_dir, pfx_sfx, search_subdir)
            
            for f_path in files:
                """ Check File """
                # 指定より古いファイルを無視
                if ignore_old:
                    f_mtime = os.path.getmtime(f_path)
                    age_seconds = _now - f_mtime
                    ignore_old_sec = ignore_old_hour*60*60
                    if age_seconds > 0 and age_seconds > ignore_old_sec:
                        continue
                
                # まだ処理していないファイルのみ
                _, is_seen = utl.check_record_exists(db_path, conn, table_name, [table_col[0][0], f_path])
                if is_seen: continue

                # 安定したファイルのみ
                not_stable = not(utl.check_file_stability_retry(stable_check_n, f_path))
                if not_stable:
                    base.output_log(MODULE_NAME, f"File NOTE Stable, to Skip. {f_path}", log_file)
                    continue

                # Ackファイル存在チェック
                if check_ack:
                    ack_not_exists = not(utl.check_ack_file_exists(ack_ext, f_path))
                    if ack_not_exists: continue
                
                """ファイルのメタ情報"""
                dir_path, file_name = os.path.split(f_path)
                base_name, ext = os.path.splitext(file_name)
                dir_base_name  = os.path.basename(dir_path)
                stat_   = os.stat(f_path)
                size_kb = stat_.st_size / 1024
                mtime_  = stat_.st_mtime
                ts_ = dt.fromtimestamp(mtime_)
                ts_yyyymmdd = ts_.strftime("%Y%m%d")
                ts_yyyy = ts_.strftime("%Y")
                ts_mm = ts_.strftime("%m")
                ts_dd = ts_.strftime("%d")
                ts_hh = ts_.strftime("%H")
                
                """発見後処理"""
                fpath_mod = f_path # 元のパスを保持
                
                # 発見したファイルをコピー
                if copy_found:
                    try:
                        shutil.copy2(f_path, copy_to)
                        fpath_mod = os.path.join(copy_to, file_name)
                    except Exception as e:
                        base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", log_file)
                        pass
                
                # 発見したファイルを移動
                #### TBD ####
                
                # 発見したファイルをzip圧縮
                #### TBD ####
                                
                """ キュー送信 """
                queue_out.put(
                    (
                        count,
                        [
                            fpath_mod,
                            dir_path,
                            dir_base_name,
                            file_name,
                            base_name,
                            ext,
                            size_kb,
                            ts_,
                            ts_yyyymmdd,
                            ts_yyyy,
                            ts_mm,
                            ts_dd,
                            ts_hh,
                        ]
                    )
                )
                
                # 発見済みファイル名に格納
                utl.insert_records_to_db(db_path, conn, table_name, table_col, [[f_path, mtime_]])
                base.output_log(MODULE_NAME, f"Queued file: {f_path}")
                
                """ 送信後処理 """
                count += 1
                queue_out.put((count, ['____'] + [None]*(n_col_def-1))) # セグメント区切り
                
            time.sleep(interval_sec)

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue
    """======== LOOP END ========"""
    
    try:
        """DBクリア"""
        conn = utl.initialize_db(db_path, conn, table_name, table_col)
        conn.close()

    except Exception as e:
        ErrorIF.report_exception(e, phase="shutdown", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)
    
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Worker Stop.", log_file)
    return None

