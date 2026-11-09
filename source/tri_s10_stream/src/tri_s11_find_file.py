# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [File]
# Ver: 07.g.20261108    By: IWATA, Y.'s Agent AI
# Mod: pyflakes の指摘を整理（使っていない import・変数、置き換えの無い f-string を削除。動作は変えない）
########################################
フォルダに置かれたファイルを見つけて、そのファイルの情報を取り込む入口

主要関数:
f_find_file_trigger  # フォルダを監視し、条件に合うファイルの情報をキューに流す。

位置づけ: ファイルの中身は読まず、パス・名前・更新日時などの情報だけを渡す。中身を使う場合は tri_s11_find_text_csv / tri_s11_find_text_json を使う。ファイルの移動・アップロード等は Act（tri_s12_act_file、tri_s12_act_ftp 等）が行う。
設計意図: 発見済みのファイルを SQLite に記録し、同じファイルを二重に処理しない。書込み途中のファイルを渡さないよう、サイズ・更新日時の安定や ack ファイルを確かめてから渡す。
前提・制約: 発見済みの記録は再起動でクリアされるため、再起動後は残っているファイルを再び検出する（古いファイルは ignore_old で除外できる）。
"""
MODULE_NAME = "FIND_File"

N_BASE_COL = 13                 # 出力列 0～12（file_path ～ hh）の数。extra_col_def の列はこの後ろに付ける
EXTRA_SRC_ALLOWED = ("filename", "dirname", "dirpath", "device", "const")  # ファイルを読まないため header は不可

"""standard library"""
from   datetime import datetime as dt
import os
import queue
import shutil # copy file
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Tuple, Optional

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
# Helper
########################################

def _load_extra_col_def(config: Dict[str, Any], n_col_def: int) -> Tuple[Optional[List[Dict[str, Any]]], Any]:
    """
    extra_col_def を読み、検証して、列番号順のリストにして返す（起動時に1回）
    キーは 13（N_BASE_COL）から始まる連番、col_def の列数は 13 + 追加数 であること。使える src は EXTRA_SRC_ALLOWED
    Returns: (extra_col_def のリスト（指定なしは None）, extra_col_default)
    Mod: 2026.10.20    By: IWATA, Y.'s Agent AI
    """
    extra_col_default = config.get("extra_col_default", "__")
    raw = config.get("extra_col_def", None)
    if raw is None:
        return None, extra_col_default # 指定なしは従来どおり（列数の確認もしない）

    if not isinstance(raw, dict) or not raw:
        raise ValueError(f"extra_col_def は列番号をキーとする辞書（{{\"{N_BASE_COL}\": {{...}}}}）で指定してください: {raw!r}")
    bad = [k for k in raw.keys() if not str(k).strip().isdigit()]
    if bad:
        raise ValueError(
            f"extra_col_def のキーは列番号（数字）で指定してください: {bad}。"
            f"閉じ括弧 }} の位置を確認してください（後ろの設定が extra_col_def の中に入っている可能性があります）")

    items = sorted(raw.items(), key=lambda kv: int(kv[0]))
    keys = [int(k) for k, _v in items]
    expected = list(range(N_BASE_COL, N_BASE_COL + len(items)))
    if keys != expected:
        raise ValueError(f"extra_col_def のキーは {N_BASE_COL} から始まる連番にしてください: 指定={keys}  期待={expected}")
    if n_col_def != N_BASE_COL + len(items):
        raise ValueError(
            f"column の列数（{n_col_def}）が {N_BASE_COL} + extra_col_def の数（{len(items)}）と一致しません。"
            f"column に {N_BASE_COL}～{N_BASE_COL + len(items) - 1} の列を定義してください")

    extra_col_def = [v for _k, v in items]
    utl.validate_extra_col_def(extra_col_def, allowed_src=EXTRA_SRC_ALLOWED)
    return extra_col_def, extra_col_default


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
    Mod: 2026.10.20    By: IWATA, Y.'s Agent AI（extra_col_def に対応）
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
        
        extra_col_def, extra_col_default = _load_extra_col_def(config, len(col_def))
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} extra_col_def={extra_col_def} extra_col_default={extra_col_default!r}", log_file)
        
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
                
                # 追加列（extra_col_def。ファイル名・フォルダ名等は元のパスから求める）
                extra_vals = utl.resolve_extra_col_values(f_path, extra_col_def, extra_col_default)
                
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
                        ] + extra_vals
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

