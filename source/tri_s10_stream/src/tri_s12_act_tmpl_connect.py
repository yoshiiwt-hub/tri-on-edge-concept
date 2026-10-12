# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [Connection Template]
# Ver: 06.a.20261004    By: IWATA, Y.'s Agent AI
# Mod: 終点行の除外を各 Act の [:-1] から core へ移し、CUT_LAST_ROW 属性で宣言する形に変更（B-2）
########################################
a_template_conn_trigger  # サーバ等外部システム接続型 Act Trigger のテンプレート

動作概要
    1. 起動時に接続設定を読み込み _connect / _send / _close のヘルパー関数を用意する
    2. ループ内でキューからセグメントを受信し項目数の整合性を確認する
    3. オフライン時は skip_on_offline 設定に応じてスキップまたは再接続待ちを行う
    4. _send() でペイロードを構築して外部システムへ送信する
    5. 接続断発生時は _close() でコネクションを閉じ is_online=False に移行する
    6. stop_event が立ちキューが空になった時点でループを終了しコネクションをクローズする

接続設定
    server_ip          : 接続先サーバの IP アドレス（デフォルト: "127.0.0.1"）
    server_port        : 接続先サーバのポート番号（デフォルト: 9000）
    timeout_sec        : 接続タイムアウト秒数（デフォルト: 5.0）
    reconnect_interval_sec : 再接続インターバル秒数（デフォルト: 5.0）
    ack_mode           : 送信ごとに ACK を待つか（デフォルト: False）
    skip_on_offline    : オフライン時にセグメントをスキップするか（デフォルト: True）
    encode             : 文字エンコーディング（デフォルト: "utf-8"）
    sep                : データ区切り文字（デフォルト: "\\n"）
    chunk              : 受信チャンクサイズ バイト（デフォルト: 1024）
"""
MODULE_NAME = "ACT_Connect"

"""standard library"""
import csv
from   datetime import datetime as dt
import json
import os
import socket
import queue
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Tuple, Sequence, Optional

"""external library"""
## Not Using

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
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# Act Trigger Functions
########################################

def a_template_conn_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    サーバ等外部システム接続型 ActTrigger のテンプレート
    Mod: 2026.05.18    By: IWATA,Y.
    """
    TRIGGER_NAME = "a_template_conn_trigger"  ####
    _SIG_OFFLINE = f"ConnectionFailed.comm.loop.{TRIGGER_NAME}"
    
    # ---- HELPER FUNCS ----
    def _connect(ip: str, port: int, timeout: float):
        """#### 接続処理を実装 ####"""
        raise NotImplementedError  ####

    def _send(conn, send_str: str, encode: str, ack_mode: bool, chunk: int):
        """#### 送信処理を実装 ####"""
        raise NotImplementedError  ####

    def _close(conn) -> None:
        """#### 切断処理を実装 ####"""
        raise NotImplementedError  ####

    def _responsive_wait(seconds: float) -> None:
        """stop_event を見ながら細かく待つ（即座に停止へ反応できるように。tri_s11_find_slmp.py 等と同様）"""
        _wait_end = time.time() + seconds
        while time.time() < _wait_end and not stop_event.is_set():
            time.sleep(min(0.05, max(0.0, _wait_end - time.time())))
    
    
    # ---- CONN INIT（接続オブジェクト・状態変数）----
    conn                 = None
    is_online            = False
    reconnect_fail_count = 0
    last_restart_time    = 0.0
    skipped_seg_count    = 0

    """======== LOOP INIT ========"""
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)

        n_col_def = len(col_def)
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]

        # ---- LOOP INIT（固有設定）####
        pass  ####

        # 接続設定
        server_ip   = str(config.get("server_ip", "127.0.0.1"))
        server_port = int(config.get("server_port", 9000))
        timeout_sec        = float(config.get("timeout_sec", 5.0))
        reconnect_interval_sec = float(config.get("reconnect_interval_sec", 5.0))
        ack_mode        = bool(config.get("ack_mode", False))
        skip_on_offline = bool(config.get("skip_on_offline", True))
        encode = str(config.get("encode", "utf-8"))
        sep    = str(config.get("sep", "\n"))
        chunk  = int(config.get("chunk", 1024))
        
    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config",
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== LOOP START ========"""
    while True:
        try:
            segment = post_queue.get(timeout=1.0)

        except queue.Empty:
            if not is_online:
                _now = time.time()
                if (_now - last_restart_time) > reconnect_interval_sec:
                    try:
                        conn = _connect(server_ip, server_port, timeout_sec)  #### CONNECT ####
                        is_online = True; reconnect_fail_count = 0
                        last_restart_time = _now
                    except Exception as e:
                        reconnect_fail_count += 1
                        ErrorIF.report_event({
                            "trigger_id": TRIGGER_NAME, "phase": "loop", "category": "comm",
                            "exc_type": type(e).__name__,
                            "message": f"Re-Connect Failed ({reconnect_fail_count}). {server_ip}:{server_port}",
                            "signature": _SIG_OFFLINE,
                        })
                        last_restart_time = _now
            ErrorIF.emit_summary_if_needed()
            if stop_event.is_set() and post_queue.qsize()==0 and getattr(post_queue,"unfinished_tasks",0)==0:
                break
            continue

        if segment is None:
            post_queue.task_done(); break

        try:
            # --- 共通検証 ---
            if len(segment[0]) != n_col_def: continue
            seg_id  = segment[0][0]
            # 終点行（変化後）の除外は core が行う（CUT_LAST_ROW = True）
            if not segment: continue
            if isinstance(segment[0][-1], list) and segment[0][-1][0] is None: continue

            # --- オフライン処理（共通） ---
            if not is_online:
                if skip_on_offline:
                    skipped_seg_count += 1; continue
                else:
                    while not is_online and not stop_event.is_set():
                        try:
                            conn = _connect(server_ip, server_port, timeout_sec)  #### CONNECT ####
                            is_online = True; reconnect_fail_count = 0
                        except Exception:
                            reconnect_fail_count += 1
                            _responsive_wait(reconnect_interval_sec)
                    if stop_event.is_set(): break

            # ---- SEND（固有：ペイロード構築＋送信）####
            send_str = ""  ####
            try:
                _ack = _send(conn, send_str, encode, ack_mode, chunk)  #### SEND ####

            except Exception as e:
                _sig = ErrorIF.report_exception(e, phase="loop", category="comm", trigger_id=TRIGGER_NAME, log_file=log_file)
                
                if conn: _close(conn)  #### DISCONNECT ####
                conn = None
                
                is_online = False; reconnect_fail_count = 0
                last_restart_time = time.time()
                if _sig == SIGNAL_STOP: stop_event.set()

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="data",
                                            trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP: stop_event.set(); break
            continue

        finally:
            post_queue.task_done()

    """======== LOOP END ========"""
    
    if conn: _close(conn)  #### DISCONNECT ####
    
    if skipped_seg_count > 0:
        base.output_log(MODULE_NAME,
            f"WARNING  {TRIGGER_NAME} Stopped with {skipped_seg_count} skipped.", log_file)
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)

# CUT_LAST_ROW: True = セグメントの終点行（終了条件が成立した行。例: LOT 変化後の行・find の区切り行）を
#               core が除外してから渡す / False = 終点行を含めて全行を渡す（Act 側で扱いを判断する）
#               属性が無い場合は False と同じ扱い。意図を示すため、False の場合も必ず明記すること
#               ※ 上限（seg_max_rows / seg_max_sec）による強制クローズのセグメントは、終点行が無いため除外しない
a_template_conn_trigger.QMODE = "imq"
a_template_conn_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す

########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, f"Run Standalone.")
    base.output_log(MODULE_NAME, f"test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
