# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [Connection Template]
# Ver: 06.d.20261108    By: IWATA, Y.'s Agent AI
# Mod: pyflakes の指摘を整理（使っていない import・変数、置き換えの無い f-string を削除。動作は変えない）
########################################
外部のシステムへ接続して送る Act を作るときの雛形

主要関数:
a_template_conn_trigger  # 接続・送信・切断の流れと、接続できないとき（not_connectable）の扱いを備えた Act の雛形。

位置づけ: 新しい接続先へ送る Act は、この雛形の _connect / _send / _close を書き換えて作る。キューの扱いだけの雛形は tri_s12_act_tmpl_queue。
設計意図: 判定（何回失敗したら接続できないとするか）・リトライ・接続できない間の扱いはトリガーに書き、状態の記録・再接続の間隔・
          通知・ログは共通の tri_s01_connect_state（ConnectState）に任せる。新しい Act でも同じ振る舞いになるよう、その見本を含める。
          システムのオフラインモード（base.get_mode_is_offline）のときの挙動は、トリガーごとにスタブ（_on_system_offline）に書く。
前提・制約: 作り方は『新規トリガー開発ガイドライン』を参照。
"""
MODULE_NAME = "ACT_Connect"

"""standard library"""
from   datetime import datetime as dt
import os
import queue
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, Optional

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
from   src.tri_s01_error_if       import ErrorIF, SIGNAL_STOP
from   src.tri_s01_connect_state  import ConnectState

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
    Mod: 2026.11.06    By: IWATA, Y.'s Agent AI（接続できない状態の管理を ConnectState に置き換え）

    [設定]（この雛形には設定例が無いため、ここに記載する）
        server_ip              : 接続先サーバの IP アドレス（デフォルト: "127.0.0.1"）
        server_port            : 接続先サーバのポート番号（デフォルト: 9000）
        timeout_sec            : 接続タイムアウト秒数（デフォルト: 5.0）
        retry_max              : 連続でこの回数失敗したら、接続できない（not_connectable）とする（デフォルト: 3）
        retry_wait_sec         : リトライ（接続し直して送り直す）の間隔秒数（デフォルト: 1.0）
        reconnect_interval_sec : 接続できない間に、再接続を試みる間隔秒数（デフォルト: 5.0）
        not_connectable_action : 接続できない間に届いたセグメントの扱い（デフォルト: "skip"）
                                 "skip" = 送らずにスキップする / "wait" = 再接続できるまで待つ
        ack_mode               : 送信ごとに ACK を待つか（デフォルト: False）
        encode                 : 文字エンコーディング（デフォルト: "utf-8"）
        sep                    : データ区切り文字（デフォルト: "\n"）
        chunk                  : 受信チャンクサイズ バイト（デフォルト: 1024）
    [接続できない状態の扱い]
        判定（連続失敗の回数）とリトライはこのトリガーで行う（connect_state には報告しない）。
        連続 retry_max 回失敗したら conn_state.set_not_connectable() で報告し、以降は reconnect_interval_sec ごとに
        再接続を試みて、結果を set_connectable() / reconnect_failed() で報告する。
    """
    TRIGGER_NAME = "a_template_conn_trigger"  ####

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

    def _on_system_offline(segment: list, seg_id: Any) -> bool:
        """
        #### システムのオフラインモード（base.get_mode_is_offline() が True）のときの挙動を実装 ####
        戻り値: True = このセグメントを送らない（呼び出し側でスキップする）/ False = いつもどおり送る
        例: 外へ送らない Act なら、件数を数えて True を返す
        """
        return False  ####

    def _responsive_wait(seconds: float) -> None:
        """stop_event を見ながら細かく待つ（即座に停止へ反応できるように。tri_s11_find_slmp.py 等と同様）"""
        _wait_end = time.time() + seconds
        while time.time() < _wait_end and not stop_event.is_set():
            time.sleep(min(0.05, max(0.0, _wait_end - time.time())))


    # ---- CONN INIT（接続オブジェクト・状態変数）----
    conn              = None
    fail_count        = 0      # 連続失敗の回数（判定に使う。トリガーの中だけで数える）
    skipped_seg_count = 0

    """======== LOOP INIT ========"""
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)

        n_col_def = len(col_def)

        # ---- LOOP INIT（固有設定）####
        pass  ####

        # 接続設定
        server_ip   = str(config.get("server_ip", "127.0.0.1"))
        server_port = int(config.get("server_port", 9000))
        timeout_sec = float(config.get("timeout_sec", 5.0))
        ack_mode    = bool(config.get("ack_mode", False))
        encode = str(config.get("encode", "utf-8"))
        chunk  = int(config.get("chunk", 1024))

        # 接続できない状態の扱い（判定・リトライ・接続できない間の扱いはトリガーで決める）
        retry_max              = int(config.get("retry_max", 3))
        retry_wait_sec         = float(config.get("retry_wait_sec", 1.0))
        reconnect_interval_sec = float(config.get("reconnect_interval_sec", 5.0))
        not_connectable_action = str(config.get("not_connectable_action", "skip")).strip().lower()
        if retry_max < 1 or retry_wait_sec < 0:
            raise ValueError(f"config['retry_max'] must be >= 1 and config['retry_wait_sec'] >= 0. Got: {retry_max} / {retry_wait_sec}")
        if not_connectable_action not in ("skip", "wait"):
            raise ValueError(f"config['not_connectable_action'] must be 'skip' / 'wait'. Got: {not_connectable_action!r}")
        conn_state = ConnectState(TRIGGER_NAME, f"{server_ip}:{server_port}", reconnect_interval_sec, MODULE_NAME, log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config",
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    # ---- 接続・リトライ（判定はトリガー。connect_state へ報告するのは移行・再接続の結果だけ）----
    def _drop_conn() -> None:
        nonlocal conn
        if conn:
            try:   _close(conn)  #### DISCONNECT ####
            except Exception: pass
        conn = None

    def _try_connect() -> Optional[BaseException]:
        """接続を1回試みる。成功なら None、失敗ならその例外"""
        nonlocal conn
        try:
            conn = _connect(server_ip, server_port, timeout_sec)  #### CONNECT ####
            return None
        except Exception as e:
            conn = None
            return e

    def _send_with_retry(send_str: str) -> bool:
        """
        送る。失敗したら retry_wait_sec ごとにリトライ（接続し直して送り直す）する。
        連続 retry_max 回失敗したら not_connectable と判定して報告し、False を返す（送れたら連続失敗の回数を 0 に戻す）
        """
        nonlocal fail_count
        while not stop_event.is_set():
            e = _try_connect() if conn is None else None
            if e is None:
                try:
                    _send(conn, send_str, encode, ack_mode, chunk)  #### SEND ####
                    fail_count = 0
                    return True
                except Exception as e_send:
                    e = e_send
                    _drop_conn()
            fail_count += 1
            if fail_count >= retry_max:
                conn_state.set_not_connectable(e)   # 判定：連続 retry_max 回の失敗
                return False
            base.output_log(MODULE_NAME,            # リトライは画面のみ（connect_state・ErrorIF へは報告しない）
                f"{TRIGGER_NAME} Retry ({fail_count}/{retry_max}). {server_ip}:{server_port}  ({type(e).__name__}: {e})")
            _responsive_wait(retry_wait_sec)
        return False

    def _reconnect_if_due() -> None:
        """接続できない間、再接続の間隔が過ぎていれば1回試みて、結果を connect_state へ報告する"""
        nonlocal fail_count
        if not conn_state.should_try_reconnect():
            return
        e = _try_connect()
        if e is None:
            fail_count = 0
            conn_state.set_connectable()
        else:
            conn_state.reconnect_failed(e)

    """======== LOOP START ========"""
    while True:
        try:
            segment = post_queue.get(timeout=1.0)

        except queue.Empty:
            _reconnect_if_due()
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

            # --- システムのオフラインモード（挙動はスタブ _on_system_offline に書く）---
            if base.get_mode_is_offline() and _on_system_offline(segment, seg_id):
                continue

            # --- 接続できないとき（not_connectable）の扱い ---
            _reconnect_if_due()
            if not conn_state.is_connectable():
                if not_connectable_action == "skip":
                    skipped_seg_count += 1; continue
                while not conn_state.is_connectable() and not stop_event.is_set():   # "wait"
                    _reconnect_if_due()
                    _responsive_wait(min(1.0, reconnect_interval_sec))
                if stop_event.is_set(): break

            # ---- SEND（固有：ペイロード構築＋送信）####
            send_str = ""  ####
            if not _send_with_retry(send_str):
                skipped_seg_count += 1   # 送れなかったセグメント（送り直す場合は tri_s12_act_socket の resend_max を参照）

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="data",
                                            trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP: stop_event.set(); break
            continue

        finally:
            post_queue.task_done()

    """======== LOOP END ========"""

    _drop_conn()

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
    base.output_log(MODULE_NAME, "Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
