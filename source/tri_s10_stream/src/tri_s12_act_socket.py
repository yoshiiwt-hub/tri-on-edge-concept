# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [Socket Send]
# Ver: 02.b.20261005    By: IWATA, Y.'s Agent AI
# Mod: 統計まわりを init_stats / calc_stats / export_out_coldef に集約（出力内容は従来どおり。B-9）。初回接続の失敗を ErrorIF へ報告（phase=startup / category=comm。A-21）
########################################
a_send_seg_trigger    # セグメントの全行をJSON形式でTCPソケット送信する
a_send_stats_trigger  # セグメントの統計量を算出し、1件をJSON形式でTCPソケット送信する

動作概要
    1. 起動時にソケットサーバへ接続する（a_send_stats_trigger の場合はあわせて
       column_register_map / out_coldef から統計マップを初期化する）
    2. ループ内でキューからセグメントを受信し項目数の整合性を確認する
    3. a_send_seg_trigger   : セグメントの全行を1行ずつJSON形式で逐次送信する
       a_send_stats_trigger : build_stats() で統計量を算出して1レコードを送信する
    4. ack_mode=True の場合は送信ごとにサーバから "OK" 応答を待ってから次の送信へ進む
    5. 接続断時は is_online=False に移行し reconnect_interval_sec 秒ごとに再接続を試みる
    6. skip_on_offline=True の場合はオフライン中のセグメントをスキップしてキューを消費する

接続設定（共通）
    server_ip              : 送信先サーバの IP アドレス（デフォルト: "127.0.0.1"）
    server_port             : 送信先サーバのポート番号（デフォルト: 9000）
    timeout_sec             : ソケット接続タイムアウト秒数（デフォルト: 5.0）
    reconnect_interval_sec  : 再接続インターバル秒数（デフォルト: 5.0）
    skip_on_offline         : オフライン時にセグメントをスキップするか（デフォルト: True）
    encode                  : 文字エンコーディング（デフォルト: "utf-8"）
    sep                     : データ区切り文字（デフォルト: "\\n"）
    chunk                   : 受信チャンクサイズ バイト（デフォルト: 1024）
    ack_mode                : 送信ごとに ACK を待つか（デフォルト: False）

a_send_stats_trigger 専用設定
    column_register_map : 統計量算出用の列マッピング定義（必須）
    out_coldef           : 統計量の出力列定義（必須）
    save_dir             : out_coldef を保存するディレクトリ（デフォルト: "output"）
"""
MODULE_NAME = "ACT_Socket"

"""standard library"""
from   datetime import datetime as dt
import json
import os
import socket
import queue
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Optional

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
from   src.tri_s10_stream_wave_stat  import init_stats, calc_stats, export_out_coldef  # Calc Stats（Act 共通。B-9）


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
# 共通送信ループ（a_send_seg_trigger / a_send_stats_trigger の実体）
########################################

def _run_send_trigger(
    mode:       str,  # "seg" または "stat"
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """TCPソケットでサーバへ送信する共通ループ。mode で送信内容だけが変わる"""
    TRIGGER_NAME = "a_send_seg_trigger" if mode == "seg" else "a_send_stats_trigger"
    _SIG_OFFLINE = f"ConnectionFailed.comm.loop.{TRIGGER_NAME}"

    def _connect(_ip: str, _port: int, _to: float) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(_to)
        s.connect((_ip, _port))
        return s

    def _responsive_wait(seconds: float) -> None:
        """stop_event を見ながら細かく待つ（即座に停止へ反応できるように。tri_s11_find_slmp.py 等と同様）"""
        _wait_end = time.time() + seconds
        while time.time() < _wait_end and not stop_event.is_set():
            time.sleep(min(0.05, max(0.0, _wait_end - time.time())))

    """======== LOOP INIT ========"""
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start. mode={mode}", log_file)

        # データ項目定義
        n_col_def = len(col_def)
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_keys={col_keys}", log_file)

        # 出力データ項目定義（a_send_stats_trigger のみ）
        if mode == "stat":
            stats_ctx    = init_stats(config, col_def, MODULE_NAME, TRIGGER_NAME)

            # save out_coldef
            save_dir = os.path.join(system_root, config.get("save_dir", "output"))
            export_out_coldef(stats_ctx.out_coldef,
                os.path.join(save_dir, f"output_column_define_{TRIGGER_NAME}.json"), "utf-8", MODULE_NAME)

        # 接続設定
        server_ip   = str(config.get("server_ip", "127.0.0.1"))
        server_port = int(config.get("server_port", 9000))
        timeout_sec = float(config.get("timeout_sec", 5.0))
        reconnect_interval_sec = float(config.get("reconnect_interval_sec", 5.0))
        skip_on_offline = bool(config.get("skip_on_offline", True))
        base.output_log(MODULE_NAME,
            f"server_ip={server_ip} server_port={server_port} timeout_sec={timeout_sec} "
            f"reconnect_interval_sec={reconnect_interval_sec} skip_on_offline={skip_on_offline} ", log_file)

        # Socket通信設定
        encode   = str(config.get("encode", "utf-8"))
        sep      = str(config.get("sep", "\n"))
        chunk    = int(config.get("chunk", 1024))
        ack_mode = bool(config.get("ack_mode", False))

        # 初回接続
        tcp_socket: Optional[socket.socket] = None
        is_online = False
        try:
            tcp_socket = _connect(server_ip, server_port, timeout_sec)
            is_online  = True
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Connected. {server_ip}:{server_port}", log_file)
        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="startup", category="comm",  # 初回接続の失敗を報告（A-21）
                                             trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                return None
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Connect Failed. {server_ip}:{server_port}"
                f"  ({type(e).__name__}: {e})  Go Offline.", log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== LOOP START ========"""
    reconnect_fail_count = 0
    last_reconnect_try = time.time()
    skipped_seg_count = 0

    while True:

        """キュー受信"""
        try:
            segment = post_queue.get(timeout=1.0)

        except queue.Empty:
            # オフライン中はここで再接続を試みる（backgroundリトライ）
            if not is_online and (time.time() - last_reconnect_try) >= reconnect_interval_sec:
                last_reconnect_try = time.time()
                try:
                    tcp_socket = _connect(server_ip, server_port, timeout_sec)
                    is_online  = True
                    reconnect_fail_count = 0
                    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Reconnected. {server_ip}:{server_port}", log_file)
                except Exception as e:
                    reconnect_fail_count += 1
                    ErrorIF.report_event({
                        "trigger_id": TRIGGER_NAME,
                        "phase"     : "loop",
                        "category"  : "comm",
                        "exc_type"  : type(e).__name__,
                        "message"   : f"Re-Connect Failed ({reconnect_fail_count} times). {server_ip}:{server_port}",
                        "signature" : _SIG_OFFLINE,
                    })
                    base.output_log(MODULE_NAME,
                        f"{TRIGGER_NAME} Re-Connect Failed ({reconnect_fail_count}). "
                        f"{server_ip}:{server_port}  ({type(e).__name__}: {e})", log_file)

            if (stop_event.is_set() and
                    post_queue.qsize() == 0 and
                    getattr(post_queue, "unfinished_tasks", 0) == 0):
                break
            continue

        """sentinel → 終了"""
        if segment is None:
            post_queue.task_done()
            break

        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} ...Received Queue.")

        """セグメント処理"""
        try:
            """項目の整合性（個数チェック）"""
            n_col_act = len(segment[0])
            if n_col_act != n_col_def:
                base.output_log(MODULE_NAME,
                    f"Segment Columns NOT Match: seg_id={segment[0][0]}  "
                    f"Expected:{n_col_def}  Actual:{n_col_act}", log_file)
                continue

            seg_id = segment[0][0]

            """終点行（変化後）の除外は core が行う（CUT_LAST_ROW = True）"""

            """空セグメントチェック"""
            if len(segment) == 0:
                base.output_log(MODULE_NAME,
                    f"WARNING  空セグメント。スキップ。 seg_id={seg_id}", log_file)
                continue

            """空レコードチェック"""
            if (isinstance(segment[0][-1], list)) and (segment[0][-1][0] is None):
                base.output_log(MODULE_NAME,
                    f"WARNING  空レコード。スキップ。 seg_id={seg_id}", log_file)
                continue

            """==== オフライン時の処理 ===="""
            if not is_online:

                if skip_on_offline:
                    skipped_seg_count += 1
                    base.output_log(MODULE_NAME,
                        f"WARNING  OFFLINE - Segment Skipped. "
                        f"seg_id={seg_id}  total_skipped={skipped_seg_count}  "
                        f"{server_ip}:{server_port}", log_file)
                    continue

                else:
                    """ブロック待ち（停止チェック付き）"""
                    base.output_log(MODULE_NAME,
                        f"{TRIGGER_NAME} OFFLINE - Waiting for reconnect... seg_id={seg_id}", log_file)
                    while not is_online and not stop_event.is_set():
                        try:
                            tcp_socket           = _connect(server_ip, server_port, timeout_sec)
                            is_online            = True
                            reconnect_fail_count = 0
                            base.output_log(MODULE_NAME,
                                f"{TRIGGER_NAME} Reconnected. {server_ip}:{server_port}", log_file)
                        except Exception as e:
                            reconnect_fail_count += 1
                            base.output_log(MODULE_NAME,
                                f"{TRIGGER_NAME} Re-Connect Failed ({reconnect_fail_count}). "
                                f"Retry in {reconnect_interval_sec}s...", log_file)
                            _responsive_wait(reconnect_interval_sec)

                    if stop_event.is_set():
                        break

            """==== Sending ===="""
            sent_count = 0
            send_error = False

            # ==== mode="seg": 全行送信 ====
            if mode == "seg":
                for row_num, row_val in enumerate(segment, start=1):

                    """ dict -> JSON """
                    row_dict = {col_keys[i]: row_val[i] for i in range(len(col_keys))}
                    send_str = json.dumps(row_dict, ensure_ascii=False, default=str) + sep

                    try:
                        tcp_socket.sendall(("PUT" + sep).encode(encode))
                        tcp_socket.sendall(send_str.encode(encode))

                        """ACK: Wait for 'OK\n' """
                        if ack_mode:
                            ack_raw = tcp_socket.recv(chunk).decode(encode).strip()
                            if ack_raw != "OK":
                                base.output_log(MODULE_NAME,
                                    f"{TRIGGER_NAME} ACK NG: seg_id={seg_id}  row={row_num}  ack={ack_raw!r}", log_file)

                        sent_count += 1

                    except Exception as e:
                        _sig = ErrorIF.report_exception(
                            e, phase="loop", category="comm",
                            trigger_id=TRIGGER_NAME, log_file=log_file)

                        if tcp_socket:
                            try:   tcp_socket.close()
                            except Exception: pass
                        tcp_socket           = None
                        is_online            = False
                        reconnect_fail_count = 0
                        last_reconnect_try   = time.time()

                        base.output_log(MODULE_NAME,
                            f"{TRIGGER_NAME} Connection Lost. to Offline. "
                            f"seg_id={seg_id}  sent={sent_count}/{len(segment)}", log_file)

                        if _sig == SIGNAL_STOP:
                            stop_event.set()

                        send_error = True
                        break

            # ==== mode="stat": 統計量1件送信 ====
            else:
                stats_dict = calc_stats(stats_ctx, segment, seg_id)
                send_str = json.dumps(stats_dict, ensure_ascii=False, default=str) + sep

                try:
                    tcp_socket.sendall(("PUT" + sep).encode(encode))
                    tcp_socket.sendall(send_str.encode(encode))

                    """ACK: Wait for 'OK\n' """
                    if ack_mode:
                        ack_raw = tcp_socket.recv(chunk).decode(encode).strip()
                        if ack_raw != "OK":
                            base.output_log(MODULE_NAME,
                                f"{TRIGGER_NAME} ACK NG (summary): seg_id={seg_id}  ack={ack_raw!r}", log_file)

                    sent_count = 1

                except Exception as e:
                    _sig = ErrorIF.report_exception(
                        e, phase="loop", category="comm",
                        trigger_id=TRIGGER_NAME, log_file=log_file)

                    if tcp_socket:
                        try:   tcp_socket.close()
                        except Exception: pass
                    tcp_socket           = None
                    is_online            = False
                    reconnect_fail_count = 0
                    last_reconnect_try   = time.time()

                    base.output_log(MODULE_NAME,
                        f"{TRIGGER_NAME} Connection Lost. to Offline. "
                        f"seg_id={seg_id}  (stat mode)", log_file)

                    if _sig == SIGNAL_STOP:
                        stop_event.set()

                    send_error = True

            if not send_error:
                base.output_log(MODULE_NAME,
                    f"{TRIGGER_NAME} Segment Sent. "
                    f"seg_id={seg_id}  mode={mode}  "
                    f"sent={sent_count}  original_rows={len(segment)}", log_file)

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="data",
                                            trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue

        finally:
            post_queue.task_done()

    """======== LOOP END ========"""
    if tcp_socket:
        try:   tcp_socket.close()
        except Exception: pass

    if skipped_seg_count > 0:
        base.output_log(MODULE_NAME,
            f"WARNING  {TRIGGER_NAME} Stopped with {skipped_seg_count} segment(s) skipped due to OFFLINE.", log_file)

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None


########################################
# Act Trigger Functions（薄いラッパー）
########################################

def a_send_seg_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """セグメントの全行をJSON形式でTCPソケット送信する"""
    return _run_send_trigger("seg", post_queue, col_def, config, stop_event)

a_send_seg_trigger.QMODE = "imq"
a_send_seg_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


def a_send_stats_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """セグメントの統計量を算出し、1件をJSON形式でTCPソケット送信する"""
    return _run_send_trigger("stat", post_queue, col_def, config, stop_event)

a_send_stats_trigger.QMODE = "imq"
a_send_stats_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, f"Run Standalone.")
    base.output_log(MODULE_NAME, f"test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
