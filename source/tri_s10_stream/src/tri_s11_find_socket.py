# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [Socket]
# Ver: 06.g.20261001    By: IWATA, Y.'s Agent AI
# Mod: モジュール名を tri_edge_XX_* から tri_sXX_* 形式に変更（tri_edge_11_find_socket → tri_s11_find_socket）
########################################
f_socket_client_trigger  # TCPソケットサーバに接続しデータをポーリング取得してキューに送信する Find Trigger

動作概要
    1. 起動時にソケットサーバへ接続を試みる（失敗時はオフラインモードで待機）
    2. ループ内でリクエストメッセージを送信しサーバからデータを受信する
    3. 受信 JSON を col_def の dtype 定義に従いキャストして値リストを作成する
    4. キャスト済み値を (count, value) タプルとしてキューに投入する
    5. 接続断時はオフラインモードに移行し reconnect_interval_sec ごとに再接続を試みる
    6. stop_event が立つまでループを継続し、終了時にソケットをクローズする

接続設定
    server_ip              : ソケットサーバの IP アドレス（デフォルト: "127.0.0.1"）
    server_port            : ソケットサーバのポート番号（デフォルト: 52001）
    connect_timeout_sec    : 接続タイムアウト秒数（デフォルト: 5）
    reconnect_interval_sec : 再接続インターバル秒数（デフォルト: 10）
    polling_ms             : ポーリング間隔ミリ秒（デフォルト: 100）
    req_msg                : サーバへ送信するリクエスト文字列（デフォルト: "GET"）
    encode                 : 文字エンコーディング（デフォルト: "utf-8"）
    sep                    : データ区切り文字（デフォルト: "\\n"）
    chunk                  : 受信チャンクサイズ バイト（デフォルト: 1024）
    time_format            : 時刻文字列のパースフォーマット（デフォルト: "%Y-%m-%d %H:%M:%S.%f"）
    time_default           : 時刻変換失敗時のデフォルト値（デフォルト: "2999-01-01 00:00:00.000000"）
    int_default            : int 変換失敗時のデフォルト値（デフォルト: -9999）
    float_default          : float 変換失敗時のデフォルト値（デフォルト: -9999.9）
    str_default            : str 変換失敗時のデフォルト値（デフォルト: "__"）
"""
MODULE_NAME = "FIND_Socket"

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

def f_socket_client_trigger(
    queue_out: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    Get Data from Socket Server
    Mod: 2026.09.30    By: IWATA, Y.'s Agent AI
    """
    TRIGGER_NAME = "f_socket_client_trigger"
    _SIG_OFFLINE = f"ConnectionFailed.comm.loop.{TRIGGER_NAME}"

    def _connect(_ip: str, _port :int, _to :float):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(_to)
        s.connect((_ip, _port))
        return s
    
    def recv_until_delimiter(sock, delimiter, decode, chunk=1024, buf=""):
        """Receive until Delimiter"""
        _counter = 0
        """Search Delimiter"""
        while delimiter not in buf:
            data = sock.recv(chunk).decode(decode)
            if not data: return None, buf
            buf += data
            
            _counter += 1
            if 999 < _counter:
                raise err.TriEdgeDataError(f"Delimiter Not Found in {_counter} chunks.")
        """Cutout Value, Remainder"""
        loc = buf.find(delimiter)
        return buf[:loc + 1], buf[loc + 1:]

    def _responsive_wait(seconds: float) -> None:
        """stop_event を見ながら細かく待つ（即座に停止へ反応できるように。tri_s11_find_slmp.py 等と同様）"""
        _wait_end = time.time() + seconds
        while time.time() < _wait_end and not stop_event.is_set():
            time.sleep(min(0.05, max(0.0, _wait_end - time.time())))
    
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)
    try:
        VERBOSE = config.get("VERBOSE", False)

        """ Col Define """
        key_define   = [col_def[k][0] for k in sorted(col_def.keys())]
        ##dtype_define = [col_def[k][1] for k in sorted(col_def.keys())]
        
        # dtype cast plan
        default_val_config = {
            "time_format"  : config.get("time_format", "%Y-%m-%d %H:%M:%S.%f") ,
            "time_default" : config.get("time_default", "2999-01-01 00:00:00.000000") ,
            "int_default"  : config.get("int_default", -9999) ,
            "float_default": config.get("float_default", -9999.9) ,
            "str_default"  : config.get("str_default", "__")
        }
        cast_plan = base.set_cast_dtype(col_def, default_val_config)
        base.output_log(MODULE_NAME, f"cast_plan={cast_plan}  default_val_config={default_val_config}", log_file)
        
        """ Settings """
        server_ip   = config.get("server_ip", "127.0.0.1")
        server_port = int(config.get("server_port", 52001))
        polling_ms  = config.get("polling_ms", 100)
        req_msg = config.get("req_msg", "GET")
        encode  = config.get("encode" , "utf-8")
        sep     = config.get("sep"    , "\n")
        chunk   = config.get("chunk"  , 1024)
        connect_timeout_sec    = config.get("connect_timeout_sec", 5)
        reconnect_interval_sec = config.get("reconnect_interval_sec", 10)
        
    except Exception as e:
        ErrorIF.report_exception(
            e,
            phase="startup",
            category="config",
            trigger_id=TRIGGER_NAME,
            log_file=log_file,
        )
        stop_event.set()
        return None

    """======== LOOP INIT ========"""
    is_online = False
    tcp_socket = None
    try:
        tcp_socket = _connect(server_ip, server_port, connect_timeout_sec)
        is_online  = True
        base.output_log(MODULE_NAME, f"Socket Comm Connected. {server_ip}:{server_port}", log_file)
        
    except Exception as e:
        _sig = ErrorIF.report_exception(
            e,
            phase="startup",
            category="comm",
            trigger_id=TRIGGER_NAME,
            log_file=log_file,
        )
        if _sig == SIGNAL_STOP:
            stop_event.set()
            return None
        base.output_log(MODULE_NAME, f"Socket Comm CANNOT Connect. {server_ip}:{server_port}  ({type(e).__name__}: {e})  Go on Off-Line Mode.", log_file)
    
    """======== MAIN LOOP START ========"""
    reconnect_fail_count = 0
    last_reconnect_try = time.time()
    count = 0
    _rem = ""
    while not stop_event.is_set():
        try:
            if not is_online:
                """(Server Offline)"""
                if time.time() - last_reconnect_try >= reconnect_interval_sec:
                    # try re-connect
                    last_reconnect_try = time.time()
                    try:
                        tcp_socket = _connect(server_ip, server_port, connect_timeout_sec)
                        is_online = True
                        reconnect_fail_count = 0
                        base.output_log(MODULE_NAME, f"Re-Connected {server_ip}:{server_port}  Back to Online.", log_file)

                    except Exception as e:
                        reconnect_fail_count += 1
                        ErrorIF.report_event({
                            "trigger_id": TRIGGER_NAME,
                            "phase": "loop",
                            "category": "comm",
                            "exc_type": type(e).__name__,
                            "message": f"Re-Connect Failed ({reconnect_fail_count} times). {server_ip}:{server_port}",
                            "signature": _SIG_OFFLINE,
                        })
                        if not ErrorIF.should_suppress(_SIG_OFFLINE):
                            base.output_log(MODULE_NAME,
                                f"Re-Connect Failed ({reconnect_fail_count} times)."
                                f"{server_ip}:{server_port}  ({type(e).__name__}): {e}", log_file)
                        else:
                            base.output_log(MODULE_NAME,
                                f"Re-Connect Failed ({reconnect_fail_count} times)."
                                f"{server_ip}:{server_port}")

                ErrorIF.emit_summary_if_needed()
                time.sleep(2)
                continue
            
            """Server Online"""
            # 1. Socket Send/Receive
            tcp_socket.sendall(req_msg.encode(encode))
            _res, _rem = recv_until_delimiter(tcp_socket, sep, encode, chunk, _rem)
            if not _res or not _res.strip():
                raise ConnectionResetError("Server closed connection.")
            
            # received value
            value = json.loads(_res[:-1])
            
            # cast to valid dtype
            value, cast_fails = base.cast_dtype(value, cast_plan)
            if cast_fails > 0:
                ErrorIF.report_event({
                    "signature": "CAST_FAIL",
                    "phase": "loop",
                    "category": "data",
                    "trigger_id": TRIGGER_NAME,
                    "message": f"Cast failed {cast_fails} cols.",
                    "log_file": log_file
                })
            
            # 2. Put Queue to StreamCore
            queue_out.put((
                count,
                value
            ))
            
            ## (3. Viz to Debug)
            if VERBOSE:
                _timestamp  = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]##
                _dev_name = base.load_device_info()["dev_name"]##
                sys.stdout.write(f"{count:<3} {_timestamp} {_dev_name} {value}\n")##
                sys.stdout.write(f"{len(_res)} {'#'*int(len(_res)/10)}\n")##
                sys.stdout.flush()##
            
            count += 1
            _responsive_wait(polling_ms / 1000)
        
        except socket.timeout:
            continue

        except (ConnectionError, OSError) as e:
            # Conn NG -> to Offline Mode
            _sig = ErrorIF.report_exception(
                e,
                phase="loop",
                category="comm",
                trigger_id=TRIGGER_NAME,
                log_file=log_file
            )
            
            tcp_socket.close()
            tcp_socket = None
            _rem = ""# 前回接続の残骸を持ち越さない（A-13）
            reconnect_fail_count = 0
            last_reconnect_try   = time.time()
            is_online = False
            base.output_log(MODULE_NAME, f"Connection Lost. to Offline. {server_ip}:{server_port}", log_file)
            
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
                
            continue

        except err.TriEdgeDataError as e:
            """区切り文字の受信上限超過（プロトコル異常）。通信断ではないのでオフライン化せず、バッファを捨てて継続する（A-13）"""
            _rem = ""
            _sig = ErrorIF.report_exception(
                e,
                phase="loop",
                category="data",
                trigger_id=TRIGGER_NAME,
                log_file=log_file
            )
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue

        except Exception as e:
            """通信以外の予期しない例外（JSONデコード失敗等）。オフライン化はせずスキップして継続する"""
            _sig = ErrorIF.report_exception(
                e,
                phase="loop",
                category="data",
                trigger_id=TRIGGER_NAME,
                log_file=log_file
            )
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue
    """======== MAIN LOOP END ========"""
    try:
        if tcp_socket: tcp_socket.close()
    except Exception:
        pass
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None
