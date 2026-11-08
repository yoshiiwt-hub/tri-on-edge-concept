# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [Socket]
# Ver: 06.k.20261108    By: IWATA, Y.'s Agent AI
# Mod: pyflakes の指摘を整理（使っていない import・変数、置き換えの無い f-string を削除。動作は変えない）
########################################
TCP ソケットサーバーに要求を送り、返ってきた値を取り込む入口

主要関数:
f_socket_client_trigger  # ソケットサーバーに接続して値を定期的に要求し、キューに流す。

位置づけ: 独自のソケットサーバー（他システムやエージェント）から値を受け取る、汎用のクライアント。
設計意図: 応答を受け取った直後の時刻を取得時刻としてキューに入れ、StreamCore の受信時刻ではなく実際に値を得た時刻でセグメントを作れるようにする（A-7）。接続の失敗・受信中の切断はその場でつなぎ直し（リトライ）、続けて失敗したら接続できない（not_connectable）として一定間隔で自動的につなぎ直す。接続できない状態の記録・再接続の間隔は tri_s01_connect_state に任せる。
前提・制約: サーバーは、要求（req_msg）に対して区切り文字（sep）で終わる JSON を返すこと。
"""
MODULE_NAME = "FIND_Socket"

"""standard library"""
from   datetime import datetime as dt
import json
import os
import queue
import socket
import sys
import threading
import time
import traceback
from   typing   import Any, Dict

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
from   src.tri_s01_connect_state  import ConnectState

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
    Mod: 2026.11.06    By: IWATA, Y.'s Agent AI（接続できない状態の管理を ConnectState に置き換え）

    [接続できない状態の扱い]
        接続の失敗・受信中の切断は、retry_wait_sec ごとにその場でつなぎ直す（リトライ。このトリガーの中だけ）。
        連続 retry_max 回失敗したら、接続できない（not_connectable）と判定して ConnectState へ報告し、
        以降は reconnect_interval_sec ごとに再接続を試みて、結果を ConnectState へ報告する（その間は値を取り込まない）。
        受信のタイムアウト（socket.timeout）は失敗に数えない（次の要求へ進む）。
        接続の状態（ConnectState が登録）: connect_state / not_connectable_since / reconnect_fail_count / total_not_connectable
    """
    TRIGGER_NAME = "f_socket_client_trigger"

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
        reconnect_interval_sec = float(config.get("reconnect_interval_sec", 10))
        retry_max              = int(config.get("retry_max", 3))
        retry_wait_sec         = float(config.get("retry_wait_sec", 1.0))
        if retry_max < 1 or retry_wait_sec < 0:
            raise ValueError(f"config['retry_max'] must be >= 1 and config['retry_wait_sec'] >= 0. Got: {retry_max} / {retry_wait_sec}")
        base.output_log(MODULE_NAME,
            f"server_ip={server_ip} server_port={server_port} retry_max={retry_max} retry_wait_sec={retry_wait_sec} "
            f"reconnect_interval_sec={reconnect_interval_sec}", log_file)

        # 接続の状態（判定・リトライはこのトリガー。記録・再接続の間隔・通知は ConnectState）
        conn_state = ConnectState(TRIGGER_NAME, f"{server_ip}:{server_port}", reconnect_interval_sec, MODULE_NAME, log_file)
        
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
    tcp_socket = None
    fail_count = 0      # 連続失敗の回数（判定に使う。このトリガーの中だけで数える）

    def _drop_socket() -> None:
        nonlocal tcp_socket
        if tcp_socket:
            try:   tcp_socket.close()
            except Exception: pass
        tcp_socket = None

    def _on_fail(e: BaseException) -> None:
        """
        接続の失敗・受信中の切断を数える。連続 retry_max 回で not_connectable と判定して ConnectState へ報告する。
        それ未満なら retry_wait_sec 待って、次のループでつなぎ直す（リトライは画面のみ。ConnectState・ErrorIF へは報告しない）
        """
        nonlocal fail_count, _rem
        _drop_socket()
        _rem = ""   # 前回接続の残骸を持ち越さない（A-13）
        fail_count += 1
        if fail_count >= retry_max:
            conn_state.set_not_connectable(e)   # 判定：連続 retry_max 回の失敗
            return
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Retry ({fail_count}/{retry_max}). {server_ip}:{server_port}  ({type(e).__name__}: {e})")
        _responsive_wait(retry_wait_sec)

    # 初回接続（retry_wait_sec ごとに retry_max 回まで試みる）
    _err = None
    for _try in range(1, retry_max + 1):
        try:
            tcp_socket = _connect(server_ip, server_port, connect_timeout_sec)
            _err = None
            base.output_log(MODULE_NAME, f"Socket Comm Connected. {server_ip}:{server_port}", log_file)
            break
        except Exception as e:
            _err = e
            if _try < retry_max:
                base.output_log(MODULE_NAME,       # リトライは画面のみ
                    f"{TRIGGER_NAME} Connect Retry ({_try}/{retry_max}). {server_ip}:{server_port}  ({type(e).__name__}: {e})")
                _responsive_wait(retry_wait_sec)
    if _err is not None:
        _sig = ErrorIF.report_exception(
            _err,
            phase="startup",
            category="comm",
            trigger_id=TRIGGER_NAME,
            log_file=log_file,
        )
        if _sig == SIGNAL_STOP:
            stop_event.set()
            return None
        conn_state.set_not_connectable(_err, report=False)   # ErrorIF へは上で phase=startup として通知済み
    
    """======== MAIN LOOP START ========"""
    count = 0
    _rem = ""
    while not stop_event.is_set():
        try:
            if not conn_state.is_connectable():
                """(Not Connectable) 再接続の間隔ごとに試み、結果を ConnectState へ報告する"""
                if conn_state.should_try_reconnect():
                    try:
                        tcp_socket = _connect(server_ip, server_port, connect_timeout_sec)
                        fail_count = 0
                        _rem = ""
                        conn_state.set_connectable()
                    except Exception as e:
                        tcp_socket = None
                        conn_state.reconnect_failed(e)

                ErrorIF.emit_summary_if_needed()
                _responsive_wait(min(2.0, reconnect_interval_sec))
                continue

            """(Connectable) リトライ中は、ここでつなぎ直す"""
            if tcp_socket is None:
                try:
                    tcp_socket = _connect(server_ip, server_port, connect_timeout_sec)
                except Exception as e:   # 接続のタイムアウトも失敗に数える
                    _on_fail(e)
                    continue
            
            """Server Online"""
            # 1. Socket Send/Receive
            tcp_socket.sendall(req_msg.encode(encode))
            _res, _rem = recv_until_delimiter(tcp_socket, sep, encode, chunk, _rem)
            if not _res or not _res.strip():
                raise ConnectionResetError("Server closed connection.")
            acquired_at = dt.now()  # 取得時刻（応答を受け取った直後。A-7）
            
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
            
            # 2. Put Queue to StreamCore（3つ目は取得時刻。Core が seg_ts・seg_id に使う。A-7）
            queue_out.put((
                count,
                value,
                acquired_at
            ))
            
            ## (3. Viz to Debug)
            if VERBOSE:
                _timestamp  = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]##
                _dev_name = base.load_device_info()["dev_name"]##
                sys.stdout.write(f"{count:<3} {_timestamp} {_dev_name} {value}\n")##
                sys.stdout.write(f"{len(_res)} {'#'*int(len(_res)/10)}\n")##
                sys.stdout.flush()##
            
            count += 1
            fail_count = 0   # 取り込めたら連続失敗の回数を 0 に戻す
            _responsive_wait(polling_ms / 1000)
        
        except socket.timeout:
            continue

        except (ConnectionError, OSError) as e:
            # 受信中の切断・通信の失敗 → リトライ（連続 retry_max 回で not_connectable）
            _on_fail(e)
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
