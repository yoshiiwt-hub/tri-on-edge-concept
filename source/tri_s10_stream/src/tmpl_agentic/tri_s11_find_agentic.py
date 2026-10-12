# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [Agentic Template]
# Ver: 01.e.20261005    By: IWATA, Y.'s Agent AI
# Mod: 初回接続の失敗を ErrorIF へ報告（phase=startup / category=comm。A-21）
########################################
f_agentic_trigger  # エージェントスクリプトを Subprocess で起動し、TCPソケットでデータを受信してキューに流す

動作概要
    1. agent_script をサブプロセスで起動し、一定時間待機
    2. TCP ソケットで agent に接続し、GET/ACK/NACK プロトコルでデータを受信
    3. 受信 JSON データを col_def の dtype に従いキャストして queue_out に送信
    4. キャスト失敗時は NACK を送信してリトライ（最大 99 回）
    5. 通信失敗時はエージェントを再起動してオフラインモードから復帰
    6. ハートビートファイルを定期的に更新（agent 側の生存確認用）

接続設定
    agent_name            : エージェント識別名（必須）
    agent_script          : エージェントスクリプトのパス（必須）
    agent_python          : Python 実行ファイルのパス（省略可）
    server_ip             : エージェントが待ち受ける IP アドレス（デフォルト: "127.0.0.1"）
    server_port           : エージェントが待ち受ける TCP ポート番号（デフォルト: 52001）
    connect_timeout_sec   : 接続タイムアウト秒（デフォルト: 5）
    server_start_wait_sec : エージェント起動後の待機時間秒（デフォルト: 10.0）
    interval_reconn_sec   : 再接続試行間隔秒（デフォルト: 5.0）
    polling_ms            : ポーリング間隔ミリ秒（デフォルト: 100）
    hb_interval_sec       : ハートビート更新間隔秒（デフォルト: 3.0）
    req_msg               : 要求メッセージ文字列（デフォルト: "GET"）
    encode                : 通信エンコーディング（デフォルト: "utf-8"）
    sep                   : パケット区切り文字（デフォルト: "\\n"）
    chunk                 : 受信チャンクサイズ（デフォルト: 1024）
"""
MODULE_NAME = "FIND_AGENTIC"

"""standard library"""
from   datetime import datetime as dt
import json
import os
import pathlib
import queue
import socket
import subprocess
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, Optional

"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()
    
except ModuleNotFoundError:
    try: # Check and Add Path
        dir_valid = 'tmpl_agentic'  ## (system_root)/src/.
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
from src.tri_s01_error_if import ErrorIF, SIGNAL_STOP

"""system dir, file"""
system_root  = base.get_system_root()
log_file     = base.get_log_path()
work_dir     = base.get_work_dir()
work_run_dir = base.get_work_sub_dirs().get("run", os.path.join(work_dir, "run"))


########################################
# f_agentic_trigger
########################################

def f_agentic_trigger(
    queue_out:  queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    Agent を Subprocess で起動・管理
    Mod: 2026.04.24    By: IWATA,Y.
    """
    TRIGGER_NAME = "f_agentic_trigger"
    _SIG_OFFLINE = f"ConnectionFailed.comm.loop.{TRIGGER_NAME}"
    
    ################################
    # 0. Helper Func
    ################################
    def _connect(_ip: str, _port: int, _to: float) -> socket.socket:
        """socket connection"""
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(_to)
        s.connect((_ip, _port))
        return s

    def recv_until_delimiter(sock, delimiter, decode, chunk=1024, buf=""):
        """receive packet"""
        _counter = 0
        buf_bytes   = buf.encode(decode) if buf else b""
        delim_bytes = delimiter.encode(decode)
        while delim_bytes not in buf_bytes:
            data = sock.recv(chunk)
            if not data: return None, ""
            buf_bytes += data
            _counter += 1
            if 999 < _counter: return None, ""
            
        loc    = buf_bytes.find(delim_bytes)
        result = buf_bytes[:loc + 1].decode(decode)
        remain = buf_bytes[loc + 1:].decode(decode)
        return result, remain
    
    def _start_agent(python_exe: str, script_path: str, env: dict) -> Optional[subprocess.Popen]:
        """start subprocess"""
        try:
            cmd = [python_exe, script_path] if python_exe else [script_path]
            proc = subprocess.Popen(
                cmd,
                env=env,
                cwd=system_root,
            )
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Agent Started. pid={proc.pid} cmd={cmd}", log_file)
            return proc
            
        except Exception as e:
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Agent Start FAILED. {e}", log_file)
            return None

    def _stop_agent_if_exists(proc: Optional[subprocess.Popen], stop_file: pathlib.Path) -> None:
        """stop subprocess"""
        try:
            stop_file.touch()
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop File Created: {stop_file}", log_file)
            
        except Exception as e:
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop File Create FAILED: {e}", log_file)

        if proc is not None:
            try:
                proc.wait(timeout=8.0)
                base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Agent Process Ended. pid={proc.pid}", log_file)
                
            except subprocess.TimeoutExpired:
                proc.kill()
                base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Agent Process Killed (timeout). pid={proc.pid}", log_file)
                
            except Exception as e:
                base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Agent Process Wait Error: {e}", log_file)

    ################################
    # 1. Config
    ################################
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)
    try:
        """Env for Agent"""
        agent_env = os.environ.copy()

        """Col Define"""
        key_define = [col_def[k][0] for k in sorted(col_def.keys())]
        dtype_define = [col_def[k][1] for k in sorted(col_def.keys())]
        agent_env["KEY_DEFINE"]   = json.dumps(key_define)
        agent_env["DTYPE_DEFINE"] = json.dumps(dtype_define)
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} key_define={key_define}", log_file)
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} dtype_define={dtype_define}", log_file)
        
        """dtype cast plan"""
        default_val_config = {
            "time_format"  : config.get("time_format"  , "%Y-%m-%d %H:%M:%S.%f"),
            "time_default" : config.get("time_default" , "2999-01-01 00:00:00.000000"),
            "int_default"  : config.get("int_default"  , -9999),
            "float_default": config.get("float_default", -9999.9),
            "str_default"  : config.get("str_default"  , "__"),
        }
        cast_plan = base.set_cast_dtype(col_def, default_val_config)
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} cast_plan={cast_plan}", log_file)
        
        """System Info"""
        agent_env["TRIGGER_NAME"] = TRIGGER_NAME
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} agent_env['TRIGGER_NAME']={agent_env['TRIGGER_NAME']}", log_file)

        agent_name = config.get("agent_name")
        if not agent_name:
            raise ValueError("[agent_name] is NOT in Setting.")
        
        agent_python = config.get("agent_python")
        script_path = config.get("agent_script")
        if not script_path:
            raise ValueError("[agent_script] is NOT in Setting.")
        if agent_python: agent_python = str(pathlib.Path(agent_python))
        if script_path:  script_path = str(pathlib.Path(script_path))
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} agent_name={agent_name} agent_python={agent_python} script_path={script_path}", log_file)
        
        """System Dir, File"""
        # System Root
        agent_env["TRIGGER_SYSTEM_ROOT"] = system_root
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} agent_env['TRIGGER_SYSTEM_ROOT']={agent_env['TRIGGER_SYSTEM_ROOT']}", log_file)

        # Agent Stop File
        agent_stop_file_path = pathlib.Path(work_run_dir) / f"_{agent_name}_{TRIGGER_NAME}.oneshot_stop"
        if agent_stop_file_path.exists():
            try:
                agent_stop_file_path.unlink()
                base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Old Stop File Removed: {agent_stop_file_path}")
            except:
                base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Old Stop File NOT Removed. System Go on.")
            
        agent_env["AGENT_STOP_FILE"] = str(agent_stop_file_path)
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} agent_stop_file_path={agent_stop_file_path}", log_file)
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} agent_env['AGENT_STOP_FILE']={agent_env['AGENT_STOP_FILE']}", log_file)
        
        # Trigger Heartbeat File
        trigger_hb_path = pathlib.Path(work_run_dir) / f"_{TRIGGER_NAME}_{agent_name}.heartbeat"
        if trigger_hb_path.exists():
            try:
                trigger_hb_path.unlink()
                base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Old Heartbeat File Removed: {trigger_hb_path}")
            except:
                base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Old Heartbeat File NOT Removed. System Go on.")
        
        # Initial Heartbeat 
        try:
            trigger_hb_path.touch()
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Heartbeat File Initialized: {trigger_hb_path}", log_file)
        except Exception as e:
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Heartbeat File Init Failed: {e}", log_file)
        
        agent_env["TRIGGER_HB_FILE"] = str(trigger_hb_path)
        
        hb_interval_sec = float(config.get("hb_interval_sec", 3.0))
        
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} trigger_hb_path={trigger_hb_path} hb_interval_sec={hb_interval_sec}", log_file)
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} agent_env['TRIGGER_HB_FILE']={agent_env['TRIGGER_HB_FILE']}", log_file)
        
        """Config"""
        server_ip   = config.get("server_ip", "127.0.0.1")
        server_port = int(config.get("server_port", 52001))
        polling_ms  = config.get("polling_ms", 100)
        polling_sec = polling_ms / 1000
        
        connect_timeout_sec   = config.get("connect_timeout_sec", 5)
        server_start_wait_sec = config.get("server_start_wait_sec", 10.0)
        interval_reconn_sec   = config.get("interval_reconn_sec", 5.0)
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} server_ip={server_ip} server_port={server_port} connect_timeout_sec={connect_timeout_sec} server_start_wait_sec={server_start_wait_sec} polling_ms={polling_ms}", log_file)
        
        req_msg = config.get("req_msg", "GET")
        encode  = config.get("encode", "utf-8")
        sep     = config.get("sep", "\n")
        chunk   = config.get("chunk", 1024)
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} req_msg={req_msg} encode={encode} sep={sep.encode(encode)} chunk={chunk}", log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    ################################
    # 3. Start Subprocess, Initial Connection
    ################################
    # Start Subprocess and Wait a Little
    agent_proc = _start_agent(
        agent_python,
        script_path,
        agent_env
    )
    time.sleep(server_start_wait_sec)
    
    # Initial Heartbeat Touch
    try:
        trigger_hb_path.touch()
        last_heartbeat_time = time.time()
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Initial Heartbeat Updated.", log_file)
    except Exception as e:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Initial Heartbeat Update Failed: {e}", log_file)

    # Initial Connection
    is_online  = False
    tcp_socket = None
    try:
        tcp_socket = _connect(
            server_ip,
            server_port,
            connect_timeout_sec
        )
        is_online  = True
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Socket Connected. {server_ip}:{server_port}", log_file)
        
    except Exception as e:
        _sig = ErrorIF.report_exception(e, phase="startup", category="comm",  # 初回接続の失敗を報告（A-21）
                                         trigger_id=TRIGGER_NAME, log_file=log_file)
        if _sig == SIGNAL_STOP:
            stop_event.set()
            return None
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Initial Connect FAILED. Go Offline. ({type(e).__name__}: {e})", log_file)

    # Initial Val (Dummy) to Queue
    queue_out.put((0, [None] * len(key_define)))

    ################################
    # 4. Main Loop
    ################################
    """======== INIT LOOP ========"""
    reconnect_fail_count = 0
    last_restart_time    = 0.0
    last_heartbeat_time  = 0.0
    count = 0
    _rem  = ""
    """======== MAIN LOOP START ========"""
    try:
        while not stop_event.is_set():
            _now = time.time()

            """heartbeat touch"""
            if _now - last_heartbeat_time >= hb_interval_sec:
                try:
                    trigger_hb_path.touch()
                    last_heartbeat_time = _now
                except Exception:
                    pass
                    
            """==== OFFLINE MODE ===="""
            if not is_online:
                if _now - last_restart_time >= interval_reconn_sec:
                    """Try to Re-Connect"""
                    last_restart_time = _now
                    # terminate old proc
                    if agent_proc is not None:
                        try:
                            agent_proc.terminate()
                            agent_proc.wait(timeout=5.0)
                        except Exception:
                            try: agent_proc.kill()
                            except Exception: pass
                        agent_proc = None
                    
                    # remove stop file
                    if agent_stop_file_path.exists():
                        try: agent_stop_file_path.unlink()
                        except Exception: pass

                    # restart agent
                    agent_proc = _start_agent(
                        agent_python,
                        script_path,
                        agent_env
                    )
                    time.sleep(server_start_wait_sec)

                    # re-connect
                    try:
                        tcp_socket = _connect(
                            server_ip,
                            server_port,
                            connect_timeout_sec
                        )
                        is_online = True # connected
                        reconnect_fail_count = 0
                        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Re-Connected. {server_ip}:{server_port}  Back to Online.", log_file)

                    except Exception as e:
                        # NOT connected
                        reconnect_fail_count += 1
                        ErrorIF.report_event({
                            "trigger_id": TRIGGER_NAME,
                            "phase"    : "loop",
                            "category" : "comm",
                            "exc_type" : type(e).__name__,
                            "message"  : f"Re-Connect Failed ({reconnect_fail_count} times). {server_ip}:{server_port}",
                            "signature": _SIG_OFFLINE,
                        })
                        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Re-Connect Failed ({reconnect_fail_count}). {server_ip}:{server_port}  ({type(e).__name__}): {e}", log_file)

                ErrorIF.emit_summary_if_needed()
                time.sleep(2)
                continue

            """==== ONLINE MODE ===="""
            try:
                """1. Socket Send/Receive"""
                tcp_socket.sendall(
                    ("GET" + sep).encode(encode)
                )
                _res, _rem = recv_until_delimiter(
                    tcp_socket,
                    sep,
                    encode,
                    chunk,
                    _rem
                )
                if not _res or not _res.strip():
                    raise ConnectionResetError("Server Closed Connection.")
                ##print(f"{_res}", flush=True)##
                
                """2. Cast Dtype"""
                value = json.loads(_res[:-1])
                value, cast_fails = base.cast_dtype(value, cast_plan)
                ##print(f"cast_fails={cast_fails} value={value}", flush=True)##
                
                """loop start"""
                _cnt = 0
                while cast_fails > 0:
                    ErrorIF.report_event({
                        "signature": "CAST_FAIL",
                        "phase"    : "loop",
                        "category" : "data",
                        "trigger_id": TRIGGER_NAME,
                        "message"  : f"Cast failed {cast_fails} cols.",
                        "log_file" : log_file,
                    })
                    
                    tcp_socket.sendall(
                        ("NACK" + sep).encode(encode)
                    )
                    _res, _rem = recv_until_delimiter(
                        tcp_socket,
                        sep,
                        encode,
                        chunk,
                        _rem
                    )
                    if not _res or not _res.strip():
                        raise ConnectionResetError("Server Closed Connection.")
                    ##print(f"_res_post={_res_post}", flush=True)##
                    
                    value = json.loads(_res[:-1])
                    value, cast_fails = base.cast_dtype(value, cast_plan) ##print(f"cast_fails={cast_fails} value={value}", flush=True)##
                    
                    _cnt += 1
                    if _cnt == 99: raise Exception("NACK Retry Limit Reached.")
                """loop end"""
                
                tcp_socket.sendall(
                    ("ACK" + sep).encode(encode)
                )
                
                """3. Put Queue"""
                queue_out.put((
                    count,
                    value
                ))

                """4. Viz (Debug)"""
                _timestamp = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]##
                _dev_name  = base.load_device_info()["dev_name"]##
                sys.stdout.write(f"{count:<3} {_timestamp} {_dev_name} {value}\n")##
                sys.stdout.flush()##
                
                count += 1
                time.sleep(polling_sec)
                
                #### TBD / ####
                '''payload = ','.join([
                    "POST",
                    "2026/1/1 10:00:00",
                    "ABCDE",
                    "10",
                    "10.5",
                    "吾輩わがはいは猫である。名前はまだ無い。どこで生れたかとんと見当けんとうがつかぬ。何でも薄暗いじめじめした所でニャーニャー泣いていた事だけは記憶している。吾輩はここで始めて人間というものを見た。しかもあとで聞くとそれは書生という人間中で一番獰悪どうあくな種族であったそうだ。この書生というのは時々我々を捕つかまえて煮にて食うという話である。"  ## LARGE VALUE
                ])
                print(payload)##
                tcp_socket.sendall(
                    (payload + sep).encode(encode)
                )
                _res, _ = recv_until_delimiter(
                    tcp_socket,
                    sep,
                    encode,
                    chunk,
                    ""
                )
                if not _res or not _res.strip():
                    raise ConnectionResetError("Server Closed Connection.")
                base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Received {_res}")'''
                #### / TBD ####
                
            except socket.timeout:
                continue

            except Exception as e:
                """Conn Error to OFFLINE MODE"""
                _sig = ErrorIF.report_exception(
                    e,
                    phase="loop",
                    category="comm",
                    trigger_id=TRIGGER_NAME,
                    log_file=log_file
                )
                if tcp_socket:
                    try: tcp_socket.close()
                    except Exception: pass
                tcp_socket = None
                _rem  = ""
                is_online = False
                last_restart_time = 0.0
                base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Connection Lost. to Offline. {server_ip}:{server_port}", log_file)

                if _sig == SIGNAL_STOP:
                    stop_event.set()
                    break

                continue
    
    except Exception as e:
        _sig = ErrorIF.report_exception(
            e,
            phase="loop",
            category="data",
            trigger_id=TRIGGER_NAME,
            log_file=log_file
        )
        if _sig == SIGNAL_STOP:
            stop_event.set()
            
        """======== MAIN LOOP END ========"""
    finally:
        """Stop Agent"""
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Shutdown. Creating Stop File.", log_file)
        _stop_agent_if_exists(agent_proc, agent_stop_file_path)
        if tcp_socket:
            try: tcp_socket.close()
            except Exception: pass
        try: trigger_hb_path.unlink(missing_ok=True)
        except Exception: pass

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None
