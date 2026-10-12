# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Agent Module [Socket Server]
# Ver: 01.d.20261001    By: IWATA, Y.'s Agent AI
# Mod: モジュール名を tri_edge_XX_* から tri_sXX_* 形式に変更（tri_edge_19_socket_server_agent → tri_s19_socket_server_agent）
########################################

動作概要
    1. 環境変数（TRIGGER_NAME / TRIGGER_SYSTEM_ROOT / BULK_READ 等）から設定を読み込む
    2. TCP ソケットサーバを起動し、親トリガー（f_agentic_trigger 等）からの接続を待機
    3. GET リクエストを受信したら PLC 等から最新値を取得して JSON 形式で返す
    4. ACK 受信で次の取得サイクルへ、NACK 受信で同じ値を再送
    5. AGENT_STOP_FILE が存在する場合はサーバを終了
    6. ハートビートファイル（TRIGGER_HB_FILE）の更新を確認し、タイムアウトで自己終了
 - NOT to Use f-string for Retrofit.
"""
MODULE_NAME = "AGENT"
AGENT_NAME = "tri_s19_socket_server_agent"
boot_log   = "{}_Boot.log".format(AGENT_NAME)

"""standard library"""
from   datetime import datetime as dt
import json
import os
import pathlib
import socket
import sys
import threading
import time
import traceback
import random

"""external library"""
## NOT Use

"""local module"""
## NOT Use


####################
## System Dir, File (from OS.Environ)
####################
def _now_str():
    return dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def _check_not_null(_name, _val, _log):
    if not _val:
        msg = "{} [{}] {} NOT Exists. Stop Process.".format(_now_str(), MODULE_NAME, _name)
        print(msg, flush=True)
        with open(_log, "a", encoding="utf-8-sig") as f: f.write(msg + "\n")
        sys.exit(-1)
    else:
        msg = "{} [{}] {}={}".format(_now_str(), MODULE_NAME, _name, _val)
        print(msg, flush=True)
        with open(_log, "a", encoding="utf-8-sig") as f: f.write(msg + "\n")
    return None

def _check_file_exists(_name, _path, _log):
    if not _path.name or not _path.exists():
        msg = "{} [{}] {} NOT Exists. Stop Process.".format(_now_str(), MODULE_NAME, _name)
        print(msg, flush=True)
        with open(_log, "a", encoding="utf-8-sig") as f: f.write(msg + "\n")
        sys.exit(-1)
    else:
        msg = "{} [{}] {}={}".format(_now_str(), MODULE_NAME, _name, _path)
        print(msg, flush=True)
        with open(_log, "a", encoding="utf-8-sig") as f: f.write(msg + "\n")
    return None

def get_agent_root():
    if getattr(sys, 'frozen', False): # PyInstaller
        agent_base_dir = pathlib.Path(sys.executable).parent
    elif "__compiled__" in dir():     # Nuitka
        agent_base_dir = pathlib.Path(sys.executable).parent
    else:                             # Python
        agent_base_dir = pathlib.Path(__file__).parent
    return agent_base_dir


AGENT_ROOT = get_agent_root()
_check_not_null("AGENT_ROOT", AGENT_ROOT, boot_log)

PARENT_TRIGGER = os.environ.get("TRIGGER_NAME", None)
_check_not_null("PARENT_TRIGGER", PARENT_TRIGGER, boot_log)

TRIGGER_SYSTEM_ROOT = os.environ.get("TRIGGER_SYSTEM_ROOT", None)
_check_not_null("TRIGGER_SYSTEM_ROOT", TRIGGER_SYSTEM_ROOT, boot_log)

LOG_PATH = pathlib.Path(TRIGGER_SYSTEM_ROOT) / "log" / "{}_{}.log".format(AGENT_NAME, PARENT_TRIGGER)
_check_not_null("LOG_PATH", LOG_PATH, boot_log)
if LOG_PATH.name and LOG_PATH.exists():
    try: LOG_PATH.unlink(missing_ok=True)
    except Exception: pass

AGENT_STOP_FILE = os.environ.get("AGENT_STOP_FILE", None)
_check_not_null("AGENT_STOP_FILE", AGENT_STOP_FILE, LOG_PATH)
agent_stop_file_path = pathlib.Path(AGENT_STOP_FILE)
if agent_stop_file_path.name and agent_stop_file_path.exists():
    try: agent_stop_file_path.unlink(missing_ok=True)
    except Exception: pass

TRIGGER_HB_FILE = os.environ.get("TRIGGER_HB_FILE", None)
_check_not_null("TRIGGER_HB_FILE", TRIGGER_HB_FILE, LOG_PATH)
trigger_hb_file_path = pathlib.Path(TRIGGER_HB_FILE)


####################
## Config
####################
def read_json_safely(filepath: pathlib.Path):
    """check encode, new line and read json"""
    if not (filepath.name and filepath.exists()):
        return {}, ""
    
    encodings = ['utf-8-sig', 'cp932', 'euc-jp']
    content, error = "", None
    for enc in encodings:
        try:
            with open(filepath, mode='r', encoding=enc) as f:
                content = json.load(f)
            return content, enc        
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            error = e
            continue            
    raise ValueError("Encode NOT Match in {} or Format INVALID: {}".format(encodings, filepath)) from error

CFG_PATH = AGENT_ROOT / "{}.json".format(AGENT_NAME)
_check_file_exists("CFG_PATH", CFG_PATH, LOG_PATH)
config, _ = read_json_safely(CFG_PATH)
if not config:
    print("{} [{}] Setting File CAN NOT Loaded. Stop Process.".format(_now_str(), MODULE_NAME), flush=True)
    sys.exit(-1)
print("config={}".format(config), flush=True)

VERBOSE  = config.get("VERBOSE", False)
encode   = config.get("encode", "utf-8")
sep      = config.get("sep", "\n")
chunk    = config.get("chunk", 1024)
print("{} [{}] VERBOSE={} encode={} sep={}".format(_now_str(), MODULE_NAME, VERBOSE, encode, sep.encode('utf-8')), flush=True)

ip   = config.get("ip", socket.gethostbyname(socket.gethostname()))
port = config.get("port", 52001)
timeout_sec = config.get("timeout_sec", 5.0)
hb_timeout_sec = config.get("hb_timeout_sec", 10.0)
print("{} [{}] ip={} port={} timeout_sec={} hb_timeout_sec={}".format(_now_str(), MODULE_NAME, ip, port, timeout_sec, hb_timeout_sec), flush=True)

req_rep = config.get("req_rep", True)
push_interval_sec = config.get("push_interval_sec", 0.01)
print("{} [{}] req_rep={} push_interval_sec={}".format(_now_str(), MODULE_NAME, req_rep, push_interval_sec), flush=True)

"""Col Define (from OS.Environ)"""
key_define = json.loads(os.environ.get("KEY_DEFINE", "[]"))
dtpye_define = json.loads(os.environ.get("DTYPE_DEFINE", "[]"))
print("{} [{}] key_define={}".format(_now_str(), MODULE_NAME, key_define), flush=True)
print("{} [{}] dtpye_define={}".format(_now_str(), MODULE_NAME, dtpye_define), flush=True)


####################
## Supervise Thread
####################
def _watch_control_files():
    """Supervise Control File and Heartbeat"""
    
    CHECK_INTERVAL = 2.0
    STARTUP_GRACE_PERIOD = 10.0  # Skip HB Check when Startup
    startup_time = time.time()

    """==== Loop Start ===="""
    while not _stop_event.is_set():
        time.sleep(CHECK_INTERVAL)

        """(A) Check Control File"""
        if agent_stop_file_path.name and agent_stop_file_path.exists():
            _stop_event.set()
            
            msg = "{} [{}] Stop File Detected. Self-Stop.".format(_now_str(), MODULE_NAME)
            print(msg, flush=True)
            with open(LOG_PATH, "a", encoding="utf-8-sig") as f: f.write(msg + "\n")
            return

        """(B) Check Trigger Heartbeat (to Stop Gently)"""
        if time.time() - startup_time < STARTUP_GRACE_PERIOD:
            continue

        if trigger_hb_file_path.name and trigger_hb_file_path.exists():
            try:
                hb_age = time.time() - trigger_hb_file_path.stat().st_mtime
                if hb_age > hb_timeout_sec:
                    _stop_event.set()
                    
                    msg = "{} [{}] Trigger Heartbeat Timeout ({:.1f}s > {:.1f}s). Self-Stop.".format(_now_str(), MODULE_NAME, hb_age, hb_timeout_sec)
                    print(msg, flush=True)
                    with open(LOG_PATH, "a", encoding="utf-8-sig") as f: f.write(msg + "\n")
                    return
                    
            except Exception as e:
                msg = "{} [{}] Heartbeat Check Error: {}\n{}".format(_now_str(), MODULE_NAME, e, traceback.format_exc())
                print(msg, flush=True)
                with open(LOG_PATH, "a", encoding="utf-8-sig") as f: f.write(msg + "\n")
                pass

        elif trigger_hb_file_path.name: # as just booted
            pass
    """==== Loop End ===="""
    return


_stop_event = threading.Event()
_watch_thread = threading.Thread(target=_watch_control_files, daemon=True)
_watch_thread.start()

msg = "{} [{}] Watch Thread Started.".format(_now_str(), MODULE_NAME)
print(msg, flush=True)
with open(LOG_PATH, "a", encoding="utf-8-sig") as f: f.write(msg + "\n")


####################
## Helper Func
####################
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


def gen_value(dtypes):####
    """Generate Demo Values"""
    value = []
    for dtype in dtypes:
        if   dtype.lower() == "time":
            value.append(
                random.choice([
                    ##"abc",  ## INVALID VALUE ####
                    dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] ####
                ])
            )
        elif dtype.lower() == "int":
            value.append(
                random.randint(0, 100)
            )
        elif dtype.lower() == "float":
            value.append(
                random.choice([
                    ##"abc",  ## INVALID VALUE ####
                    round(random.uniform(0, 100), 2) ####
                ])
            )
        elif dtype.lower() == "str":
            value.append(
                random.choice([
                    "", "A", "BCDEF", "XYZ", ####
                    "吾輩わがはいは猫である。名前はまだ無い。どこで生れたかとんと見当けんとうがつかぬ。何でも薄暗いじめじめした所でニャーニャー泣いていた事だけは記憶している。吾輩はここで始めて人間というものを見た。しかもあとで聞くとそれは書生という人間中で一番獰悪どうあくな種族であったそうだ。この書生というのは時々我々を捕つかまえて煮にて食うという話である。"  #### LARGE VALUE
            ]))
        else:
            value.append(None)
    return value


####################
## Socket Server Init
####################
tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
tcp_server.bind((ip, port))
tcp_server.listen(1)
tcp_server.settimeout(timeout_sec)

msg = "{} [{}] Server Ready. Waiting Connection at {}:{}".format(_now_str(), MODULE_NAME, ip, port)
print(msg, flush=True)
with open(LOG_PATH, "a", encoding="utf-8-sig") as f: f.write(msg + "\n")
time.sleep(0.2)


####################
## Main Loop
####################
"""==== LOOP (Re-Connection) START ===="""
while not _stop_event.is_set():
    try:
        """Waiting Connect"""
        client, address = tcp_server.accept()
        if VERBOSE: print("{} [{}] Connected from {}".format(_now_str(), MODULE_NAME, address), flush=True)
        time.sleep(0.2)

        """==== LOOP (Send/Receive) START ===="""
        _req_remain = ""
        while not _stop_event.is_set():
            """Check Control / Heartbeat"""
            if agent_stop_file_path.name and agent_stop_file_path.exists():
                break
            
            if req_rep:# Cli: Request -> Svr: Replay
                try: _req_result, _req_remain = recv_until_delimiter(client, sep, encode, chunk, _req_remain)
                except BrokenPipeError: break
                except OSError: break
                
                if not _req_result:
                    break
                
                req_stripped = _req_result.strip()
                if req_stripped in set(["GET", "NACK"]):
                    if VERBOSE: print("{} [{}] RCV: {}".format(_now_str(), MODULE_NAME, req_stripped))
                    
                    value    = gen_value(dtpye_define)####
                    send_str = json.dumps(value) + sep
                    try: client.sendall(send_str.encode(encode))
                    except BrokenPipeError: break
                    except OSError: break
                    
                #### TBD (Post) / ####
                elif req_stripped.startswith("POST"):
                    if VERBOSE: print("{} [{}] RCV: {}".format(_now_str(), MODULE_NAME, req_stripped))
                    
                    data = req_stripped.split(",")[1:]
                    try:
                        msg = "{} [{}] RCV: {} {}".format(_now_str(), MODULE_NAME, req_stripped[:4], data)
                        with open(LOG_PATH, "a", encoding="utf-8-sig") as f: f.write(msg + "\n")
                        client.sendall(("ACK" + sep).encode(encode))
                    except BrokenPipeError: break
                    except OSError: break
                #### / TBD ####
                    
                elif req_stripped == "ACK":
                    if VERBOSE: print("{} [{}] RCV: {} Confirmed to Transfer.".format(_now_str(), MODULE_NAME, req_stripped))
                    
                else:
                    if VERBOSE: print("{} [{}] Unknown Request: {}".format(_now_str(), MODULE_NAME, _req_result), flush=True)
            
            else:# SVR Push -> OS buf <- Cli Rcv
                value        = gen_value(dtpye_define)
                send_str     = json.dumps(value) + sep
                try: client.sendall(send_str.encode(encode))
                except BrokenPipeError: break
                except OSError: break
                
                time.sleep(push_interval_sec)                
        """==== LOOP (Send/Receive) END ===="""
        try:
            client.close()
        except Exception as e:
            print("{} [{}] Exception: {}\n{}".format(_now_str(), MODULE_NAME, e, traceback.format_exc()), flush=True)
            pass
        
        if VERBOSE: print("{} [{}] Client Closed. Wait Next Connection.".format(_now_str(), MODULE_NAME), flush=True)

    except socket.timeout:
        print("{} [{}] Socket Timeout. Continue.".format(_now_str(), MODULE_NAME), flush=True)
        continue

    except KeyboardInterrupt:
        print("{} [{}] KeyboardInterrupt. Server Stopped.".format(_now_str(), MODULE_NAME), flush=True)
        _stop_event.set()
        break

    except Exception as e:
        print("{} [{}] Exception: {}\n{}".format(_now_str(), MODULE_NAME, e, traceback.format_exc()), flush=True)
        if _stop_event.is_set():
            break
        time.sleep(1)
        continue

"""==== LOOP (Re-Connection) END ===="""

####################
## Clean-up
####################
print("{} [{}] Server Cleanup.".format(_now_str(), MODULE_NAME), flush=True)
try: tcp_server.close()
except Exception: pass

if agent_stop_file_path.name:
    try: agent_stop_file_path.unlink(missing_ok=True)
    except Exception: pass

print("{} [{}] Server Exit.".format(_now_str(), MODULE_NAME), flush=True)
sys.exit(0)
