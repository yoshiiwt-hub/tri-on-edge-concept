# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [FTP Server]
# Ver: 01.e.20261001    By: IWATA, Y.'s Agent AI
# Mod: モジュール名を tri_edge_XX_* から tri_sXX_* 形式に変更（tri_edge_11_find_ftp → tri_s11_find_ftp）
########################################
f_find_ftp_trigger  # FTPサーバでファイルを受信し、ファイルパスをキューに流す

動作概要
    1. pyftpdlib でFTPサーバを起動（デーモンスレッド）
    2. クライアントがファイルをアップロード → on_file_received コールバック発火
    3. 受信ファイルパスを内部キュー経由でメインループへ渡す
    4. pfx_sfx フィルタ・安定チェック・Ackチェックを経て queue_out に送信

出力列（f_find_file_trigger と同一）
    0: file_path   (受信ファイルのフルパス)
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

pfx_sfx フィルタ
    [[接頭語リスト], [接尾語リスト]]  空リスト = フィルタなし

stable_check
    stable_check_n: 0 = スキップ, 1以上 = 1秒間隔でn回チェック

ack制御
    check_ack   : true → ackファイルが存在する場合のみ処理を実行
    ack_file_ext: ackファイルの拡張子（例: "done"）

接続設定
    ftp_host    : FTPサーバのホスト名またはIPアドレス（環境変数 FTP_HOST）
    ftp_port    : FTPサーバのポート番号（環境変数 FTP_PORT）
    ftp_user    : FTPユーザ名（環境変数 FTP_USER）
    ftp_pass    : FTPパスワード（環境変数 FTP_PASS）
    ftp_perm    : FTPパーミッション文字列（デフォルト: "elw"）
    home_dir    : ファイル受信先ディレクトリ（デフォルト: "work"）
"""
MODULE_NAME = "FIND_FTP"

"""standard library"""
from   datetime import datetime  as dt
import os
import queue
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Optional

"""external library"""
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers    import FTPHandler
from pyftpdlib.servers     import FTPServer

"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()

except ModuleNotFoundError:
    try:
        dir_valid = 'comm_ftp'
        cur_dir  = os.path.dirname(os.path.abspath(__file__))
        dir_name = os.path.basename(cur_dir)
        if dir_name != dir_valid:
            raise FileNotFoundError(f".py MUST be in '{dir_valid}' dir. Current: {dir_name}")

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
import src.tri_s19_util  as utl

"""system dir, file"""
system_root = base.get_system_root()
log_file    = base.get_log_path()
work_dir    = base.get_work_dir()

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# Find Trigger Function
########################################

def f_find_ftp_trigger(
    queue_out:  queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """
    FTPサーバでファイルを受信し、ファイルパスをキューに流す
    Mod: 2026.06.12    By: IWATA,Y.
    """
    TRIGGER_NAME = "f_find_ftp_trigger"

    """======== STARTUP ========"""
    try:
        n_col_def = len(col_def)
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}", log_file)

        """ FTPサーバ設定 """
        ftp_host = str(os.getenv('FTP_HOST', None))
        ftp_port = int(os.getenv('FTP_PORT', None))
        ftp_user = str(os.getenv('FTP_USER', None))
        ftp_pass = str(os.getenv('FTP_PASS', None))
        ftp_perm = str(config.get("ftp_perm", "elrafmw"))
        if not all([ftp_host, ftp_port, ftp_user, ftp_pass]):
            raise ValueError("Config Error: One or more FTP configuration values are missing or empty.")
        base.output_log(MODULE_NAME, f"FTP Config: {ftp_host}:{ftp_port} user={ftp_user} perm={ftp_perm}", log_file)
        
        """ 詳細パラメータ """
        home_dir = base.get_absolute_path(str(config.get("home_dir", "work")))
        os.makedirs(home_dir, exist_ok=True)
        
        passive_ports = config.get('passive_ports', [60000, 60100])
        server_timeout = int(config.get('server_timeout', 60))
        max_cons_per_ip = int(config.get("max_cons_per_ip", 5)) ## 同一IPからの同時最大接続
        
        base.output_log(MODULE_NAME, f"ftp_home_dir={home_dir} passive_ports={passive_ports} server_timeout={server_timeout} max_cons_per_ip={max_cons_per_ip}", log_file)

        """ pfx_sfx フィルタ """
        pfx_sfx_raw = config.get("pfx_sfx", [[], []])
        _pfx_list = pfx_sfx_raw[0] if len(pfx_sfx_raw) > 0 else []
        _sfx_list = pfx_sfx_raw[1] if len(pfx_sfx_raw) > 1 else []
        filter_prefixes = tuple(p.lower() for p in _pfx_list) if _pfx_list else ()
        filter_suffixes = tuple(s.lower() for s in _sfx_list) if _sfx_list else ()
        base.output_log(MODULE_NAME, f"ftpfilter_prefixes={filter_prefixes} ftpfilter_suffixes={filter_suffixes} ftp_home_dir={home_dir}", log_file)

        """ 安定チェック """
        stable_check_n = int(config.get("stable_check_n", 1))
        base.output_log(MODULE_NAME, f"stable_check_n={stable_check_n}", log_file)

        """ Ack設定 """
        check_ack    = bool(config.get("check_ack", False))
        ack_file_ext = str(config.get("ack_file_ext", "done")).strip().lstrip(".")
        if check_ack and not ack_file_ext:
            raise ValueError("ack_file_ext is required when check_ack=true")
        base.output_log(MODULE_NAME, f"check_ack={check_ack} ack_file_ext={ack_file_ext}", log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config",
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== FTPサーバ起動 ========"""
    # on_file_received → メインループへのスレッド間通信用
    _recv_queue: queue.Queue = queue.Queue()

    class _TriEdgeFTPHandler(FTPHandler):
        def on_connect(self):
            base.metrics_set_last(MODULE_NAME, "last_connect_time", dt.now().strftime("%Y/%m/%d %H:%M:%S"))
            base.output_log(MODULE_NAME,
                f"FTP Client connected: {self.remote_ip}:{self.remote_port}", log_file)

        def on_disconnect(self):
            base.metrics_set_last(MODULE_NAME, "last_disconnect_time", dt.now().strftime("%Y/%m/%d %H:%M:%S"))
            base.output_log(MODULE_NAME,
                f"FTP Client disconnected: {self.remote_ip}:{self.remote_port}", log_file)

        def on_file_received(self, file_path: str) -> None:
            base.metrics_add(MODULE_NAME, "total_received_files", 1)
            base.output_log(MODULE_NAME,
                f"File received: {file_path}", log_file)
            _recv_queue.put(file_path)

        def on_incomplete_file_received(self, file_path: str) -> None:
            base.metrics_add(MODULE_NAME, "total_incomplete_transfers", 1)
            base.output_log(MODULE_NAME,
                f"WARNING  Incomplete transfer, removing: {file_path}", log_file)
            try:
                os.remove(file_path)
            except Exception:
                pass

    server: Optional[FTPServer] = None
    server_thread: Optional[threading.Thread] = None

    try:
        authorizer = DummyAuthorizer()
        authorizer.add_user(ftp_user, ftp_pass, homedir=home_dir, perm=ftp_perm)
        _TriEdgeFTPHandler.authorizer = authorizer
        
        _TriEdgeFTPHandler.passive_ports = range(passive_ports[0], passive_ports[1])
        _TriEdgeFTPHandler.timeout = server_timeout
        _TriEdgeFTPHandler.max_cons_per_ip = max_cons_per_ip
        
        server = FTPServer((ftp_host, ftp_port), _TriEdgeFTPHandler)

        server_thread = threading.Thread(
            target=server.serve_forever,
            daemon=True,
            name=f"{TRIGGER_NAME}_server",
        )
        server_thread.start()

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} FTP Server started. {ftp_host}:{ftp_port}  "
            f"user={ftp_user}  home={home_dir}", log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="comm",
                                    trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None
    
    """======== MAIN LOOP ========"""
    count = 0
    while not stop_event.is_set():
        try:
            file_path = _recv_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        try:
            file_path = str(file_path)

            """pfx_sfxフィルタ"""
            filename_lower = os.path.basename(file_path).lower()
            if filter_prefixes and not filename_lower.startswith(filter_prefixes):
                base.output_log(MODULE_NAME,
                    f"Prefix not matched, skip: {file_path}", log_file)
                continue
            if filter_suffixes and not filename_lower.endswith(filter_suffixes):
                base.output_log(MODULE_NAME,
                    f"Suffix not matched, skip: {file_path}", log_file)
                continue

            """安定チェック"""
            if stable_check_n > 0:
                if not utl.check_file_stability_retry(stable_check_n, file_path):
                    base.output_log(MODULE_NAME,
                        f"File not stable, skip: {file_path}", log_file)
                    continue

            """Ackチェック"""
            if check_ack:
                if not utl.check_ack_file_exists(ack_file_ext, file_path):
                    base.output_log(MODULE_NAME,
                        f"Ack not found, skip: {file_path}", log_file)
                    continue

            """ファイルメタ情報"""
            dir_path, file_name = os.path.split(file_path)
            base_name, ext = os.path.splitext(file_name)
            dir_base_name  = os.path.basename(dir_path)

            if os.path.exists(file_path):
                stat_   = os.stat(file_path)
                size_kb = stat_.st_size / 1024
                mtime_  = stat_.st_mtime
                ts_     = dt.fromtimestamp(mtime_)
            else:
                size_kb = 0.0
                ts_     = dt.now()

            ts_yyyymmdd = ts_.strftime("%Y%m%d")
            ts_yyyy     = ts_.strftime("%Y")
            ts_mm       = ts_.strftime("%m")
            ts_dd       = ts_.strftime("%d")
            ts_hh       = ts_.strftime("%H")

            """キュー送信"""
            queue_out.put((
                count,
                [
                    file_path,
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
            ))
            base.output_log(MODULE_NAME,
                f"Queued #{count}: {file_path}", log_file)
            count += 1

            """セグメント区切り"""
            queue_out.put((count, ['____'] + [None] * (n_col_def - 1)))
            count += 1

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="io",
                                            trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue

    """======== CLEANUP ========"""
    if server is not None:
        try:
            server.close_all()
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} FTP Server stopped.", log_file)
        except Exception:
            pass

    if server_thread is not None:
        server_thread.join(timeout=5.0)

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
