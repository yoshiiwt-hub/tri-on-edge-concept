# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [Remote File (SMB)]
# Ver: 01.d.20261001    By: IWATA, Y.'s Agent AI
# Mod: モジュール名を tri_edge_XX_* から tri_sXX_* 形式に変更（tri_edge_11_find_smb → tri_s11_find_smb）
########################################
f_find_remote_file_trigger  # SMB共有フォルダを監視し、未処理ファイルのパスをキューに流す

動作概要
    1. smbclient で SMB 共有フォルダに接続し、ファイル一覧を取得
    2. pfx_sfx フィルタでファイル名の接頭語・接尾語を確認
    3. ignore_old 設定で古いファイルをスキップ
    4. SQLite の seen_files DB で発見済みかを確認（重複処理防止）
    5. 安定チェック（サイズ・更新時刻）・ACK ファイルチェックを実施
    6. copy_found=true の場合、ローカルにコピーしてから queue_out に送信
    7. 発見済みファイルを DB に記録して次回スキップ

出力列
    0: file_path   (ファイルのフルパス、copy_found=true の場合はローカルコピー先)
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

■ 接続設定
    smb_server    : SMB サーバのホスト名または IP アドレス（必須）
    smb_port      : SMB ポート番号（デフォルト: 445）
    smb_share     : SMB 共有名（必須）
    smb_username  : 認証ユーザ名（省略可、環境変数 SMB_USERNAME からも取得）
    smb_password  : 認証パスワード（省略可、環境変数 SMB_PASSWORD からも取得）
    smb_domain    : ドメイン名（省略可、環境変数 SMB_DOMAIN からも取得）
    remote_dir    : 監視対象のリモートサブディレクトリ（デフォルト: 共有ルート）
    search_subdir : サブディレクトリを再帰的に検索するか（デフォルト: false）
    pfx_sfx       : ファイル名フィルタ [[接頭語], [接尾語]]（デフォルト: [[""], [".csv", ".json"]]）
    stable_check_n: 安定チェック回数（デフォルト: 1）
    check_ack     : ACK ファイル存在チェックを行うか（デフォルト: false）
    ack_file_ext  : ACK ファイル拡張子（デフォルト: ".done"）
    ignore_old    : 古いファイルを無視するか（デフォルト: true）
    ignore_old_hour: 無視する閾値時間（デフォルト: 168 = 7日）
    copy_found    : 発見ファイルをローカルにコピーするか（デフォルト: false）
    copy_to       : ローカルコピー先ディレクトリ（デフォルト: "work/spool"）
    interval_sec  : ポーリング間隔秒（デフォルト: 5）
"""
MODULE_NAME = "FIND_SMB"

"""standard library"""
from   datetime import datetime  as dt
import os
import queue
import shutil
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Tuple, Optional

"""external library"""
import smbclient
import smbclient.path

"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()

except ModuleNotFoundError:
    try:
        dir_valid = 'comm_smb'
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
import src.tri_s01_error_if as err
from   src.tri_s01_error_if import ErrorIF, SIGNAL_STOP
import src.tri_s19_util  as utl

"""system dir, file"""
system_root    = base.get_system_root()
log_file       = base.get_log_path()
work_dir       = base.get_work_dir()
work_db_dir    = base.get_work_sub_dirs().get("db"   , os.path.join(work_dir, "db"))
work_spool_dir = base.get_work_sub_dirs().get("spool", os.path.join(work_dir, "spool"))

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# HELPER FUNCTIONS
########################################

def _list_smb_files(
    smb_path: str,
    pfx_sfx: Tuple[List[str], List[str]],
    search_subdir: bool = False,
) -> List[str]:
    """SMB共有フォルダのファイルをリストアップ（名前順）"""
    prefixes = tuple(f.lower() for f in (pfx_sfx[0] or []))
    suffixes = tuple(f.lower() for f in (pfx_sfx[1] or []))
    result = []

    def _scan(path: str) -> None:
        try:
            for entry in smbclient.scandir(path):
                if entry.is_file(follow_symlinks=False):
                    name_lower = entry.name.lower()
                    match_pfx = name_lower.startswith(prefixes) if prefixes else True
                    match_sfx = name_lower.endswith(suffixes)   if suffixes else True
                    if match_pfx and match_sfx:
                        result.append(entry.path)
                elif entry.is_dir(follow_symlinks=False) and search_subdir:
                    _scan(entry.path)
        except Exception as e:
            base.output_log(MODULE_NAME, f"_list_smb_files scan error: {path}  {e}", log_file)

    _scan(smb_path)
    result.sort()
    return result


def _check_smb_file_stability(smb_path: str, n: int = 1) -> bool:
    """SMBファイルの安定チェック（サイズ・更新時刻が n 回とも変化なし）"""
    CHECK_INTERVAL = 1
    try:
        for _ in range(n):
            stat1 = smbclient.stat(smb_path)
            time.sleep(CHECK_INTERVAL)
            stat2 = smbclient.stat(smb_path)
            if stat1.st_mtime != stat2.st_mtime or stat1.st_size != stat2.st_size:
                return False
        return True
    except Exception as e:
        base.output_log(MODULE_NAME, f"_check_smb_file_stability error: {smb_path}  {e}", log_file)
        return False


def _check_smb_ack_exists(smb_path: str, ack_ext: str) -> bool:
    """ACKファイルの存在チェック"""
    if not ack_ext:
        return True
    try:
        return smbclient.path.exists(f"{smb_path}.{ack_ext}")
    except Exception:
        return False


########################################
# Find Trigger Functions
########################################

def f_find_remote_file_trigger(
    queue_out: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    SMB共有フォルダを監視し、条件に該当したファイル情報をキューに流す
    Mod: 2026.06.12    By: IWATA,Y.
    """
    TRIGGER_NAME = "f_find_remote_file_trigger"

    try:
        """ SETTING """
        # SMB接続設定
        smb_server   = config.get("smb_server", "")
        smb_port     = int(config.get("smb_port", 445))
        smb_share    = config.get("smb_share", "")
        smb_username = config.get("smb_username", "") or os.getenv("SMB_USERNAME", "")
        smb_password = config.get("smb_password", "") or os.getenv("SMB_PASSWORD", "")
        smb_domain   = config.get("smb_domain",   "") or os.getenv("SMB_DOMAIN",   "")

        if not smb_server or not smb_share:
            raise ValueError(f"smb_server and smb_share required. smb_server={smb_server!r} smb_share={smb_share!r}")

        # リモートフォルダパス構築
        remote_dir = config.get("remote_dir", "").replace("/", "\\").strip("\\")
        smb_base   = f"\\\\{smb_server}\\{smb_share}"
        smb_dir    = f"{smb_base}\\{remote_dir}" if remote_dir else smb_base

        # ファイル監視設定
        search_subdir  = config.get("search_subdir", False)
        pfx_sfx        = config.get("pfx_sfx", [[""], [".csv", ".json"]])
        stable_check_n = int(config.get("stable_check_n", 1))

        check_ack = config.get("check_ack", False)
        ack_ext   = config.get("ack_file_ext", ".done").replace(".", "") if check_ack else ""

        ignore_old      = config.get("ignore_old", True)
        ignore_old_sec  = config.get("ignore_old_hour", 7 * 24) * 3600 if ignore_old else 0

        copy_found = config.get("copy_found", False)
        if copy_found:
            copy_to = base.get_absolute_path(config.get("copy_to", "work/spool"))
            os.makedirs(copy_to, exist_ok=True)

        interval_sec = config.get("interval_sec", 5)

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} smb_dir={smb_dir} search_subdir={search_subdir} pfx_sfx={pfx_sfx} "
            f"stable_check_n={stable_check_n} check_ack={check_ack} ignore_old={ignore_old} "
            f"copy_found={copy_found} interval_sec={interval_sec}",
            log_file)

        """ SMBセッション登録 """
        session_kwargs: Dict[str, Any] = {"port": smb_port}
        if smb_username:
            session_kwargs["username"] = smb_username
        if smb_password:
            session_kwargs["password"] = smb_password

        smbclient.register_session(smb_server, **session_kwargs)
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} SMB Session Registered. server={smb_server} port={smb_port}", log_file)

        """発見済みファイル格納用DB"""
        db_path    = os.path.join(work_db_dir, f"{TRIGGER_NAME}_seen_files.db")
        table_name = "seen_files"
        table_col  = {
            0: ["path" , "TEXT", "PRIMARY KEY"],
            1: ["mdate", "REAL", ""]
        }
        conn = utl.initialize_db(db_path, None, table_name, table_col)
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} seen_files DB={db_path}", log_file)

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
            files = _list_smb_files(smb_dir, pfx_sfx, search_subdir)

            for smb_path in files:
                """ Check File """
                # 指定より古いファイルを無視
                if ignore_old:
                    try:
                        f_mtime = smbclient.stat(smb_path).st_mtime
                        if (_now - f_mtime) > ignore_old_sec:
                            continue
                    except Exception:
                        continue

                # まだ処理していないファイルのみ
                _, is_seen = utl.check_record_exists(db_path, conn, table_name, [table_col[0][0], smb_path])
                if is_seen:
                    continue

                # 安定したファイルのみ
                if not _check_smb_file_stability(smb_path, stable_check_n):
                    base.output_log(MODULE_NAME, f"File NOT Stable, skip. {smb_path}", log_file)
                    continue

                # ACKファイル存在チェック
                if check_ack and not _check_smb_ack_exists(smb_path, ack_ext):
                    continue

                """ファイルのメタ情報"""
                smb_dir_path  = smb_path.rsplit("\\", 1)[0] if "\\" in smb_path else smb_path
                file_name     = smb_path.rsplit("\\", 1)[1] if "\\" in smb_path else smb_path
                base_name, ext = os.path.splitext(file_name)
                dir_base_name  = smb_dir_path.rsplit("\\", 1)[-1] if "\\" in smb_dir_path else smb_dir_path

                try:
                    stat_   = smbclient.stat(smb_path)
                    size_kb = stat_.st_size / 1024
                    mtime_  = stat_.st_mtime
                except Exception:
                    size_kb = 0.0
                    mtime_  = 0.0

                ts_ = dt.fromtimestamp(mtime_) if mtime_ else dt.now()
                ts_yyyymmdd = ts_.strftime("%Y%m%d")
                ts_yyyy     = ts_.strftime("%Y")
                ts_mm       = ts_.strftime("%m")
                ts_dd       = ts_.strftime("%d")
                ts_hh       = ts_.strftime("%H")

                """発見後処理"""
                fpath_mod = smb_path

                # ローカルにコピー
                if copy_found:
                    try:
                        local_dest = os.path.join(copy_to, file_name)
                        with smbclient.open_file(smb_path, mode="rb") as src_f:
                            with open(local_dest, "wb") as dst_f:
                                shutil.copyfileobj(src_f, dst_f)
                        fpath_mod = local_dest
                        base.output_log(MODULE_NAME, f"Copied to local: {local_dest}", log_file)
                    except Exception as e_copy:
                        base.output_log(MODULE_NAME, f"Copy Error: {e_copy}\n{traceback.format_exc()}", log_file)

                """ キュー送信 """
                queue_out.put((
                    count,
                    [
                        fpath_mod,
                        smb_dir_path,
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

                # 発見済みに登録
                utl.insert_records_to_db(db_path, conn, table_name, table_col, [[smb_path, mtime_]])
                base.output_log(MODULE_NAME, f"Queued file: {smb_path}", log_file)

                """ セグメント区切り送信 """
                count += 1
                queue_out.put((count, ['____'] + [None] * (n_col_def - 1)))

            time.sleep(interval_sec)

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            time.sleep(5)
            continue
    """======== LOOP END ========"""

    try:
        conn = utl.initialize_db(db_path, conn, table_name, table_col)
        conn.close()
    except Exception as e:
        ErrorIF.report_exception(e, phase="shutdown", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)

    try:
        smbclient.reset_connection_cache()
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} SMB Session Released.", log_file)
    except Exception:
        pass

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Worker Stop.", log_file)
    return None


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)