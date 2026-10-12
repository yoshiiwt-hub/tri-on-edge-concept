# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [FTP Client]
# Ver: 02.a.20261004    By: IWATA, Y.'s Agent AI
# Mod: 終点行の除外を各 Act の [:-1] から core へ移し、CUT_LAST_ROW 属性で宣言する形に変更（B-2）
########################################
a_send_ftp_trigger  # セグメントの file_path 列のファイルを FTP サーバへ送信する

動作概要
    1. キューからセグメントを受信し、各行の file_path 列からファイルパスを取得（終点行は除外）
       フルパス（絶対パス）のファイルのみ処理し、相対パス・フォルダは異常としてログに残す
    2. 安定チェック（サイズ・更新時刻）を行う
    3. FTP サーバの remote_dir へ送信する（remote_dir が無ければ上の階層から順に作成）
       atomic=true の場合は <名前>.temp で送信してから本来の名前にリネーム（失敗時は .temp を削除）
    4. 送信後、ローカルのファイルを after_upload に従って処理する（keep / delete / move / rename）
    5. 正常は画面表示のみ、異常はログファイルにも残し、運用系メトリクスへ報告する
         last_ok_file / total_ok_files / last_ng_file / total_ng_files
    ※ 後処理・報告は tri_s12_act_comb_rdb_smb と同じ挙動
    ※ 接続は最初の送信時に行い、以後は使い回す（切断時は次の送信時に再接続）

■ 入力設定
    file_path_col : ファイルパスの列番号（デフォルト: 4。find 側 col:0 は act 側 col:4 になる）
    stable_check_n: 安定チェック回数（デフォルト: 1。0 でチェックしない）

■ FTP接続設定（環境変数。find ftp（FTPサーバ）の FTP_HOST 等とは別）
    FTP_SEND_HOST : FTP サーバのホスト名または IP アドレス（必須）
    FTP_SEND_PORT : FTP サーバのポート番号（デフォルト: 21）
    FTP_SEND_USER : FTP ユーザ名（必須）
    FTP_SEND_PASS : FTP パスワード

■ 送信設定
    remote_dir    : FTP サーバ内の保存先フォルダ（デフォルト: ログイン直後のフォルダ。"/" 始まりは絶対パス）
    atomic        : true → <名前>.temp で送信してからリネーム（デフォルト: false）
    overwrite     : サーバに同じ名前がある場合に上書きするか（デフォルト: true。false ならスキップ）
    passive       : パッシブモードを使うか（デフォルト: true）
    timeout_sec   : 通信タイムアウト秒（デフォルト: 30）

■ アップロード後の処理
    after_upload  : "keep"（残す。デフォルト）/ "delete"（削除）/ "move"（move_to へ移動）/ "rename"（リネーム）
    move_to       : 移動先のローカルフォルダ（after_upload="move" の場合に必須）
    rename_pfx_sfx: [接頭語, 接尾語]（after_upload="rename" の場合）。接頭語は元の名前の前、接尾語は拡張子の後に付ける
                    （例 ["DONE__", ".DONE"]: LOT001_a.csv → DONE__LOT001_a.csv.DONE）。未指定・空欄の側は付けない
    ※ "keep"、または "rename" で接頭語・接尾語とも空の場合は、ファイルが残るため、送信済み（パスと更新時刻）を
       SQLite に記録して再送しない
"""
MODULE_NAME = "ACT_FTP"

"""standard library"""
from   datetime import datetime  as dt
import ftplib
import os
import queue
import sqlite3
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
system_root    = base.get_system_root()
log_file       = base.get_log_path()
work_dir       = base.get_work_dir()
work_db_dir    = base.get_work_sub_dirs().get("db"   , os.path.join(work_dir, "db"))

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# HELPER FUNCTIONS
########################################

def _ftp_exists(ftp: ftplib.FTP, name: str) -> bool:
    """カレントフォルダに同じ名前のファイルがあるか（SIZE で確認。無ければ 550 エラー）"""
    try:
        ftp.size(name)
        return True
    except ftplib.error_perm:
        return False


def _ftp_ensure_dir(ftp: ftplib.FTP, remote_dir: str) -> None:
    """remote_dir へ移動する。無いフォルダは上の階層から順に作成する（権限はある前提）"""
    if remote_dir.startswith("/"):
        ftp.cwd("/")
    for part in [p for p in remote_dir.split("/") if p and p != "."]:
        try:
            ftp.cwd(part)
        except ftplib.error_perm:
            ftp.mkd(part)
            ftp.cwd(part)


def _upload_to_ftp(ftp: ftplib.FTP, local_path: str, atomic: bool, overwrite: bool) -> Tuple[bool, str]:
    """
    ローカルファイルを FTP サーバのカレントフォルダへ送信する
    atomic=true の場合は <名前>.temp で送信してから本来の名前にリネーム（失敗時は .temp を削除）
    Returns:
        (送信したか（overwrite=false で同名があればスキップして False）, リモートのファイル名)
    """
    file_name = os.path.basename(local_path)
    exists = _ftp_exists(ftp, file_name)
    if exists and not overwrite:
        return False, file_name

    if not atomic:
        with open(local_path, "rb") as f:
            ftp.storbinary(f"STOR {file_name}", f)
        return True, file_name

    tmp_name = f"{file_name}.temp"
    try:
        with open(local_path, "rb") as f:
            ftp.storbinary(f"STOR {tmp_name}", f)
        try:
            ftp.rename(tmp_name, file_name)
        except ftplib.error_perm:
            # 同名ファイルがあるとリネームできないサーバ向け：削除してからリネーム
            if not exists:
                raise
            ftp.delete(file_name)
            ftp.rename(tmp_name, file_name)
    except Exception:
        try:
            ftp.delete(tmp_name)
        except Exception:
            pass
        raise
    return True, file_name


########################################
# Act Trigger Function
########################################

def a_send_ftp_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """
    セグメントの file_path 列のファイルを FTP サーバへ送信する
    Mod: 2026.10.04    By: IWATA, Y.'s Agent AI（相対パス・フォルダは処理しない。A-16）
    """
    TRIGGER_NAME = "a_send_ftp_trigger"

    """======== STARTUP ========"""
    conn_seen = None
    try:
        n_col_def = len(col_def)
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}", log_file)

        # ---- 入力設定 ----
        file_path_col = int(config.get("file_path_col", 4))
        if not (0 <= file_path_col < n_col_def):
            raise ValueError(f"file_path_col={file_path_col} out of range (0-{n_col_def-1})")
        stable_check_n = int(config.get("stable_check_n", 1))

        # ---- FTP接続設定（環境変数） ----
        ftp_host = os.getenv("FTP_SEND_HOST", "")
        ftp_port = int(os.getenv("FTP_SEND_PORT", "") or 21)
        ftp_user = os.getenv("FTP_SEND_USER", "")
        ftp_pass = os.getenv("FTP_SEND_PASS", "")
        if not ftp_host or not ftp_user:
            raise ValueError(f"FTP_SEND_HOST and FTP_SEND_USER required. host={ftp_host!r} user={ftp_user!r}")

        # ---- 送信設定 ----
        remote_dir  = str(config.get("remote_dir", "")).replace("\\", "/")
        atomic      = bool(config.get("atomic", False))
        overwrite   = bool(config.get("overwrite", True))
        passive     = bool(config.get("passive", True))
        timeout_sec = float(config.get("timeout_sec", 30))

        # ---- アップロード後の処理 ----
        after_upload = config.get("after_upload", "keep")
        if after_upload not in ("keep", "delete", "move", "rename"):
            raise ValueError(f"config['after_upload'] Invalid. after_upload={after_upload!r}")

        move_to = ""
        if after_upload == "move":
            if not config.get("move_to", ""):
                raise ValueError("config['move_to'] is NOT Set.")
            move_to = base.get_absolute_path(config["move_to"])
            os.makedirs(move_to, exist_ok=True)

        _pfx_sfx   = list(config.get("rename_pfx_sfx", None) or []) + ["", ""]
        rename_pfx = _pfx_sfx[0] or ""
        rename_sfx = _pfx_sfx[1] or ""
        do_rename  = after_upload == "rename" and bool(rename_pfx or rename_sfx)

        # ファイルが残る場合（keep、接頭語・接尾語とも空の rename）は、送信済みを記録して再送しない
        record_seen = after_upload == "keep" or (after_upload == "rename" and not do_rename)
        if record_seen:
            seen_db_path = os.path.join(work_db_dir, f"{TRIGGER_NAME}_uploaded_files.db")
            os.makedirs(work_db_dir, exist_ok=True)
            conn_seen = sqlite3.connect(seen_db_path, check_same_thread=False)
            conn_seen.execute("CREATE TABLE IF NOT EXISTS uploaded_files (path TEXT, mtime REAL, PRIMARY KEY (path, mtime))")
            conn_seen.commit()
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} uploaded_files DB={seen_db_path}", log_file)

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} ftp={ftp_host}:{ftp_port} user={ftp_user} remote_dir={remote_dir!r} "
            f"atomic={atomic} overwrite={overwrite} passive={passive} timeout_sec={timeout_sec} "
            f"file_path_col={file_path_col} stable_check_n={stable_check_n} "
            f"after_upload={after_upload} move_to={move_to} rename_pfx_sfx={[rename_pfx, rename_sfx]}", log_file)

        # 運用系メトリクスの初期値登録
        base.metrics_add(MODULE_NAME, "total_ok_files", 0)
        base.metrics_add(MODULE_NAME, "total_ng_files", 0)
        base.metrics_set_last(MODULE_NAME, "last_ok_file", "-")
        base.metrics_set_last(MODULE_NAME, "last_ng_file", "-")

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config",
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== HELPER ========"""
    ftp: Optional[ftplib.FTP] = None

    def _close_ftp() -> None:
        nonlocal ftp
        if ftp is not None:
            try:
                ftp.quit()
            except Exception:
                try:
                    ftp.close()
                except Exception:
                    pass
        ftp = None

    def _get_ftp() -> ftplib.FTP:
        """接続を返す（未接続・切断時は接続し、remote_dir へ移動する）"""
        nonlocal ftp
        if ftp is not None:
            try:
                ftp.voidcmd("NOOP")
                return ftp
            except Exception:
                _close_ftp()
        _ftp = ftplib.FTP(timeout=timeout_sec)
        try:
            _ftp.connect(ftp_host, ftp_port)
            _ftp.login(ftp_user, ftp_pass)
            _ftp.set_pasv(passive)
            _ftp.voidcmd("TYPE I")  # SIZE（同名確認）は ASCII モードだと拒否するサーバがあるため、最初からバイナリにする
            _ftp_ensure_dir(_ftp, remote_dir)
        except Exception:
            try:
                _ftp.close()
            except Exception:
                pass
            raise
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} FTP Connected. {ftp_host}:{ftp_port}  dir={_ftp.pwd()}", log_file)
        ftp = _ftp
        return ftp

    def _report_ok(file_name: str, msg: str) -> None:
        """正常：画面表示のみ（ログファイルには書かない）＋メトリクス"""
        base.output_log(MODULE_NAME, msg)
        base.metrics_add(MODULE_NAME, "total_ok_files", 1)
        base.metrics_set_last(MODULE_NAME, "last_ok_file", file_name)

    def _report_ng(file_name: str, msg: str) -> None:
        """異常：ログファイルにも残す＋メトリクス"""
        base.output_log(MODULE_NAME, msg, log_file)
        base.metrics_add(MODULE_NAME, "total_ng_files", 1)
        base.metrics_set_last(MODULE_NAME, "last_ng_file", file_name)

    def _is_seen(path: str, mtime: float) -> bool:
        cur = conn_seen.execute("SELECT 1 FROM uploaded_files WHERE path=? AND mtime=? LIMIT 1", (path, mtime))
        return cur.fetchone() is not None

    def _send_file(seg_id: Any, local_path: str) -> bool:
        """
        1ファイルを送信し、後処理する
        Returns: ErrorIF から停止指示があれば True
        """
        file_name = os.path.basename(local_path)

        if not os.path.isabs(local_path):
            _report_ng(file_name, f"Not an absolute path, skip: seg_id={seg_id}  {local_path}")
            return False
        if not os.path.isfile(local_path):
            _reason = "Not a file (folder), skip" if os.path.isdir(local_path) else "File NOT Found"
            _report_ng(file_name, f"{_reason}: seg_id={seg_id}  {local_path}")
            return False

        mtime = os.path.getmtime(local_path)
        if record_seen and _is_seen(local_path, mtime):
            base.output_log(MODULE_NAME, f"Already Uploaded, skip. seg_id={seg_id}  {local_path}")
            return False

        # 安定したファイルのみ
        if stable_check_n > 0 and not utl.check_file_stability_retry(stable_check_n, local_path):
            _report_ng(file_name, f"File NOT Stable, skip. seg_id={seg_id}  {local_path}")
            return False

        # 送信
        try:
            mtime = os.path.getmtime(local_path)
            uploaded, remote_name = _upload_to_ftp(_get_ftp(), local_path, atomic, overwrite)
        except Exception as e_up:
            _close_ftp()  # 状態が不明なため、次の送信時に接続し直す
            _report_ng(file_name, f"Upload Error: seg_id={seg_id}  {local_path}  {e_up}")
            _sig = ErrorIF.report_exception(
                e_up, phase="loop", category="io",
                trigger_id=TRIGGER_NAME, log_file=log_file)
            return _sig == SIGNAL_STOP

        if not uploaded:
            base.output_log(MODULE_NAME, f"Remote File Exists, skip (overwrite=false). seg_id={seg_id}  {remote_dir}/{remote_name}", log_file)
            return False
        _report_ok(file_name, f"Upload OK: seg_id={seg_id}  {local_path} -> ftp://{ftp_host}:{ftp_port} {remote_dir}/{remote_name}")

        # 送信後の処理
        try:
            if record_seen:
                conn_seen.execute("INSERT OR IGNORE INTO uploaded_files (path, mtime) VALUES (?, ?)", (local_path, mtime))
                conn_seen.commit()
            elif after_upload == "delete":
                os.remove(local_path)
            elif after_upload == "move":
                os.replace(local_path, os.path.join(move_to, file_name))
            elif do_rename:
                os.replace(local_path, os.path.join(os.path.dirname(local_path), f"{rename_pfx}{file_name}{rename_sfx}"))
        except Exception as e_after:
            _report_ng(file_name, f"After Upload Error ({after_upload}): seg_id={seg_id}  {local_path}  {e_after}")
        return False

    """======== LOOP START ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start Loop...", log_file)
    while True:
        """キュー受信"""
        try:
            segment = post_queue.get(timeout=1.0)

        except queue.Empty:
            if (stop_event.is_set() and
                    post_queue.qsize() == 0 and
                    getattr(post_queue, "unfinished_tasks", 0) == 0):
                break
            continue

        """sentinel → 終了"""
        if segment is None:
            post_queue.task_done()
            break

        """セグメント処理"""
        try:
            n_col_act = len(segment[0])
            if n_col_act != n_col_def:
                base.output_log(MODULE_NAME,
                    f"Segment Columns NOT Match: seg_id={segment[0][0]}  "
                    f"Expected:{n_col_def}  Actual:{n_col_act}", log_file)
                continue

            seg_id  = segment[0][0]
            # 終点行（変化後）の除外は core が行う（CUT_LAST_ROW = True）

            stop_requested = False
            for row in segment:
                file_path = row[file_path_col]
                if not file_path or file_path == "____":
                    continue
                if _send_file(seg_id, str(file_path)):
                    stop_requested = True
                    break
            if stop_requested:
                stop_event.set()
                break

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
    _close_ftp()
    if conn_seen is not None:
        try:
            conn_seen.close()
        except Exception:
            pass

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None

a_send_ftp_trigger.QMODE = "imq"
a_send_ftp_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す
a_send_ftp_trigger.ACCEPTS = {"qvalue": ("file_path",)}


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
