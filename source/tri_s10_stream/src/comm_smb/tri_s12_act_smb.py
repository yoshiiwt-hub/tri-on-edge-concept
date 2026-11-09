# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [Remote File (SMB)]
# Ver: 02.h.20261108    By: IWATA, Y.'s Agent AI
# Mod: pyflakes の指摘を整理（使っていない import・変数、置き換えの無い f-string を削除。動作は変えない）
########################################
Find が見つけたファイルを、ネットワーク上の共有フォルダ（SMB）へ送り届ける出口

主要関数:
a_send_remote_file_trigger  # セグメントの file_path 列のファイルを SMB の共有フォルダへアップロードし、送った後のファイルを後処理する。

位置づけ: 上位側の共有フォルダへファイルを集約する場合に使う。FTP サーバーへ送る場合は tri_s12_act_ftp を使う。
設計意図: 一時名（.tmp）に書き込んでからリネームし、受け手が書込み途中のファイルを読まないようにする。保存先のフォルダは、固定のパスのほか、行の値（ロット番号等）で行ごとに変えられるようにする。送った後のファイルを残す場合は、送信済みを SQLite に記録して二重に送らない。共有フォルダに届かなければ、つなぎ直して送り直し（リトライ）、続けて失敗したら接続できない（not_connectable）とする。接続できない状態の記録・再接続の間隔は tri_s01_connect_state に任せる。
前提・制約: パスワードは環境変数（SMB_PASSWORD）で渡す。絶対パスのファイルのみ扱う。
"""
MODULE_NAME = "ACT_SMB"

"""standard library"""
from   datetime import datetime  as dt
import os
import queue
import shutil
import sqlite3
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, Tuple, Optional

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
from   src.tri_s01_error_if import ErrorIF, SIGNAL_STOP
from   src.tri_s01_connect_state import ConnectState
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

def _upload_to_smb(local_path: str, smb_dir: str, overwrite: bool) -> Tuple[bool, str]:
    """
    ローカルファイルを SMB へアップロードする（<名前>.tmp に書き込んでからリネーム。失敗時は .tmp を削除）
    Returns:
        (アップロードしたか（overwrite=false で同名があればスキップして False）, リモートパス)
    """
    file_name   = os.path.basename(local_path)
    remote_path = f"{smb_dir}\\{file_name}"
    if not overwrite and smbclient.path.exists(remote_path):
        return False, remote_path

    tmp_path = f"{remote_path}.tmp"
    try:
        with open(local_path, "rb") as src_f:
            with smbclient.open_file(tmp_path, mode="wb") as dst_f:
                shutil.copyfileobj(src_f, dst_f)
        smbclient.replace(tmp_path, remote_path)
    except Exception:
        try:
            smbclient.remove(tmp_path)
        except Exception:
            pass
        raise
    return True, remote_path


########################################
# Act Trigger Function
########################################

def a_send_remote_file_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """
    セグメントの file_path 列のファイルを SMB 共有フォルダへアップロードする
    Mod: 2026.10.06    By: IWATA, Y.'s Agent AI（remote_dir / move_to に数値を指定すると、その列の値をアップロード先・移動先にする）

    [動作の要点]（旧モジュール Docstring から移動）
        相対パス・フォルダは処理せず、異常としてログに残す
        remote_dir / move_to に数値を指定した場合（Find と同じ列番号。行ごとにその列の値をフォルダにする）:
            値がファイルパス（末尾の要素に拡張子あり）なら、そのフォルダを使う
            remote_dir: 相対パスは共有フォルダ（smb_share）からの相対。絶対パスは //smb_server/smb_share 配下の UNC のみ
                        （別の共有・ドライブ指定・".." を含むパスは異常）
            move_to   : 相対パスは system_root から、絶対パスはそのまま
            無いフォルダは作成する。値が空・"____"・使えない場合は処理せず異常として記録する（ファイルは元の場所に残る）
            make_subdir_level は、この列の値で決めたフォルダの下に適用する
        運用系メトリクス: last_ok_file / total_ok_files / last_ng_file / total_ng_files（正常は画面表示のみ、異常はログファイルにも残す）
        接続の扱い:
            送信に失敗したら、共有フォルダに届くか（セッションの登録とフォルダの確認）を確かめる。
            届かなければ接続の失敗として retry_wait_sec ごとにつなぎ直して送り直し、連続 retry_max 回で not_connectable とする。
            届くならファイル側の失敗（権限・パス等）として、今までどおりそのファイルだけ異常として記録する
            接続できない間は reconnect_interval_sec ごとに再接続を試みる。届いたファイルは not_connectable_action に従う
            （"wait" = 再接続できるまで待つ（既定）/ "skip" = 送らない。後処理もしないので、ファイルは元の場所に残る）
            接続の状態（ConnectState が登録。ACT_SMB）: connect_state / not_connectable_since / reconnect_fail_count / total_not_connectable
    """
    TRIGGER_NAME = "a_send_remote_file_trigger"

    """======== STARTUP ========"""
    conn_seen = None
    try:
        n_col_def = len(col_def)
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}", log_file)

        # ---- 入力設定 ----
        file_path_col = base.resolve_act_col(config.get("file_path_col", 0), col_def, "file_path_col")  # Find と同じ列番号
        stable_check_n = int(config.get("stable_check_n", 1))

        # ---- アップロード後の処理 ----
        after_upload = config.get("after_upload", "keep")
        if after_upload not in ("keep", "delete", "move", "rename"):
            raise ValueError(f"config['after_upload'] Invalid. after_upload={after_upload!r}")

        move_to, move_to_col = "", None
        if after_upload == "move":
            move_to, move_to_col = utl.parse_dir_setting(config, "move_to", n_col_def)  # 数値なら列の値を移動先にする
            if move_to_col is None:
                if not move_to:
                    raise ValueError("config['move_to'] is NOT Set.")
                move_to = base.get_absolute_path(move_to)
                os.makedirs(move_to, exist_ok=True)

        _pfx_sfx   = list(config.get("rename_pfx_sfx", None) or []) + ["", ""]
        rename_pfx = _pfx_sfx[0] or ""
        rename_sfx = _pfx_sfx[1] or ""
        do_rename  = after_upload == "rename" and bool(rename_pfx or rename_sfx)
        make_subdir_level = utl.parse_make_subdir_level(config)

        # ファイルが残る場合（keep、接頭語・接尾語とも空の rename）は、送信済みを記録して再送しない
        record_seen = after_upload == "keep" or (after_upload == "rename" and not do_rename)
        if record_seen:
            seen_db_path = os.path.join(work_db_dir, f"{TRIGGER_NAME}_uploaded_files.db")
            os.makedirs(work_db_dir, exist_ok=True)
            conn_seen = sqlite3.connect(seen_db_path, check_same_thread=False)
            conn_seen.execute("CREATE TABLE IF NOT EXISTS uploaded_files (path TEXT, mtime REAL, PRIMARY KEY (path, mtime))")
            conn_seen.commit()
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} uploaded_files DB={seen_db_path}", log_file)

        # ---- SMB接続設定 ----
        smb_server   = config.get("smb_server", "")
        smb_port     = int(config.get("smb_port", 445))
        smb_share    = config.get("smb_share", "")
        smb_username = config.get("smb_username", "") or os.getenv("SMB_USERNAME", "")
        smb_password = config.get("smb_password", "") or os.getenv("SMB_PASSWORD", "")
        smb_domain   = config.get("smb_domain",   "") or os.getenv("SMB_DOMAIN",   "")
        overwrite    = bool(config.get("overwrite", True))
        retry_max              = int(config.get("retry_max", 3))
        retry_wait_sec         = float(config.get("retry_wait_sec", 5.0))
        reconnect_interval_sec = float(config.get("reconnect_interval_sec", 10.0))
        if retry_max < 1 or retry_wait_sec < 0:
            raise ValueError(f"config['retry_max'] must be >= 1 and config['retry_wait_sec'] >= 0. Got: {retry_max} / {retry_wait_sec}")
        not_connectable_action = str(config.get("not_connectable_action", "wait")).strip().lower()
        if not_connectable_action not in ("skip", "wait"):
            raise ValueError(f"config['not_connectable_action'] must be 'skip' / 'wait'. Got: {not_connectable_action!r}")

        if not smb_server or not smb_share:
            raise ValueError(f"smb_server and smb_share required. smb_server={smb_server!r} smb_share={smb_share!r}")

        _remote_dir, remote_dir_col = utl.parse_dir_setting(config, "remote_dir", n_col_def)  # 数値なら列の値をアップロード先にする
        remote_dir = _remote_dir.replace("/", "\\").strip("\\")
        if remote_dir == ".":
            remote_dir = ""
        smb_base   = f"\\\\{smb_server}\\{smb_share}"
        smb_dir    = f"{smb_base}\\{remote_dir}" if remote_dir else smb_base

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} file_path_col={file_path_col} stable_check_n={stable_check_n} "
            f"after_upload={after_upload} move_to={move_to} rename_pfx_sfx={[rename_pfx, rename_sfx]} "
            f"smb_dir={smb_dir} remote_dir_col={remote_dir_col} move_to_col={move_to_col} "
            f"overwrite={overwrite} make_subdir_level={make_subdir_level}", log_file)
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} retry_max={retry_max} retry_wait_sec={retry_wait_sec} "
            f"reconnect_interval_sec={reconnect_interval_sec} not_connectable_action={not_connectable_action}", log_file)

        # SMBセッション登録（ドメイン指定時は DOMAIN\user 形式にする）
        session_kwargs: Dict[str, Any] = {"port": smb_port}
        if smb_username:
            session_kwargs["username"] = f"{smb_domain}\\{smb_username}" if smb_domain and "\\" not in smb_username else smb_username
        if smb_password:
            session_kwargs["password"] = smb_password

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

    """======== SMB の接続の状態（判定・リトライはこのトリガー。記録・再接続の間隔・通知は ConnectState） ========"""
    smb_state = ConnectState(TRIGGER_NAME, smb_base, reconnect_interval_sec, MODULE_NAME, log_file)
    smb_fail  = 0       # 連続失敗の回数（判定に使う。このトリガーの中だけで数える）
    smb_ok    = False   # 直前の操作で、共有フォルダに届いていたか

    def _smb_wait(seconds: float) -> None:
        """stop_event を見ながら細かく待つ"""
        _end = time.time() + seconds
        while time.time() < _end and not stop_event.is_set():
            time.sleep(min(0.05, max(0.0, _end - time.time())))

    def _smb_connect() -> Optional[BaseException]:
        """
        セッションを登録し（切れていれば smbclient がつなぎ直す）、共有フォルダに届くか確かめる。成功なら None、失敗ならその例外
        """
        nonlocal smb_ok
        try:
            smbclient.register_session(smb_server, **session_kwargs)
            if remote_dir:
                smbclient.makedirs(smb_dir, exist_ok=True)   # 設定のアップロード先（無ければ作る）
            smbclient.stat(smb_dir)
            smb_ok = True
            return None
        except Exception as e:
            smb_ok = False
            return e

    def _smb_count_fail(e: BaseException, what: str) -> bool:
        """
        接続の失敗を1回数える。連続 retry_max 回で not_connectable と判定して ConnectState へ報告し、False を返す
        （続けるなら retry_wait_sec 待って True。リトライは画面のみに出し、ConnectState・ErrorIF へは報告しない）
        """
        nonlocal smb_fail
        smb_fail += 1
        if smb_fail >= retry_max:
            smb_state.set_not_connectable(e)   # 判定：連続 retry_max 回の失敗
            return False
        if stop_event.is_set():
            return False
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Retry ({smb_fail}/{retry_max}). {what}  {smb_base}  ({type(e).__name__}: {e})")
        _smb_wait(retry_wait_sec)
        return True

    def _smb_run(work, what: str) -> Tuple[bool, Any]:
        """
        work() を実行する。失敗したら共有フォルダに届くかを確かめ、届かなければ接続の失敗として
        retry_wait_sec ごとにつなぎ直してやり直す（リトライ）。届くならファイル側の失敗として、例外のまま返す。
        Returns: (実行できたか（not_connectable になったら False）, work() の戻り値)
        """
        nonlocal smb_fail
        while True:
            e = None if smb_ok else _smb_connect()
            if e is None:
                try:
                    r = work()
                    smb_fail = 0
                    return True, r
                except Exception as ex:
                    if _smb_connect() is None:
                        raise
                    e = ex
            if not _smb_count_fail(e, what):
                return False, None

    def _smb_reconnect_if_due() -> None:
        """接続できない間、再接続の間隔が過ぎていれば再接続を試み、結果を ConnectState へ報告する"""
        nonlocal smb_fail
        if smb_state.should_try_reconnect():
            e = _smb_connect()
            if e is None:
                smb_fail = 0
                smb_state.set_connectable()
            else:
                smb_state.reconnect_failed(e)

    def _smb_ready(seg_id: Any) -> bool:
        """
        送る前に、接続できる状態かを確かめる（接続できない間は再接続を試みる）。
        接続できなければ not_connectable_action に従う: "wait" = 再接続できるまで待つ / "skip" = 送らない（False）
        """
        _smb_reconnect_if_due()
        if smb_state.is_connectable():
            return True
        if not_connectable_action == "skip":
            return False
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} NOT CONNECTABLE - Waiting for reconnect... seg_id={seg_id}  {smb_base}", log_file)
        while not smb_state.is_connectable() and not stop_event.is_set():
            _smb_reconnect_if_due()
            if not smb_state.is_connectable():
                _smb_wait(min(1.0, reconnect_interval_sec))
        return smb_state.is_connectable()

    """SMB の初回接続（retry_wait_sec ごとに retry_max 回まで試みる。つながらなくても止めない）"""
    _smb_err: Optional[BaseException] = None
    for _try in range(1, retry_max + 1):
        _smb_err = _smb_connect()
        if _smb_err is None:
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} SMB Session Registered. server={smb_server} port={smb_port}", log_file)
            break
        if _try < retry_max and not stop_event.is_set():
            base.output_log(MODULE_NAME,       # リトライは画面のみ
                f"{TRIGGER_NAME} SMB Connect Retry ({_try}/{retry_max}). {smb_base}  ({type(_smb_err).__name__}: {_smb_err})")
            _smb_wait(retry_wait_sec)
    if _smb_err is not None:
        _sig = ErrorIF.report_exception(_smb_err, phase="startup", category="comm",  # 初回接続の失敗を報告（A-21）
                                         trigger_id=TRIGGER_NAME, log_file=log_file)
        if _sig == SIGNAL_STOP:
            stop_event.set()
            return None
        smb_state.set_not_connectable(_smb_err, report=False)   # ErrorIF へは上で phase=startup として通知済み

    """======== HELPER ========"""
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

    def _resolve_bases(row: list) -> Tuple[Optional[str], Optional[str], str]:
        """
        アップロード先・移動先の基準フォルダを返す（remote_dir / move_to が列指定なら、その行の列の値から決める）
        Returns: (アップロード先, 移動先, 異常の理由)。使えない値の場合は (None, None, 理由)
        Mod: 2026.10.06    By: IWATA, Y.'s Agent AI
        """
        remote_base, move_base = smb_dir, move_to
        if remote_dir_col is not None:
            folder = utl.get_dir_from_value(row[remote_dir_col])
            remote_base = utl.resolve_smb_dir(folder, smb_base) if folder is not None else None
            if remote_base is None:
                return None, None, f"remote_dir value invalid (col={remote_dir_col}): {row[remote_dir_col]!r}"
        if move_to_col is not None:
            folder = utl.get_dir_from_value(row[move_to_col])
            if folder is None:
                return None, None, f"move_to value empty (col={move_to_col}): {row[move_to_col]!r}"
            move_base = base.get_absolute_path(folder) if folder else system_root
        return remote_base, move_base, ""

    def _remote_dir_for(local_path: str, remote_base: str) -> str:
        """
        アップロード先フォルダ（make_subdir_level 分、元のフォルダ構造を remote_base の下に再現。無ければ作成）
        Mod: 2026.10.06    By: IWATA, Y.'s Agent AI（基準フォルダを引数で受け取る。列指定の場合も作成する）
        """
        dirs = utl.get_make_subdir(local_path, make_subdir_level)
        if not dirs and remote_dir_col is None:
            return remote_base  # 設定のフォルダは起動時に作成済み
        dest = "\\".join([remote_base] + dirs)
        smbclient.makedirs(dest, exist_ok=True)
        return dest

    def _move_dir_for(local_path: str, move_base: str) -> str:
        """after_upload="move" の移動先フォルダ（make_subdir_level 分、元のフォルダ構造を move_base の下に再現。無ければ作成）"""
        dirs = utl.get_make_subdir(local_path, make_subdir_level)
        dest = os.path.join(move_base, *dirs) if dirs else move_base
        os.makedirs(dest, exist_ok=True)
        return dest

    def _is_seen(path: str, mtime: float) -> bool:
        cur = conn_seen.execute("SELECT 1 FROM uploaded_files WHERE path=? AND mtime=? LIMIT 1", (path, mtime))
        return cur.fetchone() is not None

    def _send_file(seg_id: Any, local_path: str, row: list) -> bool:
        """
        1ファイルをアップロードし、後処理する
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

        # アップロード先・移動先（列指定の場合はこの行の値。使えない値なら送らない）
        remote_base, move_base, dest_ng = _resolve_bases(row)
        if dest_ng:
            _report_ng(file_name, f"Destination Invalid, skip: seg_id={seg_id}  {local_path}  {dest_ng}")
            return False

        mtime = os.path.getmtime(local_path)
        if record_seen and _is_seen(local_path, mtime):
            base.output_log(MODULE_NAME, f"Already Uploaded, skip. seg_id={seg_id}  {local_path}")
            return False

        # 安定したファイルのみ
        if stable_check_n > 0 and not utl.check_file_stability_retry(stable_check_n, local_path):
            _report_ng(file_name, f"File NOT Stable, skip. seg_id={seg_id}  {local_path}")
            return False

        # アップロード（共有フォルダに届かないときはリトライ）。接続できない間は not_connectable_action に従う
        #   "wait": 再接続できるまで待って、このファイルを送り直す / "skip": 送らず、後処理もしない（ファイルは元の場所に残る）
        try:
            mtime = os.path.getmtime(local_path)
            done, res = False, None
            while not done and _smb_ready(seg_id):
                done, res = _smb_run(
                    lambda: _upload_to_smb(local_path, _remote_dir_for(local_path, remote_base), overwrite),
                    f"seg_id={seg_id}  {file_name}")
                if not done and (not_connectable_action == "skip" or stop_event.is_set()):
                    break
        except Exception as e_up:
            _report_ng(file_name, f"Upload Error: seg_id={seg_id}  {local_path}  {e_up}")
            _sig = ErrorIF.report_exception(
                e_up, phase="loop", category="io",
                trigger_id=TRIGGER_NAME, log_file=log_file)
            return _sig == SIGNAL_STOP

        if not done:
            _report_ng(file_name, f"NOT CONNECTABLE - NOT Uploaded: seg_id={seg_id}  {local_path}")
            return False
        uploaded, remote_path = res
        if not uploaded:
            base.output_log(MODULE_NAME, f"Remote File Exists, skip (overwrite=false). seg_id={seg_id}  {remote_path}", log_file)
            return False
        _report_ok(file_name, f"Upload OK: seg_id={seg_id}  {local_path} -> {remote_path}")

        # アップロード後の処理
        try:
            if record_seen:
                conn_seen.execute("INSERT OR IGNORE INTO uploaded_files (path, mtime) VALUES (?, ?)", (local_path, mtime))
                conn_seen.commit()
            elif after_upload == "delete":
                os.remove(local_path)
            elif after_upload == "move":
                os.replace(local_path, os.path.join(_move_dir_for(local_path, move_base), file_name))
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
            _smb_reconnect_if_due()   # 接続できない間は再接続を試みる（間隔は ConnectState）
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
                if _send_file(seg_id, str(file_path), row):
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
    if conn_seen is not None:
        try:
            conn_seen.close()
        except Exception:
            pass

    try:
        smbclient.reset_connection_cache()
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} SMB Session Released.", log_file)
    except Exception:
        pass

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None

a_send_remote_file_trigger.QMODE = "imq"
a_send_remote_file_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す
a_send_remote_file_trigger.ACCEPTS = {"qvalue": ("file_path",)}


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
