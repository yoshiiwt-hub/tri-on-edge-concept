# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [FILE HANDLE]
# Ver: 08.c.20261006    By: IWATA, Y.'s Agent AI
# Mod: to_dir に数値を指定すると col_def のインデックスとみなし、行ごとにその列の値を出力先フォルダにする
########################################
a_handle_file_trigger  # 検出したファイルをmove/copy/delete/zip/createする

動作概要
    1. キューからセグメントを受信し、各行のファイルパスを取得
    2. pfx_sfx フィルタでファイル名の接頭語・接尾語を確認
       ファイルパスは find から渡されたフルパス（絶対パス）のファイルのみを処理する。
       相対パス・フォルダのパスは処理せず、異常としてログに残す（op="create" は対象外）
    3. stable_check でファイルが書込み完了済みか確認（サイズ・更新時刻の安定チェック）
       ※ op="create" の場合、対象は未作成のファイルのためスキップされる
    4. check_ack=true の場合、ackファイルの存在を確認（op="create" ではスキップ）
    5. op の指定（move / copy / delete / zip / create）に従いファイルを処理
    6. to_name プレースホルダを展開してリネームしながら操作（move / copy / zip / create 時）
    7. 正常は画面表示のみ、異常（見つからない・安定しない・ack なし・失敗）はログファイルにも残し、
       運用系メトリクスへ報告する（tri_s12_act_comb_rdb_smb と同じ）
         last_ok_file / total_ok_files / last_ng_file / total_ng_files
       ※ pfx_sfx で対象外になったファイルは異常として扱わない（画面表示のみ）
       ※ 同名ファイルがあり上書きしない場合（overwrite=false / create_overwrite=false）はスキップしてログに残す（件数には数えない）

op
    "move"   : to_dir へ移動（to_name 指定でリネームも同時に実施。<名前>.tmp に書き込んでからリネーム）
    "copy"   : to_dir へコピー（to_name 指定でリネームも同時に実施。<名前>.tmp に書き込んでからリネーム）
    "delete" : ファイルを削除
    "zip"    : to_dir へ zip 圧縮。to_name 未指定時は
               "{ファイル名（拡張子除く）}.zip" を使用（<名前>.tmp に書き込んでからリネーム）。
               zip_delete_src=true で圧縮成功後に元ファイルを削除
    ※ フォルダは扱わない（フォルダ処理は別の act として検討中。_dev/src/tri_s12_act_dir.py.draft.txt 参照）
    "create" : to_dir に新規ファイルを作成する。file_path 列の値は実在チェックせず、
               ファイル名のプレースホルダ展開の元ネタとしてのみ使用する。
               create_size_bytes（既定0=空ファイル）バイトのファイルを
               create_fill_byte（既定0x00）で埋めて作成する。
               既にファイルが存在する場合、create_overwrite=false（既定）ならスキップ

to_name プレースホルダ（省略すると元の名前を維持。zip は既定 "{basename}.zip"）
    {filename}  : 元のファイル名（拡張子含む）
    {basename}  : 拡張子なしのファイル名
    {ext}       : 拡張子（ドット含む。例: .csv）
    {yyyymmdd}  : 実行時の日付 YYYYMMDD
    {hhmmss}    : 実行時の時刻 HHMMSS
    {ts}        : 実行時のタイムスタンプ YYYYMMDD_HHMMSS

stable_check
    stable_check_n: 0 = スキップ, 1以上 = 1秒間隔でn回チェック（op="create" では常にスキップ）

ack制御
    check_ack   : true → ackファイルが存在する場合のみ処理を実行（op="create" では常にスキップ）
    ack_file_ext: ackファイルの拡張子（例: "done" → ファイルパス.done）
    delete_ack  : true → 処理完了後にackファイルを削除（op="create" では対象外）

pfx_sfx フィルタ（ファイル名の接頭語・接尾語で処理対象を絞り込み）
    pfx_sfx: [[接頭語リスト], [接尾語リスト]]
    接頭語リスト: 空リスト [] = フィルタなし  ["data_", "log_"] = いずれかで始まるもの
    接尾語リスト: 空リスト [] = フィルタなし  [".csv", ".json"] = いずれかで終わるもの
    例: [[""], [".csv", ".json"]]  →  拡張子 .csv または .json のみ処理

出力先フォルダ（move / copy / zip / create）
    to_dir: 文字列 → そのフォルダ（相対パスは system_root から）
            数値   → col_def のインデックス。行ごとに、その列の値を出力先フォルダにする
                     - 値がファイルパス（末尾の要素に拡張子あり）なら、そのフォルダを使う
                     - 相対パスは system_root から、絶対パスはそのまま。無いフォルダは作成する
                     - 値が空・"____" の場合は処理せず異常として記録する（ファイルは元の場所に残る）
            例: "to_dir": "D:/out" / "to_dir": 7（列7の値 "D:/out/LOT001/a.csv" → D:/out/LOT001 へ）

上書き（move / copy / zip）
    overwrite: false → 移動先・コピー先・圧縮先に同名ファイルがある場合はスキップ（既定 true = 上書き）

フォルダ構造の再現（move / copy / zip）
    make_subdir_level: to_dir の下に、元ファイルのフォルダを何階層再現するか（既定 0）。無いフォルダは作成する
         0 : 再現しない（to_dir 直下にファイルのみ）
         1 : 元ファイルの親フォルダ        例 C:/in/A/B/x.csv → to_dir/B/x.csv
         2 : 親々フォルダ/親フォルダ       例 C:/in/A/B/x.csv → to_dir/A/B/x.csv
        -1 : パスにあるフォルダ全て        例 C:/in/A/B/x.csv → to_dir/in/A/B/x.csv
       渡されたパスにある範囲のみ（ドライブ・UNC の //server/share は含めない。指定より浅いパスはある分だけ）
       ※ delete（移動先なし）・create（実ファイルなし）では使わない

delete専用オプション
    （なし）※ 旧 delete_keep_folder はフォルダ処理の廃止に伴い無視する（設定されていれば起動時に WARN）

zip専用オプション
    zip_delete_src: true → 圧縮成功後に元のファイルを削除する（既定 false）

create専用オプション
    create_size_bytes: 作成するファイルのサイズ（バイト数。既定 0 = 空ファイル）
    create_fill_byte  : ファイルを埋めるバイト値 0-255（既定 0）
    create_overwrite  : true → 作成先に同名ファイルが既にあっても上書きする（既定 false = スキップ）
"""
MODULE_NAME = "ACT_Handle_File"

"""standard library"""
from   datetime import datetime as dt
import os
import queue
import shutil
import sys
import threading
import traceback
import zipfile
from   typing   import Any, Dict, Optional

"""external library"""
## Not Using

"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()

except ModuleNotFoundError:
    try:
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
# Act Trigger Function
########################################

def a_handle_file_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """
    検出したファイルをmove/copy/delete/zip/createする
    Mod: 2026.10.06    By: IWATA, Y.'s Agent AI（to_dir に数値を指定すると、その列の値を出力先フォルダにする）
    """
    TRIGGER_NAME = "a_handle_file_trigger"

    """======== STARTUP ========"""
    try:
        n_col_def = len(col_def)
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}", log_file)

        """ ファイルパス列 """
        # stream_core が先頭4列（seg_id, seg_ind, seg_ts, 0）を付加するため、
        # find_trigger の col:0 は act_trigger では col:4 になる
        file_path_col = int(config.get("file_path_col", 4))
        if not (0 <= file_path_col < n_col_def):
            raise ValueError(
                f"file_path_col={file_path_col} out of range (0-{n_col_def-1})")

        """ pfx_sfx フィルタ """
        pfx_sfx_raw = config.get("pfx_sfx", [[], []])
        _pfx_list = pfx_sfx_raw[0] if len(pfx_sfx_raw) > 0 else []
        _sfx_list = pfx_sfx_raw[1] if len(pfx_sfx_raw) > 1 else []
        filter_prefixes = tuple(p.lower() for p in _pfx_list) if _pfx_list else ()
        filter_suffixes = tuple(s.lower() for s in _sfx_list) if _sfx_list else ()

        """ 安定チェック """
        stable_check_n = int(config.get("stable_check_n", 1))

        """ Ack設定 """
        check_ack    = bool(config.get("check_ack", False))
        ack_file_ext = str(config.get("ack_file_ext", "done")).strip().lstrip(".")
        delete_ack   = bool(config.get("delete_ack", False))

        if check_ack and not ack_file_ext:
            raise ValueError("ack_file_ext is required when check_ack=true")

        """ ファイル操作 """
        op = str(config.get("op", "move")).lower().strip()
        if op not in ("move", "copy", "delete", "zip", "create"):
            raise ValueError(
                f"op must be 'move' | 'copy' | 'delete' | 'zip' | 'create'. Got: {op!r}")

        to_dir, to_dir_col = utl.parse_dir_setting(config, "to_dir", n_col_def)  # 数値なら列の値を出力先フォルダにする
        to_name = str(config.get("to_name", "")).strip()
        overwrite = bool(config.get("overwrite", True))
        make_subdir_level = utl.parse_make_subdir_level(config)

        if op in ("move", "copy", "zip", "create") and to_dir_col is None:
            if not to_dir:
                raise ValueError(f"to_dir is required for op={op!r}")
            to_dir = base.get_absolute_path(to_dir)
            os.makedirs(to_dir, exist_ok=True)

        """ delete専用オプション（旧 delete_keep_folder はフォルダ処理の廃止に伴い無視） """
        if "delete_keep_folder" in config:
            base.output_log(MODULE_NAME,
                "WARNING  delete_keep_folder is ignored (folders are not handled by this act).", log_file)

        """ zip専用オプション """
        zip_delete_src = bool(config.get("zip_delete_src", False))

        """ create専用オプション """
        create_size_bytes = int(config.get("create_size_bytes", 0))
        create_fill_byte  = int(config.get("create_fill_byte", 0))
        create_overwrite  = bool(config.get("create_overwrite", False))

        if create_size_bytes < 0:
            raise ValueError(
                f"create_size_bytes must be >= 0. Got: {create_size_bytes}")
        if not (0 <= create_fill_byte <= 255):
            raise ValueError(
                f"create_fill_byte must be 0-255. Got: {create_fill_byte}")

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} op={op!r} to_dir={to_dir!r} to_dir_col={to_dir_col} to_name={to_name!r} overwrite={overwrite} make_subdir_level={make_subdir_level} "
            f"file_path_col={file_path_col} stable_check_n={stable_check_n} "
            f"check_ack={check_ack} ack_file_ext={ack_file_ext!r} delete_ack={delete_ack} "
            f"filter_prefixes={filter_prefixes} filter_suffixes={filter_suffixes} "
            f"zip_delete_src={zip_delete_src} "
            f"create_size_bytes={create_size_bytes} create_fill_byte={create_fill_byte} "
            f"create_overwrite={create_overwrite}",
            log_file)

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

    def _skip_exists(dest_path: str, flag_name: str) -> bool:
        """同名ファイルがあり上書きしない場合は、スキップしてログに残す（件数には数えない）"""
        base.output_log(MODULE_NAME,
            f"WARNING  Already exists, skip ({flag_name}=false): {dest_path}", log_file)
        return False

    def _match_pfx_sfx(src_path: str) -> bool:
        """ファイル名が pfx_sfx フィルタに合致するか判定する"""
        name = os.path.basename(src_path).lower()
        ok_pfx = name.startswith(filter_prefixes) if filter_prefixes else True
        ok_sfx = name.endswith(filter_suffixes)   if filter_suffixes else True
        return ok_pfx and ok_sfx

    def _check_src_path(src_path: str) -> Optional[str]:
        """
        処理対象のパスを確認する。問題があれば理由を返す（問題なければ None）
        - 相対パスは処理しない（カレントディレクトリ次第で対象が変わるため）
        - フォルダは処理しない（ファイル処理専用。フォルダ処理は別 act で検討）
        Mod: 2026.10.04    By: IWATA, Y.'s Agent AI（A-16）
        """
        if not os.path.isabs(src_path):
            return "Not an absolute path"
        if os.path.isdir(src_path):
            return "Not a file (folder)"
        return None

    def _base_dir(row: list) -> Optional[str]:
        """
        出力先フォルダ（to_dir）。to_dir が列指定の場合は、その行の列の値から決める（無ければ作成）
        値が空・"____" の場合は None（呼び出し側で異常として扱う）
        Mod: 2026.10.06    By: IWATA, Y.'s Agent AI
        """
        if to_dir_col is None:
            return to_dir
        folder = utl.get_dir_from_value(row[to_dir_col])
        if folder is None:
            return None
        dest = base.get_absolute_path(folder) if folder else system_root
        os.makedirs(dest, exist_ok=True)
        return dest

    def _dest_dir(src_path: str, base_dir: str) -> str:
        """
        move / copy / zip の出力先フォルダ（make_subdir_level 分、元のフォルダ構造を base_dir の下に再現。無ければ作成）
        Mod: 2026.10.06    By: IWATA, Y.'s Agent AI（出力先を引数で受け取る）
        """
        dirs = utl.get_make_subdir(src_path, make_subdir_level)
        if not dirs:
            return base_dir
        dest = os.path.join(base_dir, *dirs)
        os.makedirs(dest, exist_ok=True)
        return dest

    def _render_name(src_path: str) -> str:
        """to_name テンプレートを展開する。to_name が空なら元のファイル名を返す"""
        _, filename = os.path.split(src_path)
        if not to_name:
            return filename
        basename, ext = os.path.splitext(filename)
        now = dt.now()
        return to_name.format(
            filename = filename,
            basename = basename,
            ext      = ext,
            yyyymmdd = now.strftime("%Y%m%d"),
            hhmmss   = now.strftime("%H%M%S"),
            ts       = now.strftime("%Y%m%d_%H%M%S"),
        )

    def _render_zip_name(src_path: str) -> str:
        """
        zip操作の出力ファイル名を決定する。
        to_name 指定時はそれを展開して使用（拡張子含め自由指定）。
        未指定時は "{ファイル名（拡張子除く）}.zip" を既定値とする
        （_render_name をそのまま使うと拡張子が元のまま残ってしまうため専用処理）。
        """
        if to_name:
            return _render_name(src_path)
        stem, _ = os.path.splitext(os.path.basename(src_path))
        return f"{stem}.zip"

    _CREATE_CHUNK_SIZE = 1024 * 1024  # 1MB単位で書込み（大サイズファイルでもメモリ節約）

    def _create_file(dest_path: str, size_bytes: int, fill_byte: int) -> None:
        """
        指定サイズのファイルを新規作成する（size_bytes=0 で空ファイル）。
        アトミック書込み（一時ファイルへ書込み → rename）。
        """
        tmp_path = f"{dest_path}.tmp"
        try:
            with open(tmp_path, "wb") as f:
                remaining = size_bytes
                if remaining > 0:
                    buf = bytes([fill_byte]) * min(remaining, _CREATE_CHUNK_SIZE)
                    while remaining > 0:
                        n = min(len(buf), remaining)
                        f.write(buf[:n] if n < len(buf) else buf)
                        remaining -= n
            os.replace(tmp_path, dest_path)
            
        except Exception:
            try: os.remove(tmp_path)
            except OSError: pass
            raise

    def _exec_op(src_path: str, row: list) -> bool:
        """ファイル操作を実行する。成功時 True を返す"""

        base_dir = to_dir
        if op != "delete":
            base_dir = _base_dir(row)
            if base_dir is None:
                _report_ng(os.path.basename(src_path),
                    f"Destination folder value is empty (to_dir col={to_dir_col}), skip: {src_path}")
                return False

        if op == "create":
            # create は既存ファイルの検出を前提としない。file_path 列の値は
            # ファイル名プレースホルダ展開の元ネタとしてのみ使用する。
            dest_name = _render_name(src_path)
            dest_path = os.path.join(base_dir, dest_name)
            if os.path.exists(dest_path) and not create_overwrite:
                return _skip_exists(dest_path, "create_overwrite")
            _create_file(dest_path, create_size_bytes, create_fill_byte)
            _report_ok(dest_name,
                f"Created: {dest_path}  size={create_size_bytes}byte  "
                f"fill=0x{create_fill_byte:02X}")
            return True

        src_name = os.path.basename(src_path)
        if not os.path.exists(src_path):
            _report_ng(src_name, f"File not found, skip: {src_path}")
            return False
        if not os.path.isfile(src_path):# 呼出し前の確認後にフォルダへ置き換わった場合の保険
            _report_ng(src_name, f"Not a file (folder), skip: {src_path}")
            return False

        if op == "delete":
            os.remove(src_path)
            _report_ok(src_name, f"Deleted file: {src_path}")

        elif op == "zip":
            dest_name = _render_zip_name(src_path)
            zip_path  = os.path.join(_dest_dir(src_path, base_dir), dest_name)
            if os.path.exists(zip_path) and not overwrite:
                return _skip_exists(zip_path, "overwrite")
            tmp_zip   = f"{zip_path}.tmp"
            try:
                with zipfile.ZipFile(tmp_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(src_path, os.path.basename(src_path))
                os.replace(tmp_zip, zip_path)
            except Exception:
                try: os.remove(tmp_zip)
                except OSError: pass
                raise
            _report_ok(dest_name, f"Zipped: {src_path} → {zip_path}")

            if zip_delete_src:
                os.remove(src_path)
                base.output_log(MODULE_NAME,
                    f"Deleted source after zip: {src_path}")

        elif op == "copy":
            dest_name = _render_name(src_path)
            dest_path = os.path.join(_dest_dir(src_path, base_dir), dest_name)
            if os.path.exists(dest_path) and not overwrite:
                return _skip_exists(dest_path, "overwrite")
            tmp_path  = f"{dest_path}.tmp"
            try:
                shutil.copy2(src_path, tmp_path)
                os.replace(tmp_path, dest_path)
            except Exception:
                try: os.remove(tmp_path)
                except OSError: pass
                raise
            _report_ok(dest_name, f"Copied: {src_path} → {dest_path}")

        elif op == "move":
            dest_name = _render_name(src_path)
            dest_path = os.path.join(_dest_dir(src_path, base_dir), dest_name)
            if os.path.exists(dest_path) and not overwrite:
                return _skip_exists(dest_path, "overwrite")
            tmp_path  = f"{dest_path}.tmp"
            try:
                shutil.copy2(src_path, tmp_path)
                os.replace(tmp_path, dest_path)
                os.remove(src_path)
            except Exception:
                try: os.remove(tmp_path)
                except OSError: pass
                raise
            _report_ok(dest_name, f"Moved: {src_path} → {dest_path}")

        return True

    def _delete_ack(src_path: str) -> None:
        """Ackファイルを削除する（存在しない場合はスキップ）"""
        if not ack_file_ext:
            return
        ack_path = f"{src_path}.{ack_file_ext}"
        if os.path.exists(ack_path):
            try:
                os.remove(ack_path)
                base.output_log(MODULE_NAME,
                    f"Ack deleted: {ack_path}")
            except Exception as e:
                _report_ng(os.path.basename(ack_path), f"WARNING  Ack delete failed: {ack_path}  ({e})")

    """======== LOOP START ========"""
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

            if not segment:
                base.output_log(MODULE_NAME,
                    f"WARNING  空セグメント、スキップ。 seg_id={seg_id}", log_file)
                continue

            handled = 0
            skipped = 0
            for row_num, row in enumerate(segment, start=1):
                try:
                    file_path = row[file_path_col]
                    if not file_path:
                        skipped += 1
                        continue

                    """pfx_sfxフィルタ（対象外は異常ではないため画面表示のみ）"""
                    if not _match_pfx_sfx(str(file_path)):
                        base.output_log(MODULE_NAME,
                            f"pfx_sfx not matched, skip: {file_path}")
                        skipped += 1
                        continue

                    """パスチェック（find から渡されたフルパスのファイルのみ処理。op="create" は名前の材料にするだけのため対象外）"""
                    if op != "create":
                        _path_ng = _check_src_path(str(file_path))
                        if _path_ng:
                            _report_ng(os.path.basename(str(file_path)), f"{_path_ng}, skip: {file_path}")
                            skipped += 1
                            continue

                    """安定チェック（op="create" は対象実ファイルが存在しないため常にスキップ）"""
                    if op != "create" and stable_check_n > 0:
                        if not utl.check_file_stability_retry(stable_check_n, str(file_path)):
                            _report_ng(os.path.basename(str(file_path)),
                                f"File not stable, skip: {file_path}")
                            skipped += 1
                            continue

                    """Ackチェック（op="create" は対象実ファイルが存在しないため常にスキップ）"""
                    if op != "create" and check_ack:
                        if not utl.check_ack_file_exists(ack_file_ext, str(file_path)):
                            _report_ng(os.path.basename(str(file_path)),
                                f"Ack not found, skip: {file_path}")
                            skipped += 1
                            continue

                    """操作実行"""
                    ok = _exec_op(str(file_path), row)

                    """Ack削除（op="create" は対象実ファイルが存在しないため対象外）"""
                    if ok and delete_ack and op != "create":
                        _delete_ack(str(file_path))

                    if ok:
                        handled += 1
                    else:
                        skipped += 1

                except Exception as e:
                    _report_ng(os.path.basename(str(row[file_path_col])),
                        f"Row error (row={row_num}): {e}\n{traceback.format_exc()}")
                    skipped += 1

            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Done. seg_id={seg_id}  op={op!r}  "
                f"handled={handled}  skipped={skipped}")

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
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None

a_handle_file_trigger.QMODE = "imq"
a_handle_file_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す
a_handle_file_trigger.ACCEPTS = {"qvalue": ("file_path",)}


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
