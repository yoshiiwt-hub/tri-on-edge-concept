# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [FILE HANDLE]
# Ver: 08.h.20261109    By: IWATA, Y.'s Agent AI
# Mod: Docstring から、リポジトリに無いファイルへの参照を削除
########################################
Find が検出したファイルを、現場のファイル運用ルールに沿って後始末する Act

主要関数:
a_handle_file_trigger  # 検出したファイルを move / copy / delete / zip / create する。

位置づけ: 収集済みファイルの退避・整理・圧縮、送信完了の印となるファイルの作成に使う。
設計意図: 書き込み途中のファイルや、相手側の書き込み完了前のファイルを処理しないことを最優先とする（書き込みの安定確認・ack ファイルの確認・.tmp に書いてからリネーム）。
前提・制約: 絶対パスのファイルのみを扱う。フォルダは対象外。
"""
MODULE_NAME = "ACT_Handle_File"

"""standard library"""
from   datetime import datetime as dt
import os
import queue
import shutil
import string
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

    [動作の要点]（旧モジュール Docstring から移動）
        相対パス・フォルダのパスは処理せず、異常としてログに残す（op="create" は対象外）
        pfx_sfx で対象外になったファイルは異常として扱わない（画面表示のみ）
        同名ファイルがあり上書きしない場合（overwrite=false / create_overwrite=false）はスキップしてログに残す（件数には数えない）
        move / copy / zip は <名前>.tmp に書き込んでからリネームする
        to_dir に数値を指定した場合、値が空・"____" の行は処理せず異常として記録する（ファイルは元の場所に残る）
        旧 delete_keep_folder はフォルダ処理の廃止に伴い無視する（設定されていれば起動時に WARN）
        フォルダの処理は別の Act として検討中
        運用系メトリクス: last_ok_file / total_ok_files / last_ng_file / total_ng_files（正常は画面表示のみ、異常はログファイルにも残す）
    """
    TRIGGER_NAME = "a_handle_file_trigger"

    """======== STARTUP ========"""
    try:
        n_col_def = len(col_def)
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}", log_file)

        """ ファイルパス列（Find と同じ列番号で書く。行の中の位置に変換する） """
        file_path_col = base.resolve_act_col(config.get("file_path_col", 0), col_def, "file_path_col")

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
        to_name_cols = _parse_to_name_cols(to_name, col_def)  # {colN} → 行の中の位置
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
            f"{TRIGGER_NAME} op={op!r} to_dir={to_dir!r} to_dir_col={to_dir_col} to_name={to_name!r} to_name_cols={to_name_cols} overwrite={overwrite} make_subdir_level={make_subdir_level} "
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

    def _col_values(row: list) -> Dict[str, str]:
        """
        to_name の {colN} に差し込む値（その行の列の値を文字列にしたもの）
        値が空・None、またはファイル名に使えない文字を含む場合は ValueError（呼び出し側で異常として扱う）
        Mod: 2026.10.25    By: IWATA, Y.'s Agent AI
        """
        values = {}
        for field, pos in to_name_cols.items():
            val = row[pos]
            text = "" if val is None else str(val)
            if not text:
                raise ValueError(f"to_name {{{field}}} value is empty")
            bad = [c for c in _INVALID_NAME_CHARS if c in text]
            if bad:
                raise ValueError(f"to_name {{{field}}} value {text!r} has invalid characters {bad}")
            values[field] = text
        return values

    def _render_name(src_path: str, col_vals: Dict[str, str]) -> str:
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
            **col_vals,
        )

    def _render_zip_name(src_path: str, col_vals: Dict[str, str]) -> str:
        """
        zip操作の出力ファイル名を決定する。
        to_name 指定時はそれを展開して使用（拡張子含め自由指定）。
        未指定時は "{ファイル名（拡張子除く）}.zip" を既定値とする
        （_render_name をそのまま使うと拡張子が元のまま残ってしまうため専用処理）。
        """
        if to_name:
            return _render_name(src_path, col_vals)
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
        col_vals: Dict[str, str] = {}
        if op != "delete":
            base_dir = _base_dir(row)
            if base_dir is None:
                _report_ng(os.path.basename(src_path),
                    f"Destination folder value is empty (to_dir col={to_dir_col}), skip: {src_path}")
                return False

            try:
                col_vals = _col_values(row)
            except ValueError as e:
                _report_ng(os.path.basename(src_path), f"{e}, skip: {src_path}")
                return False

        if op == "create":
            # create は既存ファイルの検出を前提としない。file_path 列の値は
            # ファイル名プレースホルダ展開の元ネタとしてのみ使用する。
            dest_name = _render_name(src_path, col_vals)
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
            dest_name = _render_zip_name(src_path, col_vals)
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
            dest_name = _render_name(src_path, col_vals)
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
            dest_name = _render_name(src_path, col_vals)
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

_INVALID_NAME_CHARS = '/\\:*?"<>|'  # ファイル名に使えない文字（{colN} の値の確認用）


def _parse_to_name_cols(to_name: str, col_def: Dict[int, Any]) -> Dict[str, int]:
    """
    to_name から {colN} を取り出し、{"colN": 行の中の位置} を返す（起動時に呼ぶ。列番号が範囲外なら ValueError）
    Mod: 2026.10.25    By: IWATA, Y.'s Agent AI
    """
    cols: Dict[str, int] = {}
    for _text, field, _spec, _conv in string.Formatter().parse(to_name):
        if field and field.startswith("col") and field[3:].isdigit():
            cols[field] = base.resolve_act_col(int(field[3:]), col_def, f"to_name {{{field}}}")
    return cols


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
