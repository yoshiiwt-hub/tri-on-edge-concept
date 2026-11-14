# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [FTP Server + Text]
# Ver: 01.b.20261114    By: IWATA,Y.'s Agent AI
# Mod: 停止を速くし、サーバーのスレッドが残らないようにした（サーバーは専用のスレッドで回して、同じスレッドで閉じる）
########################################
装置が FTP で送ってくる CSV・TSV 等の区切りテキストを、エッジ側の FTP サーバーで受け取り、行ごとのデータとして取り込む入口

主要関数:
f_find_ftp_csv_trigger  # FTP サーバーで区切りテキストを受信し、読んで1行ずつキューに流す（1ファイル = 1セグメント）。

位置づけ: 受け取り方は tri_s11_find_ftp、読み方（列の割当て・追加列・型変換・読み取った後の処理）は tri_s11_find_text_csv と同じ。列の割当ての関数は tri_s11_find_text_csv のものを使う。
設計意図: 受信完了の通知で処理するため、フォルダを見回るより早く確実に検出できる。設定の名前は2つのトリガーと同じにし、設定例を組み合わせて書けるようにする。
前提・制約: 接続情報は環境変数（FTP_HOST / FTP_PORT / FTP_USER / FTP_PASS）で渡す。受信のたびに1回だけ読む（読込みに失敗したファイルは読み直さない。送り直してもらう）。
"""
MODULE_NAME = "FIND_FTP_Csv"

"""standard library"""
from   datetime import datetime  as dt
import os
import queue
import sys
import threading
import traceback
from   typing   import Any, Dict, List, Optional, Tuple

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
from   src.tri_s11_find_text_csv import _check_index_keys, _build_col_assign, _remap_row   # 列の割当て（text_csv と同じ）

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

def f_find_ftp_csv_trigger(
    queue_out:  queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """
    FTP サーバーで区切りテキストを受信し、読んで1行ずつキューに流す（1ファイル = 1セグメント）
    Mod: 2026.11.14    By: IWATA,Y.'s Agent AI

    [動作の要点]
        受け取り: tri_s11_find_ftp と同じ。受信完了・受信後のリネームで動き、pfx_sfx（既定 [[], [".csv"]]）→ 安定チェック → ACK を確かめる
                  見回り・読んだファイルの記録（seen_files）は無い。同じ名前で送り直されたら、もう一度読む
        読込み  : tri_s11_find_text_csv と同じ。delimiter / data_start_row / row_range / column_source_map / extra_col_def /
                  encodings / 型変換の既定値で読み、全行を流してから区切り行（"____"）を流す
        後片付け: after_read（keep / delete / move / rename。既定 keep）。delete / move / rename では ACK ファイルも削除する
        読込みの失敗: ErrorIF へ報告し（phase=loop, category=io）、そのファイルは飛ばす（後片付けもしない。読み直さない）
        ログ    : 1ファイルごとの正常なログは画面のみ。読込みの失敗・後片付けの失敗はログファイルにも残す
        出力列  : col_def の列順に、データ列 + extra_col_def の列が入る（text_csv と同じ）
        運用系メトリクス（FIND_FTP_Csv）:
            total_received_files / total_renamed_files / total_incomplete_transfers / total_read_files / total_read_errors
    """
    TRIGGER_NAME = "f_find_ftp_csv_trigger"

    """======== STARTUP ========"""
    try:
        n_col_def = len(col_def)
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}", log_file)

        """ FTPサーバ設定（tri_s11_find_ftp と同じ） """
        ftp_host = str(os.getenv('FTP_HOST', None))
        ftp_port = int(os.getenv('FTP_PORT', None))
        ftp_user = str(os.getenv('FTP_USER', None))
        ftp_pass = str(os.getenv('FTP_PASS', None))
        ftp_perm = str(config.get("ftp_perm", "elrafmw"))
        if not all([ftp_host, ftp_port, ftp_user, ftp_pass]):
            raise ValueError("Config Error: One or more FTP configuration values are missing or empty.")
        base.output_log(MODULE_NAME, f"FTP Config: {ftp_host}:{ftp_port} user={ftp_user} perm={ftp_perm}", log_file)

        home_dir = base.get_absolute_path(str(config.get("home_dir", "work")))
        os.makedirs(home_dir, exist_ok=True)
        passive_ports   = config.get('passive_ports', [60000, 60100])
        server_timeout  = int(config.get('server_timeout', 60))
        max_cons_per_ip = int(config.get("max_cons_per_ip", 5))
        base.output_log(MODULE_NAME, f"ftp_home_dir={home_dir} passive_ports={passive_ports} server_timeout={server_timeout} max_cons_per_ip={max_cons_per_ip}", log_file)

        """ pfx_sfx フィルタ（既定は .csv で終わるファイル） """
        pfx_sfx_raw = config.get("pfx_sfx", [[], [".csv"]])
        _pfx_list = pfx_sfx_raw[0] if len(pfx_sfx_raw) > 0 else []
        _sfx_list = pfx_sfx_raw[1] if len(pfx_sfx_raw) > 1 else []
        filter_prefixes = tuple(p.lower() for p in _pfx_list) if _pfx_list else ()
        filter_suffixes = tuple(s.lower() for s in _sfx_list) if _sfx_list else ()

        """ 安定チェック・Ack 設定 """
        stable_check_n = int(config.get("stable_check_n", 1))
        check_ack    = bool(config.get("check_ack", False))
        ack_file_ext = str(config.get("ack_file_ext", "done")).strip().lstrip(".")
        if check_ack and not ack_file_ext:
            raise ValueError("ack_file_ext is required when check_ack=true")
        base.output_log(MODULE_NAME,
            f"filter_prefixes={filter_prefixes} filter_suffixes={filter_suffixes} stable_check_n={stable_check_n} "
            f"check_ack={check_ack} ack_file_ext={ack_file_ext}", log_file)

        """ 読み取った後の処理（tri_s11_find_text_csv と同じ。既定 keep） """
        after_read = config.get("after_read", "keep")
        if after_read not in ("keep", "delete", "move", "rename"):
            raise ValueError(f"config['after_read'] Invalid. after_read={after_read!r}")
        move_to = ""
        if after_read == "move":
            if not config.get("move_to", ""):
                raise ValueError("config['move_to'] is NOT Set.")
            move_to = base.get_absolute_path(config["move_to"])
            if os.path.normcase(os.path.abspath(move_to)) == os.path.normcase(os.path.abspath(home_dir)):
                raise ValueError(f"config['move_to'] must be different from home_dir. move_to={move_to} home_dir={home_dir}")
            os.makedirs(move_to, exist_ok=True)
        _pfx_sfx   = list(config.get("rename_pfx_sfx", None) or []) + ["", ""]
        rename_pfx = _pfx_sfx[0] or ""
        rename_sfx = _pfx_sfx[1] or ""
        do_rename  = after_read == "rename" and bool(rename_pfx or rename_sfx)
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} after_read={after_read} move_to={move_to} rename_pfx_sfx={[rename_pfx, rename_sfx]}", log_file)

        """ パース設定（tri_s11_find_text_csv と同じ） """
        delimiter      = config.get("delimiter", ",")
        data_start_row = int(config.get("data_start_row", 0))

        # extra_col_def は辞書形式（キー = 値を入れる col_def の列番号）。キーの昇順に並べて parse_delimited_text に渡す
        extra_col_def_raw = config.get("extra_col_def", None)
        if extra_col_def_raw is not None:
            _check_index_keys(extra_col_def_raw, "extra_col_def")
            _extra_items = sorted(extra_col_def_raw.items(), key=lambda kv: int(kv[0]))
            extra_col_target_idx = [int(k) for k, _v in _extra_items]
            extra_col_def        = [_v      for _k, _v in _extra_items]
        else:
            extra_col_target_idx = []
            extra_col_def = None
        utl.validate_extra_col_def(extra_col_def)

        extra_col_default = config.get("extra_col_default", "__")
        encodings         = tuple(config.get("encodings", ("utf-8-sig", "cp932", "euc-jp")))

        """ 読取り対象行の範囲（省略可。データ行基準・1始点・両端含む） """
        row_range_raw = config.get("row_range", None)
        if row_range_raw is not None:
            if not isinstance(row_range_raw, (list, tuple)) or len(row_range_raw) not in (1, 2):
                raise ValueError(
                    f"row_range must be [start] or [start, end] (1始点・両端含む). Got: {row_range_raw!r}")
            row_range_start = int(row_range_raw[0])
            if row_range_start < 1:
                raise ValueError(f"row_range の start は1以上である必要があります: {row_range_start}")
            if len(row_range_raw) == 2 and row_range_raw[1] is not None:
                row_range_end = int(row_range_raw[1])
                if row_range_end < row_range_start:
                    raise ValueError(
                        f"row_range の end({row_range_end}) は start({row_range_start}) 以上である必要があります")
            else:
                row_range_end = None  # 指定なし = 最終行まで
        else:
            row_range_start, row_range_end = None, None  # 省略時は全行読取り

        """ 列の割当て（指定の無い列は 列番号＝CSV区切り位置。重複は設定ファイルで後に書いた方を優先） """
        column_source_map = config.get("column_source_map", None) or {}
        _check_index_keys(column_source_map, "column_source_map")
        n_extra = len(extra_col_def) if extra_col_def else 0
        for col_key, csv_idx in column_source_map.items():
            if not (0 <= int(col_key) < n_col_def):
                raise ValueError(
                    f"column_source_map のキー {col_key!r} が col_def の範囲外です (有効範囲: 0-{n_col_def - 1})")
            if int(csv_idx) < 0:
                raise ValueError(f"column_source_map[{col_key!r}]={csv_idx} は0以上である必要があります")
        for target_idx in extra_col_target_idx:
            if not (0 <= target_idx < n_col_def):
                raise ValueError(
                    f"extra_col_def のキー {target_idx} が col_def の範囲外です (有効範囲: 0-{n_col_def - 1})")
        _cfg_keys = list(config.keys())
        extra_after_map = (
            "extra_col_def" in _cfg_keys and "column_source_map" in _cfg_keys
            and _cfg_keys.index("extra_col_def") > _cfg_keys.index("column_source_map"))
        col_assign = _build_col_assign(n_col_def, column_source_map, extra_col_target_idx, extra_after_map)
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} delimiter={delimiter!r} data_start_row={data_start_row} "
            f"row_range=[{row_range_start}, {row_range_end}] encodings={encodings} "
            f"extra_col_def={extra_col_def} extra_col_default={extra_col_default!r} col_assign={col_assign}", log_file)

        """ 型変換 """
        default_val_config = {
            "time_format"  : config.get("time_format", "%Y-%m-%d %H:%M:%S.%f"),
            "time_default" : config.get("time_default", "2999-01-01 00:00:00.000000"),
            "int_default"  : config.get("int_default", -9999),
            "float_default": config.get("float_default", -9999.9),
            "str_default"  : config.get("str_default", "__"),
        }
        cast_plan = base.set_cast_dtype(col_def, default_val_config)
        base.output_log(MODULE_NAME, f"cast_plan={cast_plan}", log_file)

        for _k in ("total_received_files", "total_renamed_files", "total_incomplete_transfers",
                   "total_read_files", "total_read_errors"):
            base.metrics_add(MODULE_NAME, _k, 0)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config",
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== FTPサーバ（tri_s11_find_ftp と同じ） ========"""
    _recv_queue: queue.Queue = queue.Queue()

    class _TriEdgeFTPHandler(FTPHandler):
        def on_connect(self):
            base.metrics_set_last(MODULE_NAME, "last_connect_time", dt.now().strftime("%Y/%m/%d %H:%M:%S"))
            base.output_log(MODULE_NAME, f"FTP Client connected: {self.remote_ip}:{self.remote_port}", log_file)

        def on_disconnect(self):
            base.metrics_set_last(MODULE_NAME, "last_disconnect_time", dt.now().strftime("%Y/%m/%d %H:%M:%S"))
            base.output_log(MODULE_NAME, f"FTP Client disconnected: {self.remote_ip}:{self.remote_port}", log_file)

        def on_file_received(self, file_path: str) -> None:
            base.metrics_add(MODULE_NAME, "total_received_files", 1)
            base.output_log(MODULE_NAME, f"File received: {file_path}")     # 正常は画面のみ
            _recv_queue.put((file_path, None))

        def ftp_RNTO(self, path: str):
            res = super().ftp_RNTO(path)     # 成功時のみ (src, dst) を返す（失敗時は 550 応答で None）
            if res:
                src_path, dst_path = res
                if os.path.isfile(dst_path):     # ディレクトリのリネームは対象外
                    base.metrics_add(MODULE_NAME, "total_renamed_files", 1)
                    base.output_log(MODULE_NAME, f"File renamed: {src_path} -> {dst_path}")     # 正常は画面のみ
                    _recv_queue.put((dst_path, src_path))
            return res

        def on_incomplete_file_received(self, file_path: str) -> None:
            base.metrics_add(MODULE_NAME, "total_incomplete_transfers", 1)
            base.output_log(MODULE_NAME, f"WARNING  Incomplete transfer, removing: {file_path}", log_file)
            try:
                os.remove(file_path)
            except Exception:
                pass

    server: Optional[FTPServer] = None
    server_thread: Optional[threading.Thread] = None
    srv_stop = threading.Event()                        # FTP サーバーを止める合図（CLEANUP で立てる）

    try:
        authorizer = DummyAuthorizer()
        authorizer.add_user(ftp_user, ftp_pass, homedir=home_dir, perm=ftp_perm)
        _TriEdgeFTPHandler.authorizer = authorizer
        _TriEdgeFTPHandler.passive_ports = range(passive_ports[0], passive_ports[1])
        _TriEdgeFTPHandler.timeout = server_timeout
        _TriEdgeFTPHandler.max_cons_per_ip = max_cons_per_ip
        server = FTPServer((ftp_host, ftp_port), _TriEdgeFTPHandler)

        def _serve() -> None:
            """FTP サーバーを回し、srv_stop が立ったら同じスレッドで閉じる
            （別のスレッドから close_all() を呼んでも serve_forever() は抜けないため）"""
            try:
                while not srv_stop.is_set():
                    server.serve_forever(timeout=0.5, blocking=False, handle_exit=False)
            finally:
                server.close_all()

        server_thread = threading.Thread(target=_serve, daemon=True, name=f"{TRIGGER_NAME}_server")
        server_thread.start()
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} FTP Server started. {ftp_host}:{ftp_port}  user={ftp_user}  home={home_dir}", log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="comm",
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== HELPER (loop) ========"""
    def _match_pfx_sfx(path: str) -> Optional[str]:
        """pfx_sfx フィルタ。該当すれば None、該当しなければ "Prefix" / "Suffix" を返す"""
        filename_lower = os.path.basename(path).lower()
        if filter_prefixes and not filename_lower.startswith(filter_prefixes):
            return "Prefix"
        if filter_suffixes and not filename_lower.endswith(filter_suffixes):
            return "Suffix"
        return None

    def _read_rows(f_path: str) -> Tuple[List[List[Any]], str]:
        """区切りテキストを読み、row_range で絞り込み、col_def の並びに組み替えて型変換した行のリストを返す"""
        records, used_encoding = utl.parse_delimited_text(
            f_path,
            delimiter=delimiter,
            data_start_row=data_start_row,
            extra_col_def=extra_col_def,
            extra_col_default=extra_col_default,
            encodings=encodings,
        )
        rows: List[List[Any]] = []
        for row_i, row in enumerate(records, start=1):
            if row_range_start is not None and row_i < row_range_start:
                continue
            if row_range_end is not None and row_i > row_range_end:
                break  # records は行番号順のため、これ以降は全て範囲外
            value, cast_fails = base.cast_dtype(_remap_row(row, col_assign, n_extra), cast_plan)
            if cast_fails > 0:
                ErrorIF.report_event({
                    "signature": "CAST_FAIL",
                    "phase": "loop",
                    "category": "data",
                    "trigger_id": TRIGGER_NAME,
                    "message": f"Cast failed {cast_fails} cols. file={f_path}",
                    "log_file": log_file,
                })
            rows.append(value)
        return rows, used_encoding

    def _after_read(f_path: str) -> bool:
        """
        読み取った後の処理（delete / move / rename）。delete / move / rename の場合は ACK ファイルも削除する
        失敗した場合はログ・ErrorIF に記録して継続する
        Returns: ErrorIF から停止指示があれば True
        """
        if after_read == "keep" or (after_read == "rename" and not do_rename):
            return False
        ack_path = f"{f_path}.{ack_file_ext}" if (check_ack and ack_file_ext) else None
        try:
            name = os.path.basename(f_path)
            if after_read == "delete":
                os.remove(f_path)
            elif after_read == "move":
                os.replace(f_path, os.path.join(move_to, name))
            else:
                os.replace(f_path, os.path.join(os.path.dirname(f_path), f"{rename_pfx}{name}{rename_sfx}"))
            if ack_path and os.path.exists(ack_path):
                os.remove(ack_path)
            base.output_log(MODULE_NAME, f"After Read ({after_read}): {f_path}")     # 正常は画面のみ
        except Exception as e:
            base.output_log(MODULE_NAME, f"After Read Error ({after_read}): {f_path}  {e}", log_file)
            _sig = ErrorIF.report_exception(e, phase="loop", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)
            return _sig == SIGNAL_STOP
        return False

    """======== MAIN LOOP ========"""
    while not stop_event.is_set():
        try:
            file_path, renamed_from = _recv_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        try:
            file_path = str(file_path)

            """リネーム: リネーム前から pfx_sfx に該当していれば受信時に処理済み"""
            if renamed_from is not None and _match_pfx_sfx(str(renamed_from)) is None:
                base.output_log(MODULE_NAME,
                    f"Renamed from matched name (already handled), skip: {file_path}")    # 正常は画面のみ
                continue

            """pfx_sfxフィルタ"""
            unmatched = _match_pfx_sfx(file_path)
            if unmatched:
                base.output_log(MODULE_NAME, f"{unmatched} not matched, skip: {file_path}")     # 正常は画面のみ（.temp 等）
                continue

            """安定チェック"""
            if stable_check_n > 0:
                if not utl.check_file_stability_retry(stable_check_n, file_path):
                    base.output_log(MODULE_NAME, f"File not stable, skip: {file_path}", log_file)
                    continue

            """Ackチェック"""
            if check_ack:
                if not utl.check_ack_file_exists(ack_file_ext, file_path):
                    base.output_log(MODULE_NAME, f"Ack not found, skip: {file_path}", log_file)
                    continue

            """テキストパース（失敗したファイルは飛ばす。後片付けもしない）"""
            try:
                rows, used_encoding = _read_rows(file_path)
            except (FileNotFoundError, ValueError) as e:
                base.metrics_add(MODULE_NAME, "total_read_errors", 1)
                base.output_log(MODULE_NAME, f"Read Error, skip: {file_path}  {e}", log_file)
                _sig = ErrorIF.report_exception(e, phase="loop", category="io",
                                                trigger_id=TRIGGER_NAME, log_file=log_file)
                if _sig == SIGNAL_STOP:
                    stop_event.set()
                    break
                continue

            """キュー送信（1ファイル = 1セグメント。最後に区切り行）"""
            count = 0
            for value in rows:
                queue_out.put((count, value))
                count += 1
            queue_out.put((count, ['____'] * n_col_def))
            base.metrics_add(MODULE_NAME, "total_read_files", 1)
            base.output_log(MODULE_NAME,
                f"Queued: {file_path}  encoding={used_encoding}  rows={len(rows)}")     # 正常は画面のみ（件数は metrics）

            """読み取った後の処理"""
            if _after_read(file_path):
                stop_event.set()
                break

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="io",
                                            trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue

    """======== CLEANUP ========"""
    if server_thread is not None:
        srv_stop.set()
        server_thread.join(timeout=5.0)
        if server_thread.is_alive():
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} FTP Server did not stop in 5 sec.", log_file)
        else:
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} FTP Server stopped.", log_file)

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
