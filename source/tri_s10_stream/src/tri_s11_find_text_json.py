# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [JSON]
# Ver: 01.f.20261002    By: IWATA, Y.'s Agent AI
# Mod: 読み取った後の処理（after_read: keep / delete / move / rename）を追加
########################################
f_read_json_trigger   # フォルダを監視し、JSON ファイルを読み込んで内容をキューに流す

動作概要
    1. monitor_dir をポーリングし、未処理ファイルを発見したら utl.load_json でパース
       - SQLite の seen_files DB で重複処理を防止
       - pfx_sfx フィルタ・stable_check・ACKファイルチェック（任意）に対応
    2. mapping_config に従って、JSON内容から共通データ(common)とループリスト(to_record)を抽出する
       - common: 対象キーをリストで指定（そのまま値取得）または辞書で指定（datetime_convert等の変換付き）
       - to_record: リストパス配下の各要素から item_key_map の値を取り出し、common + 要素値でレコード化
       - handle_json=true の場合、JSONファイル自体（ファイル名）も1レコードとして送信する
    3. 各レコードは col_def の dtype に従って型変換してからキューへ送信し、
       ファイル末尾でセグメント区切り（'____'）を送信する（1ファイル = 1セグメント）
    4. ファイルI/Oエラー（OSError/PermissionError）はオフラインモードへ移行し、
       同種エラーが頻発（デフォルト: 60秒に5回）していれば offline_interval_sec だけ読み取りを
       スキップする（tri_edge_11_find_read_csv.py と同様の設計）。それ以外の予期しない例外は
       オフライン化せず、そのファイルをスキップして継続する
    5. 読み取った後、after_read に従ってファイルを削除・移動・リネームする（既定 keep = 残す）
       ack_file_ext 指定時は、delete / move / rename の場合に ACK ファイルも削除する
    6. stop_event が立つまでループを継続

出力列
    col_def の列順に、common_data（dir_path, file_name, common_conf の値）+ item_key_map の値
    （または handle_json 時は file_name）が入る
    （queue_out.put で送る values の長さ・順序は col_def と完全に一致させること）

接続設定
    monitor_dir      : 監視対象ディレクトリ（デフォルト: "."）
    search_subdir    : サブディレクトリを再帰的に検索するか（デフォルト: false）
    pfx_sfx          : ファイル名フィルタ [[接頭語], [接尾語]]（デフォルト: [[], [".json"]]）
    encoding         : JSONファイルの文字エンコーディング（デフォルト: "utf-8-sig"）
    stable_check_n   : 安定チェック回数（デフォルト: 1）
    interval_sec     : ポーリング間隔秒（デフォルト: 2）
    ack_file_ext     : ACKファイル拡張子（空文字列の場合はチェックしない、デフォルト: ""）
    offline_interval_sec: 読込みエラー頻発検知後、次に読み取りを再開するまでの待機秒数（デフォルト: 30）
    mapping_config   : JSON内容の抽出定義。
                          "common"   : {出力キー: [パス...] または {"path": [...], "action": "datetime_convert", "src_fmt":..., "dst_fmt":...}}
                          "to_record": {"list_path": [...], "item_key_map": {出力キー: JSON内キー}}
    handle_json      : JSONファイル自体（ファイル名）も1レコードとして送信するか（デフォルト: false）
    time_format / time_default / int_default / float_default / str_default
                     : dtype cast のフォーマット・デフォルト値（base.set_cast_dtype と同じ）
    after_read       : 読み取った後の処理（デフォルト: "keep"）
                          "keep"  : 残す（従来どおり。seen_files は再起動でクリアされるため、再起動後に読み直す）
                          "delete": 削除する
                          "move"  : move_to へ移動する（監視フォルダと同じ・search_subdir=true で監視フォルダ配下は起動時エラー）
                          "rename": rename_pfx_sfx = [接頭語, 接尾語] で名前を変える（接頭語は元の名前の前、接尾語は拡張子の後。
                                    未指定・空欄の側は付けない）。rename 済みの名前（接頭語で始まり接尾語で終わる）は監視対象から外す
                        レコードとセグメント区切りを送り、読み取り済みとして記録した後に行う。読み取りに失敗したファイルには行わない
                        移動先・リネーム先に同名ファイルがある場合は上書きする
    move_to          : 移動先フォルダ（after_read="move" の場合に必須。system_root からの相対パス可）
    rename_pfx_sfx   : [接頭語, 接尾語]（after_read="rename" の場合）
"""
MODULE_NAME = "FIND_Json"

"""standard library"""
from   datetime import datetime as dt
import os
import queue
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Tuple, Sequence, Optional

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
import src.tri_s19_util        as utl

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

def f_read_json_trigger(
    queue_out: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
    ) -> None:
    """
    指定フォルダを監視し、JSONを読込んで展開
    Mod: 2026.10.02    By: IWATA, Y.'s Agent AI（after_read を追加）
    """
    TRIGGER_NAME = "f_read_json_trigger"

    def _get_val_by_path(data: Dict, path: List[str]) -> Any:
        """内部ヘルパー関数：パス指定による値取得"""
        try:
            curr = data
            for key in path:
                curr = curr.get(key, {})
            return curr if not isinstance(curr, dict) or path else None  # 辞書が返ってきた場合、値取得(暫定仕様)
        except Exception:
            return None

    try:
        """ SETTING（監視） """
        monitor_dir   = os.path.join(system_root, config.get("monitor_dir", "."))
        os.makedirs(monitor_dir, exist_ok=True)
        search_subdir = config.get("search_subdir", False)
        interval_sec  = config.get("interval_sec", 2)

        pfx_sfx        = config.get("pfx_sfx", ([], [".json"]))
        encoding       = config.get("encoding", "utf-8-sig")
        stable_check_n = int(config.get("stable_check_n", "1"))
        ack_ext        = config.get("ack_file_ext", "").replace(".", "")
        check_ack      = bool(ack_ext)
        offline_interval_sec = config.get("offline_interval_sec", 30)

        """ SETTING（読み取った後の処理） """
        after_read = config.get("after_read", "keep")
        if after_read not in ("keep", "delete", "move", "rename"):
            raise ValueError(f"config['after_read'] Invalid. after_read={after_read!r}")

        move_to = ""
        if after_read == "move":
            if not config.get("move_to", ""):
                raise ValueError("config['move_to'] is NOT Set.")
            move_to = base.get_absolute_path(config["move_to"])
            # 移動先が監視対象に含まれると読み直してしまうため、起動時に止める
            _mon = os.path.normcase(os.path.abspath(monitor_dir))
            _mv  = os.path.normcase(os.path.abspath(move_to))
            if _mv == _mon or (search_subdir and os.path.commonpath([_mon, _mv]) == _mon):
                raise ValueError(f"config['move_to'] must be outside monitor_dir. move_to={move_to} monitor_dir={monitor_dir} search_subdir={search_subdir}")
            os.makedirs(move_to, exist_ok=True)

        _pfx_sfx   = list(config.get("rename_pfx_sfx", None) or []) + ["", ""]
        rename_pfx = _pfx_sfx[0] or ""
        rename_sfx = _pfx_sfx[1] or ""
        do_rename  = after_read == "rename" and bool(rename_pfx or rename_sfx)
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} after_read={after_read} move_to={move_to} rename_pfx_sfx={[rename_pfx, rename_sfx]}", log_file)

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} monitor_dir={monitor_dir} search_subdir={search_subdir} "
            f"interval_sec={interval_sec} pfx_sfx={pfx_sfx} encoding={encoding} "
            f"stable_check_n={stable_check_n} check_ack={check_ack} ack_ext={ack_ext} "
            f"offline_interval_sec={offline_interval_sec}", log_file)

        """ SETTING（マッピング） """
        mapping_conf = config.get("mapping_config", {})
        handle_json  = config.get("handle_json", False)
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} mapping_conf={mapping_conf} handle_json={handle_json}", log_file)

        """ SETTING（dtype cast） """
        n_col_def = len(col_def)
        default_val_config = {
            "time_format"  : config.get("time_format", "%Y-%m-%d %H:%M:%S.%f"),
            "time_default" : config.get("time_default", "2999-01-01 00:00:00.000000"),
            "int_default"  : config.get("int_default", -9999),
            "float_default": config.get("float_default", -9999.9),
            "str_default"  : config.get("str_default", "__"),
        }
        cast_plan = base.set_cast_dtype(col_def, default_val_config)
        base.output_log(MODULE_NAME, f"cast_plan={cast_plan}", log_file)

        """ 発見済みファイル名格納用DB """
        db_path    = os.path.join(work_db_dir, f"{TRIGGER_NAME}_seen_files.db")
        table_name = "seen_files"
        table_col  = {
            0: ["path" , "TEXT", "PRIMARY KEY"],
            1: ["mdate", "REAL", ""]
        }
        conn = utl.initialize_db(db_path, None, table_name, table_col)
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} seen_files={db_path}", log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    def _is_renamed(f_path: str) -> bool:
        """rename 済み（接頭語で始まり接尾語で終わる）ファイルか。読み直さないよう監視対象から外す"""
        name = os.path.basename(f_path)
        return do_rename and name.startswith(rename_pfx) and name.endswith(rename_sfx)

    def _after_read(f_path: str, ack_path: Optional[str] = None) -> bool:
        """
        読み取った後の処理（delete / move / rename）。delete / move / rename の場合は ACK ファイルも削除する
        失敗した場合はログ・ErrorIF に記録して継続する（読み取り済みとして記録済みのため、動作中に読み直さない）
        Mod: 2026.10.02    By: IWATA, Y.'s Agent AI
        Returns: ErrorIF から停止指示があれば True
        """
        if after_read == "keep" or (after_read == "rename" and not do_rename):
            return False
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
            base.output_log(MODULE_NAME, f"After Read ({after_read}): {f_path}")
        except Exception as e:
            base.output_log(MODULE_NAME, f"After Read Error ({after_read}): {f_path}  {e}", log_file)
            _sig = ErrorIF.report_exception(e, phase="loop", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)
            return _sig == SIGNAL_STOP
        return False

    """======== LOOP START ========"""
    # is_online: ファイル読み取りが正常に行えているか（tri_edge_11_find_read_csv.py と同様）
    is_online = True
    last_retry_try = time.time()
    count = 0

    while not stop_event.is_set():
        try:
            """エラー頻発により読み取り休止"""
            if not is_online:
                if time.time() - last_retry_try >= offline_interval_sec:
                    last_retry_try = time.time()
                    is_online = True
                    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Retry Reading. Back to Online.", log_file)
                else:
                    time.sleep(interval_sec)
                    continue

            files = utl.list_files(monitor_dir, pfx_sfx, search_subdir)

            for f_path in files:
                """ Check File """
                # rename 済みのファイルは対象外
                if _is_renamed(f_path): continue

                # まだ処理していないファイルのみ
                _, is_seen = utl.check_record_exists(db_path, conn, table_name, [table_col[0][0], f_path])
                if is_seen: continue

                # 安定したファイルのみ
                not_stable = not(utl.check_file_stability_retry(stable_check_n, f_path))
                if not_stable: continue

                # Ackファイル存在チェック
                if check_ack:
                    ack_not_exists = not(utl.check_ack_file_exists(ack_ext, f_path))
                    if ack_not_exists: continue

                try:
                    """ファイルのメタ情報"""
                    dir_path, file_name = os.path.split(f_path)
                    stat_  = os.stat(f_path)
                    mtime_ = stat_.st_mtime

                    """JSONロード"""
                    json_content = utl.load_json(f_path, encoding)
                    if not json_content:
                        continue

                    """1. 共通データ(Common)の抽出"""
                    common_conf = mapping_conf.get("common", {})
                    common_data = [dir_path, file_name]

                    for k, v in common_conf.items():
                        if isinstance(v, list):
                            # 値読込み（対象キーをリストで指定）
                            common_data.append(_get_val_by_path(json_content, v))

                        elif isinstance(v, dict):
                            # 変換読込み（対象キーを辞書で指定）
                            path = v.get("path", [])
                            raw_val = _get_val_by_path(json_content, path)

                            if v.get("action") == "datetime_convert" and raw_val:
                                try:
                                    src_fmt = v.get("src_fmt", "")
                                    dst_fmt = v.get("dst_fmt", "")
                                    common_data.append(dt.strptime(raw_val, src_fmt).strftime(dst_fmt))
                                except Exception:
                                    common_data.append(None)
                            else:
                                common_data.append(raw_val)

                    """2. ループリスト(Loop)の特定"""
                    loop_conf = mapping_conf.get("to_record", {})
                    list_path = loop_conf.get("list_path", [])
                    item_map  = loop_conf.get("item_key_map", {})

                    target_list = _get_val_by_path(json_content, list_path)
                    if not isinstance(target_list, list):
                        target_list = []  # リストでない場合は空扱い

                    base.output_log(MODULE_NAME, f"Found File: {f_path}", log_file)

                    """展開・送信"""
                    # 1. リスト内要素を1レコードずつ処理（レコードを完成させてから1回だけ送信する）
                    for item in target_list:
                        if not isinstance(item, dict):
                            continue

                        record_val = common_data.copy()
                        for out_k, in_k in item_map.items():
                            record_val.append(item.get(in_k))

                        value, cast_fails = base.cast_dtype(record_val, cast_plan)
                        if cast_fails > 0:
                            ErrorIF.report_event({
                                "signature": "CAST_FAIL",
                                "phase": "loop",
                                "category": "data",
                                "trigger_id": TRIGGER_NAME,
                                "message": f"Cast failed {cast_fails} cols. file={f_path}",
                                "log_file": log_file,
                            })

                        queue_out.put((count, value))
                        count += 1
                        time.sleep(0.1)

                    # 2. jsonファイル自体を処理
                    if handle_json:
                        record_val_ = common_data.copy()
                        record_val_.append(file_name)

                        value, cast_fails = base.cast_dtype(record_val_, cast_plan)
                        if cast_fails > 0:
                            ErrorIF.report_event({
                                "signature": "CAST_FAIL",
                                "phase": "loop",
                                "category": "data",
                                "trigger_id": TRIGGER_NAME,
                                "message": f"Cast failed {cast_fails} cols (handle_json). file={f_path}",
                                "log_file": log_file,
                            })

                        queue_out.put((count, value))
                        count += 1
                        time.sleep(0.1)

                    """3. セグメント区切り送信"""
                    queue_out.put((count, ['____'] * n_col_def))
                    count = 0

                    """4. 発見済みファイル名に格納"""
                    utl.insert_records_to_db(db_path, conn, table_name, table_col, [[f_path, mtime_]])

                    """5. 読み取った後の処理（ACK ファイルも削除）"""
                    if _after_read(f_path, f"{f_path}.{ack_ext}" if check_ack else None):
                        stop_event.set()
                        break

                except (OSError, PermissionError) as e:
                    """ファイルI/Oエラー → オフライン化を検討"""
                    ErrorIF.report_exception(e, phase="loop", category="comm", trigger_id=TRIGGER_NAME, log_file=log_file)

                    signature = err.make_signature(type(e).__name__, "comm", "loop", TRIGGER_NAME)
                    if ErrorIF.is_frequent(signature):
                        is_online = False
                        last_retry_try = time.time()
                        base.output_log(MODULE_NAME,
                            f"{TRIGGER_NAME} Read Errors Frequent. Go Offline for {offline_interval_sec}s.", log_file)
                        break

                    continue

                except Exception as e:
                    """通信以外の予期しない例外（マッピング設定ミス等）。オフライン化はせずスキップして継続する"""
                    ErrorIF.report_exception(e, phase="loop", category="data", trigger_id=TRIGGER_NAME, log_file=log_file)
                    continue

            time.sleep(interval_sec)

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue
    """======== LOOP END ========"""

    try:
        """DBクリア"""
        conn = utl.initialize_db(db_path, conn, table_name, table_col)
        conn.close()

    except Exception as e:
        ErrorIF.report_exception(e, phase="shutdown", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Worker Stop.", log_file)
    return None


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")