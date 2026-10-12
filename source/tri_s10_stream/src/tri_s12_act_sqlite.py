# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [SQLite Store]
# Ver: 02.b.20261005    By: IWATA, Y.'s Agent AI
# Mod: 統計まわりを tri_s10_stream_wave_stat の init_stats / calc_stats に集約（値が出せなかった項目を共通形式でログに出す。B-9）
########################################
a_store_seg_trigger    # セグメントの全行を SQLite に保存する
a_store_stats_trigger  # セグメントの統計量を算出し、SQLite に保存する

動作概要
    1. segment を受信
    2. a_store_seg_trigger   : セグメントの全行を col_def の列定義のまま
                                INSERT OR REPLACE（seg_id + row_num が一致する行を上書き）
       a_store_stats_trigger : column_register_map / out_coldef に基づき統計量を算出
                                （tri_s10_stream_wave_stat の compile_register_map / build_stats を使用）
                                してから INSERT OR REPLACE（seg_id が一致する行を上書き）

SQLite設定（共通）
    db_path     : SQLiteファイルパス（絶対パス または システムルートからの相対パス）
    table_name  : テーブル名（英数字・アンダースコアのみ。省略時 a_store_stats_trigger は"stats"、
                  a_store_seg_trigger は"segments"）
    reset_table : 起動時にテーブルをDROPしてから作り直すか（デフォルト: False）

a_store_stats_trigger 専用設定
    column_register_map : 統計量算出用の列マッピング定義（必須）
    out_coldef           : 統計量の出力列定義（必須）

テーブル仕様（a_store_stats_trigger）
    固定列: seg_id TEXT PRIMARY KEY, seg_ts_start TEXT, seg_ts_end TEXT, seg_rows INTEGER
    可変列: out_coldef の dtype に応じて INTEGER / REAL / TEXT を自動生成

テーブル仕様（a_store_seg_trigger）
    列: col_def をそのまま使う（seg_id/seg_ind/seg_ts/seg_status 等の segment_header 列は
        col_def に既に含まれているため、別途合成しない）
    PK: col_def の先頭2列（慣例上 seg_id, seg_ind）の複合PRIMARY KEY
"""
MODULE_NAME = "ACT_SQLite"

"""standard library"""
from   datetime import datetime as dt
import os
import queue
import re
import sqlite3
import sys
import threading
import traceback
from   typing   import Any, Dict, List, Optional, Tuple

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
from   src.tri_s10_stream_wave_stat import init_stats, calc_stats  # Calc Stats（Act 共通。B-9）

"""system dir, file"""
system_root = base.get_system_root()
log_file    = base.get_log_path()
work_dir    = base.get_work_dir()

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# SQLite helper
########################################

_DTYPE_TO_SQL: Dict[str, str] = {
    "int"  : "INTEGER",
    "float": "REAL",
    "str"  : "TEXT",
    "time" : "TEXT",
}

_STATS_FIXED_COLS: List[Tuple[str, str]] = [
    ("seg_id",       "TEXT"),
    ("seg_ts_start", "TEXT"),
    ("seg_ts_end",   "TEXT"),
    ("seg_rows",     "INTEGER"),
]


def _build_user_cols(coldef: Dict) -> List[Tuple[str, str]]:
    """out_coldef または col_def から (列名, SQLite型) のリストを index 昇順で返す"""
    result: List[Tuple[str, str]] = []
    for idx in sorted(coldef.keys(), key=int):
        entry    = coldef[idx]
        col_name = str(entry[0])
        dtype    = str(entry[1]).lower() if len(entry) > 1 else "str"
        result.append((col_name, _DTYPE_TO_SQL.get(dtype, "TEXT")))
    return result


def _ensure_table(
    conn:       sqlite3.Connection,
    table_name: str,
    fixed_cols: List[Tuple[str, str]],
    user_cols:  List[Tuple[str, str]],
    pk_cols:    List[str],
) -> None:
    """テーブルが存在しない場合のみ作成する（pk_cols で複合PKにも対応）"""
    col_defs: List[str] = [f'"{c}" {t}' for c, t in fixed_cols + user_cols]
    pk_clause = ""
    if pk_cols:
        pk_clause = ", PRIMARY KEY (" + ", ".join(f'"{c}"' for c in pk_cols) + ")"
    conn.execute(
        f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)}{pk_clause})'
    )
    conn.commit()


def _to_sqlite_val(v: Any) -> Any:
    """SQLite ネイティブ型（int/float/str/bytes/None）以外は str に変換する"""
    if v is None or isinstance(v, (int, float, str, bytes)):
        return v
    return str(v)


def _insert_stats(
    conn:          sqlite3.Connection,
    table_name:    str,
    all_col_names: List[str],
    stats_dict:    Dict[str, Any],
) -> None:
    """stats_dict を INSERT OR REPLACE で保存する"""
    values       = [_to_sqlite_val(stats_dict.get(c)) for c in all_col_names]
    placeholders = ", ".join("?" * len(all_col_names))
    col_list     = ", ".join(f'"{c}"' for c in all_col_names)
    conn.execute(
        f'INSERT OR REPLACE INTO "{table_name}" ({col_list}) VALUES ({placeholders})',
        values,
    )
    conn.commit()


def _insert_seg_rows(
    conn:          sqlite3.Connection,
    table_name:    str,
    all_col_names: List[str],
    segment:       List[List[Any]],
) -> None:
    """セグメントの全行を INSERT OR REPLACE で保存する
    （各行に seg_id/seg_ind が既に含まれているため、そのまま列に対応させる）"""
    to_insert = [[_to_sqlite_val(v) for v in row] for row in segment]
    placeholders = ", ".join("?" * len(all_col_names))
    col_list     = ", ".join(f'"{c}"' for c in all_col_names)
    conn.executemany(
        f'INSERT OR REPLACE INTO "{table_name}" ({col_list}) VALUES ({placeholders})',
        to_insert,
    )
    conn.commit()


########################################
# 共通保存ループ（a_store_seg_trigger / a_store_stats_trigger の実体）
########################################

def _run_store_trigger(
    mode:       str,  # "seg" または "stats"
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """SQLiteへ保存する共通ループ。mode で保存内容・テーブル構造だけが変わる"""
    TRIGGER_NAME = "a_store_seg_trigger" if mode == "seg" else "a_store_stats_trigger"

    """======== STARTUP ========"""
    conn: Optional[sqlite3.Connection] = None
    try:
        n_col_def = len(col_def)
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_keys={col_keys}  mode={mode}", log_file)

        if mode == "stats":
            """ 統計量マップ """
            stats_ctx    = init_stats(config, col_def, MODULE_NAME, TRIGGER_NAME)
            out_coldef   = stats_ctx.out_coldef

            user_cols  = _build_user_cols(out_coldef)
            fixed_cols = _STATS_FIXED_COLS
            pk_cols    = ["seg_id"]
        else:
            """ raw行モード：col_def には既に seg_id/seg_ind 等が含まれているため、
                それをそのままテーブル列として使う（合成の固定列は追加しない） """
            user_cols  = _build_user_cols(col_def)
            fixed_cols = []
            pk_cols    = [c for c, _ in user_cols[:2]]  # 慣例上 col_def[0]=seg_id, [1]=seg_ind

        all_col_names = [c for c, _ in fixed_cols] + [c for c, _ in user_cols]

        """ テーブル設定 """
        db_path = base.get_absolute_path(str(config.get("db_path", "output/stats.db")))
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        table_name = str(config.get("table_name", "stats" if mode == "stats" else "segments")).strip()
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', table_name):
            raise ValueError(
                f"table_name must match [A-Za-z_][A-Za-z0-9_]*. Got: {table_name!r}")

        reset_table = bool(config.get("reset_table", False))

        """ テーブル初期化 """
        conn = sqlite3.connect(db_path, check_same_thread=False)

        if reset_table:
            conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            conn.commit()
            base.output_log(MODULE_NAME,
                f"!!! WARNING !!! Table Reset by config 'reset_table': True  "
                f"table={table_name}  db={db_path}", log_file)

        _ensure_table(conn, table_name, fixed_cols, user_cols, pk_cols)

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} db={db_path}  table={table_name}  mode={mode}  "
            f"cols({len(all_col_names)})={all_col_names}", log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config",
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        if conn:
            try: conn.close()
            except Exception: pass
        stop_event.set()
        return None

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

            if not segment:
                base.output_log(MODULE_NAME,
                    f"WARNING  空セグメント、スキップ。 seg_id={seg_id}", log_file)
                continue

            """統計量算出 or raw行保存"""
            if mode == "stats":
                stats_dict = calc_stats(stats_ctx, segment, seg_id)# 値が出せなかった項目は calc_stats がログに出す
                _insert_stats(conn, table_name, all_col_names, stats_dict)

                base.output_log(MODULE_NAME,
                    f"{TRIGGER_NAME} Saved. seg_id={seg_id}  rows={len(segment)}  mode=stats", log_file)
            else:
                _insert_seg_rows(conn, table_name, all_col_names, segment)

                base.output_log(MODULE_NAME,
                    f"{TRIGGER_NAME} Saved. seg_id={seg_id}  rows={len(segment)}  mode=seg", log_file)

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
    if conn:
        try: conn.close()
        except Exception: pass

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None


########################################
# Act Trigger Functions（薄いラッパー）
########################################

def a_store_seg_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """セグメントの全行を SQLite に保存する"""
    return _run_store_trigger("seg", post_queue, col_def, config, stop_event)

a_store_seg_trigger.QMODE = "imq"
a_store_seg_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


def a_store_stats_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """セグメントの統計量を算出し、SQLite に保存する"""
    return _run_store_trigger("stats", post_queue, col_def, config, stop_event)

a_store_stats_trigger.QMODE = "imq"
a_store_stats_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
