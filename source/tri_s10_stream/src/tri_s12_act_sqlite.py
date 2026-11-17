# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [SQLite Store]
# Ver: 02.d.20261116    By: IWATA,Y.'s Agent AI
# Mod: 死活監視の結果を機器ごとに最新の1行で保つ a_upsert_alive_to_sqlite_trigger を追加
########################################
セグメントの値や統計量を、エッジ上の SQLite に蓄積する出口

主要関数:
a_store_seg_trigger    # セグメントの全行を SQLite に保存する。
a_store_stats_trigger  # セグメントの統計量を求め、SQLite に保存する。
a_upsert_alive_to_sqlite_trigger  # tri_s11_find_alive の結果を、機器ごとに1行の専用テーブルへ UPSERT し、状態の変化を通知する。
_run_store_trigger     # 2つの trigger の共通ループ（保存する内容とテーブルの構造だけが異なる）。

位置づけ: 上位の DB が無い・つながらない環境で、エッジ上にデータを貯める。統計量の算出は tri_s10_stream_wave_stat を使う。
設計意図: 同じセグメント（同じ行）が再び届いた場合は上書きし（INSERT OR REPLACE）、重複した行が増えないようにする。
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
    """SQLiteへ保存する共通ループ。mode で保存内容・テーブル構造だけが変わる

    [テーブル仕様]（旧モジュール Docstring から移動）
        a_store_stats_trigger: 固定列 seg_id TEXT PRIMARY KEY, seg_ts_start TEXT, seg_ts_end TEXT, seg_rows INTEGER
                               可変列は out_coldef の dtype に応じて INTEGER / REAL / TEXT を自動生成
                               INSERT OR REPLACE（seg_id が一致する行を上書き）
        a_store_seg_trigger  : 列は col_def をそのまま使う（segment_header の列は col_def に含まれるため合成しない）
                               PK は col_def の先頭2列（慣例上 seg_id, seg_ind）の複合 PRIMARY KEY
                               INSERT OR REPLACE（seg_id + 行番号が一致する行を上書き）
    """
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
# 死活監視の結果（tri_s11_find_alive）を機器ごとに最新の1行で保つ
# （列・行の作り方・通知文は tri_s12_act_rdb_alive と同じ考え方。pg8000 を使わないよう、SQLite 用にここに置く）
########################################

ALIVE_TABLE_DEFAULT = "triedge_device_alive"
ALIVE_SEPARATOR     = "____"

# 専用テーブルの列（列名, SQLite の型）。device_key が主キー。updated_at は Act が付ける
ALIVE_TABLE_COLUMNS: List[Tuple[str, str]] = [
    ("device_key" , "TEXT"),
    ("device_name", "TEXT"),
    ("status"     , "TEXT"),
    ("checked_at" , "TEXT"),
    ("host"       , "TEXT"),
    ("port"       , "INTEGER"),
    ("rtt_ms"     , "REAL"),
    ("detail"     , "TEXT"),
    ("updated_at" , "TEXT"),
]
ALIVE_INPUT_COLUMNS  = [c for c, _ in ALIVE_TABLE_COLUMNS if c != "updated_at"]
ALIVE_INPUT_REQUIRED = ("device_key", "status")


def _alive_input_loc(col_def: Dict[int, Any]) -> Dict[str, int]:
    """受け取る列の名前 → 位置（col_def のインデックス）。device_key・status が無ければ ValueError（起動時に1回）"""
    loc: Dict[str, int] = {}
    for k in sorted(col_def.keys()):
        name = str(col_def[k][0])
        if name in ALIVE_INPUT_COLUMNS and name not in loc:
            loc[name] = int(k)
    missing = [n for n in ALIVE_INPUT_REQUIRED if n not in loc]
    if missing:
        raise ValueError(f"column に必須の列がありません: {missing}  （受け取る列: {ALIVE_INPUT_COLUMNS}）")
    return loc


def _ensure_alive_table(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    専用テーブルが無ければ作る。あれば列と device_key 単独の一意性（主キー・UNIQUE）を確かめ、合わなければ ValueError
    （既存のテーブルは変えない）
    Mod: 2026.11.16    By: IWATA,Y.'s Agent AI
    Returns: True: 新規作成 / False: 既存
    """
    cols = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    if not cols:
        col_defs = ", ".join(f'"{c}" {t}' + (" PRIMARY KEY" if c == "device_key" else "") for c, t in ALIVE_TABLE_COLUMNS)
        conn.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')
        conn.commit()
        return True

    errors: List[str] = []
    have = {str(r[1]): str(r[2]).upper() for r in cols}      # 列名 → 型
    for c, t in ALIVE_TABLE_COLUMNS:
        if c not in have:
            errors.append(f"列 {c} がありません")
        elif have[c] != t:
            errors.append(f"列 {c} の型が {have[c]} です（{t} が必要）")
    pk = [str(r[1]) for r in cols if int(r[5]) > 0]
    unique_on_key = (pk == ["device_key"])
    for idx in conn.execute(f'PRAGMA index_list("{table_name}")').fetchall():
        if int(idx[2]) == 1 and (len(idx) < 5 or int(idx[4]) == 0):   # unique かつ部分インデックスでない
            idx_cols = [str(r[2]) for r in conn.execute(f'PRAGMA index_info("{idx[1]}")').fetchall()]
            if idx_cols == ["device_key"]:
                unique_on_key = True
    if not unique_on_key:
        errors.append("device_key 単独の主キー（PRIMARY KEY）または一意性（UNIQUE）がありません（UPSERT に必要）")
    if errors:
        raise ValueError(f"死活監視テーブルの定義が合いません。テーブルは変更しないため、直すか別の table_name を指定してください。"
                         f"  table={table_name}  errors={errors}")
    return False


def _alive_record(row: List[Any], loc: Dict[str, int], now: dt) -> Optional[Dict[str, Any]]:
    """セグメントの1行から、テーブルに入れる値の dict を作る（区切り行・device_key が空の行は None）"""
    def _get(name: str) -> Any:
        i = loc.get(name)
        if i is None or i >= len(row):
            return None
        v = row[i]
        return None if v == ALIVE_SEPARATOR else v

    key = _get("device_key")
    if key is None or str(key).strip() == "":
        return None

    checked_at = _get("checked_at")
    if isinstance(checked_at, str):
        try:
            checked_at = dt.fromisoformat(checked_at)
        except ValueError:
            pass                                              # 読めない文字列はそのまま入れる
    try:
        port = None if _get("port") is None else int(_get("port"))
    except (TypeError, ValueError):
        port = None
    try:
        rtt_ms = None if _get("rtt_ms") is None else float(_get("rtt_ms"))
    except (TypeError, ValueError):
        rtt_ms = None

    def _str(name: str) -> Optional[str]:
        v = _get(name)
        return None if v is None else str(v)

    return {
        "device_key" : str(key),
        "device_name": _str("device_name"),
        "status"     : _str("status"),
        "checked_at" : checked_at,
        "host"       : _str("host"),
        "port"       : port,
        "rtt_ms"     : rtt_ms,
        "detail"     : _str("detail"),
        "updated_at" : now,
    }


def _alive_change_message(rec: Dict[str, Any], prev: Optional[str], time_format: str) -> Optional[str]:
    """
    状態が変わった機器の通知文を返す（変わっていなければ None）。前回の状態が無い機器（初回）は NG のときだけ通知する
    例: 「機器３（dev03）：OK → NG（2026-10-08 10:00:00、timeout）」
    """
    new = rec.get("status")
    if prev is None:
        if new != "NG":
            return None
    elif prev == new:
        return None
    checked_at = rec.get("checked_at")
    if isinstance(checked_at, dt):
        when = checked_at.strftime(time_format)
    else:
        when = "-" if checked_at is None else str(checked_at)
    name = rec.get("device_name") or rec["device_key"]
    return (f"{name}（{rec['device_key']}）：{prev if prev is not None else '-'} → {new}"
            f"（{when}、{rec.get('detail') or '-'}）")


def _set_alive_metrics(states: Dict[str, Optional[str]], names: Dict[str, str], last_checked_at: Any) -> None:
    """機器ごとの状態から運用系メトリクス（台数・NG 台数・NG の機器・最後に確認した日時）を更新する"""
    ng_keys = [k for k, s in states.items() if s != "OK"]
    base.metrics_set_last(MODULE_NAME, "devices_total", len(states))
    base.metrics_set_last(MODULE_NAME, "ng_count", len(ng_keys))
    ng_list = ", ".join(names.get(k) or k for k in ng_keys)
    if len(ng_list) > 500:
        ng_list = ng_list[:500] + "..."
    base.metrics_set_last(MODULE_NAME, "ng_devices", ng_list if ng_list else "-")
    if isinstance(last_checked_at, dt):
        last_checked_at = last_checked_at.strftime("%Y-%m-%d %H:%M:%S")
    base.metrics_set_last(MODULE_NAME, "last_checked_at", "-" if last_checked_at is None else str(last_checked_at))


def a_upsert_alive_to_sqlite_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """
    Find Alive の結果（1周期 = 1セグメント）を SQLite の専用テーブルへ UPSERT し、状態が変わった機器をオンデマンド通知する
    Mod: 2026.11.16    By: IWATA,Y.'s Agent AI

    [専用テーブル（列は固定。tri_s12_act_rdb_alive と同じ列）]
        device_key TEXT PRIMARY KEY / device_name TEXT / status TEXT（OK / NG）/ checked_at TEXT /
        host TEXT / port INTEGER / rtt_ms REAL（NG のときは NULL）/ detail TEXT / updated_at TEXT（更新した日時）
        既存のテーブルは、列と device_key 単独の一意性を確かめる（合わなければ変更せずに起動時に止まる）
    受け取る列（列名で取り出す。device_key・status は必須、ほかは無ければ NULL）
        device_key, device_name, status, checked_at, host, port, rtt_ms, detail
    [設定]
        db_path（既定 output/alive.db）/ table_name（既定 triedge_device_alive）/
        teams_notify（既定 true）/ notify_time_format（既定 %Y-%m-%d %H:%M:%S）
    [動作の要点]
        区切り行・device_key が空の行は書かない。同じ機器が複数行あれば後の行を使う
        機器ごとに SAVEPOINT で区切って UPSERT し（1行の失敗で他の行を失わない）、セグメントで1回コミットする
        前回の状態は起動時にテーブルから読み、メモリに持つ。OK と NG が入れ替わったとき（初回は NG のとき）だけ通知する
    """
    TRIGGER_NAME = "a_upsert_alive_to_sqlite_trigger"

    """======== STARTUP ========"""
    conn: Optional[sqlite3.Connection] = None
    try:
        loc = _alive_input_loc(col_def)
        base.output_log(MODULE_NAME, f"input columns (name -> loc): {loc}", log_file)

        db_path = base.get_absolute_path(str(config.get("db_path", "output/alive.db")))
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        table_name = str(config.get("table_name", ALIVE_TABLE_DEFAULT) or ALIVE_TABLE_DEFAULT).strip()
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', table_name):
            raise ValueError(f"table_name must match [A-Za-z_][A-Za-z0-9_]*. Got: {table_name!r}")
        teams_notify       = bool(config.get("teams_notify", True))
        notify_time_format = str(config.get("notify_time_format", "%Y-%m-%d %H:%M:%S"))

        conn = sqlite3.connect(db_path, check_same_thread=False)
        created = _ensure_alive_table(conn, table_name)
        base.output_log(MODULE_NAME,
            f"死活監視テーブルを{'新規作成しました' if created else '確認しました'}。 table={table_name}  db={db_path}", log_file)

        names_list = [c for c, _ in ALIVE_TABLE_COLUMNS]
        cols_str   = ", ".join(f'"{c}"' for c in names_list)
        set_str    = ", ".join(f'"{c}" = excluded."{c}"' for c in names_list if c != "device_key")
        sql_upsert = (f'INSERT INTO "{table_name}" ({cols_str}) VALUES ({", ".join("?" * len(names_list))})'
                      f' ON CONFLICT("device_key") DO UPDATE SET {set_str}')
        base.output_log(MODULE_NAME, f"UPSERT SQL: {sql_upsert}", log_file)

        # 前回の状態（起動時に1回読み、以降はメモリに持つ）
        states: Dict[str, Optional[str]] = {}
        names:  Dict[str, str] = {}
        for key, status, name in conn.execute(f'SELECT "device_key", "status", "device_name" FROM "{table_name}"'):
            states[str(key)] = None if status is None else str(status)
            if name:
                names[str(key)] = str(name)
        base.output_log(MODULE_NAME, f"前回の状態を読み込みました。 {len(states)}台  {states}", log_file)

        base.metrics_add(MODULE_NAME, "total_segments", 0)
        base.metrics_add(MODULE_NAME, "state_changes", 0)
        base.metrics_set_last(MODULE_NAME, "last_seg_id", "-")
        _set_alive_metrics(states, names, None)

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Init OK.  db={db_path}  table={table_name}  teams_notify={teams_notify}", log_file)

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
            if not segment:
                continue
            seg_id = segment[0][0]

            # 1. テーブルに入れる値（区切り行は除く。同じ機器が複数行あれば後の行を使う）
            now = dt.now()
            records: Dict[str, Dict[str, Any]] = {}
            for row in segment:
                rec = _alive_record(row, loc, now)
                if rec is not None:
                    records[rec["device_key"]] = rec
            if not records:
                base.output_log(MODULE_NAME, f"seg_id={seg_id}  No Device Row. skipped.")
                continue

            # 2. 機器ごとに UPSERT（1行の失敗で他の行を失わないよう SAVEPOINT で区切る）し、セグメントで1回コミット
            done: List[Dict[str, Any]] = []
            n_err = 0
            try:
                for rec in records.values():
                    conn.execute("SAVEPOINT triedge_alive_row")
                    try:
                        conn.execute(sql_upsert, [_to_sqlite_val(rec[c]) for c in names_list])
                        conn.execute("RELEASE SAVEPOINT triedge_alive_row")
                        done.append(rec)
                    except sqlite3.Error as e_rec:
                        conn.execute("ROLLBACK TO SAVEPOINT triedge_alive_row")
                        conn.execute("RELEASE SAVEPOINT triedge_alive_row")
                        base.output_log(MODULE_NAME,
                            f"Record UPSERT Error: seg_id={seg_id}  device_key={rec['device_key']}  {e_rec}", log_file)
                        n_err += 1
                conn.commit()
            except Exception:
                try: conn.rollback()
                except Exception: pass
                raise
            base.output_log(MODULE_NAME, f"SQLite UPSERT 完了: seg_id={seg_id}  ok={len(done)}  err={n_err}")   # 正常は画面のみ

            # 3. 状態の変化（コミットできた行のみ）→ メモリの状態を更新し、変化があれば通知
            messages: List[str] = []
            for rec in done:
                msg = _alive_change_message(rec, states.get(rec["device_key"]), notify_time_format)
                if msg:
                    messages.append(msg)
                states[rec["device_key"]] = rec["status"]
                if rec.get("device_name"):
                    names[rec["device_key"]] = rec["device_name"]

            # 4. 運用系メトリクス（通知は送信時点の metrics を載せるため、通知の要求より先に更新）
            base.metrics_add(MODULE_NAME, "total_segments", 1)
            base.metrics_set_last(MODULE_NAME, "last_seg_id", seg_id)
            _set_alive_metrics(states, names,
                               max((r["checked_at"] for r in done if isinstance(r["checked_at"], dt)), default=None))
            if messages:
                base.metrics_add(MODULE_NAME, "state_changes", len(messages))
                for m in messages:
                    base.output_log(MODULE_NAME, f"状態変化: {m}", log_file)
                if teams_notify:
                    base.metrics_notify(MODULE_NAME, "\n\n".join(messages))

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="io",
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

a_upsert_alive_to_sqlite_trigger.QMODE = "imq"
a_upsert_alive_to_sqlite_trigger.CUT_LAST_ROW = True  # 終点行（区切り行 '____'）は core で除外して受け取る


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
