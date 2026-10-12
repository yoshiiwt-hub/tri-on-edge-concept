# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# TriEdge Act Trigger Module
# Ver: 07.c.20261007    By: IWATA, Y.'s Agent AI
# Mod: a_insert_to_pg_trigger に col_custom_value を追加（指定した列に、セグメントの値にかかわらず固定値・現在時刻を入れる）
########################################
"""
MODULE_NAME = "ACT_RDB"

"""standard library"""
from   datetime import datetime as dt
from   datetime import timezone, timedelta
import json
import os
import queue
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Tuple, Sequence, Optional

"""external library"""
import pg8000.dbapi as pg_dbapi


"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()

except ModuleNotFoundError:
    try: # Check and Add Path
        dir_valid = 'it_rdb'  ## (system_root)/src/.
        cur_dir  = os.path.dirname(os.path.abspath(__file__))
        dir_name = os.path.basename(cur_dir)
        if dir_name != dir_valid:
            raise FileNotFoundError(f".py MUST be in {dir_valid} . Current: {dir_name}")

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
import src.it_rdb.tri_s19_rdb_postgres as rdb
from   src.tri_s10_stream_wave_stat import init_stats, calc_stats  # Calc Stats（Act 共通。B-9）

"""system dir, file"""
system_root = base.get_system_root()
log_dir  = base.get_log_dir()
log_file = base.get_log_path()
work_dir       = base.get_work_dir()
work_db_dir    = base.get_work_sub_dirs().get("db"   , os.path.join(work_dir, "db"))
work_run_dir   = base.get_work_sub_dirs().get("run"  , os.path.join(work_dir, "run"))
work_spool_dir = base.get_work_sub_dirs().get("spool", os.path.join(work_dir, "spool"))

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)

"""timezone"""
JST = timezone(timedelta(hours=9))


_ERR_VALUES_MAX_ROWS = 3    # INSERT 失敗時に「列名 = 値」を出す行数（1セグメントあたり）
_ERR_VALUE_MAX_LEN   = 200  # 1つの値の表示上限（超えた分は切り詰めて長さを付ける）


def _format_row_values(col_def_selected: Dict[int, Any], values: Sequence[Any]) -> str:
    """
    INSERT 失敗時のログ用に、行の値を「列名=値(repr)」の形で並べる（原因の列を特定しやすくするため）
    Mod: 2026.10.02    By: IWATA, Y.'s Agent AI
    """
    parts = []
    for i, v in enumerate(values):
        _def = col_def_selected.get(i)
        name = _def[0] if _def else f"col{i}"
        r = repr(v)
        if len(r) > _ERR_VALUE_MAX_LEN:
            r = f"{r[:_ERR_VALUE_MAX_LEN]}...(len={len(r)})"
        parts.append(f"{name}={r}")
    return ", ".join(parts)


########################################
# Act Trigger Functions
########################################

def a_insert_to_pg_trigger(
    post_queue,
    col_def,
    config,
    stop_event,
) -> None:
    """
    IMQ のセグメントデータを PostgreSQL テーブルへ INSERT する Act Trigger
    - 接続時、テーブル自動作成 + スキーマ整合チェック
    - 接続断時は再接続を試みる
    - col_custom_value: 指定した列に、セグメントの値にかかわらず固定値・現在時刻を入れる（キーは col_def のインデックス）
        {"<col>": {"src": "const", "value": 値}} / {"<col>": {"src": "now"}} / {"<col>": {"src": "now", "format": "%Y-%m-%d %H:%M:%S"}}
        現在時刻はセグメントごとに1回取得（全行で同じ）。format 省略時は datetime のまま（time 型の列向け）
        UTC→JST 変換の後に上書きする。hide_col_loc に含まれる列・範囲外・src の誤りは起動時エラー
    Mod: 2026.10.07    By: IWATA, Y.'s Agent AI（col_custom_value を追加）
    """

    TRIGGER_NAME = "a_pg_insert_imq_trigger"
    func_name  = sys._getframe().f_code.co_name
    func_obj   = globals().get(func_name)
    func_qmode = getattr(func_obj, "QMODE", "imq")

    """======== LOOP INIT ========"""
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Before Loop Setting.", log_file)

        # データ項目定義
        n_col_def = len(col_def)
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_def={col_def}", log_file)

        # (テーブル登録対象を選定、jsonb型変換対象を選定)
        hide_col_loc = config.get("hide_col_loc", [1, 2, 3])
        jsonb_col_loc = config.get("jsonb_col_loc", [9])
        utc_to_jst_col_loc = config.get("utc_to_jst_col_loc", [8])

        _ind = 0
        col_def_selected = {}
        jsonb_col_loc_selected = []
        utc_to_jst_col_loc_selected = []

        for k, v in col_def.items():
            if k in hide_col_loc:
                continue

            if k in jsonb_col_loc:
                jsonb_col_loc_selected.append(_ind)
                v = list(v)
                v[1] = "jsonb"

            if k in utc_to_jst_col_loc:
                utc_to_jst_col_loc_selected.append(_ind)

            col_def_selected[_ind] = v
            _ind += 1

        # (テーブル登録日時)
        insert_when_inserted = config.get("insert_when_inserted", False)
        if insert_when_inserted :
            _ind = len(col_def_selected.keys())
            col_def_selected[_ind] = ["uploaded_datetime", "time", "key", "key", "Timestamp When Data Inserted"]

        base.output_log(MODULE_NAME, f"col_def_selected: {col_def_selected}", log_file)
        base.output_log(MODULE_NAME, f"jsonb_col_loc_selected: {jsonb_col_loc_selected}", log_file)
        base.output_log(MODULE_NAME, f"utc_to_jst_col_loc_selected: {utc_to_jst_col_loc_selected}", log_file)

        # (指定した列に固定値・現在時刻を入れる。キーは col_def のインデックス、値は登録列の位置に対応付ける)
        col_custom_map = rdb.pg_parse_col_custom_value(config, col_def, hide_col_loc)
        base.output_log(MODULE_NAME, f"col_custom_value (登録列の位置): {col_custom_map}", log_file)

        # 接続設定
        config = rdb.pg_merge_env(config)
        pg_schema = config.get("pg_schema", "public")
        pg_table  = config.get("pg_table" , "")
        if not pg_table:
            raise ValueError("config['pg_table'] is NOT Set.")

        pg_reconnect_max  = int(config.get("pg_reconnect_max" , 3))
        pg_reconnect_wait = float(config.get("pg_reconnect_wait", 5.0))
        exclude_last_row  = bool(config.get("exclude_last_row" , True))

        # INSERT SQL 生成
        sql_insert = rdb.pg_make_insert_sql(pg_schema, pg_table, col_def_selected)
        base.output_log(MODULE_NAME, f"INSERT SQL: {sql_insert}", log_file)

        # PostgreSQL 初期接続
        conn_pg = None
        conn_pg = rdb.pg_connect_if_needed(conn_pg, config, TRIGGER_NAME, log_file)

        # テーブル自動作成（存在しなければ）
        rdb.pg_ensure_table(conn_pg, pg_schema, pg_table, col_def_selected, log_file)

        # スキーマ整合チェック（既存テーブルも含め必ず実行）
        schema_errors = rdb.pg_check_schema(conn_pg, pg_schema, pg_table, col_def_selected, log_file)
        if schema_errors:
            raise ValueError(
                f"DB Schema Invalid."
                f'  table="{pg_schema}"."{pg_table}"'
                f"  errors={schema_errors}"
            )

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Init OK."
            f'  table="{pg_schema}"."{pg_table}"'
            f"  exclude_last_row={exclude_last_row}", log_file)

    except Exception as e:
        ErrorIF.report_exception(
            e, phase="startup", category="config",
            trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== LOOP START ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start Loop...", log_file)
    while True:
        """キュー受信"""
        try:
            segment = post_queue.get(timeout=1.0)

        except queue.Empty:
            exit_loop = (
                stop_event.is_set()     and
                post_queue.qsize() == 0 and
                getattr(post_queue, "unfinished_tasks", 0) == 0
            )
            if exit_loop:
                break
            continue

        # センチネル受信時、終了
        if segment is None:
            post_queue.task_done()
            break

        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} ...Received Queue.")

        """セグメント処理"""
        try:
            """項目数整合性チェック"""
            n_col_act = len(segment[0])
            if n_col_act != n_col_def:
                base.output_log(MODULE_NAME,
                    f"Columns NOT Match: seg_id={segment[0][0]}"
                    f"  Expected:{n_col_def}  Actual:{n_col_act}", log_file)
                continue

            seg_id = segment[0][0]

            """終点行を除外（最終行は変化後のため）"""
            if exclude_last_row:
                segment = segment[:-1]

            """空セグメントチェック"""
            if len(segment) == 0:
                base.output_log(MODULE_NAME, f"seg_id={seg_id}  Null Segment was skipped.")
                continue

            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Segment 処理開始: seg_id={seg_id}  rows={len(segment)}")

            """セグメント単位 PostgreSQL 書込み"""
            try:
                # PostgreSQL 接続確認・再接続
                for _retry in range(pg_reconnect_max + 1):
                    try:
                        conn_pg = rdb.pg_connect_if_needed(conn_pg, config, TRIGGER_NAME, log_file)
                        break

                    except pg_dbapi.DatabaseError as e_conn:
                        base.output_log(MODULE_NAME,
                            f"PostgreSQL 接続失敗 ({_retry+1}/{pg_reconnect_max}): {e_conn}", log_file)
                        if _retry >= pg_reconnect_max:
                            raise
                        time.sleep(pg_reconnect_wait)
                        conn_pg = None

                # レコード単位 INSERT
                n_ok  = 0
                n_err = 0
                cur = conn_pg.cursor()
                try:
                    _now = dt.now()  # col_custom_value の現在時刻（セグメント内の全行で同じ時刻）
                    for row_num, row_val in enumerate(segment, start=1):
                        _ind = 0
                        row_val_selected = [v for i, v in enumerate(row_val) if i not in hide_col_loc]

                        # jsonb 列を json.dumps() に変換
                        for j in jsonb_col_loc_selected:
                            if j < len(row_val_selected):
                                val = row_val_selected[j]
                                if isinstance(val, (dict, list)):
                                    row_val_selected[j] = json.dumps(val, ensure_ascii=False)
                                elif val == "____":
                                    row_val_selected[j] = None

                        # UTC → JST 変換
                        for j in utc_to_jst_col_loc_selected:
                            if j < len(row_val_selected):
                                val = row_val_selected[j]
                                if val and isinstance(val, str):
                                    try:
                                        dt_utc = dt.fromisoformat(val.replace("Z", "+00:00"))
                                        dt_jst = dt_utc.astimezone(JST)
                                        row_val_selected[j] = dt_jst.strftime("%Y-%m-%d %H:%M:%S")
                                    except (ValueError, TypeError):
                                        pass

                        # 指定した列は、セグメントの値にかかわらず固定値・現在時刻にする（UTC→JST 変換の後に上書き）
                        if col_custom_map:
                            rdb.pg_apply_col_custom_value(row_val_selected, col_custom_map, _now)

                        if insert_when_inserted:
                            row_val_selected.append(dt.now())

                        try:
                            cur.execute(sql_insert, tuple(row_val_selected))
                            n_ok += 1

                        except Exception as e_rec:
                            # レコード単体エラー：ロールバックしてスキップ（最初の数行は「列名=値」も出して原因の列を追えるようにする）
                            conn_pg.rollback()
                            _msg = f"Record INSERT Error: seg_id={seg_id}  row={row_num}  {e_rec}"
                            if n_err < _ERR_VALUES_MAX_ROWS:
                                _msg += f"\n    values: {_format_row_values(col_def_selected, row_val_selected)}"
                            base.output_log(MODULE_NAME, _msg, log_file)
                            n_err += 1
                finally:
                    cur.close()

                # セグメント全体コミット
                conn_pg.commit()
                base.output_log(MODULE_NAME, f"PostgreSQL INSERT 完了: seg_id={seg_id}  ok={n_ok}  err={n_err}")

            except Exception as e_seg:
                try:
                    conn_pg.rollback()
                except Exception:
                    pass
                base.output_log(MODULE_NAME,
                    f"Segment Error: seg_id={seg_id}  {e_seg}", log_file)
                raise

        except Exception as e:
            _sig = ErrorIF.report_exception(
                e, phase="loop", category="data",
                trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue

        finally:
            post_queue.task_done()

    """======== LOOP END ========"""

    if conn_pg is not None:
        try:
            conn_pg.close()
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} PostgreSQL 接続クローズ.", log_file)
        except Exception as e:
            base.output_log(MODULE_NAME, f"PostgreSQL Close Error: {e}", log_file)

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None

a_insert_to_pg_trigger.QMODE = "imq"
a_insert_to_pg_trigger.CUT_LAST_ROW = False  # 全行（終点行を含む）を受け取り、exclude_last_row 設定で Act 側が判断する（意図を示すため False も明記）


def a_calc_stats_to_pg_trigger(
    post_queue,
    col_def,
    config,
    stop_event,
) -> None:
    """
    波形統計量を算出し、PostgreSQL テーブルへ INSERT する Act Trigger
    Mod: 2026.06.24    By: IWATA,Y.

    ■ 動作概要
        1. segment を受信
        2. column_register_map / out_coldef に基づき統計量を算出
           （tri_s10_stream_wave_stat の compile_register_map / build_stats を使用）
        3. 算出結果を PostgreSQL テーブルへ INSERT
        - 接続時、テーブル自動作成 + スキーマ整合チェック（a_insert_to_pg_trigger と同様）
        - 接続断時は再接続を試みる

    ■ column_register_map / out_coldef フォーマット
        column_register_map と out_coldef の仕様は tri_s10_stream_wave_stat.compile_register_map
        を参照（TriEdgeDoc_01-5_波形統計量算出_設定ガイド.md も参照）。

    ■ テーブル仕様
        固定列: seg_id BIGINT, seg_ts_start TIMESTAMP, seg_ts_end TIMESTAMP, seg_rows INTEGER
        可変列: out_coldef の dtype に応じて自動生成（pg_ensure_table / pg_check_schema を使用）
        重複時: upsert=true の場合のみ ON CONFLICT(seg_id) DO UPDATE（デフォルト: false = 通常INSERT）
                ※ upsert=true を使う場合、対象テーブルの seg_id 列に事前に
                   UNIQUE または PRIMARY KEY 制約を付与しておくこと（pg_ensure_table は付与しない）

    ■ PostgreSQL設定
        pg_schema, pg_table        : 保存先テーブル
        pg_reconnect_max/wait      : 再接続設定
        exclude_last_row           : セグメント終点行（変化後の行）を集計対象から除外するか（デフォルト: true）
        upsert                     : true の場合 ON CONFLICT(seg_id) DO UPDATE で上書き（デフォルト: false）
    """

    TRIGGER_NAME = "a_calc_stats_to_pg_trigger"
    func_name  = sys._getframe().f_code.co_name
    func_obj   = globals().get(func_name)
    func_qmode = getattr(func_obj, "QMODE", "imq")

    """======== LOOP INIT ========"""
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Before Loop Setting.", log_file)

        # データ項目定義
        n_col_def = len(col_def)
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_keys={col_keys}", log_file)

        # 統計量算出設定（tri_s10_stream_wave_stat）
        stats_ctx    = init_stats(config, col_def, MODULE_NAME, TRIGGER_NAME)
        out_coldef   = stats_ctx.out_coldef

        # 固定列（seg_id / seg_ts_start / seg_ts_end / seg_rows） + out_coldef の可変列
        col_def_selected = {
            0: ["seg_id",       "int" , "key", "key", "Segment ID"],
            1: ["seg_ts_start", "time", "key", "key", "Segment Start Timestamp"],
            2: ["seg_ts_end",   "time", "key", "key", "Segment End Timestamp"],
            3: ["seg_rows",     "int" , "key", "key", "Segment Row Count"],
        }
        for _k, _v in sorted(out_coldef.items(), key=lambda x: int(x[0])):
            col_def_selected[len(col_def_selected)] = _v
        base.output_log(MODULE_NAME, f"col_def_selected: {col_def_selected}", log_file)

        # 接続設定
        config = rdb.pg_merge_env(config)
        pg_schema = config.get("pg_schema", "public")
        pg_table  = config.get("pg_table" , "")
        if not pg_table:
            raise ValueError("config['pg_table'] is NOT Set.")

        pg_reconnect_max  = int(config.get("pg_reconnect_max" , 3))
        pg_reconnect_wait = float(config.get("pg_reconnect_wait", 5.0))
        exclude_last_row  = bool(config.get("exclude_last_row" , True))
        upsert            = bool(config.get("upsert", False))

        # INSERT SQL 生成（upsert=true の場合は ON CONFLICT(seg_id) DO UPDATE を付加）
        sql_insert = rdb.pg_make_insert_sql(pg_schema, pg_table, col_def_selected)
        if upsert:
            _update_cols = ", ".join(
                f'"{col_def_selected[i][0]}" = EXCLUDED."{col_def_selected[i][0]}"'
                for i in range(1, len(col_def_selected))  # seg_id(0) 以外を上書き
            )
            sql_insert += f' ON CONFLICT ("{col_def_selected[0][0]}") DO UPDATE SET {_update_cols}'
        base.output_log(MODULE_NAME, f"INSERT SQL: {sql_insert}", log_file)

        # PostgreSQL 初期接続
        conn_pg = None
        conn_pg = rdb.pg_connect_if_needed(conn_pg, config, TRIGGER_NAME, log_file)

        # テーブル自動作成（存在しなければ）
        rdb.pg_ensure_table(conn_pg, pg_schema, pg_table, col_def_selected, log_file)

        # スキーマ整合チェック（既存テーブルも含め必ず実行）
        schema_errors = rdb.pg_check_schema(conn_pg, pg_schema, pg_table, col_def_selected, log_file)
        if schema_errors:
            raise ValueError(
                f"DB Schema Invalid."
                f'  table="{pg_schema}"."{pg_table}"'
                f"  errors={schema_errors}"
            )

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Init OK."
            f'  table="{pg_schema}"."{pg_table}"'
            f"  exclude_last_row={exclude_last_row}  upsert={upsert}", log_file)

    except Exception as e:
        ErrorIF.report_exception(
            e, phase="startup", category="config",
            trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== LOOP START ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start Loop...", log_file)
    while True:
        """キュー受信"""
        try:
            segment = post_queue.get(timeout=1.0)

        except queue.Empty:
            exit_loop = (
                stop_event.is_set()     and
                post_queue.qsize() == 0 and
                getattr(post_queue, "unfinished_tasks", 0) == 0
            )
            if exit_loop:
                break
            continue

        # センチネル受信時、終了
        if segment is None:
            post_queue.task_done()
            break

        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} ...Received Queue.")

        """セグメント処理"""
        try:
            """項目数整合性チェック"""
            n_col_act = len(segment[0])
            if n_col_act != n_col_def:
                base.output_log(MODULE_NAME,
                    f"Columns NOT Match: seg_id={segment[0][0]}"
                    f"  Expected:{n_col_def}  Actual:{n_col_act}", log_file)
                continue

            seg_id = segment[0][0]

            """終点行を除外（最終行は変化後のため）"""
            if exclude_last_row:
                segment = segment[:-1]

            """空セグメントチェック"""
            if len(segment) == 0:
                base.output_log(MODULE_NAME, f"seg_id={seg_id}  Null Segment was skipped.", log_file)
                continue

            """統計量算出"""
            stats_dict = calc_stats(stats_ctx, segment, seg_id)# 値が出せなかった項目は calc_stats がログに出す（従来は黙って NULL）
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} 統計量算出完了: seg_id={seg_id}  rows={stats_dict.get('seg_rows')}"
                f"  keys={len(stats_dict)}", log_file)

            """セグメント単位 PostgreSQL 書込み"""
            try:
                # PostgreSQL 接続確認・再接続
                for _retry in range(pg_reconnect_max + 1):
                    try:
                        conn_pg = rdb.pg_connect_if_needed(conn_pg, config, TRIGGER_NAME, log_file)
                        break

                    except pg_dbapi.DatabaseError as e_conn:
                        base.output_log(MODULE_NAME,
                            f"PostgreSQL 接続失敗 ({_retry+1}/{pg_reconnect_max}): {e_conn}", log_file)
                        if _retry >= pg_reconnect_max:
                            raise
                        time.sleep(pg_reconnect_wait)
                        conn_pg = None

                # col_def_selected の列順に値を組み立て（build_statsで欠落したキーはNULL）
                row_val = tuple(
                    stats_dict.get(col_def_selected[i][0]) for i in range(len(col_def_selected))
                )

                cur = conn_pg.cursor()
                try:
                    cur.execute(sql_insert, row_val)
                    conn_pg.commit()
                    base.output_log(MODULE_NAME,
                        f"PostgreSQL INSERT 完了: seg_id={seg_id}", log_file)

                except Exception as e_ins:
                    conn_pg.rollback()
                    base.output_log(MODULE_NAME,
                        f"Stats INSERT Error: seg_id={seg_id}  {e_ins}"
                        f"\n    values: {_format_row_values(col_def_selected, row_val)}", log_file)
                    raise

                finally:
                    cur.close()

            except Exception as e_seg:
                try:
                    conn_pg.rollback()
                except Exception:
                    pass
                base.output_log(MODULE_NAME,
                    f"Segment Error: seg_id={seg_id}  {e_seg}", log_file)
                raise

        except Exception as e:
            _sig = ErrorIF.report_exception(
                e, phase="loop", category="data",
                trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue

        finally:
            post_queue.task_done()

    """======== LOOP END ========"""

    if conn_pg is not None:
        try:
            conn_pg.close()
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} PostgreSQL 接続クローズ.", log_file)
        except Exception as e:
            base.output_log(MODULE_NAME, f"PostgreSQL Close Error: {e}", log_file)

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None

a_calc_stats_to_pg_trigger.QMODE = "imq"
a_calc_stats_to_pg_trigger.CUT_LAST_ROW = False  # 全行（終点行を含む）を受け取り、exclude_last_row 設定で Act 側が判断する（意図を示すため False も明記）


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
