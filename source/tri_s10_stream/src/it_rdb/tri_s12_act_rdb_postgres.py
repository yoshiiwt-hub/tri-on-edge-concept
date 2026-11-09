# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# TriEdge Act Trigger Module
# Ver: 07.g.20261108    By: IWATA, Y.'s Agent AI
# Mod: pyflakes の指摘を整理（使っていない import・変数、置き換えの無い f-string、決まり文句の func_qmode などを削除。動作は変えない）
########################################
セグメントの値や統計量を、上位 DB（PostgreSQL）のテーブルへ登録する出口

主要関数:
a_insert_to_pg_trigger      # セグメントの全行を PostgreSQL のテーブルへ INSERT する。
a_calc_stats_to_pg_trigger  # セグメントの統計量を求め、PostgreSQL のテーブルへ INSERT する。

位置づけ: エッジで集めたデータを上位側の DB に蓄積する。DB の接続・テーブル管理の部品は tri_s19_rdb_postgres、統計量の算出は tri_s10_stream_wave_stat を使う。
設計意図: 接続時にテーブルを自動で作成し、既存のテーブルとの構成の食い違いを確かめて、配置時の設定ミスを起動時に見つける。登録しない列・JSONB の列・UTC から日本時間に変える列・固定値を入れる列を設定で指定し、DB 側のテーブル設計に合わせられるようにする。接続に失敗したり途中で切れたりしたら、つなぎ直してセグメントを書き直し（リトライ）、続けて失敗したら接続できない（not_connectable）とする。接続できない状態の記録・再接続の間隔は tri_s01_connect_state に任せる。
前提・制約: 接続情報は環境変数（PG_HOST / PG_PORT / PG_DB / PG_USER / PG_PASS）で渡す。
"""
MODULE_NAME = "ACT_RDB"

"""standard library"""
from   datetime import datetime as dt
from   datetime import timezone, timedelta
import json
import os
import queue
import sys
import time
import traceback
from   typing   import Any, Dict, Sequence, Optional

"""external library"""
## pg8000（PostgreSQL）は tri_s19_rdb_postgres が使う


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
from   src.tri_s01_error_if import ErrorIF, SIGNAL_STOP
import src.it_rdb.tri_s19_rdb_postgres as rdb
from   src.tri_s01_connect_state import ConnectState
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
    - 接続の失敗（つながらない・途中で切れた）は retry_wait_sec ごとにつなぎ直してセグメントを書き直す（リトライ）
        連続 retry_max 回で not_connectable と判定して ConnectState へ報告し、reconnect_interval_sec ごとに再接続する
        接続できない間に届いたセグメントは not_connectable_action（skip / wait）に従う。1行だけの INSERT エラーは、その行を飛ばす
        テーブルの作成・構成の確認は、初めてつながったときに行う（起動時につながらなくても止まらない）
    - hide_col_loc / jsonb_col_loc / utc_to_jst_col_loc: 登録しない列 / jsonb にする列 / UTC→JST 変換する列
        Find と同じ列番号のリスト。先頭の固定4列は "seg_id" / "seg_ind" / "seg_ts" / "seg_status" の名前で書く
        （既定: ["seg_ind", "seg_ts", "seg_status"] / [5] / [4]）
    - col_custom_value: 指定した列に、セグメントの値にかかわらず固定値・現在時刻を入れる（キーは Find と同じ列番号）
        {"<col>": {"src": "const", "value": 値}} / {"<col>": {"src": "now"}} / {"<col>": {"src": "now", "format": "%Y-%m-%d %H:%M:%S"}}
        現在時刻はセグメントごとに1回取得（全行で同じ）。format 省略時は datetime のまま（time 型の列向け）
        UTC→JST 変換の後に上書きする。hide_col_loc に含まれる列・範囲外・src の誤りは起動時エラー
    Mod: 2026.10.21    By: IWATA, Y.'s Agent AI（列の指定を Find と同じ列番号に変更）
    """

    TRIGGER_NAME = "a_pg_insert_imq_trigger"

    """======== LOOP INIT ========"""
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Before Loop Setting.", log_file)

        # データ項目定義
        n_col_def = len(col_def)
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_def={col_def}", log_file)

        # (テーブル登録対象を選定、jsonb型変換対象を選定)
        # 列の指定は Find と同じ列番号（先頭の固定4列は "seg_id" 等の名前）。行の中の位置に変換する
        hide_col_loc, jsonb_col_loc, utc_to_jst_col_loc = rdb.pg_resolve_col_locs(config, col_def)
        base.output_log(MODULE_NAME, f"hide_col_loc={hide_col_loc}  jsonb_col_loc={jsonb_col_loc}  utc_to_jst_col_loc={utc_to_jst_col_loc}  (行の中の位置)", log_file)

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

        # (指定した列に固定値・現在時刻を入れる。キーは Find と同じ列番号、値は登録列の位置に対応付ける)
        col_custom_map = rdb.pg_parse_col_custom_value(config, col_def, hide_col_loc)
        base.output_log(MODULE_NAME, f"col_custom_value (登録列の位置): {col_custom_map}", log_file)

        # 接続設定
        config = rdb.pg_merge_env(config)
        pg_schema = config.get("pg_schema", "public")
        pg_table  = config.get("pg_table" , "")
        if not pg_table:
            raise ValueError("config['pg_table'] is NOT Set.")

        retry_max              = int(config.get("retry_max", 3))
        retry_wait_sec         = float(config.get("retry_wait_sec", 5.0))
        reconnect_interval_sec = float(config.get("reconnect_interval_sec", 10.0))
        if retry_max < 1 or retry_wait_sec < 0:
            raise ValueError(f"config['retry_max'] must be >= 1 and config['retry_wait_sec'] >= 0. Got: {retry_max} / {retry_wait_sec}")
        pg_target = f"{config.get('pg_host')}:{config.get('pg_port')}/{config.get('pg_dbname')}"
        not_connectable_action = str(config.get("not_connectable_action", "skip")).strip().lower()
        if not_connectable_action not in ("skip", "wait"):
            raise ValueError(f"config['not_connectable_action'] must be 'skip' / 'wait'. Got: {not_connectable_action!r}")
        exclude_last_row  = bool(config.get("exclude_last_row" , True))

        # INSERT SQL 生成
        sql_insert = rdb.pg_make_insert_sql(pg_schema, pg_table, col_def_selected)
        base.output_log(MODULE_NAME, f"INSERT SQL: {sql_insert}", log_file)

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} pg={pg_target}  retry_max={retry_max}  retry_wait_sec={retry_wait_sec}  "
            f"reconnect_interval_sec={reconnect_interval_sec}" + f"  not_connectable_action={not_connectable_action}", log_file)

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

    """======== 接続の状態（判定・リトライはこのトリガー。記録・再接続の間隔・通知は ConnectState） ========"""
    conn_pg    = None
    conn_state = ConnectState(TRIGGER_NAME, pg_target, reconnect_interval_sec, MODULE_NAME, log_file)
    fail_count = 0      # 連続失敗の回数（判定に使う。このトリガーの中だけで数える）
    table_ready = False                          # テーブルの作成・構成の確認が済んだか（初めてつながったときに行う）
    fatal_error: Optional[BaseException] = None  # テーブルの準備の失敗（接続は生きている。起動時の設定の誤りと同じく止める）

    def _responsive_wait(seconds: float) -> None:
        """stop_event を見ながら細かく待つ"""
        _end = time.time() + seconds
        while time.time() < _end and not stop_event.is_set():
            time.sleep(min(0.05, max(0.0, _end - time.time())))

    def _drop_pg() -> None:
        """接続を閉じる"""
        nonlocal conn_pg
        if conn_pg is not None:
            try: conn_pg.close()
            except Exception: pass
        conn_pg = None

    def _conn_lost() -> bool:
        """失敗のあと、接続が切れたかを確かめる（ロールバックしてから SELECT 1）。切れていれば True"""
        if conn_pg is None:
            return True
        try:
            conn_pg.rollback()
        except Exception:
            return True
        return not rdb.pg_ping(conn_pg)

    def _prepare_table() -> None:
        """テーブルを作成し（無ければ）、構成を確かめる"""
        nonlocal table_ready
        rdb.pg_ensure_table(conn_pg, pg_schema, pg_table, col_def_selected, log_file)   # テーブル自動作成（存在しなければ）
        schema_errors = rdb.pg_check_schema(conn_pg, pg_schema, pg_table, col_def_selected, log_file)  # スキーマ整合チェック（既存テーブルも含め必ず実行）
        if schema_errors:
            raise ValueError(
                f"DB Schema Invalid."
                f'  table="{pg_schema}"."{pg_table}"'
                f"  errors={schema_errors}"
            )
        table_ready = True

    def _try_pg() -> Optional[BaseException]:
        """
        接続を確かめ、切れていればつなぎ直す（1回）。初めてつながったときはテーブルを準備する。成功なら None、失敗ならその例外
        （テーブルの準備が、接続は生きているのに失敗したときは fatal_error に残す）
        """
        nonlocal conn_pg, fatal_error
        try:
            conn_pg = rdb.pg_connect_if_needed(conn_pg, config, TRIGGER_NAME, log_file)
            if not table_ready:
                _prepare_table()
            return None
        except Exception as e:
            if conn_pg is not None and not _conn_lost():
                fatal_error = e
            _drop_pg()
            return e

    def _count_fail(e: BaseException, what: str) -> bool:
        """
        接続の失敗を1回数える。連続 retry_max 回で not_connectable と判定して ConnectState へ報告し、False を返す
        （続けるなら retry_wait_sec 待って True。リトライは画面のみに出し、ConnectState・ErrorIF へは報告しない）
        """
        nonlocal fail_count
        _drop_pg()
        fail_count += 1
        if fail_count >= retry_max:
            conn_state.set_not_connectable(e)   # 判定：連続 retry_max 回の失敗
            return False
        if stop_event.is_set():
            return False
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Retry ({fail_count}/{retry_max}). {what}  {pg_target}  ({type(e).__name__}: {e})")
        _responsive_wait(retry_wait_sec)
        return True

    def _run_with_retry(work, what: str) -> bool:
        """
        work() を実行する。接続の失敗（つながらない・途中で切れた）は retry_wait_sec ごとにつなぎ直して、最初からやり直す（リトライ）。
        接続は生きているのに失敗したとき（データのエラー）は、例外のまま返す。not_connectable になったら False
        """
        nonlocal fail_count
        while True:
            e = _try_pg()
            if fatal_error is not None:
                return False
            if e is None:
                try:
                    work()
                    fail_count = 0
                    return True
                except Exception as ex:
                    if not _conn_lost():
                        raise
                    e = ex
            if not _count_fail(e, what):
                return False

    def _reconnect_if_due() -> None:
        """接続できない間、再接続の間隔が過ぎていれば再接続を試み、結果を ConnectState へ報告する"""
        nonlocal fail_count
        if conn_state.should_try_reconnect():
            e = _try_pg()
            if fatal_error is not None:
                return
            if e is None:
                fail_count = 0
                conn_state.set_connectable()
            else:
                conn_state.reconnect_failed(e)

    """初回接続（retry_wait_sec ごとに retry_max 回まで試みる）"""
    _err: Optional[BaseException] = None
    for _try in range(1, retry_max + 1):
        _err = _try_pg()
        if _err is None or fatal_error is not None:
            break
        if _try < retry_max and not stop_event.is_set():
            base.output_log(MODULE_NAME,       # リトライは画面のみ
                f"{TRIGGER_NAME} Connect Retry ({_try}/{retry_max}). {pg_target}  ({type(_err).__name__}: {_err})")
            _responsive_wait(retry_wait_sec)
    if fatal_error is not None:
        ErrorIF.report_exception(fatal_error, phase="startup", category="config",
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        _drop_pg()
        return None
    if _err is not None:
        _sig = ErrorIF.report_exception(_err, phase="startup", category="comm",  # 初回接続の失敗を報告（A-21）
                                         trigger_id=TRIGGER_NAME, log_file=log_file)
        if _sig == SIGNAL_STOP:
            stop_event.set()
            return None
        conn_state.set_not_connectable(_err, report=False)   # ErrorIF へは上で phase=startup として通知済み

    skipped_seg_count = 0

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
            _reconnect_if_due()   # 接続できない間は再接続を試みる（間隔は ConnectState）
            if fatal_error is not None:
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

            """==== 接続できない間は、再接続の間隔ごとに再接続を試みる（セグメントが途切れず届く間も再接続できるように） ===="""
            _reconnect_if_due()
            if fatal_error is not None:
                break

            """==== 接続できない間に届いたセグメントの処理（not_connectable_action） ===="""
            if not conn_state.is_connectable():
                if not_connectable_action == "skip":
                    skipped_seg_count += 1
                    base.output_log(MODULE_NAME,
                        f"WARNING  NOT CONNECTABLE - Segment Skipped. "
                        f"seg_id={seg_id}  total_skipped={skipped_seg_count}  {pg_target}", log_file)
                    continue
                base.output_log(MODULE_NAME,
                    f"{TRIGGER_NAME} NOT CONNECTABLE - Waiting for reconnect... seg_id={seg_id}", log_file)
                while not conn_state.is_connectable() and not stop_event.is_set() and not (fatal_error is not None):
                    _reconnect_if_due()
                    if not conn_state.is_connectable():
                        _responsive_wait(min(1.0, reconnect_interval_sec))
                if stop_event.is_set() or fatal_error is not None:
                    break

            """セグメント単位 PostgreSQL 書込み（接続の失敗はリトライ）"""
            def _insert_segment() -> None:
                # レコード単位 INSERT
                n_ok  = 0
                n_err = 0
                cur = conn_pg.cursor()
                try:
                    _now = dt.now()  # col_custom_value の現在時刻（セグメント内の全行で同じ時刻）
                    for row_num, row_val in enumerate(segment, start=1):
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
                            # 接続が切れていればロールバックも失敗するので、そのまま上へ（セグメントを書き直す）
                            conn_pg.rollback()
                            _msg = f"Record INSERT Error: seg_id={seg_id}  row={row_num}  {e_rec}"
                            if n_err < _ERR_VALUES_MAX_ROWS:
                                _msg += f"\n    values: {_format_row_values(col_def_selected, row_val_selected)}"
                            base.output_log(MODULE_NAME, _msg, log_file)
                            n_err += 1
                finally:
                    try: cur.close()
                    except Exception: pass

                # セグメント全体コミット
                conn_pg.commit()
                base.output_log(MODULE_NAME, f"PostgreSQL INSERT 完了: seg_id={seg_id}  ok={n_ok}  err={n_err}")

            try:
                written = _run_with_retry(_insert_segment, f"seg_id={seg_id}")
            except Exception as e_seg:
                base.output_log(MODULE_NAME,
                    f"Segment Error: seg_id={seg_id}  {e_seg}", log_file)
                raise
            if fatal_error is not None:
                break
            if not written:
                base.output_log(MODULE_NAME,
                    f"WARNING  {TRIGGER_NAME} Segment NOT Written (not connectable). seg_id={seg_id}", log_file)

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
    if fatal_error is not None:
        ErrorIF.report_exception(fatal_error, phase="loop", category="config",   # テーブルの準備の失敗（設定の誤り）→ 止める
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
    if skipped_seg_count > 0:
        base.output_log(MODULE_NAME,
            f"WARNING  {TRIGGER_NAME} Stopped with {skipped_seg_count} segment(s) skipped (NOT CONNECTABLE).", log_file)

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
        - 接続の失敗は retry_wait_sec ごとにつなぎ直して書き直す（a_insert_to_pg_trigger と同様。連続 retry_max 回で not_connectable）

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
        retry_max / retry_wait_sec : リトライの回数・間隔（既定 3 / 5.0）
        reconnect_interval_sec     : 接続できない間の再接続の間隔（既定 10.0）
        not_connectable_action     : 接続できない間に届いたセグメント（skip / wait。既定 skip）
        exclude_last_row           : セグメント終点行（変化後の行）を集計対象から除外するか（デフォルト: true）
        upsert                     : true の場合 ON CONFLICT(seg_id) DO UPDATE で上書き（デフォルト: false）
    """

    TRIGGER_NAME = "a_calc_stats_to_pg_trigger"

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

        retry_max              = int(config.get("retry_max", 3))
        retry_wait_sec         = float(config.get("retry_wait_sec", 5.0))
        reconnect_interval_sec = float(config.get("reconnect_interval_sec", 10.0))
        if retry_max < 1 or retry_wait_sec < 0:
            raise ValueError(f"config['retry_max'] must be >= 1 and config['retry_wait_sec'] >= 0. Got: {retry_max} / {retry_wait_sec}")
        pg_target = f"{config.get('pg_host')}:{config.get('pg_port')}/{config.get('pg_dbname')}"
        not_connectable_action = str(config.get("not_connectable_action", "skip")).strip().lower()
        if not_connectable_action not in ("skip", "wait"):
            raise ValueError(f"config['not_connectable_action'] must be 'skip' / 'wait'. Got: {not_connectable_action!r}")
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

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} pg={pg_target}  retry_max={retry_max}  retry_wait_sec={retry_wait_sec}  "
            f"reconnect_interval_sec={reconnect_interval_sec}" + f"  not_connectable_action={not_connectable_action}", log_file)

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

    """======== 接続の状態（判定・リトライはこのトリガー。記録・再接続の間隔・通知は ConnectState） ========"""
    conn_pg    = None
    conn_state = ConnectState(TRIGGER_NAME, pg_target, reconnect_interval_sec, MODULE_NAME, log_file)
    fail_count = 0      # 連続失敗の回数（判定に使う。このトリガーの中だけで数える）
    table_ready = False                          # テーブルの作成・構成の確認が済んだか（初めてつながったときに行う）
    fatal_error: Optional[BaseException] = None  # テーブルの準備の失敗（接続は生きている。起動時の設定の誤りと同じく止める）

    def _responsive_wait(seconds: float) -> None:
        """stop_event を見ながら細かく待つ"""
        _end = time.time() + seconds
        while time.time() < _end and not stop_event.is_set():
            time.sleep(min(0.05, max(0.0, _end - time.time())))

    def _drop_pg() -> None:
        """接続を閉じる"""
        nonlocal conn_pg
        if conn_pg is not None:
            try: conn_pg.close()
            except Exception: pass
        conn_pg = None

    def _conn_lost() -> bool:
        """失敗のあと、接続が切れたかを確かめる（ロールバックしてから SELECT 1）。切れていれば True"""
        if conn_pg is None:
            return True
        try:
            conn_pg.rollback()
        except Exception:
            return True
        return not rdb.pg_ping(conn_pg)

    def _prepare_table() -> None:
        """テーブルを作成し（無ければ）、構成を確かめる"""
        nonlocal table_ready
        rdb.pg_ensure_table(conn_pg, pg_schema, pg_table, col_def_selected, log_file)   # テーブル自動作成（存在しなければ）
        schema_errors = rdb.pg_check_schema(conn_pg, pg_schema, pg_table, col_def_selected, log_file)  # スキーマ整合チェック（既存テーブルも含め必ず実行）
        if schema_errors:
            raise ValueError(
                f"DB Schema Invalid."
                f'  table="{pg_schema}"."{pg_table}"'
                f"  errors={schema_errors}"
            )
        table_ready = True

    def _try_pg() -> Optional[BaseException]:
        """
        接続を確かめ、切れていればつなぎ直す（1回）。初めてつながったときはテーブルを準備する。成功なら None、失敗ならその例外
        （テーブルの準備が、接続は生きているのに失敗したときは fatal_error に残す）
        """
        nonlocal conn_pg, fatal_error
        try:
            conn_pg = rdb.pg_connect_if_needed(conn_pg, config, TRIGGER_NAME, log_file)
            if not table_ready:
                _prepare_table()
            return None
        except Exception as e:
            if conn_pg is not None and not _conn_lost():
                fatal_error = e
            _drop_pg()
            return e

    def _count_fail(e: BaseException, what: str) -> bool:
        """
        接続の失敗を1回数える。連続 retry_max 回で not_connectable と判定して ConnectState へ報告し、False を返す
        （続けるなら retry_wait_sec 待って True。リトライは画面のみに出し、ConnectState・ErrorIF へは報告しない）
        """
        nonlocal fail_count
        _drop_pg()
        fail_count += 1
        if fail_count >= retry_max:
            conn_state.set_not_connectable(e)   # 判定：連続 retry_max 回の失敗
            return False
        if stop_event.is_set():
            return False
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Retry ({fail_count}/{retry_max}). {what}  {pg_target}  ({type(e).__name__}: {e})")
        _responsive_wait(retry_wait_sec)
        return True

    def _run_with_retry(work, what: str) -> bool:
        """
        work() を実行する。接続の失敗（つながらない・途中で切れた）は retry_wait_sec ごとにつなぎ直して、最初からやり直す（リトライ）。
        接続は生きているのに失敗したとき（データのエラー）は、例外のまま返す。not_connectable になったら False
        """
        nonlocal fail_count
        while True:
            e = _try_pg()
            if fatal_error is not None:
                return False
            if e is None:
                try:
                    work()
                    fail_count = 0
                    return True
                except Exception as ex:
                    if not _conn_lost():
                        raise
                    e = ex
            if not _count_fail(e, what):
                return False

    def _reconnect_if_due() -> None:
        """接続できない間、再接続の間隔が過ぎていれば再接続を試み、結果を ConnectState へ報告する"""
        nonlocal fail_count
        if conn_state.should_try_reconnect():
            e = _try_pg()
            if fatal_error is not None:
                return
            if e is None:
                fail_count = 0
                conn_state.set_connectable()
            else:
                conn_state.reconnect_failed(e)

    """初回接続（retry_wait_sec ごとに retry_max 回まで試みる）"""
    _err: Optional[BaseException] = None
    for _try in range(1, retry_max + 1):
        _err = _try_pg()
        if _err is None or fatal_error is not None:
            break
        if _try < retry_max and not stop_event.is_set():
            base.output_log(MODULE_NAME,       # リトライは画面のみ
                f"{TRIGGER_NAME} Connect Retry ({_try}/{retry_max}). {pg_target}  ({type(_err).__name__}: {_err})")
            _responsive_wait(retry_wait_sec)
    if fatal_error is not None:
        ErrorIF.report_exception(fatal_error, phase="startup", category="config",
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        _drop_pg()
        return None
    if _err is not None:
        _sig = ErrorIF.report_exception(_err, phase="startup", category="comm",  # 初回接続の失敗を報告（A-21）
                                         trigger_id=TRIGGER_NAME, log_file=log_file)
        if _sig == SIGNAL_STOP:
            stop_event.set()
            return None
        conn_state.set_not_connectable(_err, report=False)   # ErrorIF へは上で phase=startup として通知済み

    skipped_seg_count = 0

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
            _reconnect_if_due()   # 接続できない間は再接続を試みる（間隔は ConnectState）
            if fatal_error is not None:
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

            """==== 接続できない間は、再接続の間隔ごとに再接続を試みる（セグメントが途切れず届く間も再接続できるように） ===="""
            _reconnect_if_due()
            if fatal_error is not None:
                break

            """==== 接続できない間に届いたセグメントの処理（not_connectable_action） ===="""
            if not conn_state.is_connectable():
                if not_connectable_action == "skip":
                    skipped_seg_count += 1
                    base.output_log(MODULE_NAME,
                        f"WARNING  NOT CONNECTABLE - Segment Skipped. "
                        f"seg_id={seg_id}  total_skipped={skipped_seg_count}  {pg_target}", log_file)
                    continue
                base.output_log(MODULE_NAME,
                    f"{TRIGGER_NAME} NOT CONNECTABLE - Waiting for reconnect... seg_id={seg_id}", log_file)
                while not conn_state.is_connectable() and not stop_event.is_set() and not (fatal_error is not None):
                    _reconnect_if_due()
                    if not conn_state.is_connectable():
                        _responsive_wait(min(1.0, reconnect_interval_sec))
                if stop_event.is_set() or fatal_error is not None:
                    break

            """セグメント単位 PostgreSQL 書込み（接続の失敗はリトライ）"""
            # col_def_selected の列順に値を組み立て（build_statsで欠落したキーはNULL）
            row_val = tuple(
                stats_dict.get(col_def_selected[i][0]) for i in range(len(col_def_selected))
            )

            def _insert_stats() -> None:
                cur = conn_pg.cursor()
                try:
                    cur.execute(sql_insert, row_val)
                    conn_pg.commit()
                    base.output_log(MODULE_NAME,
                        f"PostgreSQL INSERT 完了: seg_id={seg_id}", log_file)
                finally:
                    try: cur.close()
                    except Exception: pass

            try:
                written = _run_with_retry(_insert_stats, f"seg_id={seg_id}")
            except Exception as e_ins:
                base.output_log(MODULE_NAME,
                    f"Stats INSERT Error: seg_id={seg_id}  {e_ins}"
                    f"\n    values: {_format_row_values(col_def_selected, row_val)}", log_file)
                raise
            if fatal_error is not None:
                break
            if not written:
                base.output_log(MODULE_NAME,
                    f"WARNING  {TRIGGER_NAME} Segment NOT Written (not connectable). seg_id={seg_id}", log_file)

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
    if fatal_error is not None:
        ErrorIF.report_exception(fatal_error, phase="loop", category="config",   # テーブルの準備の失敗（設定の誤り）→ 止める
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
    if skipped_seg_count > 0:
        base.output_log(MODULE_NAME,
            f"WARNING  {TRIGGER_NAME} Stopped with {skipped_seg_count} segment(s) skipped (NOT CONNECTABLE).", log_file)

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
