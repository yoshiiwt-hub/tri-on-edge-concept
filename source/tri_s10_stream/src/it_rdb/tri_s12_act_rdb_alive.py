# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# TriEdge Act Trigger Module [RDB Alive]
# Ver: 01.a.20261008    By: IWATA, Y.'s Agent AI
# Mod: 新規作成（死活監視の結果を PostgreSQL の専用テーブルへ UPSERT し、状態が変わった機器をオンデマンド通知する）
########################################
a_upsert_alive_to_pg_trigger  # Find Alive（tri_s11_find_alive）の結果を、機器ごとに1行の専用テーブルへ UPSERT する Act Trigger

動作概要
    1. 起動時、専用テーブルが無ければ作成する（列コメントも登録）
       既にあれば、列の名前・型と、device_key 単独の主キー／ユニーク制約を確認する（無ければ起動時エラー。ALTER はしない）
    2. 起動時にテーブルから各機器の前回の状態（status）を読み、以降はメモリに持つ
    3. セグメント（1周期の全機器）ごとに INSERT ... ON CONFLICT (device_key) DO UPDATE で更新し、1回コミットする
    4. 状態が変わった機器（OK → NG、NG → OK）があれば、オンデマンド通知（Teams）を要求する
       前回の状態が無い機器（初回）は NG のときだけ通知する
    5. 運用系メトリクス（NG 台数・NG の機器・状態変化の回数等）を更新する
    ※ 履歴は持たない（機器ごとに最新の1行）

専用テーブル（列は固定）
    device_key  TEXT PRIMARY KEY  機器の識別子
    device_name TEXT              機器名
    status      TEXT              OK / NG
    checked_at  TIMESTAMP         確認した日時
    host        TEXT              接続先
    port        INTEGER           ポート
    rtt_ms      REAL              接続にかかった時間 ミリ秒（NG のときは NULL）
    detail      TEXT              判定の詳細
    updated_at  TIMESTAMP         テーブルを更新した日時

設定（act_trigger）
    pg_schema         : スキーマ名（デフォルト: "public"）
    pg_table          : テーブル名（デフォルト: "triedge_device_alive"）
    pg_reconnect_max  : 再接続の回数（デフォルト: 3）
    pg_reconnect_wait : 再接続の間隔 秒（デフォルト: 5.0）
    teams_notify      : 状態が変わったときにオンデマンド通知を要求する（デフォルト: true）
    notify_time_format: 通知文の日時の書式（デフォルト: "%Y-%m-%d %H:%M:%S"）
    pg_host 等の接続情報は他の rdb Act と同じ（環境変数で上書き可）

受け取る列（column + segment_header の列名で値を取り出す。device_key・status は必須、ほかは無ければ NULL）
    device_key, device_name, status, checked_at, host, port, rtt_ms, detail
"""
MODULE_NAME = "ACT_RDB_Alive"

"""standard library"""
from   datetime import datetime as dt
import os
import queue
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Optional, Sequence, Tuple

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
from   src.tri_s01_error_if import ErrorIF, SIGNAL_STOP
import src.it_rdb.tri_s19_rdb_postgres as rdb

"""system dir, file"""
system_root = base.get_system_root()
log_dir  = base.get_log_dir()
log_file = base.get_log_path()
work_dir = base.get_work_dir()

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# Alive Table Definition
########################################

ALIVE_TABLE_DEFAULT = "triedge_device_alive"
SEPARATOR = "____"

# 専用テーブルの列（列名, DDL型, col_def 型（pg_check_schema 用）, 列コメント）
ALIVE_TABLE_COLUMNS: List[Tuple[str, str, str, str]] = [
    ("device_key" , "TEXT PRIMARY KEY", "str"  , "機器の識別子（Find Alive の targets の key）"),
    ("device_name", "TEXT"            , "str"  , "機器名"),
    ("status"     , "TEXT"            , "str"  , "死活の状態（OK / NG）"),
    ("checked_at" , "TIMESTAMP"       , "time" , "確認した日時"),
    ("host"       , "TEXT"            , "str"  , "接続先（IP アドレスまたはホスト名）"),
    ("port"       , "INTEGER"         , "int"  , "TCP ポート"),
    ("rtt_ms"     , "REAL"            , "float", "接続にかかった時間 ミリ秒（NG のときは NULL）"),
    ("detail"     , "TEXT"            , "str"  , "判定の詳細（connected / refused / timeout 等）"),
    ("updated_at" , "TIMESTAMP"       , "time" , "テーブルを更新した日時"),
]

# セグメントから受け取る列（updated_at は Act が付ける）
ALIVE_INPUT_COLUMNS = [c[0] for c in ALIVE_TABLE_COLUMNS if c[0] != "updated_at"]
ALIVE_INPUT_REQUIRED = ("device_key", "status")


def alive_col_def() -> Dict[int, List[str]]:
    """専用テーブルの列を pg_check_schema 用の col_def 形式で返す"""
    return {i: [c[0], c[2]] for i, c in enumerate(ALIVE_TABLE_COLUMNS)}


def make_alive_create_sqls(schema: str, table: str) -> List[str]:
    """
    専用テーブルの CREATE TABLE と COMMENT ON COLUMN の SQL を返す
    Mod: 2026.10.08    By: IWATA, Y.'s Agent AI
    """
    col_ddl = ",\n".join(f'  "{c[0]}" {c[1]}' for c in ALIVE_TABLE_COLUMNS)
    sqls = [f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (\n{col_ddl}\n)']
    for c in ALIVE_TABLE_COLUMNS:
        comment = c[3].replace("'", "''")
        sqls.append(f'COMMENT ON COLUMN "{schema}"."{table}"."{c[0]}" IS \'{comment}\'')
    return sqls


def make_alive_upsert_sql(schema: str, table: str) -> str:
    """
    device_key をキーにした UPSERT（INSERT ... ON CONFLICT (device_key) DO UPDATE）の SQL を返す
    device_key 以外の列（device_name を含む）はすべて新しい値で上書きする
    Mod: 2026.10.08    By: IWATA, Y.'s Agent AI
    """
    names = [c[0] for c in ALIVE_TABLE_COLUMNS]
    cols_str = ", ".join(f'"{n}"' for n in names)
    vals_str = ", ".join(["%s"] * len(names))
    set_str  = ", ".join(f'"{n}" = EXCLUDED."{n}"' for n in names if n != "device_key")
    return (f'INSERT INTO "{schema}"."{table}" ({cols_str}) VALUES ({vals_str})'
            f' ON CONFLICT ("device_key") DO UPDATE SET {set_str}')


def parse_alive_input_columns(col_def: Dict[int, Any]) -> Dict[str, int]:
    """
    受け取る列の名前 → 位置（col_def のインデックス）を返す（起動時に1回）
    device_key・status が無い場合は ValueError（ほかの列は無ければ NULL を入れる）
    Mod: 2026.10.08    By: IWATA, Y.'s Agent AI
    """
    loc: Dict[str, int] = {}
    for k in sorted(col_def.keys()):
        name = str(col_def[k][0])
        if name in ALIVE_INPUT_COLUMNS and name not in loc:
            loc[name] = int(k)
    missing = [n for n in ALIVE_INPUT_REQUIRED if n not in loc]
    if missing:
        raise ValueError(f"column に必須の列がありません: {missing}  （受け取る列: {ALIVE_INPUT_COLUMNS}）")
    return loc


def pg_table_exists(conn, schema: str, table: str) -> bool:
    """テーブルがあれば True"""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables"
            " WHERE table_schema = %s AND table_name = %s",
            (schema, table))
        return cur.fetchone()[0] > 0
    finally:
        cur.close()


def pg_has_unique_on(conn, schema: str, table: str, column: str) -> bool:
    """
    column 単独の主キー／ユニーク制約（またはユニークインデックス。部分・式インデックスは除く）があれば True
    ON CONFLICT (column) が使えるかの確認に使う
    Mod: 2026.10.08    By: IWATA, Y.'s Agent AI
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT a.attname"
            " FROM pg_index i"
            " JOIN pg_class t     ON t.oid = i.indrelid"
            " JOIN pg_namespace n ON n.oid = t.relnamespace"
            " JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)"
            " WHERE n.nspname = %s AND t.relname = %s"
            "   AND i.indisunique AND i.indnatts = 1"
            "   AND i.indpred IS NULL AND i.indexprs IS NULL",
            (schema, table))
        rows = cur.fetchall()
    finally:
        cur.close()
    return any(str(r[0]) == column for r in rows)


def pg_ensure_alive_table(conn, schema: str, table: str, log_file: str) -> bool:
    """
    専用テーブルが無ければ作成し（列コメントも登録）、あれば列と device_key のユニーク制約を確認する
    確認で誤りがあれば ValueError（既存テーブルは変更しない）
    Mod: 2026.10.08    By: IWATA, Y.'s Agent AI
    Returns: True: 新規作成 / False: 既存
    """
    if not pg_table_exists(conn, schema, table):
        cur = conn.cursor()
        try:
            for sql in make_alive_create_sqls(schema, table):
                cur.execute(sql)
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            cur.close()
        base.output_log(MODULE_NAME, f'死活監視テーブルを新規作成しました。 "{schema}"."{table}"', log_file)
        return True

    base.output_log(MODULE_NAME, f'死活監視テーブルは既に存在します。 "{schema}"."{table}"', log_file)
    errors = rdb.pg_check_schema(conn, schema, table, alive_col_def(), log_file)
    if not pg_has_unique_on(conn, schema, table, "device_key"):
        errors.append('device_key 単独の主キー（PRIMARY KEY）またはユニーク制約（UNIQUE）がありません'
                      '（UPSERT に必要。テーブルは変更しないため、制約を追加するか別のテーブル名を指定してください）')
    if errors:
        raise ValueError(f'死活監視テーブルの定義が合いません。 table="{schema}"."{table}"  errors={errors}')
    return False


def pg_load_alive_states(conn, schema: str, table: str) -> Dict[str, str]:
    """テーブルから各機器の前回の状態を {device_key: status} で読む（起動時に1回）"""
    cur = conn.cursor()
    try:
        cur.execute(f'SELECT "device_key", "status" FROM "{schema}"."{table}"')
        return {str(r[0]): (None if r[1] is None else str(r[1])) for r in cur.fetchall()}
    finally:
        cur.close()


def make_alive_record(row: Sequence[Any], loc: Dict[str, int], now: dt) -> Optional[Dict[str, Any]]:
    """
    セグメントの1行から、テーブルに入れる値の dict を作る（区切り行・device_key が空の行は None）
    Mod: 2026.10.08    By: IWATA, Y.'s Agent AI
    """
    def _get(name: str) -> Any:
        i = loc.get(name)
        if i is None or i >= len(row):
            return None
        v = row[i]
        return None if v == SEPARATOR else v

    key = _get("device_key")
    if key is None or str(key).strip() == "":
        return None

    checked_at = _get("checked_at")
    if isinstance(checked_at, str):
        try:
            checked_at = dt.fromisoformat(checked_at)
        except ValueError:
            checked_at = None

    port = _get("port")
    try:
        port = None if port is None else int(port)
    except (TypeError, ValueError):
        port = None

    rtt_ms = _get("rtt_ms")
    try:
        rtt_ms = None if rtt_ms is None else float(rtt_ms)
    except (TypeError, ValueError):
        rtt_ms = None

    status = _get("status")
    return {
        "device_key" : str(key),
        "device_name": None if _get("device_name") is None else str(_get("device_name")),
        "status"     : None if status is None else str(status),
        "checked_at" : checked_at,
        "host"       : None if _get("host") is None else str(_get("host")),
        "port"       : port,
        "rtt_ms"     : rtt_ms,
        "detail"     : None if _get("detail") is None else str(_get("detail")),
        "updated_at" : now,
    }


def alive_change_message(rec: Dict[str, Any], prev: Optional[str], time_format: str) -> Optional[str]:
    """
    状態が変わった機器の通知文を返す（変わっていなければ None）
    前回の状態が無い機器（初回）は NG のときだけ通知する
    例: 「機器３（dev03）：OK → NG（2026-10-08 10:00:00、timeout）」
    Mod: 2026.10.08    By: IWATA, Y.'s Agent AI
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
    """機器ごとの状態から運用系メトリクス（台数・NG 台数・NG の機器）を更新する"""
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


########################################
# Act Trigger Functions
########################################

def a_upsert_alive_to_pg_trigger(
    post_queue: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """
    Find Alive の結果（1周期 = 1セグメント）を専用テーブルへ UPSERT し、状態が変わった機器をオンデマンド通知する
    Mod: 2026.10.08    By: IWATA, Y.'s Agent AI
    """
    TRIGGER_NAME = "a_upsert_alive_to_pg_trigger"

    """======== LOOP INIT ========"""
    conn_pg = None
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Before Loop Setting.", log_file)

        # 受け取る列の位置
        loc = parse_alive_input_columns(col_def)
        base.output_log(MODULE_NAME, f"input columns (name -> loc): {loc}", log_file)

        # 接続設定
        config = rdb.pg_merge_env(config)
        pg_schema = str(config.get("pg_schema", "public"))
        pg_table  = str(config.get("pg_table" , ALIVE_TABLE_DEFAULT) or ALIVE_TABLE_DEFAULT)
        pg_reconnect_max  = int(config.get("pg_reconnect_max" , 3))
        pg_reconnect_wait = float(config.get("pg_reconnect_wait", 5.0))
        teams_notify = bool(config.get("teams_notify", True))
        notify_time_format = str(config.get("notify_time_format", "%Y-%m-%d %H:%M:%S"))

        sql_upsert = make_alive_upsert_sql(pg_schema, pg_table)
        base.output_log(MODULE_NAME, f"UPSERT SQL: {sql_upsert}", log_file)

        # PostgreSQL 初期接続、テーブル作成または確認
        conn_pg = rdb.pg_connect_if_needed(conn_pg, config, TRIGGER_NAME, log_file)
        pg_ensure_alive_table(conn_pg, pg_schema, pg_table, log_file)

        # 前回の状態（起動時に1回読み、以降はメモリに持つ）
        states: Dict[str, Optional[str]] = pg_load_alive_states(conn_pg, pg_schema, pg_table)
        names: Dict[str, str] = {}
        conn_pg.commit()
        base.output_log(MODULE_NAME, f"前回の状態を読み込みました。 {len(states)}台  {states}", log_file)

        # 運用系メトリクスの初期値
        base.metrics_add(MODULE_NAME, "total_segments", 0)
        base.metrics_add(MODULE_NAME, "state_changes", 0)
        base.metrics_set_last(MODULE_NAME, "last_seg_id", "-")
        _set_alive_metrics(states, names, None)

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Init OK."
            f'  table="{pg_schema}"."{pg_table}"  teams_notify={teams_notify}', log_file)

    except Exception as e:
        ErrorIF.report_exception(
            e, phase="startup", category="config",
            trigger_id=TRIGGER_NAME, log_file=log_file)
        if conn_pg is not None:
            try:
                conn_pg.close()
            except Exception:
                pass
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

        """セグメント処理"""
        try:
            if not segment:
                continue
            seg_id = segment[0][0]

            # 1. テーブルに入れる値（区切り行は除く。同じ機器が複数行あれば後の行を使う）
            now = dt.now()
            records: Dict[str, Dict[str, Any]] = {}
            for row in segment:
                rec = make_alive_record(row, loc, now)
                if rec is not None:
                    records[rec["device_key"]] = rec
            if not records:
                base.output_log(MODULE_NAME, f"seg_id={seg_id}  No Device Row. skipped.")
                continue

            # 2. PostgreSQL 接続確認・再接続
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

            # 3. 機器ごとに UPSERT（1行の失敗で他の行を失わないよう SAVEPOINT で区切る）し、セグメントで1回コミット
            n_ok, n_err = 0, 0
            done: List[Dict[str, Any]] = []
            cur = conn_pg.cursor()
            try:
                for rec in records.values():
                    cur.execute("SAVEPOINT triedge_alive_row")
                    try:
                        cur.execute(sql_upsert, tuple(rec[c[0]] for c in ALIVE_TABLE_COLUMNS))
                        cur.execute("RELEASE SAVEPOINT triedge_alive_row")
                        done.append(rec)
                        n_ok += 1
                    except Exception as e_rec:
                        cur.execute("ROLLBACK TO SAVEPOINT triedge_alive_row")
                        base.output_log(MODULE_NAME,
                            f"Record UPSERT Error: seg_id={seg_id}  device_key={rec['device_key']}  {e_rec}", log_file)
                        n_err += 1
                conn_pg.commit()
            except Exception:
                try:
                    conn_pg.rollback()
                except Exception:
                    pass
                raise
            finally:
                cur.close()
            base.output_log(MODULE_NAME, f"PostgreSQL UPSERT 完了: seg_id={seg_id}  ok={n_ok}  err={n_err}")

            # 4. 状態の変化（コミットできた行のみ）→ メモリの状態を更新し、変化があれば通知
            messages: List[str] = []
            for rec in done:
                msg = alive_change_message(rec, states.get(rec["device_key"]), notify_time_format)
                if msg:
                    messages.append(msg)
                states[rec["device_key"]] = rec["status"]
                if rec.get("device_name"):
                    names[rec["device_key"]] = rec["device_name"]

            # 5. 運用系メトリクス（通知は送信時点の metrics を載せるため、通知の要求より先に更新）
            base.metrics_add(MODULE_NAME, "total_segments", 1)
            base.metrics_set_last(MODULE_NAME, "last_seg_id", seg_id)
            _set_alive_metrics(states, names, max((r["checked_at"] for r in done if isinstance(r["checked_at"], dt)), default=None))
            if messages:
                base.metrics_add(MODULE_NAME, "state_changes", len(messages))
                for m in messages:
                    base.output_log(MODULE_NAME, f"状態変化: {m}", log_file)
                if teams_notify:
                    base.metrics_notify(MODULE_NAME, "\n\n".join(messages))

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

a_upsert_alive_to_pg_trigger.QMODE = "imq"
a_upsert_alive_to_pg_trigger.CUT_LAST_ROW = True  # 終点行（区切り行 '____'）は core で除外して受け取る
