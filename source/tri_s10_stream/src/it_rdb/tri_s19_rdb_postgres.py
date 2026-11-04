# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# PostgreSQL Client Utility Module
# Ver: 01.l.20261104    By: IWATA, Y.'s Agent AI
# Mod: pg_merge_env の docstring の既定値を実装に合わせて修正
########################################
PostgreSQL を使うトリガーが共通で使う、接続・取得・登録・テーブル管理の部品集

主要関数:
pg_merge_env / pg_connect_if_needed               # 接続情報を環境変数で補い、切れていれば接続し直す。
pg_fetch_diff                                     # キー列を条件に、前回以降に追加された行を取得する。
pg_make_insert_sql                                # col_def から INSERT 文を作る。
pg_ensure_table / pg_check_schema                 # テーブルを作成し、既存のテーブルと col_def の食い違いを調べる。
pg_resolve_col_locs / pg_parse_col_custom_value   # Act の列の指定を、登録する行の中の位置に変換する。

位置づけ: RDB 系のトリガー（tri_s11_find_rdb_postgres、tri_s12_act_rdb_postgres、tri_s12_act_rdb_alive）から呼ばれる。
設計意図: 純 Python の pg8000 を使い、コンパイル済みのドライバが使えない機器（32bit ARM の Linux 等）でも動くようにする。テーブルの自動作成と構成の照合で、配置時の設定ミスを起動時に見つける。長期の接続の管理は、ループを持つ呼び出し側が行う。
前提・制約: 接続情報は環境変数（PG_HOST / PG_PORT / PG_DB / PG_USER / PG_PASS）で渡す。設定に書いた値があれば、そちらを優先する。
"""

MODULE_NAME = "UTIL_RDB"

"""standard library"""
from   datetime import datetime as dt
import os
import sys
import traceback
from   typing import Any, Dict, List, Optional, Tuple

"""external library"""
import pg8000.dbapi as pg_dbapi   # PostgreSQL Adapter


"""[Bootstrap] Resolve Path and Import Base Module"""
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

"""system dir, file"""
system_root = base.get_system_root()
log_dir     = base.get_log_dir()
log_file    = base.get_log_path()

work_dir       = base.get_work_dir()
work_db_dir    = base.get_work_sub_dirs().get("db"   , os.path.join(work_dir, "db"))
work_run_dir   = base.get_work_sub_dirs().get("run"  , os.path.join(work_dir, "run"))
work_spool_dir = base.get_work_sub_dirs().get("spool", os.path.join(work_dir, "spool"))

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# 型変換テーブル
########################################

# col_def 型文字列 → PostgreSQL DDL型
COLDEF_TO_PG_DDL: Dict[str, str] = {
    "int"  : "INT",
    "float": "DOUBLE PRECISION",
    "str"  : "TEXT",
    "time" : "TIMESTAMP",
    "jsonb": "JSONB",
}

# col_def 型文字列 → information_schema.columns.data_type の許容値セット
COLDEF_TO_PG_INFO: Dict[str, set] = {
    "int"  : {"integer", "bigint", "smallint"},
    "float": {"double precision", "real", "numeric", "float"},
    "str"  : {"text", "character varying", "character"},
    "time" : {"timestamp without time zone", "timestamp with time zone", "timestamp"},
    "jsonb": {"jsonb"},
}


########################################
# RDB Client Functions
########################################

def get_connection():
    """
    環境変数からpg8000接続を取得する（ショートライフ・単発用）
    ループ内で繰り返し使う場合は pg_connect_if_needed を使うこと
    """
    return pg_dbapi.connect(
        host     = os.getenv("PG_HOST", "localhost"),
        port     = int(os.getenv("PG_PORT", "5432")),
        database = os.getenv("PG_DB"  , "postgres"),
        user     = os.getenv("PG_USER", "postgres"),
        password = os.getenv("PG_PASS", "postgres"),
    )


def pg_merge_env(config: dict) -> dict:
    """
    config の未設定（キー不在 or 空文字）の pg_* キーを
    対応する環境変数（PG_*）で補完して返す。
    元の config は変更しない（コピーを返す）。

    対応マッピング:
        pg_host     ← PG_HOST  (環境変数も無い場合: 空文字)
        pg_port     ← PG_PORT  (環境変数も無い場合: "5432")
        pg_dbname   ← PG_DB    (環境変数も無い場合: 空文字)
        pg_user     ← PG_USER  (環境変数も無い場合: 空文字)
        pg_password ← PG_PASS  (環境変数も無い場合: 空文字)

    使用例（LOOP INIT の先頭）:
        config = rdb.pg_merge_env(config)
    """
    merged = dict(config)  # shallow copy（pg_* は文字列なので十分）

    _ENV_MAP = [
        ("pg_host"    , "PG_HOST", ""),
        ("pg_port"    , "PG_PORT", "5432"),
        ("pg_dbname"  , "PG_DB"  , ""),
        ("pg_user"    , "PG_USER", ""),
        ("pg_password", "PG_PASS", ""),
    ]
    for cfg_key, env_key, default in _ENV_MAP:
        if not merged.get(cfg_key):  # 未設定 or 空文字の場合のみ補完
            merged[cfg_key] = os.getenv(env_key, default)

    return merged


def pg_connect(config: dict):
    """
    configディクトからpg8000接続を取得して返す。
    失敗時は例外をそのまま raise する（呼び元でログ・対処）。
    """
    return pg_dbapi.connect(
        host     = config.get("pg_host"    , "localhost"),
        port     = int(config.get("pg_port", 5432)),
        database = config.get("pg_dbname"  , ""),
        user     = config.get("pg_user"    , ""),
        password = config.get("pg_password", ""),
        timeout  = int(config.get("pg_connect_timeout", 10)),
    )


def pg_ping(conn) -> bool:
    """
    接続が生きているか SELECT 1 で確認する。
    失敗（接続断・未接続）の場合は False を返す。
    """
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        return True
    except Exception:
        return False


def pg_connect_if_needed(
    conn,
    config: dict,
    caller_name: str,
    log_file: str,
):
    """
    接続がなければ（または ping NG なら）新規接続して返す。
    失敗時は例外をそのまま raise する（呼び元でログ・対処）。

    引数:
        conn        : 既存接続（None可）
        config      : pg接続情報ディクト
        caller_name : ログ識別用の呼び元名称
        log_file    : ログ出力先
    """
    need_connect = (
        conn is None or
        not pg_ping(conn)
    )
    if not need_connect:
        return conn

    base.output_log(MODULE_NAME,
        f"{caller_name} PostgreSQL 接続開始: "
        f"{config.get('pg_user')}@{config.get('pg_host')}:{config.get('pg_port')}"
        f"/{config.get('pg_dbname')}", log_file)

    conn = pg_connect(config)

    base.output_log(MODULE_NAME, f"{caller_name} PostgreSQL 接続完了.", log_file)
    return conn


def pg_fetch_diff(
    conn,
    sch_tab_pg: str,
    key_pg: str,
    last_ts: Any,
    limit: int,
) -> List[Dict[str, Any]]:
    """
    差分データを取得してdictのリストで返す（長期接続・ループ内用）。
    接続管理は呼び元が担う。

    引数:
        conn       : pg8000接続オブジェクト
        sch_tab_pg : '"schema"."table"' 形式
        key_pg     : '"key_col"'        形式
        last_ts    : 前回の最終タイムスタンプ（str または datetime）
        limit      : 最大取得行数
    戻り値:
        [{"col": val, ...}, ...] / 0件の場合 []
    """
    sql = (
        f"SELECT * FROM {sch_tab_pg}"
        f" WHERE {key_pg} > %s"
        f" ORDER BY {key_pg} ASC"
        f" LIMIT %s"
    )
    cur = conn.cursor()
    try:
        cur.execute(sql, (last_ts, limit))
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()


def pg_make_insert_sql(
    schema: str,
    table: str,
    col_def: dict,
) -> str:
    """
    col_def から INSERT SQL 文字列を生成して返す。
    プレースホルダは pg8000 の %s 形式。

    引数:
        schema  : スキーマ名
        table   : テーブル名
        col_def : {0: [col_name, type, ...], 1: [...], ...} 形式
    """
    col_names = [f'"{col_def[i][0]}"' for i in range(len(col_def))]
    cols_str  = ", ".join(col_names)
    vals_str  = ", ".join(["%s"] * len(col_names))
    return f'INSERT INTO "{schema}"."{table}" ({cols_str}) VALUES ({vals_str})'


def pg_ensure_table(
    conn,
    schema: str,
    table: str,
    col_def: dict,
    log_file: str,
) -> bool:
    """
    テーブルが存在しなければ col_def の定義で CREATE する。
    既に存在する場合は何もしない（IF NOT EXISTS）。

    引数:
        conn     : pg8000接続オブジェクト
        schema   : スキーマ名
        table    : テーブル名
        col_def  : {0: [col_name, type, ...], 1: [...], ...} 形式
        log_file : ログ出力先
    戻り値:
        True: 新規作成 / False: 既存のため何もしなかった
    raises:
        ValueError : col_def に未知の型文字列が含まれる場合
        Exception  : CREATE TABLE 失敗
    """
    # 列定義DDL生成
    col_ddl_parts = []
    for i in range(len(col_def)):
        col_name = col_def[i][0]
        col_type_str = col_def[i][1]
        # seg_id はストリームコアが付与する連番列で、INT範囲超えを避けるため BIGINT に固定
        pg_type = COLDEF_TO_PG_DDL.get(col_type_str) if col_name != 'seg_id' else "BIGINT"
        if pg_type is None:
            raise ValueError(
                f"pg_ensure_table: col_def[{i}] に未知の型文字列 '{col_type_str}' が含まれています。"
                f"  有効値: {list(COLDEF_TO_PG_DDL.keys())}"
            )
        col_ddl_parts.append(f'  "{col_name}" {pg_type}')

    col_ddl = ",\n".join(col_ddl_parts)
    sql_create = (
        f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (\n'
        f'{col_ddl}\n'
        f')'
    )

    # 実行前にテーブル存在確認
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables"
            " WHERE table_schema = %s AND table_name = %s",
            (schema, table)
        )
        exists_before = (cur.fetchone()[0] > 0)

        cur.execute(sql_create)
        conn.commit()

    finally:
        cur.close()

    created = not exists_before
    if created:
        base.output_log(MODULE_NAME,
            f'pg_ensure_table: テーブルを新規作成しました。 "{schema}"."{table}"', log_file)
    else:
        base.output_log(MODULE_NAME,
            f'pg_ensure_table: テーブルは既に存在します。 "{schema}"."{table}"', log_file)

    return created


def pg_check_schema(
    conn,
    schema: str,
    table: str,
    col_def: dict,
    log_file: str,
) -> List[str]:
    """
    テーブルの列名・型を col_def と照合し、齟齬のエラーメッセージリストを返す。
    リストが空なら一致（正常）。

    チェック内容:
        1. 列数の一致
        2. 各 col_def 列がテーブルに存在するか（列名 / 大文字小文字区別なし）
        3. 各列の型が col_def と互換か

    引数:
        conn     : pg8000接続オブジェクト
        schema   : スキーマ名
        table    : テーブル名
        col_def  : {0: [col_name, type, ...], 1: [...], ...} 形式
        log_file : ログ出力先
    戻り値:
        errors: List[str]  空リスト = 問題なし
    """
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT column_name, data_type"
            " FROM information_schema.columns"
            " WHERE table_schema = %s AND table_name = %s"
            " ORDER BY ordinal_position",
            (schema, table)
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    # DBのカラム情報 {小文字列名: data_type文字列}
    db_cols = {row[0].lower(): row[1].lower() for row in rows}

    errors = []

    # 1. 列数チェック
    n_def = len(col_def)
    n_db  = len(db_cols)
    if n_def != n_db:
        errors.append(
            f"列数不一致: col_def={n_def}列  DB={n_db}列"
            f'  table="{schema}"."{table}"'
        )

    # 2 & 3. 各列の存在・型チェック
    for i in range(n_def):
        col_name     = col_def[i][0]
        col_type_str = col_def[i][1]
        col_name_lc  = col_name.lower()

        # 列の存在チェック
        if col_name_lc not in db_cols:
            errors.append(
                f"列が存在しない: col_def[{i}] '{col_name}'"
                f'  table="{schema}"."{table}"'
            )
            continue

        # 型の互換チェック
        compat_types = COLDEF_TO_PG_INFO.get(col_type_str)
        if compat_types is None:
            errors.append(
                f"col_def[{i}] '{col_name}' に未知の型文字列 '{col_type_str}'"
            )
            continue

        db_type = db_cols[col_name_lc]
        if db_type not in compat_types:
            errors.append(
                f"型不一致: col_def[{i}] '{col_name}'"
                f"  col_def型='{col_type_str}'  DB型='{db_type}'"
                f"  許容DB型={compat_types}"
            )

    if errors:
        base.output_log(MODULE_NAME,
            f'pg_check_schema: スキーマ不一致 {len(errors)}件 "{schema}"."{table}"', log_file)
        for e in errors:
            base.output_log(MODULE_NAME, f"  {e}", log_file)
    else:
        base.output_log(MODULE_NAME,
            f'pg_check_schema: スキーマ一致確認 OK  "{schema}"."{table}"  {n_def}列', log_file)

    return errors


def pg_resolve_col_locs(
    config: Dict[str, Any],
    col_def: Dict[int, Any],
) -> Tuple[List[int], List[int], List[int]]:
    """
    hide_col_loc / jsonb_col_loc / utc_to_jst_col_loc を読み、行の中の位置（固定4列 + Find の列）に変換して返す（起動時に1回）
    設定は Find と同じ列番号で書き、先頭の固定4列は "seg_id" / "seg_ind" / "seg_ts" / "seg_status" の名前で書く
      既定: hide_col_loc = ["seg_ind", "seg_ts", "seg_status"]、jsonb_col_loc = [5]、utc_to_jst_col_loc = [4]
    設定した値の範囲外・不正な名前は ValueError。既定値のうち Find の列数を超えるものは使わない（従来どおり無視する）
    Mod: 2026.10.21    By: IWATA, Y.'s Agent AI
    Returns: (hide_col_loc, jsonb_col_loc, utc_to_jst_col_loc)
    """
    defaults = {
        "hide_col_loc"      : ["seg_ind", "seg_ts", "seg_status"],
        "jsonb_col_loc"     : [5],
        "utc_to_jst_col_loc": [4],
    }
    n_find = len(col_def) - len(base.SEG_HEADER_COLS)
    result = []
    for key, default in defaults.items():
        if key in config:
            result.append(base.resolve_act_cols(config.get(key), col_def, key))
        else:
            result.append([base.resolve_act_col(v, col_def, key) for v in default
                           if isinstance(v, str) or v < n_find])
    return result[0], result[1], result[2]


def pg_parse_col_custom_value(
    config: Dict[str, Any],
    col_def: Dict[int, Any],
    hide_col_loc: List[int],
) -> Dict[int, Dict[str, Any]]:
    """
    col_custom_value（指定した列に、セグメントの値にかかわらず固定値または現在時刻を入れる設定）を検証し、
    キー（Find と同じ列番号。先頭の固定4列は "seg_id" 等の名前）を、登録する列（hide_col_loc を除いた並び）の位置に対応付けて返す（起動時に1回）
    hide_col_loc: 行の中の位置（pg_resolve_col_locs の結果）
      {"<col>": {"src": "const", "value": 値}}                  : 固定値（文字列・数値・null）
      {"<col>": {"src": "now"}}                                 : 現在時刻（datetime のまま。time 型の列向け）
      {"<col>": {"src": "now", "format": "%Y-%m-%d %H:%M:%S"}}  : 現在時刻を書式の文字列にする（str 型の列向け）
    範囲外・hide_col_loc に含まれる列・src の誤りは ValueError
    Mod: 2026.10.21    By: IWATA, Y.'s Agent AI（キーを Find と同じ列番号で書くように変更）
    Returns: {登録列の位置: 指定内容}
    """
    raw = config.get("col_custom_value", None) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"col_custom_value は列番号（Find と同じ列番号）をキーとする辞書（{{\"0\": ...}}）で指定してください: {raw!r}")
    selected_keys = [k for k in col_def.keys() if k not in hide_col_loc]  # 登録する列の並び（col_def_selected と同じ順）
    result: Dict[int, Dict[str, Any]] = {}
    for key, spec in raw.items():
        _k = str(key).strip()
        if not (_k.isdigit() or _k in base.SEG_HEADER_COLS):
            raise ValueError(
                f"col_custom_value のキーは列番号（Find と同じ列番号）か {list(base.SEG_HEADER_COLS)} で指定してください: {key!r}。"
                f"閉じ括弧 }} の位置を確認してください")
        col = base.resolve_act_col(_k, col_def, f"col_custom_value[{key!r}]")  # 行の中の位置
        if col in hide_col_loc:
            raise ValueError(f"col_custom_value のキー {col} は hide_col_loc に含まれる（登録しない）列です")
        if not isinstance(spec, dict) or spec.get("src") not in ("const", "now"):
            raise ValueError(f"col_custom_value[{key!r}] は {{\"src\": \"const\", \"value\": 値}} または {{\"src\": \"now\"}} で指定してください: {spec!r}")
        if spec["src"] == "const" and "value" not in spec:
            raise ValueError(f"col_custom_value[{key!r}] (const) には value が必要です: {spec!r}")
        if spec["src"] == "now" and "format" in spec:
            if not isinstance(spec["format"], str) or not spec["format"]:
                raise ValueError(f"col_custom_value[{key!r}] (now) の format は書式の文字列で指定してください: {spec!r}")
            dt.now().strftime(spec["format"])  # 書式の誤りを起動時に検出する
        result[selected_keys.index(col)] = spec
    return result


def pg_apply_col_custom_value(
    row_val_selected: List[Any],
    custom_map: Dict[int, Dict[str, Any]],
    now_: dt,
) -> None:
    """
    登録する1行（hide_col_loc を除いた並び）に col_custom_value を適用する（その場で書き換える）
    現在時刻は呼び出し側で1回取得して渡す（セグメント内の全行で同じ時刻にするため）
    Mod: 2026.10.07    By: IWATA, Y.'s Agent AI
    """
    for pos, spec in custom_map.items():
        if spec["src"] == "const":
            row_val_selected[pos] = spec.get("value")
        else:
            row_val_selected[pos] = now_.strftime(spec["format"]) if "format" in spec else now_


def fetch_diff_data(
    table_name: str,
    time_col: str,
    last_timestamp: Any,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    """
    差分データを取得してdictのリストで返す（ショートライフ・単発用）。
    接続は呼び出し毎にopen/closeする。
    ループ内で繰り返し呼ぶ場合は pg_fetch_diff を使うこと。

    引数:
        table_name     : '"schema"."table"' 形式（呼び元でバリデーション済みであること）
        time_col       : '"key_col"'         形式（同上）
        last_timestamp : 前回の最終タイムスタンプ
        limit          : 最大取得行数
    戻り値:
        [{"col": val, ...}, ...] / 0件の場合 []
    """
    conn = None
    try:
        conn = get_connection()
        return pg_fetch_diff(conn, table_name, time_col, last_timestamp, limit)
    finally:
        if conn:
            conn.close()


########################################
# Entry Point (for Debug)
########################################

if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, f"test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
