# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Utility Module
# Ver: 02.t.20261006    By: IWATA, Y.'s Agent AI
# Mod: extra_col_def に dirpath（ファイルの親フォルダのパス。rel で何階層さかのぼるか指定）を追加
########################################
get_device_info_to_add    # device_key_to_add で指定された端末情報（device_info.json）のキーと値を返す
get_make_subdir           # 元ファイルのパスから、copy / move 先に再現するフォルダ名（親側から指定階層分）を返す
parse_dir_setting         # フォルダ設定（to_dir 等）を読む。数値なら col_def のインデックス（列の値をフォルダにする）
get_dir_from_value        # 列の値からフォルダパスを取り出す（ファイルパスならそのフォルダ）
resolve_smb_dir           # フォルダパスを SMB の UNC パスにする（相対パスは共有フォルダからの相対）
replace_regex_str         # 正規表現で文字列を置換する
check_ack_file_exists     # ACK ファイル（.done 等）の存在を確認する
check_file_stability      # ファイルの更新時刻・サイズを確認して安定性をチェックする
check_file_stability_retry# ファイル安定チェックを n 回リトライする
list_files                # フォルダ内のファイルを接頭語・接尾語フィルタ付きで列挙する
sanitize_str              # SQL インジェクション対策で識別子を安全化する
save_json                 # オブジェクトを JSON ファイルに保存する
initialize_db             # SQLite データベースを初期化・接続して返す（ディスク寿命対応）
check_and_reconnect_db    # SQLite 接続の生存確認と再接続を行う
check_record_exists       # SQLite テーブルに指定レコードが存在するか確認する
insert_records_to_db      # SQLite テーブルに複数レコードを INSERT する
parse_delimited_text      # CSV/TSV等の区切りテキストを標準ライブラリのみでパースし、レコードのリストを返す
"""
MODULE_NAME = "UTIL"

"""standard library"""
import csv
from   datetime import datetime as dt
import json
import ntpath
import os
import re
import sqlite3
import sys
import time
import traceback
from   typing   import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

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
if __name__=='__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# Utility Functions
########################################

def replace_regex_str(
    key: str,
    sep: str = "_",
    rep: str = r"[!#$%&'=~|`()^\\\-\+\*\{\};:\[\]@?_,\./<> ]"
) -> str:
    """正規表現で文字列を置換"""
    pattern = f"(?:{rep})+" # 連続は１文字で置換
    return re.sub(pattern, sep, key)


def check_ack_file_exists(ack_ext: str, file_path: str) -> bool:
    """指定したファイルに対するdoneファイルがあるか判定"""
    if not(ack_ext):
        return True
    else:
        return os.path.exists(f"{file_path}.{ack_ext}")


def check_file_stability(path: str) -> bool:
    """
    ファイル安定チェック
    想定外の例外（権限エラー等）でもログを残して False（不安定扱い）を返す。
    従来は sys.exit(-1) しており、Find のスレッド内で呼ばれるとスレッドだけが黙って終了していた
    Mod: 2026.10.05    By: IWATA, Y.'s Agent AI（A-10）
    """
    CHECK_INTERVAL = 1

    try:
        stat1 = os.stat(path)
        time.sleep(CHECK_INTERVAL)
        stat2 = os.stat(path)

        if stat1.st_mtime != stat2.st_mtime or stat1.st_size != stat2.st_size:
            return False
        
        return True

    except FileNotFoundError as e:
        base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", log_file)
        
    except Exception as e:
        base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", log_file)
    
    return False


def check_file_stability_retry(n: int, path: str) -> bool:
    """ファイル安定チェック（複数回）"""
    for i in range(n):
        if not check_file_stability(path): return False
        
    return True


def list_files(
    dir_ : str,
    filters  : Tuple[Iterable[str], Iterable[str]],
    search_subdir: bool = False,
    sort_time    : bool = False,
) -> List[str]:
    """指定の条件に該当するファイルをリストアップ"""
    
    def _scan_dir(target_dir: str) -> List[str]:
        """ファイルをスキャン（listdirより高速。属性取得可能）"""
        with os.scandir(target_dir) as entries:
            return [e.path for e in entries if e.is_file()]

    def _walk_dir(target_dir: str) -> List[str]:
        """サブフォルダまでファイルを参照（scandir使用で高速。属性取得可能）"""
        all_files = []
        for root, dirs, files in os.walk(target_dir):
            for file in files:
                all_files.append(os.path.join(root, file))
        return all_files
        
    def _safe_getmtime(path: str) -> float:
        """安全に時刻を取得（ファイルが消えてても落ちない）"""
        try:
            return os.path.getmtime(path)
        except OSError:
            return 0.0 # 見つからない場合は一番古い扱い

    try:
        dir_ = os.path.abspath(dir_) # 絶対パス（OS対応）
        if not os.path.isdir(dir_):
            base.output_log(MODULE_NAME, f"Directory not found: {dir_}")
            return []
        
        prefixes = tuple(f.lower() for f in (filters[0] or []))
        suffixes = tuple(f.lower() for f in (filters[1] or []))
        
        """ファイルをリストアップ"""
        if search_subdir: # サブフォルダを検索
            files_all = _walk_dir(dir_)
        else            : # 指定フォルダ直下のみ検索
            files_all = _scan_dir(dir_)

        """ファイルをフィルタリング"""
        def _match_prefixes(name, prefixes): return name.startswith(prefixes) if prefixes else True
        def _match_suffixes(name, suffixes): return name.endswith(suffixes)   if suffixes else True
        
        files = [
            f for f in files_all
            if  _match_prefixes(os.path.basename(f).lower(), prefixes) and
                _match_suffixes(os.path.basename(f).lower(), suffixes)
        ]

        """ファイルをソート"""
        if sort_time: # 更新日時順
            files.sort(key=_safe_getmtime)
        else:         # 名前順
            files.sort() 
        
        return files

    except PermissionError   as e: base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", log_file)
    except FileNotFoundError as e: base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", log_file)
    except Exception         as e: base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", log_file)
    
    return []


def sanitize_str(str_: str) -> str:
    """英、数、_ 以外の文字を削除"""
    cleaned = re.sub(r"[^A-Za-z0-9_]", "", str_)
    return cleaned


def check_and_reconnect_db(
	conn: Any,
	db_path: str
) -> sqlite3.Connection:
    """Check DB Connection and Re-Connect If Disconnected"""
    if conn is None:
        return sqlite3.connect(db_path)

    try:
        conn.execute("SELECT 1")
        return conn
    except sqlite3.Error:
        return sqlite3.connect(db_path)
    
    except Exception as e:
        base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", log_file)
        return None


def initialize_db(
    db_path : str,
    conn,
    tab_name: str,
    col_def : Dict,
) -> sqlite3.Connection:
    """
    Initialize DB
    - STQはストリームデータの一時保持。次回起動時に初期化される
    - キュー用途であり、トランザクション処理はない（→journalは不要）
    - SSD寿命を考慮し、ファイル書込み処理は必要最低限にする
    """
    tab_name = sanitize_str(tab_name)
    cur = None
    try:
        db_dir = os.path.dirname(db_path)
        if db_dir: os.makedirs(db_dir, exist_ok=True)
        
        conn = check_and_reconnect_db(conn, db_path)
        cur  = conn.cursor()
        cur.execute("PRAGMA journal_mode = MEMORY;") # journal自体は不要だが、ロックの緩和のため設定
        cur.execute("PRAGMA synchronous  = OFF;") # ファイル書込みはOSに委ねる
        cur.execute("PRAGMA temp_store = MEMORY;") # 一時ファイルをRAMに置く
        cur.execute("PRAGMA page_size = 4096;") # 書込みサイズをSSDブロックサイズに合わせる（4096 or 8192）
        cur.execute("PRAGMA auto_vacuum = INCREMENTAL;")
        cur.execute(f"DROP TABLE IF EXISTS {tab_name}")

        _l = []
        for v in col_def.values(): _l.append(f"{sanitize_str(v[0])} {v[1]} {v[2]}")
        _sql_b = ", ".join(_l)
        _sql   = f"CREATE TABLE {tab_name} ({_sql_b})"        
        cur.execute(_sql)
        conn.commit()

    except Exception as e:
        base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", log_file)

    finally:
        if cur is not None: cur.close()
    
    return conn


def insert_records_to_db(
    db_path : str,
    conn,
    tab_name: str,
    col_def : Dict,
    to_insert: List[Any],
) -> sqlite3.Connection:
    """
    Create Data on DB
    Mod: 2026.10.04    By: IWATA, Y.'s Agent AI（成功したらループを抜ける。従来は成功後も3回 INSERT していた。A-3）
    """
    tab_name = sanitize_str(tab_name)
    cur = None
    
    col    = [sanitize_str(v[0]) for v in col_def.values()]
    if len(col) != len(to_insert[0]): raise ValueError(f"Invalid: {col_def} {to_insert[0]}")
    
    for i in range(3):# ロック時のみリトライ（最大3回）
        try:
            conn = check_and_reconnect_db(conn, db_path)
            cur  = conn.cursor()
            
            _sql   = f"INSERT OR IGNORE INTO {tab_name} ({', '.join(col)}) VALUES ({', '.join(['?']*len(col))})"
            cur.executemany(_sql, to_insert)
            conn.commit()
            break # 成功
    
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(0.1)
                continue
            raise e

        except Exception as e: base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", log_file)

        finally:
            if cur is not None: cur.close()
    
    return conn


def select_all_records(
    db_path : str,
    conn,
    tab_name: str,
    row_limit: int = None,
) -> Tuple[sqlite3.Connection, List[Any]]:
    """Read Data from DB"""
    tab_name = sanitize_str(tab_name)
    if row_limit is None: row_limit = 9999
    cur  = None
    rows = []
    
    try:
        conn = check_and_reconnect_db(conn, db_path)
        cur  = conn.cursor()
        cur.execute(f"SELECT * FROM {tab_name} LIMIT {row_limit}")
        rows = cur.fetchall()
    
    except Exception as e: base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", log_file)
    
    finally:
        if cur is not None: cur.close()
    
    return conn, rows


def check_record_exists(
    db_path : str,
    conn,
    tab_name: str,
    to_check: List[Any],
) -> Tuple[sqlite3.Connection, bool]:
    """
    Check Record Exists or Not at One Column
    Mod: 2026.10.04    By: IWATA, Y.'s Agent AI（DB 例外時は未登録（False）として返す。従来は UnboundLocalError。A-4）
    """
    tab_name   = sanitize_str(tab_name)
    col_to_chk = sanitize_str(to_check[0])
    val_to_chk = to_check[1]
    cur = None
    exists = False # DB 例外時は未登録扱い（取りこぼしより再処理を優先）
    try:
        conn = check_and_reconnect_db(conn, db_path)
        cur  = conn.cursor()
        cur.execute(f"SELECT 1 FROM {tab_name} WHERE {col_to_chk}=? LIMIT 1", (val_to_chk,))
        exists = cur.fetchone() is not None
    
    except Exception as e: base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", log_file)
    
    finally:
        if cur is not None: cur.close()

    return conn, exists


def load_json(
    path :str = r"./JSON.json",
    encode :str = "utf-8"
) -> Optional[Dict]:
    """json をDictに読込む"""
    try:
        if not(check_file_stability(path)): return {}
        
        with open(path, "r", encoding=encode) as f:
            dict_ = json.load(f)
        
        # '##*', '_comment*' キーを再帰的に削除
        def clean_dict_for_json(d):
            if isinstance(d, dict):
                return {
                    k: clean_dict_for_json(v) for k, v in d.items() if not (k.startswith("_comment") or k.startswith("##"))
            }
            elif isinstance(d, list): return [clean_dict_for_json(item) for item in d]
            else: return d
        
        return clean_dict_for_json(dict_)

    except Exception as e:
        base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", log_file)
        return None


def validate_extra_col_def(extra_col_def: Optional[List[Any]]) -> None:
    """parse_delimited_text に渡す extra_col_def の構造を検証"""
    if not extra_col_def:
        return

    for i, src in enumerate(extra_col_def):
        if not isinstance(src, dict) or "src" not in src:
            raise ValueError(f"extra_col_def[{i}] は 'src' キーを持つ辞書で指定してください: {src!r}")

        src_type = src["src"]

        if src_type == "header":
            loc_ = src.get("loc")
            if not (isinstance(loc_, (list, tuple)) and len(loc_) == 2):
                raise ValueError(f"extra_col_def[{i}] (header) には loc:[row_idx, col_idx] が必要です: {src!r}")

        elif src_type == "device":
            if "key" not in src:
                raise ValueError(f"extra_col_def[{i}] (device) には key が必要です: {src!r}")

        elif src_type == "const":
            pass # None がそのまま値になる

        elif src_type == "filename":
            pos_ = src.get("pos")
            has_pos   = isinstance(pos_, (list, tuple)) and len(pos_) == 2
            has_split = "delimiter" in src and "index" in src
            if not (has_pos or has_split):
                raise ValueError(
                    f"extra_col_def[{i}] (filename) には pos:[開始,終了] または "
                    f"delimiter+index が必要です: {src!r}")

        elif src_type == "dirname":
            rel_ = src.get("rel")
            if not isinstance(rel_, int):
                raise ValueError(f"extra_col_def[{i}] (dirname) には rel:整数 が必要です: {src!r}")

        elif src_type == "dirpath":
            rel_ = src.get("rel")
            if not isinstance(rel_, int) or isinstance(rel_, bool) or rel_ > 0:
                raise ValueError(f"extra_col_def[{i}] (dirpath) には rel:0以下の整数 が必要です: {src!r}")

        else:
            raise ValueError(
                f"extra_col_def[{i}] の src={src_type!r} は非対応です（header/device/const/filename/dirname/dirpath のいずれか）: {src!r}")

    return None


def parse_delimited_text(
    file_path     : str,
    delimiter     : str = ",",
    data_start_row: int = 0,
    extra_col_def : Optional[List[Any]] = None,
    extra_col_default: Any = "__",
    encodings     : Sequence[str] = ("utf-8-sig", "cp932", "euc-jp"),
) -> Tuple[List[List[Any]], str]:
    """
    区切りテキスト(CSV/TSV等)をパース
    引数:
    - delimiter  カンマ・タブ等、任意の区切り文字を指定
    - data_start_row  データ本体の開始行番号（0始点）
    - extra_col_def  固定値カラムの定義
      - {"src": "header", "loc": [row_idx, col_idx], "default": 任意}  ファイル内の該当セルの値（ヘッダー等）
      - {"src": "device", "key": "device_info.jsonのキー", "default": 任意}  device_info.json の値
      - {"src": "const", "default": 任意}  定数値
      - {"src": "filename", "pos": [開始, 終了], "default": 任意}  ファイル名の指定位置の文字列
      - {"src": "filename", "delimiter": 区切り文字, "index": 位置, "default": 任意}  ファイル名を区切り文字で分割した要素
      - {"src": "dirname", "rel": 相対階層, "default": 任意}  ファイルパスの、指定階層のディレクトリ名
        （rel=0: ファイルの入っているディレクトリ、rel=-1: その1つ上、rel=-2: さらに1つ上 ...）
      - {"src": "dirpath", "rel": 相対階層, "default": 任意}  ファイルの親フォルダのパス（rel は dirname と同じ数え方。0以下）
        （例 D:\\img\\LOT1\\A\\x.txt: rel=0 → D:\\img\\LOT1\\A、rel=-1 → D:\\img\\LOT1。ルートより上は範囲外として default）
      
      - "default" はカラムごとの取得失敗時デフォルト値（省略時は extra_col_default を使う。None を明示指定すると、col_def の型ごとの既定値（str_default 等）に委ねられる）。取得失敗時はログに出力したうえで処理を継続      - encodings を順に試し、成功したら読込む（セーフティ・エンコード）
    戻り値:
    - records  データ行 + extra_col_def を結合したレコードのリスト
    - used_encoding  実際に読込みに使用したエンコード名
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Text File Not Found: {file_path}")

    """1. セーフティ・エンコードで全行読込み"""
    all_rows, used_encoding, error = [], "", None
    for enc in encodings:
        try:
            with open(file_path, mode="r", encoding=enc, newline="") as f:
                reader   = csv.reader(f, delimiter=delimiter)
                all_rows = [row for row in reader if row] # 空行除外
            used_encoding = enc
            break
        except (UnicodeDecodeError, csv.Error) as e:
            error = e
            continue
    else:
        raise ValueError(f"Encode NOT Match in {list(encodings)} or Invalid Format: {file_path}") from error

    """ 2. 全レコード共通（ヘッダー値など）を解決 """
    extra_vals: List[Any] = []
    if extra_col_def:
        device_info = base.load_device_info()
        fname = os.path.basename(file_path)
        dir_parts = os.path.normpath(os.path.dirname(file_path)).split(os.sep)

        for src in extra_col_def:
            if not isinstance(src, dict) or "src" not in src:
                raise ValueError(f"extra_col_def の要素は 'src' キーを持つ辞書で指定してください: {src!r}")

            src_type   = src["src"]
            col_default = src["default"] if "default" in src else extra_col_default

            # ファイル内セルの値（ヘッダー等）
            if src_type == "header":
                r_idx, c_idx = src["loc"]
                try:
                    extra_vals.append(all_rows[r_idx][c_idx])
                except IndexError:
                    base.output_log(
                        MODULE_NAME,
                        f"extra_col_def(header) のセル位置 loc=[{r_idx},{c_idx}] が範囲外のため "
                        f"default={col_default!r} を使用します: {file_path}", log_file)
                    extra_vals.append(col_default)

            # device_info.json の値
            elif src_type == "device":
                key = src["key"]
                if key not in device_info:
                    base.output_log(
                        MODULE_NAME,
                        f"extra_col_def(device) の key={key!r} が device_info.json にないため "
                        f"default={col_default!r} を使用します（利用可能なキー={list(device_info.keys())}）: "
                        f"{file_path}", log_file)
                    extra_vals.append(col_default)
                else:
                    extra_vals.append(device_info[key])

            # 定数値
            elif src_type == "const":
                extra_vals.append(src.get("default"))

            # ファイル名から抽出
            elif src_type == "filename":
                try:
                    if "pos" in src:
                        start, end = src["pos"]
                        if start < 0 or end < start:
                            raise ValueError(f"invalid pos={src['pos']}")
                        value = fname[start:end + 1]
                        if value == "":
                            raise ValueError(f"pos={src['pos']} out of range for filename={fname!r}")

                    elif "delimiter" in src and "index" in src:
                        idx = src["index"]
                        if idx < 0:
                            raise ValueError(f"invalid index={idx}")
                        value = fname.split(src["delimiter"])[idx]

                    else:
                        raise ValueError(f"invalid filename spec: {src}")

                    extra_vals.append(value)

                except (IndexError, ValueError) as e:
                    base.output_log(
                        MODULE_NAME,
                        f"extra_col_def(filename) の抽出({src})に失敗したため "
                        f"default={col_default!r} を使用します（filename={fname!r}）: {e}: {file_path}", log_file)
                    extra_vals.append(col_default)

            # ファイルが置かれているディレクトリパスから抽出
            elif src_type == "dirname":
                try:
                    rel = src["rel"]
                    idx = rel - 1
                    extra_vals.append(dir_parts[idx])
                except IndexError:
                    base.output_log(
                        MODULE_NAME,
                        f"extra_col_def(dirname) の rel={src.get('rel')} が範囲外のため "
                        f"default={col_default!r} を使用します（dir_parts={dir_parts}）: {file_path}", log_file)
                    extra_vals.append(col_default)

            # ファイルの親フォルダのパス（rel 階層さかのぼる）
            elif src_type == "dirpath":
                folder = os.path.dirname(os.path.abspath(file_path))
                for _ in range(-int(src["rel"])):
                    parent = os.path.dirname(folder)
                    if parent == folder: # ルートより上は範囲外
                        folder = None
                        break
                    folder = parent
                if folder is None:
                    base.output_log(
                        MODULE_NAME,
                        f"extra_col_def(dirpath) の rel={src.get('rel')} が範囲外のため "
                        f"default={col_default!r} を使用します: {file_path}", log_file)
                    extra_vals.append(col_default)
                else:
                    extra_vals.append(folder)

            else:
                raise ValueError(f"extra_col_def の src={src_type!r} は非対応です: {src!r}")

    """ 3. データ本体行を抽出 """
    records = [row + extra_vals for row in all_rows[data_start_row:]]

    return records, used_encoding


def get_device_info_to_add(config: Dict[str, Any]) -> Tuple[List[str], List[Any]]:
    """
    act_trigger 設定の device_key_to_add で指定されたキーのうち、device_info.json に存在するものと、その値を返す
    Returns: (キーのリスト, 値のリスト)  ※ 指定なし・該当なしは ([], [])
    Mod: 2026.10.05    By: IWATA, Y.'s Agent AI（csv / diag_gb / diag_mt / diag_spc に重複していた処理を集約。B-9）
    """
    device_info = base.load_device_info()
    keys = [s for s in (config.get("device_key_to_add", []) or []) if s and s in device_info.keys()]
    return keys, [device_info[s] for s in keys]


def parse_make_subdir_level(config: Dict[str, Any]) -> int:
    """
    act_trigger 設定の make_subdir_level を読む（既定 0。-1 以下は -1＝全階層として扱う）
    Mod: 2026.10.06    By: IWATA, Y.'s Agent AI
    """
    levels = int(config.get("make_subdir_level", 0) or 0)
    return -1 if levels < 0 else levels


def get_make_subdir(src_path: str, levels: int) -> List[str]:
    """
    元ファイルのパスから、copy / move 先に再現するフォルダ名のリストを返す（親々 → 親 の順）
      levels =  0 : []（ファイルのみ）
      levels =  1 : [親フォルダ]
      levels =  2 : [親々フォルダ, 親フォルダ]
      levels = -1 : パスにあるフォルダ全て
    渡されたパスにある範囲のみ（ドライブ・UNC の //server/share・ルートは含めない）。
    指定より浅いパスは、ある分だけ返す。"." / ".." は含めない（移動先の外へ出ないため）
    Mod: 2026.10.06    By: IWATA, Y.'s Agent AI
    """
    if not levels:
        return []
    _drive, rest = ntpath.splitdrive(src_path)  # Windows 形式（C: / \\server\share）を OS によらず除く
    dirs = [p for p in re.split(r"[\\/]+", rest)[:-1] if p and p not in (".", "..")]  # 末尾（ファイル名）を除く
    return dirs if levels < 0 else dirs[-levels:]


def parse_dir_setting(config: Dict[str, Any], key: str, n_col_def: int) -> Tuple[str, Optional[int]]:
    """
    フォルダ設定（to_dir / move_to / remote_dir）を読む
      文字列 → (フォルダパス, None)
      数値   → ("", col_def のインデックス)（行ごとに、その列の値をフォルダとして使う）
    true / false・小数・範囲外のインデックスは ValueError
    Mod: 2026.10.06    By: IWATA, Y.'s Agent AI
    """
    v = config.get(key, "")
    if isinstance(v, (bool, float)):
        raise ValueError(f"config[{key!r}] must be a folder path (string) or a col_def index (int). Got: {v!r}")
    if isinstance(v, int):
        if not (0 <= v < n_col_def):
            raise ValueError(f"config[{key!r}]={v} (col_def index) out of range (0-{n_col_def - 1})")
        return "", v
    return ("" if v is None else str(v).strip()), None


def get_dir_from_value(val: Any) -> Optional[str]:
    """
    列の値からフォルダパスを取り出す
    - 末尾の要素に拡張子があればファイルパスとみなし、そのフォルダを返す（例 out/A/x.csv → out/A、x.csv → ""）
    - それ以外はフォルダパスとしてそのまま返す（末尾の区切り文字は除く）
    - 値が空・"____"（セグメント区切り）の場合は None
    Mod: 2026.10.06    By: IWATA, Y.'s Agent AI
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s or s == "____":
        return None
    s2 = s.rstrip("\\/") or s[:1]  # "/" 等ルートのみの場合はそのまま
    idx = max(s2.rfind("/"), s2.rfind("\\"))
    if not os.path.splitext(s2[idx + 1:])[1]:
        return s2
    folder = s2[:idx] if idx > 0 else s2[:idx + 1]  # idx=0（"/x.csv"）はルート、idx=-1（"x.csv"）は ""
    if folder.endswith(":"):
        folder += "\\"  # "C:\x.csv" → "C:\"
    return folder


def resolve_smb_dir(folder: str, smb_base: str) -> Optional[str]:
    """
    フォルダパスを SMB の UNC パス（\\\\server\\share\\...）にする
    - UNC の絶対パスは、smb_base（設定の smb_server / smb_share）と同じ共有の配下のみ受け付ける
    - それ以外（先頭の区切り文字は無視）は共有フォルダからの相対パス
    - ドライブ指定（C:\\ 等）・".." を含むパス・別の共有は None（異常）
    Mod: 2026.10.06    By: IWATA, Y.'s Agent AI
    """
    f = folder.replace("/", "\\")
    if f.startswith("\\\\"):
        f_l, base_l = f.rstrip("\\").lower(), smb_base.rstrip("\\").lower()
        if f_l != base_l and not f_l.startswith(base_l + "\\"):
            return None
        rest = f.rstrip("\\")[len(base_l):]
    elif ntpath.splitdrive(f)[0]:
        return None
    else:
        rest = f
    parts = [p for p in rest.split("\\") if p and p != "."]
    if ".." in parts:
        return None
    return "\\".join([smb_base.rstrip("\\")] + parts)


def save_json(
        dic_: dict,
        file_: str = r'./dict.json',
        enc_: str = "utf-8-sig"
    ) -> None:
    """dictをjson形式のテキストで保存"""
    def convert(o):
        if isinstance(o, (list, tuple)): return list(o)
        if isinstance(o, dict):          return o
        if hasattr(o, "item"):           return o.item()
        return None
    
    with open(file_, "w", encoding=enc_) as f:
        json.dump(dic_, f, ensure_ascii=False, separators=(',', ':'), indent=2, default=convert)
    return None


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    
    base.output_log(MODULE_NAME, f"test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
