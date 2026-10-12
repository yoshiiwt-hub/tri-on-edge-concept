# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [Text]
# Ver: 01.o.20261007    By: IWATA, Y.'s Agent AI
# Mod: column_source_map・extra_col_def のキーが列番号（数字）でない場合、起動時に原因（閉じ括弧の位置等）が分かるメッセージで止める
########################################
f_find_text_trigger   # フォルダを監視し、CSV/TSV等のテキストファイルをパースしてキューに流す

動作概要
    1. monitor_dir をポーリングし、未処理ファイルを発見したら util.parse_delimited_text でパース
       - SQLite の seen_files DB で重複処理を防止（tri_s11_find_file.py と同様）
       - pfx_sfx フィルタ・stable_check に対応
    2. パース結果（データ行 + extra_col_def の結合レコード）を col_def の dtype に従って型変換し、
       1行ずつキューへ送信。ファイル末尾でセグメント区切り（'____'）を送信する
       （1ファイル = 1セグメント。tri_s11_find_file.py の f_read_json_trigger と同様）
    3. util.parse_delimited_text はファイル不存在・全エンコード失敗時にそのまま例外を投げる（util は
       ErrorIF に依存しないため）。このトリガー側でその例外を受け止め、ErrorIF.report_exception で
       報告したうえで、同種エラーが頻発（デフォルト: 60秒に5回）していれば、tri_s11_find_socket.py
       と同様に一定期間（offline_interval_sec）だけ読み取りをスキップし、期間経過後に自動で読み取りを
       再開する（オフラインモード）
    4. 読み取った後、after_read に従ってファイルを削除・移動・リネームする（既定 keep = 残す）
    5. stop_event が立つまでループを継続

出力列
    col_def の列順に、データ列 + extra_col_def の列（指定していれば）が入る
    （queue_out.put で送る values の長さ・順序は col_def と完全に一致させること）

接続設定
    monitor_dir      : 監視対象ディレクトリ（デフォルト: "."）
    search_subdir    : サブディレクトリを再帰的に検索するか（デフォルト: false）
    pfx_sfx          : ファイル名フィルタ [[接頭語], [接尾語]]（デフォルト: [[], [".csv"]]）
    stable_check_n   : 安定チェック回数（デフォルト: 1）
    interval_sec     : ポーリング間隔秒（デフォルト: 2）
    delimiter        : 区切り文字（デフォルト: ","。タブ区切りは "\t" を指定）
    data_start_row   : データ本体の開始行番号・0始点（デフォルト: 0）
    extra_col_def    : 追加カラム定義（デフォルト: null）。辞書形式で指定する：
                          {"0": {...}, "27": {...}, ...}
                        キーは「この要素の値を割り当てる col_def のインデックス」そのもの
                        （column_source_map のキーと同じ流儀）。そのため column_source_map・
                        extra_col_def どちらも、JSON上は任意の順序・任意の位置に記載してよい
                        （col_def の並び順どおりに書く必要はない）。各要素は "src" キーを持つ辞書：
                          - {"src": "header", "loc": [row_idx, col_idx], "default": 任意}
                          - {"src": "device", "key": "device_info.jsonのキー", "default": 任意}
                          - {"src": "const", "default": 任意}
                          - {"src": "filename", "pos": [開始, 終了], "default": 任意}
                          - {"src": "filename", "delimiter": 区切り文字, "index": 位置, "default": 任意}
                          - {"src": "dirname", "rel": 相対階層, "default": 任意}
                          - {"src": "dirpath", "rel": 相対階層, "default": 任意}
                        ("loc" はセルの行・列位置、"pos" はファイル名の文字位置範囲。混同しないよう別名にしている。
                         "dirname.rel" は 0=直近の親ディレクトリ、-1=その1つ上、のように指定する。
                         "dirpath" は dirname と同じ数え方で、フォルダ名ではなくフォルダのパスを返す。
                         例 D:/img/LOT1/A/x.txt: rel=0 → D:/img/LOT1/A、rel=-1 → D:/img/LOT1。ルートより上は default。
                         Act（tri_s12_act_comb_rdb_smb の file_search_dir 等）へ、検索・保存先のフォルダを渡す用途)
                        "src" が不正等の設定ミスは、起動時に utl.validate_extra_col_def で検知して即停止する
                        （実行時にたまたま値が取れない場合は、その要素の "default" で継続する。詳細は util 側 docstring 参照）
    extra_col_default: extra_col_def の各要素に "default" が無い場合に使うデフォルト値（デフォルト: "__"）
    column_source_map: 列マッピング定義（デフォルト: null）
                        {col_defのインデックス(文字列): CSV区切り位置(0始点の整数)}
                        例: {"0": 0, "1": 2}  → col_def[0]にはCSVの0番目の値、col_def[1]には
                        CSVの2番目の値を割り当てる（間の1番目の値は使わない）
                        col_def の各インデックスには、次の順で値を割り当てる
                          - column_source_map・extra_col_def のどちらにも無い → CSVの同じ位置の値（列番号＝区切り位置）
                          - どちらか一方にある → その設定
                          - 両方にある → 設定ファイルで後に書いた方（column_source_map と extra_col_def の記載順）
                        CSVに該当位置の値が無い場合は None（col_def の dtype の既定値）。CSVの余った列は使わない
                        割当て漏れ・重複は設定どおりとして扱い、エラーにしない。キーが col_def の範囲外の場合のみ起動時エラー
    encodings        : 読込みを試すエンコード候補（デフォルト: ["utf-8-sig", "cp932", "euc-jp"]）
    offline_interval_sec: 読込みエラー頻発検知後、次に読み取りを再開するまでの待機秒数（デフォルト: 30）
    row_range        : 読み取り対象のデータ行範囲（デフォルト: null＝全行読取り）。1始点・両端含む。
                        [開始行]         : 開始行から最終行まで（例: [10] → 10行目?最終行）
                        [開始行, 終了行] : 開始行から終了行まで（例: [10, 100] → 10行目?100行目）
                        ※ data_start_row でスキップした後の「データ本体」の中での行番号
                          （ヘッダー行等は含まない）。extra_col_defの付与・型変換より前に適用される。
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
MODULE_NAME = "FIND_Read_Csv"

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
# 列マッピング・ヘルパー
########################################

def _check_index_keys(d: Any, name: str) -> None:
    """
    column_source_map / extra_col_def が「列番号（数字）をキーとする辞書」か確認する（起動時）
    JSON の閉じ括弧の位置を誤ると、後ろの設定（extra_col_default 等）が中に入り、int(キー) で原因の分かりにくいエラーになるため
    Mod: 2026.10.07    By: IWATA, Y.'s Agent AI
    """
    if not isinstance(d, dict):
        raise ValueError(f"{name} は列番号をキーとする辞書（{{\"0\": ...}}）で指定してください: {d!r}")
    bad = [k for k in d.keys() if not str(k).strip().lstrip("-").isdigit()]
    if bad:
        raise ValueError(
            f"{name} のキーは col_def のインデックス（数字）で指定してください: {bad}。"
            f"閉じ括弧 }} の位置を確認してください（後ろの設定が {name} の中に入っている可能性があります）")


def _build_col_assign(
    n_col_def:            int,
    column_source_map:    Optional[Dict[str, int]],
    extra_col_target_idx: List[int],
    extra_after_map:      bool,
) -> List[Tuple[str, int]]:
    """
    col_def の各インデックスに割り当てる値の取得元を決める（起動時に1回だけ実行）
    Returns: [("csv", CSV区切り位置) または ("extra", extra_col_def 内の並び順位置), ...]（col_def の並び順）
    - 既定は ("csv", 列番号)（列番号＝区切り位置）
    - column_source_map・extra_col_def で上書きする。両方にある列は、設定ファイルで後に書いた方が勝つ
      （extra_after_map=True なら extra_col_def を後から適用する）。同じ設定内の重複も後勝ち
    Mod: 2026.10.06    By: IWATA, Y.'s Agent AI
    """
    assign: List[Tuple[str, int]] = [("csv", i) for i in range(n_col_def)]

    def _apply_map() -> None:
        for col_key, csv_idx in (column_source_map or {}).items():
            assign[int(col_key)] = ("csv", int(csv_idx))

    def _apply_extra() -> None:
        for pos, target_idx in enumerate(extra_col_target_idx):
            assign[target_idx] = ("extra", pos)

    if extra_after_map:
        _apply_map(); _apply_extra()
    else:
        _apply_extra(); _apply_map()
    return assign


def _remap_row(
    row:     List[Any],
    assign:  List[Tuple[str, int]],
    n_extra: int,
) -> List[Any]:
    """
    parse_delimited_text が返す1行（CSV生データ列 + extra_col_def列を連結したもの）から、
    assign（_build_col_assign の結果）に従って col_def の並びに沿った新しい行を組み立てる。
    CSVに該当位置の値が無い場合は None とする（CSVの余った列は使わない）。
    Mod: 2026.10.06    By: IWATA, Y.'s Agent AI（列番号＝区切り位置の既定割当てに対応）
    """
    raw_data   = row[: len(row) - n_extra] if n_extra else row
    extra_data = row[len(row) - n_extra :] if n_extra else []

    new_row: List[Any] = []
    for src, idx in assign:
        if src == "extra":
            new_row.append(extra_data[idx])
        else:
            new_row.append(raw_data[idx] if 0 <= idx < len(raw_data) else None)
    return new_row


########################################
# Find Trigger Functions
########################################

def f_find_text_trigger(
    queue_out: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
    ) -> None:
    """
    CSV/TSV等の区切りテキストファイルを監視し、パースしてキューに流す
    Mod: 2026.10.06    By: IWATA, Y.'s Agent AI（列の割当て: 指定の無い列は列番号＝区切り位置、重複は後勝ち）
    """
    TRIGGER_NAME = "f_find_text_trigger"

    try:
        """ SETTING（監視） """
        monitor_dir = os.path.join(system_root, config.get("monitor_dir", "."))
        os.makedirs(monitor_dir, exist_ok=True)
        search_subdir = config.get("search_subdir", False)
        pfx_sfx = config.get("pfx_sfx", ([], [".csv"]))

        stable_check_n = int(config.get("stable_check_n", "1"))
        interval_sec   = config.get("interval_sec", 2)

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

        """ SETTING（パース） """
        delimiter         = config.get("delimiter", ",")
        data_start_row     = int(config.get("data_start_row", 0))

        # extra_col_def は設定ファイル上、辞書形式（{"0": {...}, "27": {...}, ...}）で書く。
        # キーは「この値を割り当てる col_def のインデックス」そのもの
        # （column_source_map のキーと同じ流儀）。JSON上の記載順は問わない＝任意の順序・
        # 任意の位置に書いてよい。int(キー)の昇順に並べ替えてから
        # utl.parse_delimited_text へリスト化して渡す（同関数はcol_def非依存でリストの
        # 並び順のまま追加値を計算するだけなので、そちら側は変更不要）。
        # 各キーが実際にどのcol_defインデックスへ入るかは extra_target_to_pos に保持し、
        # _remap_row 側で参照する。
        extra_col_def_raw = config.get("extra_col_def", None)
        if extra_col_def_raw is not None:
            _check_index_keys(extra_col_def_raw, "extra_col_def")
            _extra_items = sorted(extra_col_def_raw.items(), key=lambda kv: int(kv[0]))
            extra_col_target_idx = [int(k) for k, _v in _extra_items]
            extra_col_def        = [_v      for _k, _v in _extra_items]
        else:
            extra_col_target_idx = []
            extra_col_def = None

        # extra_col 設定チェック
        utl.validate_extra_col_def(extra_col_def)
        
        extra_col_default  = config.get("extra_col_default", "__")
        encodings          = tuple(config.get("encodings", ("utf-8-sig", "cp932", "euc-jp")))
        offline_interval_sec = config.get("offline_interval_sec", 30)

        """ 読取り対象行の範囲設定（省略可。データ行基準・1始点・両端含む） """
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

            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} row_range=[{row_range_start}, {row_range_end}]  "
                f"(データ行基準・1始点・両端含む)", log_file)
        else:
            row_range_start, row_range_end = None, None  # 省略時は全行読取り

        """ 列の割当て（指定の無い列は 列番号＝CSV区切り位置。重複は設定ファイルで後に書いた方を優先） """
        column_source_map = config.get("column_source_map", None) or {}
        _check_index_keys(column_source_map, "column_source_map")
        n_extra = len(extra_col_def) if extra_col_def else 0
        n_col_def_check = len(col_def)

        # キーが col_def の範囲外だと黙って無視されるため、起動時に止める（割当て漏れ・重複は設定どおりとして扱う）
        for col_key, csv_idx in column_source_map.items():
            if not (0 <= int(col_key) < n_col_def_check):
                raise ValueError(
                    f"column_source_map のキー {col_key!r} が col_def の範囲外です "
                    f"(有効範囲: 0-{n_col_def_check - 1})")
            if int(csv_idx) < 0:
                raise ValueError(
                    f"column_source_map[{col_key!r}]={csv_idx} は0以上である必要があります")
        for target_idx in extra_col_target_idx:
            if not (0 <= target_idx < n_col_def_check):
                raise ValueError(
                    f"extra_col_def のキー {target_idx} が col_def の範囲外です "
                    f"(有効範囲: 0-{n_col_def_check - 1})")

        # 両方に同じ列がある場合は、設定ファイルで後に書いた方を優先（設定は記載順に読み込まれる）
        _cfg_keys = list(config.keys())
        extra_after_map = (
            "extra_col_def" in _cfg_keys and "column_source_map" in _cfg_keys
            and _cfg_keys.index("extra_col_def") > _cfg_keys.index("column_source_map"))
        col_assign = _build_col_assign(n_col_def_check, column_source_map, extra_col_target_idx, extra_after_map)
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} column_source_map={column_source_map}  "
            f"extra_col_target_idx={extra_col_target_idx}  extra_after_map={extra_after_map}  "
            f"col_assign={col_assign}", log_file)

        base.output_log(
            MODULE_NAME,
            f"{TRIGGER_NAME} monitor_dir={monitor_dir} pfx_sfx={pfx_sfx} interval_sec={interval_sec} "
            f"delimiter={delimiter!r} data_start_row={data_start_row} extra_col_def={extra_col_def} "
            f"extra_col_default={extra_col_default!r} encodings={encodings} "
            f"offline_interval_sec={offline_interval_sec}", log_file)

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

        """発見済みファイル名格納用DB"""
        db_path = os.path.join(work_db_dir, f"{TRIGGER_NAME}_seen_files.db")
        table_name = "seen_files"
        table_col = {
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
    # is_online: ファイル読み取りが正常に行えているか
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

                """テキストパース"""
                try:
                    records, used_encoding = utl.parse_delimited_text(
                        f_path,
                        delimiter=delimiter,
                        data_start_row=data_start_row,
                        extra_col_def=extra_col_def,
                        extra_col_default=extra_col_default,
                        encodings=encodings,
                    )

                except (FileNotFoundError, ValueError) as e:
                    ErrorIF.report_exception(e, phase="loop", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)

                    # 同種エラーの頻発判定
                    # デフォルト: 60秒に5回。error_policy.json に準拠
                    signature = err.make_signature(type(e).__name__, "io", "loop", TRIGGER_NAME)
                    if ErrorIF.is_frequent(signature):
                        # エラー頻発を検知。オフライン待機に移行
                        is_online = False
                        last_retry_try = time.time()
                        base.output_log(
                            MODULE_NAME,
                            f"{TRIGGER_NAME} Read Errors Frequent. Go Offline for {offline_interval_sec}s.", log_file)
                        break

                    # 単発の読込み失敗。seen_files に登録しないため次回リトライされる
                    continue

                base.output_log(MODULE_NAME, f"Found File: {f_path}  encoding={used_encoding}  rows={len(records)}", log_file)

                """型変換してキュー送信（row_range指定時はデータ行番号(1始点)で絞り込む）"""
                for row_i, row in enumerate(records, start=1):
                    if row_range_start is not None and row_i < row_range_start:
                        continue
                    if row_range_end is not None and row_i > row_range_end:
                        break  # records は行番号順のため、これ以降は全て範囲外

                    row = _remap_row(row, col_assign, n_extra)

                    value, cast_fails = base.cast_dtype(row, cast_plan)
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

                """セグメント区切り送信"""
                queue_out.put((count, ['____'] * n_col_def))
                count = 0

                """発見済みファイル名に格納"""
                mtime_ = os.path.getmtime(f_path)
                utl.insert_records_to_db(db_path, conn, table_name, table_col, [[f_path, mtime_]])

                """読み取った後の処理"""
                if _after_read(f_path):
                    stop_event.set()
                    break

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