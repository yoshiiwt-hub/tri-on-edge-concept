# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Stored Common Utility
# Ver: 01.e.20260930    By: IWATA, Y.'s Agent AI
# Mod: parquet キャッシュ関数（save_parquet / load_parquet / load_or_create_parquet）を追加。
#      if __main__ に単体テストを追加。
########################################
"""
MODULE_NAME = "UTIL"

"""standard library"""
import csv
import glob
import json
import os
import pickle
from   datetime import datetime as dt
from   typing   import Any, Callable, Dict, List, Optional, Sequence, Tuple

"""external library"""
import pandas as pd
try: import pyarrow
except ImportError: pass


def log(msg: str, src: str = "UTIL") -> None:
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{ts}  [{src}]  {msg}")


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

    # 保存先ディレクトリが無ければ作成
    dir_ = os.path.dirname(file_)
    if dir_:
        os.makedirs(dir_, exist_ok=True)
    
    with open(file_, "w", encoding=enc_) as f:
        json.dump(dic_, f, ensure_ascii=False, separators=(',', ':'), indent=2, default=convert)
    
    return None


########################################
# ヘッダー行の正規化
########################################

def _sanitize_header_row(header_row: List[str]) -> List[str]:
    """
    列名が空文字列、または既に出現済み（重複）の場合、"column_{列位置}" を自動的に割り当てる
    （列位置は0始点。最初に出現した名前はそのまま使い、2回目以降の重複だけ置き換える）。
    """
    seen: set = set()
    result: List[str] = []
    n_renamed = 0
    for idx, name in enumerate(header_row):
        name = (name or "").strip()
        if not name or name in seen:
            name = f"column_{idx}"
            n_renamed += 1
        seen.add(name)
        result.append(name)

    if n_renamed > 0:
        log(f"WARNING  列名が空または重複していたため、{n_renamed}列を自動採番しました "
             f"(column_連番): {result}")

    return result


########################################
# セーフティ・エンコード CSV 読込み
########################################

def read_csv_all_rows(
    file_path: str,
    delimiter: str = ",",
    encodings: Sequence[str] = ("utf-8-sig", "cp932", "euc-jp"),
) -> Tuple[List[List[str]], str]:
    """
    区切りテキスト(CSV/TSV等)を標準ライブラリのみでセーフティ・エンコード読込みする
    （tri_edge_00_util.py の parse_delimited_text と同じ考え方。空行は除外する）。
    base.py に依存しないスタンドアロン実装のため、学習スクリプトから直接呼べる。

    戻り値:
        all_rows      : 全行（ヘッダー行も含む）のリスト
        used_encoding : 実際に読込みに使用したエンコード名
    ファイル不存在・全エンコード候補で読込み失敗した場合は例外を投げる
    （FileNotFoundError / ValueError。呼び出し側で対応すること）。
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Text File Not Found: {file_path}")

    all_rows, used_encoding, error = [], "", None
    for enc in encodings:
        try:
            with open(file_path, mode="r", encoding=enc, newline="") as f:
                reader   = csv.reader(f, delimiter=delimiter)
                all_rows = [row for row in reader if row]  # 空行除外
            used_encoding = enc
            break
        except (UnicodeDecodeError, csv.Error) as e:
            error = e
            continue
    else:
        raise ValueError(f"Encode NOT Match in {list(encodings)} or Invalid Format: {file_path}") from error

    return all_rows, used_encoding


########################################
# ファイル列挙
########################################

def list_files(
    dir_: str,
    pfx_sfx: Tuple[List[str], List[str]] = ([""], [".csv"]),
    search_subdir: bool = False,
) -> List[str]:
    """dir_ 内のファイルを、前方一致・後方一致でフィルタして列挙する（名前順）"""
    prefixes = tuple(pfx_sfx[0]) if pfx_sfx and pfx_sfx[0] else ("",)
    suffixes = tuple(pfx_sfx[1]) if pfx_sfx and pfx_sfx[1] else ("",)

    pattern    = os.path.join(dir_, "**" if search_subdir else "", "*")
    all_files  = glob.glob(pattern, recursive=search_subdir)

    result = [
        f for f in all_files
        if os.path.isfile(f)
        and os.path.basename(f).startswith(prefixes)
        and os.path.basename(f).endswith(suffixes)
    ]
    return sorted(result)


########################################
# セグメントCSV群 → DataFrame 結合
########################################

def read_segment_csv_dir(
    data_dir      : str,
    pfx_sfx       : Tuple[List[str], List[str]] = ([""], [".csv"]),
    search_subdir : bool = False,
    encodings     : Sequence[str] = ("utf-8-sig", "cp932", "euc-jp"),
    data_start_row: int = 5,
    header_row_idx: int = 0,
    seg_id_col    : str = "seg_id",
    seg_index_col : str = "seg_index",
) -> Tuple[pd.DataFrame, List[str]]:
    """
    TriEdgeの act_csv.py（a_save_seg_trigger）が保存したセグメントCSV群
    （1ファイル=1セグメント、5行ヘッダー形式。デフォルト data_start_row=5 はこれに対応）、
    または同様の「ヘッダー数行＋データ本体」形式のCSV群を読込み、1つのDataFrameに結合する。
    read_csv_all_rows のセーフティ・エンコード機構を使うため、find_text_csv.py と
    同じ複数候補（utf-8-sig→cp932→euc-jp）を順に試す
    （単一エンコード固定だった頃より頑健。1ファイルの読込みに失敗しても、その1件をスキップして継続する）。

    列名は header_row_idx で指定した行（0始点）をそのまま使う。
    TriEdgeの act_csv.py 出力では列名は0行目（デフォルト）にあるが、他のロガー等が出力する
    CSV（例: メタ情報行が0行目、実際の列名は3行目、といった構成）では header_row_idx で
    明示的にずらす必要がある。ヘッダー行の列数とデータ行の列数が一致しない場合は、
    設定ミスの可能性が高いとみなして分かりやすいエラーメッセージで停止する
    （device_key_to_add で列が追加されている場合でも、col_def由来の列数とズレて
      誤った列名を割り当ててしまう事故を防ぐ）。
    数値として解釈できる列は自動的に数値型に変換する（pandas.read_csv の自動型推論相当）。
    列名が空文字列・重複している場合は "column_{列位置}" を自動採番する（_sanitize_header_row）。

    seg_index（波形内での位置）・seg_id（セグメント識別子）は、実データに既に同名の列があれば
    そのまま使う。無い場合のみ、ファイル内の行番号・ファイル名から合成する。

    戻り値:
        df_merge   : 結合済みDataFrame（seg_id/seg_index が自動付与された場合は先頭に入る）
        header_row : ファイル自身のヘッダー行（正規化済み、seg_id/seg_index付与前の元の並び）。
                     複数ファイルで列構成が異なる場合は ValueError（列インデックス指定が
                     ファイルによって意味が変わってしまうと危険なため、これは許容しない）
    """
    files = list_files(data_dir, pfx_sfx, search_subdir)
    log(f"対象ファイル数={len(files)}  data_dir={data_dir}  pfx_sfx={pfx_sfx}")

    dfs: List[pd.DataFrame] = []
    ref_header_row: Optional[List[str]] = None

    for f_path in files:
        try:
            all_rows, used_encoding = read_csv_all_rows(f_path, delimiter=",", encodings=encodings)
        except (FileNotFoundError, ValueError) as e:
            log(f"WARNING  ファイル読込み失敗: {f_path}  {e}")
            continue

        if len(all_rows) <= header_row_idx:
            log(f"WARNING  ヘッダー行(header_row_idx={header_row_idx})が存在しません。"
                 f"スキップします: {f_path}  (総行数={len(all_rows)})")
            continue

        header_row = _sanitize_header_row(all_rows[header_row_idx])
        data_rows  = all_rows[data_start_row:]
        if not data_rows:
            continue

        # ヘッダー行とデータ行の列数不一致は、header_row_idx/data_start_row の設定ミスの
        # 可能性が高い。pandasの分かりにくいAssertionErrorに落ちる前に検知して知らせる。
        n_head = len(header_row)
        n_data = len(data_rows[0])
        if n_head != n_data:
            raise ValueError(
                f"ヘッダー行(header_row_idx={header_row_idx})の列数={n_head} と "
                f"データ行(data_start_row={data_start_row})の列数={n_data} が一致しません: {f_path}\n"
                f"  header_row={header_row}\n"
                f"  設定ファイルの header_row_idx / data_start_row が、このCSVの実際の行構成と"
                f"合っているか確認してください。")

        # 列インデックス指定（val_to_judge_idx 等）が全ファイルで同じ意味になるよう、
        # ヘッダー構成がファイル間で一致していることを検証する
        if ref_header_row is None:
            ref_header_row = header_row
        elif header_row != ref_header_row:
            raise ValueError(
                f"ファイル間でヘッダー構成が一致しません（列インデックス指定の前提が崩れます）: {f_path}\n"
                f"  基準ヘッダー ={ref_header_row}\n"
                f"  このファイル ={header_row}")

        df = pd.DataFrame(data_rows, columns=header_row)

        # 数値として解釈できる列は数値型に変換する（read_csv の自動型推論相当）
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass  # 数値化できない列（文字列列）はそのまま

        if seg_index_col not in df.columns:
            df.insert(0, seg_index_col, range(len(df)))
        if seg_id_col not in df.columns:
            df.insert(0, seg_id_col, os.path.splitext(os.path.basename(f_path))[0])

        dfs.append(df)

    if not dfs:
        raise ValueError(f"有効な学習データが1件もありません（data_dir={data_dir}）")

    df_merge = pd.concat(dfs, ignore_index=True)
    log(f"結合後の総行数={len(df_merge)}  セグメント数={df_merge[seg_id_col].nunique()}")
    return df_merge, ref_header_row


########################################
# Pickle 保存・読込み
########################################

def save_pickle(obj: Any, file_: str) -> str:
    """
    objをpickle形式で保存する
    Mod: 2026.08.15    By: IWATA,Y.（AnalyzeFunctionsDT.py の SAVE_PICKLE を移植。
         保存先ディレクトリが無ければ作成する）
    """
    dir_ = os.path.dirname(file_)
    if dir_:
        os.makedirs(dir_, exist_ok=True)

    with open(file_, "wb") as f:
        pickle.dump(obj, f)
    log(f"Pickle保存: {file_}")

    return file_


def load_pickle(file_: str) -> Any:
    """
    pickleファイルをロードする
    Mod: 2026.08.15    By: IWATA,Y.（AnalyzeFunctionsDT.py の LOAD_PICKLE を移植。
         元実装は FORCE フラグでキャッシュ無視・None返却に対応していたが、本関数は
         単純化して「読込み専用」とした。ファイルが無ければ FileNotFoundError を投げる
         ので、キャッシュの有無判定は呼び出し側（または load_or_create_pickle）で行う）
    """
    if not os.path.exists(file_):
        raise FileNotFoundError(f"Pickle File Not Found: {file_}")

    with open(file_, "rb") as f:
        obj = pickle.load(f)
    log(f"Pickle読込み: {file_}")

    return obj


def load_or_create_pickle(pickle_path: str, loader: Callable[[], Any]) -> Any:
    """
    pickle_path が存在すればそれをロードして返す（重い読込み・前処理をスキップする簡易キャッシュ）。
    存在しなければ loader() を呼び出して元データを読込み、その結果を pickle_path に保存してから返す。
    Mod: 2026.08.15    By: IWATA,Y.（AnalyzeFunctionsDT.py の SAVE_PICKLE/LOAD_PICKLE を
         「キャッシュがあれば使う、無ければ作って保存する」という1回の呼び出しにまとめた版。
         元の LOAD_PICKLE(FORCE, PKLFL) の FORCE引数（強制再読込み）に相当する動作が
         必要な場合は、呼び出し側で先に pickle_path を削除するか、本関数を使わず
         save_pickle/load_pickle を直接呼び分けること）

    Args:
        pickle_path (str): キャッシュ先の .pickle ファイルパス
        loader (Callable[[], Any]): pickle_path が存在しない場合に呼び出す、
            元データを読み込んで返す関数（引数なし）。
            例: lambda: pd.read_csv(csv_path) / lambda: read_segment_csv_dir(data_dir)
    Returns:
        Any: pickle_path から読み込んだ、または loader() が返したオブジェクト
    """
    if os.path.exists(pickle_path):
        return load_pickle(pickle_path)

    obj = loader()
    save_pickle(obj, pickle_path)

    return obj


########################################
# Parquet 保存・読込み
########################################

def save_parquet(df: pd.DataFrame, file_: str) -> str:
    """
    DataFrameをparquet形式で保存する（save_pickle と同じインターフェース）
    保存先ディレクトリが無ければ作成する
    """
    dir_ = os.path.dirname(file_)
    if dir_:
        os.makedirs(dir_, exist_ok=True)

    df.to_parquet(file_, index=False)
    log(f"Parquet保存: {file_}")

    return file_


def load_parquet(file_: str) -> pd.DataFrame:
    """
    parquetファイルをDataFrameとして読込む（load_pickle と同じインターフェース）
    ファイルが無ければ FileNotFoundError を投げる
    """
    if not os.path.exists(file_):
        raise FileNotFoundError(f"Parquet File Not Found: {file_}")

    df = pd.read_parquet(file_)
    log(f"Parquet読込み: {file_}")

    return df


def load_or_create_parquet(parquet_path: str, loader: Callable[[], pd.DataFrame]) -> pd.DataFrame:
    """
    parquet_path が存在すればそれをロードして返す（重い読込み・前処理をスキップする簡易キャッシュ）。
    存在しなければ loader() を呼び出してDataFrameを読込み、parquet_path に保存してから返す
    （load_or_create_pickle と同じキャッシュパターン。DataFrameに特化した版）。
    """
    if os.path.exists(parquet_path):
        return load_parquet(parquet_path)

    df = loader()
    save_parquet(df, parquet_path)

    return df


########################################
# check_df
########################################
def check_df(df: pd.DataFrame, col_def: Dict[str, Any]) -> pd.DataFrame:
    """
    列ごとの概要（型・値の範囲・ユニーク値件数）を算出する
    Mod: 2026.07.19    By: IWATA,Y.（AnalyzeFunctionsDT.py の CHK_DF を移植）
    """
    rows = []
    col_names_ordered = [col_def[str(i)][0] for i in range(len(col_def))]

    for name in col_names_ordered:
        if name not in df.columns:
            continue
        srs = df[name]
        n_unique = int(srs.nunique(dropna=True))
        unq_sorted = srs.dropna().sort_values().unique()

        if pd.api.types.is_numeric_dtype(srs):
            vmin, vmax = srs.min(), srs.max()
        else:
            vmin = unq_sorted[0] if len(unq_sorted) else None
            vmax = unq_sorted[-1] if len(unq_sorted) else None

        rows.append({
            "col_name": name,
            "dtype": str(srs.dtype),
            "count": int(srs.count()),
            "miss_num": int(srs.isnull().sum()),
            "n_unique": n_unique,
            "min": vmin,
            "max": vmax,
        })

    return pd.DataFrame(rows)


########################################
# Entry Point（単体テスト）
########################################
if __name__ == "__main__":
    import tempfile

    log("=== Parquet 単体テスト 開始 ===", "TEST")

    with tempfile.TemporaryDirectory() as tmp_dir:
        parquet_path = os.path.join(tmp_dir, "test.parquet")

        df_orig = pd.DataFrame({
            "col_a": [1, 2, 3],
            "col_b": [0.1, 0.2, 0.3],
            "col_c": ["x", "y", "z"],
        })

        # save_parquet / load_parquet 往復テスト
        save_parquet(df_orig, parquet_path)
        df_loaded = load_parquet(parquet_path)
        assert df_orig.equals(df_loaded), "往復テスト NG"
        log("save_parquet / load_parquet 往復: OK", "TEST")

        # load_or_create_parquet — キャッシュあり（loader が呼ばれないことを確認）
        call_count = {"n": 0}
        def _loader() -> pd.DataFrame:
            call_count["n"] += 1
            return df_orig

        load_or_create_parquet(parquet_path, _loader)
        assert call_count["n"] == 0, "キャッシュあり: loader が呼ばれた（NG）"
        log("load_or_create_parquet（キャッシュあり）: OK", "TEST")

        # load_or_create_parquet — キャッシュなし（loader が呼ばれ、ファイルが作成されることを確認）
        new_path = os.path.join(tmp_dir, "new.parquet")
        load_or_create_parquet(new_path, _loader)
        assert call_count["n"] == 1, "キャッシュなし: loader が呼ばれなかった（NG）"
        assert os.path.exists(new_path), "キャッシュなし: ファイルが作成されなかった（NG）"
        log("load_or_create_parquet（キャッシュなし）: OK", "TEST")

        # load_parquet — ファイルなし → FileNotFoundError
        try:
            load_parquet(os.path.join(tmp_dir, "nonexistent.parquet"))
            assert False, "FileNotFoundError が発生しなかった（NG）"
        except FileNotFoundError:
            log("load_parquet（ファイルなし）→ FileNotFoundError: OK", "TEST")

    log("=== Parquet 単体テスト 全件 OK ===", "TEST")
