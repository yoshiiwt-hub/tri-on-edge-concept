# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Data Load Module [Guard Band Learning Data]
# Ver: 01.d.20260930    By: IWATA, Y.'s Agent AI
# Mod: _00_load_raw() に load_or_create_pickle() によるキャッシュを実装。
#      CSVセグメント群の読込み（utl.read_segment_csv_dir、ファイル数が多いと重い）の結果を
#      cache/sin_wave_raw.pickle に保存し、2回目以降はそちらから読む。前処理
#      （_10_preprocess）はキャッシュ対象に含めず、毎回実行する
#      （前処理ロジックを直した際に、古いキャッシュのせいで反映されない事故を防ぐため）。
#      強制的に読み直したい場合は、cache/sin_wave_raw.pickle を削除・退避すること。
########################################
データを読込み、DataFrameと列定義を返すデータロードモジュール

動作概要
    1. _00_load_raw()   : 生データを読込む（CSVセグメント群の読込み結果を pickle キャッシュする）
    2. _10_preprocess() : 前処理（キャッシュ対象外。毎回実行する）
    3. load_data()      : 公開用データアクセッサ

戻り値
    df      : pandas.DataFrame
    col_def : {"0": [name, dtype, num/cat, exp/obj, comment], ...}
"""
MODULE_NAME = "SinWave"

"""standard library"""
import os
from   pathlib  import Path
import sys
from   typing   import Any, Dict, List, Optional, Tuple

"""external library"""
import pandas as pd
pd.options.display.max_rows = None
pd.options.display.max_columns = None
pd.options.display.show_dimensions = True
pd.options.display.precision = 2
pd.options.display.float_format = '{:.2f}'.format

"""local module"""
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    sys_dir = Path(sys._MEIPASS)
else:
    sys_dir = Path(__file__).resolve().parent.parent
if str(sys_dir) not in sys.path: sys.path.insert(0, str(sys_dir))
import tri_s29_util as utl


########################################
# キャッシュ先パス
########################################
_CACHE_DIR     = Path(__file__).resolve().parent / "cache"
_CACHE_RAW_PKL = _CACHE_DIR / "sin_wave_raw.pickle"


########################################
# 1. Load Raw Data (Wave Data)
########################################
def _00_load_raw() -> Tuple[pd.DataFrame, List[str]]:
    """
    Load Raw Data
    Mod: 2026.08.15    By: IWATA,Y.（utl.load_or_create_pickle() でキャッシュ対応。
         CSVセグメント群の読込み（utl.read_segment_csv_dir）はファイル数が多いと重いため、
         結果を _CACHE_RAW_PKL に保存し、2回目以降はそちらから読む。
         強制的に読み直したい場合は、_CACHE_RAW_PKL（cache/sin_wave_raw.pickle）を
         削除するか退避すること）
    """

    def _read_raw() -> Tuple[pd.DataFrame, List[str]]:
        return utl.read_segment_csv_dir(
            data_dir=Path(__file__).resolve().parent / "data" / "sin" ,
            pfx_sfx=(["sin_wave_"], [".csv"]),
            search_subdir=False,
            encodings=("utf-8-sig", "cp932", "euc-jp"),
            data_start_row=5,
            header_row_idx=0,
        )

    df_raw, header_row = utl.load_or_create_pickle(str(_CACHE_RAW_PKL), _read_raw)
    return df_raw, header_row


########################################
# 2. Pre-Processing (Wave Data)
########################################
def _10_preprocess(df_raw: pd.DataFrame, header_row: List[str]) -> "tuple[pd.DataFrame, dict]":
    """Pre-Processing"""

    df = df_raw.drop("seg_index", axis=1)
    print(f"df.shape={df.shape}", flush=True)##
    print(f"df.head=\n{df.head()}", flush=True)##
    
    col_def = {
        "0": ["seg_id"    , "str" , "key", "key", "Segment ID"] ,
        "1": ["seg_ind"   , "int" , "key", "key", "Segment Row Index"] ,
        "2": ["seg_ts"    , "time", "key", "key", "Timestamp When Data Collected"] ,
        "3": ["seg_status", "int" , "key", "key", "Segment Process Status"] ,
        
        "4": ["Timestamp", "time" , "key"     , "key"        , "データ発生時の時刻"] ,
        "5": ["LOT"      , "str"  , "key"     , "key"        , "製造id"] ,
        "6": ["Sin"      , "float", "numetory", "explanatory", "軸１トルク電流値（単位：Ｎ・ｍ／Ａ)）"] ,
        "7": ["Cos"      , "float", "numetory", "explanatory", "押出速度（単位：ｍｍ／ｓｅｃ）"] ,
        "8": ["Tan"      , "float", "numetory", "explanatory", "機内温度（単位：℃）"] ,
        "9": ["CL"       , "int"  , "category", "objective"  , "正常／異常"] ,
        
        "10": ["dev_name"   , "str" , "key", "key", "dev_name"] ,
        "11": ["dev_ip"     , "str" , "key", "key", "dev_ip"] ,
        "12": ["kotei_name" , "str" , "key", "key", "kotei_name"] ,
    }
    
    print("col_def=", flush=True)##
    for k, v in col_def.items():
        print(f"{k} : {v}", flush=True)##
    
    return df, col_def


########################################
# 3. Data Accessor (Wave Data)
########################################
def load_wave_data() -> "tuple[pd.DataFrame, dict]":
    """Data Accessor for External Module"""

    df_raw, header_row = _00_load_raw()
    df, col_def = _10_preprocess(df_raw, header_row)

    return df, col_def


########################################
# 4. Segment Stats（波形統計量）
########################################
def _default_stat_register_map() -> "tuple[dict, dict]":
    """
    load_stat_data() のデフォルトの統計量定義（column_register_map / out_coldef）。
    Sin/Cos/Tan（col_defの列インデックス6/7/8）それぞれについて mean/std/min/max/med を、
    CL（目的変数。セグメント内では一定値の想定）・LOT（同上）を mode（最頻値、実質「その1値」）
    で1セグメント1値ずつ計算する。
    Mod: 2026.08.09    By: IWATA,Y.

    column_register_map の "index": [src_col_idx, "stat_name"] における src_col_idx は、
    load_wave_data() が返す col_def の列インデックス（0始点）と一致させている
    （0:seg_id 1:seg_ind 2:seg_ts 3:seg_status 4:Timestamp 5:LOT 6:Sin 7:Cos 8:Tan 9:CL ...）。
    """
    stat_targets = [("Sin", 6), ("Cos", 7), ("Tan", 8)]
    stat_kinds   = ["mean", "std", "min", "max", "med"]

    column_register_map: Dict[str, Any] = {}
    out_coldef:           Dict[str, Any] = {}

    idx = 0
    for col_name, src_idx in stat_targets:
        for kind in stat_kinds:
            column_register_map[str(idx)] = [src_idx, kind]
            out_coldef[str(idx)] = [
                f"{col_name}_{kind}", "float", "numetory", "explanatory",
                f"{col_name}の{kind}（セグメント内）"
            ]
            idx += 1

    # 目的変数(CL)・LOT（いずれもセグメント内では一定値の想定）は mode（最頻値）で代表値を取る
    column_register_map[str(idx)] = [9, "mode"]
    out_coldef[str(idx)] = ["CL", "int", "category", "objective", "正常／異常"]
    idx += 1

    column_register_map[str(idx)] = [5, "mode"]
    out_coldef[str(idx)] = ["LOT", "str", "key", "key", "製造id"]

    return column_register_map, out_coldef


########################################
# 5. Data Accessor (Stat Data)
########################################
def load_data(
    column_register_map: "Optional[Dict[str, Any]]" = None,
    out_coldef:           "Optional[Dict[str, Any]]" = None,
) -> "tuple[pd.DataFrame, dict]":
    """
    Data Accessor for External Module（波形統計量版）
    Mod: 2026.08.09    By: IWATA,Y.

    load_wave_data() が返す波形データ（1セグメント=複数行）を、seg_id ごとにグループ化し、
    tri_s29_calc_stats.py の compile_register_map() / build_stats() を使って
    1セグメント=1行の統計量（Sin/Cos/Tan の mean/std/min/max/med 等）に変換する。

    column_register_map / out_coldef を省略した場合は _default_stat_register_map() の
    既定値（Sin/Cos/Tan の mean/std/min/max/med + CL/LOT）を使う。呼び出し側で独自の
    統計量定義を渡したい場合は、tri_s29_calc_stats.compile_register_map() の
    docstring（column_register_map / output_column のフォーマット）を参照して指定すること。

    Returns:
        df      : pandas.DataFrame（1行=1セグメントの統計量。seg_id/seg_ts_start/seg_ts_end/
                  seg_rows は常に付与され、それ以降が column_register_map で指定した統計量）
        col_def : {"0": [name, dtype, role, var_type, comment], ...}
    """
    from tri_s29_calc_stats import compile_register_map, build_stats

    utl.log(f"Load Stat Data Start.", MODULE_NAME)

    df, col_def = load_wave_data()
    
    #### 統計量定義 ####
    '''column_register_map = {
          "0": [-1, "seg_len"]
        , "1": [6,  "mean"]
        , "2": [6,  "mean", [0.25, 0.75]]
        , "3": [6,  "med"]
        , "4": [6,  "min"]
        , "5": [6,  "max"]
        , "6": [6,  "range"]
        , "7": [6,  "std"]
        , "8": [6,  "count"]
        , "9": [6,  "n_null"]
        , "10": [6, "pertl", 25]
        , "11": [6, "pertl", 75]
        , "12": [6, "iqr"]
        , "13": [6, "sig_u", 3]
        , "14": [6, "sig_l", 3]
        , "15": [5, "mode"]
        , "16": [5, "nth",  1]
        , "17": [5, "nth", -1]
        , "18": [5, "nth", [0.25, 0.75], 10]
    }
    
    out_coldef = {
          "0":  ["Seg_Len",              "int",   "numetory", "explanatory", "区間長"]
        , "1":  ["Sin_mean",             "float", "numetory", "explanatory", "平均＿全区間"]
        , "2":  ["Sin_mean_025_075",     "float", "numetory", "explanatory", "平均＿25～75％区間"]
        , "3":  ["Sin_med",              "float", "numetory", "explanatory", "中央値＿全区間"]
        , "4":  ["Sin_min",              "float", "numetory", "explanatory", "最小値＿全区間"]
        , "5":  ["Sin_max",              "float", "numetory", "explanatory", "最大値＿全区間"]
        , "6":  ["Sin_range",            "float", "numetory", "explanatory", "範囲＿全区間"]
        , "7":  ["Sin_std",              "float", "numetory", "explanatory", "標準偏差＿全区間"]
        , "8":  ["Sin_count",            "int",   "numetory", "explanatory", "個数＿全区間"]
        , "9":  ["Sin_n_null",           "int",   "numetory", "explanatory", "Null数＿全区間"]
        , "10": ["Sin_pertl_25",         "float", "numetory", "explanatory", "第１四分位＿全区間"]
        , "11": ["Sin_pertl_75",         "float", "numetory", "explanatory", "第３四分位＿全区間"]
        , "12": ["Sin_iqr",              "float", "numetory", "explanatory", "IQR＿全区間"]
        , "13": ["Sin_sig_u",            "float", "numetory", "explanatory", "上方３σ＿全区間"]
        , "14": ["Sin_sig_l",            "float", "numetory", "explanatory", "下方３σ＿全区間"]
        , "15": ["LOT_mode",             "str",   "category", "explanatory",    "最頻値＿全区間"]
        , "16": ["LOT_first",            "str",   "category", "explanatory",    "最初行＿全区間"]
        , "17": ["LOT_last",             "str",   "category", "explanatory",    "最終行＿全区間"]
        , "18": ["LOT_nth_025_075_10",   "str",   "category", "explanatory",    "１０行目＿25～75％区間"]
    }'''
    
    if column_register_map is None or out_coldef is None:
        column_register_map, out_coldef = _default_stat_register_map()
    ################

    col_keys = [col_def[str(i)][0] for i in range(len(col_def))]
    compiled_map = compile_register_map(column_register_map, out_coldef, col_keys)
    utl.log(f"compiled_map entries={len(compiled_map)}", MODULE_NAME)

    rows: List[Dict[str, Any]] = []
    for seg_id, seg_df in df.groupby("seg_id", sort=False):
        # build_stats() は seg[0][0]=seg_id, seg[0][2]=seg_ts_start, seg[-1][2]=seg_ts_end という
        # 行構造を前提とする。col_keys（col_defの並び順）でリスト化すればこの前提と一致する
        # （0:seg_id 2:seg_ts のため）。
        seg = seg_df[col_keys].values.tolist()
        stats_dict = build_stats(seg, compiled_map)
        rows.append(stats_dict)

    df_stat = pd.DataFrame(rows)
    print(f"df_stat.shape={df_stat.shape}", flush=True)##
    print(f"df_stat.head=\n{df_stat.head()}", flush=True)##

    # ---- 出力col_def組み立て：固定ヘッダ(seg_id/seg_ts_start/seg_ts_end/seg_rows) + out_coldef ----
    stat_col_def: Dict[str, List[Any]] = {
        "0": ["seg_id",       "str",  "key", "key", "Segment ID"],
        "1": ["seg_ts_start", "time", "key", "key", "セグメント開始時刻"],
        "2": ["seg_ts_end",   "time", "key", "key", "セグメント終了時刻"],
        "3": ["seg_rows",     "int",  "key", "key", "セグメント行数"],
    }
    next_idx = len(stat_col_def)
    for k in sorted(out_coldef.keys(), key=int):
        stat_col_def[str(next_idx)] = list(out_coldef[k])
        next_idx += 1

    print("col_def=", flush=True)##
    for k, v in stat_col_def.items():
        print(f"{k} : {v}", flush=True)##

    utl.log(f"Load Stat Data End.  df_stat.shape={df_stat.shape}", MODULE_NAME)
    return df_stat, stat_col_def


########################################
# Entry Point
########################################
if __name__ == "__main__":
    ##load_wave_data()
    df, col = load_data()
    
    print("________________________________")##
    for c in list(df.columns):
        print(c,'\n')##
        print(df[[c]].head(5),'\n')##
        print(df[[c]].describe(),'\n')##
        print("________________________________")##
