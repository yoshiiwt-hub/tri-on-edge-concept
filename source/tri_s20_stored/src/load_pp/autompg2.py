# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Data Load Module [Autompg2]
# Ver: 01.b.20261118    By: IWATA,Y.'s Agent AI
# Mod: キャッシュを utl.load_or_create_cache に変更（pyarrow が無い環境では pickle で保存する）
########################################
bokeh の autompg2（1999・2008年の車種別の燃費と諸元）を読み、分析に使えるデータと列定義にする（工業製品の回帰の例）

主要関数:
load_data  # データと列定義を返す。

位置づけ: データロードモジュールの1つ。設定の TITLE（Autompg2）から選ばれ、tri_s20_eda_runner / tri_s20_learn_us_runner に呼ばれる。
設計意図: 読込みは bokeh_sampledata から行い、結果を cache/autompg2_raw.parquet（pyarrow が無いときは .pickle）にキャッシュする。前処理は毎回実行する。
前提・制約: bokeh_sampledata が必要（パッケージに同梱のデータを読むため、ネット接続は不要）。読み直す場合は cache/autompg2_raw.parquet / .pickle を削除・退避する。
            cty（市街地の燃費）は目的変数 hwy（高速道路の燃費）とほぼ同じ量で、説明変数にするとリークになるため key として分析から外す。
            seaborn の mpg（1970～82年の UCI データ）とは別のデータ。
            戻り値の col_def は {"0": [name, dtype, num/cat, exp/obj, comment], ...}。
出典・ライセンス: 米国 EPA の燃費データ（FuelEconomy.gov。ggplot2 の mpg と同じ抜粋）。パブリックドメイン（bokeh_sampledata のライセンス台帳による）。
            クレジット表記：U.S. Environmental Protection Agency vehicle fuel economy data, provided through FuelEconomy.gov by DOE and EPA.
"""
MODULE_NAME = "Autompg2"

"""standard library"""
import sys
from pathlib import Path

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
_CACHE_RAW = Path(__file__).resolve().parent / "cache" / "autompg2_raw"  # 拡張子（.parquet / .pickle）は utl.load_or_create_cache が決める


########################################
# 1. Load Raw Data
########################################
def _00_load_raw() -> pd.DataFrame:
    """Load Raw Data（初回は bokeh_sampledata から読み、キャッシュする）"""
    utl.log("Load Raw Data Start.", MODULE_NAME)

    def _read_raw() -> pd.DataFrame:
        from bokeh_sampledata.autompg2 import autompg2
        return autompg2.copy()

    df_raw = utl.load_or_create_cache(str(_CACHE_RAW), _read_raw)
    print(f"df_raw.shape={df_raw.shape}", flush=True)##

    utl.log("Load Raw Data End.\n", MODULE_NAME)
    return df_raw


########################################
# 2. Pre-Processing
########################################
def _10_preprocess(df_raw: pd.DataFrame) -> "tuple[pd.DataFrame, dict]":
    """Pre-Processing"""
    utl.log("Pre-Processing Start.", MODULE_NAME)

    df = df_raw.drop(columns=["Unnamed: 0"])  # 元の CSV の行番号
    print(f"df.shape={df.shape}", flush=True)##
    print(f"df.head=\n{df.head()}", flush=True)##

    col_def = {
        "0":  ["manufacturer", "str"  , "category", "explanatory", "メーカー"],
        "1":  ["model"       , "str"  , "key"     , "key"        , "車種名"],
        "2":  ["displ"       , "float", "numetory", "explanatory", "排気量（リットル）"],
        "3":  ["year"        , "int"  , "category", "explanatory", "年式（1999, 2008）"],
        "4":  ["cyl"         , "int"  , "category", "explanatory", "気筒数"],
        "5":  ["trans"       , "str"  , "category", "explanatory", "変速機（auto / manual と段数）"],
        "6":  ["drv"         , "str"  , "category", "explanatory", "駆動方式（front, rear, 4x4）"],
        "7":  ["cty"         , "int"  , "key"     , "key"        , "市街地の燃費（マイル／ガロン）。hwy とほぼ同じ量でリークになるため分析から外す"],
        "8":  ["hwy"         , "int"  , "numetory", "objective"  , "高速道路の燃費（マイル／ガロン）"],
        "9":  ["fl"          , "str"  , "category", "explanatory", "燃料の種類"],
        "10": ["class"       , "str"  , "category", "explanatory", "車の分類（compact, suv など）"],
    }

    print("col_def=", flush=True)
    for k, v in col_def.items():
        print(f"{k} : {v}", flush=True)##

    utl.log("Pre-Processing End.\n", MODULE_NAME)
    return df, col_def


########################################
# 3. Data Accessor
########################################
def load_data() -> "tuple[pd.DataFrame, dict]":
    """Data Accessor for External Module"""

    df_raw      = _00_load_raw()
    df, col_def = _10_preprocess(df_raw)
    print(utl.check_df(df, col_def), flush=True)##

    return df, col_def


########################################
# Entry Point
########################################
if __name__ == "__main__":
    load_data()
