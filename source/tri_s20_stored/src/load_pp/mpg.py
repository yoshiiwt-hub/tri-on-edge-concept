# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Data Load Module [Mpg]
# Ver: 01.b.20261118    By: IWATA,Y.'s Agent AI
# Mod: キャッシュを utl.load_or_create_cache に変更（pyarrow が無い環境では pickle で保存する）
########################################
seaborn の mpg（1970～82年の自動車の燃費と諸元）を読み、分析に使えるデータと列定義にする（工業製品の回帰の例）

主要関数:
load_data  # データと列定義を返す。

位置づけ: データロードモジュールの1つ。設定の TITLE（Mpg）から選ばれ、tri_s20_eda_runner / tri_s20_learn_us_runner に呼ばれる。
設計意図: 読込みは seaborn から行い、結果を cache/mpg_raw.parquet（pyarrow が無いときは .pickle）にキャッシュする（seaborn のデータは予告なく変わることがあるため、初回の内容を固定する）。前処理は毎回実行する。
前提・制約: 初回だけネット接続が必要（seaborn-data の GitHub から取得）。読み直す場合は cache/mpg_raw.parquet / .pickle を削除・退避する。
            horsepower が欠損した6行は除く。bokeh の autompg2（1999・2008年の EPA データ）とは別のデータ。
            戻り値の col_def は {"0": [name, dtype, num/cat, exp/obj, comment], ...}。
出典・ライセンス: UCI Machine Learning Repository の Auto MPG（398件）を seaborn-data が少し加工したもの（origin を文字にする等）。
            ライセンスは CC BY 4.0。利用時は次の出典を明記する：
            Quinlan, R. (1993). Auto MPG [Dataset]. UCI Machine Learning Repository. https://doi.org/10.24432/C5859H.
"""
MODULE_NAME = "Mpg"

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
_CACHE_RAW = Path(__file__).resolve().parent / "cache" / "mpg_raw"  # 拡張子（.parquet / .pickle）は utl.load_or_create_cache が決める


########################################
# 1. Load Raw Data
########################################
def _00_load_raw() -> pd.DataFrame:
    """Load Raw Data（初回は seaborn から取得し、キャッシュする）"""
    utl.log("Load Raw Data Start.", MODULE_NAME)

    def _read_raw() -> pd.DataFrame:
        import seaborn as sns
        return sns.load_dataset("mpg")

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

    n_raw = len(df_raw)
    df = df_raw.dropna(subset=["horsepower"]).reset_index(drop=True)
    utl.log(f"horsepower が欠損した {n_raw - len(df)} 行を除外", MODULE_NAME)
    print(f"df.shape={df.shape}", flush=True)##
    print(f"df.head=\n{df.head()}", flush=True)##

    col_def = {
        "0": ["mpg"         , "float", "numetory", "objective"  , "燃費（マイル／ガロン）"],
        "1": ["cylinders"   , "int"  , "category", "explanatory", "気筒数"],
        "2": ["displacement", "float", "numetory", "explanatory", "排気量（立方インチ）"],
        "3": ["horsepower"  , "float", "numetory", "explanatory", "馬力"],
        "4": ["weight"      , "int"  , "numetory", "explanatory", "車両重量（ポンド）"],
        "5": ["acceleration", "float", "numetory", "explanatory", "0-60mph の加速時間（秒）"],
        "6": ["model_year"  , "int"  , "numetory", "explanatory", "年式（19xx 年の下2桁）"],
        "7": ["origin"      , "str"  , "category", "explanatory", "生産地域（usa, japan, europe）"],
        "8": ["name"        , "str"  , "key"     , "key"        , "車名"],
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
