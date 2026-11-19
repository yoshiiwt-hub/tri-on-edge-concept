# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Data Load Module [Diamonds]
# Ver: 01.c.20261118    By: IWATA,Y.'s Agent AI
# Mod: キャッシュを utl.load_or_create_cache に変更（pyarrow が無い環境では pickle で保存する）
########################################
seaborn の diamonds（ダイヤの価格と属性）を読み、分析に使えるデータと列定義にする（工業製品の回帰の例）

主要関数:
load_data  # データと列定義を返す。

位置づけ: データロードモジュールの1つ。設定の TITLE（Diamonds）から選ばれ、tri_s20_eda_runner / tri_s20_learn_us_runner に呼ばれる。
設計意図: 読込みは seaborn から行い、結果を cache/diamonds_raw.parquet（pyarrow が無いときは .pickle）にキャッシュする（seaborn のデータは予告なく変わることがあるため、初回の内容を固定する）。前処理は毎回実行する。
前提・制約: 初回だけネット接続が必要（seaborn-data の GitHub から取得）。読み直す場合は cache/diamonds_raw.parquet / .pickle を削除・退避する。
            x / y / z が 0 の行（測定値の異常）は、EDA で見えるようにあえて残す。
            戻り値の col_def は {"0": [name, dtype, num/cat, exp/obj, comment], ...}。
出典・ライセンス: ggplot2 に同梱のデータ（https://ggplot2.tidyverse.org/reference/diamonds.html）を seaborn-data が再配布したもの。
            元の出典・データのライセンスは明記されていない（ggplot2 パッケージ自体は MIT）。データ自体は本リポジトリに含めない（利用者が seaborn-data から取得する）。
            利用条件は、利用者が確認すること。
"""
MODULE_NAME = "Diamonds"

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
_CACHE_RAW = Path(__file__).resolve().parent / "cache" / "diamonds_raw"  # 拡張子（.parquet / .pickle）は utl.load_or_create_cache が決める


########################################
# 1. Load Raw Data
########################################
def _00_load_raw() -> pd.DataFrame:
    """Load Raw Data（初回は seaborn から取得し、キャッシュする）"""
    utl.log("Load Raw Data Start.", MODULE_NAME)

    def _read_raw() -> pd.DataFrame:
        import seaborn as sns
        return sns.load_dataset("diamonds")

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

    df = df_raw.copy()
    for c in ["cut", "color", "clarity"]:
        df[c] = df[c].astype(str)  # seaborn は category 型で返すため、他のデータセットと同じ文字列にそろえる
    print(f"df.shape={df.shape}", flush=True)##
    print(f"df.head=\n{df.head()}", flush=True)##

    col_def = {
        "0": ["carat"  , "float", "numetory", "explanatory", "重さ（カラット）"],
        "1": ["cut"    , "str"  , "category", "explanatory", "カットの品質（Fair, Good, Very Good, Premium, Ideal）"],
        "2": ["color"  , "str"  , "category", "explanatory", "色（D が最良 ～ J が最低）"],
        "3": ["clarity", "str"  , "category", "explanatory", "透明度（I1 が最低 ～ IF が最良）"],
        "4": ["depth"  , "float", "numetory", "explanatory", "深さの比率（%）＝ z / mean(x, y)"],
        "5": ["table"  , "float", "numetory", "explanatory", "テーブル幅の比率（%）＝ 上面の幅 / 最も広い部分の幅"],
        "6": ["price"  , "int"  , "numetory", "objective"  , "価格（米ドル）"],
        "7": ["x"      , "float", "numetory", "explanatory", "長さ（mm）"],
        "8": ["y"      , "float", "numetory", "explanatory", "幅（mm）"],
        "9": ["z"      , "float", "numetory", "explanatory", "高さ（mm）"],
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
