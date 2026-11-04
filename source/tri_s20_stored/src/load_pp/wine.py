# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Data Load Module [Wine]
# Ver: 01.d.20261102    By: IWATA, Y.'s Agent AI
# Mod: モジュール冒頭の Docstring を規約に合わせて簡素化
########################################
Wine のデータセットを読み、分析に使えるデータと列定義にする（データロードモジュールの見本）

主要関数:
load_data  # データと列定義を返す。

位置づけ: データロードモジュールの1つ。設定の TITLE（Wine）から選ばれ、tri_s20_eda_runner に呼ばれる。新しいデータセットのモジュールを作るときの見本。
設計意図: 読込み（_00_load_raw）と前処理（_10_preprocess）を分け、データセットごとに書き換える箇所をはっきりさせる。
前提・制約: 戻り値の col_def は {"0": [name, dtype, num/cat, exp/obj, comment], ...}。
"""
MODULE_NAME = "Wine"

"""standard library"""
import os
import sys
from pathlib import Path

"""external library"""
import pandas as pd
pd.options.display.max_rows = None
pd.options.display.max_columns = None
pd.options.display.show_dimensions = True
pd.options.display.precision = 2
pd.options.display.float_format = '{:.2f}'.format
from   sklearn.datasets import load_wine

"""local module"""
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    sys_dir = Path(sys._MEIPASS)
else:
    sys_dir = Path(__file__).resolve().parent.parent
if str(sys_dir) not in sys.path: sys.path.insert(0, str(sys_dir))
import tri_s29_util as utl


########################################
# 1. Load Raw Data
########################################
def _00_load_raw() -> pd.DataFrame:
    """Load Raw Data"""
    utl.log(f"Load Raw Data Start.", MODULE_NAME)

    data = load_wine()
    df_raw = pd.DataFrame(data.data, columns=data.feature_names)
    df_raw["quality"] = pd.Series(data.target)
    print(f"df_raw.shape={df_raw.shape}", flush=True)##

    utl.log(f"Load Raw Data End.\n", MODULE_NAME)
    return df_raw


########################################
# 2. Pre-Processing
########################################
def _10_preprocess(df_raw: pd.DataFrame) -> "tuple[pd.DataFrame, dict]":
    """Pre-Processing"""
    utl.log(f"Pre-Processing Start.", MODULE_NAME)
    
    df = df_raw.rename(columns={"od280/od315_of_diluted_wines": "od280od315"})
    feature_names = [c for c in df.columns if c != "quality"]
    print(f"df.shape={df.shape}", flush=True)##
    print(f"df.head=\n{df.head()}", flush=True)##

    col_def = {}
    for i, name in enumerate(feature_names):
        col_def[str(i)] = [name, "float", "numetory", "explanatory", f"{name}（ワイン成分値）"]
    
    col_def[str(len(feature_names))] = ["quality", "int", "category", "objective", "ワイン品種クラス"]
    
    print("col_def=", flush=True)
    for k, v in col_def.items():
        print(f"{k} : {v}", flush=True)##
    
    utl.log(f"Pre-Processing End.\n", MODULE_NAME)
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
