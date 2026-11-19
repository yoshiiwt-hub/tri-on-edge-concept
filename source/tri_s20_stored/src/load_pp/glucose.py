# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Data Load Module [Glucose]
# Ver: 01.b.20261118    By: IWATA,Y.'s Agent AI
# Mod: キャッシュを utl.load_or_create_cache に変更（pyarrow が無い環境では pickle で保存する）
########################################
bokeh の glucose（センサの電流値と血糖値の時系列、5分間隔）を読み、分析に使えるデータと列定義にする（センサ値から測定値を推定する回帰の例）

主要関数:
load_data  # データと列定義を返す（1行 = 1時刻）。

位置づけ: データロードモジュールの1つ。設定の TITLE（Glucose）から選ばれ、tri_s20_eda_runner / tri_s20_learn_us_runner に呼ばれる。
設計意図: 読込みは bokeh_sampledata から行い、結果を cache/glucose_raw.parquet（pyarrow が無いときは .pickle）にキャッシュする。前処理は毎回実行する。
            対話的なグラフは質的変数が1つ以上ないと描けないため、日時から月（month）を作って質的変数にする。
前提・制約: bokeh_sampledata が必要（パッケージに同梱のデータを読むため、ネット接続は不要）。読み直す場合は cache/glucose_raw.parquet / .pickle を削除・退避する。
            isig が "nil"（欠測）の行は除く。波形モード（IS_WAVE）用の load_wave_data() は無い。
            戻り値の col_def は {"0": [name, dtype, num/cat, exp/obj, comment], ...}。
出典・ライセンス: bokeh のサンプルデータ（bokeh のメンテナが知人から直接受け取った測定記録）。パブリックドメイン（bokeh_sampledata のライセンス台帳による）。
"""
MODULE_NAME = "Glucose"

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
_CACHE_RAW = Path(__file__).resolve().parent / "cache" / "glucose_raw"  # 拡張子（.parquet / .pickle）は utl.load_or_create_cache が決める


########################################
# 1. Load Raw Data
########################################
def _00_load_raw() -> pd.DataFrame:
    """Load Raw Data（初回は bokeh_sampledata から読み、キャッシュする）"""
    utl.log("Load Raw Data Start.", MODULE_NAME)

    def _read_raw() -> pd.DataFrame:
        from bokeh_sampledata.glucose import data
        # parquet はインデックスを保存しないため、どちらの形式でも同じ列になるよう、日時のインデックスを列にしてからキャッシュする
        return data.reset_index().rename(columns={"datetime": "Timestamp"})

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
    df["isig"] = pd.to_numeric(df["isig"], errors="coerce")  # "nil"（欠測）は NaN になる
    n_raw = len(df)
    df = df.dropna(subset=["isig"]).reset_index(drop=True)
    utl.log(f"isig が欠測（nil）の {n_raw - len(df)} 行を除外", MODULE_NAME)
    df["month"] = df["Timestamp"].dt.strftime("%Y-%m")
    df = df[["Timestamp", "month", "isig", "glucose"]]
    print(f"df.shape={df.shape}", flush=True)##
    print(f"df.head=\n{df.head()}", flush=True)##

    col_def = {
        "0": ["Timestamp", "time" , "key"     , "key"        , "測定時刻（5分間隔）"],
        "1": ["month"    , "str"  , "category", "explanatory", "測定した月（Timestamp から作成）"],
        "2": ["isig"     , "float", "numetory", "explanatory", "センサの電流値"],
        "3": ["glucose"  , "int"  , "numetory", "objective"  , "血糖値（mg/dL）"],
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
