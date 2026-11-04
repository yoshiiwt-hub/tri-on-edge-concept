# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# EDA Runner
# Ver: 01.f.20261102    By: IWATA, Y.'s Agent AI
# Mod: モジュール冒頭の Docstring を規約に合わせて簡素化
########################################
蓄積したデータを読み込み、探索的データ分析（EDA）の統計表とグラフをまとめて作る入口

主要関数:
load       # load_pp のデータロードモジュールを呼び、データと列定義を得る。
explore    # 統計量を求めて Excel に出力する（tri_s21_eda_profiler）。
visualise  # グラフを作る（tri_s21_eda_viz_interact / tri_s21_eda_viz_static）。

位置づけ: Stored 領域の分析の入口。データの読込み・統計・可視化のモジュールを、設定（tri_s20_setting.json）に従って動的に呼び出す。学習の入口は tri_s20_learn_us_runner。
設計意図: 読込み・統計・可視化を差し替えられる部品に分け、データセットが変わってもデータロードモジュールを1つ追加するだけで同じ分析を回せるようにする。
前提・制約: 設定は同じフォルダの tri_s20_setting.json から読む（設定項目の説明は同ファイルのコメントを参照）。単体で実行する（__main__）。
"""
MODULE_NAME = "Eda_Runner"

"""standard library"""
from   datetime import datetime as dt
import importlib
import json
import os
from   pathlib  import Path
import sys
import traceback
from   typing   import Any, Dict, List, Tuple, Sequence, Optional

"""external library"""
# (Not Using)

"""local module"""
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    sys_dir = Path(sys._MEIPASS)
else:
    sys_dir = Path(__file__).resolve().parent
if str(sys_dir) not in sys.path: sys.path.insert(0, str(sys_dir))
import tri_s29_util as utl


########################################
# Helpers
########################################
def _load_setting(json_path: str) -> dict:
    if not os.path.exists(json_path):
        print(f"[ERROR] 設定ファイルが見つかりません: {json_path}", flush=True)
        sys.exit(1)
    with open(json_path, encoding="utf-8") as f:
        return json.load(f)

def _resolve_outdir(outdir_val: str, json_path: str) -> str:
    if os.path.isabs(outdir_val):
        return outdir_val
    return os.path.normpath(os.path.join(os.path.dirname(json_path), outdir_val))


########################################
# 1. Load and Pre-Processing
########################################
def load(TITLE, MODULE, OUTPUT) -> Tuple:
    utl.log(f"Data Load Start.", MODULE_NAME)
    try:
        _module = importlib.import_module(f"{MODULE[0]}.{MODULE[1]}")
        if not hasattr(_module, MODULE[2]):
            raise AttributeError(f"{MODULE[0]}.{MODULE[1]} に {MODULE[2]} がありません")
        _func = getattr(_module, MODULE[2])
        
        df, col_def = _func()
        
        save_col_path = f"{OUTPUT}/{TITLE}_ColDef.json"
        utl.save_json(col_def, save_col_path)
        
    except Exception as e:
        utl.log(f"ERROR  {e}")
        print(traceback.format_exc(), flush=True)
        raise
    
    utl.log(f"Data Load End.", MODULE_NAME)
    return df, col_def


########################################
# 2. Explore Data
########################################
def explore(TITLE, MODULE, OUTPUT, df, col_def) -> None:
    utl.log(f"Explore Start.", MODULE_NAME)
    try:
        _module = importlib.import_module(f"{MODULE[0]}.{MODULE[1]}")
        if not hasattr(_module, MODULE[2]):
            raise AttributeError(f"{MODULE[0]}.{MODULE[1]} に {MODULE[2]} がありません")
        _func = getattr(_module, MODULE[2])
        _func(OUTPUT, TITLE, df, col_def)
        
    except Exception as e:
        utl.log(f"ERROR  {e}", MODULE_NAME)
        print(traceback.format_exc(), flush=True)
        raise

    utl.log(f"Explore End.", MODULE_NAME)


########################################
# 3. Visualise
########################################
def visualise(TITLE, MODULE, df, col_def, CONFIG) -> None:
    utl.log(f"Visualise Start.", MODULE_NAME)
    try:
        _module = importlib.import_module(f"{MODULE[0]}.{MODULE[1]}")
        if not hasattr(_module, MODULE[2]):
            raise AttributeError(f"{MODULE[0]}.{MODULE[1]} に {MODULE[2]} がありません")
        
        _func = getattr(_module, MODULE[2])
        _func(TITLE, df, col_def, CONFIG)

    except Exception as e:
        utl.log(f"ERROR  {e}", MODULE_NAME)
        print(traceback.format_exc(), flush=True)
        raise

    utl.log(f"Visualise End.", MODULE_NAME)


########################################
# Entry Point
########################################
if __name__ == "__main__":

    ################################
    # Load Config                  #
    ################################
    JSON_PATH   = os.path.join(os.path.dirname(__file__), "tri_s20_setting.json")
    setting     = _load_setting(JSON_PATH)
    TITLE       = setting["TITLE"]
    OUTDIR      = _resolve_outdir(setting["OUTDIR"], JSON_PATH)
    IS_WAVE     = setting["eda"]["IS_WAVE"]
    LOAD_MODULE = TITLE.lower()
    CONFIG      = {
        "OUTDIR"        : OUTDIR,
        "INTERACT_MODE" : setting["eda"]["INTERACT_MODE"],
        "graph_to_viz"  : setting["eda"]["graph_to_viz"],
    }
    os.makedirs(OUTDIR, exist_ok=True)

    ################################
    # Wave Data                    #
    ################################
    if IS_WAVE:
        
        # Load Data
        df, col_def = load(TITLE, ["load_pp", LOAD_MODULE, "load_wave_data"], CONFIG["OUTDIR"])

        # Interactive Plot
        visualise(TITLE, ["eda", "tri_s21_eda_viz_interact", "main"] , df, col_def, {**CONFIG, "graph_to_viz": ["overlap"]})

        exit(0)##
    
    ################################
    # Stat (Table) Data            #
    ################################
    # Load Data
    df, col_def = load(TITLE, ["load_pp", LOAD_MODULE, "load_data"], CONFIG["OUTDIR"])

    # Explore Data
    explore(TITLE, ["eda", "tri_s21_eda_profiler", "main"], CONFIG["OUTDIR"], df, col_def)
    
    # Stable Plot
    ##visualise(TITLE, ["eda", "tri_s21_eda_viz_static", "main"], df, col_def, CONFIG)

    # Interactive Plot
    visualise(TITLE, ["eda", "tri_s21_eda_viz_interact", "main"] , df, col_def, CONFIG)
    