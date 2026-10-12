# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# EDA Runner
# Ver: 01.e.20260930    By: IWATA, Y.'s Agent AI
# Mod: __main__ブロックの設定を tri_s20_setting.json から読み込む方式に変更。
#      _load_setting() / _resolve_outdir() ヘルパー関数を追加。
########################################
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
    