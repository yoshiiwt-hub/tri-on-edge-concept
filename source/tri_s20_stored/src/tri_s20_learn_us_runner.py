# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Learn Runner
# Ver: 02.c.20260930    By: IWATA, Y.'s Agent AI
# Mod: __main__ブロックの設定を tri_s20_setting.json から読み込む方式に変更。
#      _load_setting() / _resolve_outdir() ヘルパー関数を追加。
########################################
"""
MODULE_NAME = "Learn_Runner"

"""standard library"""
import importlib
import json
import os
from   pathlib  import Path
import sys
import traceback
from   typing   import Any, Dict, List, Tuple

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
# 1. Load Data
########################################
def load(TITLE, MODULE, OUTPUT) -> Tuple:
    """
    MODULE = [package, module_name, func_name]。load_pp配下のデータロードモジュールを
    動的importして呼び出す（tri_s20_eda_runner.py の load() と同じ構成）。
    func_name には、(df, col_def) を返す関数であれば何を指定してもよい
    （例: sin_wave.py の load_wave_data()＝波形データ、load_data()＝波形統計量。
    今後 load_pp/ に追加する他モジュールの関数も同様に指定可能）。
    """
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
        utl.log(f"ERROR  {e}", MODULE_NAME)
        print(traceback.format_exc(), flush=True)
        raise

    utl.log(f"Data Load End.", MODULE_NAME)
    return df, col_def


########################################
# 2. Learn
########################################
def learn(TITLE, MODULE, df, col_def, CONFIG) -> None:
    """
    MODULE = [package, module_name, func_name]。learn配下の学習モジュールを動的import
    して呼び出す（tri_s20_eda_runner.py の visualise() と同じ構成・引数順）。
    呼び出し先の関数は main(TITLE, df, col_def, config) の形式であること
    （tri_s21_learn_us_gb.py / tri_s21_learn_us_mt.py の main() 参照）。
    """
    utl.log(f"Learn Start.", MODULE_NAME)
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

    utl.log(f"Learn End.", MODULE_NAME)


########################################
# Entry Point
########################################
if __name__ == "__main__":

    ################################
    # Load Config                  #
    ################################
    JSON_PATH    = os.path.join(os.path.dirname(__file__), "tri_s20_setting.json")
    setting      = _load_setting(JSON_PATH)
    TITLE        = setting["TITLE"]
    OUTDIR       = _resolve_outdir(setting["OUTDIR"], JSON_PATH)
    LOAD_MODULE  = TITLE.lower()
    LEARN_MODULE = setting["learn"]["LEARN_MODULE"]
    LOAD_FUNC    = setting["learn"]["LOAD_FUNC"]
    config_key   = setting["learn"]["LEARN_CONFIG_KEY"]
    LEARN_CONFIG = {**setting["learn"][config_key], "outdir": OUTDIR}
    os.makedirs(OUTDIR, exist_ok=True)
    
    # Load Data
    df, col_def = load(TITLE, ["load_pp", LOAD_MODULE, LOAD_FUNC], OUTDIR)

    # Learn
    learn(TITLE, ["learn", LEARN_MODULE, "main"], df, col_def, LEARN_CONFIG)
