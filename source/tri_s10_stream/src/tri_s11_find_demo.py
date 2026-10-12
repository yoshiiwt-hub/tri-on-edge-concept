# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [Demo]
# Ver: 06.e.20261001    By: IWATA, Y.'s Agent AI
# Mod: モジュール名を tri_edge_XX_* から tri_sXX_* 形式に変更（tri_edge_11_find_demo → tri_s11_find_demo）
########################################
f_demo_random_trigger  # デモ用乱数を生成してキューに送信する Find Trigger
f_demo_csv_trigger     # デモ用 CSV ファイルを1行ずつ読み込んでキューに送信する Find Trigger

動作概要
    1. f_demo_random_trigger: col_def の dtype に従いランダム値を生成してポーリング送信
    2. f_demo_csv_trigger: デモ用 CSV ファイルを指定インターバルで1行ずつキューに送信
    3. どちらも stop_event が立つまでループを継続
    4. 動作確認・テスト目的のスタンドアロン実行にも対応
"""
MODULE_NAME = "FIND_Demo"

"""standard library"""
import csv
from   datetime import datetime as dt
import json
import os
import queue
import random
import shutil # copy file
import socket
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Tuple, Sequence, Optional

"""external library"""
## NOT Use

"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()
    
except ModuleNotFoundError:
    try: # Check and Add Path
        cur_dir  = os.path.dirname(os.path.abspath(__file__))
        dir_name = os.path.basename(cur_dir)
        if dir_name != 'src':
            raise FileNotFoundError(f".py MUST be in 'src' dir. Current: {dir_name}")
        
        root_dir = os.path.dirname(cur_dir)
        if root_dir not in sys.path:
            sys.path.append(root_dir)
        
        for i in range(10):
            if cur_dir in sys.path:
                sys.path.remove(cur_dir)
            else:
                break
        
        import src.tri_s00_supervise_base as base
        sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully after Path Adjusted.\n")
        sys.stdout.flush()
        
    except Exception as e:
        msg = f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]    {e}\n{traceback.format_exc()}"
        sys.stdout.write(f"{msg}\n")
        sys.stdout.flush()
        
        with open(f"{MODULE_NAME}_Import_Error.log", "a", encoding="utf-8-sig") as f:
            f.write(f"{msg}\n")
        sys.exit(1)

"""local module"""
import src.tri_s01_error_if       as err
from   src.tri_s01_error_if       import ErrorIF, SIGNAL_STOP

"""system dir, file"""
system_root = base.get_system_root()
log_dir  = base.get_log_dir()
log_file = base.get_log_path()
work_dir = base.get_work_dir()
work_db_dir    = base.get_work_sub_dirs().get("db"   , os.path.join(work_dir, "db"))
work_run_dir   = base.get_work_sub_dirs().get("run"  , os.path.join(work_dir, "run"))
work_spool_dir = base.get_work_sub_dirs().get("spool", os.path.join(work_dir, "spool"))

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__=='__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# Find Trigger Functions
########################################

def f_demo_random_trigger(
    queue_out: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    デモ）乱数を生成してキューに送信
    Mod: 2026.03.31    By: IWATA,Y.
    """
    TRIGGER_NAME = "f_demo_random_trigger"

    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)
        
        time_func   = dt.now()##
        int_range   = config.get("int_range", [0, 100])
        float_range = config.get("float_range", [0, 100])
        float_round = config.get("float_round", 2)
        str_list    = config.get("str_list", ["ABC", "DEF", "XYZ"])
        
        """定義から key, dtype を取得"""
        key_actual   = [col_def[k][0] for k in sorted(col_def.keys())]
        dtype_actual = [col_def[k][1] for k in sorted(col_def.keys())]
        
    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None
    
    """======== LOOP START ========"""
    count = 0
    while not stop_event.is_set():
        try:
            """dtype に応じてランダム値を生成"""
            value = []
            for dtype in dtype_actual:
                if   dtype == "time":  value.append(time_func)
                elif dtype == "int":   value.append(random.randint(int_range[0], int_range[1]))
                elif dtype == "float": value.append(round(random.uniform(float_range[0], float_range[1]), float_round))
                elif dtype == "str":   value.append(random.choice(str_list))
                else:                  value.append(None)
            
            queue_out.put(
                (
                    count,
                    value
                )
            )
            ## (Viz to Debug)
            _timestamp  = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]##
            _dev_name = base.load_device_info()["dev_name"]##
            sys.stdout.write(f"{count:<3}  {_timestamp}  {_dev_name}  {value}\n")##
            sys.stdout.flush()##
            
            count += 1
            time.sleep(0.5)

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="data", trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue
    """======== LOOP END ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None


def f_demo_csv_trigger(
    queue_out: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    デモ）CSVを読み込んでキューに送信
    Mod: 2026.04.19    By: IWATA,Y.
    """
    TRIGGER_NAME = "f_demo_csv_trigger"

    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)
        key_define   = [col_def[k][0] for k in sorted(col_def.keys())]
        ##dtype_define = [col_def[k][1] for k in sorted(col_def.keys())]
        
        # config
        file_path = config.get("file_path")
        encode = config.get("encode", "utf-8")
        interval = config.get("interval", 0.1)
        
        # dtype cast plan
        default_val_config = {
            "time_format"  : config.get("time_format", "%Y-%m-%d %H:%M:%S.%f") ,
            "time_default" : config.get("time_default", "2999-01-01 00:00:00.000000") ,
            "int_default"  : config.get("int_default", -9999) ,
            "float_default": config.get("float_default", -9999.9) ,
            "str_default"  : config.get("str_default", "__")
        }
        cast_plan = base.set_cast_dtype(col_def, default_val_config)
        base.output_log(MODULE_NAME, f"cast_plan={cast_plan}", log_file)
        
        with open(os.path.join(system_root, file_path), encoding=encode, newline="") as f:
            reader = csv.reader(f)
            rows   = list(reader)
        
        key_actual = rows[0]
        all_values = rows[1:].copy()
        
    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== LOOP START ========"""
    count = 0
    queue_out.put( # Set Dummy to Initial Value
        (
            0,
            [None]*len(key_actual)
        )
    )
    while not stop_event.is_set():
        try:
            value = all_values[int(count % len(all_values))]
            
            """Cleanse Value"""
            value, cast_fails = base.cast_dtype(value, cast_plan)
            if cast_fails > 0:
                ErrorIF.report_event({
                    "signature": "CAST_FAIL",
                    "phase": "loop",
                    "category": "data",
                    "trigger_id": TRIGGER_NAME,
                    "message": f"Cast failed {cast_fails} cols.",
                    "log_file": log_file
                })
            
            """==== Put Queue to StreamCore ===="""
            queue_out.put(
                (
                    count,
                    value
                )
            )
            
            # 波形の画面表示（確認用）##
            _timestamp  = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            _dev_name = base.load_device_info()["dev_name"]
            _key_show = value[1]
            _val_show = int(value[2] / 2) + 55
            sys.stdout.write(f"{count:<3} Time:{_timestamp} EQP:{_dev_name} Prod:{_key_show} Val:{_val_show:<3} {'#' * _val_show}\n")##
            sys.stdout.flush()##
            
            count += 1
            time.sleep(interval)

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="data", trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue
    """======== LOOP END ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None
