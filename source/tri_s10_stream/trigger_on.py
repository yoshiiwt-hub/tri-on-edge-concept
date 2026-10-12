# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# TriEdge System BootLoader [Trigger On]
# Ver: 02.b.20260930    By: IWATA, Y.'s Agent AI
# Mod: モジュール名変更（tri_s 形式）に伴い import 名を更新
########################################
"""
MODULE_NAME = "BOOT"

"""standard library"""
from   datetime import datetime as dt
import os
import sys
import traceback

"""external library"""
## NOT Use

class NullWriter:
    def write(self, *args, **kwargs): pass
    def flush(self, *args, **kwargs): pass


def _output_log(msg: str, file_: str):
    try:
        ts = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        sys.stdout.write(f"{ts}  {msg}\n")
        sys.stdout.flush()
        
        with open(file_, "a", encoding="utf-8-sig") as f:
            f.write(f"{ts}  {msg}\n")
            
    except Exception as e:
        sys.stdout.write(f"{e}\n{traceback.format_exc()}\n")
        sys.stdout.flush()
        pass # 続行を優先
    return None


########################################
# BootLoader
########################################

def main():
    if sys.stdout is None: sys.stdout = NullWriter()
    if sys.stderr is None: sys.stderr = NullWriter()
    
    cur_dir  = ""
    boot_log = ""
    
    try:
        """ 1. Check Current Dir """
        is_frozen_exe = bool(getattr(sys, 'frozen', False))
        if is_frozen_exe:# .exe (pyinstaller) の場合
            cur_dir = os.path.dirname(sys.executable)
        else:            # .py の場合
            cur_dir = os.path.dirname(os.path.abspath(__file__))
        
        boot_log = os.path.join(cur_dir, "BOOT.log")
        with open(boot_log, "w", encoding="utf-8-sig") as f:
            f.write(f"--- TriEdge Boot Log Initialized at {dt.now()} ---\n")
        
        _output_log(f"Current Dir={cur_dir}", boot_log)
        _output_log(f"Boot Log={boot_log}", boot_log)
        
    except Exception as e:
        # ここでのエラーは、実行フォルダ直下にログ出力
        _output_log(f"{e}\n{traceback.format_exc()}", "TRIEDGE_BOOT.log")
        sys.exit(1)
    
    try:
        """ 2. Add Path """
        _output_log(f"Start to Add Path...", boot_log)
        
        if cur_dir not in sys.path:
            sys.path.append(cur_dir)
        
        src_dir = os.path.join(cur_dir,"src")
        for i in range(10):
            if src_dir in sys.path:
                sys.path.remove(src_dir)
            else:
                break
        
        _output_log(f"sys.path={sys.path}", boot_log)
        
    except Exception as e:
        _output_log(f"{e}\n{traceback.format_exc()}", boot_log)
        sys.exit(1)
    
    try:
        """ 3. Boot Process """
        _output_log(f"Start to Boot...", boot_log)
        
        # Start Main Loop
        import src.tri_s09_operator as operator
        operator.main_loop()
        
    except Exception as e:
        _output_log(f"{e}\n{traceback.format_exc()}", boot_log)
        sys.exit(1)


########################################
# Entry Point
########################################
if __name__ == "__main__":
    main()
