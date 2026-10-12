# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# TriEdge Control Panel
# Ver: 02.d.20260430    By: IWATA,Y.
########################################
"""
MODULE_NAME = "UI"

"""standard library"""
from   datetime import datetime as dt
import os
import subprocess
import sys
import time
import tkinter as tk
import traceback

"""external library"""
## NOT Use


# 1. Define System Dirs and Files
is_frozen_exe = bool(getattr(sys, 'frozen', False))
if is_frozen_exe: # .exe
    _UI_ROOT = os.path.dirname(sys.executable)
else:             # .py
    _UI_ROOT  = os.path.dirname(os.path.abspath(__file__))

_UI_LOG   = os.path.join(_UI_ROOT, "ui.log")
if os.path.exists(_UI_LOG):
    try: os.remove(_UI_LOG)
    except: pass

_ICO_PATH = os.path.join(_UI_ROOT, "triedge.ico")

_BAT_PATH = os.path.join(_UI_ROOT, "trigger_on_caller.bat")
_EXE_PATH = os.path.join(_UI_ROOT, "trigger_on.exe")
if not(os.path.exists(_BAT_PATH)) and not(os.path.exists(_EXE_PATH)):
    msg = f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  NOT Exists Trigger_on. STOP UI."
    print(msg, flush=True)
    with open(_UI_LOG, "a") as f: f.write(f"{msg}\n")
    sys.exit(-1)

_STREAM_HEARTBEAT_PATH = os.path.join(_UI_ROOT, "work", "run",   "_heartbeat_stream_core.triedge")
_STREAM_STOP_FILE_PATH = os.path.join(_UI_ROOT, "work", "run",   "_oneshot_stop_stream.triedge_control")


########################################
# Config
########################################

"""==== Color, Style ===="""
_TITLE = "TriEdge Control Panel"
_GEOMETRY = "400x350"

_C_BG         = "#2b2b2b"
_C_CELL_BG    = "#1e1e1e"
_C_NORMAL     = "#0CC078"
_C_ERROR      = "#FF4B4B"
_C_PENDING    = "#FFAA00"
_C_PROCESSING = "#00AAFF"
_C_GRAY       = "#888888"
_C_DARKGRAY   = "#555555"
_C_SEP        = "#444444"
_C_FONT_BASE  = "#CCCCCC"
_C_FONT_BASE_B = _C_BG

_FONT_TYPE = "Courier New" # Arial (propotional)  Courier New
_FONT_SIZE_LARGE = 36
_FONT_SIZE_MID = 16
_FONT_SIZE_SMALL = 10


########################################
# App
########################################
class TriEdgeApp(tk.Tk):
    def __init__(self):
        """Initialize Object"""
        super().__init__()

        self.title(_TITLE)
        if os.path.exists(_ICO_PATH) and os.name == "nt":
            self.iconbitmap(_ICO_PATH)
        self.geometry(_GEOMETRY)
        self.resizable(False, False)
        self.configure(bg=_C_BG) 

        self.lbl_title = tk.Label(
            self,
            text=_TITLE,
            font=(_FONT_TYPE, _FONT_SIZE_MID, "bold"),
            bg=_C_BG,
            fg=_C_FONT_BASE
        )
        self.lbl_title.pack(pady=20)

        self.lbl_status_icon = tk.Label(
            self,
            text="　未接続",
            font=(_FONT_TYPE, _FONT_SIZE_LARGE, "bold"),
            bg=_C_BG,
            fg=_C_ERROR
        )
        self.lbl_status_icon.pack(pady=10)

        self.lbl_time = tk.Label(
            self,
            text="Last Seen: --",
            font=(_FONT_TYPE, _FONT_SIZE_SMALL),
            bg=_C_BG,
            fg=_C_GRAY
        )
        self.lbl_time.pack(pady=0)

        self.frame_btn = tk.Frame(
            self,
            bg=_C_BG
        )
        self.frame_btn.pack(pady=30)

        self.btn_start = tk.Button(
            self.frame_btn,
            text="接続",
            font=(_FONT_TYPE, _FONT_SIZE_MID, "bold"),
            bg=_C_NORMAL,
            fg=_C_FONT_BASE_B,
            width=10,
            command=self.cmd_start
        )
        self.btn_start.grid(row=0, column=0, padx=10)

        self.btn_stop = tk.Button(
            self.frame_btn,
            text="切断",
            font=(_FONT_TYPE, _FONT_SIZE_MID, "bold"),
            bg=_C_ERROR,
            fg=_C_FONT_BASE,
            width=10,
            command=self.cmd_stop
        )
        self.btn_stop.grid(row=0, column=1, padx=10)

        # 点滅制御用の変数
        self.is_running  = False
        self.blink_state = True
        self.is_stopping = False

        # ループ開始
        self.check_status()
        self.blink_status() # 点滅ループも起動


    def check_status(self):
        """定期的にハートビートを監視する"""
        current_running = False
        last_time_str = "--"

        if os.path.exists(_STREAM_HEARTBEAT_PATH):
            try:
                last_time = os.path.getmtime(_STREAM_HEARTBEAT_PATH)
                last_time_str = dt.fromtimestamp(last_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                if time.time() - last_time < 5.0:
                    current_running = True
                
            except:
                pass

        self.is_running = current_running

        # 停止を確認したらフラグを解除
        if self.is_stopping and not self.is_running:
            self.is_stopping = False

        if not self.is_running and not self.is_stopping:
            self.lbl_status_icon.configure(
                text="　未接続",
                fg=_C_ERROR
            )
            self.lbl_time.configure(
                text=f"Last Seen: {last_time_str}",
                fg=_C_GRAY
            )
            self.btn_start.configure(state="normal")
            self.btn_stop.configure(state="disabled")
            
        elif self.is_running:                              # 稼働中（点滅はblink_statusに任せる）
            self.lbl_time.configure(
                text=f"Last Heartbeat: {last_time_str}",
                fg=_C_NORMAL
            )
            self.btn_start.configure(state="disabled")
            self.btn_stop.configure(state="disabled" if self.is_stopping else "normal")

        self.after(3000, self.check_status)


    def blink_status(self):
        """稼働中・停止処理中の点滅"""
        if self.is_stopping: # 停止処理中を最優先で判定
            if self.blink_state:
                self.lbl_status_icon.configure(
                    text="　切断中...",
                    fg=_C_ERROR
                )
            else:
                self.lbl_status_icon.configure(
                    text="　切断中...",
                    fg=_C_BG
                )
            self.blink_state = not self.blink_state

        elif self.is_running:
            if self.blink_state:
                self.lbl_status_icon.configure(
                    text="　接続中...",
                    fg=_C_NORMAL
                )
            else:
                self.lbl_status_icon.configure(
                    text="　接続中...",
                    fg=_C_NORMAL
                )
            self.blink_state = not self.blink_state

        else:
            self.blink_state = True

        self.after(500, self.blink_status)


    def cmd_start(self):
        try:
            if os.path.exists(_BAT_PATH):# BAT あり → cmd.exe で実行
                cmd = ["cmd.exe", "/c", _BAT_PATH]
                creation_flag = 0
                shell_flag = False

            elif os.path.exists(_EXE_PATH):# BAT 無し → EXE を実行
                cmd = [_EXE_PATH]
                creation_flag = subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0
                shell_flag = False
                
            else:
                raise FileNotFoundError("trigger_on_caller.bat も trigger_on.exe も存在しません")

            subprocess.Popen(
                cmd,
                creationflags=creation_flag,
                shell=shell_flag
            )
        except Exception as e:
            msg = f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  _UI_ROOT={_UI_ROOT} {e}"
            print(msg)
            with open(_UI_LOG, "a") as f: f.write(f"{msg}\n")
            pass
    

    def cmd_stop(self):
        try:
            with open(_STREAM_STOP_FILE_PATH, "w") as f:
                pass
            self.is_stopping = True

        except Exception as e:
            with open(_UI_LOG, "a") as f: f.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  _UI_ROOT={_UI_ROOT} {e}\n")
            pass


if __name__ == "__main__":
    app = TriEdgeApp()
    app.mainloop()
