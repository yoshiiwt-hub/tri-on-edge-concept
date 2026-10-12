# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [Viz Demo]
# Ver: 05.a.20261004    By: IWATA, Y.'s Agent AI
# Mod: 終点行の除外を core へ移し CUT_LAST_ROW で宣言。a_plot_overlay_web_trigger も a_plot_segment_trigger と同じく終点行を除外する（B-2）
########################################
a_plot_segment_trigger     # 各セグメントをグラフ画像（PNG）に保存する Act Trigger
a_plot_overlay_web_trigger # 複数セグメントの波形重ね合わせプロットを Flask Web で配信する Act Trigger

動作概要
    1. キューからセグメントを受信
    2. a_plot_segment_trigger: セグメントの指定列（plot_target_loc）値を折れ線グラフ化して PNG 保存
       - ack_ext 指定時は保存後に .done ファイルを作成
    3. a_plot_overlay_web_trigger: セグメントの指定列値を蓄積して波形重ね合わせグラフを生成
       - Flask で overlay.png を Web 配信し、2 秒ごとに自動リフレッシュ

接続設定
    plot_target_loc : プロット対象の col_def 列インデックス（デフォルト: 0）
    save_dir        : 画像ファイルの保存先ディレクトリ（デフォルト: "output"）
    ack_ext         : 完了 ACK ファイルの拡張子（省略可）
    web_port        : Flask Web サーバのポート番号（デフォルト: 8000）
    max_history     : 重ね合わせセグメント数の最大値（デフォルト: 50）
"""
MODULE_NAME = "ACT_VIZ"

"""standard library"""
from   datetime    import datetime as dt
import gc
import os
import queue
import sys
import threading
import time
import traceback
from   typing      import Any, Dict, List, Tuple, Sequence, Optional
from   collections import deque

"""external library"""
from   flask       import Flask, send_file, render_template_string
import matplotlib
import matplotlib.pyplot as plt

"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()
    
except ModuleNotFoundError:
    try: # Check and Add Path
        dir_valid = 'viz'  ## (system_root)/src/.
        cur_dir  = os.path.dirname(os.path.abspath(__file__))
        dir_name = os.path.basename(cur_dir)
        if dir_name != dir_valid:
            raise FileNotFoundError(f".py MUST be in {dir_valid} . Current: {dir_name}")
        
        root_dir = os.path.abspath(os.path.join(cur_dir, "..", ".."))
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
work_dir       = base.get_work_dir()
work_db_dir    = base.get_work_sub_dirs().get("db"   , os.path.join(work_dir, "db"))
work_run_dir   = base.get_work_sub_dirs().get("run"  , os.path.join(work_dir, "run"))
work_spool_dir = base.get_work_sub_dirs().get("spool", os.path.join(work_dir, "spool"))

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__=='main':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# Act Trigger Functions
########################################

def a_plot_segment_trigger(
	queue_out: queue.Queue,
	col_def: Dict[int, Any],
	config: Dict[str, Any],
	stop_event
) -> None:
    """
    各セグメントをグラフ画像にして保存
    Mod: 2026.04.06    By: IWATA,Y.
    """
    TRIGGER_NAME = "a_plot_segment_trigger"
    func_name = sys._getframe().f_code.co_name
    func_obj = globals().get(func_name)
    func_qmode = getattr(func_obj, "QMODE", "imq")
    
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)
        matplotlib.use("Agg") # GUI不要のバックエンド
        target_loc   = config.get("plot_target_loc", 0) + 4 # 0:3 seg_id seg_ts seg_ind seg_status
        save_dir = os.path.join(system_root, config.get("save_dir", "output"))####
        os.makedirs(save_dir, exist_ok=True)
        ack_ext  = config.get("ack_ext", "")
        
        """Plot Confog"""
        plot_config  = config.get("plot_config", {})
        fig_w        = plot_config.get("fig_w"        , 3000/300)
        fig_h        = plot_config.get("fig_h"        , 2000/300)
        fig_dpi      = plot_config.get("fig_dpi"      , 300)
        field_color  = plot_config.get("field_color"  , "w")
        edge_color   = plot_config.get("edge_color"   , "k")
        line_color   = plot_config.get("line_color"   , "darkorange")
        line_color_2 = plot_config.get("line_color_2" , "royalblue")
        line_w       = plot_config.get("line_w"       , 2.5)
        font_size    = plot_config.get("font_size"    , 10)        
        plt.rc("figure" , fc=field_color, ec=edge_color)
        plt.rc("savefig", fc=field_color, ec=edge_color)
        plt.rc("axes"   , fc=field_color, ec=edge_color, labelcolor=edge_color, titlecolor=edge_color)
        plt.rc("xtick"  , color=edge_color)
        plt.rc("ytick"  , color=edge_color)
        plt.rc("text"   , color=edge_color)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== LOOP START ========"""
    file_counter = 1
    while True:
        try:
            segment = queue_out.get(timeout=1.0)
        
        except queue.Empty:
            if stop_event.is_set() and queue_out.qsize() == 0 and getattr(queue_out, "unfinished_tasks", 0) == 0: break
            continue
        
        if segment is None:
            queue_out.task_done()
            break
        
        try:
            """セグメントID生成"""
            seg_id = segment[0][0]

            """終点行（変化後）の除外は core が行う（CUT_LAST_ROW = True）"""
            
            """Segmentから値を取得"""
            values = [
                row_val[target_loc]
                for row_val in segment
                if isinstance(row_val[target_loc], (int, float))
            ]
            if not values:
                base.output_log(MODULE_NAME, f"Skip empty segment: id={seg_id}")
                continue
            
            """プロット作成"""
            fig, ax = plt.subplots(figsize=(fig_w, fig_h))
            ax.plot(values, lw=line_w, color=line_color, alpha=1.0)
            ax.set_title(f"Segment ID: {seg_id}", fontsize=font_size+2)
            ax.grid(True)

            """保存"""
            filename = f"{save_dir}/seg_{file_counter:03}_{seg_id}.png"
            fig.savefig(filename, dpi=fig_dpi, bbox_inches="tight")            
            plt.clf()
            plt.close(fig)            
            if file_counter % 10 == 0:
                gc.collect()
            
            """make Ack file"""
            if ack_ext:
                time.sleep(0.5)
                with open(f"{save_dir}/seg_{file_counter:03}_{seg_id}.png.done", "w") as f: pass
            
            base.output_log(MODULE_NAME, f"Saved segment plot: {filename}  (len={len(values)})")
            file_counter += 1
            time.sleep(0.2)

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break  # finally経由でtask_done()後にループ離脱
            continue # ループ継続
        
        finally:
            queue_out.task_done()
    """======== LOOP END ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None

a_plot_segment_trigger.QMODE = "imq"
a_plot_segment_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


def a_plot_overlay_web_trigger(
	queue_out: queue.Queue,
	col_def: Dict[int, Any],
	config: Dict[str, Any],
	stop_event: threading.Event
) -> None:
    """
    波形重ね合わせプロット
    Mod: 2026.04.06    By: IWATA,Y.
    """
    TRIGGER_NAME = "a_plot_overlay_web_trigger"
    func_name = sys._getframe().f_code.co_name
    func_obj = globals().get(func_name)
    func_qmode = getattr(func_obj, "QMODE", "imq")

    HTML_TEMPLATE = """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>Overlay Segment Viewer</title>
        <style>
            body { font-family: sans-serif; text-align: center; background: #fafafa; margin: 20px; }
            h2 { margin-bottom: 10px; }
            img { width: 90%; max-width: 900px; border: 1px solid #ccc; border-radius: 8px; margin: 10px 0; }
            p { color: #666; font-size: 14px; }
        </style>
        <meta http-equiv="refresh" content="2">
    </head>
    <body>
        <h2>{{ annotation }}</h2><h4>{{ now_str }}</h4>
        {% if latest %}
            <img src="/img/{{ latest }}" alt="Overlay plot">
        {% else %}
            <p>No segments plotted yet.</p>
        {% endif %}
        <p>Auto-refresh every 2 seconds.</p>
    </body>
    </html>
    """
    
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)
        matplotlib.use("Agg") # GUI不要のバックエンド
        
        # 設定
        target_loc   = config.get("plot_target_loc", 0) + 4 # 0:3 seg_id seg_ts seg_ind seg_status
        annotation   = col_def.get(target_loc, "")[4]
        port         = config.get("web_port", 8000)
        save_dir     = os.path.join(system_root, config.get("save_dir", "work/spool/"))
        overlay_file = os.path.join(save_dir, "overlay.png")
        os.makedirs(save_dir, exist_ok=True)
        max_history  = config.get("max_history", 50)
        seg_history  = deque(maxlen=max_history)
        
        """Plot Confog"""
        plot_config  = config.get("plot_config", {})
        fig_w        = plot_config.get("fig_w"        , 3000/300)
        fig_h        = plot_config.get("fig_h"        , 2000/300)
        fig_dpi      = plot_config.get("fig_dpi"      , 300)
        field_color  = plot_config.get("field_color"  , "w")
        edge_color   = plot_config.get("edge_color"   , "k")
        line_color   = plot_config.get("line_color"   , "darkorange")
        line_color_2 = plot_config.get("line_color_2" , "royalblue")
        line_w       = plot_config.get("line_w"       , 2.5)
        font_size    = plot_config.get("font_size"    , 10)        
        plt.rc("figure" , fc=field_color, ec=edge_color)
        plt.rc("savefig", fc=field_color, ec=edge_color)
        plt.rc("axes"   , fc=field_color, ec=edge_color, labelcolor=edge_color, titlecolor=edge_color)
        plt.rc("xtick"  , color=edge_color)
        plt.rc("ytick"  , color=edge_color)
        plt.rc("text"   , color=edge_color)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None
    
    """"Flask App"""
    app = Flask(__name__)

    @app.route("/")
    def index():
        latest = "overlay.png" if os.path.exists(overlay_file) else None
        return render_template_string(HTML_TEMPLATE, annotation=annotation, latest=latest, max_history=max_history, now_str=dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

    @app.route("/img/<name>")
    def serve_image(name):
        path = os.path.join(save_dir, name)
        path = os.path.abspath(path)
        if os.path.exists(path):
            return send_file(path, mimetype="image/png")
        return "Not found", 404

    # Flaskを別スレッドで起動
    def run_server():
        app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

    threading.Thread(target=run_server, daemon=True).start()
    print(f"[WEBPLOT] Web server started: http://localhost:{port}")

    """======== LOOP START ========"""
    file_counter = 1
    while True:
        try:
            segment = queue_out.get(timeout=1.0)

        except queue.Empty:
            if stop_event.is_set() and queue_out.qsize() == 0 and getattr(queue_out, "unfinished_tasks", 0) == 0: break
            continue
        
        if segment is None:
            queue_out.task_done()
            break
        
        try:            
            # セグメントから値を抽出
            seg_id = segment[0][0]
            """Segmentから値を取得"""
            values = [
                row_val[target_loc]
                for row_val in segment
                if isinstance(row_val[target_loc], (int, float))
            ]
            '''values = [
                val[target_loc]
                for ts, idx, key, dtype, val in segment[:-1]
                if isinstance(val[target_loc], (int, float))
            ]'''
            if not values: continue
            seg_history.append((seg_id, values))

            """プロット作成"""
            fig, ax = plt.subplots(figsize=(fig_w, fig_h))
            n = len(seg_history)
            for i, (sid, vals) in enumerate(seg_history):
                if i < n-1:
                    ax.plot(vals, lw=line_w*0.5, color=line_color_2, alpha=0.5) # 過去分の波形
                else:
                    ax.plot(vals, lw=line_w, color=line_color, alpha=0.9) # 最新の波形

            ax.set_title(f"Overlay of last {n} segments (Newest = orange)", fontsize=font_size+2)
            ax.grid(True)

            """保存"""
            fig.savefig(overlay_file, dpi=fig_dpi, bbox_inches="tight")
            
            plt.clf()
            plt.close(fig)            
            if file_counter % 10 == 0: gc.collect()            
            print(f"[WEBPLOT] Updated overlay with segment {seg_id}")##

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break # finally経由でtask_done()後にループ離脱
            continue # ループ継続
        
        finally:
            queue_out.task_done()
        
        file_counter += 1
        time.sleep(1)
    """======== LOOP END ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None

a_plot_overlay_web_trigger.QMODE = "imq"
a_plot_overlay_web_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")    
    base.output_log(MODULE_NAME, f"test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
