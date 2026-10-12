# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [Tkinter Viz - Graph]
# Ver: 02.a.20261004    By: IWATA, Y.'s Agent AI
# Mod: 終点行の除外を各 Act の [:-1] から core へ移し、CUT_LAST_ROW 属性で宣言する形に変更（B-2）
########################################
a_viz_graph_trigger

動作概要
    1. 起動時にtkinterウィンドウを作成
    2. セグメントを受信するたびに、val_col_idx で指定した列の値を折れ線グラフとして描画
    3. セグメント受信のたびに root.update() でイベントを処理する。mainloop()はキュー受信をブロッキングするため使用しない。
    4. ウィンドウを閉じられた場合は、停止制御ファイルを作成し、本体（stream_core）にGently Stopを要求。
    5. stop_event が立ったら、ウィンドウを破棄してループを終了

接続設定
    val_col_idx    : 表示対象の列インデックス（必須。col_defの並びを指定。segment_header分は自動的に加算）
    window_title   : ウィンドウタイトル
    window_w       : ウィンドウ幅
    window_h       : ウィンドウ高さ
    margin         : プロット領域の余白px
    max_overlay    : 描画する直近セグメント数
    line_color     : 最新セグメントの線色
    overlay_color  : 重ね描きする過去セグメントの線色
    bg_color       : 背景色
    text_color     : ラベル文字色
"""
MODULE_NAME = "ACT_Viz_Tkinter_Graph"

"""standard library"""
import os
import queue
import sys
import threading
import tkinter as tk
import traceback
from   datetime import datetime as dt
from   typing   import Any, Dict, List, Optional, Tuple

"""external library"""
## Not Using

"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()

except ModuleNotFoundError:
    try:
        dir_valid = 'tmpl_viz'  ## (system_root)/src/.
        cur_dir  = os.path.dirname(os.path.abspath(__file__))
        dir_name = os.path.basename(cur_dir)
        if dir_name != dir_valid:
            raise FileNotFoundError(f".py MUST be in '{dir_valid}' dir. Current: {dir_name}")

        root_dir = os.path.abspath(os.path.join(cur_dir, "..", ".."))  ## (system_root)/src/.
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
from   src.tri_s01_error_if       import ErrorIF, SIGNAL_STOP

"""system dir, file"""
system_root = base.get_system_root()
log_file    = base.get_log_path()
work_dir    = base.get_work_dir()

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)


########################################
# 設定デフォルト値一覧
########################################
_DEFAULTS = {
    "window_title" : "TriEdge Viz",
    "window_w"     : 800,
    "window_h"     : 400,
    "margin"       : 40,
    "max_overlay"  : 5,
    "line_color"   : "lime",
    "overlay_color": "gray",
    "bg_color"     : "black",
    "text_color"   : "white",
}


########################################
# 座標変換ヘルパー
########################################

def _scale_points_to_canvas(
    values : List[float],
    width  : int,
    height : int,
    margin : int,
) -> List[float]:
    """
    valuesをCanvas座標の配列 [x0,y0, x1,y1, ...] に変換
    values が空または1点の場合は空リストを返す。
    """
    n = len(values)
    if n < 2:
        return []

    plot_w = max(1, width  - 2 * margin)
    plot_h = max(1, height - 2 * margin)

    v_min = min(values)
    v_max = max(values)
    v_range = v_max - v_min

    points: List[float] = []
    for i, v in enumerate(values):
        x = margin + (i / (n - 1)) * plot_w
        if v_range == 0:
            y = margin + plot_h / 2
        else:
            y = margin + (1 - (v - v_min) / v_range) * plot_h
        points.append(x)
        points.append(y)

    return points


def _extract_numeric_col(segment: List[List[Any]], col_idx: int) -> List[float]:
    """segment（行のリスト）から col_idx 列の値を float に変換できるものだけ抽出して返す"""
    vals: List[float] = []
    for row in segment:
        try:
            v = float(row[col_idx])
            if v == v:  # NaN除外
                vals.append(v)
        except (IndexError, TypeError, ValueError):
            pass
    return vals


def _build_layout(
    root:      tk.Tk,
    window_w:  int,
    window_h:  int,
    bg_color:  str,
    text_color: str,
) -> Tuple[tk.Label, tk.Canvas]:
    """
    ウィンドウのレイアウト（上部の情報ラベル + 波形描画用Canvas）を組立て。
    """
    info_label = tk.Label(root, text="waiting for data...", bg=bg_color, fg=text_color, anchor="w")
    info_label.pack(fill="x")

    canvas = tk.Canvas(root, width=window_w, height=window_h - 24, bg=bg_color, highlightthickness=0)
    canvas.pack(fill="both", expand=True)

    return info_label, canvas


########################################
# キュー受信・共通前処理ヘルパー
########################################

def _pump_idle(root: tk.Tk, window_closed: Dict[str, bool], log_file: str) -> None:
    """
    キューが空だった時（post_queue.get がタイムアウトした時）の処理
    """
    if window_closed["flag"]:
        return
    try:
        root.update()
    except tk.TclError:
        window_closed["flag"] = True


def _validate_segment(
    segment:   List[List[Any]],
    n_col_def: int,
    log_file:  str,
) -> Optional[Tuple[Any, List[List[Any]]]]:
    """
    受信セグメントへの前処理：
      1. 列数が col_def と一致しているか確認
      2. seg_id を取得
      3. 終点行（変化後の1行）の除外は core が行う（CUT_LAST_ROW = True。ここでは除外しない）
      4. 除外後に空でないか確認
    いずれかのチェックで問題があれば None を返す
    """
    n_col_act = len(segment[0])
    if n_col_act != n_col_def:
        base.output_log(MODULE_NAME,
            f"Segment Columns NOT Match: seg_id={segment[0][0]}  "
            f"Expected:{n_col_def}  Actual:{n_col_act}", log_file)
        return None

    seg_id = segment[0][0]
    trimmed = segment  # 終点行（変化後）の除外は core が行う（CUT_LAST_ROW = True）

    if not trimmed:
        base.output_log(MODULE_NAME, f"WARNING  空セグメントです。スキップします。id={seg_id}", log_file)
        return None

    return seg_id, trimmed


########################################
# Act Trigger Function
########################################

def a_viz_graph_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """
    セグメントの波形をtkinterウィンドウにリアルタイム表示
    Mod: 2026.07.30    By: IWATA,Y.
    """
    TRIGGER_NAME = "a_viz_graph_trigger"

    """======== STARTUP ========"""
    root: Optional[tk.Tk] = None
    try:
        n_col_def = len(col_def)

        val_col_idx_raw = config.get("val_col_idx")
        if val_col_idx_raw is None:
            raise ValueError("val_col_idx is required.")
        val_col_idx = int(val_col_idx_raw)
        
        _seg_header_offset = 4 # segment_header 4列分を補正
        actual_col_idx = val_col_idx + _seg_header_offset
        if not (0 <= actual_col_idx < n_col_def):
            raise ValueError(
                f"val_col_idx={val_col_idx} が範囲外です（col_defの有効範囲: "
                f"0～{n_col_def - _seg_header_offset - 1}）。col_def={col_def}")

        window_title  = str(config.get("window_title", _DEFAULTS["window_title"]))
        window_w      = int(config.get("window_w", _DEFAULTS["window_w"]))
        window_h      = int(config.get("window_h", _DEFAULTS["window_h"]))
        margin        = int(config.get("margin", _DEFAULTS["margin"]))
        max_overlay   = int(config.get("max_overlay", _DEFAULTS["max_overlay"]))
        line_color    = str(config.get("line_color", _DEFAULTS["line_color"]))
        overlay_color = str(config.get("overlay_color", _DEFAULTS["overlay_color"]))
        bg_color      = str(config.get("bg_color", _DEFAULTS["bg_color"]))
        text_color    = str(config.get("text_color", _DEFAULTS["text_color"]))

        col_name = col_def[actual_col_idx][0] if actual_col_idx in col_def else f"col{val_col_idx}"

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} val_col_idx={val_col_idx}(actual={actual_col_idx}, name={col_name})  "
            f"window={window_w}x{window_h}  max_overlay={max_overlay}", log_file)

        """ Tkウィンドウ構築 """
        root = tk.Tk()
        root.title(window_title)

        icon_path = os.path.join(system_root, "triedge.ico")
        if os.path.exists(icon_path):
            try:
                root.iconbitmap(icon_path)
            except Exception as e:
                base.output_log(MODULE_NAME,
                    f"WARNING  アイコンの読込みに失敗しました。tkinter標準アイコンを使用します: "
                    f"{icon_path}  ({type(e).__name__}: {e})", log_file)
        root.geometry(f"{window_w}x{window_h}")

        # ウィンドウ終了処理
        window_closed = {"flag": False}

        def _on_close():
            window_closed["flag"] = True
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} ウィンドウが閉じられました。TriEdge本体にGentle Stopを要求します"
                f"（Ctrl+Cと同じ停止制御ファイル方式）。", log_file)
            try:
                stop_ctrl_path = base.get_ctrl_stop_stream_path()
                with open(stop_ctrl_path, "w"):
                    pass
                base.output_log(MODULE_NAME, f"停止制御ファイルを作成しました: {stop_ctrl_path}", log_file)
            except Exception as e:
                base.output_log(MODULE_NAME,
                    f"WARNING  停止制御ファイルの作成に失敗しました: {type(e).__name__}: {e}", log_file)
            try:
                root.destroy()
            except Exception:
                pass

        root.protocol("WM_DELETE_WINDOW", _on_close)

        ########################################
        # ウィンドウレイアウト・ボタン押下アクション /
        ########################################
        history: List[List[float]] = []

        """レイアウト変更時は _build_layout() を修正"""
        info_label, canvas = _build_layout(root, window_w, window_h, bg_color, text_color)
        
        """ボタン押下の処理"""
        def _on_clear_click() -> None:
            """履歴（重ね描き対象）をクリアする"""
            history.clear()
            canvas.delete("all")
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} 履歴をクリアしました（ボタン押下）", log_file)

        clear_button = tk.Button(root, text="Clear History", command=_on_clear_click,
                                  bg=bg_color, fg=text_color)
        clear_button.pack(fill="x")

        # ボタンを追加した分、ウィンドウの高さを拡張
        root.update_idletasks()
        root.geometry(f"{window_w}x{window_h + clear_button.winfo_reqheight()}")
        
        root.update()
        ########################################
        # / ウィンドウレイアウト・ボタン押下アクション
        ########################################

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        if root is not None:
            try: root.destroy()
            except Exception: pass
        stop_event.set()
        return None

    """======== LOOP START ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start Loop...", log_file)
    while True:

        """キュー受信"""
        try:
            segment = post_queue.get(timeout=1.0)

        except queue.Empty:
            _pump_idle(root, window_closed, log_file)
            if (stop_event.is_set() and
                    post_queue.qsize() == 0 and
                    getattr(post_queue, "unfinished_tasks", 0) == 0):
                break
            continue

        """sentinel → 終了"""
        if segment is None:
            post_queue.task_done()
            break

        """セグメント処理"""
        try:
            validated = _validate_segment(segment, n_col_def, log_file)
            if validated is None:
                continue
            seg_id, segment = validated

            vals = _extract_numeric_col(segment, actual_col_idx)
            if len(vals) < 2:
                base.output_log(MODULE_NAME,
                    f"WARNING  描画に十分な数値データがありません。スキップします。id={seg_id}", log_file)
                continue

            if window_closed["flag"]:
                # 全体を止めないよう受信だけ継続
                continue

            ########################################
            # 画面表示データ更新 /
            ########################################
            history.append(vals)
            if len(history) > max_overlay:
                history.pop(0)

            canvas.delete("all")
            for i, past_vals in enumerate(history[:-1]):
                pts = _scale_points_to_canvas(past_vals, window_w, window_h - 24, margin)
                if pts:
                    canvas.create_line(*pts, fill=overlay_color, width=1)

            latest_pts = _scale_points_to_canvas(vals, window_w, window_h - 24, margin)
            if latest_pts:
                canvas.create_line(*latest_pts, fill=line_color, width=2)

            v_min, v_max, v_mean = min(vals), max(vals), sum(vals) / len(vals)
            info_label.config(
                text=f"seg_id={seg_id}  col={col_name}  rows={len(vals)}  "
                     f"min={v_min:.3f}  mean={v_mean:.3f}  max={v_max:.3f}")
            
            root.update()
            ########################################
            # / 画面表示データ更新
            ########################################
            
        except tk.TclError as e:
            window_closed["flag"] = True
            base.output_log(MODULE_NAME,
                f"WARNING  Tk操作でエラーが発生しました。以降は描画を停止します: {e}", log_file)
            continue

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="data",
                                            trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue

        finally:
            post_queue.task_done()

    """======== LOOP END ========"""
    if root is not None:
        try: root.destroy()
        except Exception: pass

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None

a_viz_graph_trigger.QMODE = "imq"
a_viz_graph_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    
    ########################################
    # レイアウトのプレビュー表示
    ########################################
    _root = tk.Tk()
    _root.title(_DEFAULTS["window_title"] + " (Layout Preview)")
    _root.geometry(f"{_DEFAULTS['window_w']}x{_DEFAULTS['window_h']}")

    _info_label, _canvas = _build_layout(
        _root, _DEFAULTS["window_w"], _DEFAULTS["window_h"], _DEFAULTS["bg_color"], _DEFAULTS["text_color"])

    # プレビュー用ダミー波形
    import math
    _preview_vals = [50 + 40 * math.sin(i / 3) for i in range(30)]
    _preview_pts  = _scale_points_to_canvas(
        _preview_vals, _DEFAULTS["window_w"], _DEFAULTS["window_h"] - 24, _DEFAULTS["margin"])
    if _preview_pts:
        _canvas.create_line(*_preview_pts, fill=_DEFAULTS["line_color"], width=2)
    _info_label.config(
        text=f"seg_id=PREVIEW  col=Sin  rows={len(_preview_vals)}  "
             f"min={min(_preview_vals):.3f}  mean={sum(_preview_vals)/len(_preview_vals):.3f}  "
             f"max={max(_preview_vals):.3f}")

    # ウィンドウレイアウト・ボタン押下アクション
    def _preview_on_clear_click() -> None:
        _canvas.delete("all")
        print(f"{MODULE_NAME} [Preview] Clear History clicked.")

    _clear_button = tk.Button(_root, text="Clear History", command=_preview_on_clear_click,
                               bg=_DEFAULTS["bg_color"], fg=_DEFAULTS["text_color"])
    _clear_button.pack(fill="x")

    _root.update_idletasks()
    _root.geometry(f"{_DEFAULTS['window_w']}x{_DEFAULTS['window_h'] + _clear_button.winfo_reqheight()}")

    _root.mainloop() # プレビュー時のみ使用
