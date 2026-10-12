# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [Tkinter Viz - Value List]
# Ver: 02.a.20261004    By: IWATA, Y.'s Agent AI
# Mod: 終点行の除外を各 Act の [:-1] から core へ移し、CUT_LAST_ROW 属性で宣言する形に変更（B-2）
########################################
a_viz_value_trigger

動作概要
    1. 起動時にtkinterウィンドウを1つ作成し、display_cols で指定した列の数だけラベル行を並べる。
    2. セグメントを受信するたびに、そのセグメントの開始行／最終行の値で各ラベルを更新する。履歴は持たない。
        ※ dtype（col_defの2番目の要素）に従い表示：
            "str"  : そのまま文字列化
            "int"  : 整数に変換して表示
            "float": 小数点以下4桁で表示
            "time" : strftimeを持つ値（datetime型）はフォーマットして表示、それ以外はそのまま文字列化
            上記以外: そのまま文字列化
            型変換に失敗した場合、または値自体がNoneの場合は "(invalid)" を表示。
    3. セグメント受信のたびに root.update() でイベントを処理する。mainloop()はキュー受信をブロッキングするため使用しない。
    4. ウィンドウを閉じられた場合は、停止制御ファイルを作成し、本体（stream_core）にGently Stopを要求
    5. stop_event が立ったら、ウィンドウを破棄してループを終了

接続設定
    display_cols   : 表示対象の列番号のリスト（segment_header + col_defを結合した列番号。省略時は全列を表示）
    row_select     : セグメントのどの行の値を表示するか。"first"（先頭行）| "last"（末尾行）
    window_title   : ウィンドウタイトル
    window_w       : ウィンドウ幅
    window_h       : ウィンドウ高さ
    bg_color       : 背景色
    label_color    : 列名部分の文字色
    value_color    : 値部分の文字色
    comment_color  : コメント（col_defの5番目の要素）部分の文字色
    font_family    : フォント名
    font_size      : フォントサイズ
"""
MODULE_NAME = "ACT_Viz_Tkinter_Value"

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
## Not Using（tkinterは標準ライブラリ）

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
    "window_title" : "TriEdge Value Viz",
    "window_w"     : 640,
    # "window_h"   : 省略時は 30 + 26×表示列数 で動的計算。
    "row_select"   : "first",
    "bg_color"     : "black",
    "label_color"  : "cyan",
    "value_color"  : "white",
    "comment_color": "gray",
    "font_family"  : "Consolas",
    "font_size"    : 12,
}


########################################
# 値フォーマット整形ヘルパー
########################################

def _format_value(value: Any, dtype: str) -> str:
    """
    col_def の dtype に従って value を表示用文字列に変換する。
    """
    if value is None:
        return "(invalid)"
    try:
        dtype = str(dtype or "str").lower()
        if dtype == "str":
            return str(value)
        elif dtype == "int":
            return str(int(value))
        elif dtype == "float":
            return f"{float(value):.4f}"
        elif dtype == "time":
            if hasattr(value, "strftime"):
                return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            return str(value)
        else:
            return str(value)
    except (TypeError, ValueError):
        return "(invalid)"


########################################
# レイアウト構築・行データ変換ヘルパー
########################################

def _build_layout(
    root:          tk.Tk,
    col_def:       Dict[int, Any],
    display_cols:  List[int],
    bg_color:      str,
    label_color:   str,
    value_color:   str,
    comment_color: str,
    name_font:     tuple,
    value_font:    tuple,
    comment_font:  tuple,
) -> Tuple[Dict[int, tk.Label], Dict[int, str]]:
    """
    ウィンドウのレイアウトを組み立てる。
    戻り値:
        value_labels : {col_defの列インデックス: 値表示用のLabelウィジェット}
        dtype_map    : {col_defの列インデックス: dtype}（_format_value に渡すため）
    """
    value_labels: Dict[int, tk.Label] = {}
    dtype_map:    Dict[int, str] = {}

    for row_i, col_idx in enumerate(display_cols):
        col_entry = col_def[col_idx]
        col_name  = col_entry[0] if len(col_entry) > 0 else f"col{col_idx}"
        dtype     = col_entry[1] if len(col_entry) > 1 else "str"
        role      = col_entry[2] if len(col_entry) > 2 else ""
        var_type  = col_entry[3] if len(col_entry) > 3 else ""
        comment   = col_entry[4] if len(col_entry) > 4 else ""

        dtype_map[col_idx] = dtype
        meta_str = f"({dtype}/{role}/{var_type})" if (role or var_type) else f"({dtype})"

        name_lbl = tk.Label(root, text=f"{col_name} {meta_str}:", bg=bg_color, fg=label_color,
                             anchor="w", font=name_font)
        name_lbl.grid(row=row_i, column=0, sticky="w", padx=(8, 6), pady=2)

        val_lbl = tk.Label(root, text="-", bg=bg_color, fg=value_color,
                            anchor="w", font=value_font)
        val_lbl.grid(row=row_i, column=1, sticky="w", padx=(0, 12), pady=2)

        comment_lbl = tk.Label(root, text=comment, bg=bg_color, fg=comment_color,
                                anchor="w", font=comment_font)
        comment_lbl.grid(row=row_i, column=2, sticky="w", pady=2)

        value_labels[col_idx] = val_lbl

    return value_labels, dtype_map


def _row_to_dict(row: List[Any], display_cols: List[int]) -> Dict[int, Any]:
    """
    display_cols分の {列インデックス: 値} 辞書を組立てる。
    """
    result: Dict[int, Any] = {}
    for col_idx in display_cols:
        result[col_idx] = row[col_idx] if col_idx < len(row) else None
    return result


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

def a_viz_value_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """
    最新セグメントの指定列の値を、tkinterウィンドウに表示
    Mod: 2026.07.30    By: IWATA,Y.
    """
    TRIGGER_NAME = "a_viz_value_trigger"

    """======== STARTUP ========"""
    root: Optional[tk.Tk] = None
    try:
        n_col_def = len(col_def)

        display_cols_raw = config.get("display_cols")
        if display_cols_raw:
            display_cols = [int(c) for c in display_cols_raw]
        else:
            display_cols = list(range(n_col_def))  # 省略時: col_def全列を表示

        for c in display_cols:
            if not (0 <= c < n_col_def):
                raise ValueError(
                    f"display_cols contains out-of-range index {c} "
                    f"(valid: 0-{n_col_def - 1})。col_def={col_def}")

        window_title = str(config.get("window_title", _DEFAULTS["window_title"]))
        window_w     = int(config.get("window_w", _DEFAULTS["window_w"]))
        window_h     = int(config.get("window_h", 30 + 26 * len(display_cols)))
        row_select   = str(config.get("row_select", _DEFAULTS["row_select"])).lower()
        if row_select not in ("first", "last"):
            raise ValueError(f"row_select must be 'first' or 'last'. Got: {row_select!r}")
        bg_color     = str(config.get("bg_color", _DEFAULTS["bg_color"]))
        label_color  = str(config.get("label_color", _DEFAULTS["label_color"]))
        value_color  = str(config.get("value_color", _DEFAULTS["value_color"]))
        comment_color = str(config.get("comment_color", _DEFAULTS["comment_color"]))
        font_family  = str(config.get("font_family", _DEFAULTS["font_family"]))
        font_size    = int(config.get("font_size", _DEFAULTS["font_size"]))

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} display_cols={display_cols}  window={window_w}x{window_h}  "
            f"row_select={row_select}", log_file)

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
        root.configure(bg=bg_color)

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
        # ウィンドウレイアウト定義 /
        ########################################
        name_font    = (font_family, font_size, "bold")
        value_font   = (font_family, font_size)
        comment_font = (font_family, max(font_size - 1, 8))
        
        """レイアウト変更時は _build_layout() を修正"""
        value_labels, dtype_map = _build_layout(
            root, col_def, display_cols,
            bg_color, label_color, value_color, comment_color,
            name_font, value_font, comment_font,
        )

        root.update()
        ########################################
        # / ウィンドウレイアウト定義
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

            if window_closed["flag"]:
                continue

            ########################################
            # 画面表示データ更新 /
            ########################################
            target_row = segment[0] if row_select == "first" else segment[-1]
            
            """変更時は _row_to_dict() を修正"""
            row_dict = _row_to_dict(target_row, display_cols)
            for col_idx, lbl in value_labels.items():
                formatted = _format_value(row_dict.get(col_idx), dtype_map[col_idx])
                lbl.config(text=formatted)
            
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

a_viz_value_trigger.QMODE = "imq"
a_viz_value_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    
    ########################################
    # レイアウトのプレビュー表示
    ########################################
    # プレビュー用ダミー col_def
    _preview_col_def = {
        0: ["seg_id",     "str",   "key",       "key",         "Segment ID"],
        1: ["seg_ind",    "int",   "key",       "key",         "Segment Row Index"],
        2: ["seg_ts",     "time",  "key",       "key",         "Timestamp When Data Collected"],
        3: ["seg_status", "int",   "key",       "key",         "Segment Process Status"],
        4: ["Timestamp",  "time",  "key",       "key",         "データ発生時の時刻"],
        5: ["LOT",        "str",   "key",       "key",         "製造id"],
        6: ["Sin",        "float", "numetory",  "explanatory", "軸１トルク電流値"],
    }
    _preview_display_cols = list(_preview_col_def.keys())

    _root = tk.Tk()
    _root.title(_DEFAULTS["window_title"] + " (Layout Preview)")
    _root.geometry(f"{_DEFAULTS['window_w']}x{30 + 26 * len(_preview_display_cols)}")
    _root.configure(bg=_DEFAULTS["bg_color"])

    _name_font    = (_DEFAULTS["font_family"], _DEFAULTS["font_size"], "bold")
    _value_font   = (_DEFAULTS["font_family"], _DEFAULTS["font_size"])
    _comment_font = (_DEFAULTS["font_family"], max(_DEFAULTS["font_size"] - 1, 8))

    _value_labels, _dtype_map = _build_layout(
        _root, _preview_col_def, _preview_display_cols,
        _DEFAULTS["bg_color"], _DEFAULTS["label_color"], _DEFAULTS["value_color"], _DEFAULTS["comment_color"],
        _name_font, _value_font, _comment_font,
    )

    # プレビュー用ダミー値
    _preview_row = ["SEG001", 42, "2026-07-30 12:00:00.000", 0, "2026-07-30 12:00:00.000", "LOT0001", 12.3456]
    _preview_row_dict = _row_to_dict(_preview_row, _preview_display_cols)
    for _col_idx, _lbl in _value_labels.items():
        _lbl.config(text=_format_value(_preview_row_dict.get(_col_idx), _dtype_map[_col_idx]))

    _root.mainloop() # プレビュー時のみ使用
