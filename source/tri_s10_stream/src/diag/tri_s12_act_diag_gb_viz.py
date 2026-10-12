# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [Guard Band Diagnosis - Visualization Demo]
# Ver: 02.a.20261004    By: IWATA, Y.'s Agent AI
# Mod: 終点行の除外を各 Act の [:-1] から core へ移し、CUT_LAST_ROW 属性で宣言する形に変更（B-2）
########################################
a_gb_diag_viz_trigger  # 波形データを事前学習済みガードバンドと比較し、判定結果を画像で保存するデモ用 Act Trigger

動作概要
    1. 起動時に models（複数モデルの定義）を全て検証・コンパイルする
       （モデルごとのガードバンド定義CSVを事前に読込んでおく。実行中の都度読込みはしない）
    2. ループ内でキューからセグメントを受信し項目数の整合性を確認する
    3. セグメントの終点行（変化後）を除外してから判定する
    4. model_select_col_idx の値で使用するモデルを選択する
       （model_val_mapで値→モデルキーのn対1対応。該当モデルが無ければdefault_modelにフォールバック。
       それも無ければ判定をスキップする）
    5. セグメント内の各行（位置＝seg_index相当）について、選択したモデルのtarget_col_idxの値を
       ガードバンド定義の上限・下限と比較し、異常点を判定する
    6. 波形（実測値の折れ線）・ガードバンド上限/下限・逸脱した点への×印を1枚の画像に描画して保存する
       （1セグメント=1画像）
    7. stop_event が立ちキューが空になった時点でループを終了する

    ※ 本トリガーは「判定結果を画像として見る」デモ用であり、CSV保存は行わない
      （CSV保存が必要な場合は tri_edge_12_act_gb_diag.py を使用すること）
    ※ pandas/numpyは直接使用しない（matplotlibの内部依存を除き、標準ライブラリのみで実装）

ガードバンド定義CSV（gb_def_csv）のフォーマット
    tri_edge_12_act_gb_diag.py / learn_gb.py と共通。
    必須列: seg_index列（デフォルト列名 "seg_index"）、
            下限列（{gb_col_name}_GB_Lw）、上限列（{gb_col_name}_GB_Up）

接続設定
    save_dir             : 画像保存先ディレクトリ（system_root からの相対パス、デフォルト: "output"）
    file_pfx              : 画像ファイル名のプレフィックス（デフォルト: "gbdiag_viz"）
    encode                 : ガードバンド定義CSV読込みの文字エンコーディング（デフォルト: "utf-8"）

    model_select_col_idx  : モデル選択に使う列のインデックス（必須。品番・設備状態等が入る列）
    model_val_map          : 値 → モデルキー のn対1対応（必須）
                              {"モデルキー": ["値1", "値2", ...], ...}
    models                 : モデル定義の辞書（必須。1つ以上）
                              {"モデルキー": {
                                  "target_col_idx": ...,
                                  "gb_def_csv": ...,
                                  "gb_col_name": ...,
                                  "gb_seg_index_col": ...（省略可、デフォルト"seg_index"）
                              }, ...}
    default_model          : model_select_col_idx の値がどのモデルにも一致しない場合に
                              フォールバックするモデルキー（省略可）。
                              省略時、該当モデルが無いセグメントは判定をスキップする（警告ログ出力）

    plot_config           : 画像の見た目設定（省略可。tri_s12_act_viz.py / learn_gb.py と同じ考え方）
        fig_w               : 画像の幅（インチ、デフォルト: 10）
        fig_h               : 画像の高さ（インチ、デフォルト: 6）
        fig_dpi             : 解像度（デフォルト: 150）
        field_color         : 背景色（デフォルト: "w"）
        edge_color          : 軸・文字・目盛りの色（デフォルト: "k"）
        line_color          : 実測値の線の色（デフォルト: "darkorange"）
        line_color_2        : ガードバンド上限・下限の線の色（デフォルト: "royalblue"）
        fill_color          : 上限・下限の間の塗りつぶし色（デフォルト: "royalblue"）
        fill_alpha          : 塗りつぶしの不透明度（デフォルト: 0.08）
        mark_color          : 逸脱点の×印の色（デフォルト: "red"）
        mark_size           : 逸脱点の×印の大きさ（デフォルト: 100）
        line_w              : 線幅（デフォルト: 2.0）
        font_size           : フォントサイズ（デフォルト: 10）
"""
MODULE_NAME = "ACT_GB_Diag_Viz"

"""standard library"""
import csv
from   datetime import datetime as dt
import json
import os
import queue
import re
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Tuple, Sequence, Optional

"""external library"""
import matplotlib
matplotlib.use("Agg")  # 画面表示なしの環境でも保存できるようにする
import matplotlib.pyplot as plt

"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()
    
except ModuleNotFoundError:
    try: # Check and Add Path
        cur_dir  = os.path.dirname(os.path.abspath(__file__))
        dir_name = os.path.basename(cur_dir)
        if dir_name != 'diag':
            raise FileNotFoundError(f".py MUST be in 'src/diag' dir. Current: {dir_name}")
        
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
import src.tri_s19_util        as utl

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
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# Guard Band Helper
########################################
_GB_DIAG_COLS: List[str] = ["seg_id", "anomaly_wave", "n_points", "n_anomaly_points", "anomaly_indices"]


def _load_gb_def(csv_path: str, seg_index_col: str, gb_col_name: str, encode_: str) -> Dict[int, Tuple[Optional[float], Optional[float]]]:
    """
    ガードバンド定義CSVを読込み、{seg_index: (下限, 上限)} の辞書を返す（標準csvモジュールのみ使用）。
    下限・上限が空欄（_gb_mt_learn_diag.py の offset_trimming で None 化された行）は None のまま保持する
    （None の位置は、判定をスキップする＝そのseg_indexでは異常判定しないことを意味する）。
    """
    import csv as _csv

    col_lw = f"{gb_col_name}_GB_Lw"
    col_up = f"{gb_col_name}_GB_Up"
    gb_bounds: Dict[int, Tuple[Optional[float], Optional[float]]] = {}

    with open(csv_path, "r", newline="", encoding=encode_) as f:
        reader = _csv.DictReader(f)
        if seg_index_col not in (reader.fieldnames or []):
            raise ValueError(f"gb_def_csv に列 {seg_index_col!r} がありません。列一覧={reader.fieldnames}")
        if col_lw not in (reader.fieldnames or []) or col_up not in (reader.fieldnames or []):
            raise ValueError(f"gb_def_csv に列 {col_lw!r}/{col_up!r} がありません。列一覧={reader.fieldnames}")

        for row in reader:
            try:
                seg_index = int(float(row[seg_index_col]))
            except (ValueError, TypeError):
                continue

            def _to_float(s: Any) -> Optional[float]:
                if s is None or str(s).strip() == "":
                    return None
                try:
                    return float(s)
                except ValueError:
                    return None

            gb_bounds[seg_index] = (_to_float(row[col_lw]), _to_float(row[col_up]))

    return gb_bounds


########################################
# Plot Helper
########################################
def _plot_diag(
    segment: List[List[Any]],
    target_col_idx: int,
    gb_bounds: Dict[int, Tuple[Optional[float], Optional[float]]],
    anomaly_indices: List[int],
    seg_id: Any,
    title: str,
    outdir: str,
    plot_config: Optional[Dict[str, Any]] = None,
) -> str:
    """
    ガードバンド判定結果を画像化する（matplotlib使用）
    Mod: 2026.07.18    By: IWATA,Y.（_gb_mt_learn_diag.py の _visualize_per_wave を、
         外部ライブラリ（_AnalyzeFunctionsVZ_DT）に依存せず matplotlib のみで再実装）
    実測値の折れ線・ガードバンド上限/下限・逸脱した点への×印を1枚の画像に描画する。
    """
    plot_config = plot_config or {}
    fig_w        = plot_config.get("fig_w"       , 10)
    fig_h        = plot_config.get("fig_h"       , 6)
    fig_dpi      = plot_config.get("fig_dpi"     , 150)
    field_color  = plot_config.get("field_color" , "w")
    edge_color   = plot_config.get("edge_color"  , "k")
    line_color   = plot_config.get("line_color"  , "darkorange")   # 実測値
    line_color_2 = plot_config.get("line_color_2", "royalblue")    # GB上限・下限
    fill_color   = plot_config.get("fill_color"  , "royalblue")
    fill_alpha   = plot_config.get("fill_alpha"  , 0.08)
    mark_color   = plot_config.get("mark_color"  , "red")
    mark_size    = plot_config.get("mark_size"   , 100)
    line_w       = plot_config.get("line_w"      , 2.0)
    font_size    = plot_config.get("font_size"   , 10)

    _nan = float("nan")
    x     = list(range(len(segment)))
    y     = [rec[target_col_idx] if isinstance(rec[target_col_idx], (int, float)) else _nan for rec in segment]
    gb_lw = [gb_bounds.get(i, (None, None))[0] for i in x]
    gb_up = [gb_bounds.get(i, (None, None))[1] for i in x]
    gb_lw = [v if v is not None else _nan for v in gb_lw]
    gb_up = [v if v is not None else _nan for v in gb_up]

    plt.rc("figure" , fc=field_color, ec=edge_color)
    plt.rc("savefig", fc=field_color, ec=edge_color)
    plt.rc("axes"   , fc=field_color, ec=edge_color, labelcolor=edge_color, titlecolor=edge_color)
    plt.rc("xtick"  , color=edge_color)
    plt.rc("ytick"  , color=edge_color)
    plt.rc("text"   , color=edge_color)
    plt.rc("font"   , size=font_size)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    ax.fill_between(x, gb_lw, gb_up, color=fill_color, alpha=fill_alpha, zorder=0, label="Guard Band")
    ax.plot(x, gb_up, color=line_color_2, linewidth=line_w * 0.7, linestyle="--", zorder=1)
    ax.plot(x, gb_lw, color=line_color_2, linewidth=line_w * 0.7, linestyle="--", zorder=1)
    ax.plot(x, y,     color=line_color,   linewidth=line_w,      zorder=2, label="Value")

    if anomaly_indices:
        mark_x = anomaly_indices
        mark_y = [y[i] for i in anomaly_indices]
        ax.scatter(mark_x, mark_y, marker="x", s=mark_size, c=mark_color, linewidths=line_w, zorder=3, label="Anomaly")

    anomaly_wave = 1 if anomaly_indices else 0
    status = "NG" if anomaly_wave else "OK"
    ax.set_title(f"GB Diag: {title}  seg_id={seg_id}  [{status}]", fontsize=font_size + 2, color=edge_color)
    ax.set_xlabel("seg_index", fontsize=font_size, color=edge_color)
    ax.set_ylabel("value", fontsize=font_size, color=edge_color)
    ax.tick_params(labelsize=font_size)
    ax.legend(loc="best", fontsize=font_size)
    ax.grid(True, alpha=0.3)

    os.makedirs(outdir, exist_ok=True)
    safe_seg_id = re.sub(r"[^0-9A-Za-z_-]", "", str(seg_id))
    save_path = os.path.join(outdir, f"{title}_{safe_seg_id}_{status}.png")
    fig.tight_layout()
    fig.savefig(save_path, dpi=fig_dpi, facecolor=field_color, edgecolor=edge_color)
    plt.close(fig)

    return save_path


########################################
# Act Trigger Functions
########################################

def a_gb_diag_viz_trigger(
    post_queue: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    波形データを事前学習済みガードバンドと比較し、判定結果を画像で保存する（デモ用）
    Mod: 2026.07.18    By: IWATA,Y.
    """
    TRIGGER_NAME = "a_gb_diag_viz_trigger"
    func_name = sys._getframe().f_code.co_name
    func_obj = globals().get(func_name)
    func_qmode = getattr(func_obj, "QMODE", "imq")

    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)
        save_dir = os.path.join(system_root, config.get("save_dir", "output"))
        os.makedirs(save_dir, exist_ok=True)
        encode_  = config.get("encode", "utf-8")
        file_pfx = config.get("file_pfx", "gbdiag_viz")

        """データ項目定義"""
        n_col_def = len(col_def)#項目数
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_keys={col_keys}", log_file)

        """ モデル選択設定
            model_select_col_idx の値（品番・設備状態等）でモデル（ガードバンド定義）を切り替える。
            model_val_map で「値 → モデルキー」をn対1で対応付ける。
            全モデルは起動時に一括で検証・コンパイルしておき、実行中の都度ファイル読込みは行わない。
            該当モデルが無いセグメントは、default_model が指定されていればそれにフォールバックし、
            無ければ判定をスキップする（安全側に倒す。クラッシュさせない）。
        """
        model_select_col_idx = config.get("model_select_col_idx")
        if model_select_col_idx is None:
            raise ValueError("model_select_col_idx is required")
        model_select_col_idx = int(model_select_col_idx)
        if not (0 <= model_select_col_idx < n_col_def):
            raise ValueError(f"model_select_col_idx={model_select_col_idx} out of range (0-{n_col_def-1})")

        models_cfg = config.get("models")
        if not models_cfg:
            raise ValueError("models is required (at least 1 model)")

        model_val_map_cfg = config.get("model_val_map")
        if not model_val_map_cfg:
            raise ValueError("model_val_map is required (値 → モデルキー の対応)")

        # 値 → モデルキー の逆引き辞書を構築（n対1）。同じ値が複数モデルに重複していないか検証する。
        value_to_model: Dict[str, str] = {}
        for model_key, val_list in model_val_map_cfg.items():
            if str(model_key) not in models_cfg:
                raise ValueError(f"model_val_map のモデルキー {model_key!r} が models にありません: {list(models_cfg.keys())}")
            for raw_val in val_list:
                v = str(raw_val)
                if v in value_to_model and value_to_model[v] != str(model_key):
                    raise ValueError(
                        f"model_val_map の値 {v!r} が複数のモデル（{value_to_model[v]!r} と {model_key!r}）"
                        f"に重複して割り当てられています")
                value_to_model[v] = str(model_key)

        default_model = config.get("default_model")
        if default_model is not None and str(default_model) not in models_cfg:
            raise ValueError(f"default_model={default_model!r} not found in models: {list(models_cfg.keys())}")

        compiled_models: Dict[str, Dict[str, Any]] = {}
        for model_key, model_cfg in models_cfg.items():
            """判定対象列（モデルごとに指定可能）"""
            _target_col_idx = model_cfg.get("target_col_idx")
            if _target_col_idx is None:
                raise ValueError(f"models[{model_key!r}].target_col_idx is required")
            _target_col_idx = int(_target_col_idx)
            if not (0 <= _target_col_idx < n_col_def):
                raise ValueError(f"models[{model_key!r}].target_col_idx={_target_col_idx} out of range (0-{n_col_def-1})")

            """ガードバンド定義CSVの読込み（標準csvモジュールのみ。pandas不使用）"""
            _gb_def_csv = model_cfg.get("gb_def_csv")
            if not _gb_def_csv:
                raise ValueError(f"models[{model_key!r}].gb_def_csv is required")
            _gb_def_csv = base.get_absolute_path(_gb_def_csv)

            _gb_col_name = model_cfg.get("gb_col_name")
            if not _gb_col_name:
                raise ValueError(f"models[{model_key!r}].gb_col_name is required")

            _gb_seg_index_col = model_cfg.get("gb_seg_index_col", "seg_index")

            _gb_bounds = _load_gb_def(_gb_def_csv, _gb_seg_index_col, _gb_col_name, encode_)

            compiled_models[str(model_key)] = {
                "target_col_idx": _target_col_idx,
                "gb_bounds": _gb_bounds,
            }
            base.output_log(MODULE_NAME,
                f"model={model_key!r}  target_col_idx={_target_col_idx}  gb_def_csv={_gb_def_csv}  "
                f"gb_col_name={_gb_col_name}  gb_seg_index_col={_gb_seg_index_col}  entries={len(_gb_bounds)}", log_file)

        base.output_log(MODULE_NAME,
            f"model_select_col_idx={model_select_col_idx}  models={list(compiled_models.keys())}  "
            f"model_val_map={model_val_map_cfg}  default_model={default_model}", log_file)

        """画像の見た目設定（省略可）"""
        plot_config = config.get("plot_config", {})
        base.output_log(MODULE_NAME, f"file_pfx={file_pfx}  plot_config={plot_config}", log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== LOOP START ========"""
    file_counter = 1
    while True:
        try:
            segment = post_queue.get(timeout=1.0)
        
        except queue.Empty:
            if stop_event.is_set() and post_queue.qsize() == 0 and getattr(post_queue, "unfinished_tasks", 0) == 0: break
            continue
        
        if segment is None:
            post_queue.task_done()
            break
        
        try:
            """項目の整合性（※現状、個数のみ）"""
            n_col_act = len(segment[0])
            if n_col_act != n_col_def:
                base.output_log(MODULE_NAME,
                f"Segment Columns NOT Match with Columns Define: seg_id={segment[0][0]}  Expected:{n_col_def}  Actual:{n_col_act}", log_file)
                continue
            
            """セグメントid"""
            seg_id = segment[0][0]
            
            """終点行（変化後）の除外は core が行う（CUT_LAST_ROW = True）"""
            
            """空セグメントチェック"""
            if len(segment) == 0:
                base.output_log(MODULE_NAME, f"WARNING  セグメントが空です。スキップします。id={seg_id}")
                continue
            
            """空レコードチェック"""
            print(segment[0])
            if (isinstance(segment[0][-1], list)) and (segment[0][-1][0] is None):
                base.output_log(MODULE_NAME, f"WARNING  レコードが空です。スキップします。id={seg_id}")
                continue

            """モデル選択（value_to_modelで値→モデルキーを引き、該当モデルを取得する）"""
            raw_val = str(segment[0][model_select_col_idx])
            model_key = value_to_model.get(raw_val)
            model = compiled_models.get(model_key) if model_key else None
            if model is None and default_model is not None:
                model_key = str(default_model)
                model = compiled_models.get(model_key)
            if model is None:
                base.output_log(MODULE_NAME,
                    f"WARNING  該当モデルが見つかりません。判定をスキップします。"
                    f"id={seg_id}  raw_val={raw_val!r}  models={list(compiled_models.keys())}", log_file)
                continue

            target_col_idx = model["target_col_idx"]
            gb_bounds       = model["gb_bounds"]

            """ガードバンド判定（標準ライブラリのみ。pandas不使用）
               seg_index はセグメント内での位置（0始点）として扱う。
               ガードバンド定義に該当する seg_index が無い、上限/下限が None の位置、
               target_col_idx の値が数値でない位置（設定ミス等）は判定をスキップする
               （_gb_mt_learn_diag.py の offset_trimming 等と同じ扱い）。"""
            anomaly_indices: List[int] = []
            n_type_error = 0
            for seg_index, rec in enumerate(segment):
                val = rec[target_col_idx]
                bounds = gb_bounds.get(seg_index)
                if bounds is None or val is None:
                    continue
                if not isinstance(val, (int, float)):
                    n_type_error += 1
                    continue
                lw, up = bounds
                if lw is not None and val < lw:
                    anomaly_indices.append(seg_index)
                elif up is not None and val > up:
                    anomaly_indices.append(seg_index)

            if n_type_error > 0:
                base.output_log(MODULE_NAME,
                    f"WARNING  target_col_idx={target_col_idx} の値が数値でない点が"
                    f"{n_type_error}件ありました（判定をスキップ）。target_col_idxの設定を"
                    f"見直してください。id={seg_id}  例: {segment[0][target_col_idx]!r}", log_file)

            anomaly_wave     = 1 if anomaly_indices else 0
            n_points         = len(segment)
            n_anomaly_points = len(anomaly_indices)
            
            if anomaly_wave == 1:
                base.output_log(MODULE_NAME,
                    f"!!! 異常な波形を検出しました !!!  波形ID = {seg_id}  "
                    f"正常範囲 逸脱数 = {n_anomaly_points}  "
                    f"正常範囲 逸脱箇所 = {anomaly_indices}", log_file)

            """判定結果を画像化（1セグメント=1画像。実測値・ガードバンド上下限・逸脱点への×印）"""
            plot_path = _plot_diag(
                segment, target_col_idx, gb_bounds, anomaly_indices,
                seg_id, f"{file_pfx}_{model_key}", save_dir, plot_config)

            base.output_log(MODULE_NAME,
                f"{os.path.basename(plot_path)} 画像保存完了: id={seg_id}  model={model_key}  n_points={n_points}  "
                f"anomaly_wave={anomaly_wave}  n_anomaly_points={n_anomaly_points}  "
                f"anomaly_indices={anomaly_indices}", log_file)

            file_counter += 1
        
        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break  # finally経由でtask_done()後にループ離脱
            continue # ループ継続
        
        finally:
            post_queue.task_done()
    """======== LOOP END ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None

a_gb_diag_viz_trigger.QMODE = "imq"
a_gb_diag_viz_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, f"Run Standalone.")
    base.output_log(MODULE_NAME, f"test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)