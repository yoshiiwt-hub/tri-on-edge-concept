# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Guard Band Learning
# Ver: 03.b.20260930    By: IWATA, Y.'s Agent AI
# Mod: tri_s20_learn_us_runner.py から呼び出される形に変更。設定ファイル
#      （tri_edge_21_learn_gb.json）の読込みを廃止し、main(TITLE, df, col_def, config)
#      として呼ばれる形式に統一（tri_s20_eda_runner.py の visualise() が
#      呼び出す各モジュールの main() と同じ引数構成）。データ読込みも呼び出し元
#      （runner）の load() に一本化されたため、本モジュール内では行わない。
########################################
learn_gb  # runnerから渡された波形データ（load_pp/sin_wave.py の load_wave_data() 等）から
          # ガードバンドを生成する

動作概要
    1. main(TITLE, df, col_def, config) が呼び出される
       （df: 波形データ。1セグメント=複数行。col_def: 対応する列定義）
    2. df の列名 "seg_ind"（セグメント内の行位置。sin_wave.py の命名）を、
       以降の集計・プロット関数が前提とする "seg_index" へ改名する
    3. seg_index ごとに min/max/mean/std（method="mm"）または
       IQR（method="iqr"）を集計し、ガードバンド（上限・下限）を生成する
    4. y方向バッファ・x方向バッファ（移動平均的な緩和）・前後端のオフセット（無効化）を適用する
    5. tri_edge_12_act_gb_diag.py がそのまま読める形式（{title}_GB_Def.csv）で保存する

    ※ 本モジュールは stream_core / TriEdge本体とは独立しており、
       tri_s20_learn_us_runner.py から呼び出して使う（単体実行は不可）
    ※ pandas/numpyを使用する（オフラインの学習専用モジュールのため、
       TriEdge本体（exeレイヤー）の「標準ライブラリのみ」方針の対象外）

config（呼び出し元 runner の LEARN_CONFIG で指定）
    val_to_judge   : ガードバンド判定対象の列名（df に含まれる数値列名。
                     例: "Sin" / "Cos" / "Tan"。デフォルト: "Sin"）
    title          : 出力ファイル名の接頭辞（デフォルト: TITLE）
    method         : "mm"（min/max/mean/std法, デフォルト） | "iqr"（四分位範囲法）
    buffer_x       : x方向（seg_index方向）の移動平均バッファ幅（デフォルト: 0）
    buffer_y       : [下方向バッファ, 上方向バッファ]（デフォルト: [0, 0]）
    offset_lw      : 下限側の前後端オフセット [前方N点, 後方N点] を無効化（デフォルト: [0, 0]）
    offset_up      : 上限側の前後端オフセット [前方N点, 後方N点] を無効化（デフォルト: [0, 0]）
    outdir         : 出力先ディレクトリ（デフォルト: "./output/"）
    encoding       : 出力CSVのエンコード（デフォルト: "cp932"）
    save_plot      : ガードバンドの可視化画像（{title}_GB_Plot.png）を保存するか（デフォルト: false）
                      ※ 判定（診断）結果の可視化ではなく、ガードバンド自体（上限・下限・元波形の重ね描き）の可視化
    plot_config    : 可視化画像の見た目設定（省略可。tri_edge_12_act_viz.py と同じキー構成）
        fig_w        : 画像の幅（インチ、デフォルト: 10）
        fig_h        : 画像の高さ（インチ、デフォルト: 6）
        fig_dpi      : 解像度（デフォルト: 150）
        field_color  : 背景色（デフォルト: "w"）
        edge_color   : 軸・文字・目盛りの色（デフォルト: "k"）
        line_color   : GB Upper（上限線）の色（デフォルト: "orangered"）
        line_color_2 : GB Lower（下限線）の色（デフォルト: "royalblue"）
        mean_color   : 平均線の色（デフォルト: "lime"）
        wave_color   : 元波形（重ね描き）の色（デフォルト: "gray"）
        fill_color   : 上限・下限の間の塗りつぶし色（デフォルト: "orange"）
        fill_alpha   : 塗りつぶしの不透明度（デフォルト: 0.1）
        wave_alpha   : 元波形の不透明度（デフォルト: 0.25）
        line_w       : 線幅（デフォルト: 2.0）
        font_size    : フォントサイズ（デフォルト: 10）
"""
MODULE_NAME = "LEARN_GB"

"""standard library"""
from   datetime import datetime as dt
import os
from   pathlib import Path
import sys
import traceback
from   typing   import Any, Dict, List, Tuple, Optional

"""external library（学習専用モジュールのため使用可）"""
import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
matplotlib.use("Agg")  # 画面表示なしの環境でも保存可能

"""local module"""
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    sys_dir = Path(sys._MEIPASS)
else:
    sys_dir = Path(__file__).resolve().parent.parent
if str(sys_dir) not in sys.path: sys.path.insert(0, str(sys_dir))
import tri_s29_util as utl


########################################
# ガードバンド生成
########################################
def _calc_mm(df: pd.DataFrame, col: str, col_lw: str, col_up: str) -> Tuple[pd.DataFrame, List[str]]:
    """min/max/mean/std 法でガードバンド生成"""
    stat = ["min", "max", "mean", "std"]

    df_gb = df.groupby("seg_index", as_index=False).agg({col: stat})
    df_gb.columns = ["seg_index"] + stat

    df_gb[col_lw] = df_gb["min"]
    df_gb[col_up] = df_gb["max"]

    df_gb["mean_up_std"] = df_gb["mean"] + df_gb["std"]
    df_gb["mean_lw_std"] = df_gb["mean"] - df_gb["std"]
    df_gb.drop(columns="std", inplace=True)

    stat_cols = [col_lw, col_up, "min", "max", "mean", "mean_up_std", "mean_lw_std"]
    return df_gb, stat_cols


def _calc_iqr(df: pd.DataFrame, col: str, col_lw: str, col_up: str) -> Tuple[pd.DataFrame, List[str]]:
    """IQR 法でガードバンド生成"""
    def pct(n: float):
        def f(x):
            return np.percentile(x, n)
        f.__name__ = f"ptl{int(n * 100)}"
        return f

    pct_list = [0.25, 0.50, 0.75]

    df_gb = df.groupby("seg_index", as_index=False).agg(
        {col: ["min"] + [pct(p) for p in pct_list] + ["max"]}
    )
    df_gb.columns = ["seg_index"] + [f"pct{int(p * 100)}" for p in [0] + pct_list + [100]]

    q1 = df_gb["pct25"]
    q3 = df_gb["pct75"]

    df_gb[col_lw] = q1 - 1.5 * abs(q3 - q1)
    df_gb[col_up] = q3 + 1.5 * abs(q3 - q1)

    stat_cols = [col_lw, col_up] + list(df_gb.columns[1:])
    return df_gb, stat_cols


def _apply_y_buffer(df_gb: pd.DataFrame, df_base: pd.DataFrame, col: str,
                     col_lw: str, col_up: str, buf_y: Tuple[float, float]) -> None:
    """上下方向の buffer を適用"""
    span = abs(df_base[col].max() - df_base[col].min()) / 100
    df_gb[col_lw] -= span * buf_y[0]
    df_gb[col_up] += span * buf_y[1]


def _apply_offset(df_gb: pd.DataFrame, col: str, offset: Tuple[int, int]) -> None:
    """前後 offset を None 化（この位置は判定をスキップする、という意味になる）"""
    left, right = offset
    if left > 0:
        df_gb.loc[df_gb.index[:left], col] = None
    if right > 0:
        df_gb.loc[df_gb.index[-right:], col] = None


def _plot_gb(
    df_gb: pd.DataFrame,
    df_wv: pd.DataFrame,
    val_to_judge: str,
    col_lw: str,
    col_up: str,
    title: str,
    outdir: str,
    plot_config: Optional[Dict[str, Any]] = None,
) -> str:
    """
    ガードバンド自体を可視化して画像保存する（matplotlib使用。判定結果の可視化ではない）
    Mod: 2026.07.17    By: IWATA,Y.（tri_edge_12_act_viz.py と同じ plot_config の考え方を移植。
         配色・線幅・フォントサイズ・画像サイズ・解像度を設定ファイルから調整できるようにした）
    上限・下限に加えて、元波形の全セグメントを薄く重ね描きして、
    ガードバンドが実データをどう包んでいるか一目で分かるようにする。
    """
    plot_config = plot_config or {}
    fig_w        = plot_config.get("fig_w"        , 10)
    fig_h        = plot_config.get("fig_h"        , 6)
    fig_dpi      = plot_config.get("fig_dpi"      , 150)
    field_color  = plot_config.get("field_color"  , "w")
    edge_color   = plot_config.get("edge_color"   , "k")
    line_color   = plot_config.get("line_color"   , "orangered")   # GB Upper
    line_color_2 = plot_config.get("line_color_2" , "royalblue")   # GB Lower
    mean_color   = plot_config.get("mean_color"   , "lime")
    wave_color   = plot_config.get("wave_color"   , "gray")
    fill_color   = plot_config.get("fill_color"   , "orange")
    fill_alpha   = plot_config.get("fill_alpha"   , 0.1)
    wave_alpha   = plot_config.get("wave_alpha"   , 0.25)
    line_w       = plot_config.get("line_w"       , 2.0)
    font_size    = plot_config.get("font_size"    , 10)

    # tri_edge_12_act_viz.py と同じ、figure/axes全体の配色スタイル指定
    plt.rc("figure" , fc=field_color, ec=edge_color)
    plt.rc("savefig", fc=field_color, ec=edge_color)
    plt.rc("axes"   , fc=field_color, ec=edge_color, labelcolor=edge_color, titlecolor=edge_color)
    plt.rc("xtick"  , color=edge_color)
    plt.rc("ytick"  , color=edge_color)
    plt.rc("text"   , color=edge_color)
    plt.rc("font"   , size=font_size)

    fig, ax = plt.subplots(figsize=(fig_w, fig_h))

    # 元波形（全セグメント）を薄く重ね描き
    for seg_id, g in df_wv.groupby("seg_id"):
        ax.plot(g["seg_index"], g[val_to_judge], color=wave_color, alpha=wave_alpha, linewidth=0.8, zorder=1)

    # ガードバンド（上限・下限・平均）
    ax.plot(df_gb["seg_index"], df_gb[col_up], color=line_color,   linewidth=line_w,       label="GB Upper", zorder=3)
    ax.plot(df_gb["seg_index"], df_gb[col_lw], color=line_color_2, linewidth=line_w,       label="GB Lower", zorder=3)
    if "mean" in df_gb.columns:
        ax.plot(df_gb["seg_index"], df_gb["mean"], color=mean_color, linewidth=line_w * 0.6, linestyle="--", label="mean", zorder=2)
    ax.fill_between(df_gb["seg_index"], df_gb[col_lw], df_gb[col_up], color=fill_color, alpha=fill_alpha, zorder=0)

    ax.set_title(f"Guard Band: {title} ({val_to_judge})", fontsize=font_size + 2, color=edge_color)
    ax.set_xlabel("seg_index", fontsize=font_size, color=edge_color)
    ax.set_ylabel(val_to_judge, fontsize=font_size, color=edge_color)
    ax.tick_params(labelsize=font_size)
    ax.legend(loc="best", fontsize=font_size)
    ax.grid(True, alpha=0.3)

    os.makedirs(outdir, exist_ok=True)
    save_path = os.path.join(outdir, f"{title}_GB_Plot_{dt.now().strftime('%Y%m%d%H%M%S')}.png")
    fig.tight_layout()
    fig.savefig(save_path, dpi=fig_dpi, facecolor=field_color, edgecolor=edge_color)
    plt.close(fig)

    return save_path


def gen_wave_gb(df_wv: pd.DataFrame, val_to_judge: str, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    波形に対してガードバンドを生成する
    Mod: 2026.07.17    By: IWATA,Y.（_gb_mt_learn_diag.py の gen_wave_gb を移植。
         save_plot オプションでガードバンド自体の可視化画像を保存できるようにした。
         ※ 判定（診断）結果の可視化は対象外。ガードバンドそのものの可視化のみ）
    """
    title:  str = cfg.get("title", "GB")
    method: str = cfg.get("method", "mm")
    buf_x:  int = int(cfg.get("buffer_x", 0))
    buf_y:  Tuple[float, float] = tuple(cfg.get("buffer_y", (0, 0)))
    offset_lw: Tuple[int, int] = tuple(cfg.get("offset_lw", (0, 0)))
    offset_up: Tuple[int, int] = tuple(cfg.get("offset_up", (0, 0)))
    outdir: str = cfg.get("outdir", "./output/")
    save_plot: bool = bool(cfg.get("save_plot", False))
    plot_config: Dict[str, Any] = cfg.get("plot_config", {})

    col_lw = f"{val_to_judge}_GB_Lw"
    col_up = f"{val_to_judge}_GB_Up"

    if method == "mm":
        df_gb, stat_cols = _calc_mm(df_wv, val_to_judge, col_lw, col_up)
    elif method == "iqr":
        df_gb, stat_cols = _calc_iqr(df_wv, val_to_judge, col_lw, col_up)
    else:
        raise ValueError(f"Unknown method: {method}")

    _apply_y_buffer(df_gb, df_wv, val_to_judge, col_lw, col_up, buf_y)

    if buf_x > 0:
        df_gb[col_lw] = df_gb[col_lw].rolling(buf_x, center=True, min_periods=1).min()
        df_gb[col_up] = df_gb[col_up].rolling(buf_x, center=True, min_periods=1).max()

    _apply_offset(df_gb, col_lw, offset_lw)
    _apply_offset(df_gb, col_up, offset_up)

    os.makedirs(outdir, exist_ok=True)
    save_path = os.path.join(outdir, f"{title}_GB_Def_{dt.now().strftime('%Y%m%d%H%M%S')}.csv")
    df_gb.to_csv(save_path, index=False, encoding=cfg.get("encoding", "cp932"))
    utl.log(f"ガードバンド定義を保存: {save_path}  行数={len(df_gb)}", MODULE_NAME)

    if save_plot:
        plot_path = _plot_gb(df_gb, df_wv, val_to_judge, col_lw, col_up, title, outdir, plot_config)
        utl.log(f"ガードバンド可視化画像を保存: {plot_path}", MODULE_NAME)

    return df_gb


########################################
# Entry Point（runnerから呼び出される）
########################################
def main(TITLE: str, df: pd.DataFrame, col_def: Dict[str, Any], config: Dict[str, Any]) -> None:
    """
    tri_s20_learn_us_runner.py から呼び出されるエントリポイント
    （tri_s20_eda_runner.py の visualise() が各可視化モジュールを呼び出すのと同じ
    引数構成: main(TITLE, df, col_def, config)）
    """
    utl.log("学習開始", MODULE_NAME)

    try:
        if "seg_ind" not in df.columns:
            raise ValueError(f"seg_ind 列がありません（列={list(df.columns)}）。波形データ"
                              f"（1セグメント=複数行）を返すロード関数を指定してください。")
        df = df.rename(columns={"seg_ind": "seg_index"})

        val_to_judge = config.get("val_to_judge", "Sin")
        if val_to_judge not in df.columns:
            raise ValueError(f"val_to_judge={val_to_judge!r} が df に存在しません（列={list(df.columns)}）")
        utl.log(f"val_to_judge={val_to_judge!r}  df.shape={df.shape}", MODULE_NAME)

        gen_wave_gb(df, val_to_judge, {
            "title":      config.get("title", TITLE),
            "method":     config.get("method", "mm"),
            "buffer_x":   config.get("buffer_x", 0),
            "buffer_y":   config.get("buffer_y", (0, 0)),
            "offset_lw":  config.get("offset_lw", (0, 0)),
            "offset_up":  config.get("offset_up", (0, 0)),
            "outdir":     config.get("outdir", "./output/"),
            "encoding":   config.get("encoding", "cp932"),
            "save_plot":  config.get("save_plot", False),
            "plot_config": config.get("plot_config", {}),
        })

    except Exception as e:
        utl.log(f"ERROR  {e}\n{traceback.format_exc()}", MODULE_NAME)
        raise

    utl.log("学習完了", MODULE_NAME)
