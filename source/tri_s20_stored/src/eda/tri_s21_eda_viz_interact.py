# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# EDA Interactive Visualizer
# Ver: 01.e.20260930    By: IWATA, Y.'s Agent AI
# Mod: 波形重ね合わせ（gen_interact_layered_wave）が、AnalyzeFunctionsVZ_UI.py内部で
#      'WVID'（波形ID）・'WVPT'（波形内位置）という列名を直接ハードコードして参照している
#      ことが判明した。TriEdge標準のセグメントデータ（seg_id/seg_ind列）では列名が
#      一致せず、is_wave判定が常にFalseになっていたため、_prepare_wave_columns() を追加し、
#      seg_id/seg_ind から WVID/WVPT へ自動変換（dfはコピーしてから追加。元データは変更しない）
#      するようにした。あわせて _save_offline() 内の traceback.format_exc() 重複呼び出しも修正。
########################################
tri_s20_eda_runner.py の visualise() ステップから呼び出せる、対話的（Plotly）可視化モジュール。
AnalyzeFunctionsVZ_UI.py（既存の対話的可視化ライブラリ）が持つ以下の関数をラップする。
    gen_interact_scatter        : 散布図（変数間の関係性）
    gen_interact_dist           : 箱ひげ→分布
    gen_interact_linechart      : 折れ線（時系列／値順の傾向）
    gen_interact_layered_wave   : 波形重ね合わせ（df に 'WVID' 列がある場合のみ）

col_def（tri_s20_eda_runner.py の load ステップが返す列定義辞書）から、各関数が必要と
する引数（l_ql_, l_qn_, tar_）を自動的に組み立てる（tri_edge_22_eda_visualizer.py の
_parse_col_def() と同じロジック）。

動作概要
    1. col_def を走査し、featurename・targetname・IsQualittv・IsQuanttv を組み立てる
    2. 各 gen_interact_* 関数を呼び出し、Figure（plotly.graph_objects.Figure）を生成
       - 1つの生成に失敗しても、残りの生成・出力を止めない（_safe() で保護）
    3. mode に応じて Figure を出力する
       - "offline"（デフォルト）: plotly.offline.plot() で HTML ファイルとして OUTPUT フォルダへ保存する
       - "online"                : ファイルには一切保存せず、メモリ上の Figure をそのまま
                                     fig.show() で対話的に即時表示する（AnalyzeFunctionsVZ_UI.py の
                                     __main__ ブロックの fig.show() と同じ）。実行環境の既定
                                     レンダラー（ブラウザ・Jupyter等）に依存するため、GUI/ブラウザの
                                     無いサーバー環境等では失敗する（この場合、ファイル保存への
                                     フォールバックは行わない。"ファイルを作らず今すぐ見る"のが
                                     このmodeの目的のため）

col_def フォーマット（tri_s20_eda_runner.py の load ステップと共通）
    {"列インデックス(str)": [name, dtype, role, var_type, comment], ...}
      role    : "key" | "numetory" | "category"
      var_type: "key" | "explanatory" | "objective"

呼び出し方（tri_s20_eda_runner.py から）
    VIZ_MODULE = "tri_edge_23_eda_interactive"
    visualise(TITLE, ["eda", VIZ_MODULE, "main"], OUTDIR, df, col_def)

    online/offline を明示的に切り替えたい場合は、直接呼び出す側で mode を渡す
    （tri_s20_eda_runner.py 側の呼び出しシグネチャ _func(OUTPUT, TITLE, df, col_def) は
     固定のため、Runner経由では常にデフォルト値 "offline" が使われる）:
        import eda.tri_edge_23_eda_interactive as viz
        viz.main(OUTDIR, TITLE, df, col_def, mode="online")

前提
    AnalyzeFunctionsVZ_UI.py が Pythonパスの通った場所に存在していること。
"""
MODULE_NAME = "Eda_Interactive"

"""standard library"""
import os
import sys
import traceback
from   pathlib import Path
from   typing  import Any, Dict, List, Tuple, Optional

"""local module"""
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    sys_dir = Path(sys._MEIPASS)
else:
    sys_dir = Path(__file__).resolve().parent.parent
if str(sys_dir) not in sys.path: sys.path.insert(0, str(sys_dir))
import tri_s29_util as utl


########################################
# col_def -> gen_interact_*() 引数への変換
########################################
def _parse_col_def(
    col_def: Dict[str, List[Any]]
) -> Tuple[List[str], Optional[str], List[str], List[str]]:
    """
    col_def を走査し、gen_interact_*() が必要とするパラメータ一式を組み立てる。
    tri_edge_22_eda_visualizer.py の _parse_col_def() と同じロジック
    （目的変数の role による target_is 判定は gen_interact_* 側では不要なため省いている）。
    Mod: 2026.08.09    By: IWATA,Y.

    Returns:
        featurename : explanatory列の列名リスト（本モジュールでは参考情報。gen_interact_*には未使用）
        targetname  : objective列の列名（1つのみ想定。複数ある場合は最初の1つを使う）
        IsQualittv  : role="category" の列名リスト
        IsQuanttv   : role="numetory" の列名リスト
    """
    featurename: List[str] = []
    targetname:  Optional[str] = None
    IsQualittv:  List[str] = []
    IsQuanttv:   List[str] = []

    for k in sorted(col_def.keys(), key=lambda s: int(s) if str(s).isdigit() else s):
        entry = col_def[k]
        name, dtype, role, var_type = entry[0], entry[1], entry[2], entry[3]

        if role == "category":
            IsQualittv.append(name)
        elif role == "numetory":
            IsQuanttv.append(name)
        # role == "key" は可視化対象外

        if var_type == "explanatory":
            featurename.append(name)
        elif var_type == "objective":
            if targetname is None:
                targetname = name

    return featurename, targetname, IsQualittv, IsQuanttv


def _safe(label_: str, func_, *args_, hint_: str = ""):
    """
    1つのグラフ生成が失敗しても、残りのグラフ生成・保存を止めないための保護ラッパー。
    AnalyzeFunctionsVZ_DT.py の gen_viz_report() に加えた _safe() と同じ考え方。
    Mod: 2026.08.09    By: IWATA,Y.
    Mod: 2026.08.09    By: IWATA,Y.  hint_ 引数を追加。エラーメッセージだけでは原因が分かりにくい
         ケース（例: 目的変数未設定時の "TypeError: None"）向けに、呼び出し側からヒント文言を渡せるようにした。
    """
    try:
        return func_(*args_)
    except Exception as e_:
        utl.log(f"WARNING  '{label_}' の生成中にエラーが発生したためスキップします: {e_}{hint_}", MODULE_NAME)
        print(traceback.format_exc(), flush=True)
        return None


def _save_fig(fig, outdir: str, title: str, suffix: str, mode: str) -> None:
    """
    Figure を mode に応じて出力する。
    Mod: 2026.08.09    By: IWATA,Y.

    Args:
        fig     : plotly.graph_objects.Figure（Noneの場合は何もしない）
        outdir  : 保存先ディレクトリ（mode="offline" 時のみ使用）
        title   : ファイル名／表示タイトルのプレフィックス
        suffix  : サフィックス（"_Scatter" 等）
        mode    : "offline" : HTMLファイルとして outdir へ保存する
                  "online"  : ファイルに保存せず、メモリ上の Figure をそのまま
                              fig.show() で対話的に即時表示する
    """
    if fig is None:
        return

    if mode == "online":
        _show_live(fig, title, suffix)
    else:
        _save_offline(fig, outdir, title, suffix)


def _show_live(fig, title: str, suffix: str) -> None:
    """
    Figure をファイルに保存せず、メモリ上のデータのまま対話的に表示する（fig.show()）。
    AnalyzeFunctionsVZ_UI.py の __main__ ブロックで使われている fig.show() と同じもの。
    実行環境のデフォルトレンダラー（ブラウザ・Jupyter等）に依存するため、GUIの無い
    サーバー環境等で呼ぶと失敗する場合がある。失敗時は警告ログを出すのみで、
    ファイル保存へのフォールバックは行わない（"ファイルを作らず今すぐ見る"のが
    このmodeの目的のため、フォールバックしてファイルを作ると意図に反するため）。
    Mod: 2026.08.09    By: IWATA,Y.
    """
    try:
        fig.show()
        utl.log(f"Shown (live, in-memory): {title}{suffix}", MODULE_NAME)
    except Exception as e:
        utl.log(f"WARNING  '{title}{suffix}' の対話的表示に失敗しました"
                 f"（GUI/ブラウザ等のレンダラーが無い環境の可能性があります）: {e}", MODULE_NAME)
        print(traceback.format_exc(), flush=True)


def _save_offline(fig, outdir: str, title: str, suffix: str) -> None:
    """
    Figure をローカルHTMLファイルとして保存する。
    Mod: 2026.08.09    By: IWATA,Y.
    """
    try:
        import plotly
        filepath = os.path.join(outdir, f"{title}{suffix}.html")
        plotly.offline.plot(fig, auto_open=False, filename=filepath)
        utl.log(f"Saved: {filepath}", MODULE_NAME)
    except Exception as e:
        utl.log(f"WARNING  '{title}{suffix}' の保存中にエラーが発生したためスキップします: {e}", MODULE_NAME)
        print(traceback.format_exc(), flush=True)


def _prepare_wave_columns(df) -> Tuple[Any, bool]:
    """
    TriEdge標準のセグメント列名（seg_id, seg_ind）を、AnalyzeFunctionsVZ_UI.py の
    gen_interact_layered_wave が内部でハードコードして参照している列名
    （WVID=波形ID, WVPT=波形内位置）に変換する。
    実際に AnalyzeFunctionsVZ_UI.py 内で参照されているのは WVID/WVPT の2つのみ
    （ログ表示にだけ出てくる COUT という3つ目の名前は、コード中では未使用のため対応不要）。

    既に WVID 列がある場合は、呼び出し側が独自にWVID/WVPT付きのdfを用意済みとみなし、
    何もしない（元の呼び出し方との後方互換のため）。
    Mod: 2026.08.09    By: IWATA,Y.

    Returns:
        df       : WVID/WVPT追加後のコピー（元のdfオブジェクトは変更しない）
        is_wave  : 波形重ね合わせ（gen_interact_layered_wave）が実行できる状態か
    """
    if "WVID" in df.columns:
        return df, True
    if "seg_id" in df.columns and "seg_ind" in df.columns:
        df = df.copy()
        df["WVID"] = df["seg_id"]
        df["WVPT"] = df["seg_ind"]
        return df, True
    return df, False


########################################
# Public Entry Point（tri_s20_eda_runner.py から呼ばれる）
########################################
def main(TITLE: str, df, col_def: Dict[str, List[Any]], config: Dict[str, Any]) -> None:####, mode: str = "offline"
    """
    対話的（Plotly）可視化レポートを生成する。
    Mod: 2026.08.09    By: IWATA,Y.

    Args:
        OUTPUT  : 出力先ディレクトリ（mode="offline" 時のみ使用。末尾スラッシュの有無どちらでも可）
        TITLE   : レポートタイトル（ファイル名／表示タイトルのプレフィックスに使われる）
        df      : pandas.DataFrame（load ステップの戻り値）
        col_def : 列定義辞書（load ステップの戻り値。フォーマットは本モジュールdocstring参照）
        mode    : "offline"（デフォルト）: HTMLファイルとして OUTPUT フォルダへ保存する
                  "online"               : ファイルに保存せず、メモリ上の Figure をそのまま
                                            fig.show() で対話的に即時表示する
                  tri_s20_eda_runner.py 経由の呼び出しでは常にデフォルト値（"offline"）が
                  使われる（呼び出しシグネチャが _func(OUTPUT, TITLE, df, col_def) で固定のため）。
                  online/offline を明示的に切り替えたい場合は、本関数を直接呼び出すこと。
    """
    from eda import tri_s29_eda_vz_ui as AFVU  # 対話的可視化本体（Figure生成）
    
    ####is_wave = config.get("IS_WAVE", False)
    graph_to_viz = config.get("graph_to_viz", ["dist"])####

    mode = config.get("INTERACT_MODE", "online")
    if mode not in ("offline", "online"):
        raise ValueError(f"mode must be 'offline' or 'online'. Got: {mode!r}")

    OUTPUT = config.get("OUTDIR", "./output/")
    outdir = os.path.join(OUTPUT, "") if OUTPUT else "./output/"
    os.makedirs(outdir, exist_ok=True)

    utl.log(f"Interactive Visualize Start.  mode={mode}", MODULE_NAME)

    featurename, targetname, IsQualittv, IsQuanttv = _parse_col_def(col_def)
    utl.log(
        f"featurename={featurename}  targetname={targetname}  "
        f"IsQualittv={IsQualittv}  IsQuanttv={IsQuanttv}",
        MODULE_NAME
    )

    if targetname is None:
        utl.log(f"WARNING  col_def に var_type='objective' の列が見つかりません。"
                 f"gen_interact_* 側の仕様上、目的変数無しでは正しく動作しない可能性があります。",
                 MODULE_NAME)

    # 波形重ね合わせ（gen_interact_layered_wave）向けに、TriEdge標準のseg_id/seg_indを
    # WVID/WVPTへ変換する（既にWVID列がある場合はそのまま。無ければdfをコピーして追加する）
    df, _ = _prepare_wave_columns(df)####

    cfg = {"title": TITLE}

    # 目的変数未設定時、_safe() のエラーメッセージに添えるヒント（原因が分かりやすくなるように）
    _no_tar_hint = "（目的変数(objective)が未設定なため、この処理は実行できなかった可能性があります）" \
        if (targetname is None) else ""

    # ---- 箱ひげ→分布 ----
    if "dist" in graph_to_viz:####if not is_wave:
        fig_dist = _safe("gen_interact_dist", AFVU.gen_interact_dist, df, IsQualittv, IsQuanttv, targetname, cfg,
                        hint_=_no_tar_hint)
        _save_fig(fig_dist, outdir, TITLE, "_Dist", mode)

    # ---- 折れ線（時系列／値順の傾向） ----
    if "line" in graph_to_viz:####if not is_wave:
        fig_line = _safe("gen_interact_linechart", AFVU.gen_interact_linechart, df, IsQualittv, IsQuanttv, targetname, cfg,
                        hint_=_no_tar_hint)
        _save_fig(fig_line, outdir, TITLE, "_Line", mode)

    # ---- 散布図（変数間の関係性） ----
    if "scat" in graph_to_viz:####if not is_wave:
        fig_scat = _safe("gen_interact_scatter", AFVU.gen_interact_scatter, df, IsQualittv, IsQuanttv, targetname, cfg,
                        hint_=_no_tar_hint)
        _save_fig(fig_scat, outdir, TITLE, "_Scatter", mode)

    # ---- 波形重ね合わせ（WVID列がある場合のみ） ----
    if "overlap" in graph_to_viz:####if is_wave:
        cfg_layer = {**cfg, "ql_to_v_cl": IsQualittv[:2]}  # AnalyzeFunctionsVZ_UI.py の __main__ 例に合わせる
        fig_layer = _safe("gen_interact_layered_wave", AFVU.gen_interact_layered_wave,
                           df, IsQualittv, IsQuanttv, targetname, cfg_layer, hint_=_no_tar_hint)
        _save_fig(fig_layer, outdir, TITLE, "_Layered", mode)

    utl.log(f"Interactive Visualize End.  outdir={outdir}", MODULE_NAME)
