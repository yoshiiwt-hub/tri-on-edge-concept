# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# EDA Visualizer
# Ver: 01.b.20260930    By: IWATA, Y.'s Agent AI
# Mod: 新規作成
########################################
tri_s20_eda_runner.py の visualise() ステップから呼び出される可視化モジュール。
AnalyzeFunctionsVZ_DT.py（既存の可視化ライブラリ）の gen_viz_report() をラップし、
col_def（tri_s20_eda_runner.py の load ステップが返す列定義辞書）から
gen_viz_report() が必要とするパラメータ（featurename, targetname, target_is,
IsQualittv, IsQuanttv）を自動的に組み立てる。

動作概要
    1. col_def を走査し、featurename（explanatory列）・targetname（objective列）・
       target_is（目的変数が数値/カテゴリのどちらか）・IsQualittv（category列）・
       IsQuanttv（numetory列）を組み立てる
    2. AnalyzeFunctionsVZ_DT.gen_viz_report() を呼び出す
       - グラフ画像（箱ひげ図・ヒストグラム・散布図・折れ線・ヒートマップ等）を
         OUTPUT フォルダへ PNG として保存
       - 画像を埋め込んだ Excel レポート（{TITLE}_img.xlsx）を同フォルダへ保存
       （画像保存・Excel生成とも gen_viz_report() 内部の処理そのままで、本モジュールは
        呼び出しパラメータの組み立てのみを担当する）

col_def フォーマット（tri_s20_eda_runner.py の load ステップと共通）
    {"列インデックス(str)": [name, dtype, role, var_type, comment], ...}
      role    : "key" | "numetory" | "category"
      var_type: "key" | "explanatory" | "objective"

前提
    AnalyzeFunctionsVZ_DT.py と、それが依存する AnalyzeFunctionsPP.py が
    Pythonパスの通った場所に存在していること（gen_viz_report() 内部で
    'AnalyzeFunctionsPP.CALC_STATS' 等が使われるため）。
"""
MODULE_NAME = "Eda_Visualizer"

"""standard library"""
import os
import sys
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
# col_def -> gen_viz_report() 引数への変換
########################################
def _parse_col_def(
    col_def: Dict[str, List[Any]]
) -> Tuple[List[str], Optional[str], str, List[str], List[str]]:
    """
    col_def を走査し、gen_viz_report() が必要とするパラメータ一式を組み立てる。
    Mod: 2026.08.09    By: IWATA,Y.

    Returns:
        featurename : explanatory列の列名リスト
        targetname  : objective列の列名（1つのみ想定。複数ある場合は最初の1つを使う）
        target_is   : targetname の role に応じて "qn"（numetory）/ "ql"（category）/ ""（不明）
        IsQualittv  : role="category" の列名リスト
        IsQuanttv   : role="numetory" の列名リスト
    """
    featurename: List[str] = []
    targetname:  Optional[str] = None
    IsQualittv:  List[str] = []
    IsQuanttv:   List[str] = []

    # col_defのキーは "0","1",... の文字列想定だが、念のため数値順にソートして走査する
    for k in sorted(col_def.keys(), key=lambda s: int(s) if str(s).isdigit() else s):
        entry = col_def[k]
        name, dtype, role, var_type = entry[0], entry[1], entry[2], entry[3]

        if role == "category":
            IsQualittv.append(name)
        elif role == "numetory":
            IsQuanttv.append(name)
        # role == "key" は可視化対象外（散布図・箱ひげ図等の軸には使わない）

        if var_type == "explanatory":
            featurename.append(name)
        elif var_type == "objective":
            if targetname is None:  # 目的変数が複数定義されていても、最初の1つのみを使う
                targetname = name

    if targetname in IsQualittv:
        target_is = "ql"
    elif targetname in IsQuanttv:
        target_is = "qn"
    else:
        target_is = ""  # 目的変数が col_def に見当たらない、または role="key" だった場合

    return featurename, targetname, target_is, IsQualittv, IsQuanttv


########################################
# Public Entry Point（tri_s20_eda_runner.py から呼ばれる）
########################################
def main(TITLE: str, df, col_def: Dict[str, List[Any]], config: Dict[str, Any]) -> None:
    """
    可視化レポートを生成する（グラフ画像フォルダ保存 ＋ Excelレポート出力）。
    Mod: 2026.08.09    By: IWATA,Y.

    Args:
        OUTPUT  : 出力先ディレクトリ（末尾スラッシュの有無どちらでも可）
        TITLE   : レポートタイトル（画像ファイル名・Excelファイル名のプレフィックスに使われる）
        df      : pandas.DataFrame（load ステップの戻り値）
        col_def : 列定義辞書（load ステップの戻り値。フォーマットは本モジュールdocstring参照）
    """
    from eda import tri_s29_eda_vz_dt as AFVD  # 可視化本体（画像保存・Excelレポート生成）

    utl.log(f"Visualize Start.", MODULE_NAME)
    OUTPUT = config.get("OUTDIR", "./output/")

    featurename, targetname, target_is, IsQualittv, IsQuanttv = _parse_col_def(col_def)
    utl.log(
        f"featurename={featurename}  targetname={targetname}({target_is})  "
        f"IsQualittv={IsQualittv}  IsQuanttv={IsQuanttv}",
        MODULE_NAME
    )

    if targetname is None:
        utl.log(f"WARNING  col_def に var_type='objective' の列が見つかりません。"
                 f"目的変数なし（相関・分布の可視化のみ）として続行します。", MODULE_NAME)

    # 出力先ディレクトリ（末尾スラッシュを保証。gen_viz_report() 内部は文字列結合でパスを組むため）
    outdir = os.path.join(OUTPUT, "") if OUTPUT else "./output/"
    os.makedirs(outdir, exist_ok=True)

    # モバイル（Pydroid3等）かどうかで画像サイズを切り替え（AnalyzeFunctionsVZ_DT.py の
    # __main__ ブロックと同じ判定方式に合わせている）
    IS_MOBILE = bool(str(Path(__file__).resolve())[:9] in ["/storage/"])
    figsize = [4.2, 6.5, 300, 0.5, 10] if IS_MOBILE else [3000 / 300, 2000 / 300, 300, 1.0, 10]

    cfg = dict(
        figsize=figsize,
        title=TITLE,
        outdir=outdir,
        FC="darkslategray",
        EC="snow",
    )

    # 目的変数側の数値列に欠損があると後続の可視化処理で崩れるため、
    # AnalyzeFunctionsVZ_DT.py の __main__ ブロックと同じく事前に除外しておく
    df_viz = df.dropna(subset=IsQuanttv) if IsQuanttv else df

    AFVD.gen_viz_report(featurename, targetname, target_is, df_viz, IsQualittv, IsQuanttv, cfg)

    utl.log(f"Visualize End.  outdir={outdir}", MODULE_NAME)
