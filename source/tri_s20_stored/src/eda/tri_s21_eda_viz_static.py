# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# EDA Visualizer
# Ver: 01.d.20261108    By: IWATA, Y.'s Agent AI
# Mod: pyflakes の指摘を整理（使っていない import・変数、置き換えの無い f-string を削除。動作は変えない）
########################################
データの分布・関係・相関のグラフを画像で保存し、Excel のレポートにまとめる

主要関数:
main            # グラフの画像と Excel のレポートを作る（tri_s20_eda_runner の visualise から呼ばれる）。
_parse_col_def  # col_def から、説明変数・目的変数・質的変数・量的変数の列名を組み立てる。

位置づけ: グラフの生成とレポートの作成そのものは tri_s29_eda_vz_dt（旧 AnalyzeFunctionsVZ_DT）が受け持ち、このモジュールは col_def から引数を組み立てて呼び出す。対話的に確かめる場合は tri_s21_eda_viz_interact を使う。
設計意図: col_def の role / var_type から引数を自動で組み立て、データセットごとにグラフの設定を書かなくて済むようにする。画像を Excel に埋め込み、ファイル1つで共有できるようにする。
前提・制約: col_def の role は key / numetory / category、var_type は key / explanatory / objective。量的変数に欠損がある行は除いて描く。
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
        name, role, var_type = entry[0], entry[2], entry[3]

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

    utl.log("Visualize Start.", MODULE_NAME)
    OUTPUT = config.get("OUTDIR", "./output/")

    featurename, targetname, target_is, IsQualittv, IsQuanttv = _parse_col_def(col_def)
    utl.log(
        f"featurename={featurename}  targetname={targetname}({target_is})  "
        f"IsQualittv={IsQualittv}  IsQuanttv={IsQuanttv}",
        MODULE_NAME
    )

    if targetname is None:
        utl.log("WARNING  col_def に var_type='objective' の列が見つかりません。"
                 "目的変数なし（相関・分布の可視化のみ）として続行します。", MODULE_NAME)

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
