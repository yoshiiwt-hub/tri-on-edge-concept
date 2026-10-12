# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# MT-method (Mahalanobis-Taguchi) Unit Space Learning
# Ver: 03.b.20260930    By: IWATA, Y.'s Agent AI
# Mod: tri_s20_learn_us_runner.py から呼び出される形に変更。設定ファイル
#      （tri_edge_21_learn_mt.json）の読込みを廃止し、main(TITLE, df, col_def, config)
#      として呼ばれる形式に統一（tri_s20_eda_runner.py の visualise() が
#      呼び出す各モジュールの main() と同じ引数構成）。データ読込み（統計量化を含む）も
#      呼び出し元（runner）の load() に一本化されたため、本モジュール内では行わない
#      （旧来の input_mode="segment"/"stats" の分岐は廃止。df は既に
#      1セグメント=1行の統計量テーブルである前提）。
########################################
学習データ（1セグメント=1行の統計量テーブル）から単位空間（平均・共分散逆行列・分散）を生成する

動作概要
    1. main(TITLE, df, col_def, config) が呼び出される
       （df: 統計量テーブル。1セグメント=1行。col_def: 対応する列定義）
    2. mt_target_col_names で指定した列を各セグメントごとに並べ、学習行列を作る
    3. 平均ベクトル・共分散行列・共分散行列の疑似逆行列・分散を計算
    4. 単位空間を {title}_unit_space.json に保存する

    ※ 本モジュールは stream_core / TriEdge本体とは独立しており、
       tri_s20_learn_us_runner.py から呼び出して使う（単体実行は不可）

config（呼び出し元 runner の LEARN_CONFIG で指定）
    mt_target_col_names : 単位空間の変数として使う列名（df に含まれる列名。
                          例: ["Sin_mean", "Sin_std", "Sin_med"]）（必須）
    title  : 出力ファイル名の接頭辞（デフォルト: TITLE）
    outdir : 出力先ディレクトリ（デフォルト: "./output/"）
"""
MODULE_NAME = "LEARN_MT"

"""standard library"""
from   datetime import datetime as dt
import os
import sys
import traceback
from   pathlib import Path
from   typing   import Any, Dict, List

"""external library（学習専用モジュールのため使用可）"""
import numpy as np
import pandas as pd

"""local module"""
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    sys_dir = Path(sys._MEIPASS)
else:
    sys_dir = Path(__file__).resolve().parent.parent
if str(sys_dir) not in sys.path: sys.path.insert(0, str(sys_dir))
import tri_s29_util as utl


########################################
# 単位空間 生成
########################################
def mahalanobis_unitspace(matrix: List[List[float]], variables: List[str]) -> Dict[str, Any]:
    """
    学習データ行列（行=セグメント、列=変数）から単位空間を計算する
    Mod: 2026.07.16    By: IWATA,Y.（_gb_mt_learn_diag.py の mahalanobis_unitspace を移植）
    """
    arr = np.array(matrix, dtype=float)

    mean = np.mean(arr, axis=0)     # 平均
    cov  = np.cov(arr.T)            # 共分散
    rcov = np.linalg.pinv(np.atleast_2d(cov) if cov.ndim == 0 else cov)  # 共分散行列の疑似逆行列
    var  = np.diag(np.atleast_2d(cov) if cov.ndim == 0 else cov)         # 各変数の分散（対角成分）

    return {
        "variables": variables,
        "mean"     : mean.tolist(),
        "inv_cov"  : rcov.tolist(),
        "var"      : var.tolist(),
    }


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
        mt_target_col_names = config.get("mt_target_col_names")
        if not mt_target_col_names:
            raise ValueError("mt_target_col_names is required in config")

        title  = config.get("title", TITLE)
        outdir = config.get("outdir", "./output/")

        missing = [c for c in mt_target_col_names if c not in df.columns]
        if missing:
            raise ValueError(
                f"mt_target_col_names に存在しない列があります: {missing}（df列={list(df.columns)}）")

        matrix: List[List[float]] = df[mt_target_col_names].values.tolist()

        if len(matrix) < 2:
            raise ValueError(f"学習データが不足しています（有効件数={len(matrix)}）。少なくとも2件以上必要です。")
        if len(matrix) <= len(mt_target_col_names):
            utl.log(
                f"WARNING  学習データ件数（{len(matrix)}）が変数数（{len(mt_target_col_names)}）以下です。"
                f"共分散行列が正則にならない可能性があります（pinvで疑似逆行列を計算するため処理自体は継続します）。",
                MODULE_NAME)

        unit_space = mahalanobis_unitspace(matrix, mt_target_col_names)

        os.makedirs(outdir, exist_ok=True)
        save_path = os.path.join(outdir, f"{title}_unit_space_{dt.now().strftime('%Y%m%d%H%M%S')}.json")
        utl.save_json(unit_space, save_path, enc_="utf-8")####utf-8-sig
        utl.log(f"単位空間を保存: {save_path}  変数数={len(mt_target_col_names)}", MODULE_NAME)

    except Exception as e:
        utl.log(f"ERROR  {e}\n{traceback.format_exc()}", MODULE_NAME)
        raise

    utl.log("学習完了", MODULE_NAME)
