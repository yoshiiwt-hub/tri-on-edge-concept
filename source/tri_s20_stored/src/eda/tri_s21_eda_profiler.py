# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Stats Calculation & Excel Export
# Ver: 01.b.20260930    By: IWATA, Y.'s Agent AI
# Mod: 新規作成
########################################
"""
MODULE_NAME = "Profiler"

"""standard library"""
import itertools as itr
import os
from   pathlib  import Path
import sys
from   typing import Any, Dict, List, Tuple

"""external library"""
import numpy  as np
import pandas as pd
pd.options.display.max_rows = None
pd.options.display.max_columns = None
pd.options.display.show_dimensions = True
pd.options.display.precision = 2
pd.options.display.float_format = '{:.2f}'.format
import scipy.stats as st

"""local module"""
if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
    sys_dir = Path(sys._MEIPASS)
else:
    sys_dir = Path(__file__).resolve().parent.parent
if str(sys_dir) not in sys.path: sys.path.insert(0, str(sys_dir))
import tri_s29_util as utl


########################################
# Functions
########################################
def _split_qualtv_quanttv(col_def: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """
    col_def の num/cat（3番目の要素）から質的変数(ISQUALTV)・量的変数(ISQUANTV)を判定する。
    "category" -> 質的変数 / "numetory" -> 量的変数 / "key" -> 対象外（識別子等のため統計対象にしない）
    """
    isqualtv: List[str] = []
    isquanttv: List[str] = []
    for idx in sorted(col_def.keys(), key=lambda k: int(k)):
        name, dtype, num_cat = col_def[idx][0], col_def[idx][1], col_def[idx][2]
        if num_cat == "category":
            isqualtv.append(name)
        elif num_cat == "numetory":
            isquanttv.append(name)
        # "key" は対象外（識別子・タイムスタンプ等）
    return isqualtv, isquanttv


def ORG__check_df(df: pd.DataFrame, col_def: Dict[str, Any]) -> pd.DataFrame:
    """
    列ごとの概要（型・値の範囲・ユニーク値件数）を算出する
    Mod: 2026.07.19    By: IWATA,Y.（AnalyzeFunctionsDT.py の CHK_DF を移植）
    """
    rows = []
    col_names_ordered = [col_def[str(i)][0] for i in range(len(col_def))]

    for name in col_names_ordered:
        if name not in df.columns:
            continue
        srs = df[name]
        n_unique = int(srs.nunique(dropna=True))
        unq_sorted = srs.dropna().sort_values().unique()

        if pd.api.types.is_numeric_dtype(srs):
            vmin, vmax = srs.min(), srs.max()
        else:
            vmin = unq_sorted[0] if len(unq_sorted) else None
            vmax = unq_sorted[-1] if len(unq_sorted) else None

        rows.append({
            "col_name": name,
            "dtype": str(srs.dtype),
            "count": int(srs.count()),
            "miss_num": int(srs.isnull().sum()),
            "n_unique": n_unique,
            "min": vmin,
            "max": vmax,
        })

    return pd.DataFrame(rows)


def count_unique_qualtv(df: pd.DataFrame, col_def: Dict[str, Any]) -> pd.DataFrame:
    """
    質的変数（"category"）のみを対象に、値ごとの出現回数を算出する
    Mod: 2026.07.19    By: IWATA,Y.（AnalyzeFunctionsDT.py の CNT_UNQ を移植、対象を質的変数のみに限定）
    Returns:
        列: col_name, value, count（valueは"..."で囲んだ文字列表記）
    """
    isqualtv, _ = _split_qualtv_quanttv(col_def)

    rows = []
    for c in isqualtv:
        if c not in df.columns:
            continue
        vc = df[c].value_counts(dropna=True).sort_index()
        for val, cnt in vc.items():
            rows.append({"col_name": c, "value": f'"{val}"', "count": int(cnt)})

    return pd.DataFrame(rows)


def calc_pca_eigenvector(df: pd.DataFrame, isquanttv: List[str], n_components: int = 8) -> pd.DataFrame:
    """
    量的変数を対象にPCA（主成分分析）を行い、固有ベクトルを算出する
    Mod: 2026.07.19    By: IWATA,Y.（AnalyzeFunctionsUS.py の PrimaryComponentAnalysis を移植。
         sklearnを直接使用し、AnalyzeFunctionsUS.py本体には依存しない自己完結版）
    Returns:
        行=元の変数名、列=主成分（p1_寄与率千分率_累積寄与率千分率、のような列名） のDataFrame
        （量的変数が2列未満、または欠損等で計算不可の場合は空のDataFrameを返す）
    """
    if len(isquanttv) < 2:
        return pd.DataFrame()

    from sklearn.decomposition import PCA
    from sklearn.preprocessing import StandardScaler

    df_num = df[isquanttv].dropna()
    if len(df_num) < 2:
        return pd.DataFrame()

    n_comp = min(20, len(isquanttv), n_components, len(df_num))

    scaler = StandardScaler()
    arr = scaler.fit_transform(df_num.values)

    pca = PCA(n_components=n_comp, random_state=23)
    pca.fit(arr)

    evr = pca.explained_variance_ratio_
    evr_acc = np.hstack([0, evr.cumsum()])[1:]
    pcomp_names = [
        f"p{i+1}_{evr[i]*1000:03.0f}_{evr_acc[i]*1000:03.0f}" for i in range(n_comp)
    ]

    df_vec = pd.DataFrame(pca.components_, columns=isquanttv, index=pcomp_names)
    return df_vec.T


def calc_stats(df: pd.DataFrame, col_def: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """
    DataFrameの質的/量的統計量・相関を算出する
    Mod: 2026.07.19    By: IWATA,Y.（AnalyzeFunctionsPP.py の CALC_STATS を移植）
    Returns:
        {"stat_ql": ..., "stat_qn": ..., "corr_spearman": ..., "corr_pearson": ...,
         "cramer": ..., "corr_ratio": ..., "data_sample": ...}
    """
    isqualtv, isquanttv = _split_qualtv_quanttv(col_def)

    stats_qualtv = ["count", "miss_num", "unq_num", "unique_val", "freq", "freq_num", "freq_rate"]
    stats_quanttv = [
        "count", "miss_num",
        "mean", "std", "sig3", "sig3u", "sig3l", "sig3_n", "sig3u_n", "sig3l_n",
        "min", "max", "range",
        "pct01", "pct05", "pct10", "pct25", "median", "pct75", "pct90", "pct95", "pct99",
        "iqr", "iqrl", "iqru", "iqr_n", "iqru_n", "iqrl_n",
        "skewness", "kurtosis",
    ]

    # ---- 質的変数の統計量 ----
    if isqualtv:
        df_ql = pd.DataFrame(index=isqualtv, columns=stats_qualtv)
        for idx in isqualtv:
            srs = df.loc[:, idx]
            n = len(srs)
            if n == 0:
                continue
            unq = srs.dropna().sort_values().unique()

            df_ql.loc[idx, "count"] = n
            df_ql.loc[idx, "miss_num"] = int(srs.isnull().sum())
            df_ql.loc[idx, "unq_num"] = len(unq)
            df_ql.loc[idx, "unique_val"] = ", ".join(str(s) for s in unq)

            if srs.dropna().empty:
                continue
            freq_val = srs.mode().iloc[0]
            freq_n = int((srs == freq_val).sum())
            df_ql.loc[idx, "freq"] = freq_val
            df_ql.loc[idx, "freq_num"] = freq_n
            df_ql.loc[idx, "freq_rate"] = freq_n / n

        df_ql[["count", "miss_num", "unq_num", "freq_num"]] = (
            df_ql[["count", "miss_num", "unq_num", "freq_num"]]
            .fillna(0).infer_objects(copy=False).astype("int")
        )
        df_ql["freq_rate"] = df_ql["freq_rate"].astype("float")
    else:
        df_ql = pd.DataFrame()

    # ---- 量的変数の統計量 ----
    if isquanttv:
        df_qn = pd.DataFrame(index=isquanttv, columns=stats_quanttv)
        for idx in isquanttv:
            srs = df.loc[:, idx]
            n = len(srs)
            if n == 0:
                continue

            df_qn.loc[idx, "count"] = n
            df_qn.loc[idx, "miss_num"] = int(srs.isnull().sum())

            avg, std = srs.mean(), srs.std()
            df_qn.loc[idx, "mean"] = avg
            df_qn.loc[idx, "std"] = std
            df_qn.loc[idx, "sig3"] = std * 3
            df_qn.loc[idx, "sig3u"] = avg + std * 3
            df_qn.loc[idx, "sig3l"] = avg - std * 3

            sig3u_n = int((srs > avg + std * 3).sum())
            sig3l_n = int((srs < avg - std * 3).sum())
            df_qn.loc[idx, "sig3_n"] = n - sig3u_n - sig3l_n
            df_qn.loc[idx, "sig3u_n"] = sig3u_n
            df_qn.loc[idx, "sig3l_n"] = sig3l_n

            mn, mx = srs.min(), srs.max()
            df_qn.loc[idx, "min"] = mn
            df_qn.loc[idx, "max"] = mx
            df_qn.loc[idx, "range"] = abs(mx - mn)

            med = srs.median()
            pct25, pct75 = np.percentile(srs.dropna(), 25), np.percentile(srs.dropna(), 75)
            iqr = abs(pct75 - pct25)
            iqrl, iqru = pct25 - iqr * 1.5, pct75 + iqr * 1.5
            for p, key in [(1, "pct01"), (5, "pct05"), (10, "pct10")]:
                df_qn.loc[idx, key] = np.percentile(srs.dropna(), p)
            df_qn.loc[idx, "pct25"] = pct25
            df_qn.loc[idx, "median"] = med
            df_qn.loc[idx, "pct75"] = pct75
            for p, key in [(90, "pct90"), (95, "pct95"), (99, "pct99")]:
                df_qn.loc[idx, key] = np.percentile(srs.dropna(), p)
            df_qn.loc[idx, "iqr"] = iqr
            df_qn.loc[idx, "iqrl"] = iqrl
            df_qn.loc[idx, "iqru"] = iqru

            iqru_n = int((srs > med + iqr).sum())
            iqrl_n = int((srs < med - iqr).sum())
            df_qn.loc[idx, "iqr_n"] = n - iqru_n - iqrl_n
            df_qn.loc[idx, "iqru_n"] = iqru_n
            df_qn.loc[idx, "iqrl_n"] = iqrl_n

            df_qn.loc[idx, "skewness"] = srs.skew()
            df_qn.loc[idx, "kurtosis"] = srs.kurt()

        int_cols = ["count", "miss_num", "sig3_n", "sig3u_n", "sig3l_n", "iqr_n", "iqru_n", "iqrl_n"]
        df_qn[int_cols] = df_qn[int_cols].fillna(0).infer_objects(copy=False).astype("int")
        float_cols = [c for c in stats_quanttv if c not in int_cols]
        df_qn[float_cols] = df_qn[float_cols].astype("float")
    else:
        df_qn = pd.DataFrame()

    # ---- 相関係数（量的変数間） ----
    corr_spearman = df[isquanttv].corr("spearman") if len(isquanttv) >= 2 else pd.DataFrame()
    corr_pearson = df[isquanttv].corr("pearson") if len(isquanttv) >= 2 else pd.DataFrame()

    # ---- クラメールの連関係数（質的×質的） ----
    if len(isqualtv) >= 2:
        cramer = pd.DataFrame(columns=isqualtv, index=isqualtv, dtype=float)
        for ql1, ql2 in itr.product(isqualtv, isqualtv):
            table = pd.crosstab(df[ql1], df[ql2])
            if table.empty or min(table.shape) < 1:
                continue
            x2, _, _, _ = st.chi2_contingency(table, correction=False)
            n = table.sum().sum()
            denom = n * (min(table.shape) - 1)
            cramer.loc[ql1, ql2] = np.sqrt(x2 / denom) if denom > 0 else np.nan
    else:
        cramer = pd.DataFrame()

    # ---- 相関比（質的×量的） ----
    if isqualtv and isquanttv:
        corr_ratio = pd.DataFrame(columns=isqualtv, index=isquanttv, dtype=float)
        for ql in isqualtv:
            cat_srs = df[ql]
            cat_unq = list(pd.Series(cat_srs.unique()).dropna())
            for qn in isquanttv:
                num = df[qn]
                num_mean = num.dropna().mean()
                num_var_all = ((num - num_mean) ** 2).sum()
                if num_var_all == 0:
                    continue
                cat_var = [
                    len(num[cat_srs == c].dropna()) * (num[cat_srs == c].dropna().mean() - num_mean) ** 2
                    for c in cat_unq
                ]
                corr_ratio.loc[qn, ql] = sum(cat_var) / num_var_all
    else:
        corr_ratio = pd.DataFrame()

    data_sample = pd.concat([df.head(100), df.tail(100)]) if len(df) > 0 else pd.DataFrame()

    check_df_result = utl.check_df(df, col_def)
    unique_qualtv_result = count_unique_qualtv(df, col_def)
    pca_eigenvector_result = calc_pca_eigenvector(df, isquanttv)

    col_def_df = pd.DataFrame(
        [[idx] + list(col_def[idx]) for idx in sorted(col_def.keys(), key=lambda k: int(k))],
        columns=["index", "name", "dtype", "num_cat", "exp_obj", "comment"],
    )

    return {
        "check_df": check_df_result,
        "col_def": col_def_df,
        "stat_ql": df_ql,
        "stat_qn": df_qn,
        "unique_qualtv": unique_qualtv_result,
        "corr_spearman": corr_spearman,
        "corr_pearson": corr_pearson,
        "cramer": cramer,
        "corr_ratio": corr_ratio,
        "pca_eigenvector": pca_eigenvector_result,
        "data_sample": data_sample,
    }


def save_stats_excel(stats: Dict[str, pd.DataFrame], outdir: str, title: str) -> str:
    """calc_stats() の結果をExcelファイル（シート分け、相関系は色分け）に保存する"""
    from openpyxl.formatting.rule import ColorScaleRule
    from openpyxl.utils import get_column_letter

    os.makedirs(outdir, exist_ok=True)
    save_path = os.path.join(outdir, f"{title}_Stats.xlsx")

    sheet_map = [
        ("col_def", "ColumnDef"),
        ("check_df", "CheckDF"),
        ("stat_ql", "Stat_Ql"),
        ("stat_qn", "Stat_Qn"),
        ("unique_qualtv", "UniqueQualtv"),
        ("corr_spearman", "Corr_Spearman"),
        ("corr_pearson", "Corr_Pearson"),
        ("cramer", "CramerCA"),
        ("corr_ratio", "CorrRatio"),
        ("pca_eigenvector", "EigenVector"),
        ("data_sample", "DataSample"),
    ]

    # 相関系シート: -1〜1（Spearman/Pearson） or 0〜1（Cramer/CorrRatio）で色分けする
    color_scale_sheets = {
        "Corr_Spearman": "m1to1",
        "Corr_Pearson":  "m1to1",
        "CramerCA":      "0to1",
        "CorrRatio":     "0to1",
        "EigenVector":   "m1to1_green",
    }

    with pd.ExcelWriter(save_path, engine="openpyxl") as writer:
        for key, sheet_name in sheet_map:
            df_ = stats.get(key)
            if df_ is None or df_.empty:
                continue
            df_.to_excel(writer, sheet_name=sheet_name, index=True)

            # フィルターボタン（ヘッダ行に対して設定。データ範囲全体）
            ws = writer.sheets[sheet_name]
            ws.auto_filter.ref = ws.dimensions

        # ---- 相関系シートへの色分け（ColorScaleRule） ----
        for sheet_name, scale_kind in color_scale_sheets.items():
            if sheet_name not in writer.sheets:
                continue
            ws = writer.sheets[sheet_name]
            n_rows = ws.max_row
            n_cols = ws.max_column
            if n_rows < 2 or n_cols < 2:
                continue

            # データ範囲（1行目=ヘッダ、A列=インデックス列を除く）
            first_col_letter = get_column_letter(2)
            last_col_letter = get_column_letter(n_cols)
            cell_range = f"{first_col_letter}2:{last_col_letter}{n_rows}"

            if scale_kind == "m1to1":
                rule = ColorScaleRule(
                    start_type="num", start_value=-1, start_color="0000FF",
                    mid_type="num",   mid_value=0,    mid_color="FFFFFF",
                    end_type="num",   end_value=1,    end_color="FF0000",
                )
            elif scale_kind == "m1to1_green":
                # EigenVector: 0（白）を中心に、絶対値が大きい（+1/-1に近い）ほど緑を濃く
                # （寄与の大きい変数を、符号によらず目立たせるため両端を同じ色にする）
                rule = ColorScaleRule(
                    start_type="num", start_value=-1, start_color="00B050",
                    mid_type="num",   mid_value=0,    mid_color="FFFFFF",
                    end_type="num",   end_value=1,    end_color="00B050",
                )
            else:  # "0to1"
                rule = ColorScaleRule(
                    start_type="num", start_value=0, start_color="FFFFFF",
                    end_type="num",   end_value=1,   end_color="800080",
                )
            ws.conditional_formatting.add(cell_range, rule)

    return save_path


def main(OUTDIR, TITLE, df, col_def):
    stats = calc_stats(df, col_def)
    save_path = save_stats_excel(stats, OUTDIR, TITLE)
