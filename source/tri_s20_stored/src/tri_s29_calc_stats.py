# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Segment Stats Calculation Functions（20_stored 用ポータブル版）
# Ver: 01.a.20260716    By: IWATA,Y.
# Mod: src/tri_edge_19_calc_stats.py（Ver 01.a.20260615）を 20_stored/ へ複製。
#      src/ 本体（stream_core）から独立させるため、tri_edge_00_supervise_base への
#      依存を外し、簡易な print ベースのログ出力に置き換えた。
#      計算ロジック自体（compile_register_map / build_stats / 各 stat 関数）は無改造。
########################################
compile_register_map  # column_register_map と output_column を検証・コンパイルして計算関数リストを返す
build_stats           # コンパイル済みエントリリストに基づきセグメントの統計量 dict を構築して返す
_stat_mean            # 平均値を算出する
_stat_min             # 最小値を算出する
_stat_max             # 最大値を算出する
_stat_std             # 母標準偏差を算出する
_stat_count           # 有効値件数を返す
_stat_sum             # 合計値を算出する
_stat_range           # 範囲（max - min）を算出する
_stat_first           # 先頭値を返す
_stat_last            # 最終値を返す
_stat_med             # 中央値を算出する
_stat_iqr             # IQR（四分位範囲）を算出する

動作概要
    1. compile_register_map() で column_register_map と output_column を検証し計算クロージャリストを生成する
    2. make_stat_func() で stat_name（mean / min / max / std / count / sum / range / med / iqr / first / last / nth / pertl / sig_u / sig_l / mode / n_null / seg_len）に対応するクロージャを生成する
    3. スライス指定（[start_frac, end_frac]）により対象区間を絞り込んでから統計量を算出できる
    4. build_stats() はコンパイル済みリストを受け取りセグメント全行を走査して統計量 dict を返す
    5. warn 発生時は {out_key}_WARN キーに理由を記録し value が None の場合はそのキーを出力しない

    ※ build_stats() は seg[0][0]=seg_id, seg[0][2]=seg_ts_start, seg[-1][2]=seg_ts_end という
       行構造を前提とする（src/save_csv.py 等が保存する TriEdge のセグメント行構造と同じ）。
       20_stored 側の学習スクリプトでセグメントを組み立てる際は、この並びに合わせること。
"""
MODULE_NAME = "Calc_Seg_Stats"

"""standard library"""
from   datetime import datetime as dt
from   typing   import Any, Dict, List, Tuple, Sequence, Optional


def _log(msg: str) -> None:
    """簡易ログ出力（printのみ。20_stored はポータブルにするため外部モジュールに依存しない）"""
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{ts}  [{MODULE_NAME}]  {msg}")


########################################
# Calculate Stats Functions
########################################

def _stat_mean(vals: List[float]) -> float:
    """平均値"""
    return round(sum(vals) / len(vals), 4)

def _stat_min(vals: List[float]) -> float:
    """最小値"""
    return min(vals)

def _stat_max(vals: List[float]) -> float:
    """最大値"""
    return max(vals)

def _stat_std(vals: List[float]) -> float:
    """標準偏差（母標準偏差）"""
    _m = sum(vals) / len(vals)
    return round((sum((v - _m) ** 2 for v in vals) / len(vals)) ** 0.5, 4)

def _stat_count(vals: List[float]) -> int:
    """有効値件数"""
    return len(vals)

def _stat_sum(vals: List[float]) -> float:
    """合計"""
    return round(sum(vals), 4)

def _stat_range(vals: List[float]) -> float:
    """範囲（max - min）"""
    return round(max(vals) - min(vals), 4)

def _stat_first(vals: List[float]) -> float:
    """先頭値（float変換済み）"""
    return vals[0]

def _stat_last(vals: List[float]) -> float:
    """最終値（float変換済み）"""
    return vals[-1]

def _stat_med(vals: List[float]) -> float:
    """中央値（線形補間なし・ソート後の中央）"""
    sv = sorted(vals)
    n  = len(sv)
    mid = n // 2
    return round(sv[mid] if n % 2 else (sv[mid - 1] + sv[mid]) / 2, 4)

def _stat_iqr(vals: List[float]) -> float:
    """IQR（75%ile - 25%ile、線形補間）"""
    sv = sorted(vals)
    n  = len(sv)
    def _p(pct: float) -> float:
        idx = (pct / 100.0) * (n - 1)
        lo  = int(idx)
        hi  = lo + 1
        if hi >= n:
            return sv[-1]
        return sv[lo] + (idx - lo) * (sv[hi] - sv[lo])
    return round(_p(75) - _p(25), 4)


########################################
# Stats Function Map
########################################

"""統計量 関数マップ（数値列・パラメータなし）"""
stat_funcs: Dict[str, Any] = {
    "mean"  : _stat_mean,
    "min"   : _stat_min,
    "max"   : _stat_max,
    "std"   : _stat_std,
    "count" : _stat_count,
    "range" : _stat_range,
    "med"   : _stat_med,
    "iqr"   : _stat_iqr,
    "sum"   : _stat_sum,
    "first" : _stat_first,
    "last"  : _stat_last
}

"""パーセンタイル 有効値セット"""
_VALID_PERTL = {1, 5, 10, 25, 75, 90, 95, 99}


def compile_register_map(
    column_register_map: Dict,
    output_column:       Dict,
    c_keys:              List[str],
) -> List[Tuple[str, Any]]:
    """column_register_map と output_column を検証・コンパイルしてリストを返す
    [エントリ形式]
        column_register_map:
            "index": [src_col_idx, "stat_name"]
            "index": [src_col_idx, "stat_name", [start, end]]
            "index": [src_col_idx, "stat_name", param]
            "index": [src_col_idx, "stat_name", [start, end], param]

        output_column:
            "index": ["出力キー名", "dtype", "role", "var_type", "説明"]
            ※ index は column_register_map と一致させること
    [検証]
        - column_register_map / output_column のキーが一致しない → ValueError（起動停止）
        - src_col_idx 範囲外 → ValueError
        - 不正 stat_name / 不正 params → ValueError
    [返り値]
        [(out_key, func(seg, n_rows) -> (value, warn_or_None)), ...]
        index 昇順にソート済み
    """
    crm = {str(k): v for k, v in column_register_map.items()}
    oc  = {str(k): v for k, v in output_column.items()}

    # キー整合性チェック
    crm_keys = set(crm.keys())
    oc_keys  = set(oc.keys())
    if crm_keys != oc_keys:
        msgs = []
        missing_oc  = sorted(crm_keys - oc_keys)
        missing_crm = sorted(oc_keys - crm_keys)
        if missing_oc:  msgs.append(f"output_column に不足: {missing_oc}")
        if missing_crm: msgs.append(f"column_register_map に不足: {missing_crm}")
        raise ValueError("column_register_map / output_column キー不一致  " + "  ".join(msgs))

    compiled: List[Tuple[str, Any]] = []

    for idx in sorted(crm_keys, key=int):
        entry  = crm[idx]
        oc_def = oc[idx]

        # 出力キー名（output_column の第1要素）
        try:
            out_key = str(oc_def[0])
        except (IndexError, TypeError) as e:
            raise ValueError(f"[{idx}] output_column エントリ不正: {oc_def}  ({e})")

        # column_register_map エントリ パース
        try:
            src_col_idx = int(entry[0])
            stat_name   = str(entry[1])
            rest        = list(entry[2:])
        except (IndexError, ValueError, TypeError) as e:
            raise ValueError(f"[{idx}] column_register_map エントリ不正: {entry}  ({e})")

        # スライス解析（rest[0] がリスト/タプル → スライス指定）
        slice_spec  = None
        stat_params = rest
        if rest and isinstance(rest[0], (list, tuple)) and len(rest[0]) == 2:
            try:
                s_start = float(rest[0][0])
                s_end   = float(rest[0][1])
                if 0.0 <= s_start < s_end <= 1.0:
                    slice_spec  = (s_start, s_end)
                    stat_params = rest[1:]
            except (TypeError, ValueError):
                pass   # 不正スライス → 全行

        # src_col_idx 範囲チェック（seg_len 以外）
        if stat_name != "seg_len" and not (0 <= src_col_idx < len(c_keys)):
            raise ValueError(
                f"[{idx}] src_col_idx={src_col_idx} out of range "
                f"(valid: 0?{len(c_keys)-1})")

        # クロージャ生成（不正パラメータは ValueError で即時検出）
        try:
            func = make_stat_func(stat_name, src_col_idx, slice_spec, stat_params)
        except ValueError as e:
            raise ValueError(f"[{idx}] {e}")

        compiled.append((out_key, func))

    return compiled


########################################
# Stats Builder Functions
########################################

def extract_float_vals(seg: list, col_idx: int) -> List[float]:
    """指定列から float 変換可能な値のみ抽出して返す"""
    vals: List[float] = []
    for row in seg:
        try:
            v = float(row[col_idx])
            if v == v:   # NaN 除外
                vals.append(v)
        except (TypeError, ValueError):
            pass
    return vals


def calc_pertl(vals: List[float], p: float) -> float:
    """パーセンタイル算出"""
    sv = sorted(vals)
    n  = len(sv)
    if n == 1:
        return round(sv[0], 4)
    idx = (p / 100.0) * (n - 1)
    lo  = int(idx)
    hi  = lo + 1
    if hi >= n:
        return round(sv[-1], 4)
    return round(sv[lo] + (idx - lo) * (sv[hi] - sv[lo]), 4)


def get_eff_seg(seg: list, n_rows: int, slice_spec: Optional[Tuple]) -> list:
    """start_frac end_fracで指定された対象区間を返す"""
    if slice_spec is None:
        return seg
    s, e  = slice_spec
    row_s = int(n_rows * s)
    row_e = n_rows if e >= 1.0 else int(n_rows * e)
    return seg[row_s:row_e]


def make_stat_func(
    stat_name:   str,
    src_col_idx: int,
    slice_spec:  Optional[Tuple[float, float]],
    stat_params: list,
) -> Any:
    """stat計算ロジックを生成。LOOP INIT 時に呼び出す"""

    # seg_len : セグメントの全行数（スライス非適用）
    if stat_name == "seg_len":
        def _f(seg, n_rows):
            return n_rows, None
        return _f

    # nth : raw値（任意型）を対象区間内のn番目から取得
    if stat_name == "nth":
        try:
            n = int(stat_params[0])
        except (IndexError, ValueError, TypeError):
            raise ValueError("nth: stat_params[0]=n (int) required")
        if n == 0:
            raise ValueError("nth: n must not be 0")
        def _f(seg, n_rows, _ss=slice_spec, _src=src_col_idx, _n=n):
            es = get_eff_seg(seg, n_rows, _ss)
            ne = len(es)
            if ne == 0:
                return None, "slice_empty"
            ri = (_n - 1) if _n > 0 else _n
            if not (-ne <= ri < ne):
                return None, f"nth={_n} out of range (eff_rows={ne})"
            return es[ri][_src], None
        return _f

    # pertl : パーセンタイル（線形補間）
    if stat_name == "pertl":
        try:
            p = int(stat_params[0])
        except (IndexError, ValueError, TypeError):
            raise ValueError(
                f"pertl: stat_params[0]=p (int) required  valid={sorted(_VALID_PERTL)}")
        if p not in _VALID_PERTL:
            raise ValueError(f"pertl: p={p} invalid  valid={sorted(_VALID_PERTL)}")
        def _f(seg, n_rows, _ss=slice_spec, _src=src_col_idx, _p=p):
            es   = get_eff_seg(seg, n_rows, _ss)
            vals = extract_float_vals(es, _src)
            if not vals:
                return None, "no_valid_values"
            return calc_pertl(vals, _p), None
        return _f

    # sig_u / sig_l : mean ± n×std
    if stat_name in ("sig_u", "sig_l"):
        try:
            n_sigma = float(stat_params[0])
        except (IndexError, ValueError, TypeError):
            raise ValueError(f"{stat_name}: stat_params[0]=n (float) required")
        sign = 1.0 if stat_name == "sig_u" else -1.0
        def _f(seg, n_rows, _ss=slice_spec, _src=src_col_idx, _ns=n_sigma, _sg=sign):
            es   = get_eff_seg(seg, n_rows, _ss)
            vals = extract_float_vals(es, _src)
            if not vals:
                return None, "no_valid_values"
            _m   = sum(vals) / len(vals)
            _std = (sum((v - _m) ** 2 for v in vals) / len(vals)) ** 0.5
            return round(_m + _sg * _ns * _std, 4), None
        return _f

    # mode : 最頻値（数値・文字列どちらも可）
    if stat_name == "mode":
        def _f(seg, n_rows, _ss=slice_spec, _src=src_col_idx):
            es       = get_eff_seg(seg, n_rows, _ss)
            raw_vals = [row[_src] for row in es if row[_src] is not None]
            if not raw_vals:
                return None, "no_valid_values"
            freq: Dict[Any, int] = {}
            for v in raw_vals:
                freq[v] = freq.get(v, 0) + 1
            return max(freq, key=lambda k: freq[k]), None
        return _f

    # n_null : float変換不可の件数（対象区間内）
    if stat_name == "n_null":
        def _f(seg, n_rows, _ss=slice_spec, _src=src_col_idx):
            es = get_eff_seg(seg, n_rows, _ss)
            return len(es) - len(extract_float_vals(es, _src)), None
        return _f

    # stat_funcs : 数値統計（パラメータなし）
    sf = stat_funcs.get(stat_name)
    if sf is None:
        _avail = sorted(stat_funcs.keys()) + [
            "nth", "pertl", "sig_u", "sig_l", "mode", "n_null", "seg_len"]
        raise ValueError(f"unknown stat_name='{stat_name}'  available={_avail}")

    def _f(seg, n_rows, _ss=slice_spec, _src=src_col_idx, _sf=sf):
        es   = get_eff_seg(seg, n_rows, _ss)
        vals = extract_float_vals(es, _src)
        if not vals:
            return None, "no_valid_values"
        return _sf(vals), None
    return _f


def build_stats(
    seg:      list,
    compiled: List[Tuple[str, Any]],
) -> Dict[str, Any]:
    """
    コンパイル済みエントリリストに基づきサマリー dict を構築
    固定ヘッダ（seg_id, seg_ts_start, seg_ts_end, seg_rows）は常に付与。
    warn 発生時は {out_key}_WARN キーに理由を記録。
    value が None（out of range等）の場合はそのキーを出力しない。
    """
    n_rows = len(seg)
    result: Dict[str, Any] = {
        "seg_id"      : seg[0][0],
        "seg_ts_start": seg[0][2],
        "seg_ts_end"  : seg[-1][2],
        "seg_rows"    : n_rows,
    }
    for out_key, func in compiled:
        value, warn = func(seg, n_rows)
        if warn:
            result[f"{out_key}_WARN"] = warn
        elif value is not None:
            result[out_key] = value
    return result


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    _log("Run Standalone (self-test).")
