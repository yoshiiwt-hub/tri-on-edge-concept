# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Segment Stats Calculation Functions
# Ver: 01.c.20261005    By: IWATA, Y.'s Agent AI
# Mod: Act Trigger 共通の init_stats / calc_stats / export_out_coldef を追加（統計まわりの配線の集約。B-9）
########################################
init_stats            # [Act 共通] 設定の必須チェック・コンパイルを行い StatsContext を返す（起動時に1回）
calc_stats            # [Act 共通] build_stats で統計量を算出し、値が出せなかった項目（_WARN）を共通形式でログに出す
export_out_coldef     # [Act 共通] 出力列定義（out_coldef）を JSON ファイルに保存する
compile_register_map  # column_register_map と output_column を検証・コンパイルして計算関数リストを返す
build_stats           # コンパイル済みエントリリストに基づきセグメントの統計量 dict を構築して返す
_stat_mean            # 平均値を算出する
_stat_min             # 最小値を算出する
_stat_max             # 最大値を算出する
_stat_std             # 母標準偏差を算出する
_stat_count           # 有効値件数を返す
_stat_sum             # 合計値を算出する
_stat_range           # 範囲（max - min）を算出する
_stat_med             # 中央値を算出する
_stat_iqr             # IQR（四分位範囲）を算出する

動作概要
    1. compile_register_map() で column_register_map と output_column を検証し計算クロージャリストを生成する
    2. make_stat_func() で stat_name（mean / min / max / std / count / sum / range / med / iqr / first / last / nth / pertl / sig_u / sig_l / mode / n_null / seg_len / const）に対応するクロージャを生成する
       ※ first/last/nth/mode/const は数値専用のfloat変換を経由せず、行の生の値（任意型）をそのまま返す
    3. スライス指定（[start_frac, end_frac]）により対象区間を絞り込んでから統計量を算出できる
    4. build_stats() はコンパイル済みリストを受け取りセグメント全行を走査して統計量 dict を返す
    5. warn 発生時は {out_key}_WARN キーに理由を記録し value が None の場合はそのキーを出力しない
    6. Act Trigger は init_stats（起動時）→ calc_stats（セグメントごと）を使う。calc_stats は build_stats の結果を
       そのまま返し（_WARN キーも含む。出力内容は Act ごとに従来どおり）、_WARN があればログファイルに1行出す
"""
MODULE_NAME = "Calc_Seg_Stats"

"""standard library"""
from   datetime import datetime as dt
import json
import os
import sys
import traceback
from   typing   import Any, Dict, List, Tuple, Sequence, Optional

"""external library"""
## Not Using

"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()
    
except ModuleNotFoundError:
    try: # Check and Add Path
        cur_dir  = os.path.dirname(os.path.abspath(__file__))
        dir_name = os.path.basename(cur_dir)
        if dir_name != 'src':
            raise FileNotFoundError(f".py MUST be in 'src' dir. Current: {dir_name}")
        
        root_dir = os.path.dirname(cur_dir)
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
# Not Using


"""system dir, file"""
system_root = base.get_system_root()
log_dir  = base.get_log_dir()
log_file = base.get_log_path()


########################################
# Act Trigger Common Functions（B-9）
########################################

class StatsContext:
    """
    init_stats が返す、統計量算出の実行コンテキスト
        compiled_map   : compile_register_map の結果（build_stats に渡す）
        out_coldef     : 出力列定義（設定値そのまま）
        out_keys       : 出力キー名のリスト（out_coldef の index 昇順）
        base_col_names : 固定ヘッダ（seg_id / seg_ts_start / seg_ts_end / seg_rows）+ out_keys
        module_name    : ログ出力に使う呼び出し元のモジュール名
        trigger_name   : 呼び出し元のトリガー名
    Mod: 2026.10.05    By: IWATA, Y.'s Agent AI（B-9）
    """
    FIXED_HEADER: List[str] = ["seg_id", "seg_ts_start", "seg_ts_end", "seg_rows"]

    def __init__(self, compiled_map: List[Tuple[str, Any]], out_coldef: Dict, module_name: str, trigger_name: str):
        self.compiled_map   = compiled_map
        self.out_coldef     = out_coldef
        self.out_keys       = [str(out_coldef[idx][0]) for idx in sorted(out_coldef.keys(), key=int)]
        self.base_col_names = self.FIXED_HEADER + self.out_keys
        self.module_name    = module_name
        self.trigger_name   = trigger_name


def init_stats(config: Dict[str, Any], col_def: Dict[int, Any], module_name: str, trigger_name: str) -> StatsContext:
    """
    Act Trigger の起動時に1回呼ぶ。column_register_map / out_coldef の必須チェックとコンパイルを行う
    （不正な設定は ValueError。呼び出し側の startup / config として扱われる）
    Mod: 2026.10.05    By: IWATA, Y.'s Agent AI（各 Act に重複していた初期化を集約。B-9）
    """
    column_register_map = config.get("column_register_map")
    if not column_register_map:
        raise ValueError(f"column_register_map is required for {trigger_name}")
    out_coldef = config.get("out_coldef")
    if not out_coldef:
        raise ValueError(f"out_coldef is required for {trigger_name}")

    col_keys = [col_def[k][0] for k in sorted(col_def.keys())]
    compiled_map = compile_register_map(column_register_map, out_coldef, col_keys)
    ctx = StatsContext(compiled_map, out_coldef, module_name, trigger_name)
    base.output_log(module_name,
        f"{trigger_name} stats initialized. entries={len(compiled_map)}  out_keys={ctx.out_keys}", log_file)
    return ctx


def calc_stats(ctx: StatsContext, segment: list, seg_id: Any) -> Dict[str, Any]:
    """
    セグメントの統計量を算出する（build_stats の結果をそのまま返す。_WARN キーも含む）
    値が出せなかった項目（_WARN）がある場合は、項目と理由をログファイルに1行出す（全 Act 共通の形式）
    Mod: 2026.10.05    By: IWATA, Y.'s Agent AI（B-9）
    """
    stats_dict = build_stats(segment, ctx.compiled_map)
    warns = {k[:-len("_WARN")]: v for k, v in stats_dict.items() if k.endswith("_WARN")}
    if warns:
        base.output_log(ctx.module_name,
            f"WARNING  {ctx.trigger_name} Stats value not available: seg_id={seg_id}  {warns}", log_file)
    return stats_dict


def export_out_coldef(out_coldef: Dict, file_path: str, encode: str = "utf-8", module_name: str = MODULE_NAME) -> str:
    """
    出力列定義（out_coldef）を JSON ファイルに保存する（受信側などがスキーマを参照できるように）
    ファイル名は呼び出し側が決める（既存の Act ごとのファイル名を維持するため）
    Mod: 2026.10.05    By: IWATA, Y.'s Agent AI（B-9）
    """
    _dir = os.path.dirname(file_path)
    if _dir:
        os.makedirs(_dir, exist_ok=True)
    with open(file_path, "w", encoding=encode) as f:
        json.dump(out_coldef, f, ensure_ascii=False, indent=2)
    base.output_log(module_name, f"Output Column Define Saved: {file_path}", log_file)
    return file_path


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
            "index": [-1, "const", value]              ← 固定値（セグメント内容に依存しない。src_col_idxは任意の値でOK）

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

        # src_col_idx 範囲チェック（seg_len / const 以外）
        if stat_name not in ("seg_len", "const") and not (0 <= src_col_idx < len(c_keys)):
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

    # const : 固定値をそのまま返す（セグメント内容に依存しない）
    if stat_name == "const":
        if not stat_params:
            raise ValueError("const: stat_params[0]=value required")
        const_val = stat_params[0]
        def _f(seg, n_rows, _v=const_val):
            return _v, None
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

    # first / last : raw値（任意型。数値でも文字列でもよい）を対象区間の先頭／末尾行から取得
    # ※ mean/min/max等とは違い数値専用のextract_float_valsを経由しない
    #   （経由すると文字列列で(invalid)になったり、数字文字列がfloat化される不具合があったため分離した）
    if stat_name in ("first", "last"):
        def _f(seg, n_rows, _ss=slice_spec, _src=src_col_idx, _which=stat_name):
            es = get_eff_seg(seg, n_rows, _ss)
            if not es:
                return None, "slice_empty"
            row = es[0] if _which == "first" else es[-1]
            return row[_src], None
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
            "nth", "pertl", "sig_u", "sig_l", "mode", "n_null", "seg_len", "const"]
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
    base.output_log(MODULE_NAME, f"Run Standalone.")
    base.output_log(MODULE_NAME, f"test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
