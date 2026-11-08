# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Segment Stats Calculation Functions
# Ver: 01.g.20261108    By: IWATA, Y.'s Agent AI
# Mod: pyflakes の指摘を整理（使っていない import・変数、置き換えの無い f-string を削除。動作は変えない）
########################################
セグメントから統計量を求める処理を、統計量を出力する Act の間で共通化する

主要関数:
init_stats            # 設定を検証・コンパイルし、実行コンテキストを返す（Act の起動時に1回）。
calc_stats            # セグメントの統計量を求め、求められなかった項目をログに出す（セグメントごと）。
compile_register_map  # column_register_map と output_column を検証し、計算関数のリストにする。
build_stats           # コンパイル済みのリストに従って、統計量の辞書を作る。
export_out_coldef     # 出力列の定義を JSON ファイルに保存する。

位置づけ: 統計量を出力する Act（CSV・SQLite・Socket・MQTT・RDB・PLC 等）から呼ばれる。Act は init_stats（起動時）→ calc_stats（セグメントごと）の順に使う。
設計意図: 設定の検証と計算関数の組み立ては起動時に済ませ、セグメントごとの処理は組み立て済みの関数を順に呼ぶだけにする。設定の誤りは起動時に止め、稼働中に気づく事態を避ける。値を出せなかった項目は理由（{出力キー}_WARN）を残し、黙って欠けないようにする。
前提・制約: 列の指定は Find と同じ列番号で書き、先頭の固定4列は seg_id / seg_ind / seg_ts / seg_status の名前で書く。
"""
MODULE_NAME = "Calc_Seg_Stats"

"""standard library"""
from   datetime import datetime as dt
import json
import os
import sys
import traceback
from   typing   import Any, Dict, List, Tuple, Optional

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
            ※ src_col_idx は Find と同じ列番号（0 始まり）。先頭の固定4列は "seg_id" / "seg_ind" / "seg_ts" / "seg_status" の名前で書く
              （行の中の位置への変換は base.resolve_act_col）
            "index": [src_col_idx, "stat_name"]
            "index": [src_col_idx, "stat_name", [start, end]]
            "index": [src_col_idx, "stat_name", param]
            "index": [src_col_idx, "stat_name", [start, end], param]
            "index": [-1, "const", value]              ← 固定値（セグメント内容に依存しない。src_col_idxは任意の値でOK）
            "index": [src_index, "judge", rules]          ← 判定（src_index は column_register_map の番号。Find の列番号ではない）
            "index": [src_index, "judge", rules, default]
                rules  : [{"in": [値, ...], "to": 判定結果}, {"range": [最小, 最大], "to": 判定結果}, ...]
                         上から順に照合し、最初に一致した "to" を出す。どれにも一致しなければ default（省略時は値なし）
                         元の列の値が出せなかった場合も default
                in     : 元の列の値がリストのどれかと等しい（元の列の dtype が str / int / float）
                range  : 最小 <= 元の列の値 <= 最大（両端を含む。null は下限・上限なし。元の列の dtype が int / float）
                ※ 判定結果は必ず自分の列（新しい列）に出す。元の列の値は変えない
                ※ in の値は元の列の dtype に、to / default は自分の列の dtype に、起動時に変換する（int は整数のみ）

        output_column:
            "index": ["出力キー名", "dtype", "role", "var_type", "説明"]
            ※ index は column_register_map と一致させること
    [検証]
        - column_register_map / output_column のキーが一致しない → ValueError（起動停止）
        - src_col_idx 範囲外・不正な名前 → ValueError
        - 不正 stat_name / 不正 params → ValueError
        - judge: 元の列が無い・自分自身・judge / dtype が int・float・str 以外（time 等）/ str の列に range /
                 rules の形が不正（range の最小 > 最大を含む）/ dtype に変換できない値 → ValueError
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
            src_col_raw = entry[0]
            stat_name   = str(entry[1])
            rest        = list(entry[2:])
        except (IndexError, ValueError, TypeError) as e:
            raise ValueError(f"[{idx}] column_register_map エントリ不正: {entry}  ({e})")

        # judge : 別の統計量の列の値を判定する（Find の列は使わない）
        if stat_name == "judge":
            compiled.append((out_key, _compile_judge(idx, entry, crm, oc)))
            continue

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

        # src_col_idx を行の中の位置に変換（Find の列番号 + 4、または固定4列の名前。seg_len / const は列を使わない）
        if stat_name in ("seg_len", "const"):
            src_col_idx = -1
        else:
            src_col_idx = base.resolve_act_col(src_col_raw, c_keys, f"column_register_map[{idx}]")

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
    """stat計算ロジックを生成。LOOP INIT 時に呼び出す
    stat_name: mean / min / max / std / count / sum / range / med / iqr / first / last / nth / pertl / sig_u / sig_l / mode / n_null / seg_len / const
    ※ first / last / nth / mode / const は数値専用の float 変換を経由せず、行の生の値（任意型）をそのまま返す
    slice_spec: [start_frac, end_frac] で対象区間を絞り込んでから算出する（seg_len は全行数のため適用しない）
    """

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
            "nth", "pertl", "sig_u", "sig_l", "mode", "n_null", "seg_len", "const", "judge"]
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
    judges: List[Tuple[str, Any]] = []
    for out_key, func in compiled:
        if isinstance(func, _JudgeFunc):
            judges.append((out_key, func))
            continue
        value, warn = func(seg, n_rows)
        if warn:
            result[f"{out_key}_WARN"] = warn
        elif value is not None:
            result[out_key] = value
    # judge は通常の統計量を算出した後に判定する（元の列の値を使うため）
    for out_key, func in judges:
        value = func(result)
        if value is not None:
            result[out_key] = value
    return result


########################################
# Judge Functions（統計量の値を条件で判定し、判定結果を新しい列に出す）
########################################

JUDGE_DTYPES = ("int", "float", "str")


def _judge_cast(value: Any, dtype: str, where: str) -> Any:
    """
    judge の値（in の値・to・default）を dtype に合わせる（起動時に呼ぶ）
    str: 文字列にする / int: 整数にする（整数にならない値はエラー）/ float: 数値にする
    変換できない値・bool は ValueError
    Mod: 2026.10.29    By: IWATA, Y.'s Agent AI
    """
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{where}: {value!r} cannot be used as {dtype}")
    if dtype == "str":
        return str(value)
    try:
        num = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{where}: {value!r} cannot be converted to {dtype}")
    if dtype == "int":
        if not num.is_integer():
            raise ValueError(f"{where}: {value!r} is not an integer (dtype=int)")
        return int(num)
    return num


class _JudgeFunc:
    """
    judge の判定関数（_compile_judge が作り、build_stats が通常の統計量の後に呼ぶ）
        src_key   : 元の列の出力キー名
        src_dtype : 元の列の dtype（照合の仕方を決める）
        rules     : [("in", frozenset, to) / ("range", (最小 or None, 最大 or None), to), ...]（上から順に照合）
        default   : どれにも一致しない・元の列の値が無いときの値（None なら値なし）
    Mod: 2026.10.29    By: IWATA, Y.'s Agent AI
    """
    def __init__(self, src_key: str, src_dtype: str, rules: List[Tuple[str, Any, Any]], default: Any):
        self.src_key   = src_key
        self.src_dtype = src_dtype
        self.rules     = rules
        self.default   = default

    def __call__(self, result: Dict[str, Any]) -> Any:
        value = result.get(self.src_key)
        if value is None:
            return self.default
        if self.src_dtype == "str":
            key = str(value)
        else:
            try:
                key = float(value)
            except (TypeError, ValueError):
                return self.default
        for kind, cond, to in self.rules:
            if kind == "in":
                if key in cond:
                    return to
            elif (cond[0] is None or cond[0] <= key) and (cond[1] is None or key <= cond[1]):
                return to
        return self.default


def _compile_judge(idx: str, entry: list, crm: Dict[str, Any], oc: Dict[str, Any]) -> _JudgeFunc:
    """
    judge エントリ [src_index, "judge", rules(, default)] を検証・コンパイルする（起動時）
    src_index は column_register_map の番号。条件は元の列の dtype、判定結果は自分の列の dtype に合わせる
    Mod: 2026.10.29    By: IWATA, Y.'s Agent AI
    """
    src_raw = entry[0]
    if isinstance(src_raw, bool) or not str(src_raw).strip().isdigit():
        raise ValueError(f"[{idx}] judge: source index {src_raw!r} must be a column_register_map index")
    src = str(int(str(src_raw).strip()))
    if src not in crm:
        raise ValueError(f"[{idx}] judge: source index {src} is not in column_register_map")
    if src == idx:
        raise ValueError(f"[{idx}] judge: source index must not be itself")
    src_entry = crm[src]
    if isinstance(src_entry, (list, tuple)) and len(src_entry) > 1 and str(src_entry[1]) == "judge":
        raise ValueError(f"[{idx}] judge: source index {src} is also judge (judge of judge is not allowed)")

    try:
        src_key, src_dtype = str(oc[src][0]), str(oc[src][1])
        dst_dtype = str(oc[idx][1])
    except (IndexError, TypeError) as e:
        raise ValueError(f"[{idx}] judge: output_column entry has no dtype  ({e})")
    for name, dtype in (("source", src_dtype), ("output", dst_dtype)):
        if dtype not in JUDGE_DTYPES:
            raise ValueError(f"[{idx}] judge: {name} dtype {dtype!r} is not supported (use {list(JUDGE_DTYPES)})")

    rest = list(entry[2:])
    if not rest or len(rest) > 2 or not isinstance(rest[0], list) or not rest[0]:
        raise ValueError(f"[{idx}] judge: [src_index, \"judge\", rules(, default)] with non-empty rules list is required")
    default = _judge_cast(rest[1], dst_dtype, f"[{idx}] judge default") if len(rest) == 2 and rest[1] is not None else None

    rules: List[Tuple[str, Any, Any]] = []
    for i, rule in enumerate(rest[0]):
        where = f"[{idx}] judge rules[{i}]"
        if not isinstance(rule, dict) or "to" not in rule or (("in" in rule) == ("range" in rule)) \
                or set(rule.keys()) - {"in", "range", "to"}:
            raise ValueError(f"{where}: must be {{\"in\": [...], \"to\": ...}} or {{\"range\": [min, max], \"to\": ...}}")
        to = _judge_cast(rule["to"], dst_dtype, f"{where} to")
        if "in" in rule:
            values = rule["in"]
            if not isinstance(values, list) or not values:
                raise ValueError(f"{where}: in must be a non-empty list")
            rules.append(("in", frozenset(_judge_cast(v, "str" if src_dtype == "str" else "float", f"{where} in")
                                          for v in values), to))
        else:
            if src_dtype == "str":
                raise ValueError(f"{where}: range cannot be used for str source (use in)")
            bounds = rule["range"]
            if not isinstance(bounds, list) or len(bounds) != 2:
                raise ValueError(f"{where}: range must be [min, max]")
            lo, hi = (None if v is None else _judge_cast(v, "float", f"{where} range") for v in bounds)
            if lo is not None and hi is not None and lo > hi:
                raise ValueError(f"{where}: range min {lo} > max {hi}")
            rules.append(("range", (lo, hi), to))
    return _JudgeFunc(src_key, src_dtype, rules, default)


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, "Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
