# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Stream Wave Cutout Module
# Ver: 01.d.20261005    By: IWATA, Y.'s Agent AI
# Mod: 閾値キーの読み出し箇所に残っていた #### threshold の印を削除（A-1）
########################################
init_state             # ステージの開始時刻・評価回数の状態を初期化する（監視開始前に1回呼ぶ）
wrap_spec              # 開始・終了条件の設定（単一形式/多段形式）を内部仕様 {stages:[...]} にそろえる
TRANS_TABLE            # 点種別の遷移表（0:非波形 1:開始点 2:中間点 3:終点）
process_stream_record  # 1レコードごとに開始・終了を判定し、波形セグメントを切り出す
cond_funcs             # 開始・終了判定の関数表（equal / not_equal / change / duration / duration_time / upto / downto / diff）

動作概要
    1. tri_s10_stream_core のメインループから、受信したレコードごとに process_stream_record を呼ぶ
    2. 開始・終了条件（直列ステージ × 並列チェック）を判定し、点種別（開始点・中間点・終点・非波形）を決める
    3. 開始点〜終点のレコードをバッファに蓄積し、終点を検出したら on_cutout_segment（コールバック）で
       呼び出し側へ渡す（キュー送信・STQ 登録・集計は呼び出し側の責務）
    4. 終点＝開始点の場合は、新しいセグメントの開始点として扱う
    5. セグメントが上限（seg_max_rows 行 / seg_max_sec 秒。先に達した方。0=無制限）に達したら、
       その時点のバッファを強制クローズして渡し（on_cutout_segment(..., forced=True)）、非波形に戻る
       （次のレコードから開始条件を評価し直す）
    ※ base 以外のローカルモジュールに依存しない（tri_s10_stream_core を import しない＝循環 import なし）
    ※ ステージ状態はモジュールのグローバル変数で持つ（1プロセス1ストリーム。クラス化は段階2 / チェックリスト B-1）
"""
MODULE_NAME = "WAVE_CUTOUT"

"""standard library"""
from   datetime    import datetime as dt
import os
import sys
import traceback
from   typing      import Any, Callable, Dict, List, Tuple, Sequence, Optional

"""external library"""
## NOT Use


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


"""system dir, file"""
log_file = base.get_log_path()


########################################
# State
########################################

# 現在のセグメントの開始時刻
SEG_START_TS: Optional[dt] = None

# ステージ開始時刻、評価回数カウンタ（各ステージ毎に保持）
STAGE_TIMESTAMP: Dict[str, List[Optional[dt]]] = {"start": [], "end": []}
STAGE_COUNT: Dict[str, List[int]] = {"start": [], "end": []}

# セグメント長の上限（0=無制限）
SEG_MAX_ROWS: int   = 0
SEG_MAX_SEC : float = 0.0

def init_state(start_spec: Dict[str, Any], end_spec: Dict[str, Any],
               seg_max_rows: int = 0, seg_max_sec: float = 0.0) -> None:
    """ステージ開始時刻・評価回数カウンタを初期化。セグメント長の上限を設定
    """
    global STAGE_TIMESTAMP, STAGE_COUNT, SEG_MAX_ROWS, SEG_MAX_SEC, SEG_START_TS
    SEG_MAX_ROWS = int(seg_max_rows or 0)
    SEG_MAX_SEC  = float(seg_max_sec or 0)
    SEG_START_TS = None
    STAGE_TIMESTAMP = {
        "start": [None] * len(start_spec.get("stages", [])),
        "end":   [None] * len(end_spec.get("stages",   [])),
    }
    STAGE_COUNT = {
        "start": [0] * len(start_spec.get("stages", [])),
        "end":   [0] * len(end_spec.get("stages",   [])),
    }


def wrap_spec(spec: Dict[str, Any]) -> Dict[str, Any]:
    """JSONの単一形式/多段形式を内部仕様にラップ"""
    if "stages" in spec: return spec
    return {
        "stages": [{
            "op": "AND",
            "checks": [{
                "method": spec.get("method"),
                "target_loc": spec.get("target_loc", 0),
                "thr": spec.get("border", spec.get("thr"))
            }]
        }]
    }


"""点種別判定 1:開始点 2:中間点 3:終点 0:非波形"""
TRANS_TABLE = [
    [0, 1], # prev_is = 0  [False, True]
    [2, 3], # prev_is = 1
    [2, 3], # prev_is = 2
    [0, 1], # prev_is = 3
]


########################################
# Segment Cutout
########################################
def _check_seg_limit(n_rows: int, timestamp: Any) -> Optional[str]:
    """セグメント長の上限に達したか判定"""
    if SEG_MAX_ROWS > 0 and n_rows >= SEG_MAX_ROWS:
        return "rows"
    if SEG_MAX_SEC > 0 and SEG_START_TS is not None:
        if (timestamp - SEG_START_TS).total_seconds() >= SEG_MAX_SEC:
            return "sec"
    return None


def _seg_log(msg: str) -> None:
    """セグメント検出ログ。通常は画面のみ。デバッグモード時のみログ出力"""
    base.output_log(MODULE_NAME, msg, log_file if base.get_mode_is_debug() else "")


"""波形 開始終了判定 関数群"""
def cond_equal(p_v: Any, p_s: int, ind: int, val: Any, seg: List[Optional[Any]], thr: Any) -> bool:
    """閾値に等しいか判定"""
    return val == thr


def cond_not_equal(p_v: Any, p_s: int, ind: int, val: Any, seg: List[Optional[Any]], thr: Any) -> bool:
    """閾値と異なるか判定"""
    return val != thr


def cond_change(p_v: Any, p_s: int, ind: int, val: Any, seg: List[Optional[Any]], thr: Any) -> bool:
    """変化点（前回値と異なるか）判定"""
    return val != p_v


def cond_duration(p_v: Any, p_s: int, ind: int, val: Any, seg: List[Optional[Any]], thr: int) -> bool:
    """継続回数の判定"""
    try:
        count = seg[2] if seg and len(seg) > 2 else None
        return (thr is not None and thr > 0 and count is not None and count >= thr)
    except Exception:
        return False


def cond_duration_time(p_v: Any, p_s: int, ind: int, val: Any, seg: List[Optional[Any]], thr: float) -> bool:
    """経過時間の判定"""
    try:
        start_ts = seg[0] if seg else None
        now_ts   = seg[1] if seg else None
        if start_ts is None or now_ts is None:
            return False
        if thr is None or thr <= 0:
            return False
        elapsed = (now_ts - start_ts).total_seconds()
        return elapsed >= thr
    except Exception:
        return False


def cond_upto(p_v: Any, p_s: int, ind: int, val: Any, seg: List[Optional[Any]], thr: Any) -> bool:
    """
    前回値から上昇傾向かつ今回値が閾値範囲に収まるか判定
    Args:
        thr: 単一値 or 辞書 {"upper": value} or {"lower": l, "upper": u}
    """
    # 初回チェック(前回値なし)は比較不能なので強制的にFalse
    if p_v is None:
        return False

    # 上昇傾向チェック
    if val <= p_v:
        return False

    if isinstance(thr, dict):
        lower = thr.get("lower", float("-inf"))
        upper = thr.get("upper", float("inf"))

        # 前回値が閾値範囲内なら今回は無視
        if lower <= p_v <= upper:
            return False

        return lower <= val <= upper

    else:
        # 後方互換: 単一値は下限値として扱う
        if p_v >= thr:
            return False

        return thr <= val


def cond_downto(p_v: Any, p_s: int, ind: int, val: Any, seg: List[Optional[Any]], thr: Any) -> bool:
    """
    前回値から下降傾向かつ今回値が閾値範囲に収まるか判定
    Args:
        thr: 単一値 or 辞書 {"lower": value} or {"lower": l, "upper": u}
    """
    # 初回チェック(前回値なし)は比較不能なので強制的にFalse
    if p_v is None:
        return False

    # 下降傾向チェック
    if val >= p_v:
        return False

    if isinstance(thr, dict):
        lower = thr.get("lower", float("-inf"))
        upper = thr.get("upper", float("inf"))

        # 前回値が閾値範囲内なら今回は無視
        if lower <= p_v <= upper:
            return False

        return lower <= val <= upper
    else:
        # 後方互換: 単一値は上限値として扱う
        if p_v <= thr:
            return False

        return val <= thr


def cond_diff(p_v: Any, p_s: int, ind: int, val: Any, seg: List[Optional[Any]], thr: float) -> bool:
    """
    差分閾値判定
    Args:
        thr: 差分の閾値 (絶対値)
    """
    # 初回チェック(前回値なし)は比較不能なので強制的にFalse
    if p_v is None: return False

    return abs(val - p_v) >= thr


"""波形 開始終了判定 関数群 マッピング"""
cond_funcs = {
    "equal"    : cond_equal,
    "not_equal": cond_not_equal,
    "change"   : cond_change,
    "duration" : cond_duration,
    "duration_time": cond_duration_time,
    "upto"     : cond_upto,
    "downto"   : cond_downto,
    "diff"     : cond_diff,
}


def _evaluate_check(prev_values: Optional[Sequence[Any]], prev_status: Optional[int], idx: int, values: Sequence[Any], seg: List[Optional[int]], check: Dict[str, Any]) -> bool:
    """
    単一チェックを評価（毎回実施）
    checkは {method|func, target_loc, thr} を含む
    """
    func = check.get("func")
    if func is None:
        # method から関数解決（従来形式をサポート）
        method = check.get("method")
        func = cond_funcs.get(method) if isinstance(method, str) else None

    loc  = int(check.get("target_loc", 0))
    thr  = check.get("thr", check.get("border"))
    if func is None:
        return False

    if loc < 0 or loc >= len(values):
        return False

    p_v = prev_values[loc] if prev_values is not None else None
    v   = values[loc]
    try:
        return bool(func(p_v, prev_status, idx, v, seg, thr))

    except Exception:
        return False


def _evaluate_stage(prev_values: Optional[Sequence[Any]], prev_status: Optional[int], idx: int, values: Sequence[Any], seg: List[Optional[int]], stage: Dict[str, Any]) -> bool:
    """
    並列チェック群をAND/ORで評価（毎回全チェックを実施）
    stageは {op, checks:[...]}.
    """
    op = (stage.get("op") or "AND").upper()
    checks = stage.get("checks") or []
    if not checks:
        return False

    results = [_evaluate_check(prev_values, prev_status, idx, values, seg, c) for c in checks]
    if op == "OR":
        return any(results)

    return all(results)


def evaluate_specs(record: Tuple[Any, int, Sequence[Any], Any, Sequence[Any]], prev_values: Optional[Sequence[Any]], prev_status: Optional[int], seg_range: List[Optional[int]], specs: Dict[str, Any], progress: List[bool]) -> bool:
    """
    直列ステージを先頭から評価。成立済みステージは免除。 未成立ステージが成立すれば状況を更新。すべてのステージがTrue（済/今回）なら True を返す。
    """

    ts, index, value = record
    stages = (specs.get("stages") or [])

    # 進捗リストの長さをステージ数に合わせる
    if progress is None or len(progress) != len(stages):
        progress[:] = [False] * len(stages)

    # 直列評価
    for i, stage in enumerate(stages):
        if progress[i]:
            continue  # 一度Trueなら次回以降は免除

        # ステージローカル開始時刻管理（初めてこのステージを評価するタイミングで開始時刻をセット）
        # progress_ts は monitor_stream 側で管理し、ここでは seg_range の代わりに [stage_start_ts[i], ts] を渡す
        ok = _evaluate_stage(prev_values, prev_status, index, value, seg_range, stage)
        if ok:
            progress[i] = True
        else:
            return False

    return True


def format_progress(cond_progress: Dict[str, List[bool]]) -> str:
    """デバッグ用に進捗配列を簡易整形して文字列化"""
    try:
        s = ''.join('1' if b else '0' for b in cond_progress.get('start', []))
        e = ''.join('1' if b else '0' for b in cond_progress.get('end',   []))
        return f"start=[{s}] end=[{e}]"
    except Exception:
        return "start=[] end=[]"


def _round_ts(ts: dt, round_ms: int) -> dt:
    """タイムスタンプを round_ms 単位で切り捨て（0=丸めなし）"""
    if round_ms <= 0:
        return ts
    us = (ts.microsecond // (round_ms * 1000)) * (round_ms * 1000)
    return ts.replace(microsecond=us)
def process_stream_record(
    seg_id          : int,
    seg_ind         : int,
    record          : Tuple[Any, int, Sequence[Any], Any, Sequence[Any]],
    segment_buffer  : List[Tuple[Any, int, Sequence[Any], Any, Sequence[Any]]],
    on_cutout_segment: Callable[..., None],
    prev_values     : Optional[Sequence[Any]],
    prev_status     : Optional[int],
    seg_range       : List[Optional[int]],
    cond_specs      : Tuple[Dict[str, Any], Dict[str, Any]],  # ({stages:[...]}, {stages:[...]})
    transition      : List[Optional[int]],
    cond_progress   : Dict[str, List[bool]],                   # {"start":[...], "end":[...]}
    ts_round_ms     : int = 0,                                 # 0=丸めなし。ms単位切り捨て
) -> Tuple[
    Sequence[Any],
    Optional[int],
    List[Optional[int]]
]:
    """
    波形データを受信し、開始・終了判定を以って波形セグメントを検出・抽出する。
    リアルタイムセンサーデータを監視し、条件判定関数に基づいて波形の開始点・中間点・終点を判定する。
    検出した波形データはバッファに蓄積し、終点検出時にセグメントとして登録・送信する。

    Mod: 2026.10.04    By: IWATA, Y.'s Agent AI（seg_range の終了 index を終点行に合わせた。B-2）

    Args:
        seg_id (int): Segment ID, 16 digits TS (yyyymmddhhMMss00) to int
        seg_ind (int): Segment Record Index
        record (tuple): (timestamp, index, value) 形式のデータレコード
            - timestamp: データ発生時刻
            - index (int): データのインデックス
            - value (Sequence): データ値のリスト
        segment_buffer (list): 現在検出中の波形データを蓄積するバッファ
        on_cutout_segment (callable): セグメントが切り出された（終点を検出した）ときに呼ぶ関数（コールバック）
            on_cutout_segment(seg_copy, seg_id, seg_ind) の形で呼ぶ。キュー送信・STQ 登録・集計等は呼び出し側で行う
            上限による強制クローズの場合は on_cutout_segment(seg_copy, seg_id, seg_ind, forced=True) で呼ぶ
        prev_values (Sequence[Any]): 監視対象データの前回値
        prev_status (int or None): 前回の点種別 (0=非波形, 1=開始点, 2=中間点, 3=終点)
        seg_range (list): 現在の波形区間 [開始index, 終了index（終点行を含む）]。未確定の場合は None を含む
        cond_specs (tuple): (開始条件仕様, 終了条件仕様)
        transition (list): 状態遷移表。prev_is と判定結果から次の点種別を決定する
        cond_progress (dict): 直列ステージの成立進捗を保持する辞書
        ts_round_ms (int): seg_ts の丸め単位（ms）。0=丸めなし

    Returns:
        tuple: (prev_value, prev_is, seg_range) の更新後の値
            - prev_value (Any): 更新後の監視対象データ値
            - prev_is (int): 更新後の点種別 (0/1/2/3)
            - seg_range (list): 更新後の波形区間 [開始index, 終了index or None]
    """

    # Declare Global Var
    global STAGE_TIMESTAMP, STAGE_COUNT, SEG_START_TS

    """recordを各変数に展開"""
    timestamp, index, value = record

    """cond_specs 展開"""
    start_spec, end_spec = cond_specs

    # 進捗初期化
    cond_progress.setdefault("start", [False] * len(start_spec.get("stages", [])))
    cond_progress.setdefault("end",   [False] * len(end_spec.get("stages", [])))

    """前回値の初期化（全列保持）"""
    if prev_values is None: prev_values = value

    """条件判定"""
    if prev_status == 0 or prev_status == 3:
        # 開始側ステージの開始時刻セット・評価回数カウントアップ
        # （前段が成立済みで、まだ自身が未成立＝現在アクティブなステージのみ対象）
        for i in range(len(STAGE_TIMESTAMP["start"])):
            if all(cond_progress["start"][:i]) and not cond_progress["start"][i]:
                if STAGE_TIMESTAMP["start"][i] is None:
                    STAGE_TIMESTAMP["start"][i] = timestamp
                STAGE_COUNT["start"][i] += 1

        # ステージごとに評価（seg=[stage_start_ts, now_ts, eval_count]）
        stages = (start_spec.get("stages") or [])
        val_match = True
        for i, stage in enumerate(stages):
            if cond_progress["start"][i]: continue

            seg_pair = [STAGE_TIMESTAMP["start"][i], timestamp, STAGE_COUNT["start"][i]]
            ok = _evaluate_stage(prev_values, prev_status, index, value, seg_pair, stage)

            if ok:
                cond_progress["start"][i] = True
            else:
                val_match = False
                break

    elif prev_status == 1 or prev_status == 2:
        # 終了側ステージの開始時刻セット・評価回数カウントアップ
        # （前段が成立済みで、まだ自身が未成立＝現在アクティブなステージのみ対象）
        for i in range(len(STAGE_TIMESTAMP["end"])):
            if all(cond_progress["end"][:i]) and not cond_progress["end"][i]:
                if STAGE_TIMESTAMP["end"][i] is None:
                    STAGE_TIMESTAMP["end"][i] = timestamp
                STAGE_COUNT["end"][i] += 1

        # ステージごとに評価（seg=[stage_start_ts, now_ts, eval_count]）
        stages = (end_spec.get("stages") or [])
        val_match = True
        for i, stage in enumerate(stages):
            if cond_progress["end"][i]: continue

            seg_pair = [STAGE_TIMESTAMP["end"][i], timestamp, STAGE_COUNT["end"][i]]
            ok = _evaluate_stage(prev_values, prev_status, index, value, seg_pair, stage)

            if ok:
                cond_progress["end"][i] = True
            else:
                val_match = False
                break
    else:
        val_match = False
    
    """点種別判定 1:開始点 2:中間点 3:終点 0:非波形"""
    if prev_status is not None: val_is = transition[prev_status][val_match]
    else                      : val_is = 0

    if   val_is == 0: seg_range    = [None, None]
    elif val_is == 1: seg_range[0] = index
    elif val_is == 3: seg_range[1] = index # 終点行を含む（バッファと同じ定義。終点行の除外は Act の CUT_LAST_ROW に従い core が行う）
    
    """開始点処理"""
    if val_is == 1:
        _ts = _round_ts(dt.now(), ts_round_ms)
        seg_id = int(_ts.strftime("%Y%m%d%H%M%S%f")[:-4])
        seg_ts = _ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        seg_ind = 1
        segment_buffer.append([seg_id, seg_ind, seg_ts, 0] + value)
        SEG_START_TS = timestamp

        cond_progress["start"] = [False] * len(start_spec.get("stages", []))
        STAGE_TIMESTAMP["start"] = [None] * len(start_spec.get("stages", []))
        STAGE_COUNT["start"] = [0] * len(start_spec.get("stages", []))

        _seg_log(f"波形開始点 検出: seg_range={seg_range}")
        _seg_log(f"progress reset(start) {format_progress({'start':[False]*len(start_spec.get('stages', [])), 'end':cond_progress['end']})}")

    """中間点処理"""
    if val_is == 2:
        _ts = _round_ts(dt.now(), ts_round_ms)
        seg_ts = _ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        seg_ind += 1
        segment_buffer.append([seg_id, seg_ind, seg_ts, 0] + value)

    """セグメント長の上限（開始点・中間点で判定。先に達した方で強制クローズし、非波形に戻る）"""
    if val_is in (1, 2):
        _reason = _check_seg_limit(len(segment_buffer), timestamp)
        if _reason:
            _sec = (timestamp - SEG_START_TS).total_seconds() if SEG_START_TS is not None else 0.0
            base.output_log(MODULE_NAME,
                f"WARNING  Segment Forced Close (reason={_reason}): seg_id={seg_id}  rows={len(segment_buffer)}  "
                f"sec={_sec:.1f}  seg_max_rows={SEG_MAX_ROWS}  seg_max_sec={SEG_MAX_SEC}", log_file)

            seg_copy = segment_buffer.copy()# 浅いコピー(メモリ対策)
            if seg_copy:
                on_cutout_segment(seg_copy, seg_id, seg_ind, forced=True)

            # 進捗・ステージ状態・バッファをリセットし非波形へ（次のレコードから開始条件を評価し直す）
            cond_progress["start"] = [False] * len(start_spec.get("stages", []))
            cond_progress["end"]   = [False] * len(end_spec.get("stages", []))
            STAGE_TIMESTAMP["start"] = [None] * len(start_spec.get("stages", []))
            STAGE_COUNT["start"]     = [0]    * len(start_spec.get("stages", []))
            STAGE_TIMESTAMP["end"]   = [None] * len(end_spec.get("stages", []))
            STAGE_COUNT["end"]       = [0]    * len(end_spec.get("stages", []))
            segment_buffer.clear()
            SEG_START_TS = None
            seg_range = [None, None]
            seg_id  = 0
            seg_ind = 0
            val_is  = 0

    """終点処理"""
    if val_is == 3:
        _ts = _round_ts(dt.now(), ts_round_ms)
        seg_ts = _ts.strftime("%Y-%m-%d %H:%M:%S.%f")
        seg_ind += 1
        segment_buffer.append([seg_id, seg_ind, seg_ts, 0] + value)

        sys.stdout.write(f"\n")
        _seg_log(f"波形終点 検出: seg_range={seg_range}")

        """波形登録（クリア前にコピー）。キュー送信・STQ 登録・集計・STQ 掃除は呼び出し側（on_cutout_segment）で行う"""
        seg_copy = segment_buffer.copy()# 浅いコピー(メモリ対策)
        if seg_copy:
            on_cutout_segment(seg_copy, seg_id, seg_ind)

        # 終了成立後、終了側進捗をリセット
        cond_progress["end"] = [False] * len(end_spec.get("stages", []))
        STAGE_TIMESTAMP["end"] = [None] * len(end_spec.get("stages", []))
        STAGE_COUNT["end"] = [0] * len(end_spec.get("stages", []))
        _seg_log(f"progress reset(end) {format_progress(cond_progress)}")

        # 波形送信後のため、セグメントバッファをクリア
        segment_buffer.clear()

        """終点＝開始点の場合をチェック"""
        # 終点＝開始点再評価: 新規区間として開始側進捗をリセットしてから評価
        cond_progress["start"] = [False] * len(start_spec.get("stages", []))
        STAGE_TIMESTAMP["start"] = [None] * len(start_spec.get("stages", []))
        STAGE_COUNT["start"] = [0] * len(start_spec.get("stages", []))
        val_match2 = evaluate_specs(record, prev_values, prev_status, [index, None], start_spec, cond_progress["start"])  # start側進捗は新規区間として評価（インデックス基準）
        # ※ この一発判定パスでは seg=[index, None]（2要素）のため、
        #   seg[2] が無く cond_duration は count=None → False を返す（複数回の蓄積が前提の条件なので妥当）

        if val_match2:
            _seg_log(f"終点＝開始点 検出: seg_range={[index, None]}")
            val_is    = 1
            seg_range = [index, None]
            _ts = _round_ts(dt.now(), ts_round_ms)
            seg_id = int(_ts.strftime("%Y%m%d%H%M%S%f")[:-4])
            seg_ts = _ts.strftime("%Y-%m-%d %H:%M:%S.%f")
            seg_ind = 1
            segment_buffer.append([seg_id, seg_ind, seg_ts, 0] + value)
            SEG_START_TS = timestamp

        else:
            # 終点＝開始点不成立: 進捗・バッファをリセットし非波形へ遷移
            seg_range = [None, None]
            segment_buffer.clear()
            STAGE_TIMESTAMP["end"] = [None] * len(end_spec.get("stages", []))
            STAGE_COUNT["end"]     = [0]    * len(end_spec.get("stages", []))
            seg_id  = 0
            seg_ind = 0

    prev_values = value
    prev_status = val_is

    return seg_id, seg_ind, prev_values, prev_status, seg_range


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, "Run Standalone.")
