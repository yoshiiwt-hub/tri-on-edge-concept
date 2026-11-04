# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# SPC Rules Calculation Functions
# Ver: 01.c.20261102    By: IWATA, Y.'s Agent AI
# Mod: モジュール冒頭の Docstring を規約に合わせて簡素化
########################################
セグメント内の値の並びを、SPC の異常判定ルール（Nelson ルールの一般化）で判定する

主要関数:
compile_spc_rules  # spc_rules の設定を検証し、判定関数のリストにする（起動時に1回）。
check_spc          # 1セグメント分の値の並びに全ルールを適用し、違反した点の位置を返す。

位置づけ: tri_s12_act_diag_spc から呼ばれる。中心値（center）と標準偏差（sigma）は、このモジュールでは求めず、呼び出し側が渡す（固定値、または別途学習した値）。
設計意図: pandas / numpy を使わず標準ライブラリだけで実装し、exe を軽く保つ。ルールは関数表（_RULE_FUNCS）で切り替え、判定関数と compile_spc_rules の分岐を足すだけで追加できるようにする。
"""
MODULE_NAME = "Calc_Spc"

"""standard library"""
from   datetime import datetime as dt
from   typing   import Any, Callable, Dict, List, Optional, Tuple


def _log(msg: str) -> None:
    ts = dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    print(f"{ts}  [{MODULE_NAME}]  {msg}")


########################################
# Rule Functions
########################################

def _rule_trend(values: List[Optional[float]], center: float, sigma: float,
                 n: int, direction: str = "both") -> List[int]:
    """
    n点連続で上昇・下降しているかを判定する（Nelsonルール3の一般化）
    Args:
        n         : 連続点数（2以上）
        direction : "up"（上昇のみ）| "down"（下降のみ）| "both"（両方）
    Returns:
        違反に該当した点のインデックス一覧（重複除去・昇順）
    """
    violations: set = set()
    for i in range(len(values) - n + 1):
        window = values[i:i + n]
        if any(v is None for v in window):
            continue
        is_up   = all(window[j] < window[j + 1] for j in range(n - 1))
        is_down = all(window[j] > window[j + 1] for j in range(n - 1))
        if (direction in ("up", "both") and is_up) or (direction in ("down", "both") and is_down):
            violations.update(range(i, i + n))
    return sorted(violations)


def _rule_zone_out(values: List[Optional[float]], center: float, sigma: float,
                    m: int, n_out: int, sigma_level: float) -> List[int]:
    """
    mウィンドウ中n_out点以上が center±(sigma_level*sigma) を同じ側で超えているかを判定する
    （Nelsonルール5/6の一般化。同じ側（上側 or 下側）でのカウントのみを対象とする）
    Args:
        m           : ウィンドウの点数
        n_out       : mのうち、閾値超えとみなす最小点数
        sigma_level : 閾値＝center ± sigma_level*sigma
    Returns:
        違反に該当した点のインデックス一覧（重複除去・昇順）
    """
    violations: set = set()
    upper = center + sigma_level * sigma
    lower = center - sigma_level * sigma

    for i in range(len(values) - m + 1):
        window = values[i:i + m]
        if any(v is None for v in window):
            continue
        idx_upper = [i + j for j, v in enumerate(window) if v > upper]
        idx_lower = [i + j for j, v in enumerate(window) if v < lower]
        if len(idx_upper) >= n_out:
            violations.update(idx_upper)
        if len(idx_lower) >= n_out:
            violations.update(idx_lower)

    return sorted(violations)


"""ルール名 → 判定関数 のマップ（拡張する場合はここに追加）"""
_RULE_FUNCS: Dict[str, Callable] = {
    "trend"   : _rule_trend,
    "zone_out": _rule_zone_out,
}


########################################
# Compile & Check
########################################

def compile_spc_rules(rules_config: List[Dict[str, Any]]) -> List[Tuple[str, Callable]]:
    """
    spc_rules設定を検証・コンパイルする
    Returns:
        [(rule_label, func(values, center, sigma) -> List[int]), ...]
    """
    compiled: List[Tuple[str, Callable]] = []

    for i, rule in enumerate(rules_config):
        method = str(rule.get("method", "")).lower()
        label  = str(rule.get("label", f"{method}_{i}"))

        if method == "trend":
            n = int(rule.get("n", 0))
            direction = str(rule.get("direction", "both")).lower()
            if n < 2:
                raise ValueError(f"spc_rules[{i}] (trend) n must be >= 2. Got: {n}")
            if direction not in ("up", "down", "both"):
                raise ValueError(
                    f"spc_rules[{i}] (trend) direction must be 'up'/'down'/'both'. Got: {direction!r}")

            def _func(values, center, sigma, _n=n, _d=direction):
                return _rule_trend(values, center, sigma, _n, _d)

        elif method == "zone_out":
            m           = int(rule.get("m", 0))
            n_out       = int(rule.get("n_out", 0))
            sigma_level = float(rule.get("sigma_level", 1.0))
            if m < 1:
                raise ValueError(f"spc_rules[{i}] (zone_out) m must be >= 1. Got: {m}")
            if not (1 <= n_out <= m):
                raise ValueError(f"spc_rules[{i}] (zone_out) n_out must be within 1-{m}. Got: {n_out}")
            if sigma_level <= 0:
                raise ValueError(f"spc_rules[{i}] (zone_out) sigma_level must be > 0. Got: {sigma_level}")

            def _func(values, center, sigma, _m=m, _no=n_out, _sl=sigma_level):
                return _rule_zone_out(values, center, sigma, _m, _no, _sl)

        else:
            raise ValueError(
                f"spc_rules[{i}] unknown method: {method!r}  valid={sorted(_RULE_FUNCS.keys())}")

        compiled.append((label, _func))

    return compiled


def check_spc(
    values:   List[Optional[float]],
    center:   float,
    sigma:    float,
    compiled: List[Tuple[str, Callable]],
) -> Dict[str, List[int]]:
    """
    コンパイル済みルールリストに基づき、1セグメント分の値列を判定する
    Returns:
        {rule_label: [違反した点のインデックスのリスト], ...}
    """
    result: Dict[str, List[int]] = {}
    for label, func in compiled:
        result[label] = func(values, center, sigma)
    return result


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    _log("Run Standalone (self-test).")
