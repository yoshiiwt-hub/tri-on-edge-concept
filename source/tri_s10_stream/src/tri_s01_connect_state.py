# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Common Connect State Module
# Ver: 01.a.20261106    By: IWATA, Y.'s Agent AI
# Mod: 新規作成（各トリガーにコピーされていた「接続できない状態」の記録・再接続の間隔・通知・ログを集約）
########################################
トリガーの接続先が、つながる状態（connectable）か、つながらない状態（not_connectable）かを記録し、問い合わせに答える窓口

主要関数:
ConnectState                      # 1つの接続先の状態。トリガーが接続先ごとに1つ作り、判定した結果を報告する。
ConnectState.set_not_connectable  # not_connectable への移行を記録する（状態が変わったときだけログ・メトリクス・ErrorIF）。
ConnectState.should_try_reconnect # not_connectable で、再接続の間隔が過ぎたか（真なら、トリガーが再接続を試みる）。
ConnectState.reconnect_failed     # 再接続の失敗を記録する（回数をメトリクスに、ErrorIF へ通知）。
ConnectState.set_connectable      # connectable への復帰を記録する（状態が変わったときだけログ・メトリクス）。
ConnectState.is_connectable       # トリガー自身が最後に報告した状態（False なら not_connectable）。
get_is_connectable                # 外から、トリガー（と接続先）の状態を問い合わせる。
get_connect_states                # 外から、全トリガーの状態の一覧を得る。

位置づけ: 接続できるかの判定・リトライ・つながらない間の扱い（スキップ・待機など）・システムのオフラインモードへの対応は、
          各トリガーが行う。ここはトリガーの報告を記録し、他のモジュール（Operator など）が状態を問い合わせる先になる。
設計意図: 状態の記録・再接続の間隔・回数・通知・ログを1か所にまとめ、トリガーごとの書き方の差をなくす。
          ログ・ErrorIF への移行の通知は状態が変わったときだけ出し、再接続の失敗が続いてもログが増え続けないようにする。
前提・制約: リトライ（1回の処理の中でのやり直し）はトリガーの中で行い、ここへは報告しない。報告は、not_connectable への移行・
            再接続の失敗・connectable への復帰だけ。システムのオフラインモード（base.get_mode_is_offline）には関与しない。
            1つの ConnectState は、それを作ったトリガーのスレッドだけが更新する。問い合わせ（get_*）はどのスレッドからでもよい。
"""
MODULE_NAME = "CONNECT_STATE"

"""standard library"""
from   datetime import datetime as dt
import os
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Optional, Tuple

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

"""local module"""
from   src.tri_s01_error_if import ErrorIF

"""system dir, file"""
log_file = base.get_log_path()


########################################
# State
########################################

CONNECTABLE     = "connectable"
NOT_CONNECTABLE = "not_connectable"

_registry: Dict[Tuple[str, str], "ConnectState"] = {}   # (トリガー名, 接続先) → ConnectState
_registry_lock = threading.Lock()


class ConnectState:
    """
    1つの接続先の状態（connectable / not_connectable）を記録する。トリガーが接続先ごとに1つ作る
        trigger_name           : トリガー名（ErrorIF の trigger_id・状態の一覧のキー）
        target                 : 接続先の名前（例 "127.0.0.1:9000"。状態の一覧のキー・ログに出す）
        reconnect_interval_sec : not_connectable の間に、再接続を試みる間隔（秒）
        module_name            : メトリクス・ログに使うトリガーの MODULE_NAME（省略時はトリガー名）
        log_file               : 状態が変わったときのログの出力先（省略時は base のログファイル）
    メトリクス（module_name の下）: connect_state / not_connectable_since / reconnect_fail_count / total_not_connectable
    ※ 作った時点は connectable。同じ (トリガー名, 接続先) で作り直すと、一覧の登録は新しい方に置き換わる
    Mod: 2026.11.06    By: IWATA, Y.'s Agent AI
    """

    def __init__(self, trigger_name: str, target: str, reconnect_interval_sec: float,
                 module_name: str = "", log_file: str = ""):
        self.trigger_name           = str(trigger_name)
        self.target                 = str(target)
        self.reconnect_interval_sec = float(reconnect_interval_sec)
        self.module_name            = module_name or self.trigger_name
        self.log_file               = log_file or globals()["log_file"]
        self.signature              = f"ConnectionFailed.comm.loop.{self.trigger_name}"

        self._lock                  = threading.Lock()   # 問い合わせ（別スレッド）と更新の間の読み書きを守る
        self.state                  = CONNECTABLE
        self.not_connectable_since: Optional[str] = None
        self.reconnect_fail_count   = 0
        self.total_not_connectable  = 0
        self._last_reconnect_try    = 0.0

        base.metrics_set_last(self.module_name, "connect_state", self.state)
        base.metrics_set_last(self.module_name, "not_connectable_since", "-")
        base.metrics_set_last(self.module_name, "reconnect_fail_count", 0)
        base.metrics_add(self.module_name, "total_not_connectable", 0)
        with _registry_lock:
            _registry[(self.trigger_name, self.target)] = self

    # ---- トリガーからの報告 ----

    def set_not_connectable(self, exc: Optional[BaseException] = None, report: bool = True) -> None:
        """
        not_connectable への移行を記録する（既に not_connectable なら何もしない）
        状態が変わったときだけ、ログファイル・メトリクスに出し、ErrorIF（phase=loop / category=comm）へ通知する
        report=False は ErrorIF へ通知しない（起動時の接続失敗を、トリガーが phase=startup で通知済みの場合など）
        """
        with self._lock:
            if self.state == NOT_CONNECTABLE:
                return
            self.state                 = NOT_CONNECTABLE
            self.not_connectable_since = dt.now().strftime("%Y/%m/%d %H:%M:%S")
            self.reconnect_fail_count  = 0
            self.total_not_connectable += 1
            self._last_reconnect_try   = time.time()
        _detail = f"  ({type(exc).__name__}: {exc})" if exc is not None else ""
        base.metrics_set_last(self.module_name, "connect_state", NOT_CONNECTABLE)
        base.metrics_set_last(self.module_name, "not_connectable_since", self.not_connectable_since)
        base.metrics_set_last(self.module_name, "reconnect_fail_count", 0)
        base.metrics_add(self.module_name, "total_not_connectable", 1)
        base.output_log(self.module_name,
            f"WARNING  {self.trigger_name} Not Connectable. {self.target}{_detail}", self.log_file)
        if report:
            ErrorIF.report_event({
                "trigger_id": self.trigger_name,
                "phase"     : "loop",
                "category"  : "comm",
                "exc_type"  : type(exc).__name__ if exc is not None else "NotConnectable",
                "message"   : f"Not Connectable. {self.target}{_detail}",
                "signature" : self.signature,
            })

    def should_try_reconnect(self) -> bool:
        """
        not_connectable で、前回の試行から reconnect_interval_sec 以上たったか。真を返すときに試行の時刻を記録する
        （真なら、トリガーが再接続を試み、結果を set_connectable / reconnect_failed で報告する）
        """
        with self._lock:
            if self.state != NOT_CONNECTABLE:
                return False
            now = time.time()
            if now - self._last_reconnect_try < self.reconnect_interval_sec:
                return False
            self._last_reconnect_try = now
            return True

    def reconnect_failed(self, exc: Optional[BaseException] = None) -> None:
        """
        再接続の失敗を記録する（not_connectable のまま）。回数をメトリクスに出し、ErrorIF へ通知する
        （ログファイルへの出力は ErrorIF が行う。頻発すればポリシーで抑制される）
        """
        with self._lock:
            self.reconnect_fail_count += 1
            n = self.reconnect_fail_count
        base.metrics_set_last(self.module_name, "reconnect_fail_count", n)
        ErrorIF.report_event({
            "trigger_id": self.trigger_name,
            "phase"     : "loop",
            "category"  : "comm",
            "exc_type"  : type(exc).__name__ if exc is not None else "ReconnectFailed",
            "message"   : f"Re-Connect Failed ({n} times). {self.target}",
            "signature" : self.signature,
        })

    def set_connectable(self) -> None:
        """connectable への復帰を記録する（既に connectable なら何もしない）。状態が変わったときだけログファイル・メトリクスに出す"""
        with self._lock:
            if self.state == CONNECTABLE:
                return
            since = self.not_connectable_since
            n     = self.reconnect_fail_count
            self.state                 = CONNECTABLE
            self.not_connectable_since = None
            self.reconnect_fail_count  = 0
        base.metrics_set_last(self.module_name, "connect_state", CONNECTABLE)
        base.metrics_set_last(self.module_name, "not_connectable_since", "-")
        base.metrics_set_last(self.module_name, "reconnect_fail_count", 0)
        base.output_log(self.module_name,
            f"{self.trigger_name} Connectable. {self.target}  (not connectable since {since}, re-connect failed {n} times)",
            self.log_file)

    # ---- 状態の問い合わせ ----

    def is_connectable(self) -> bool:
        """トリガー自身が最後に報告した状態（False なら not_connectable）"""
        with self._lock:
            return self.state == CONNECTABLE

    def snapshot(self) -> Dict[str, Any]:
        """状態の写し（get_connect_states が返す1件）"""
        with self._lock:
            return {
                "trigger_name"         : self.trigger_name,
                "target"               : self.target,
                "state"                : self.state,
                "not_connectable_since": self.not_connectable_since,
                "reconnect_fail_count" : self.reconnect_fail_count,
                "total_not_connectable": self.total_not_connectable,
            }


########################################
# Query Functions（どのスレッドからでも呼べる）
########################################

def get_is_connectable(trigger_name: str, target: Optional[str] = None) -> Optional[bool]:
    """
    トリガー（と接続先）の状態を問い合わせる
        target を指定: その接続先が connectable なら True / not_connectable なら False
        target を省略: そのトリガーの接続先がすべて connectable なら True / 1つでも not_connectable なら False
        登録が無い（そのトリガーが ConnectState を作っていない）場合は None
    Mod: 2026.11.06    By: IWATA, Y.'s Agent AI
    """
    with _registry_lock:
        states = [s for (t, tgt), s in _registry.items()
                  if t == trigger_name and (target is None or tgt == target)]
    if not states:
        return None
    return all(s.is_connectable() for s in states)


def get_connect_states() -> List[Dict[str, Any]]:
    """
    全トリガーの状態の一覧（ConnectState.snapshot のリスト。トリガー名・接続先の順）
    Mod: 2026.11.06    By: IWATA, Y.'s Agent AI
    """
    with _registry_lock:
        states = [_registry[k] for k in sorted(_registry.keys())]
    return [s.snapshot() for s in states]


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, "Run Standalone.")
