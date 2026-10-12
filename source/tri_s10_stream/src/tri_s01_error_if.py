# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Error Interface Module [Minimal / Receive-only]
# Ver: 01.a.20261012    By: IWATA, Y.'s Agent AI
# Mod: 新規作成。find / act が呼ぶエラー通知の窓口（インターフェース）だけを持つ最小実装
########################################
SIGNAL_CONTINUE        # 通知後も処理を継続してよい
SIGNAL_STOP            # 呼び出し側に停止を求める（この最小実装は返さない）
TriOnEdgeDataError     # データ（プロトコル）異常を表す例外
ErrorIF                # エラー通知の窓口（クラスメソッドのみ。状態はプロセス内で共有）
    init                    # 状態を初期化する（起動時に1回）
    report_exception        # 例外を受け取って記録し、シグナルを返す
    report_event            # 辞書形式のイベントを受け取って記録し、シグナルを返す
    drain_events            # 未回収のイベントをすべて取り出す（監督側が呼ぶ）
    apply_policy            # イベントに適用するルールを返す（この最小実装は常に空）
    is_frequent             # 同じ signature が指定時間内に指定回数以上発生したか
    format_supervisor_log   # 監督ログの1行を整形する
    can_escalate            # 同じ signature を、まだエスカレーションしていないか
    mark_escalated          # エスカレーション済みにする
    enter_suppression       # 同じ signature のログ出力を抑制する
    should_suppress         # 抑制中か
    emit_summary_if_needed  # 抑制した件数のサマリを一定間隔でログに出す
    shutdown                # 終了処理（サマリを出して状態を破棄する）

動作概要
    1. find / act が起こしたエラーを受け取り、ログに残して、イベントとして保持するだけの最小実装
       （ポリシーによる判定・エスカレーション・プロセス停止などの「判断」は行わない）
    2. 返すシグナルは常に SIGNAL_CONTINUE。呼び出し側は自分で stop_event を扱う
    3. 必要になったら、同じ関数名・引数のまま、判断を持つ実装に差し替えられる
       （差し替え先は呼び出し側のコードを変えずに済むことを目標にした、窓口の約束事）
    4. signature は "{exc_type}.{category}.{phase}.{trigger_id}"（例: ConnectionFailed.comm.loop.f_xxx_trigger）
    5. イベントは辞書: time / trigger_id / phase / category / exc_type / message / signature / log_file
       phase    : startup / loop / shutdown
       category : config / data / io / comm / internal
"""
MODULE_NAME = "ERROR_IF"

"""standard library"""
from   collections import deque
from   datetime import datetime as dt
import threading
import time
import traceback
from   typing   import Any, Deque, Dict, List, Optional, Set

"""local module（無くても単体で動く）"""
try:
    import src.tri_s00_supervise_base as base
except ModuleNotFoundError:
    base = None


########################################
# Signals / Exceptions
########################################
SIGNAL_CONTINUE = "CONTINUE"
SIGNAL_STOP     = "STOP"


class TriOnEdgeDataError(Exception):
    """データ（プロトコル）の異常。通信断ではなく、受け取った内容がおかしい場合に使う"""


# 旧名の別名（名称変更前のモジュールが残っている間だけの互換用。置き換えが済んだら削除する）
TriEdgeDataError = TriOnEdgeDataError


########################################
# Error Interface
########################################
class ErrorIF:
    """
    エラー通知の窓口（受け取り専用の最小実装）
    Mod: 2026.10.12    By: IWATA, Y.'s Agent AI
    """
    EVENTS_MAX           = 1000    # 未回収イベントの保持上限（超えた古いものは捨てる）
    HISTORY_MAX          = 1000    # signature ごとの発生時刻の保持上限
    SUMMARY_INTERVAL_SEC = 60.0    # 抑制サマリの出力間隔

    _lock = threading.RLock()
    _events:      Deque[Dict[str, Any]]    = deque(maxlen=EVENTS_MAX)
    _history:     Dict[str, Deque[float]]  = {}
    _counts:      Dict[str, int]           = {}
    _escalated:   Set[str]                 = set()
    _suppressed:  Set[str]                 = set()
    _suppressed_hits: int                  = 0
    _last_summary: float                   = time.time()
    _log_file:    str                      = ""
    _policy_path: str                      = ""

    ####################
    # lifecycle
    ####################
    @classmethod
    def init(cls, policy_path: str = "", log_file: str = "") -> None:
        """状態を初期化する。policy_path は受け取るだけで読まない（この最小実装はポリシーを持たない）"""
        with cls._lock:
            cls._events           = deque(maxlen=cls.EVENTS_MAX)
            cls._history          = {}
            cls._counts           = {}
            cls._escalated        = set()
            cls._suppressed       = set()
            cls._suppressed_hits  = 0
            cls._last_summary     = time.time()
            cls._log_file         = log_file or ""
            cls._policy_path      = policy_path or ""
        return None

    @classmethod
    def shutdown(cls) -> None:
        """終了処理。抑制していた件数があればサマリを出し、状態を破棄する"""
        cls.emit_summary_if_needed(force=True)
        with cls._lock:
            cls._events.clear()
            cls._history.clear()
            cls._escalated.clear()
            cls._suppressed.clear()
        return None

    ####################
    # report
    ####################
    @staticmethod
    def make_signature(ev: Dict[str, Any]) -> str:
        """イベントから signature を作る"""
        return f"{ev.get('exc_type', 'Error')}.{ev.get('category', '-')}.{ev.get('phase', '-')}.{ev.get('trigger_id', '-')}"

    @classmethod
    def report_event(cls, event: Dict[str, Any]) -> str:
        """辞書形式のイベントを受け取り、記録して SIGNAL_CONTINUE を返す"""
        ev = dict(event or {})
        ev.setdefault("time", dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        for key, default in (("trigger_id", "-"), ("phase", "-"), ("category", "-"),
                             ("exc_type", "Error"), ("message", "")):
            ev.setdefault(key, default)
        if not ev.get("signature"):
            ev["signature"] = cls.make_signature(ev)
        sig = ev["signature"]

        with cls._lock:
            cls._events.append(ev)
            cls._counts[sig] = cls._counts.get(sig, 0) + 1
            cls._history.setdefault(sig, deque(maxlen=cls.HISTORY_MAX)).append(time.time())
            suppressed = sig in cls._suppressed
            if suppressed:
                cls._suppressed_hits += 1

        if not suppressed:
            msg = f"ERROR  {sig}  {ev['message']}"
            if ev.get("traceback"):
                msg += f"\n{ev['traceback']}"
            cls._emit(msg, ev.get("log_file", ""))
        return SIGNAL_CONTINUE

    @classmethod
    def report_exception(cls, exc: BaseException, phase: str = "loop", category: str = "internal",
                         trigger_id: str = "-", log_file: str = "", message: Optional[str] = None,
                         **_ignored: Any) -> str:
        """例外を受け取り、記録して SIGNAL_CONTINUE を返す"""
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        return cls.report_event({
            "trigger_id": trigger_id,
            "phase"     : phase,
            "category"  : category,
            "exc_type"  : type(exc).__name__,
            "message"   : message if message else (str(exc) or repr(exc)),
            "traceback" : tb,
            "log_file"  : log_file,
        })

    ####################
    # supervise（監督側が呼ぶ。この最小実装は「判断」をしない）
    ####################
    @classmethod
    def drain_events(cls) -> List[Dict[str, Any]]:
        """未回収のイベントをすべて取り出す（古い順）"""
        with cls._lock:
            events = list(cls._events)
            cls._events.clear()
        return events

    @classmethod
    def apply_policy(cls, ev: Dict[str, Any]) -> Dict[str, Any]:
        """イベントに適用するルールを返す。ポリシーを持たないため常に空（何もしない）"""
        return {}

    @classmethod
    def is_frequent(cls, signature: str, window_sec: float = 60, count: int = 5) -> bool:
        """同じ signature が window_sec 秒以内に count 回以上発生したか"""
        border = time.time() - float(window_sec)
        with cls._lock:
            hist = cls._history.get(signature, ())
            return sum(1 for t in hist if t >= border) >= int(count)

    @classmethod
    def format_supervisor_log(cls, ev: Dict[str, Any], rule: Dict[str, Any], frequent: bool = False,
                              window_sec: float = 60, count: int = 5) -> str:
        """監督ログの1行を整形する"""
        return (f"[SUPERVISOR] signature={ev.get('signature')}  phase={ev.get('phase')}  "
                f"category={ev.get('category')}  frequent={frequent}"
                f"({count} times / {window_sec} sec)  message={ev.get('message')}")

    @classmethod
    def can_escalate(cls, signature: str) -> bool:
        """まだエスカレーションしていない signature か"""
        with cls._lock:
            return signature not in cls._escalated

    @classmethod
    def mark_escalated(cls, signature: str) -> None:
        with cls._lock:
            cls._escalated.add(signature)
        return None

    @classmethod
    def enter_suppression(cls, signature: str) -> None:
        """この signature のログ出力を抑制する（件数は数え続ける）"""
        with cls._lock:
            cls._suppressed.add(signature)
        return None

    @classmethod
    def should_suppress(cls, signature: str) -> bool:
        with cls._lock:
            return signature in cls._suppressed

    @classmethod
    def emit_summary_if_needed(cls, force: bool = False) -> None:
        """抑制した件数があれば、一定間隔（または force 時）にサマリを1行ログに出す"""
        with cls._lock:
            due  = force or (time.time() - cls._last_summary >= cls.SUMMARY_INTERVAL_SEC)
            hits = cls._suppressed_hits
            if not due or hits <= 0:
                return None
            sigs = sorted(cls._suppressed)
            cls._suppressed_hits = 0
            cls._last_summary    = time.time()
        cls._emit(f"WARNING  Suppressed {hits} error log(s). signatures={sigs}", "")
        return None

    ####################
    # utility
    ####################
    @classmethod
    def get_counts(cls) -> Dict[str, int]:
        """signature ごとの累計発生件数のコピーを返す（確認・テスト用）"""
        with cls._lock:
            return dict(cls._counts)

    @classmethod
    def _emit(cls, msg: str, log_file: str = "") -> None:
        """ログ出力。base があれば base.output_log を使い、なければ標準出力"""
        target = log_file or cls._log_file
        if base is not None:
            base.output_log(MODULE_NAME, msg, target)
        else:
            print(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  {msg}", flush=True)
        return None


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    ErrorIF.init()
    try:
        raise ValueError("sample error")
    except ValueError as e:
        sig = ErrorIF.report_exception(e, phase="loop", category="data", trigger_id="sample_trigger")
    print(f"signal={sig}  counts={ErrorIF.get_counts()}")
    ErrorIF.shutdown()
