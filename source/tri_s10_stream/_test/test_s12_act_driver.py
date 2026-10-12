# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Single Run Test [Act Trigger]
# Ver: 01.a.20261012    By: IWATA, Y.'s Agent AI
# Mod: 新規作成（act を単体で動かす。既定は内蔵の設定（imq テンプレート）、環境変数・引数で設定ファイルを渡せる）
########################################
act（出力）を単体で動かし、セグメントを処理し終えることを確かめるテスト
    列定義から作ったセグメント（N_SEGMENTS 件）を、act のキューへ直接渡す（find・波形切出しは使わない）

    既定        : 内蔵の設定（列定義 + a_template_imq_trigger）で動かす。settings/ は読まない
    設定を渡す  : 環境変数か引数で、設定ファイルを渡すと、その設定で動かす
                  ・act の設定（trigger_setting_02 形式。act_trigger と segment_header）
                      環境変数  TRI_TEST_ACT_SETTING=<パス>   /  直接実行  --setting <パス>
                  ・列定義（trigger_setting_01 形式の column。act の列定義は「セグメントの先頭4列 + find の列」）
                      環境変数  TRI_TEST_FIND_SETTING=<パス>  /  直接実行  --find-setting <パス>
                  ※ 渡した設定のとおりに動く（本番の設定なら、本番の出力先へ出す）
                  ※ ストレージキュー（STQ）の act は対象外（スキップする）

    確認すること
      1. act のスレッドで未処理の例外が出ず、act が自分で停止していないこと
      2. 終了の合図（None）を渡したあと、act が時間内に処理を終えること
      3. 起動時のエラー通知（ErrorIF の phase="startup"）が出ていないこと
         （内蔵の設定では、通知が1件も出ず、処理したセグメント数がメトリクスに反映されること）

実行: tri_s10_stream/ で  python -m unittest discover -s _test -v
      または              python -m unittest _test.test_s12_act_driver -v
      または              python _test/test_s12_act_driver.py [--setting <パス>] [--find-setting <パス>]
"""
import os
import queue
import sys
import threading
import unittest
from   datetime import datetime as dt
from   typing   import Any, Callable, Dict, List, Optional, Tuple
from   unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # tri_s10_stream/（src の親）
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
TEST_DIR = os.path.dirname(os.path.abspath(__file__))                # _test/（test_s11_find_driver を読み込むため。
if TEST_DIR not in sys.path:                                         #  パッケージとして実行したときも読めるようにする）
    sys.path.insert(0, TEST_DIR)

import src.tri_s00_supervise_base as base
from   src.tri_s01_error_if import ErrorIF

# 列定義・設定ファイルの読み方は find の単体テストと同じものを使う（関数だけを読み込む）
from   test_s11_find_driver import ENV_FIND_SETTING, load_find_setting, read_setting, resolve_trigger


ENV_ACT_SETTING = "TRI_TEST_ACT_SETTING"
N_SEGMENTS      = 3           # act へ渡すセグメント数
N_ROWS          = 5           # 1セグメントの行数
TIMEOUT_SEC     = 30.0        # act が処理を終えるのを待つ上限（秒）

# 内蔵の設定（act）とセグメントの先頭4列
DEFAULT_ACT: Dict[str, Any] = {
    "module": "src.tri_s12_act_tmpl_queue",
    "func"  : "a_template_imq_trigger",
    "teams_notify": False,
}
DEFAULT_SEGMENT_HEADER: Dict[int, List[str]] = {
    0: ["seg_id",     "int",  "key", "key", "Segment ID"],
    1: ["seg_ind",    "int",  "key", "key", "Segment Row Index"],
    2: ["seg_ts",     "time", "key", "key", "Timestamp When Data Collected"],
    3: ["seg_status", "int",  "key", "key", "Segment Process Status"],
}


def load_act_setting() -> Tuple[Dict[str, Any], Dict[int, Any], Optional[str]]:
    """
    act の設定とセグメントの先頭4列を返す（環境変数 TRI_TEST_ACT_SETTING があればその設定、なければ内蔵の設定）
    Returns: (act_trigger の設定, セグメントの先頭列の定義, 設定ファイルのパス（内蔵の設定なら None）)
    """
    path = os.environ.get(ENV_ACT_SETTING, "").strip()
    if not path:
        return dict(DEFAULT_ACT), dict(DEFAULT_SEGMENT_HEADER), None
    cfg = read_setting(path)
    header = cfg.get("segment_header")
    header = {int(k): v for k, v in header.items()} if header else dict(DEFAULT_SEGMENT_HEADER)
    return dict(cfg["act_trigger"]), header, path


def make_act_col_def(header: Dict[int, Any], col_def_find: Dict[int, Any]) -> Dict[int, Any]:
    """セグメントの先頭列の後ろに find の列を付けて、act 側の列定義にする"""
    n = len(header)
    return {**header, **{int(k) + n: v for k, v in col_def_find.items()}}


def _value_for(dtype: str, i: int) -> Any:
    """列の型に合わせた値を作る（行番号 i ごとに変わる値）"""
    if dtype == "time":  return dt.now()
    if dtype == "int":   return i
    if dtype == "float": return i * 1.5
    if dtype == "str":   return "RUN"
    return None


def make_segments(col_def_find: Dict[int, Any], n_header: int, n_segments: int, n_rows: int) -> List[List[list]]:
    """列定義から、act へ渡すセグメントを作る（行: [seg_id, seg_ind, seg_ts, seg_status] + find の列）"""
    dtypes = [col_def_find[k][1] for k in sorted(col_def_find.keys())]
    segments = []
    for s in range(n_segments):
        seg_id = int(dt.now().strftime("%Y%m%d%H%M%S")) * 100 + s
        rows = []
        for i in range(n_rows):
            head = [seg_id, i + 1, dt.now(), 0] + [None] * max(0, n_header - 4)
            rows.append(head[:n_header] + [_value_for(d, i) for d in dtypes])
        segments.append(rows)
    return segments


def run_act(func: Callable, col_def_act: Dict[int, Any], config: Dict[str, Any],
            segments: List[List[list]], timeout_sec: float) -> Dict[str, Any]:
    """act をスレッドで起動し、セグメントと終了の合図（None）を渡して、処理を終えるまで待つ"""
    in_queue   = queue.Queue(maxsize=10000)
    stop_event = threading.Event()
    errors: List[BaseException] = []

    def _run() -> None:
        try:
            func(in_queue, col_def_act, config, stop_event)
        except BaseException as e:      # スレッド内の未処理例外をテスト側で確認できるようにする
            errors.append(e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    for seg in segments:
        in_queue.put(seg)
    in_queue.put(None)                  # 終了の合図（先に積んだセグメントを処理してから止まる）
    t.join(timeout=timeout_sec)
    finished = not t.is_alive()
    self_stopped = stop_event.is_set()  # act が自分で止めた（起動時のエラー等）
    stop_event.set()
    t.join(timeout=5)
    return {"errors": errors, "finished": finished, "self_stopped": self_stopped}


class TestActDriver(unittest.TestCase):

    def setUp(self) -> None:
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        ErrorIF.init()
        self.col_def_find, _find_cfg, self.find_path = load_find_setting()
        self.config, self.header, self.act_path = load_act_setting()

    def test_act_processes_segments(self):
        func = resolve_trigger(self.config)
        if str(getattr(func, "QMODE", "imq")).lower() == "stq":
            self.skipTest("storage queue (STQ) act is not supported by this test")
        label = f"act={self.act_path or 'built-in'}  column={self.find_path or 'built-in'}"

        col_def_act = make_act_col_def(self.header, self.col_def_find)
        segments = make_segments(self.col_def_find, len(self.header), N_SEGMENTS, N_ROWS)
        before = base.metrics_get_all().get("ACT_Queue", {}).get("total_segments", 0)

        res = run_act(func, col_def_act, self.config, segments, TIMEOUT_SEC)
        events = ErrorIF.drain_events()

        self.assertEqual(res["errors"], [], f"unhandled exception in act: {res['errors']}")
        self.assertFalse(res["self_stopped"], f"act stopped by itself ({label}). events: {events}")
        self.assertTrue(res["finished"], f"act did not finish within {TIMEOUT_SEC} sec ({label})")
        self.assertEqual([e for e in events if e.get("phase") == "startup"], [], label)
        if self.act_path is None:
            self.assertEqual(events, [])                                     # 内蔵の設定では通知は出ない
            m = base.metrics_get_all()["ACT_Queue"]
            self.assertEqual(m["total_segments"] - before, N_SEGMENTS)       # act がセグメントを処理した


def _apply_cli_args(argv: List[str]) -> List[str]:
    """--setting / --find-setting <パス> を環境変数に移し、残りの引数を unittest に渡す"""
    opts = {"--setting": ENV_ACT_SETTING, "--find-setting": ENV_FIND_SETTING}
    rest = [argv[0]]
    i = 1
    while i < len(argv):
        if argv[i] in opts and i + 1 < len(argv):
            os.environ[opts[argv[i]]] = argv[i + 1]
            i += 2
            continue
        rest.append(argv[i])
        i += 1
    return rest


if __name__ == "__main__":
    unittest.main(argv=_apply_cli_args(sys.argv), verbosity=2)
