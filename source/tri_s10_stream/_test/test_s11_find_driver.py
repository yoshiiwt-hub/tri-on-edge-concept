# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Single Run Test [Find Trigger]
# Ver: 01.a.20261012    By: IWATA, Y.'s Agent AI
# Mod: 新規作成（find を単体で動かす。既定は内蔵の設定（demo random）、環境変数・引数で設定ファイルを渡せる）
########################################
find（入力）を単体で動かし、キューへレコードが届くことを確かめるテスト

    既定        : 内蔵の設定（列定義 + f_demo_random_trigger）で動かす。settings/ は読まない
    設定を渡す  : trigger_setting_01 形式の JSON（column と find_trigger）を渡すと、その find で動かす
                  ・環境変数  TRI_TEST_FIND_SETTING=<設定ファイルのパス>
                  ・直接実行  python _test/test_s11_find_driver.py --setting <設定ファイルのパス>
                  ※ 渡した設定のとおりに動く（本番の設定なら、本番の接続先へつなぐ）

    確認すること
      1. find が N 件（N_RECORDS）のレコードをキューへ送ること
      2. 各レコードの列数が、列定義の列数と同じこと
      3. find のスレッドで未処理の例外が出ず、find が自分で停止していないこと
      4. 起動時のエラー通知（ErrorIF の phase="startup"）が出ていないこと（内蔵の設定では、通知が1件も出ないこと）

実行: tri_s10_stream/ で  python -m unittest discover -s _test -v
      または              python -m unittest _test.test_s11_find_driver -v
      または              python _test/test_s11_find_driver.py [--setting <設定ファイルのパス>]
"""
import importlib
import os
import queue
import sys
import threading
import time
import unittest
from   datetime import datetime as dt
from   typing   import Any, Callable, Dict, List, Optional, Tuple
from   unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # tri_s10_stream/（src の親）
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import src.tri_s00_supervise_base as base
from   src.tri_s01_error_if import ErrorIF


ENV_FIND_SETTING = "TRI_TEST_FIND_SETTING"
N_RECORDS        = 3          # 受け取るレコード数
TIMEOUT_SEC      = 30.0       # レコードを待つ上限（秒）

# 内蔵の設定（列定義と find）
DEFAULT_COL_DEF: Dict[int, List[str]] = {
    0: ["ts",  "time",  "key", "key", "Timestamp"],
    1: ["lot", "str",   "cat", "exp", "State"],
    2: ["val", "float", "num", "obj", "Value"],
    3: ["cnt", "int",   "num", "obj", "Count"],
}
DEFAULT_FIND: Dict[str, Any] = {
    "module": "src.tri_s11_find_demo",
    "func"  : "f_demo_random_trigger",
    "int_range"  : [0, 10],
    "float_range": [0, 100],
    "str_list"   : ["RUN", "IDLE"],
}


def read_setting(path: str) -> Dict[str, Any]:
    """設定ファイルを、実際の設定ファイルと同じ方法で読む（文字コード判定 → コメント（"##" 始まりのキー）除去）"""
    raw, _enc = base.read_json_safely(path)
    return base.clean_dict_comment(raw)


def load_find_setting() -> Tuple[Dict[int, Any], Dict[str, Any], Optional[str]]:
    """
    find の列定義と設定を返す（環境変数 TRI_TEST_FIND_SETTING があればその設定、なければ内蔵の設定）
    Returns: (列定義, find_trigger の設定, 設定ファイルのパス（内蔵の設定なら None）)
    """
    path = os.environ.get(ENV_FIND_SETTING, "").strip()
    if not path:
        return dict(DEFAULT_COL_DEF), dict(DEFAULT_FIND), None
    cfg = read_setting(path)
    col_def = {int(k): v for k, v in cfg["column"].items()}
    return col_def, dict(cfg["find_trigger"]), path


def resolve_trigger(trigger_config: Dict[str, Any]) -> Callable:
    """設定の module / func から、トリガー関数を取り出す"""
    return getattr(importlib.import_module(trigger_config["module"]), trigger_config["func"])


def run_find(func: Callable, col_def: Dict[int, Any], config: Dict[str, Any],
             n_records: int, timeout_sec: float) -> Dict[str, Any]:
    """find をスレッドで起動し、n_records 件を受け取ったら止める（timeout_sec で打ち切り）"""
    out_queue  = queue.Queue(maxsize=10000)
    stop_event = threading.Event()
    errors: List[BaseException] = []

    def _run() -> None:
        try:
            func(out_queue, col_def, config, stop_event)
        except BaseException as e:      # スレッド内の未処理例外をテスト側で確認できるようにする
            errors.append(e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    records: List[Tuple[int, list]] = []
    deadline = time.time() + timeout_sec
    while len(records) < n_records and time.time() < deadline and not stop_event.is_set():
        try:
            records.append(out_queue.get(timeout=0.1))
        except queue.Empty:
            continue
    self_stopped = stop_event.is_set()      # find が自分で止めた（起動時のエラー等）
    stop_event.set()
    t.join(timeout=5)
    return {"records": records, "errors": errors, "self_stopped": self_stopped}


class TestFindDriver(unittest.TestCase):

    def setUp(self) -> None:
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        ErrorIF.init()
        self.col_def, self.config, self.setting_path = load_find_setting()

    def test_find_puts_records(self):
        func = resolve_trigger(self.config)
        res = run_find(func, self.col_def, self.config, N_RECORDS, TIMEOUT_SEC)
        events = ErrorIF.drain_events()
        label = self.setting_path or "built-in setting"

        self.assertEqual(res["errors"], [], f"unhandled exception in find: {res['errors']}")
        self.assertFalse(res["self_stopped"], f"find stopped by itself ({label}). events: {events}")
        self.assertEqual([e for e in events if e.get("phase") == "startup"], [], label)
        self.assertGreaterEqual(len(res["records"]), N_RECORDS, f"records: {len(res['records'])} ({label})")
        for index, value in res["records"]:
            self.assertEqual(len(value), len(self.col_def), f"index={index} value={value}")
        if self.setting_path is None:
            self.assertEqual(events, [])                                     # 内蔵の設定では通知は出ない

    def test_builtin_random_values_follow_dtype(self):
        """内蔵の設定（demo random）の値が列の型どおりか（設定を渡したときも、内蔵の設定で確かめる）"""
        res = run_find(resolve_trigger(DEFAULT_FIND), DEFAULT_COL_DEF, DEFAULT_FIND, N_RECORDS, TIMEOUT_SEC)
        self.assertGreaterEqual(len(res["records"]), N_RECORDS)
        for _, (ts, lot, val, cnt) in res["records"]:
            self.assertIsInstance(ts, dt)
            self.assertIn(lot, DEFAULT_FIND["str_list"])
            self.assertTrue(0 <= val <= 100)
            self.assertTrue(0 <= cnt <= 10)


def _apply_cli_args(argv: List[str]) -> List[str]:
    """--setting <パス> を環境変数に移し、残りの引数を unittest に渡す"""
    rest = [argv[0]]
    i = 1
    while i < len(argv):
        if argv[i] == "--setting" and i + 1 < len(argv):
            os.environ[ENV_FIND_SETTING] = argv[i + 1]
            i += 2
            continue
        rest.append(argv[i])
        i += 1
    return rest


if __name__ == "__main__":
    unittest.main(argv=_apply_cli_args(sys.argv), verbosity=2)
