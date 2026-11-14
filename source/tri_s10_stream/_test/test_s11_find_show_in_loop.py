# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Single Run Test [Find Trigger: show_in_loop]
# Ver: 01.a.20261114    By: IWATA,Y.'s Agent AI
# Mod: 新規作成（毎ループの画面表示を、設定 show_in_loop で出すかどうか選べること）
########################################
Find トリガーの毎ループの画面表示が、設定 show_in_loop（省略時 false）で切り替わることを確かめるテスト
相手（PLC・エージェント・ソケットサーバ）は、test_s01_connect_state の偽物・小さなサーバを使う

    確認すること（トリガーごとに）
      1. show_in_loop を書かない（既定 false）と、毎ループの表示が出ないこと
      2. show_in_loop: true で、毎ループの表示が出ること
      3. 起動時のログに show_in_loop の値が出ること
      4. alive の VERBOSE（機器ごとの行）は、show_in_loop とは別に効くこと
    対象: f_demo_random_trigger / f_demo_csv_trigger / f_agentic_trigger / f_find_slmp_trigger /
          f_check_alive_trigger（要約の行。VERBOSE の機器ごとの行は別）/ f_socket_client_trigger

実行: tri_s10_stream/ で  python -m unittest _test.test_s11_find_show_in_loop -v
"""
import io
import os
import queue
import socket
import sys
import threading
import unittest
from   typing   import Any, Callable, Dict, List, Tuple
from   unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # tri_s10_stream/（src の親）
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import src.tri_s00_supervise_base as base
from   src.tri_s01_error_if import ErrorIF
from   _test.test_s01_connect_state import (   # 共通の部品（テストのクラスは import しない。import すると2回動くため）
    FIND_COL_DEF, _AgentCase, _FakePlc, _LineServer, _free_port, wait_until)


def run_trigger(func: Callable, col_def: Dict[int, Any], config: Dict[str, Any],
                n_rows: int, timeout: float = 20.0) -> Tuple[bool, List[str], str]:
    """
    トリガーを動かし、キューに n_rows 件たまったら止める
    Returns: (n_rows 件たまったか, base.output_log に渡したメッセージ, sys.stdout に書いた文字列)
    """
    msgs: List[str] = []
    out = io.StringIO()

    def _spy(tag: str, msg: str, log_file: str = "") -> None:
        msgs.append(str(msg))

    with mock.patch.object(base, "output_log", _spy), mock.patch("sys.stdout", out):
        q, stop = queue.Queue(), threading.Event()
        t = threading.Thread(target=func, args=(q, col_def, config, stop), daemon=True)
        t.start()
        ok = wait_until(lambda: q.qsize() >= n_rows, timeout)
        stop.set()
        t.join(timeout=20)
    return ok, msgs, out.getvalue()


class _ShowInLoopCase:
    """show_in_loop を省略したとき・true のときの両方を動かして比べる。これ自体はテストを持たない"""

    def _check(self, func: Callable, col_def: Dict[int, Any], config: Dict[str, Any], n_rows: int,
               shown: Callable[[List[str], str], bool]) -> None:
        ok, msgs, out = run_trigger(func, col_def, config, n_rows)
        self.assertTrue(ok, "トリガーが値を送らなかった（既定）")
        self.assertFalse(shown(msgs, out), "既定（false）で毎ループの表示が出た")
        self.assertTrue(any("show_in_loop=False" in m for m in msgs), msgs[:20])

        ok, msgs, out = run_trigger(func, col_def, {**config, "show_in_loop": True}, n_rows)
        self.assertTrue(ok, "トリガーが値を送らなかった（true）")
        self.assertTrue(shown(msgs, out), "true で毎ループの表示が出なかった")
        self.assertTrue(any("show_in_loop=True" in m for m in msgs), msgs[:20])


class TestShowInLoopDemoAliveSocketSlmp(_ShowInLoopCase, unittest.TestCase):

    def setUp(self) -> None:
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        ErrorIF.init()

    def test_demo_random(self):
        from src.tri_s11_find_demo import f_demo_random_trigger
        self._check(f_demo_random_trigger, {0: ["v", "int", "num", "obj", ""]}, {}, 2,
                    lambda msgs, out: "TEST-EQP" in out)

    def test_demo_csv(self):
        from src.tri_s11_find_demo import f_demo_csv_trigger
        col_def = {0: ["Timestamp", "time", "key", "key", ""], 1: ["LOT", "str", "key", "key", ""],
                   2: ["Sin", "float", "num", "exp", ""], 3: ["Cos", "float", "num", "exp", ""],
                   4: ["Tan", "float", "num", "exp", ""], 5: ["CL", "int", "cat", "obj", ""]}
        self._check(f_demo_csv_trigger, col_def,
                    {"file_path": "settings/data/generate_waveform_sample.csv", "interval": 0.01}, 5,
                    lambda msgs, out: "EQP:TEST-EQP" in out)

    def test_alive(self):
        from src.tri_s11_find_alive import f_check_alive_trigger
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.bind(("127.0.0.1", 0))
        srv.listen(16)
        self.addCleanup(srv.close)
        col_def = {0: ["device_key", "str", "key", "key", ""], 1: ["status", "str", "key", "key", ""]}
        config = {"targets": [{"key": "dev01", "host": "127.0.0.1", "port": srv.getsockname()[1]}],
                  "interval_sec": 0.1, "timeout_sec": 1}
        self._check(f_check_alive_trigger, col_def, config, 4,          # 2周期（1機器の行＋区切り行 ×2）
                    lambda msgs, out: any(m.startswith("Alive Checked.") for m in msgs))

    def test_alive_verbose_is_separate(self):
        from src.tri_s11_find_alive import f_check_alive_trigger
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.bind(("127.0.0.1", 0))
        srv.listen(16)
        self.addCleanup(srv.close)
        col_def = {0: ["device_key", "str", "key", "key", ""], 1: ["status", "str", "key", "key", ""]}
        config = {"targets": [{"key": "dev01", "host": "127.0.0.1", "port": srv.getsockname()[1]}],
                  "interval_sec": 0.1, "timeout_sec": 1, "VERBOSE": True}
        ok, msgs, out = run_trigger(f_check_alive_trigger, col_def, config, 4)
        self.assertTrue(ok)
        self.assertIn("dev01", out)                                     # 機器ごとの行は出る
        self.assertFalse(any(m.startswith("Alive Checked.") for m in msgs))   # 要約の行は出ない（show_in_loop 既定 false）

    def test_socket(self):
        from src.tri_s11_find_socket import f_socket_client_trigger
        port = _free_port()
        srv = _LineServer(port, "get")
        srv.start()
        self.addCleanup(srv.stop)
        config = {"server_ip": "127.0.0.1", "server_port": port, "polling_ms": 20,
                  "connect_timeout_sec": 1, "retry_max": 3, "retry_wait_sec": 0.05, "reconnect_interval_sec": 0.3}
        self._check(f_socket_client_trigger, FIND_COL_DEF, config, 3,
                    lambda msgs, out: "TEST-EQP" in out)

    def test_slmp(self):
        import src.fa_slmp.tri_s19_slmp as slmp
        from   src.fa_slmp.tri_s11_find_slmp import f_find_slmp_trigger
        p = mock.patch.object(slmp, "SlmpClient", _FakePlc)
        p.start()
        self.addCleanup(p.stop)
        _FakePlc.reset()
        config = {"plc_host": "127.0.0.1", "plc_port": 5010, "flag_mode": False, "polling_ms": 20,
                  "retry_max": 3, "retry_wait_sec": 0.05, "reconnect_interval_sec": 0.3,
                  "bulk_read": {"0": ["device", "D", 100, "word", 1]},
                  "column_register_map": {"0": {"sources": [{"bulk": "0", "offset": 0}], "unit_dtype": "int2_u"}}}
        self._check(f_find_slmp_trigger, FIND_COL_DEF, config, 3,
                    lambda msgs, out: any("Queued #" in m for m in msgs))


class TestShowInLoopAgentic(_AgentCase, _ShowInLoopCase, unittest.TestCase):

    def test_agentic(self):
        from src.tmpl_agentic.tri_s11_find_agentic import f_agentic_trigger
        self._check(f_agentic_trigger, FIND_COL_DEF, self.config, 3,
                    lambda msgs, out: "TEST-EQP" in out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
