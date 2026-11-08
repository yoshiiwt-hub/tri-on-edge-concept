# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Single Run Test [Act Trigger: Socket Send - Log Output]
# Ver: 01.b.20261106    By: IWATA, Y.'s Agent AI
# Mod: act_socket の設定の変更（skip_on_offline → not_connectable_action）に合わせた
#      新規作成（正常に送ったセグメントのログは画面のみ、ACK NG で中断したものはログファイルにも残す）
########################################
a_send_seg_trigger（ack_mode=true）を、localhost の空きポートで動かした小さなソケットサーバへつなぎ、
base.output_log の呼び出しを記録して、ログファイルに書くか（log_file を渡すか）を確かめるテスト

    確認すること
      1. 正常に送ったセグメントの「Segment Sent」は、画面のみ（log_file を渡さない）
      2. ACK NG で中断したセグメントの「Aborted (ACK NG)」は、ログファイルにも残す（log_file を渡す）

実行: tri_s10_stream/ で  python -m unittest _test.test_s12_act_socket_log -v
"""
import os
import queue
import socket
import sys
import threading
import unittest
from   datetime import datetime as dt
from   typing   import Any, Dict, List, Tuple
from   unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # tri_s10_stream/（src の親）
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import src.tri_s00_supervise_base as base
from   src.tri_s01_error_if import ErrorIF

HEADER = list(base.SEG_HEADER_COLS)   # Act に渡る行の先頭4列
COL_DEF: Dict[int, Any] = {i: [name, "str", "key", "key", ""] for i, name in enumerate(HEADER)}
COL_DEF.update({4: ["c0", "int", "num", "obj", ""], 5: ["c1", "str", "cat", "exp", ""]})


class _AckServer:
    """PUT を受けるたびに、決めた ACK（OK / NG）を返す小さなソケットサーバ"""

    def __init__(self, reply: str) -> None:
        self.reply = reply
        self.srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind(("127.0.0.1", 0))
        self.srv.listen(1)
        self.srv.settimeout(0.2)
        self.port = self.srv.getsockname()[1]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self.srv.accept()
            except socket.timeout:
                continue
            conn.settimeout(0.2)
            buf = ""
            with conn:
                while not self._stop.is_set():
                    try:
                        data = conn.recv(1024)
                    except socket.timeout:
                        continue
                    if not data:
                        break
                    buf += data.decode("utf-8")
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        if line != "PUT":                         # PUT の次の行（データ）ごとに ACK を返す
                            conn.sendall((self.reply + "\n").encode("utf-8"))

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self.srv.close()


class TestActSocketLog(unittest.TestCase):

    def setUp(self) -> None:
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        ErrorIF.init()

    def _run(self, reply: str, **overrides: Any) -> List[Tuple[str, str]]:
        """サーバ（ACK=reply）へ1セグメント送り、output_log の (メッセージ, log_file) を返す"""
        from src.tri_s12_act_socket import a_send_seg_trigger
        server = _AckServer(reply)
        self.addCleanup(server.close)
        config = {"server_ip": "127.0.0.1", "server_port": server.port, "ack_mode": True,
                  "not_connectable_action": "wait", "reconnect_interval_sec": 0.2, "timeout_sec": 5.0}
        config.update(overrides)
        logs: List[Tuple[str, str]] = []
        orig = base.output_log

        def _spy(tag, msg, log_file=""):
            logs.append((str(msg), log_file))
            return orig(tag, msg, log_file)

        in_queue   = queue.Queue()
        stop_event = threading.Event()
        segment = [[1, i + 1, dt.now(), 0, i, "aaa"] for i in range(2)]
        with mock.patch.object(base, "output_log", _spy):
            t = threading.Thread(target=a_send_seg_trigger, args=(in_queue, COL_DEF, config, stop_event), daemon=True)
            t.start()
            in_queue.put(segment)
            in_queue.put(None)
            t.join(timeout=20)
        stop_event.set()
        self.assertFalse(t.is_alive())
        return logs

    def test_sent_segment_is_screen_only(self):
        logs = self._run("OK")
        sent = [(m, f) for m, f in logs if "Segment Sent." in m]
        self.assertEqual(len(sent), 1, logs)
        self.assertFalse(sent[0][1], sent)            # log_file を渡さない（画面のみ）

    def test_aborted_segment_is_written_to_log_file(self):
        logs = self._run("NG", ack_ng_action="abort_segment")
        aborted = [(m, f) for m, f in logs if "Aborted (ACK NG)." in m]
        self.assertEqual(len(aborted), 1, logs)
        self.assertTrue(aborted[0][1], aborted)       # log_file を渡す（ログファイルにも残す）
        self.assertFalse([m for m, _ in logs if "Segment Sent." in m], logs)


if __name__ == "__main__":
    unittest.main(verbosity=2)
