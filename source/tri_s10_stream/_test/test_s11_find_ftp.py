# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Single Run Test [Find Trigger: FTP]
# Ver: 01.d.20261114    By: IWATA,Y.'s Agent AI
# Mod: 動かしている間に pyftpdlib の開始メッセージが繰り返し出ないことの確認を、停止のテストに追加。無通信の接続を server_timeout で切ることのテストを追加。
#      停止の指示から、トリガーと FTP サーバーのスレッドが 2 秒以内に終わり、同じポートで再び起動できることのテストを追加。
#      1ファイルごとの正常なログが画面のみ（ログファイルに書かない）ことのテストを追加。
#      新規作成（受信後のリネームで pfx_sfx に該当するようになったファイルの処理）
########################################
f_find_ftp_trigger を localhost の空きポートで起動し、標準ライブラリの ftplib でアップロード・リネームして
キューに送られるファイルを確かめるテスト（pyftpdlib が無い環境ではスキップ）

    確認すること
      1. 受信したファイルが pfx_sfx に該当すれば、キューに1回送られること（従来の動作）
      2. 受信時は該当せず、リネーム後に該当したファイルが1回だけ送られること
      3. 受信時から該当していたファイルは、該当する名前へリネームしても二重に送られないこと
      4. リネーム後も該当しなければ送られないこと
      5. 失敗したリネーム（存在しないファイル）とディレクトリのリネームでは何も送られないこと
      6. 1ファイルごとの正常なログ（File received / File renamed / not matched, skip / Queued）は画面のみで、
         ログファイルには書かないこと（base.output_log に log_file を渡さない）
      7. 停止の指示から、トリガーと FTP サーバーのスレッドが 2 秒以内に終わり、同じポートで再び起動できること。
         動かしている間に pyftpdlib の開始メッセージ（starting FTP server）が繰り返し出ないこと
      8. 無通信の接続は server_timeout の秒数でサーバーから切られること

実行: tri_s10_stream/ で  python -m unittest _test.test_s11_find_ftp -v
"""
import ftplib
import importlib.util
import io
import logging
import os
import queue
import shutil
import socket
import sys
import tempfile
import threading
import time
import unittest
from   typing   import Any, Dict, List, Tuple
from   unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # tri_s10_stream/（src の親）
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import src.tri_s00_supervise_base as base
from   src.tri_s01_error_if import ErrorIF

HAS_PYFTPDLIB = importlib.util.find_spec("pyftpdlib") is not None

FTP_USER = "tri_test"
FTP_PASS = "tri_test_pass"
COL_DEF: Dict[int, Any] = {i: [f"c{i}", "str", "key", "key", ""] for i in range(13)}
SEP = "____"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _LogCapture(logging.Handler):
    """pyftpdlib のログを集める（開始メッセージが繰り返し出ないことの確認用）"""
    def __init__(self) -> None:
        super().__init__()
        self.messages: List[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())

    def __enter__(self) -> "_LogCapture":
        logging.getLogger("pyftpdlib").addHandler(self)
        return self

    def __exit__(self, *exc: Any) -> None:
        logging.getLogger("pyftpdlib").removeHandler(self)


@unittest.skipUnless(HAS_PYFTPDLIB, "pyftpdlib is not installed")
class TestFindFtpRename(unittest.TestCase):

    def setUp(self) -> None:
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        ErrorIF.init()

        self.home_dir = tempfile.mkdtemp(prefix="tri_test_ftp_")
        self.addCleanup(shutil.rmtree, self.home_dir, True)
        self.port = _free_port()
        env = mock.patch.dict(os.environ, {"FTP_HOST": "127.0.0.1", "FTP_PORT": str(self.port),
                                           "FTP_USER": FTP_USER, "FTP_PASS": FTP_PASS})
        env.start()
        self.addCleanup(env.stop)

        from src.comm_ftp.tri_s11_find_ftp import f_find_ftp_trigger
        self.config = {
            "home_dir"      : self.home_dir,
            "passive_ports" : [61000, 61100],
            "pfx_sfx"       : [[], [".csv"]],
            "stable_check_n": 0,
            "check_ack"     : False,
        }
        self.out_queue  = queue.Queue()
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=f_find_ftp_trigger,
                                       args=(self.out_queue, COL_DEF, self.config, self.stop_event),
                                       daemon=True)
        self.thread.start()
        self.addCleanup(self._stop)
        self.ftp = self._connect()
        self.addCleanup(self._quit)

    def _connect(self) -> ftplib.FTP:
        deadline = time.time() + 10.0
        while True:
            try:
                ftp = ftplib.FTP()
                ftp.connect("127.0.0.1", self.port, timeout=5)
                ftp.login(FTP_USER, FTP_PASS)
                return ftp
            except OSError:
                if time.time() > deadline:
                    raise
                time.sleep(0.1)

    def _quit(self) -> None:
        try:
            self.ftp.quit()
        except Exception:
            pass

    def _stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=10)

    def _upload(self, name: str) -> None:
        self.ftp.storbinary(f"STOR {name}", io.BytesIO(b"a,b\n1,2\n"))

    def _queued_names(self, wait_sec: float = 1.5) -> List[str]:
        """wait_sec の間キューを読み、送られたファイル名（区切り行を除く）を返す"""
        names: List[str] = []
        deadline = time.time() + wait_sec
        while time.time() < deadline:
            try:
                item = self.out_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            value = item[1]
            if value[0] != SEP:
                names.append(value[3])
        return names

    def test_received_matched_file_is_queued(self):
        self._upload("plain.csv")
        self.assertEqual(self._queued_names(), ["plain.csv"])

    def test_rename_to_matched_name_is_queued_once(self):
        self._upload("a.tmp")
        self.assertEqual(self._queued_names(), [])
        self.ftp.rename("a.tmp", "a.csv")
        self.assertEqual(self._queued_names(), ["a.csv"])
        self.assertTrue(os.path.isfile(os.path.join(self.home_dir, "a.csv")))

    def test_rename_from_matched_name_is_not_queued_again(self):
        self._upload("b.csv")
        self.ftp.rename("b.csv", "c.csv")
        self.assertEqual(self._queued_names(), ["b.csv"])

    def test_rename_to_unmatched_name_is_not_queued(self):
        self._upload("d.tmp")
        self.ftp.rename("d.tmp", "e.txt")
        self.assertEqual(self._queued_names(), [])

    def test_failed_rename_and_dir_rename_are_ignored(self):
        with self.assertRaises(ftplib.error_perm):
            self.ftp.rename("missing.tmp", "missing.csv")
        self.ftp.mkd("dir_tmp")
        self.ftp.rename("dir_tmp", "dir.csv")
        self.assertEqual(self._queued_names(), [])
        self.assertFalse(self.stop_event.is_set())

    def test_normal_file_logs_are_screen_only(self):
        logs: List[Tuple[str, str]] = []
        orig = base.output_log

        def _spy(tag, msg, log_file=""):
            logs.append((str(msg), log_file))
            return orig(tag, msg, log_file)

        with mock.patch.object(base, "output_log", _spy):
            self._upload("g.tmp")
            self.assertEqual(self._queued_names(), [])
            self.ftp.rename("g.tmp", "g.csv")
            self.assertEqual(self._queued_names(), ["g.csv"])
        for head in ("File received: ", "File renamed: ", "Suffix not matched, skip: ", "Queued #"):
            hits = [(m, f) for m, f in logs if m.startswith(head)]
            self.assertTrue(hits, (head, logs))
            self.assertFalse([f for _, f in hits if f], (head, hits))   # log_file を渡さない（画面のみ）

    def _restart(self, **overrides: Any) -> None:
        """止めてから、設定を変えて同じポートで再び起動する（後片付けは setUp の _stop）"""
        self._stop()
        from src.comm_ftp.tri_s11_find_ftp import f_find_ftp_trigger
        self.out_queue, self.stop_event = queue.Queue(), threading.Event()
        self.thread = threading.Thread(target=f_find_ftp_trigger,
                                       args=(self.out_queue, COL_DEF, {**self.config, **overrides}, self.stop_event),
                                       daemon=True)
        self.thread.start()
        self.ftp = self._connect()

    def test_stop_is_quick_and_server_thread_ends(self):
        with _LogCapture() as cap:
            time.sleep(1.5)                                     # serve の繰り返しが3回ほど回る間
        self.assertFalse([m for m in cap.messages if "starting" in m], cap.messages)
        srv = [th for th in threading.enumerate() if th.name == "f_find_ftp_trigger_server" and th.is_alive()]
        self.assertEqual(len(srv), 1)
        t0 = time.time()
        self._stop()                                            # クライアントがつないだままでも止まる
        self.assertLess(time.time() - t0, 2.0)
        self.assertFalse(self.thread.is_alive())
        self.assertFalse(srv[0].is_alive())                     # サーバーのスレッドが残らない

        from src.comm_ftp.tri_s11_find_ftp import f_find_ftp_trigger
        self.out_queue, self.stop_event = queue.Queue(), threading.Event()
        self.thread = threading.Thread(target=f_find_ftp_trigger,
                                       args=(self.out_queue, COL_DEF, self.config, self.stop_event),
                                       daemon=True)
        self.thread.start()                                     # 同じポートで再び起動（後片付けは setUp の _stop）
        self.ftp = self._connect()
        self._upload("again.csv")
        self.assertEqual(self._queued_names(), ["again.csv"])

    def test_idle_connection_is_closed_by_server_timeout(self):
        self._restart(server_timeout=1)
        self.ftp.voidcmd("NOOP")                                # つながっている
        time.sleep(3.0)                                         # server_timeout（1 秒）より長く放置
        with self.assertRaises((ftplib.Error, EOFError, OSError)):
            self.ftp.voidcmd("NOOP")                            # サーバーから切られている


if __name__ == "__main__":
    unittest.main(verbosity=2)
