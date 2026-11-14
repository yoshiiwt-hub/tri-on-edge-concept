# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Single Run Test [Find Trigger: FTP + Text]
# Ver: 01.b.20261114    By: IWATA,Y.'s Agent AI
# Mod: 停止の指示から、トリガーと FTP サーバーのスレッドが 2 秒以内に終わり、同じポートで再び起動できることのテストを追加。
#      新規作成（FTP で受け取った区切りテキストを読み、1ファイル = 1セグメントで流す f_find_ftp_csv_trigger）
########################################
f_find_ftp_csv_trigger を localhost の空きポートで起動し、標準ライブラリの ftplib で区切りテキストを送って、
キューに流れる行を確かめるテスト（pyftpdlib が無い環境ではスキップ）

    確認すること
      1. 受け取った CSV の全行が、col_def の並び（column_source_map / extra_col_def の割当て、型変換）で流れ、最後に区切り行が流れる
      2. delimiter / data_start_row / row_range / encodings が、tri_s11_find_text_csv と同じように効く
      3. pfx_sfx（既定 .csv）に合わないファイルは読まない。受信後のリネームで合うようになったファイルは1回だけ読む
      4. 安定チェック（stable_check_n）と ACK（check_ack）が効く（ACK が無ければ読まない）
      5. after_read（keep / delete / move / rename）で、読んだファイルと ACK ファイルが片付く。既定は keep
      6. 読込みに失敗したファイルは ErrorIF へ報告して飛ばし（後片付けしない）、次のファイルは読む
      7. 設定の誤りで、起動時に止まる
      8. 1ファイルごとの正常なログは画面のみ（log_file を渡さない）。読込みの失敗はログファイルにも残す
      9. 停止の指示から、トリガーと FTP サーバーのスレッドが 2 秒以内に終わり、同じポートで再び起動できる

実行: tri_s10_stream/ で  python -m unittest _test.test_s11_find_ftp_csv -v
"""
import ftplib
import importlib.util
import io
import os
import queue
import shutil
import socket
import sys
import tempfile
import threading
import time
import unittest
from   datetime import datetime as dt
from   typing   import Any, Dict, List, Optional, Tuple
from   unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # tri_s10_stream/（src の親）
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import src.tri_s00_supervise_base as base
from   src.tri_s01_error_if import ErrorIF

HAS_PYFTPDLIB = importlib.util.find_spec("pyftpdlib") is not None

FTP_USER = "tri_test"
FTP_PASS = "tri_test_pass"
SEP = "____"
TRIGGER = "f_find_ftp_csv_trigger"

# 0: 時刻 / 1: 値（float）/ 2: 件数（int）/ 3: LOT（ファイル名の "_" 区切りの1番目）/ 4: 固定値
COL_DEF: Dict[int, Any] = {
    0: ["ts" , "time" , "key", "key", ""],
    1: ["val", "float", "num", "exp", ""],
    2: ["cnt", "int"  , "num", "exp", ""],
    3: ["LOT", "str"  , "key", "key", ""],
    4: ["src", "str"  , "key", "key", ""],
}
BASE_CONFIG: Dict[str, Any] = {
    "stable_check_n"   : 0,
    "data_start_row"   : 1,                                   # 1行目は見出し
    "column_source_map": {"0": 0, "1": 2, "2": 1},            # CSV の [時刻, 件数, 値] を col_def の [時刻, 値, 件数] へ
    "extra_col_def"    : {"3": {"src": "filename", "delimiter": "_", "index": 0},
                          "4": {"src": "const", "default": "FTP"}},
}
CSV_TEXT = "ts,cnt,val\n2026-11-14 10:00:00.000000,3,1.5\n2026-11-14 10:00:01.000000,4,2.5\n"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@unittest.skipUnless(HAS_PYFTPDLIB, "pyftpdlib is not installed")
class TestFindFtpText(unittest.TestCase):

    def setUp(self) -> None:
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        # 本番と同じポリシー（ループ中の io の失敗は、待って続ける）。ポリシーのファイルが無いときは、ErrorIF の既定で動く
        policy = os.path.join(ROOT, "settings", "error_policy.json")
        ErrorIF.init(policy_path=policy) if os.path.isfile(policy) else ErrorIF.init()
        ErrorIF.drain_events()

        self.home_dir = tempfile.mkdtemp(prefix="tri_test_ftp_csv_")
        self.addCleanup(shutil.rmtree, self.home_dir, True)
        self.port = _free_port()
        env = mock.patch.dict(os.environ, {"FTP_HOST": "127.0.0.1", "FTP_PORT": str(self.port),
                                           "FTP_USER": FTP_USER, "FTP_PASS": FTP_PASS})
        env.start()
        self.addCleanup(env.stop)
        self.ftp: Optional[ftplib.FTP] = None

    # ---- helper ----
    def _start(self, extra: Optional[Dict[str, Any]] = None) -> Tuple[queue.Queue, threading.Event, threading.Thread]:
        from src.comm_ftp.tri_s11_find_ftp_csv import f_find_ftp_csv_trigger
        config = {"home_dir": self.home_dir, "passive_ports": [61200, 61300], **BASE_CONFIG, **(extra or {})}
        q, stop = queue.Queue(), threading.Event()
        t = threading.Thread(target=f_find_ftp_csv_trigger, args=(q, COL_DEF, config, stop), daemon=True)
        t.start()
        self.addCleanup(lambda: (stop.set(), t.join(timeout=10)))
        return q, stop, t

    def _connect(self) -> ftplib.FTP:
        deadline = time.time() + 10.0
        while True:
            try:
                ftp = ftplib.FTP()
                ftp.connect("127.0.0.1", self.port, timeout=5)
                ftp.login(FTP_USER, FTP_PASS)
                self.addCleanup(lambda: _quiet(ftp.quit))
                return ftp
            except OSError:
                if time.time() > deadline:
                    raise
                time.sleep(0.1)

    def _upload(self, name: str, data: bytes) -> None:
        if self.ftp is None:
            self.ftp = self._connect()
        self.ftp.storbinary(f"STOR {name}", io.BytesIO(data))

    @staticmethod
    def _segments(q: queue.Queue, wait_sec: float = 1.5) -> List[List[list]]:
        """wait_sec の間キューを読み、区切り行で区切ったセグメント（行のリスト）のリストを返す"""
        segs: List[List[list]] = []
        cur: List[list] = []
        deadline = time.time() + wait_sec
        while time.time() < deadline:
            try:
                _count, value = q.get(timeout=0.1)
            except queue.Empty:
                continue
            if value[0] == SEP:
                segs.append(cur)
                cur = []
            else:
                cur.append(value)
        if cur:
            segs.append(cur)                                       # 区切り行が来なかった行（あれば不具合）
        return segs

    def _events(self) -> List[Tuple[str, str]]:
        return [(e.get("phase"), e.get("category")) for e in ErrorIF.drain_events() if e.get("trigger_id") == TRIGGER]

    # ---- 1. 全行・列の割当て・型変換・区切り行 ----
    def test_rows_mapped_cast_and_separator(self):
        q, _, _ = self._start()
        self._upload("LOT01_20261114.csv", CSV_TEXT.encode("utf-8"))
        segs = self._segments(q)
        self.assertEqual(len(segs), 1)                              # 1ファイル = 1セグメント
        self.assertEqual(segs[0], [
            [dt(2026, 11, 14, 10, 0, 0), 1.5, 3, "LOT01", "FTP"],
            [dt(2026, 11, 14, 10, 0, 1), 2.5, 4, "LOT01", "FTP"],
        ])

    # ---- 2. delimiter / data_start_row / row_range / encodings ----
    def test_delimiter_row_range_and_encoding(self):
        q, _, _ = self._start({"delimiter": "\t", "data_start_row": 2, "row_range": [2, 3],
                               "encodings": ["utf-8", "cp932"], "column_source_map": {"0": 0, "1": 2, "2": 1},
                               "extra_col_def": {"3": {"src": "header", "loc": [0, 1]}, "4": {"src": "const", "default": "FTP"}}})
        text = ("品番\tLOT_ア\n見出し\tcnt\tval\n"                    # 0行目: ヘッダ（LOT を取る）/ 1行目: 見出し
                "2026-11-14 10:00:00.000000\t1\t1.0\n2026-11-14 10:00:01.000000\t2\t2.0\n"
                "2026-11-14 10:00:02.000000\t3\t3.0\n2026-11-14 10:00:03.000000\t4\t4.0\n")
        self._upload("tab.csv", text.encode("cp932"))               # utf-8 で読めない → cp932
        segs = self._segments(q)
        self.assertEqual([r[2] for r in segs[0]], [2, 3])           # データ行の 2〜3 行目だけ
        self.assertEqual({r[3] for r in segs[0]}, {"LOT_ア"})

    # ---- 3. pfx_sfx とリネーム ----
    def test_pfx_sfx_and_rename(self):
        q, _, _ = self._start()
        self._upload("skip.txt", CSV_TEXT.encode())                 # .csv でない → 読まない
        self._upload("a.tmp", CSV_TEXT.encode())                    # 受信時は該当しない
        self.ftp.rename("a.tmp", "A1_x.csv")                        # リネームで該当 → 1回だけ読む
        self._upload("B1_y.csv", CSV_TEXT.encode())                 # 受信時に読む
        self.ftp.rename("B1_y.csv", "B2_y.csv")                     # 該当していた名前からのリネーム → 読まない
        segs = self._segments(q, 2.0)
        self.assertEqual([s[0][3] for s in segs], ["A1", "B1"])

    # ---- 4. 安定チェックと ACK ----
    def test_stable_check_and_ack(self):
        q, _, _ = self._start({"stable_check_n": 1, "check_ack": True, "ack_file_ext": "done"})
        self._upload("NOACK_1.csv", CSV_TEXT.encode())              # ACK が無い → 読まない
        self._upload("ACK_1.csv.done", b"")                         # ACK を先に置く
        self._upload("ACK_1.csv", CSV_TEXT.encode())                # 安定チェック（約1秒）の後に読む
        segs = self._segments(q, 4.0)
        self.assertEqual([s[0][3] for s in segs], ["ACK"])

    # ---- 5. after_read ----
    def test_after_read(self):
        move_to = tempfile.mkdtemp(prefix="tri_test_ftp_csv_moved_")
        self.addCleanup(shutil.rmtree, move_to, True)
        cases = [
            ({}, ["K_1.csv"]),                                                   # 既定 keep → 残る
            ({"after_read": "delete", "check_ack": True}, []),                   # ACK も消える
            ({"after_read": "move", "move_to": move_to}, []),
            ({"after_read": "rename", "rename_pfx_sfx": ["DONE__", ""]}, ["DONE__K_1.csv"]),
        ]
        for extra, remain in cases:
            with self.subTest(extra=extra):
                for f in os.listdir(self.home_dir):
                    os.remove(os.path.join(self.home_dir, f))
                self.port = _free_port()
                os.environ["FTP_PORT"] = str(self.port)
                self.ftp = None
                q, stop, t = self._start(extra)
                if extra.get("check_ack"):
                    self._upload("K_1.csv.done", b"")
                self._upload("K_1.csv", CSV_TEXT.encode())
                self.assertEqual(len(self._segments(q)), 1)
                self.assertEqual(sorted(os.listdir(self.home_dir)), remain)
                stop.set()
                t.join(timeout=10)
        self.assertEqual(os.listdir(move_to), ["K_1.csv"])

    # ---- 6. 読込みの失敗 ----
    def test_read_error_skips_and_continues(self):
        q, stop, _ = self._start({"encodings": ["ascii"], "after_read": "delete"})
        self._upload("BAD_1.csv", "時刻,値\n1,2\n".encode("cp932"))  # ascii で読めない
        self._upload("OK_1.csv", CSV_TEXT.encode())
        segs = self._segments(q, 2.0)
        self.assertEqual([s[0][3] for s in segs], ["OK"])           # 失敗したファイルは流さず、次は読む
        self.assertFalse(stop.is_set())
        self.assertEqual(os.listdir(self.home_dir), ["BAD_1.csv"])  # 失敗したファイルは片付けない
        self.assertIn(("loop", "io"), self._events())

    # ---- 7. 設定の誤り ----
    def test_config_errors_stop_at_startup(self):
        bad = [
            {"after_read": "copy"},
            {"after_read": "move"},                                  # move_to が無い
            {"after_read": "move", "move_to": None},
            {"row_range": [0]},
            {"row_range": [3, 2]},
            {"column_source_map": {"9": 0}},                         # col_def の範囲外
            {"extra_col_def": {"9": {"src": "const"}}},
            {"extra_col_def": {"3": {"src": "filename"}}},           # pos / delimiter+index が無い
            {"check_ack": True, "ack_file_ext": ""},
        ]
        for extra in bad:
            with self.subTest(extra=extra):
                _, stop, t = self._start(extra)
                t.join(timeout=10)
                self.assertTrue(stop.is_set())
        _, stop, t = self._start({"after_read": "move", "move_to": self.home_dir})   # 移動先が home_dir と同じ
        t.join(timeout=10)
        self.assertTrue(stop.is_set())

    # ---- 8. ログ ----
    def test_log_screen_only_for_normal_and_file_for_errors(self):
        calls: List[Tuple[str, str]] = []
        real = base.output_log

        def spy(tag: str, msg: str, log_file: str = "") -> None:
            calls.append((msg, log_file))
            real(tag, msg, log_file)

        with mock.patch.object(base, "output_log", spy):
            q, _, _ = self._start({"encodings": ["ascii"], "after_read": "delete"})
            self._upload("OK_1.csv", CSV_TEXT.encode())
            self._upload("BAD_1.csv", "時刻\n1\n".encode("cp932"))
            self._segments(q, 2.0)
        normal = [(m, lf) for m, lf in calls if m.startswith(("File received", "Queued", "After Read ("))]
        self.assertEqual(len(normal), 4)                            # 受信2・Queued 1・After Read 1
        self.assertTrue(all(lf == "" for _, lf in normal))           # 画面のみ
        errors = [(m, lf) for m, lf in calls if m.startswith("Read Error")]
        self.assertEqual(len(errors), 1)
        self.assertNotEqual(errors[0][1], "")                       # ログファイルにも残す

    def test_stop_is_quick_and_server_thread_ends(self):
        q, stop, t = self._start()
        self._upload("A_1.csv", CSV_TEXT.encode())
        self.assertEqual(len(self._segments(q)), 1)
        srv = [th for th in threading.enumerate() if th.name == "f_find_ftp_csv_trigger_server" and th.is_alive()]
        self.assertEqual(len(srv), 1)
        t0 = time.time()
        stop.set()                                              # クライアントがつないだままでも止まる
        t.join(timeout=10)
        self.assertLess(time.time() - t0, 2.0)
        self.assertFalse(t.is_alive())
        self.assertFalse(srv[0].is_alive())                     # サーバーのスレッドが残らない

        q2, _, _ = self._start()                                # 同じポートで再び起動
        self.ftp = None
        self._upload("A_2.csv", CSV_TEXT.encode())
        self.assertEqual(len(self._segments(q2)), 1)


def _quiet(func) -> None:
    try:
        func()
    except Exception:
        pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
