# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Unit Test [Connect State] / Single Run Test [Act / Find: not_connectable]
# Ver: 01.f.20261109    By: IWATA, Y.'s Agent AI
# Mod: pg8000 が入っていない環境では、RDB の接続できない状態のテストをスキップするようにした
########################################
tri_s01_connect_state（ConnectState）の単体テストと、
ConnectState を使うトリガーを、止めたり戻したりできる接続先（localhost の小さなサーバ・組込みブローカー・偽の PLC など）に
つないで、接続できない状態の扱いを確かめるテスト

    確認すること
      A. ConnectState
        1. 作った時点は connectable。問い合わせ（get_is_connectable / get_connect_states）で読めること
        2. set_not_connectable / set_connectable は状態が変わったときだけ、ログファイル・メトリクス・ErrorIF に出ること
        3. should_try_reconnect は not_connectable で再接続の間隔が過ぎたときだけ真。reconnect_failed は回数を数え ErrorIF へ通知すること
        4. report=False は ErrorIF へ通知しないこと / 接続先が複数のとき・登録が無いときの問い合わせ
      B. act socket（a_send_seg_trigger、ack_mode=true）
        1. サーバが止まると、連続 retry_max 回の失敗で not_connectable に移り、届いたセグメントはスキップすること
           （リトライは ErrorIF へ通知しない。移行の通知は1回）
        2. サーバが戻ると connectable に戻り、送れなかったセグメントを送り直すこと
        3. 1回だけの失敗は、その場のリトライで送れて、状態は connectable のままであること
      C. find socket（f_socket_client_trigger）
        1. サーバが止まると、連続 retry_max 回の失敗で not_connectable に移り、戻ると connectable に戻って値の取り込みを再開すること
        2. 起動時に接続できないときは、ErrorIF へ phase=startup で1回通知し、not_connectable から始まること
      D. MQTT（a_mqtt_pub_seg_trigger / f_find_mqtt_trigger。組込みブローカー amqtt を使う。無ければスキップ）
        1. ブローカーが止まると両方が not_connectable に移り（通知は1回ずつ）、戻ると connectable に戻って送受信を再開すること
        2. act の起動時にブローカーが無いときは、phase=startup で1回通知し、not_connectable から始まること
      E. SLMP（a_slmp_write_trigger / f_find_slmp_trigger。偽のクライアントを使う）
        1. 1回だけの失敗は、その場で読み直す・書き直す（状態は connectable のまま、通知も無い）
        2. 続けて失敗すると not_connectable（書けなかったセグメント・スキップしたセグメントは書かない）、戻ると connectable
      F. エージェント型（f_agentic_trigger。小さなエージェントを使う）
        1. エージェントが止まると not_connectable に移り、エージェントを起動し直して connectable に戻ること
      G. RDB（a_insert_to_pg_trigger / a_upsert_alive_to_pg_trigger / f_table_diff_bulk_trigger。偽の接続を使う）
        1. 起動時に DB が無くても止まらず（phase=startup で1回通知）、戻ると connectable になってテーブルを準備し、書き込むこと
        2. 書込み・読出しの途中で切れると、リトライの後 not_connectable に移り、戻ると再開すること。テーブルの構成が違えば止まること
      H. FTP act（pyftpdlib の FTP サーバ）: サーバが止まると not_connectable、既定の wait で待ち、戻ると同じファイルを送ること
      I. SMB act / find（偽の smbclient）
        1. SMB act は H と同じ。SMB find は、起動時に届かなくても止まらず、戻ると取り込むこと
           SMB find は smb_domain があると、ユーザー名を「ドメイン名\\ユーザー名」の形にしてセッションを登録すること（SMB act と同じ）

実行: tri_s10_stream/ で  python -m unittest _test.test_s01_connect_state -v
"""
import io
import json
import os
import queue
import shutil
import socket
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
import src.tri_s01_connect_state  as cs
from   src.tri_s01_error_if import ErrorIF


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(cond: Callable[[], bool], timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return cond()


def _has_module(name: str) -> bool:
    import importlib.util
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:
        return False


class _LineServer:
    """
    localhost の小さなサーバ（同じポートで止めたり戻したりできる）
        mode="ack": PUT の次の行（データ）を受け取るたびに "OK" を返す。受け取ったデータを received に残す
        mode="get": 要求を受け取るたびに、JSON（[連番]）＋改行を返す
        drop_first=True: 最初の接続だけ、受け付けた直後に閉じる（1回だけの失敗を作る）
    """

    def __init__(self, port: int, mode: str, drop_first: bool = False) -> None:
        self.port, self.mode, self.drop_first = port, mode, drop_first
        self.received: List[dict] = []
        self.n_accept = 0
        self._srv: Optional[socket.socket] = None
        self._conns: List[socket.socket] = []
        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []

    def start(self) -> None:
        self._stop.clear()
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", self.port))
        srv.listen(5)
        srv.settimeout(0.1)
        self._srv = srv
        t = threading.Thread(target=self._accept_loop, daemon=True)
        t.start()
        self._threads.append(t)

    def stop(self) -> None:
        self._stop.set()
        for t in self._threads:
            t.join(timeout=5)
        self._threads = []
        for c in self._conns:
            try:   c.close()
            except Exception: pass
        self._conns = []
        if self._srv:
            self._srv.close()
            self._srv = None

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._srv.accept()
            except (socket.timeout, OSError):
                continue
            self.n_accept += 1
            if self.drop_first and self.n_accept == 1:
                conn.close()
                continue
            self._conns.append(conn)
            t = threading.Thread(target=self._serve, args=(conn,), daemon=True)
            t.start()
            self._threads.append(t)

    def _serve(self, conn: socket.socket) -> None:
        conn.settimeout(0.1)
        buf, n = "", 0
        while not self._stop.is_set():
            try:
                data = conn.recv(4096)
            except socket.timeout:
                continue
            except OSError:
                return
            if not data:
                return
            buf += data.decode("utf-8")
            if self.mode == "get":
                n += 1
                buf = ""
                try:   conn.sendall((json.dumps([n]) + "\n").encode("utf-8"))
                except OSError: return
                continue
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                if line == "PUT":
                    continue
                self.received.append(json.loads(line))
                try:   conn.sendall(b"OK\n")
                except OSError: return


def conn_events() -> List[dict]:
    """ErrorIF にたまった、接続（ConnectionFailed.comm.*）のイベント"""
    return [e for e in ErrorIF.drain_events() if str(e.get("signature", "")).startswith("ConnectionFailed.comm.")]


def wait_conn_events(n: int = 1, word: str = "Not Connectable", timeout: float = 5.0) -> List[dict]:
    """
    word を含む接続のイベントが n 件たまるまで待って、たまったイベントを返す
    （ConnectState は状態を変えてから ErrorIF へ報告するので、状態が変わった直後にはまだ届いていないことがある）
    """
    ev: List[dict] = []
    wait_until(lambda: (ev.extend(conn_events()) or sum(1 for e in ev if word in e["message"]) >= n), timeout)
    time.sleep(0.05)   # 余分な通知が無いことも確かめられるように、少し待ってから残りも集める
    ev.extend(conn_events())
    return ev


########################################
# A. ConnectState
########################################

class TestConnectState(unittest.TestCase):

    def setUp(self) -> None:
        ErrorIF.init()
        self.logs: List[Tuple[str, str]] = []
        orig = base.output_log

        def _spy(tag, msg, log_file=""):
            self.logs.append((str(msg), log_file))
            return orig(tag, msg, log_file)

        p = mock.patch.object(base, "output_log", _spy)
        p.start()
        self.addCleanup(p.stop)

    def _file_logs(self, word: str) -> List[str]:
        return [m for m, f in self.logs if f and word in m]

    def test_initial_state_and_query(self):
        st = cs.ConnectState("t_init", "host:1", 0.2, "MOD_INIT")
        self.assertTrue(st.is_connectable())
        self.assertIs(cs.get_is_connectable("t_init"), True)
        self.assertIs(cs.get_is_connectable("t_init", "host:1"), True)
        self.assertIsNone(cs.get_is_connectable("no_such_trigger"))
        snap = [s for s in cs.get_connect_states() if s["trigger_name"] == "t_init"]
        self.assertEqual(snap[0]["state"], cs.CONNECTABLE)
        self.assertEqual(base.metrics_get_all()["MOD_INIT"]["connect_state"], cs.CONNECTABLE)

    def test_transitions_are_reported_once(self):
        st = cs.ConnectState("t_tr", "host:2", 0.2, "MOD_TR")
        st.set_not_connectable(ConnectionRefusedError("refused"))
        st.set_not_connectable(ConnectionRefusedError("refused again"))       # 既に not_connectable → 何もしない
        self.assertFalse(st.is_connectable())
        self.assertIs(cs.get_is_connectable("t_tr"), False)
        self.assertEqual(len(self._file_logs("Not Connectable")), 1)
        ev = conn_events()
        self.assertEqual(len(ev), 1)
        self.assertEqual(ev[0]["signature"], "ConnectionFailed.comm.loop.t_tr")
        m = base.metrics_get_all()["MOD_TR"]
        self.assertEqual((m["connect_state"], m["total_not_connectable"]), (cs.NOT_CONNECTABLE, 1))
        self.assertNotEqual(m["not_connectable_since"], "-")

        st.set_connectable()
        st.set_connectable()                                                  # 既に connectable → 何もしない
        self.assertTrue(st.is_connectable())
        self.assertEqual(len([m for m in self._file_logs("Connectable. host:2") if "Not Connectable" not in m]), 1)
        m = base.metrics_get_all()["MOD_TR"]
        self.assertEqual((m["connect_state"], m["not_connectable_since"]), (cs.CONNECTABLE, "-"))

    def test_reconnect_interval_and_failures(self):
        st = cs.ConnectState("t_rc", "host:3", 0.2, "MOD_RC")
        self.assertFalse(st.should_try_reconnect())                           # connectable の間は偽
        st.set_not_connectable(report=False)
        self.assertEqual(conn_events(), [])                                   # report=False は通知しない
        self.assertFalse(st.should_try_reconnect())                           # 移った直後は間隔が過ぎていない
        time.sleep(0.25)
        self.assertTrue(st.should_try_reconnect())
        self.assertFalse(st.should_try_reconnect())                           # 試行の時刻を記録したので、次の間隔まで偽
        st.reconnect_failed(ConnectionRefusedError("refused"))
        st.reconnect_failed(ConnectionRefusedError("refused"))
        self.assertEqual(base.metrics_get_all()["MOD_RC"]["reconnect_fail_count"], 2)
        self.assertEqual(len(conn_events()), 2)                               # 再接続の失敗は ErrorIF へ通知
        self.assertEqual(self._file_logs("Re-Connect Failed"), [])            # ログファイルへの出力は ErrorIF に任せる
        st.set_connectable()
        self.assertEqual(base.metrics_get_all()["MOD_RC"]["reconnect_fail_count"], 0)

    def test_multiple_targets(self):
        a = cs.ConnectState("t_multi", "pg", 0.2, "MOD_MULTI")
        cs.ConnectState("t_multi", "smb", 0.2, "MOD_MULTI")
        a.set_not_connectable(report=False)
        self.assertIs(cs.get_is_connectable("t_multi"), False)                # 1つでも not_connectable なら False
        self.assertIs(cs.get_is_connectable("t_multi", "smb"), True)
        self.assertIs(cs.get_is_connectable("t_multi", "pg"), False)


########################################
# B. act socket
########################################

SEG_COL_DEF: Dict[int, Any] = {i: [n, "str", "key", "key", ""] for i, n in enumerate(base.SEG_HEADER_COLS)}
SEG_COL_DEF.update({4: ["v", "int", "num", "obj", ""]})


def _segment(seg_id: int) -> List[list]:
    return [[seg_id, 1, dt.now(), 0, seg_id * 10]]


class TestActSocketNotConnectable(unittest.TestCase):

    def setUp(self) -> None:
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        ErrorIF.init()
        self.port = _free_port()

    def _start_act(self, extra: Dict[str, Any]) -> Tuple[queue.Queue, threading.Event, threading.Thread]:
        from src.tri_s12_act_socket import a_send_seg_trigger
        config = {"server_ip": "127.0.0.1", "server_port": self.port, "ack_mode": True, "timeout_sec": 1.0,
                  "retry_max": 3, "retry_wait_sec": 0.05, "reconnect_interval_sec": 0.3,
                  "not_connectable_action": "skip", "resend_max": 3, **extra}
        q, stop = queue.Queue(), threading.Event()
        t = threading.Thread(target=a_send_seg_trigger, args=(q, SEG_COL_DEF, config, stop), daemon=True)
        t.start()
        self.addCleanup(self._stop, q, stop, t)
        return q, stop, t

    @staticmethod
    def _stop(q, stop, t) -> None:
        q.put(None)
        t.join(timeout=10)
        stop.set()

    def test_not_connectable_skip_and_resend_after_reconnect(self):
        srv = _LineServer(self.port, "ack")
        srv.start()
        self.addCleanup(srv.stop)
        q, stop, t = self._start_act({})
        m0 = dict(base.metrics_get_all().get("ACT_Socket", {}))

        q.put(_segment(1))
        self.assertTrue(wait_until(lambda: [r["seg_id"] for r in srv.received] == [1]))
        conn_events()

        srv.stop()                                                            # サーバが止まる
        q.put(_segment(2))                                                    # 送れない → リトライ → 3回目で not_connectable
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_send_seg_trigger") is False))
        q.put(_segment(3))                                                    # 接続できない間 → スキップ
        self.assertTrue(wait_until(lambda: base.metrics_get_all()["ACT_Socket"]["total_skipped_segments"]
                                   - m0.get("total_skipped_segments", 0) == 1))
        ev = wait_conn_events()
        self.assertEqual(sum(1 for e in ev if "Not Connectable" in e["message"]), 1)   # 移行の通知は1回
        self.assertFalse([e for e in ev if "Retry" in e["message"]])                  # リトライは通知しない

        srv.start()                                                           # サーバが戻る
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_send_seg_trigger") is True))
        self.assertTrue(wait_until(lambda: 2 in [r["seg_id"] for r in srv.received]))  # 送れなかった 2 を送り直す
        self.assertNotIn(3, [r["seg_id"] for r in srv.received])               # スキップした 3 は送らない
        m = base.metrics_get_all()["ACT_Socket"]
        self.assertEqual(m["total_not_connectable"] - m0.get("total_not_connectable", 0), 1)
        self.assertEqual(m["connect_state"], cs.CONNECTABLE)

    def test_single_failure_is_retried_in_place(self):
        srv = _LineServer(self.port, "ack", drop_first=True)                 # 最初の接続だけ閉じる → 1回だけ失敗
        srv.start()
        self.addCleanup(srv.stop)
        q, stop, t = self._start_act({})
        self.assertTrue(wait_until(lambda: srv.n_accept >= 1))
        conn_events()
        q.put(_segment(5))
        self.assertTrue(wait_until(lambda: [r["seg_id"] for r in srv.received] == [5]))
        self.assertIs(cs.get_is_connectable("a_send_seg_trigger"), True)
        self.assertEqual(conn_events(), [])                                   # 状態は変わらず、通知も無い


########################################
# C. find socket
########################################

FIND_COL_DEF: Dict[int, Any] = {0: ["n", "int", "num", "obj", ""]}


class TestFindSocketNotConnectable(unittest.TestCase):

    def setUp(self) -> None:
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        ErrorIF.init()
        self.port = _free_port()

    def _start_find(self) -> queue.Queue:
        from src.tri_s11_find_socket import f_socket_client_trigger
        config = {"server_ip": "127.0.0.1", "server_port": self.port, "polling_ms": 20,
                  "connect_timeout_sec": 1, "retry_max": 3, "retry_wait_sec": 0.05, "reconnect_interval_sec": 0.3}
        q, stop = queue.Queue(), threading.Event()
        t = threading.Thread(target=f_socket_client_trigger, args=(q, FIND_COL_DEF, config, stop), daemon=True)
        t.start()
        self.addCleanup(lambda: (stop.set(), t.join(timeout=10)))
        return q

    @staticmethod
    def _drain(q: queue.Queue) -> int:
        n = 0
        while True:
            try:
                q.get_nowait()
                n += 1
            except queue.Empty:
                return n

    def test_not_connectable_and_back(self):
        srv = _LineServer(self.port, "get")
        srv.start()
        self.addCleanup(srv.stop)
        q = self._start_find()
        self.assertTrue(wait_until(lambda: q.qsize() >= 3))
        conn_events()

        srv.stop()
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("f_socket_client_trigger") is False))
        ev = wait_conn_events()
        self.assertEqual(sum(1 for e in ev if "Not Connectable" in e["message"]), 1)
        self._drain(q)
        time.sleep(0.3)
        self.assertEqual(self._drain(q), 0)                                   # 接続できない間は取り込まない

        srv.start()
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("f_socket_client_trigger") is True))
        self.assertTrue(wait_until(lambda: q.qsize() >= 3))                    # 取り込みを再開

    def test_startup_without_server(self):
        # 本番と同じポリシー（起動時の通信の失敗は待って続ける）。ポリシーのファイルが無いときは、ErrorIF の既定（停止しない）で動く
        policy = os.path.join(ROOT, "settings", "error_policy.json")
        if os.path.isfile(policy):
            ErrorIF.init(policy_path=policy)
        q = self._start_find()                                                # サーバなしで起動
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("f_socket_client_trigger") is False, timeout=20))
        ev = [e for e in ErrorIF.drain_events() if e.get("trigger_id") == "f_socket_client_trigger"]
        self.assertEqual([e.get("phase") for e in ev if e.get("category") == "comm"], ["startup"])  # startup で1回
        srv = _LineServer(self.port, "get")
        srv.start()
        self.addCleanup(srv.stop)
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("f_socket_client_trigger") is True))
        self.assertTrue(wait_until(lambda: q.qsize() >= 1))


########################################
# D. MQTT（act / find）
########################################

def _mqtt_available() -> bool:
    import importlib.util
    try:
        return all(importlib.util.find_spec(m) is not None for m in ("paho.mqtt.client", "amqtt.broker"))
    except Exception:
        return False


class _Broker:
    """組込みブローカー（amqtt）を同じポートで止めたり戻したりする"""

    def __init__(self, port: int) -> None:
        self.port = port
        self._stop: Optional[threading.Event] = None
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        import src.comm_mqtt.tri_s19_mqtt as mqtt_util
        self._stop, ready = threading.Event(), threading.Event()
        self._thread = mqtt_util.start_embedded_broker(self.port, True, ready, self._stop)
        if not ready.wait(timeout=15):
            raise RuntimeError("embedded broker did not start")

    def stop(self) -> None:
        if self._stop is not None:
            self._stop.set()
            self._thread.join(timeout=10)
            self._stop = None


@unittest.skipUnless(_mqtt_available(), "paho-mqtt / amqtt not installed")
class TestMqttNotConnectable(unittest.TestCase):

    def setUp(self) -> None:
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        ErrorIF.init()
        self.port = _free_port()
        self.common = {"broker_mode": "external", "broker_host": "127.0.0.1", "broker_port": self.port,
                       "retry_max": 3, "retry_wait_sec": 0.05, "reconnect_interval_sec": 0.3}

    def _start(self, func, args_q, col_def, config) -> threading.Event:
        stop = threading.Event()
        t = threading.Thread(target=func, args=(args_q, col_def, config, stop), daemon=True)
        t.start()
        self.addCleanup(lambda: (stop.set(), args_q.put(None), t.join(timeout=20)))
        return stop

    def _start_act(self) -> queue.Queue:
        from src.comm_mqtt.tri_s12_act_mqtt import a_mqtt_pub_seg_trigger
        q = queue.Queue()
        self._start(a_mqtt_pub_seg_trigger, q, SEG_COL_DEF,
                    {**self.common, "publish_topic": "t/conn", "not_connectable_action": "skip"})
        return q

    def _start_find(self) -> queue.Queue:
        from src.comm_mqtt.tri_s11_find_mqtt import f_find_mqtt_trigger
        q = queue.Queue()
        self._start(f_find_mqtt_trigger, q, FIND_COL_DEF,
                    {**self.common, "push_mode": True, "topic_map": {"0": {"topic": "t/conn", "payload_key": "v"}}})
        return q

    @staticmethod
    def _values(q: queue.Queue) -> List[Any]:
        out = []
        while True:
            try:
                item = q.get_nowait()
            except queue.Empty:
                return out
            if item is not None and item[1][0] is not None:
                out.append(item[1][0])

    def test_broker_down_and_back(self):
        brk = _Broker(self.port)
        brk.start()
        self.addCleanup(brk.stop)
        fq = self._start_find()
        aq = self._start_act()
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("f_find_mqtt_trigger") is True
                                   and cs.get_is_connectable("a_mqtt_pub_seg_trigger") is True, timeout=20))
        got: List[Any] = []
        aq.put(_segment(1))
        self.assertTrue(wait_until(lambda: (got.extend(self._values(fq)) or 10 in got), timeout=10))
        conn_events()

        brk.stop()                                                            # ブローカーが止まる
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("f_find_mqtt_trigger") is False, timeout=20))
        aq.put(_segment(2))                                                   # 送れない → リトライ → not_connectable
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_mqtt_pub_seg_trigger") is False, timeout=20))
        ev = wait_conn_events(2)
        for name in ("f_find_mqtt_trigger", "a_mqtt_pub_seg_trigger"):       # 移行の通知は1回ずつ
            self.assertEqual(sum(1 for e in ev if e.get("trigger_id") == name and "Not Connectable" in e["message"]), 1)

        brk.start()                                                           # ブローカーが戻る
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("f_find_mqtt_trigger") is True
                                   and cs.get_is_connectable("a_mqtt_pub_seg_trigger") is True, timeout=20))
        self._values(fq)
        got = []
        aq.put(_segment(4))
        self.assertTrue(wait_until(lambda: (got.extend(self._values(fq)) or 40 in got), timeout=10))
        self.assertNotIn(20, got)                                             # 接続できない間のセグメントは送らない

    def test_startup_without_broker(self):
        policy = os.path.join(ROOT, "settings", "error_policy.json")
        if os.path.isfile(policy):
            ErrorIF.init(policy_path=policy)
        self._start_act()                                                     # ブローカーなしで起動
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_mqtt_pub_seg_trigger") is False, timeout=20))
        ev = [e for e in ErrorIF.drain_events() if e.get("trigger_id") == "a_mqtt_pub_seg_trigger"]
        self.assertEqual([e.get("phase") for e in ev if e.get("category") == "comm"], ["startup"])  # startup で1回
        brk = _Broker(self.port)
        brk.start()
        self.addCleanup(brk.stop)
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_mqtt_pub_seg_trigger") is True, timeout=20))


########################################
# E. SLMP（act / find）。PLC の代わりに、止めたり戻したりできる偽のクライアントを使う
########################################

class _FakePlc:
    """
    偽の PLC（SlmpClient の代わり。テストの間だけ差し替える）
        down=True の間は、接続も読み書きも通信の失敗になる（つながっていた接続も切れる）
        drop_next=N: 次の N 回の読み書きだけ通信の失敗にする（1回だけの失敗を作る）
    """
    down      = False
    drop_next = 0
    n_read    = 0
    writes: List[Tuple[str, int, list]] = []

    def __init__(self, *args, **kwargs) -> None:
        self._connected = False

    @classmethod
    def reset(cls) -> None:
        cls.down, cls.drop_next, cls.n_read, cls.writes = False, 0, 0, []

    def connect(self) -> None:
        if _FakePlc.down:
            raise ConnectionRefusedError("fake plc is down")
        self._connected = True

    def disconnect(self) -> None:
        self._connected = False

    def _check(self) -> None:
        if not self._connected:
            raise ConnectionError("SlmpClient: not connected")
        if _FakePlc.down or _FakePlc.drop_next > 0:
            _FakePlc.drop_next = max(0, _FakePlc.drop_next - 1)
            self._connected = False
            raise ConnectionResetError("fake plc connection reset")

    def device_read_bulk(self, device_type: str, device_no: int, count: int) -> List[int]:
        self._check()
        _FakePlc.n_read += 1
        return [_FakePlc.n_read] * count

    def device_write(self, device_type: str, device_no: int, words: list) -> None:
        self._check()
        _FakePlc.writes.append((device_type, device_no, list(words)))


class TestSlmpNotConnectable(unittest.TestCase):

    def setUp(self) -> None:
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        import src.fa_slmp.tri_s19_slmp as slmp
        p2 = mock.patch.object(slmp, "SlmpClient", _FakePlc)
        p2.start()
        self.addCleanup(p2.stop)
        _FakePlc.reset()
        ErrorIF.init()
        self.common = {"plc_host": "127.0.0.1", "plc_port": 5010,
                       "retry_max": 3, "retry_wait_sec": 0.05, "reconnect_interval_sec": 0.3}

    def _start(self, func, q, col_def, config) -> None:
        stop = threading.Event()
        t = threading.Thread(target=func, args=(q, col_def, config, stop), daemon=True)
        t.start()
        self.addCleanup(lambda: (stop.set(), q.put(None), t.join(timeout=10)))

    def _start_find(self) -> queue.Queue:
        from src.fa_slmp.tri_s11_find_slmp import f_find_slmp_trigger
        q = queue.Queue()
        self._start(f_find_slmp_trigger, q, FIND_COL_DEF,
                    {**self.common, "flag_mode": False, "polling_ms": 20,
                     "bulk_read": {"0": ["device", "D", 100, "word", 1]},
                     "column_register_map": {"0": {"sources": [{"bulk": "0", "offset": 0}], "unit_dtype": "int2_u"}}})
        return q

    def _start_act(self) -> queue.Queue:
        from src.fa_slmp.tri_s12_act_slmp import a_slmp_write_trigger
        q = queue.Queue()
        self._start(a_slmp_write_trigger, q, SEG_COL_DEF,
                    {**self.common, "write_mode": "seg", "not_connectable_action": "skip",
                     "write_map": {"0": {"src_col": 0, "target": "device", "device_type": "D",
                                         "device_no": 300, "dtype": "word"}}})
        return q

    @staticmethod
    def _written() -> List[int]:
        return [w[2][0] // 10 for w in _FakePlc.writes]                       # 書いた値（seg_id * 10）→ seg_id

    def test_find_not_connectable_and_back(self):
        q = self._start_find()
        self.assertTrue(wait_until(lambda: q.qsize() >= 3))
        conn_events()
        _FakePlc.down = True                                                  # PLC が止まる
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("f_find_slmp_trigger") is False))
        ev = wait_conn_events()
        self.assertEqual(sum(1 for e in ev if "Not Connectable" in e["message"]), 1)
        self.assertFalse([e for e in ev if e.get("category") == "comm" and "Not Connectable" not in e["message"]
                          and "Re-Connect" not in e["message"]])              # リトライは通知しない
        _FakePlc.down = False                                                 # PLC が戻る
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("f_find_slmp_trigger") is True))
        n = q.qsize()
        self.assertTrue(wait_until(lambda: q.qsize() >= n + 3))               # 取り込みを再開

    def test_find_single_failure_is_retried(self):
        q = self._start_find()
        self.assertTrue(wait_until(lambda: q.qsize() >= 2))
        conn_events()
        _FakePlc.drop_next = 1                                                # 1回だけ失敗
        n = q.qsize()
        self.assertTrue(wait_until(lambda: q.qsize() >= n + 3))
        self.assertIs(cs.get_is_connectable("f_find_slmp_trigger"), True)
        self.assertEqual(conn_events(), [])

    def test_act_not_connectable_skip_and_back(self):
        q = self._start_act()
        q.put(_segment(1))
        self.assertTrue(wait_until(lambda: self._written() == [1]))
        _FakePlc.drop_next = 1                                                # 1回だけ失敗 → その場で書き直す
        q.put(_segment(2))
        self.assertTrue(wait_until(lambda: self._written() == [1, 2]))
        self.assertIs(cs.get_is_connectable("a_slmp_write_trigger"), True)
        conn_events()

        _FakePlc.down = True                                                  # PLC が止まる
        q.put(_segment(3))                                                    # 書けない → リトライ → not_connectable
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_slmp_write_trigger") is False))
        q.put(_segment(4))                                                    # 接続できない間 → スキップ
        time.sleep(0.1)
        ev = wait_conn_events()
        self.assertEqual(sum(1 for e in ev if "Not Connectable" in e["message"]), 1)
        _FakePlc.down = False                                                 # PLC が戻る
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_slmp_write_trigger") is True))
        q.put(_segment(5))
        self.assertTrue(wait_until(lambda: 5 in self._written()))
        self.assertEqual(self._written(), [1, 2, 5])                          # 3（not_connectable）・4（スキップ）は書かない


########################################
# F. エージェント型（tmpl_agentic の find）。小さなエージェント（別プロセス）を使う
########################################

_TEST_AGENT_SRC = r"""
import json, os, pathlib, socket, sys, time
port = int(os.environ["TEST_AGENT_PORT"])
down = pathlib.Path(os.environ["TEST_AGENT_DOWN"])
stop = pathlib.Path(os.environ["AGENT_STOP_FILE"])
if down.exists():
    sys.exit(0)
srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", port))
srv.listen(1)
srv.settimeout(0.1)
n = 0
while not down.exists() and not stop.exists():
    try:
        conn, _ = srv.accept()
    except socket.timeout:
        continue
    conn.settimeout(0.1)
    buf = ""
    while not down.exists() and not stop.exists():
        try:
            data = conn.recv(1024)
        except socket.timeout:
            continue
        if not data:
            break
        buf += data.decode("utf-8")
        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            if line in ("GET", "NACK"):
                n += 1
                conn.sendall((json.dumps([n]) + "\n").encode("utf-8"))
            elif line.startswith("POST "):
                with open(os.environ["TEST_AGENT_LOG"], "a", encoding="utf-8") as f:
                    f.write(line[5:] + "\n")
                conn.sendall(b"ACK\n")
    conn.close()
srv.close()
"""


class _AgentCase:
    """エージェント型のテストの土台（小さなエージェントの準備と、find の確かめ方）。これ自体はテストを持たない"""

    def setUp(self) -> None:
        import tempfile
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        ErrorIF.init()
        self.tmp = tempfile.mkdtemp(prefix="tri_agent_")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.script = os.path.join(self.tmp, "test_agent.py")
        with open(self.script, "w", encoding="utf-8") as f:
            f.write(_TEST_AGENT_SRC)
        self.down = os.path.join(self.tmp, "down")
        self.log  = os.path.join(self.tmp, "posted.txt")
        self.port = _free_port()
        p2 = mock.patch.dict(os.environ, {"TEST_AGENT_PORT": str(self.port), "TEST_AGENT_DOWN": self.down,
                                          "TEST_AGENT_LOG": self.log})
        p2.start()
        self.addCleanup(p2.stop)
        self.config = {"agent_name": "test_agent", "agent_python": sys.executable, "agent_script": self.script,
                       "server_ip": "127.0.0.1", "server_port": self.port, "polling_ms": 20,
                       "connect_timeout_sec": 1, "server_start_wait_sec": 0.5,
                       "retry_max": 3, "retry_wait_sec": 0.05, "reconnect_interval_sec": 0.3}

    def _start(self, func, q, col_def, config) -> None:
        stop = threading.Event()
        t = threading.Thread(target=func, args=(q, col_def, config, stop), daemon=True)
        t.start()
        self.addCleanup(lambda: (stop.set(), q.put(None), t.join(timeout=20)))

    def _check_find(self, func, trigger_name: str) -> None:
        q = queue.Queue()
        self._start(func, q, FIND_COL_DEF, self.config)
        self.assertTrue(wait_until(lambda: q.qsize() >= 3))
        conn_events()

        open(self.down, "w").close()                                          # エージェントが止まる
        self.assertTrue(wait_until(lambda: cs.get_is_connectable(trigger_name) is False))
        self.assertEqual(sum(1 for e in wait_conn_events() if "Not Connectable" in e["message"]), 1)

        os.remove(self.down)                                                  # 起動し直したエージェントにつながる
        self.assertTrue(wait_until(lambda: cs.get_is_connectable(trigger_name) is True, timeout=20))
        n = q.qsize()
        self.assertTrue(wait_until(lambda: q.qsize() >= n + 3))


class TestAgenticNotConnectable(_AgentCase, unittest.TestCase):

    def test_tmpl_agentic_find(self):
        from src.tmpl_agentic.tri_s11_find_agentic import f_agentic_trigger
        self._check_find(f_agentic_trigger, "f_agentic_trigger")



########################################
# 共通の部品（G 以降で使う）
########################################

def _load_policy() -> None:
    """本番と同じポリシー（通信の失敗では止めない）で ErrorIF を初期化する。ファイルが無ければポリシー無し"""
    policy = os.path.join(ROOT, "settings", "error_policy.json")
    ErrorIF.init(policy_path=policy) if os.path.isfile(policy) else ErrorIF.init()


def _start_trigger(case: unittest.TestCase, func, q: queue.Queue, col_def, config) -> threading.Event:
    stop = threading.Event()
    t = threading.Thread(target=func, args=(q, col_def, config, stop), daemon=True)
    t.start()
    case.addCleanup(lambda: (stop.set(), q.put(None), t.join(timeout=20)))
    return stop


def _startup_comm_phases(trigger_name: str) -> List[str]:
    return [e.get("phase") for e in ErrorIF.drain_events()
            if e.get("trigger_id") == trigger_name and e.get("category") == "comm"]


########################################
# G. RDB（PostgreSQL。偽の接続を使う）
########################################

class _FakePg:
    """偽の PostgreSQL（down=True の間は、接続も SQL も通信の失敗になる）"""
    down          = False
    schema_errors: List[str] = []
    rows: List[tuple] = []        # コミットされた INSERT の値
    source: List[dict] = []       # find が読むテーブルの行

    @classmethod
    def reset(cls) -> None:
        cls.down, cls.schema_errors, cls.rows, cls.source = False, [], [], []


class _FakePgConn:
    def __init__(self) -> None:
        self.closed, self.pending = False, []

    def _check(self) -> None:
        import pg8000.dbapi as pg_dbapi
        if _FakePg.down or self.closed:
            raise pg_dbapi.InterfaceError("network error (fake)")

    def cursor(self):
        self._check()
        return _FakePgCursor(self)

    def commit(self) -> None:
        self._check()
        _FakePg.rows.extend(self.pending)
        self.pending = []

    def rollback(self) -> None:
        self._check()
        self.pending = []

    def close(self) -> None:
        self.closed = True


class _FakePgCursor:
    def __init__(self, conn: _FakePgConn) -> None:
        self.conn = conn

    def execute(self, sql: str, params=None) -> None:
        self.conn._check()
        if sql.lstrip().upper().startswith("INSERT"):
            self.conn.pending.append(tuple(params))

    def close(self) -> None:
        pass


def _patch_rdb(case: unittest.TestCase) -> None:
    import pg8000.dbapi as pg_dbapi
    import src.it_rdb.tri_s19_rdb_postgres as rdb

    def _ping(conn) -> bool:
        try:
            conn._check()
            return True
        except Exception:
            return False

    def _connect_if_needed(conn, config, caller_name, log_file):
        if conn is not None and _ping(conn):
            return conn
        if _FakePg.down:
            raise pg_dbapi.InterfaceError("Can't create a connection (fake)")
        return _FakePgConn()

    def _fetch_diff(conn, sch_tab_pg, key_pg, last_ts, limit):
        conn._check()
        return [r for r in _FakePg.source if str(r["ts"]) > str(last_ts)][:limit]

    for name, fn in (("pg_ping", _ping), ("pg_connect_if_needed", _connect_if_needed), ("pg_fetch_diff", _fetch_diff),
                     ("pg_ensure_table", lambda *a, **k: None),
                     ("pg_check_schema", lambda *a, **k: list(_FakePg.schema_errors))):
        p = mock.patch.object(rdb, name, fn)
        p.start()
        case.addCleanup(p.stop)


@unittest.skipUnless(_has_module("pg8000"), "pg8000 not installed")
class TestRdbNotConnectable(unittest.TestCase):

    def setUp(self) -> None:
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        _patch_rdb(self)
        _FakePg.reset()
        _load_policy()
        self.common = {"pg_host": "127.0.0.1", "pg_port": "5432", "pg_dbname": "test",
                       "retry_max": 3, "retry_wait_sec": 0.05, "reconnect_interval_sec": 0.3}

    def _start_act(self) -> Tuple[queue.Queue, threading.Event]:
        from src.it_rdb.tri_s12_act_rdb_postgres import a_insert_to_pg_trigger
        q = queue.Queue()
        stop = _start_trigger(self, a_insert_to_pg_trigger, q, SEG_COL_DEF,
                              {**self.common, "pg_table": "t_test", "exclude_last_row": False})
        return q, stop

    @staticmethod
    def _written() -> List[int]:
        return [r[-1] // 10 for r in _FakePg.rows]                           # INSERT の値（seg_id, v）の v → seg_id

    def test_act_startup_down_then_write_and_not_connectable(self):
        _FakePg.down = True                                                   # 起動時に DB が無い
        q, stop = self._start_act()
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_pg_insert_imq_trigger") is False))
        self.assertEqual(_startup_comm_phases("a_pg_insert_imq_trigger"), ["startup"])  # 止まらず、startup で1回
        self.assertFalse(stop.is_set())
        _FakePg.down = False                                                  # DB が戻る（キューが空の間に再接続）
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_pg_insert_imq_trigger") is True))
        q.put(_segment(1))
        self.assertTrue(wait_until(lambda: self._written() == [1]))
        conn_events()

        _FakePg.down = True                                                   # 書込みの途中で切れる → リトライ → not_connectable
        q.put(_segment(2))
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_pg_insert_imq_trigger") is False))
        q.put(_segment(3))                                                    # 接続できない間 → スキップ（既定 skip）
        time.sleep(0.1)
        self.assertEqual(sum(1 for e in wait_conn_events() if "Not Connectable" in e["message"]), 1)
        _FakePg.down = False
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_pg_insert_imq_trigger") is True))
        q.put(_segment(4))
        self.assertTrue(wait_until(lambda: 4 in self._written()))
        self.assertEqual(self._written(), [1, 4])

    def test_act_schema_error_stops(self):
        _FakePg.schema_errors = ["column mismatch (fake)"]                    # テーブルの構成が違う → 起動時に止める
        q, stop = self._start_act()
        self.assertTrue(wait_until(stop.is_set))

    def test_alive_not_connectable_and_back(self):
        import src.it_rdb.tri_s12_act_rdb_alive as alive
        for name, fn in (("pg_ensure_alive_table", lambda *a, **k: None),
                         ("pg_load_alive_states", lambda *a, **k: {})):
            p = mock.patch.object(alive, name, fn)
            p.start()
            self.addCleanup(p.stop)
        col_def = {i: [n, "str", "key", "key", ""] for i, n in enumerate(base.SEG_HEADER_COLS)}
        col_def.update({4: ["device_key", "str", "key", "key", ""], 5: ["status", "str", "key", "key", ""]})
        q = queue.Queue()
        _start_trigger(self, alive.a_upsert_alive_to_pg_trigger, q, col_def, {**self.common, "teams_notify": False})
        seg = lambda n: [[n, 1, dt.now(), 0, f"dev{n}", "OK"]]
        keys = lambda: [r[0] for r in _FakePg.rows]                           # UPSERT の値の先頭（device_key）
        q.put(seg(1))
        self.assertTrue(wait_until(lambda: keys() == ["dev1"]))
        conn_events()
        _FakePg.down = True                                                   # DB が止まる → リトライ → not_connectable
        q.put(seg(2))
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_upsert_alive_to_pg_trigger") is False))
        _FakePg.down = False
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_upsert_alive_to_pg_trigger") is True))
        q.put(seg(3))
        self.assertTrue(wait_until(lambda: "dev3" in keys()))
        self.assertEqual(keys(), ["dev1", "dev3"])                           # 接続できない間のセグメント（dev2）は書かない

    def test_find_not_connectable_and_back(self):
        from src.it_rdb.tri_s11_find_rdb_postgres import f_table_diff_bulk_trigger
        _FakePg.source = [{"ts": "2099-01-01 00:00:01", "v": 1}]
        q = queue.Queue()
        _start_trigger(self, f_table_diff_bulk_trigger, q, FIND_COL_DEF,
                       {**self.common, "table_key_list": [{"schema": "public", "table": "t_find", "key": "ts"}],
                        "interval_sec": 0.05, "reset_state": True})

        def payload_ts() -> List[str]:
            out = []
            while True:
                try:
                    item = q.get_nowait()
                except queue.Empty:
                    return out
                if isinstance(item[1][0], dict):
                    out.append(item[1][0]["dt_to"])

        got: List[str] = []
        self.assertTrue(wait_until(lambda: (got.extend(payload_ts()) or "2099-01-01 00:00:01" in got)))
        conn_events()
        _FakePg.down = True                                                   # DB が止まる
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("f_table_diff_bulk_trigger") is False))
        self.assertEqual(sum(1 for e in wait_conn_events() if "Not Connectable" in e["message"]), 1)
        _FakePg.source.append({"ts": "2099-01-01 00:00:02", "v": 2})
        _FakePg.down = False                                                  # 戻る → 取り込みを再開
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("f_table_diff_bulk_trigger") is True))
        self.assertTrue(wait_until(lambda: (got.extend(payload_ts()) or "2099-01-01 00:00:02" in got)))


########################################
# H. FTP act（pyftpdlib の FTP サーバを止めたり戻したりする）
########################################

def _ftp_available() -> bool:
    return _has_module("pyftpdlib")


class _FtpServer:
    """localhost の FTP サーバ（同じポートで止めたり戻したりできる）"""

    def __init__(self, port: int, root: str) -> None:
        self.port, self.root = port, root
        self._srv = None
        self._t: Optional[threading.Thread] = None

    def start(self) -> None:
        from pyftpdlib.authorizers import DummyAuthorizer
        from pyftpdlib.handlers    import FTPHandler
        from pyftpdlib.servers     import FTPServer
        auth = DummyAuthorizer()
        auth.add_user("user", "pass", self.root, perm="elradfmwMT")
        handler = type("_H", (FTPHandler,), {"authorizer": auth})
        self._srv = FTPServer(("127.0.0.1", self.port), handler)
        self._t = threading.Thread(target=self._srv.serve_forever, kwargs={"timeout": 0.1}, daemon=True)
        self._t.start()

    def stop(self) -> None:
        if self._srv is not None:
            self._srv.close_all()
            self._t.join(timeout=5)
            self._srv = None


@unittest.skipUnless(_ftp_available(), "pyftpdlib not installed")
class TestFtpActNotConnectable(unittest.TestCase):

    def setUp(self) -> None:
        import tempfile
        self.tmp = tempfile.mkdtemp(prefix="tri_ftp_act_")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.root, self.local = os.path.join(self.tmp, "server"), os.path.join(self.tmp, "local")
        os.makedirs(self.root)
        os.makedirs(self.local)
        self.port = _free_port()
        for p in (mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"}),
                  mock.patch.dict(os.environ, {"FTP_SEND_HOST": "127.0.0.1", "FTP_SEND_PORT": str(self.port),
                                               "FTP_SEND_USER": "user", "FTP_SEND_PASS": "pass"})):
            p.start()
            self.addCleanup(p.stop)
        _load_policy()

    def _file(self, name: str) -> List[list]:
        path = os.path.join(self.local, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(name)
        return [[1, 1, dt.now(), 0, path]]

    def test_server_down_wait_and_send_after_reconnect(self):
        from src.comm_ftp.tri_s12_act_ftp import a_send_ftp_trigger
        srv = _FtpServer(self.port, self.root)
        srv.start()
        self.addCleanup(srv.stop)
        col_def = {i: [n, "str", "key", "key", ""] for i, n in enumerate(base.SEG_HEADER_COLS)}
        col_def[4] = ["file_path", "str", "key", "key", ""]
        q = queue.Queue()
        _start_trigger(self, a_send_ftp_trigger, q, col_def,
                       {"file_path_col": 0, "stable_check_n": 0, "after_upload": "delete", "timeout_sec": 2,
                        "retry_max": 3, "retry_wait_sec": 0.05, "reconnect_interval_sec": 0.3})
        sent = lambda: sorted(os.listdir(self.root))
        q.put(self._file("a.txt"))
        self.assertTrue(wait_until(lambda: sent() == ["a.txt"]))
        conn_events()
        srv.stop()                                                            # サーバが止まる → リトライ → not_connectable
        q.put(self._file("b.txt"))
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_send_ftp_trigger") is False, timeout=20))
        self.assertEqual(sum(1 for e in wait_conn_events() if "Not Connectable" in e["message"]), 1)
        srv.start()                                                           # 戻る → 待っていたファイルを送る（既定 wait）
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_send_ftp_trigger") is True, timeout=20))
        self.assertTrue(wait_until(lambda: sent() == ["a.txt", "b.txt"]))


########################################
# I. SMB act / find（偽の smbclient の関数を使う）
########################################

class _FakeSmb:
    """偽の SMB 共有フォルダ（down=True の間は、どの操作も通信の失敗になる）"""

    def __init__(self) -> None:
        self.down = False
        self.files: Dict[str, bytes] = {}
        self.sessions: List[dict] = []    # register_session に渡された引数（server 以外）

    def _check(self) -> None:
        if self.down:
            raise ConnectionError("fake smb is down")

    def register_session(self, server, **kwargs) -> None:
        self._check()
        self.sessions.append(kwargs)

    def stat(self, path):
        self._check()
        size = len(self.files.get(path, b""))
        return mock.MagicMock(st_mtime=0.0, st_size=size)

    def makedirs(self, path, exist_ok=False) -> None:
        self._check()

    def exists(self, path) -> bool:
        self._check()
        return path in self.files

    def open_file(self, path, mode="rb"):
        self._check()
        fake = self

        class _W(io.BytesIO):
            def close(self_inner):
                if "w" in mode:
                    fake.files[path] = self_inner.getvalue()
                super().close()
        return _W(b"" if "w" in mode else fake.files.get(path, b""))

    def replace(self, src, dst) -> None:
        self._check()
        self.files[dst] = self.files.pop(src)

    def remove(self, path) -> None:
        self.files.pop(path, None)

    def scandir(self, path):
        self._check()
        out = []
        for p in sorted(self.files):
            if p.rsplit("\\", 1)[0] == path:
                e = mock.MagicMock(path=p)
                e.name = p.rsplit("\\", 1)[1]
                e.is_file.return_value, e.is_dir.return_value = True, False
                out.append(e)
        return out


@unittest.skipUnless(_has_module("smbclient"), "smbprotocol not installed")
class TestSmbNotConnectable(unittest.TestCase):

    def setUp(self) -> None:
        import tempfile
        import smbclient
        import smbclient.path
        self.smb = _FakeSmb()
        for p in [mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})] + [
                mock.patch.object(smbclient, n, getattr(self.smb, n))
                for n in ("register_session", "stat", "makedirs", "open_file", "replace", "remove", "scandir")] + [
                mock.patch.object(smbclient.path, "exists", self.smb.exists),
                mock.patch.object(smbclient, "reset_connection_cache", lambda: None)]:
            p.start()
            self.addCleanup(p.stop)
        self.tmp = tempfile.mkdtemp(prefix="tri_smb_")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        _load_policy()
        self.common = {"smb_server": "srv", "smb_share": "share",
                       "retry_max": 3, "retry_wait_sec": 0.05, "reconnect_interval_sec": 0.3}

    def test_act_down_wait_and_send_after_reconnect(self):
        from src.comm_smb.tri_s12_act_smb import a_send_remote_file_trigger
        col_def = {i: [n, "str", "key", "key", ""] for i, n in enumerate(base.SEG_HEADER_COLS)}
        col_def[4] = ["file_path", "str", "key", "key", ""]
        q = queue.Queue()
        _start_trigger(self, a_send_remote_file_trigger, q, col_def,
                       {**self.common, "file_path_col": 0, "stable_check_n": 0, "after_upload": "delete"})

        def seg(name: str) -> List[list]:
            path = os.path.join(self.tmp, name)
            with open(path, "w", encoding="utf-8") as f:
                f.write(name)
            return [[1, 1, dt.now(), 0, path]]

        sent = lambda: sorted(p.rsplit("\\", 1)[1] for p in self.smb.files)
        q.put(seg("a.txt"))
        self.assertTrue(wait_until(lambda: sent() == ["a.txt"]))
        conn_events()
        self.smb.down = True                                                  # 共有フォルダに届かない → not_connectable
        q.put(seg("b.txt"))
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_send_remote_file_trigger") is False))
        self.assertEqual(sum(1 for e in wait_conn_events() if "Not Connectable" in e["message"]), 1)
        self.smb.down = False                                                 # 戻る → 待っていたファイルを送る（既定 wait）
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("a_send_remote_file_trigger") is True))
        self.assertTrue(wait_until(lambda: sent() == ["a.txt", "b.txt"]))

    def test_find_startup_down_then_back(self):
        from src.comm_smb.tri_s11_find_smb import f_find_remote_file_trigger
        self.smb.down = True                                                  # 起動時に届かない → 止まらない
        self.smb.files["\\\\srv\\share\\x.csv"] = b"1"
        col_def = {i: [f"c{i}", "str", "key", "key", ""] for i in range(13)}
        q = queue.Queue()
        stop = _start_trigger(self, f_find_remote_file_trigger, q, col_def,
                              {**self.common, "interval_sec": 0.05, "stable_check_n": 0, "ignore_old": False})
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("f_find_remote_file_trigger") is False))
        self.assertEqual(_startup_comm_phases("f_find_remote_file_trigger"), ["startup"])
        self.assertFalse(stop.is_set())
        self.smb.down = False
        self.assertTrue(wait_until(lambda: cs.get_is_connectable("f_find_remote_file_trigger") is True))
        self.assertTrue(wait_until(lambda: q.qsize() >= 1))

    def test_find_session_with_domain(self):
        from src.comm_smb.tri_s11_find_smb import f_find_remote_file_trigger
        col_def = {i: [f"c{i}", "str", "key", "key", ""] for i in range(13)}
        _start_trigger(self, f_find_remote_file_trigger, queue.Queue(), col_def,
                       {**self.common, "interval_sec": 0.05, "stable_check_n": 0, "ignore_old": False,
                        "smb_username": "user", "smb_password": "pw", "smb_domain": "DOM"})
        self.assertTrue(wait_until(lambda: len(self.smb.sessions) >= 1))     # ドメインつき → DOMAIN\user
        self.assertEqual(self.smb.sessions[0]["username"], "DOM\\user")
        self.assertEqual(self.smb.sessions[0]["password"], "pw")


if __name__ == "__main__":
    unittest.main(verbosity=2)
