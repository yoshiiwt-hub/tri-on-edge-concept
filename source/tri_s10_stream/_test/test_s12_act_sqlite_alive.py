# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Single Run Test [Act Trigger: SQLite Alive]
# Ver: 01.a.20261116    By: IWATA,Y.'s Agent AI
# Mod: 新規作成（死活監視の結果を機器ごとに最新の1行で保つ a_upsert_alive_to_sqlite_trigger）
########################################
a_upsert_alive_to_sqlite_trigger に、tri_s11_find_alive と同じ形のセグメントを渡し、一時フォルダの SQLite を確かめるテスト

    確認すること
      1. テーブルが無ければ作り、機器ごとに1行（device_key が主キー）で、2回目からは上書きされる。updated_at が付く
      2. 区切り行・device_key が空の行は書かない。同じ機器が複数行あれば後の行の値になる
      3. 状態の変化（OK ↔ NG）だけを通知する。前回の状態が無い機器は NG のときだけ。teams_notify: false で通知を要求しない
      4. 起動時にテーブルから前回の状態を読み、再起動の後も変化がなければ通知しない
      5. 既存のテーブルの列が合わない・device_key に一意性が無いとき、テーブルを変えずに起動時に止まる
      6. 設定の誤り（table_name の書式、必須の列が無い）で起動時に止まる
      7. 運用系メトリクス（devices_total / ng_count / ng_devices / last_checked_at / total_segments / state_changes）が出る
      8. 今ある a_store_seg_trigger は今までどおり全行を保存する

実行: tri_s10_stream/ で  python -m unittest _test.test_s12_act_sqlite_alive -v
"""
import os
import queue
import shutil
import sqlite3
import sys
import tempfile
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

ALIVE_COLS = ["device_key", "device_name", "status", "checked_at", "host", "port", "rtt_ms", "detail"]
COL_DEF: Dict[int, Any] = {i: [n, "str", "key", "key", ""] for i, n in enumerate(list(base.SEG_HEADER_COLS) + ALIVE_COLS)}
SEP = "____"


def _row(seg_id: int, ind: int, key: Any, status: Any, name: str = "", detail: str = "connected") -> List[Any]:
    """tri_s11_find_alive の1行（先頭の固定4列は stream_core が付ける形）"""
    return [seg_id, ind, dt.now(), 0, key, name or f"name_{key}", status, dt(2026, 11, 16, 10, 0, seg_id),
            "127.0.0.1", 502, 1.5 if status == "OK" else None, detail]


class TestActSqliteAlive(unittest.TestCase):

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="tri_sqlite_alive_")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.db = os.path.join(self.tmp, "alive.db")
        self.notified: List[str] = []
        self.logs: List[Tuple[str, str]] = []

        def _log(tag: str, msg: str, log_file: str = "") -> None:
            self.logs.append((str(msg), log_file))

        for p in (mock.patch.object(base, "metrics_notify", lambda mod, msg="": self.notified.append(msg)),
                  mock.patch.object(base, "output_log", _log)):
            p.start()
            self.addCleanup(p.stop)
        ErrorIF.init()
        ErrorIF.drain_events()

    # ---- helper ----
    def _run(self, segments: List[List[List[Any]]], col_def: Dict[int, Any] = COL_DEF, func=None,
             **config: Any) -> threading.Event:
        from src.tri_s12_act_sqlite import a_upsert_alive_to_sqlite_trigger
        func = func or a_upsert_alive_to_sqlite_trigger
        q, stop = queue.Queue(), threading.Event()
        for seg in segments:
            q.put(seg)
        q.put(None)
        t = threading.Thread(target=func, args=(q, col_def, {"db_path": self.db, **config}, stop), daemon=True)
        t.start()
        t.join(timeout=20)
        self.assertFalse(t.is_alive())
        return stop

    def _rows(self, table: str = "triedge_device_alive") -> Dict[str, Tuple]:
        with sqlite3.connect(self.db) as conn:
            cur = conn.execute(f'SELECT * FROM "{table}" ORDER BY device_key')
            names = [d[0] for d in cur.description]
            return {r[0]: dict(zip(names, r)) for r in cur.fetchall()}

    def _startup_errors(self) -> List[dict]:
        return [e for e in ErrorIF.drain_events() if e.get("phase") == "startup"]

    # ---- tests ----
    def test_create_and_upsert_one_row_per_device(self):
        stop = self._run([[_row(1, 1, "dev01", "OK"), _row(1, 2, "dev02", "OK")],
                          [_row(2, 1, "dev01", "NG", detail="timeout")]])
        self.assertFalse(stop.is_set())
        rows = self._rows()
        self.assertEqual(sorted(rows), ["dev01", "dev02"])                  # 機器ごとに1行
        self.assertEqual(rows["dev01"]["status"], "NG")                    # 2回目で上書き
        self.assertEqual(rows["dev01"]["detail"], "timeout")
        self.assertIsNone(rows["dev01"]["rtt_ms"])
        self.assertEqual(rows["dev02"]["port"], 502)
        self.assertTrue(rows["dev01"]["updated_at"])
        with sqlite3.connect(self.db) as conn:
            info = conn.execute('PRAGMA table_info("triedge_device_alive")').fetchall()
        self.assertEqual([r[1] for r in info], ALIVE_COLS + ["updated_at"])
        self.assertEqual([r[1] for r in info if r[5] > 0], ["device_key"])  # 主キー

    def test_skip_separator_and_empty_key_and_last_row_wins(self):
        self._run([[_row(1, 1, "dev01", "OK"), _row(1, 2, "", "OK"), _row(1, 3, SEP, SEP),
                    _row(1, 4, "dev01", "NG")]])
        rows = self._rows()
        self.assertEqual(list(rows), ["dev01"])
        self.assertEqual(rows["dev01"]["status"], "NG")

    def test_notify_changes_only(self):
        self._run([[_row(1, 1, "dev01", "OK"), _row(1, 2, "dev02", "NG")],     # 初回: NG の dev02 だけ
                   [_row(2, 1, "dev01", "NG"), _row(2, 2, "dev02", "NG")],     # dev01 が OK → NG
                   [_row(3, 1, "dev01", "NG"), _row(3, 2, "dev02", "NG")]])    # 変化なし
        self.assertEqual(len(self.notified), 2, self.notified)
        self.assertIn("dev02）：- → NG", self.notified[0])
        self.assertNotIn("dev01", self.notified[0])
        self.assertIn("dev01）：OK → NG", self.notified[1])
        changes = [(m, lf) for m, lf in self.logs if m.startswith("状態変化:")]
        self.assertEqual(len(changes), 2)
        self.assertTrue(all(lf for _, lf in changes))                        # 状態の変化はログファイルにも残す

    def test_teams_notify_false(self):
        self._run([[_row(1, 1, "dev01", "NG")]], teams_notify=False)
        self.assertEqual(self.notified, [])
        self.assertTrue(any(m.startswith("状態変化:") for m, _ in self.logs))  # ログには残す

    def test_loads_previous_states_on_restart(self):
        self._run([[_row(1, 1, "dev01", "NG")]])
        self.notified.clear()
        self._run([[_row(1, 1, "dev01", "NG")]])                             # 再起動しても変化なし → 通知なし
        self.assertEqual(self.notified, [])
        self._run([[_row(1, 1, "dev01", "OK")]])                             # NG → OK
        self.assertEqual(len(self.notified), 1)
        self.assertIn("dev01）：NG → OK", self.notified[0])

    def test_existing_table_mismatch_stops(self):
        cases = {
            "wrong_columns": 'CREATE TABLE "t1" ("device_key" TEXT PRIMARY KEY, "status" TEXT)',
            "no_unique_key": 'CREATE TABLE "t2" (' + ", ".join(
                f'"{c}" {t}' for c, t in [("device_key", "TEXT"), ("device_name", "TEXT"), ("status", "TEXT"),
                                          ("checked_at", "TEXT"), ("host", "TEXT"), ("port", "INTEGER"),
                                          ("rtt_ms", "REAL"), ("detail", "TEXT"), ("updated_at", "TEXT")]) + ")",
        }
        for table, ddl in zip(("t1", "t2"), cases.values()):
            with self.subTest(table=table):
                with sqlite3.connect(self.db) as conn:
                    conn.execute(ddl)
                    conn.execute(f'INSERT INTO "{table}" ("device_key", "status") VALUES (?, ?)', ("keep", "OK"))
                stop = self._run([[_row(1, 1, "dev01", "OK")]], table_name=table)
                self.assertTrue(stop.is_set())
                self.assertTrue(any(e.get("category") == "config" for e in self._startup_errors()))
                with sqlite3.connect(self.db) as conn:
                    self.assertEqual(conn.execute(f'SELECT "device_key" FROM "{table}"').fetchall(), [("keep",)])

    def test_config_errors_stop(self):
        no_status = {k: v for k, v in COL_DEF.items() if v[0] != "status"}
        for name, kwargs in (("bad_table_name", {"table_name": "bad-name"}),
                             ("missing_status", {"col_def": no_status})):
            with self.subTest(name=name):
                stop = self._run([[_row(1, 1, "dev01", "OK")]], **kwargs)
                self.assertTrue(stop.is_set())
                self.assertTrue(self._startup_errors())

    def test_metrics(self):
        self._run([[_row(1, 1, "dev01", "OK"), _row(1, 2, "dev02", "NG")]])
        m = base.metrics_get_all().get("ACT_SQLite", {})
        for key in ("devices_total", "ng_count", "ng_devices", "last_checked_at", "total_segments", "state_changes"):
            self.assertIn(key, m)
        self.assertEqual(m["devices_total"], 2)
        self.assertEqual(m["ng_count"], 1)
        self.assertEqual(m["ng_devices"], "name_dev02")
        self.assertEqual(m["last_checked_at"], "2026-11-16 10:00:01")

    def test_store_seg_trigger_unchanged(self):
        from src.tri_s12_act_sqlite import a_store_seg_trigger
        col_def = {0: ["seg_id", "int", "key", "key", ""], 1: ["seg_ind", "int", "key", "key", ""],
                   2: ["v", "float", "num", "obj", ""]}
        stop = self._run([[[1, 1, 0.5], [1, 2, 1.5]], [[1, 2, 2.5]]], col_def=col_def,
                         func=a_store_seg_trigger, table_name="segments")
        self.assertFalse(stop.is_set())
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute('SELECT * FROM "segments" ORDER BY seg_ind').fetchall(),
                             [(1, 1, 0.5), (1, 2, 2.5)])                      # 同じ行は上書き（INSERT OR REPLACE）


if __name__ == "__main__":
    unittest.main(verbosity=2)
