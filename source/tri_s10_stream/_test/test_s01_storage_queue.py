# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Unit Test [Storage Queue]
# Ver: 01.a.20261107    By: IWATA, Y.'s Agent AI
# Mod: 新規作成（tri_s00_supervise_base から tri_s01_storage_queue へ移した Storage Queue の部品）
########################################
tri_s01_storage_queue（stq）の単体テスト。一時フォルダの SQLite の DB で、StreamCore と Act の使い方を順に確かめる

    確認すること
      1. DB のファイル（work/spool/_storage_queue.db）とテーブル名（storage_queue）が、移す前と同じであること
      2. col_def からテーブルの列定義を作れること（型の対応、未知の型は TEXT）
      3. 初期化 → 登録（datetime は文字列にする）→ 取得（取得済みにする。2回目は無い）→ 状態の更新 → 掃除 が動くこと
      4. エラーのセグメント・レコードをエラーテーブルへ移せること。エラーテーブルが上限を超えたら、掃除で空になること
      5. 掃除は、間隔・件数のどちらにも届かないときは何もしないこと
      6. base に Storage Queue の関数・定数・パスが残っていないこと

実行: tri_s10_stream/ で  python -m unittest _test.test_s01_storage_queue -v
"""
import os
import shutil
import sys
import tempfile
import time
import unittest
from   datetime import datetime as dt
from   typing   import Any, Dict
from   unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # tri_s10_stream/（src の親）
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import src.tri_s00_supervise_base as base
import src.tri_s01_storage_queue  as stq


COL_DEF: Dict[int, Any] = {
    0: ["seg_id",     "int",   "key", "key", ""],
    1: ["seg_ind",    "int",   "key", "key", ""],
    2: ["seg_ts",     "time",  "key", "key", ""],
    3: ["seg_status", "int",   "key", "key", ""],
    4: ["v",          "float", "num", "obj", ""],
    5: ["memo",       "bytes", "key", "obj", ""],   # 未知の型 → TEXT
}


def _seg(seg_id: int, n: int = 3) -> list:
    return [[seg_id, i, dt(2026, 11, 7, 12, 0, 0, i * 1000), 0, i * 1.5, f"m{i}"] for i in range(1, n + 1)]


class TestStorageQueue(unittest.TestCase):

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="tri_stq_")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.path = os.path.join(self.tmp, "stq.db")
        self.tab  = stq.get_storage_queue_tab()
        self.tab_col = stq.coldef_to_table_col(COL_DEF)
        self.conn = stq.stq_initialize_db(self.path, self.tab, self.tab_col)   # StreamCore（書く側）
        self.addCleanup(self.conn.close)
        self.sql  = stq.stq_prepare_sql(self.tab, self.tab_col)
        self.act  = stq.stq_open(self.path)                                     # Act（読む側）
        self.addCleanup(self.act.close)

    def _count(self, where: str = "", tab: str = "") -> int:
        return self.conn.execute(f"SELECT count(*) FROM {tab or self.tab} {where}").fetchone()[0]

    def test_path_and_table_name_are_unchanged(self):
        self.assertEqual(stq.get_storage_queue_path(),
                         os.path.join(base.get_work_sub_dirs()["spool"], "_storage_queue.db"))
        self.assertEqual(stq.get_storage_queue_tab(), "storage_queue")

    def test_coldef_to_table_col(self):
        self.assertEqual(self.tab_col[2], ["seg_ts", "TEXT", ""])
        self.assertEqual(self.tab_col[3], ["seg_status", "INTEGER", ""])
        self.assertEqual(self.tab_col[4], ["v", "REAL", ""])
        self.assertEqual(self.tab_col[5], ["memo", "TEXT", ""])

    def test_insert_select_update_cleanup(self):
        stq.stq_insert_new_seg(self.conn, self.sql["insert_new_seg"], _seg(100))
        self.assertEqual(self._count(), 3)
        rows = stq.stq_select_new_seg(self.act, self.sql["select_new_seg"], self.sql["update_seg_status"], 100)
        self.assertEqual([r[1] for r in rows], [1, 2, 3])                     # seg_ind の順
        self.assertEqual(rows[0][2], "2026-11-07 12:00:00.001000")            # datetime は文字列で登録される
        self.assertIsNone(stq.stq_select_new_seg(self.act, self.sql["select_new_seg"],
                                                 self.sql["update_seg_status"], 100))   # 取得済み（status=1）
        stq.stq_update_rec(self.act, self.sql["update_rec_status"], 100, 1, 2)
        self.assertEqual(self._count("WHERE seg_status = 2"), 1)
        stq.stq_update_seg(self.act, self.sql["update_seg_status"], 100, 2)
        self.assertEqual(self._count("WHERE seg_status = 2"), 3)
        last, cnt, deleted, cleared = stq.stq_cleanup(
            self.conn, self.sql["delete_by_status"], 2, self.sql["count_error"], self.sql["clear_error"],
            0.0, 0, 60.0, 1000)                                               # 間隔が過ぎている → 掃除する
        self.assertEqual((cnt, deleted, cleared), (0, 3, False))
        self.assertGreater(last, 0.0)
        self.assertEqual(self._count(), 0)

    def test_cleanup_not_due_does_nothing(self):
        stq.stq_insert_new_seg(self.conn, self.sql["insert_new_seg"], _seg(200))
        stq.stq_update_seg(self.conn, self.sql["update_seg_status"], 200, 2)
        now = time.time()
        res = stq.stq_cleanup(self.conn, self.sql["delete_by_status"], 2, self.sql["count_error"],
                              self.sql["clear_error"], now, 5, 60.0, 1000)
        self.assertEqual(res, (now, 5, 0, False))
        self.assertEqual(self._count(), 3)
        res = stq.stq_cleanup(self.conn, self.sql["delete_by_status"], 2, self.sql["count_error"],
                              self.sql["clear_error"], now, 1000, 60.0, 1000)  # 件数が届いた → 掃除する
        self.assertEqual(res[2], 3)

    def test_trans_to_error_and_error_table_limit(self):
        stq.stq_insert_new_seg(self.conn, self.sql["insert_new_seg"], _seg(300))
        stq.stq_trans_rec_to_error(self.act, self.sql["trans_rec_to_error"], self.sql["update_rec_status"], 300, 2, "rec err")
        stq.stq_trans_seg_to_error(self.act, self.sql["trans_seg_to_error"], self.sql["update_seg_status"], 300, "seg err")
        err_tab = f"{self.tab}_error"
        self.assertEqual(self._count(tab=err_tab), 4)                         # レコード1 ＋ セグメント3
        self.assertEqual(self._count("WHERE error_msg = 'rec err'", err_tab), 1)
        self.assertEqual(self._count("WHERE seg_status = 2"), 3)              # 元の行は処理済み（status=2）
        with mock.patch.object(stq, "STQ_ERROR_MAX", 3):                      # 上限を超えた → 空にする
            _, _, deleted, cleared = stq.stq_cleanup(
                self.conn, self.sql["delete_by_status"], 2, self.sql["count_error"], self.sql["clear_error"],
                0.0, 0, 60.0, 1000)
        self.assertEqual((deleted, cleared), (3, True))
        self.assertEqual(self._count(tab=err_tab), 0)

    def test_initialize_recreates_tables(self):
        stq.stq_insert_new_seg(self.conn, self.sql["insert_new_seg"], _seg(400))
        conn2 = stq.stq_initialize_db(self.path, self.tab, self.tab_col)      # 起動のたびに作り直す
        self.addCleanup(conn2.close)
        self.assertEqual(conn2.execute(f"SELECT count(*) FROM {self.tab}").fetchone()[0], 0)

    def test_base_has_no_storage_queue(self):
        for name in ("stq_prepare_sql", "stq_open", "stq_insert_new_seg", "stq_cleanup", "initialize_db",
                     "coldef_to_table_col", "get_storage_queue_path", "get_storage_queue_tab", "STQ_ERROR_MAX"):
            self.assertFalse(hasattr(base, name), name)


if __name__ == "__main__":
    unittest.main(verbosity=2)
