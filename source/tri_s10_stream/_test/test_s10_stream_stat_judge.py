# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Unit Test [Wave Stats: judge]
# Ver: 01.b.20261106    By: IWATA, Y.'s Agent AI
# Mod: act_socket の設定の変更（skip_on_offline → not_connectable_action）に合わせた
#      新規作成（column_register_map の judge: 統計量の値を条件で判定し、判定結果を新しい列に出す）
########################################
統計量（tri_s10_stream_wave_stat）の judge と、統計量の act（a_save_stats_trigger）を通した出力のテスト

    確認すること
      1. 数値の列 → 文字列の判定結果（range。両端を含む。上から順に最初の一致）
      2. 文字列の列 → 数値の判定結果（in。to は自分の列の dtype、in の値は元の列の dtype に自動で変換）
      3. 数値の列の in（整数と小数を同じ値として比べる）
      4. 元の列の値は変わらないこと / 判定結果は新しい列に出ること
      5. どれにも一致しない・元の列の値が出せない → default（省略時は値なし）
      6. judge の番号が元の列より前でも判定できること
      7. 設定の誤りは ValueError（起動時に停止）
      8. init_stats / calc_stats と、実際の act で判定結果が出力先に出ること
         （a_save_stats_trigger: CSV / a_store_stats_trigger: SQLite の列の型 / a_send_stats_trigger: 送信した JSON）

実行: tri_s10_stream/ で  python -m unittest _test.test_s10_stream_stat_judge -v
"""
import csv
import glob
import json
import os
import queue
import shutil
import socket
import sqlite3
import sys
import tempfile
import threading
import unittest
from   unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # tri_s10_stream/（src の親）
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import src.tri_s00_supervise_base as base
import src.tri_s10_stream_wave_stat as stat
from   src.tri_s01_error_if import ErrorIF

C_KEYS = ["seg_id", "seg_ind", "seg_ts", "seg_status", "name", "val"]   # Find の列: name=0 / val=1


def make_segment(names, vals):
    """行: [seg_id, seg_ind, seg_ts, 0, name, val]"""
    return [[20261029000000, i + 1, f"2026-10-29 00:00:0{i}.000000", 0, a, b]
            for i, (a, b) in enumerate(zip(names, vals))]


def compile_specs(specs):
    """specs: [(出力名, dtype, column_register_map のエントリ), ...] をコンパイルする"""
    crm = {str(i): entry for i, (_, _, entry) in enumerate(specs)}
    oc  = {str(i): [name, dtype, "num", "obj", ""] for i, (name, dtype, _) in enumerate(specs)}
    return stat.compile_register_map(crm, oc, C_KEYS)


LEVEL_RULES = [{"range": [None, 20], "to": "LOW"}, {"range": [20, 30], "to": "MID"}, {"range": [30, None], "to": "HIGH"}]


class TestStatJudge(unittest.TestCase):

    def setUp(self):
        self.seg = make_segment(["a", "b", "b", "c"], [10, 20, 30, 40])     # mean=25 / mode=b / min=10 / max=40

    def test_number_to_string_by_range(self):
        comp = compile_specs([
            ("mean", "float", [1, "mean"]), ("min", "float", [1, "min"]), ("max", "float", [1, "max"]),
            ("mean_lv", "str", [0, "judge", LEVEL_RULES]),
            ("min_lv",  "str", [1, "judge", LEVEL_RULES]),
            ("max_lv",  "str", [2, "judge", LEVEL_RULES]),
        ])
        r = stat.build_stats(self.seg, comp)
        self.assertEqual((r["mean_lv"], r["min_lv"], r["max_lv"]), ("MID", "LOW", "HIGH"))
        self.assertAlmostEqual(r["mean"], 25.0, places=4)                      # 元の列は変わらない

    def test_range_boundary_and_first_match(self):
        seg = make_segment(["a"], [20])
        comp = compile_specs([("v", "float", [1, "max"]), ("lv", "str", [0, "judge", LEVEL_RULES])])
        self.assertEqual(stat.build_stats(seg, comp)["lv"], "LOW")            # 20 は LOW / MID の両方に入る → 先の LOW

    def test_string_to_number_by_in_with_auto_conversion(self):
        comp = compile_specs([
            ("mode", "str", [0, "mode"]),
            ("no",   "int", [0, "judge", [{"in": ["a"], "to": 1}, {"in": ["b", "c"], "to": "2"}], "-1"]),
        ])
        r = stat.build_stats(self.seg, comp)
        self.assertEqual(r["mode"], "b")
        self.assertEqual(r["no"], 2)
        self.assertIsInstance(r["no"], int)                                     # "2" → 2（自分の列の dtype）

    def test_in_values_follow_source_dtype(self):
        seg = make_segment([1, 1, 2], [1, 1, 2])                               # name 列が数値でも str の列として比べる
        comp = compile_specs([
            ("mode", "str", [0, "mode"]),
            ("lbl",  "str", [0, "judge", [{"in": [1], "to": "ONE"}]]),          # in の 1 → "1"
            ("cnt",  "int", [1, "count"]),
            ("cnt_j", "str", [2, "judge", [{"in": [3.0], "to": "three"}]]),     # 数値の in: 3 と 3.0 は同じ
        ])
        r = stat.build_stats(seg, comp)
        self.assertEqual((r["lbl"], r["cnt_j"]), ("ONE", "three"))

    def test_default_and_no_value(self):
        comp = compile_specs([
            ("mode", "str", [0, "mode"]),
            ("with_default", "str", [0, "judge", [{"in": ["zzz"], "to": "Z"}], "__"]),
            ("no_default",   "str", [0, "judge", [{"in": ["zzz"], "to": "Z"}]]),
        ])
        r = stat.build_stats(self.seg, comp)
        self.assertEqual(r["with_default"], "__")
        self.assertNotIn("no_default", r)                                      # default 省略 → 値なし

    def test_source_not_available_uses_default(self):
        comp = compile_specs([
            ("name_mean", "float", [0, "mean"]),                               # 文字列の列の平均 → 出せない（_WARN）
            ("lv", "str", [0, "judge", LEVEL_RULES, "NA"]),
        ])
        r = stat.build_stats(self.seg, comp)
        self.assertIn("name_mean_WARN", r)
        self.assertEqual(r["lv"], "NA")

    def test_judge_index_before_source(self):
        comp = compile_specs([("lv", "str", [1, "judge", LEVEL_RULES]), ("mean", "float", [1, "mean"])])
        self.assertEqual(stat.build_stats(self.seg, comp)["lv"], "MID")

    def test_compile_errors(self):
        num = ("mean", "float", [1, "mean"])
        txt = ("mode", "str", [0, "mode"])
        bad = [
            [num, ("j", "str", [9, "judge", LEVEL_RULES])],                     # 元の列が無い
            [num, ("j", "str", [1, "judge", LEVEL_RULES])],                     # 自分自身
            [num, ("j", "str", [0, "judge", LEVEL_RULES]), ("k", "str", [1, "judge", [{"in": ["MID"], "to": "x"}]])],  # judge の judge
            [("t", "time", [-1, "const", "2026"]), ("j", "str", [0, "judge", [{"in": ["x"], "to": "y"}]])],             # 元の列が time
            [num, ("j", "time", [0, "judge", LEVEL_RULES])],                    # 自分の列が time
            [txt, ("j", "str", [0, "judge", [{"range": [0, 1], "to": "x"}]])],  # str の列に range
            [num, ("j", "str", [0, "judge", []])],                              # rules が空
            [num, ("j", "str", [0, "judge", [{"to": "x"}]])],                   # in / range が無い
            [num, ("j", "str", [0, "judge", [{"in": [1], "range": [0, 1], "to": "x"}]])],  # 両方
            [num, ("j", "str", [0, "judge", [{"in": [1]}]])],                   # to が無い
            [num, ("j", "str", [0, "judge", [{"range": [5, 1], "to": "x"}]])],  # 最小 > 最大
            [num, ("j", "str", [0, "judge", [{"range": [1], "to": "x"}]])],     # range の形
            [num, ("j", "int", [0, "judge", [{"in": [1], "to": "LOW"}]])],      # int に変換できない to
            [num, ("j", "int", [0, "judge", [{"in": [1], "to": 1.5}]])],        # 整数でない to
            [num, ("j", "str", [0, "judge", [{"in": ["x"], "to": "y"}]])],      # 数値の列の in に文字列
            [num, ("j", "int", [0, "judge", [{"in": [1], "to": 1}], "NA"])],    # int に変換できない default
            [num, ("j", "str", [0, "judge", [{"in": [True], "to": "y"}]])],     # bool は使えない
            [num, ("j", "str", ["x", "judge", LEVEL_RULES])],                   # 番号でない
        ]
        for specs in bad:
            with self.subTest(specs=specs), self.assertRaises(ValueError):
                compile_specs(specs)

    def test_init_stats_and_calc_stats(self):
        col_def = {i: [name, "str", "key", "key", ""] for i, name in enumerate(C_KEYS)}
        config = {
            "column_register_map": {"0": [1, "mean"], "1": [0, "judge", LEVEL_RULES, "__"]},
            "out_coldef": {"0": ["val_mean", "float", "num", "obj", ""], "1": ["val_level", "str", "cat", "exp", ""]},
        }
        ctx = stat.init_stats(config, col_def, "TEST", "test_trigger")
        self.assertEqual(ctx.base_col_names[-2:], ["val_mean", "val_level"])
        r = stat.calc_stats(ctx, self.seg, self.seg[0][0])
        self.assertEqual(r["val_level"], "MID")


# act で使う統計量の設定（val の平均 → 判定 / name の最頻値 → 判定）
ACT_STATS = {
    "column_register_map": {"0": [1, "mean"], "1": [0, "mode"],
                            "2": [0, "judge", LEVEL_RULES, "__"],
                            "3": [1, "judge", [{"in": ["a"], "to": 1}, {"in": ["b"], "to": 2}], -1]},
    "out_coldef": {"0": ["val_mean",  "float", "num", "obj", ""], "1": ["name_mode", "str", "cat", "exp", ""],
                   "2": ["val_level", "str",   "cat", "exp", ""], "3": ["name_no",   "int", "num", "exp", ""]},
}


class TestStatJudgeAct(unittest.TestCase):
    """実際の統計量の act で、判定結果の列が出力先に出ること"""

    def setUp(self):
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        ErrorIF.init()
        self.tmp = tempfile.mkdtemp(prefix="tri_test_judge_")
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def _run(self, func, extra: dict) -> None:
        """act をスレッドで動かし、1セグメントと終了の合図を渡して終わるまで待つ"""
        col_def = {i: [name, "str", "key", "key", ""] for i, name in enumerate(C_KEYS)}
        in_queue, stop_event = queue.Queue(), threading.Event()
        t = threading.Thread(target=func, args=(in_queue, col_def, {**ACT_STATS, **extra}, stop_event), daemon=True)
        t.start()
        in_queue.put(make_segment(["a", "b", "b", "c"], [10, 20, 30, 40]))
        in_queue.put(None)
        t.join(timeout=20)
        stop_event.set()
        self.assertFalse(t.is_alive())

    def test_csv_has_judge_columns(self):
        from src.tri_s12_act_csv import a_save_stats_trigger
        self._run(a_save_stats_trigger, {"save_dir": self.tmp})
        files = glob.glob(os.path.join(self.tmp, "*.csv"))
        self.assertEqual(len(files), 1, os.listdir(self.tmp))
        with open(files[0], encoding="utf-8", newline="") as f:
            rows = list(csv.reader(f))
        rec = dict(zip(rows[0], rows[1]))
        self.assertEqual((rec["val_level"], rec["name_mode"], rec["name_no"]), ("MID", "b", "2"))
        self.assertAlmostEqual(float(rec["val_mean"]), 25.0, places=4)

    def test_sqlite_has_judge_columns_with_dtype(self):
        from src.tri_s12_act_sqlite import a_store_stats_trigger
        db_path = os.path.join(self.tmp, "stats.db")
        self._run(a_store_stats_trigger, {"db_path": db_path})
        with sqlite3.connect(db_path) as conn:
            types = {r[1]: r[2] for r in conn.execute('PRAGMA table_info("stats")')}
            row = conn.execute('SELECT val_level, name_no, typeof(name_no) FROM "stats"').fetchall()
        self.assertEqual((types["val_level"], types["name_no"]), ("TEXT", "INTEGER"))   # out_coldef の dtype の列
        self.assertEqual(row, [("MID", 2, "integer")])

    def test_socket_sends_judge_values(self):
        from src.tri_s12_act_socket import a_send_stats_trigger
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        srv.settimeout(10)
        self.addCleanup(srv.close)
        received = []

        def _serve():
            conn, _ = srv.accept()
            with conn:
                buf = b""
                while True:
                    data = conn.recv(4096)
                    if not data:
                        break
                    buf += data
            received.extend(line for line in buf.decode("utf-8").split("\n") if line and line != "PUT")

        th = threading.Thread(target=_serve, daemon=True)
        th.start()
        self._run(a_send_stats_trigger, {"server_ip": "127.0.0.1", "server_port": srv.getsockname()[1],
                                         "ack_mode": False, "not_connectable_action": "wait", "save_dir": self.tmp})
        th.join(timeout=10)
        self.assertEqual(len(received), 1, received)
        rec = json.loads(received[0])
        self.assertEqual((rec["val_level"], rec["name_mode"], rec["name_no"]), ("MID", "b", 2))


if __name__ == "__main__":
    unittest.main(verbosity=2)
