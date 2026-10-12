# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Unit Test [Wave Cutout / Wave Stats]
# Ver: 01.b.20261012    By: IWATA, Y.'s Agent AI
# Mod: ファイル名を test_cutout_stats.py から test_s10_stream_wave.py に変更（対象モジュール tri_s10_stream_wave_* に合わせる）
########################################
波形切出し（tri_s10_stream_wave_cutout）と統計量（tri_s10_stream_wave_stat）の単体テスト
他のモジュール（find / act / error_if）には依存しない。

実行: tri_s10_stream/ で  python -m unittest discover -s _test -v
      または              python -m unittest _test.test_s10_stream_wave -v
      または              python _test/test_s10_stream_wave.py
"""
import json
import os
import sys
import tempfile
import unittest
from   datetime import datetime as dt

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # tri_s10_stream/（src の親）
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import src.tri_s10_stream_wave_cutout as cutout
import src.tri_s10_stream_wave_stat   as stat


########################################
# helper
########################################
def run_cutout(values, seg_start, seg_end, seg_max_rows=0, seg_max_sec=0.0):
    """
    values（1レコード分の値のリストを並べたもの）を順に流し、切り出されたセグメントを返す
    戻り値: [(セグメント（行のリスト）, 強制クローズか), ...]
    セグメントの行: [seg_id, seg_ind, seg_ts, 0] + 値
    """
    start = cutout.wrap_spec(seg_start)
    end   = cutout.wrap_spec(seg_end)
    cutout.init_state(start, end, seg_max_rows, seg_max_sec)

    out = []
    def on_cutout_segment(seg, seg_id, seg_ind, forced=False):
        out.append(([list(r) for r in seg], forced))

    buf, prev_values, prev_status, seg_range, progress = [], None, 0, [None, None], {}
    seg_id = seg_ind = 0
    for i, v in enumerate(values):
        record = [dt.now(), i + 1, list(v)]
        seg_id, seg_ind, prev_values, prev_status, seg_range = cutout.process_stream_record(
            seg_id, seg_ind, record, buf, on_cutout_segment, prev_values, prev_status, seg_range,
            (start, end), cutout.TRANS_TABLE, progress, 0)
    return out


START_EQ_1 = {"method": "equal", "target_loc": 0, "thr": 1}
END_EQ_0   = {"method": "equal", "target_loc": 0, "thr": 0}


########################################
# 波形切出し
########################################
class TestWaveCutout(unittest.TestCase):

    def test_wrap_spec_single_form(self):
        spec = cutout.wrap_spec({"method": "equal", "target_loc": 2, "border": 5})
        self.assertEqual(spec, {"stages": [{"op": "AND", "checks": [
            {"method": "equal", "target_loc": 2, "thr": 5}]}]})

    def test_wrap_spec_keeps_multi_stage_form(self):
        multi = {"stages": [{"op": "OR", "checks": []}]}
        self.assertIs(cutout.wrap_spec(multi), multi)

    def test_cut_segments_between_start_and_end(self):
        # 値 0,0,[1,1,1,0],0,[1,1,1,1,0],0 → 開始点〜終点（終点行を含む）で 4行 と 5行
        values = [[v, i * 10.0] for i, v in enumerate([0, 0, 1, 1, 1, 0, 0, 1, 1, 1, 1, 0, 0])]
        segs = run_cutout(values, START_EQ_1, END_EQ_0)
        self.assertEqual([len(s) for s, _ in segs], [4, 5])
        self.assertEqual([forced for _, forced in segs], [False, False])
        for seg, _ in segs:
            self.assertEqual(seg[0][4], 1)       # 先頭行は開始点（値=1）
            self.assertEqual(seg[-1][4], 0)      # 末尾行は終点（値=0）
            self.assertEqual([r[1] for r in seg], list(range(1, len(seg) + 1)))   # seg_ind は 1 始まり
            self.assertEqual(len({r[0] for r in seg}), 1)                          # seg_id は全行で同じ

    def test_no_segment_when_start_never_met(self):
        segs = run_cutout([[0, 0.0]] * 10, START_EQ_1, END_EQ_0)
        self.assertEqual(segs, [])

    def test_open_segment_is_not_emitted(self):
        # 開始したが終点が来ないまま終わった場合は、切り出されない
        segs = run_cutout([[0, 0.0], [1, 1.0], [1, 2.0]], START_EQ_1, END_EQ_0)
        self.assertEqual(segs, [])

    def test_forced_close_by_max_rows(self):
        # 開始条件を満たし続け、終了条件が来ない → 3行ごとに強制クローズされる
        segs = run_cutout([[1, float(i)] for i in range(10)], START_EQ_1, END_EQ_0, seg_max_rows=3)
        self.assertEqual([len(s) for s, _ in segs], [3, 3, 3])
        self.assertTrue(all(forced for _, forced in segs))

    def test_multi_stage_start_condition(self):
        # 段1: 値==1 が成立 → 段2: 値2 が 50 以上、の順に成立したときだけ開始する
        start = {"stages": [
            {"op": "AND", "checks": [{"method": "equal", "target_loc": 0, "thr": 1}]},
            {"op": "AND", "checks": [{"method": "upto",  "target_loc": 1, "thr": 50}]},
        ]}
        values = [[0, 0.0], [1, 10.0], [1, 20.0], [1, 60.0], [1, 70.0], [0, 70.0], [0, 70.0]]
        segs = run_cutout(values, start, END_EQ_0)
        self.assertEqual(len(segs), 1)
        self.assertEqual(segs[0][0][0][5], 60.0)    # 値2 が 50 を上回った行から始まる

    def test_cond_diff_and_change(self):
        self.assertTrue(cutout.cond_diff(10, 0, 0, 15, [], 5))
        self.assertFalse(cutout.cond_diff(10, 0, 0, 12, [], 5))
        self.assertFalse(cutout.cond_diff(None, 0, 0, 12, [], 5))
        self.assertTrue(cutout.cond_change("A", 0, 0, "B", [], None))
        self.assertFalse(cutout.cond_change("A", 0, 0, "A", [], None))


########################################
# 統計量
########################################
def make_segment(col4, col5):
    """行: [seg_id, seg_ind, seg_ts, 0, 列4, 列5]"""
    return [[20261012000000, i + 1, f"2026-10-12 00:00:0{i}.000000", 0, a, b]
            for i, (a, b) in enumerate(zip(col4, col5))]


C_KEYS = ["seg_id", "seg_ind", "seg_ts", "seg_status", "name", "val"]


def compile_specs(specs):
    """specs: [(出力名, column_register_map のエントリ), ...] をコンパイルする"""
    crm = {str(i): entry for i, (_, entry) in enumerate(specs)}
    oc  = {str(i): [name, "float", "num", "obj", ""] for i, (name, _) in enumerate(specs)}
    return stat.compile_register_map(crm, oc, C_KEYS)


class TestWaveStats(unittest.TestCase):

    def setUp(self):
        self.seg = make_segment(["a", "b", "b", "c"], [10, 20, 30, 40])

    def test_basic_stats(self):
        comp = compile_specs([
            ("mean", [5, "mean"]), ("min", [5, "min"]), ("max", [5, "max"]), ("std", [5, "std"]),
            ("count", [5, "count"]), ("sum", [5, "sum"]), ("range", [5, "range"]),
            ("med", [5, "med"]), ("iqr", [5, "iqr"]),
        ])
        r = stat.build_stats(self.seg, comp)
        self.assertEqual(r["seg_id"], 20261012000000)
        self.assertEqual(r["seg_rows"], 4)
        self.assertEqual(r["seg_ts_start"], "2026-10-12 00:00:00.000000")
        self.assertEqual(r["seg_ts_end"],   "2026-10-12 00:00:03.000000")
        self.assertAlmostEqual(r["mean"], 25.0, places=4)
        self.assertAlmostEqual(r["min"], 10.0, places=4)
        self.assertAlmostEqual(r["max"], 40.0, places=4)
        self.assertAlmostEqual(r["std"], 11.1803, places=4)      # 母標準偏差
        self.assertEqual(r["count"], 4)
        self.assertAlmostEqual(r["sum"], 100.0, places=4)
        self.assertAlmostEqual(r["range"], 30.0, places=4)
        self.assertAlmostEqual(r["med"], 25.0, places=4)
        self.assertAlmostEqual(r["iqr"], 15.0, places=4)         # 32.5 - 17.5

    def test_param_stats(self):
        comp = compile_specs([
            ("pertl25", [5, "pertl", 25]),
            ("sig_u",   [5, "sig_u", 1]),
            ("sig_l",   [5, "sig_l", 1]),
            ("nth2",    [5, "nth", 2]),
            ("nth_last", [5, "nth", -1]),
        ])
        r = stat.build_stats(self.seg, comp)
        self.assertAlmostEqual(r["pertl25"], 17.5, places=4)
        self.assertAlmostEqual(r["sig_u"], 36.1803, places=4)    # 平均 + 1σ
        self.assertAlmostEqual(r["sig_l"], 13.8197, places=4)    # 平均 - 1σ
        self.assertEqual(r["nth2"], 20)
        self.assertEqual(r["nth_last"], 40)

    def test_raw_value_stats_accept_any_type(self):
        comp = compile_specs([
            ("first", [4, "first"]), ("last", [4, "last"]), ("mode", [4, "mode"]),
            ("n_null", [4, "n_null"]),
        ])
        r = stat.build_stats(self.seg, comp)
        self.assertEqual(r["first"], "a")        # 文字列の列でも、そのまま返す
        self.assertEqual(r["last"], "c")
        self.assertEqual(r["mode"], "b")
        self.assertEqual(r["n_null"], 4)         # 数値に変換できない件数

    def test_const_and_seg_len_do_not_depend_on_content(self):
        comp = compile_specs([("c", [-1, "const", 7]), ("n", [-1, "seg_len"])])
        r = stat.build_stats(self.seg, comp)
        self.assertEqual(r["c"], 7)
        self.assertEqual(r["n"], 4)

    def test_slice_uses_part_of_segment(self):
        # 後半（50%〜100%）= 30, 40 の平均
        comp = compile_specs([("mean_late", [5, "mean", [0.5, 1.0]])])
        self.assertAlmostEqual(stat.build_stats(self.seg, comp)["mean_late"], 35.0, places=4)

    def test_warn_when_value_not_available(self):
        # 文字列の列の平均は出せない → 値のキーは出さず、{名前}_WARN に理由を残す
        comp = compile_specs([("mean_name", [4, "mean"])])
        r = stat.build_stats(self.seg, comp)
        self.assertNotIn("mean_name", r)
        self.assertEqual(r["mean_name_WARN"], "no_valid_values")

    def test_compile_errors(self):
        oc = {"0": ["x", "float", "num", "obj", ""]}
        with self.assertRaises(ValueError):                                  # 未知の統計量
            stat.compile_register_map({"0": [5, "nope"]}, oc, C_KEYS)
        with self.assertRaises(ValueError):                                  # 列番号が範囲外
            stat.compile_register_map({"0": [99, "mean"]}, oc, C_KEYS)
        with self.assertRaises(ValueError):                                  # 2つの定義のキーが不一致
            stat.compile_register_map({"0": [5, "mean"], "1": [5, "max"]}, oc, C_KEYS)
        with self.assertRaises(ValueError):                                  # pertl の p が不正
            stat.compile_register_map({"0": [5, "pertl", 33]}, oc, C_KEYS)
        with self.assertRaises(ValueError):                                  # nth の n が 0
            stat.compile_register_map({"0": [5, "nth", 0]}, oc, C_KEYS)

    def test_init_stats_and_calc_stats(self):
        col_def = {i: [name, "str", "key", "key", ""] for i, name in enumerate(C_KEYS)}
        config = {
            "column_register_map": {"0": [5, "mean"], "1": [4, "mean"]},
            "out_coldef": {"0": ["val_mean", "float", "num", "obj", ""],
                           "1": ["name_mean", "float", "num", "obj", ""]},
        }
        ctx = stat.init_stats(config, col_def, "TEST", "test_trigger")
        self.assertEqual(ctx.base_col_names,
                         ["seg_id", "seg_ts_start", "seg_ts_end", "seg_rows", "val_mean", "name_mean"])
        r = stat.calc_stats(ctx, self.seg, self.seg[0][0])
        self.assertAlmostEqual(r["val_mean"], 25.0, places=4)
        self.assertIn("name_mean_WARN", r)                  # 出せなかった項目は _WARN で残る

    def test_init_stats_requires_settings(self):
        col_def = {0: ["a", "str", "key", "key", ""]}
        with self.assertRaises(ValueError):
            stat.init_stats({}, col_def, "TEST", "t")
        with self.assertRaises(ValueError):
            stat.init_stats({"column_register_map": {"0": [0, "mean"]}}, col_def, "TEST", "t")

    def test_export_out_coldef(self):
        out_coldef = {"0": ["val_mean", "float", "num", "obj", "平均値"]}
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "sub", "column_define.json")
            ret = stat.export_out_coldef(out_coldef, path, "utf-8", "TEST")
            self.assertEqual(ret, path)
            with open(path, encoding="utf-8") as f:
                self.assertEqual(json.load(f), out_coldef)


if __name__ == "__main__":
    unittest.main(verbosity=2)
