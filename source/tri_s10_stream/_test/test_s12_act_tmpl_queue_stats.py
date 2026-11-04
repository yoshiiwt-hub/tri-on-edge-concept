# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Single Run Test [Act Trigger: Queue Template - Stats]
# Ver: 01.b.20261103    By: IWATA, Y.'s Agent AI
# Mod: 設定例のフォルダ名の変更（_settings_exsamples_mod → _settings_exsamples）に合わせた
########################################
a_template_stats_trigger をスレッドで動かし、セグメントを渡して、
統計量の計算・オンデマンド通知（運用系メトリクス）・起動時の検証を確かめるテスト

    確認すること
      1. セグメントごとに統計量を計算し、notify_stats の統計量の値・notify_values の値がメトリクスに入ること
         （judge の判定結果の列も notify_stats で通知できること）
      2. 値が出せなかった統計量は "-" になること
      3. column_register_map / out_coldef の不足、notify_stats の不明な名前で、起動時に停止すること
      4. 設定例 _settings_exsamples/trigger_setting_02.json__tmpl_queue__(stats).txt の act_trigger が、
         そのまま動くこと（社内版のみ。設定例のフォルダが無い場合はスキップ）

実行: tri_s10_stream/ で  python -m unittest _test.test_s12_act_tmpl_queue_stats -v
"""
import json
import os
import queue
import sys
import threading
import unittest
from   typing   import Any, Dict, List
from   unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # tri_s10_stream/（src の親）
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import src.tri_s00_supervise_base as base
from   src.tri_s01_error_if import ErrorIF

MODULE = "ACT_Queue"
EXAMPLE = os.path.join(ROOT, "_settings_exsamples", "trigger_setting_02.json__tmpl_queue__(stats).txt")

# Act に渡る列: 先頭4列 + Find の列（0: f0 / 1: LOT（文字列）/ 2: Sin（数値））
COL_DEF: Dict[int, Any] = {i: [name, "str", "key", "key", ""] for i, name in enumerate(base.SEG_HEADER_COLS)}
COL_DEF.update({4: ["f0", "str", "key", "key", ""], 5: ["LOT", "str", "cat", "exp", ""], 6: ["Sin", "float", "num", "obj", ""]})

STATS = {
    "column_register_map": {"0": [-1, "seg_len"], "1": [2, "mean"], "2": [1, "mode"], "3": [0, "mean"],
                            "4": [1, "judge", [{"range": [None, 0.5], "to": "LOW"}, {"range": [0.5, None], "to": "HIGH"}], "__"]},
    "out_coldef": {"0": ["Seg_Len", "int", "num", "exp", ""], "1": ["Sin_mean", "float", "num", "exp", ""],
                   "2": ["LOT_mode", "str", "cat", "exp", ""], "3": ["f0_mean", "float", "num", "exp", ""],
                   "4": ["Sin_level", "str", "cat", "exp", ""]},
}


def make_segment(seg_id: int, lots: List[str], sins: List[float]) -> List[list]:
    return [[seg_id, i + 1, f"2026-10-30 00:00:0{i}.000000", 0, "x", lot, sin]
            for i, (lot, sin) in enumerate(zip(lots, sins))]


class TestTemplateStatsTrigger(unittest.TestCase):

    def setUp(self) -> None:
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        ErrorIF.init()
        base.notify_pop_all()

    def _run(self, config: Dict[str, Any], segments: List[List[list]]) -> Dict[str, Any]:
        """トリガーを動かし、セグメントと終了の合図を渡して終わるまで待つ。終了後のメトリクスと停止の有無を返す"""
        from src.tri_s12_act_tmpl_queue import a_template_stats_trigger
        in_queue, stop_event = queue.Queue(), threading.Event()
        t = threading.Thread(target=a_template_stats_trigger, args=(in_queue, COL_DEF, config, stop_event), daemon=True)
        t.start()
        for seg in segments:
            in_queue.put(seg)
        in_queue.put(None)
        t.join(timeout=20)
        self.assertFalse(t.is_alive())
        return {"metrics": base.metrics_get_all().get(MODULE, {}), "stopped": stop_event.is_set()}

    def test_stats_are_calculated_and_notified(self):
        before = base.metrics_get_all().get(MODULE, {}).get("total_segments", 0)
        config = {**STATS, "notify_values": [{"loc": 2, "row": -1}],
                  "notify_stats": ["Sin_mean", "LOT_mode", "Sin_level", "f0_mean"]}
        res = self._run(config, [make_segment(101, ["aaa", "bbb", "bbb"], [0.2, 0.4, 0.6]),
                                 make_segment(102, ["ccc", "ccc"], [0.6, 0.8])])
        m = res["metrics"]
        self.assertFalse(res["stopped"])
        self.assertEqual(m["total_segments"] - before, 2)
        self.assertEqual(m["last_seg_id"], 102)
        self.assertAlmostEqual(m["Sin_mean"], 0.7, places=4)            # 2つ目のセグメント（0.6, 0.8）
        self.assertEqual((m["LOT_mode"], m["Sin_level"]), ("ccc", "HIGH"))   # judge の判定結果
        self.assertEqual(m["f0_mean"], "-")                              # 文字列の列の平均 → 値が出せない
        self.assertEqual(m["Sin[row=-1]"], 0.8)                          # notify_values（列2 の最後の行）
        requests, _ = base.notify_pop_all()
        self.assertEqual(sum(1 for r in requests if r["module"] == MODULE), 2)   # セグメントごとに通知を要求

    def test_startup_errors_stop_trigger(self):
        bad = [
            {},                                                             # column_register_map / out_coldef が無い
            {"column_register_map": STATS["column_register_map"]},           # out_coldef が無い
            {**STATS, "notify_stats": ["NO_SUCH_KEY"]},                     # out_coldef に無い名前
            {**STATS, "column_register_map": {**STATS["column_register_map"], "4": [9, "judge", [{"in": [1], "to": "x"}]]}},
        ]
        for config in bad:
            with self.subTest(config=config):
                res = self._run(config, [])
                self.assertTrue(res["stopped"])

    @unittest.skipUnless(os.path.isfile(EXAMPLE), "設定例のフォルダが無い（公開版）")
    def test_settings_example_runs_as_is(self):
        with open(EXAMPLE, encoding="utf-8-sig") as f:
            act = json.loads(f.read())["act_trigger"]
        config = {k: v for k, v in act.items() if not k.startswith("##") and k not in ("module", "func")}
        config["teams_notify"] = False
        self.assertEqual((act["module"], act["func"]), ("src.tri_s12_act_tmpl_queue", "a_template_stats_trigger"))
        res = self._run(config, [make_segment(201, ["aaa", "aaa", "bbb"], [1.0, 2.0, 3.0])])
        self.assertFalse(res["stopped"])
        self.assertEqual((res["metrics"]["Sin_mean"], res["metrics"]["LOT_mode"]), (2.0, "aaa"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
