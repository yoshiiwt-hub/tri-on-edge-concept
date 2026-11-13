# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Single Run Test [Act Trigger: Tkinter Viz - Graph / CSV Save]
# Ver: 01.b.20261113    By: IWATA, Y.'s Agent AI
# Mod: ウィンドウを閉じた後の保存・既定値での保存・保存に失敗した後の描画のテストを追加
#      新規作成（a_viz_graph_trigger の CSV 保存のオプション save_csv）
########################################
a_viz_graph_trigger の CSV 保存（save_csv）を確かめるテスト
画面を出さないよう、tkinter の代わりに小さな偽物（_FakeTk）を使う（tkinter が入っていない環境でも動く）

    確認すること
      1. _save_segment_csv：{file_pfx}{連番3桁}_{seg_id}.csv に、列名の1行＋全行・全列を指定の文字コードで書く。.tmp は残らない
      2. _save_segment_csv：書けないとき（保存先が無い）は例外を投げ、.tmp は残らない
      3. トリガー：save_csv: true のとき、受け取ったセグメントを 1 セグメント 1 ファイルで保存する
         （描けないセグメント＝数値が2点未満も保存する）
      4. トリガー：save_csv を書かないとき、ファイルを作らない（今までどおり）
      5. トリガー：保存に失敗しても、トリガーは止まらず、次のセグメントを受け取って保存し、描画も続ける
         （ErrorIF へ category io で報告。本番と同じポリシーで確かめる。続けて何度も失敗したときの扱いは、ポリシーが決める）
      6. トリガー：ウィンドウが閉じられた後（描画で TclError）も、受け取ったセグメントを保存する
      7. トリガー：save_dir / encode / file_pfx を書かないとき、既定（output / utf-8 / viz_graph）で保存する

実行: tri_s10_stream/ で  python -m unittest _test.test_s12_act_viz_tk_graph_csv -v
"""
import csv
import importlib.util
import os
import queue
import shutil
import sys
import tempfile
import threading
import types
import unittest
from   typing   import Any, Dict, List
from   unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # tri_s10_stream/（src の親）
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


class _FakeWidget:
    """
    偽の tkinter の部品（Label / Canvas / Button）。ほとんどの呼び出しは何もしない
    create_line は、最新の線（width=2）を描いた回数を latest_drawn に数える。fail_draw が真なら TclError を投げる
    （ウィンドウが閉じられたときと同じ。トリガーは以降の描画をやめる）
    """
    latest_drawn = 0
    fail_draw    = False
    tcl_error: type = Exception

    def __init__(self, *args, **kwargs) -> None: pass
    def pack(self, *args, **kwargs) -> None: pass
    def config(self, *args, **kwargs) -> None: pass
    def delete(self, *args, **kwargs) -> None: pass
    def winfo_reqheight(self) -> int: return 24

    def create_line(self, *args, **kwargs) -> None:
        if _FakeWidget.fail_draw:
            raise _FakeWidget.tcl_error("window closed (test)")
        if kwargs.get("width") == 2:
            _FakeWidget.latest_drawn += 1


class _FakeTk(_FakeWidget):
    """偽の tkinter.Tk。ウィンドウは作らない"""
    def title(self, *args) -> None: pass
    def geometry(self, *args) -> None: pass
    def protocol(self, *args) -> None: pass
    def iconbitmap(self, *args) -> None: pass
    def update(self) -> None: pass
    def update_idletasks(self) -> None: pass
    def destroy(self) -> None: pass


def _fake_tkinter() -> types.ModuleType:
    fake = types.ModuleType("tkinter")
    fake.Tk, fake.Label, fake.Canvas, fake.Button = _FakeTk, _FakeWidget, _FakeWidget, _FakeWidget
    fake.TclError = type("TclError", (Exception,), {})
    _FakeWidget.tcl_error = fake.TclError
    return fake


# tkinter が入っていない環境では、import の間だけ偽物を置く（import の後は元に戻す）
_stubbed = importlib.util.find_spec("tkinter") is None
if _stubbed:
    sys.modules["tkinter"] = _fake_tkinter()
try:
    import src.tmpl_viz.tri_s12_act_viz_tk_graph as viz
finally:
    if _stubbed:
        del sys.modules["tkinter"]

import src.tri_s00_supervise_base as base
from   src.tri_s01_error_if import ErrorIF

HEADER = list(base.SEG_HEADER_COLS)   # Act に渡る行の先頭4列
COL_DEF: Dict[int, Any] = {i: [name, "str", "key", "key", ""] for i, name in enumerate(HEADER)}
COL_DEF.update({4: ["LOT", "str", "cat", "exp", ""], 5: ["正弦", "float", "num", "obj", ""]})   # Find の列 0 / 1
COL_NAMES = [COL_DEF[k][0] for k in sorted(COL_DEF)]


def make_segment(seg_id: int, vals: List[float]) -> List[list]:
    return [[seg_id, i + 1, f"2026-11-13 00:00:0{i}.000000", 0, "L1", v] for i, v in enumerate(vals)]


def read_csv(path: str, encoding: str) -> List[List[str]]:
    with open(path, newline="", encoding=encoding) as f:
        return list(csv.reader(f))


class TestSaveSegmentCsv(unittest.TestCase):

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="tri_viz_csv_")
        self.addCleanup(shutil.rmtree, self.tmp, True)

    def test_file_name_header_rows_and_encoding(self):
        seg = make_segment(7, [1.5, 2.5, 3.5])
        for enc in ("utf-8", "cp932"):
            with self.subTest(encode=enc):
                path = viz._save_segment_csv(seg, COL_NAMES, self.tmp, f"g_{enc}_", 3, 7, enc)
                self.assertEqual(os.path.basename(path), f"g_{enc}_003_7.csv")
                rows = read_csv(path, enc)
                self.assertEqual(rows[0], COL_NAMES)                             # 列名の1行（日本語の列名も読める）
                self.assertEqual(rows[1:], [[str(c) for c in r] for r in seg])   # 全行・全列
        self.assertEqual([f for f in os.listdir(self.tmp) if f.endswith(".tmp")], [])

    def test_failure_raises_and_leaves_no_tmp(self):
        missing = os.path.join(self.tmp, "no_such_dir")
        with self.assertRaises(OSError):
            viz._save_segment_csv(make_segment(1, [1.0, 2.0]), COL_NAMES, missing, "g", 1, 1, "utf-8")
        with self.assertRaises(LookupError):                                      # 不明な文字コード
            viz._save_segment_csv(make_segment(1, [1.0, 2.0]), COL_NAMES, self.tmp, "g", 1, 1, "no-such-codec")
        self.assertEqual(os.listdir(self.tmp), [])


class TestVizGraphTriggerCsv(unittest.TestCase):

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="tri_viz_csv_")
        self.addCleanup(shutil.rmtree, self.tmp, True)
        for p in (mock.patch.object(viz, "tk", _fake_tkinter()),
                  mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})):
            p.start()
            self.addCleanup(p.stop)
        # 本番と同じポリシー（ループ中の io の失敗は、待って続ける）。ポリシーのファイルが無いときは、ErrorIF の既定で動く
        policy = os.path.join(ROOT, "settings", "error_policy.json")
        ErrorIF.init(policy_path=policy) if os.path.isfile(policy) else ErrorIF.init()
        ErrorIF.drain_events()
        _FakeWidget.latest_drawn, _FakeWidget.fail_draw = 0, False

    def _run(self, config: Dict[str, Any], segments: List[List[list]]) -> bool:
        """トリガーを動かし、セグメントと終了の合図を渡して終わるまで待つ。止まった（stop_event）かを返す"""
        q, stop = queue.Queue(), threading.Event()
        t = threading.Thread(target=viz.a_viz_graph_trigger, args=(q, COL_DEF, {"val_col_idx": 1, **config}, stop),
                             daemon=True)
        t.start()
        for seg in segments:
            q.put(seg)
        q.put(None)
        t.join(timeout=20)
        self.assertFalse(t.is_alive())
        return stop.is_set()

    def test_save_csv_true_saves_each_segment(self):
        stopped = self._run({"save_csv": True, "save_dir": self.tmp, "file_pfx": "viz_"},
                            [make_segment(11, [1.0, 2.0, 3.0]), make_segment(12, [5.0])])   # 12 は描けない（1点）
        self.assertFalse(stopped)
        self.assertEqual(sorted(os.listdir(self.tmp)), ["viz_001_11.csv", "viz_002_12.csv"])
        rows = read_csv(os.path.join(self.tmp, "viz_002_12.csv"), "utf-8")
        self.assertEqual(rows, [COL_NAMES, [str(c) for c in make_segment(12, [5.0])[0]]])

    def test_default_does_not_save(self):
        out = os.path.join(viz.system_root, "output")
        before = set(os.listdir(out)) if os.path.isdir(out) else set()
        self.assertFalse(self._run({"save_dir": self.tmp}, [make_segment(21, [1.0, 2.0])]))
        self.assertEqual(os.listdir(self.tmp), [])                               # save_csv を書かない → 作らない
        after = set(os.listdir(out)) if os.path.isdir(out) else set()
        self.assertEqual(after, before)

    def test_save_failure_does_not_stop_trigger(self):
        config = {"save_csv": True, "save_dir": self.tmp, "file_pfx": "viz_"}
        real = viz._save_segment_csv
        calls = {"n": 0}

        def flaky(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("disk full (test)")
            return real(*args, **kwargs)

        with mock.patch.object(viz, "_save_segment_csv", flaky):
            stopped = self._run(config, [make_segment(31, [1.0, 2.0]), make_segment(32, [3.0, 4.0])])
        self.assertFalse(stopped)
        self.assertEqual(os.listdir(self.tmp), ["viz_001_32.csv"])                # 失敗した 31 の後も、次を保存する
        self.assertEqual(_FakeWidget.latest_drawn, 2)                             # 31・32 とも描いている（描画は続く）
        ev = [e for e in ErrorIF.drain_events() if e.get("trigger_id") == "a_viz_graph_trigger"]
        self.assertEqual([(e.get("phase"), e.get("category")) for e in ev], [("loop", "io")])


    def test_saves_after_window_closed(self):
        _FakeWidget.fail_draw = True                                              # 描画で TclError → ウィンドウが閉じた扱い
        stopped = self._run({"save_csv": True, "save_dir": self.tmp, "file_pfx": "viz_"},
                            [make_segment(41, [1.0, 2.0]), make_segment(42, [3.0, 4.0]), make_segment(43, [5.0, 6.0])])
        self.assertFalse(stopped)
        self.assertEqual(sorted(os.listdir(self.tmp)), ["viz_001_41.csv", "viz_002_42.csv", "viz_003_43.csv"])
        self.assertEqual(_FakeWidget.latest_drawn, 0)                             # 描画はしていない

    def test_defaults_output_utf8_viz_graph(self):
        out = os.path.join(viz.system_root, "output")
        made_dir = not os.path.isdir(out)
        path = os.path.join(out, "viz_graph001_990051.csv")
        self.addCleanup(lambda: os.path.isfile(path) and os.remove(path))
        if made_dir:
            self.addCleanup(lambda: os.path.isdir(out) and not os.listdir(out) and os.rmdir(out))
        self.assertFalse(self._run({"save_csv": True}, [make_segment(990051, [1.0, 2.0])]))
        self.assertTrue(os.path.isfile(path))                                     # output / viz_graph / 連番 001
        with open(path, "rb") as f:
            raw = f.read()
        self.assertFalse(raw.startswith(b"\xef\xbb\xbf"))                        # utf-8（BOM なし）
        self.assertEqual(raw.decode("utf-8").splitlines()[0].split(","), COL_NAMES)


if __name__ == "__main__":
    unittest.main(verbosity=2)
