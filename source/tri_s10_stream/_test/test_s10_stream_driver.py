# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Integration Test [Stream Driver: Find -> Cutout -> Act]
# Ver: 01.a.20261012    By: IWATA, Y.'s Agent AI
# Mod: test_find_act_pipeline.py・pipeline_driver.py を統合し、test_demo_csv_sample.py の LOT ごとの切出し・統計量の確認を移した。
#      socket の相手を sample_server/socket_server.py（SocketServer）に変更
########################################
find / act を実際に呼び出して通す結合テスト
    find（入力）→ 波形切出し → act（出力）を、テスト用の簡易ドライバ（run_pipeline）でつなぐ。
    socket を使うものは、127.0.0.1 のサンプルサーバー（sample_server/socket_server.py）を相手にする。

    A. find_demo（CSV）     → act_tmpl_queue（IMQ テンプレート）  … 処理件数・通知値のメトリクスを確認
    B. find_demo（CSV）     → act_socket（全行送信 / 統計量送信）  … 受信側で内容を確認
    C. find_socket（受信）  → act_tmpl_queue                       … サンプルサーバーから受信して切出し
    D. エラー通知           … 異常な設定・接続先のとき、ErrorIF にイベントが届くこと
    E. サンプルCSV（settings/data/generate_waveform_sample.csv） … LOT ごとに切り出し、行数・統計量が CSV と一致すること

    ※ run_pipeline は StreamCore ではありません。find / act を、常駐処理なしで動かして確認するための
       テスト専用の簡易ループです（find をスレッドで起動 → 受信レコードを tri_s10_stream_wave_cutout で切り出す →
       act の CUT_LAST_ROW 属性に従って終点行を除外 → act のキューへ渡す → 指定件数で停止）。
       対象はインメモリキュー（IMQ）の act のみ。ストレージキュー（STQ）は対象外
    ※ tri_s19_util を使うモジュール（find_file / act_file / act_csv）は、ここでは対象外

実行: tri_s10_stream/ で  python -m unittest discover -s _test -v
      または              python -m unittest _test.test_s10_stream_driver -v
      または              python _test/test_s10_stream_driver.py
"""
import csv
import json
import os
import queue
import socket
import sys
import tempfile
import threading
import time
import unittest
from   datetime import datetime as dt, timedelta
from   typing   import Any, Callable, Dict, List
from   unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # tri_s10_stream/（src の親）
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
TEST_DIR = os.path.dirname(os.path.abspath(__file__))                # _test/（sample_server を読み込むため。
if TEST_DIR not in sys.path:                                         #  パッケージとして実行したときも読めるようにする）
    sys.path.insert(0, TEST_DIR)

import src.tri_s00_supervise_base     as base
from   src.tri_s01_error_if           import ErrorIF
import src.tri_s10_stream_wave_cutout as cutout
import src.tri_s10_stream_wave_stat   as stat
import src.tri_s11_find_demo          as find_demo
import src.tri_s11_find_socket        as find_socket
import src.tri_s12_act_socket         as act_socket
import src.tri_s12_act_tmpl_queue     as act_queue

from   sample_server.socket_server import SocketServer


########################################
# Test Driver（find → 波形切出し → act）
########################################

# act 側の列定義の先頭4列（find の列は、この後ろに続く）
ACT_HEADER: Dict[int, List[str]] = {
    0: ["seg_id",     "int",  "key", "key", "Segment ID"],
    1: ["seg_ind",    "int",  "key", "key", "Segment Row Index"],
    2: ["seg_ts",     "time", "key", "key", "Timestamp When Data Collected"],
    3: ["seg_status", "int",  "key", "key", "Segment Process Status"],
}


def make_act_col_def(col_def_find: Dict[int, Any]) -> Dict[int, Any]:
    """find 側の列定義の前に、セグメントのヘッダ4列を付けて act 側の列定義にする"""
    n = len(ACT_HEADER)
    return {**ACT_HEADER, **{int(k) + n: v for k, v in col_def_find.items()}}


class PipelineResult:
    """run_pipeline の結果"""
    def __init__(self) -> None:
        self.segments: List[List[list]] = []   # act へ渡したセグメント（終点行の除外後）
        self.forced:   List[bool]        = []   # 各セグメントが上限による強制クローズか
        self.records:  int               = 0    # find から受け取ったレコード数
        self.timed_out: bool             = False
        self.errors:   List[BaseException] = [] # find / act のスレッドで捕まえた例外


def run_pipeline(
    find_func:     Callable,
    find_config:   Dict[str, Any],
    col_def_find:  Dict[int, Any],
    act_func:      Callable,
    act_config:    Dict[str, Any],
    seg_start:     Dict[str, Any],
    seg_end:       Dict[str, Any],
    n_segments:    int   = 3,
    timeout_sec:   float = 20.0,
    seg_max_rows:  int   = 0,
    seg_max_sec:   float = 0.0,
) -> PipelineResult:
    """
    find を起動し、n_segments 個のセグメントを act へ渡したら止める（timeout_sec で打ち切り）
    Mod: 2026.10.12    By: IWATA, Y.'s Agent AI

    seg_start / seg_end : 設定ファイルの monitor.seg_start / seg_end と同じ形式（単一形式でも多段形式でも可）
    """
    result       = PipelineResult()
    start_spec   = cutout.wrap_spec(seg_start)
    end_spec     = cutout.wrap_spec(seg_end)
    col_def_act  = make_act_col_def(col_def_find)
    cut_last_row = bool(getattr(act_func, "CUT_LAST_ROW", False))

    input_queue     = queue.Queue(maxsize=10000)
    act_queue_      = queue.Queue(maxsize=10000)
    stop_event_find = threading.Event()
    stop_event_act  = threading.Event()

    def _run(func: Callable, *args: Any) -> None:
        try:
            func(*args)
        except BaseException as e:      # スレッド内の未処理例外をテスト側で確認できるようにする
            result.errors.append(e)

    t_find = threading.Thread(target=_run, daemon=True,
                              args=(find_func, input_queue, col_def_find, find_config, stop_event_find))
    t_act  = threading.Thread(target=_run, daemon=True,
                              args=(act_func, act_queue_, col_def_act, act_config, stop_event_act))

    def _on_cutout_segment(seg_copy: List[list], seg_id: int, seg_ind: int, forced: bool = False) -> None:
        """セグメントが切り出されたときの処理（終点行の除外 → act のキューへ）"""
        if cut_last_row and not forced:
            seg_copy = seg_copy[:-1]
            if not seg_copy:
                return None
        result.segments.append([list(r) for r in seg_copy])
        result.forced.append(forced)
        act_queue_.put(seg_copy, block=False)

    cutout.init_state(start_spec, end_spec, seg_max_rows, seg_max_sec)
    cond_progress = {"start": [False] * len(start_spec["stages"]),
                     "end":   [False] * len(end_spec["stages"])}
    seg_buffer: List[list] = []
    seg_id, seg_ind, prev_values, prev_status, seg_range = 0, 0, None, 0, [None, None]

    t_act.start()
    t_find.start()
    deadline = time.time() + timeout_sec
    try:
        while len(result.segments) < n_segments:
            if time.time() > deadline:
                result.timed_out = True
                break
            try:
                index, value = input_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            result.records += 1
            record = [dt.now(), index + 1, list(value)]
            seg_id, seg_ind, prev_values, prev_status, seg_range = cutout.process_stream_record(
                seg_id, seg_ind, record, seg_buffer, _on_cutout_segment,
                prev_values, prev_status, seg_range,
                (start_spec, end_spec), cutout.TRANS_TABLE, cond_progress, 0)
    finally:
        stop_event_find.set()      # 入力側を先に止める
        act_queue_.put(None)       # act へ終了の合図（先に積んだセグメントを処理してから止まる）
        t_act.join(timeout=10)
        stop_event_act.set()
        t_find.join(timeout=5)
    return result


########################################
# test data
########################################
TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

# 10行を繰り返し流す。RUN の連続（3行・2行）が、1周ごとに2つのセグメントになる
LOT_PATTERN = ["IDLE", "IDLE", "RUN", "RUN", "RUN", "IDLE", "IDLE", "RUN", "RUN", "IDLE"]
VALUES      = [i * 10.0 for i in range(10)]          # 0, 10, 20, ... 90
T0          = dt(2026, 10, 12, 9, 0, 0)
ROWS        = [[(T0 + timedelta(seconds=i)).strftime(TIME_FORMAT), lot, val]
               for i, (lot, val) in enumerate(zip(LOT_PATTERN, VALUES))]

# find 側の列定義: [名前, 型, 役割, 変数の種類, 説明]
COL_DEF_FIND = {
    0: ["ts",  "time",  "key", "key", "Timestamp"],
    1: ["lot", "str",   "cat", "exp", "State"],
    2: ["val", "float", "num", "obj", "Value"],
}
SEG_START = {"method": "equal", "target_loc": 1, "thr": "RUN"}    # lot が RUN になったら開始
SEG_END   = {"method": "equal", "target_loc": 1, "thr": "IDLE"}   # lot が IDLE になったら終了（終点行）

# 1周目: RUN(20,30,40) → 3行 / RUN(70,80) → 2行（終点行は act に渡す前に除外される）
EXPECTED_LENS = [3, 2, 3, 2]

# E. サンプルCSV（デモ用の波形データ）と、その列定義・切出し条件（LOT が変わるたびに区切る）
SAMPLE_CSV = os.path.join(ROOT, "settings", "data", "generate_waveform_sample.csv")
SAMPLE_COL_DEF = {
    0: ["Timestamp", "time",  "key",      "key",         "データ発生時の時刻"],
    1: ["LOT",       "str",   "key",      "key",         "製造id"],
    2: ["Sin",       "float", "numetory", "explanatory", "疑似波形（sin）"],
    3: ["Cos",       "float", "numetory", "explanatory", "疑似波形（cos）"],
    4: ["Tan",       "float", "numetory", "explanatory", "疑似波形（tan）"],
    5: ["CL",        "int",   "category", "objective",   "正常／異常"],
}
SAMPLE_SEG = {"stages": [{"op": "AND", "checks": [{"method": "change", "target_loc": 1}]}]}


def write_demo_csv(path: str) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts", "lot", "val"])
        w.writerows(ROWS)


def free_port() -> int:
    """今は誰も使っていないポート番号を返す（接続失敗のテスト用）"""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def read_lot_runs() -> list:
    """サンプルCSV から、LOT が連続する区間ごとの [LOT, 行数, 行のリスト] を求める（期待値の元）"""
    with open(SAMPLE_CSV, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    runs: list = []
    for r in rows:
        if runs and runs[-1][0] == r["LOT"]:
            runs[-1][2].append(r)
        else:
            runs.append([r["LOT"], 0, [r]])
    for run in runs:
        run[1] = len(run[2])
    return runs


class PipelineTestCase(unittest.TestCase):
    """共通の準備: 端末情報をダミーにし、作業用フォルダを用意し、ErrorIF を初期化する"""

    def setUp(self) -> None:
        p = mock.patch.object(base, "load_device_info", return_value={"dev_name": "TEST-EQP"})
        p.start()
        self.addCleanup(p.stop)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        ErrorIF.init()

    def start_server(self, server: SocketServer) -> SocketServer:
        server.start()
        self.addCleanup(server.stop)
        return server

    def assertPipelineOk(self, res: PipelineResult) -> None:
        self.assertFalse(res.timed_out, "timed out (segments not delivered)")
        self.assertEqual(res.errors, [], f"unhandled exception in worker: {res.errors}")


########################################
# A. find_demo (CSV) -> act_tmpl_queue
########################################
class TestDemoCsvToTemplate(PipelineTestCase):

    def test_segments_reach_act_and_metrics_updated(self):
        csv_path = os.path.join(self.tmp, "demo.csv")
        write_demo_csv(csv_path)
        before = base.metrics_get_all().get("ACT_Queue", {}).get("total_segments", 0)

        res = run_pipeline(
            find_demo.f_demo_csv_trigger,
            {"file_path": csv_path, "interval": 0.005, "time_format": TIME_FORMAT},
            COL_DEF_FIND,
            act_queue.a_template_imq_trigger,
            {"teams_notify": False,
             "notify_values": [{"loc": 6, "row": 0}, {"loc": 6, "row": -1}]},   # 列6 = val（先頭4列がヘッダ）
            SEG_START, SEG_END, n_segments=4)

        self.assertPipelineOk(res)
        self.assertEqual([len(s) for s in res.segments], EXPECTED_LENS)
        self.assertEqual([r[6] for r in res.segments[0]], [20.0, 30.0, 40.0])   # 終点行（IDLE）は含まれない
        self.assertTrue(all(r[5] == "RUN" for s in res.segments for r in s))

        m = base.metrics_get_all()["ACT_Queue"]
        self.assertEqual(m["total_segments"] - before, 4)       # act が4セグメント処理した
        self.assertEqual(m["val[row=0]"], 70.0)                 # 最後のセグメント（70, 80）の先頭行
        self.assertEqual(m["val[row=-1]"], 80.0)                # 最後のセグメントの最終行


########################################
# B. find_demo (CSV) -> act_socket
########################################
class TestDemoCsvToSocket(PipelineTestCase):

    def _find_args(self):
        csv_path = os.path.join(self.tmp, "demo.csv")
        write_demo_csv(csv_path)
        return {"file_path": csv_path, "interval": 0.005, "time_format": TIME_FORMAT}

    def test_send_all_rows(self):
        srv = self.start_server(SocketServer(mode="put_only"))
        res = run_pipeline(
            find_demo.f_demo_csv_trigger, self._find_args(), COL_DEF_FIND,
            act_socket.a_send_seg_trigger,
            {"server_ip": "127.0.0.1", "server_port": srv.port,
             "timeout_sec": 2.0, "reconnect_interval_sec": 1.0,
             "ack_mode": True},                  # 送信ごとにサーバの "OK" を読む（読まずに切断すると、末尾を取りこぼすことがある）
            SEG_START, SEG_END, n_segments=2)
        self.assertPipelineOk(res)

        self.assertTrue(srv.wait_puts(5), f"rows received: {len(srv.puts)}")   # 3行 + 2行（1行ごとに PUT）
        rows = srv.puts
        self.assertEqual([r["val"] for r in rows], [20.0, 30.0, 40.0, 70.0, 80.0])
        self.assertTrue(all(r["lot"] == "RUN" for r in rows))
        self.assertEqual(set(rows[0].keys()),
                         {"seg_id", "seg_ind", "seg_ts", "seg_status", "ts", "lot", "val"})

    def test_send_stats(self):
        srv = self.start_server(SocketServer(mode="put_only"))
        res = run_pipeline(
            find_demo.f_demo_csv_trigger, self._find_args(), COL_DEF_FIND,
            act_socket.a_send_stats_trigger,
            {"server_ip": "127.0.0.1", "server_port": srv.port,
             "timeout_sec": 2.0, "reconnect_interval_sec": 1.0, "ack_mode": True,
             "save_dir": self.tmp,
             "column_register_map": {"0": [6, "mean"], "1": [6, "max"], "2": [-1, "seg_len"]},
             "out_coldef": {"0": ["val_mean", "float", "num", "obj", "mean"],
                            "1": ["val_max",  "float", "num", "obj", "max"],
                            "2": ["rows",     "int",   "num", "exp", "rows"]}},
            SEG_START, SEG_END, n_segments=2)
        self.assertPipelineOk(res)

        self.assertTrue(srv.wait_puts(2), f"rows received: {len(srv.puts)}")
        a, b = srv.puts[:2]
        self.assertEqual((a["val_mean"], a["val_max"], a["rows"], a["seg_rows"]), (30.0, 40.0, 3, 3))
        self.assertEqual((b["val_mean"], b["val_max"], b["rows"], b["seg_rows"]), (75.0, 80.0, 2, 2))

        # 出力列の定義が JSON として保存される（受信側がスキーマを参照できるように）
        schema = os.path.join(self.tmp, "output_column_define_a_send_stats_trigger.json")
        with open(schema, encoding="utf-8") as f:
            self.assertEqual(json.load(f)["0"][0], "val_mean")


########################################
# C. find_socket -> act_tmpl_queue
########################################
class TestSocketToTemplate(PipelineTestCase):

    def test_receive_from_sample_server_and_cut(self):
        srv = self.start_server(SocketServer(rows=ROWS))
        res = run_pipeline(
            find_socket.f_socket_client_trigger,
            {"server_ip": "127.0.0.1", "server_port": srv.port, "polling_ms": 2,
             "connect_timeout_sec": 2, "reconnect_interval_sec": 1, "time_format": TIME_FORMAT},
            COL_DEF_FIND,
            act_queue.a_template_imq_trigger, {"teams_notify": False},
            SEG_START, SEG_END, n_segments=4)
        self.assertPipelineOk(res)
        self.assertEqual([len(s) for s in res.segments], EXPECTED_LENS)
        self.assertEqual([r[6] for r in res.segments[1]], [70.0, 80.0])
        self.assertIsInstance(res.segments[0][0][4], dt)        # 時刻の列は datetime に変換されている


########################################
# D. エラー通知（ErrorIF にイベントが届く）
########################################
class TestErrorReport(PipelineTestCase):

    def test_find_demo_bad_file_is_reported_as_config_error(self):
        res = run_pipeline(
            find_demo.f_demo_csv_trigger,
            {"file_path": os.path.join(self.tmp, "no_such_file.csv")},
            COL_DEF_FIND,
            act_queue.a_template_imq_trigger, {"teams_notify": False},
            SEG_START, SEG_END, n_segments=1, timeout_sec=1.5)
        self.assertTrue(res.timed_out)                          # find が起動に失敗して止まったので、データは来ない
        self.assertEqual(res.errors, [])                        # 例外は find の中で通知され、外へは漏れない
        events = ErrorIF.drain_events()
        hit = [e for e in events if e["trigger_id"] == "f_demo_csv_trigger"
               and e["phase"] == "startup" and e["category"] == "config"]
        self.assertTrue(hit, f"events: {events}")
        self.assertEqual(hit[0]["exc_type"], "FileNotFoundError")

    def test_find_socket_connection_refused_is_reported_as_comm_error(self):
        res = run_pipeline(
            find_socket.f_socket_client_trigger,
            {"server_ip": "127.0.0.1", "server_port": free_port(), "polling_ms": 10,
             "connect_timeout_sec": 1, "reconnect_interval_sec": 0.2},
            COL_DEF_FIND,
            act_queue.a_template_imq_trigger, {"teams_notify": False},
            SEG_START, SEG_END, n_segments=1, timeout_sec=1.5)
        self.assertTrue(res.timed_out)
        self.assertEqual(res.errors, [])
        events = ErrorIF.drain_events()
        hit = [e for e in events if e["trigger_id"] == "f_socket_client_trigger"
               and e["phase"] == "startup" and e["category"] == "comm"]
        self.assertTrue(hit, f"events: {events}")
        self.assertEqual(hit[0]["exc_type"], "ConnectionRefusedError")


########################################
# E. サンプルCSV を LOT ごとに切り出し、統計量を出す
########################################
class TestSampleCsvPerLot(PipelineTestCase):

    def test_sample_csv_matches_column_definition(self):
        with open(SAMPLE_CSV, encoding="utf-8-sig", newline="") as f:
            header = next(csv.reader(f))
        self.assertEqual(header, [SAMPLE_COL_DEF[i][0] for i in range(len(SAMPLE_COL_DEF))])

    def test_cut_segments_per_lot_and_stats(self):
        runs = read_lot_runs()
        n = len(runs)                                      # LOT の数（= 1周で切り出されるセグメント数）
        self.assertGreaterEqual(n, 2)

        res = run_pipeline(
            find_demo.f_demo_csv_trigger,
            {"file_path": SAMPLE_CSV, "interval": 0.001},
            SAMPLE_COL_DEF,
            act_queue.a_template_imq_trigger, {"teams_notify": False},
            SAMPLE_SEG, SAMPLE_SEG, n_segments=n, timeout_sec=60)
        self.assertPipelineOk(res)
        self.assertEqual(ErrorIF.drain_events(), [])       # 異常通知は出ていない

        # 1) LOT ごとに1セグメント。行数は CSV の LOT ごとの行数と同じ（終点行 = 次の LOT の先頭行は除外される）
        self.assertEqual([len(s) for s in res.segments], [r[1] for r in runs])
        self.assertEqual([s[0][5] for s in res.segments], [r[0] for r in runs])       # 列5 = LOT
        for seg in res.segments:
            self.assertEqual(len({row[5] for row in seg}), 1)                          # セグメント内は同じ LOT

        # 2) 統計量（act 側の列番号 = find の列番号 + 4: Sin=6, Cos=7, Tan=8, CL=9）
        c_keys = [v[0] for v in ACT_HEADER.values()] + [SAMPLE_COL_DEF[i][0] for i in range(len(SAMPLE_COL_DEF))]
        crm = {"0": [6, "mean"], "1": [7, "max"], "2": [9, "max"], "3": [-1, "seg_len"]}
        oc  = {"0": ["sin_mean", "float", "num", "obj", ""], "1": ["cos_max", "float", "num", "obj", ""],
               "2": ["cl_max",   "int",   "cat", "obj", ""], "3": ["rows",    "int",   "num", "exp", ""]}
        compiled = stat.compile_register_map(crm, oc, c_keys)
        for seg, (lot, n_rows, rows) in zip(res.segments, runs):
            r = stat.build_stats(seg, compiled)
            self.assertEqual(r["rows"], n_rows, lot)
            self.assertAlmostEqual(r["sin_mean"], sum(float(x["Sin"]) for x in rows) / n_rows, places=3, msg=lot)
            self.assertAlmostEqual(r["cos_max"],  max(float(x["Cos"]) for x in rows),          places=3, msg=lot)
            self.assertEqual(r["cl_max"], max(int(x["CL"]) for x in rows), lot)         # 異常（1）を含む LOT は 1


if __name__ == "__main__":
    unittest.main(verbosity=2)
