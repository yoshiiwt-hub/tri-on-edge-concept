# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Stream Core Module
# Ver: 08.b.20261005    By: IWATA, Y.'s Agent AI
# Mod: import 時の副作用（二重起動確認・停止ファイル削除・設定読込）を main() 冒頭の _prepare_boot へ移動（A-6 / B-6）。QMODE の起動時検証（B-3）。裸の except を修正（C-5）
########################################

動作概要
    0. main() の冒頭で二重起動の確認・停止ファイルの削除・設定の読込を行う（import しただけでは何もしない。
       Operator が StreamCore を再起動するたびに設定を読み直す）
    1. trigger_setting_01.json の col_def / monitor_config / find_config を読み込む
    2. trigger_setting_02.json の act_config を読み込む
    3. find_trigger 関数をスレッドで起動し、queue_in（生データキュー）を生成
    4. メインループで queue_in からデータを受信し、tri_s10_stream_wave_cutout でセグメントを切り出す
       （開始/終了条件の判定・切り出しは wave_cutout、切り出されたセグメントの送信・STQ・集計は core）
    5. セグメント完成時に IMQ（In-Memory Queue）または STQ（Storage Queue / SQLite）に送信
       act_trigger 関数の CUT_LAST_ROW 属性が True なら終点行を除外してから送信する（属性なしは False＝全行）
       ※ 上限（seg_max_rows / seg_max_sec）による強制クローズのセグメントは終点行が無いため除外しない
    6. act_trigger 関数をスレッドで起動し、セグメントキューを消費させる
    7. ハートビートファイルを定期更新し、停止ファイルが現れたらループを終了
"""
MODULE_NAME = "STREAM"

"""standard library"""
from   datetime    import datetime as dt
import importlib
import os
from   pathlib     import Path
import queue
import sys
import threading
import time
import traceback
from   typing      import Any, Dict, List, Tuple, Sequence, Optional

"""external library"""
## NOT Use


"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()
    
except ModuleNotFoundError:
    try: # Check and Add Path
        cur_dir  = os.path.dirname(os.path.abspath(__file__))
        dir_name = os.path.basename(cur_dir)
        if dir_name != 'src':
            raise FileNotFoundError(f".py MUST be in 'src' dir. Current: {dir_name}")
        
        root_dir = os.path.dirname(cur_dir)
        if root_dir not in sys.path:
            sys.path.append(root_dir)
        
        for i in range(10):
            if cur_dir in sys.path:
                sys.path.remove(cur_dir)
            else:
                break
        
        import src.tri_s00_supervise_base as base
        sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully after Path Adjusted.\n")
        sys.stdout.flush()
        
    except Exception as e:
        msg = f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]    {e}\n{traceback.format_exc()}"
        sys.stdout.write(f"{msg}\n")
        sys.stdout.flush()
        
        with open(f"{MODULE_NAME}_Import_Error.log", "a", encoding="utf-8-sig") as f:
            f.write(f"{msg}\n")
        sys.exit(1)

"""local module"""
import src.tri_s01_error_if       as err
from   src.tri_s01_error_if import ErrorIF, SIGNAL_STOP
import src.tri_s10_stream_wave_cutout as cutout

"""system dir, file"""
system_root = base.get_system_root()
log_dir  = base.get_log_dir()
log_file = base.get_log_path()
work_dir       = base.get_work_dir()
work_db_dir    = base.get_work_sub_dirs().get("db"   , os.path.join(work_dir, "db"))
work_run_dir   = base.get_work_sub_dirs().get("run"  , os.path.join(work_dir, "run"))
work_spool_dir = base.get_work_sub_dirs().get("spool", os.path.join(work_dir, "spool"))

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__=='__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)

hb_file   = base.get_stream_hb_path()
stop_file = base.get_ctrl_stop_stream_path()

"""device info / setting file（main() 冒頭の _prepare_boot で読み込む）"""
device_info: Dict[str, Any] = {}
setting_01:  Dict[str, Any] = {}
setting_02:  Dict[str, Any] = {}


########################################
# Main Loop Processing
########################################
"""Global Var"""
# ※ ステージ状態（STAGE_TIMESTAMP / STAGE_COUNT）は tri_s10_stream_wave_cutout 側で持つ

# Storage Queue Clean-up
STQ_CLEANUP_LAST_TIME: float = time.time()
STQ_CLEANUP_PROC_COUNT: int = 0
STQ_CLEANUP_INTERVAL_SEC: float = 3600 # _prepare_boot で設定値に更新
STQ_CLEANUP_INTERVAL_COUNT: int = 100  # _prepare_boot で設定値に更新

# Segment Summary（検出セグメントの定期集計。ログファイルには集計のみ残す）
SEG_SUMMARY_INTERVAL_SEC: float = 3600 # _prepare_boot で設定値に更新
SEG_SUMMARY_LAST_TIME: float = time.time()
SEG_SUMMARY: Dict[str, Any] = {}


"""セグメント集計 関数群"""
def _seg_summary_reset() -> None:
    """集計値を初期化する"""
    global SEG_SUMMARY, SEG_SUMMARY_LAST_TIME
    SEG_SUMMARY = {
        "count": 0, "dropped": 0, "forced": 0, "q_max": 0,
        "rows_min": None, "rows_max": 0, "rows_sum": 0,
        "sec_min" : None, "sec_max" : 0.0, "sec_sum" : 0.0,
    }
    SEG_SUMMARY_LAST_TIME = time.time()


def _seg_summary_add(seg: List[List[Any]]) -> None:
    """検出したセグメントの行数・長さ（秒）を集計に加える"""
    rows = len(seg)
    sec  = (dt.strptime(seg[-1][2], "%Y-%m-%d %H:%M:%S.%f") - dt.strptime(seg[0][2], "%Y-%m-%d %H:%M:%S.%f")).total_seconds()
    s = SEG_SUMMARY
    s["count"]   += 1
    s["rows_min"] = rows if s["rows_min"] is None else min(s["rows_min"], rows)
    s["rows_max"] = max(s["rows_max"], rows)
    s["rows_sum"] += rows
    s["sec_min"]  = sec if s["sec_min"] is None else min(s["sec_min"], sec)
    s["sec_max"]  = max(s["sec_max"], sec)
    s["sec_sum"]  += sec


def _seg_summary_emit() -> None:
    """segment集計を運用系メトリクスへ登録し通知を要求"""
    s = SEG_SUMMARY
    n = s["count"]
    rows_str = f"{s['rows_min']}/{s['rows_sum'] / n:.1f}/{s['rows_max']}" if n else "-"
    sec_str  = f"{s['sec_min']:.1f}/{s['sec_sum'] / n:.1f}/{s['sec_max']:.1f}" if n else "-"
    period   = time.time() - SEG_SUMMARY_LAST_TIME
    base.output_log(MODULE_NAME,
        f"[SEG SUMMARY] period={period:.0f}s  segments={n}  rows(min/avg/max)={rows_str}  "
        f"sec(min/avg/max)={sec_str}  dropped={s['dropped']}  forced={s['forced']}  act_queue_max={s['q_max']}", log_file)

    _seg_summary_metrics(n, s["dropped"], s["forced"], dt.now().strftime("%Y/%m/%d %H:%M:%S"), n, rows_str, sec_str, s["q_max"])
    base.metrics_notify(MODULE_NAME)
    _seg_summary_reset()


def _seg_summary_metrics(add_segments: int, add_dropped: int, add_forced: int, summary_time: str,
                         segments: Any, rows: str, sec: str, q_max: Any) -> None:
    """segment集計値を運用系メトリクスへ登録"""
    base.metrics_add(MODULE_NAME, "total_segments", add_segments)
    base.metrics_add(MODULE_NAME, "total_dropped_segments", add_dropped)
    base.metrics_add(MODULE_NAME, "total_forced_segments", add_forced)
    base.metrics_set_last(MODULE_NAME, "last_summary_time", summary_time)
    base.metrics_set_last(MODULE_NAME, "last_period_segments", segments)
    base.metrics_set_last(MODULE_NAME, "last_period_rows", rows)
    base.metrics_set_last(MODULE_NAME, "last_period_sec", sec)
    base.metrics_set_last(MODULE_NAME, "last_period_act_queue_max", q_max)




########################################
# Main Process
########################################

def _prepare_boot() -> None:
    """
    起動準備（main() の冒頭で呼ぶ）。二重起動の確認・前回の停止ファイルの削除・端末情報と設定の読込を行う。
    従来はモジュールの import 時に実行していたため、Operator がプロセス内で StreamCore を再起動しても
    設定が読み直されず、二重起動時の KeyboardInterrupt も import 時に発生していた。
    Mod: 2026.10.05    By: IWATA, Y.'s Agent AI（import 時の副作用を移動。A-6 / B-6）
    Raises:
        KeyboardInterrupt("Already Running"): ハートビートが更新中（他の StreamCore が稼働中）
        SystemExit: 前回の停止ファイルを削除できない（従来どおりプロセスを終了する）
    """
    global device_info, setting_01, setting_02
    global STQ_CLEANUP_LAST_TIME, STQ_CLEANUP_PROC_COUNT, STQ_CLEANUP_INTERVAL_SEC, STQ_CLEANUP_INTERVAL_COUNT
    global SEG_SUMMARY_INTERVAL_SEC

    """二重起動の確認（ハートビートファイル）"""
    if os.path.exists(hb_file):
        try:
            base.output_log(MODULE_NAME, f"Hb_file Already Exists. Checking Current Heartbeat ...", log_file)
            time.sleep(5.0) # Wait Heartbeat for Safety
            if os.path.exists(hb_file): # for HB Removed in 5 min
                last_time = os.path.getmtime(hb_file)
                if time.time() - last_time < 5.0:
                    base.output_log(MODULE_NAME, f"Already Running (hb_file={hb_file}). Exit.", log_file)
                    raise KeyboardInterrupt("Already Running")
                else:
                    os.remove(hb_file)

        except KeyboardInterrupt:
            raise
        except Exception:
            base.output_log(MODULE_NAME, f"Can NOT Remove Previous Heartbeat File. System Go on.", log_file)
            pass
    base.output_log(MODULE_NAME, f"hb_file={hb_file}", log_file)

    """前回の停止ファイルの削除"""
    if os.path.exists(stop_file):
        try:
            os.remove(stop_file)
        except Exception:
            base.output_log(MODULE_NAME, f"Can NOT Remove Previous Stop File. To STOP System.", log_file)
            sys.exit(-1)
    base.output_log(MODULE_NAME, f"stop_file={stop_file}", log_file)

    """device info"""
    device_info = base.load_device_info()
    base.output_log(MODULE_NAME, f"device_info={device_info}", log_file)

    """setting file"""
    setting_01 = base.load_trigger_setting_01()
    setting_02 = base.load_trigger_setting_02()
    base.output_log(MODULE_NAME, f"setting_01={setting_01}", log_file)
    base.output_log(MODULE_NAME, f"setting_02={setting_02}", log_file)

    """設定値に依存するグローバル値"""
    STQ_CLEANUP_LAST_TIME      = time.time()
    STQ_CLEANUP_PROC_COUNT     = 0
    STQ_CLEANUP_INTERVAL_SEC   = (setting_02 or {}).get("STQ_CLEANUP_INTERVAL_SEC", 3600)
    STQ_CLEANUP_INTERVAL_COUNT = (setting_02 or {}).get("STQ_CLEANUP_INTERVAL_COUNT", 100)
    SEG_SUMMARY_INTERVAL_SEC   = float(((setting_01 or {}).get("monitor") or {}).get("seg_summary_interval_sec", 3600))
    return None


def _worker_wrapper(func, stop_event: threading.Event, trigger_id: str, *args, **kwargs):
    """ワーカー関数をラップし、未処理例外を ErrorIF へ報告するセーフティネット"""
    try:
        func(*args, **kwargs)
    except Exception as e:
        signal = ErrorIF.report_exception(
            e, phase="loop", category="internal", trigger_id=trigger_id,
            log_file=log_file, message=f"ワーカー未処理例外: {func.__name__}")
        if signal == SIGNAL_STOP:
            stop_event.set()
    return None


def _check_stop_control_file(stop_file_path: str, stop_detect_count: int) -> int:
    """
    停止制御ファイル検出処理。2回連続検出で KeyboardInterrupt を送出
    Args:
        stop_file_path: 停止制御ファイルのパス
        stop_detect_count: これまでの連続検出回数
    Returns:
        更新後の連続検出回数
    Raises:
        KeyboardInterrupt: 2回連続検出時
    """
    if os.path.exists(stop_file_path):
        stop_detect_count += 1
        if stop_detect_count >= 2:
            base.output_log(MODULE_NAME, f"Stop File Detected ({stop_file_path}). Initiating Gentle Stop...", log_file)
            try:
                os.remove(stop_file_path)
            except Exception as e:
                base.output_log(MODULE_NAME, f"Warning: Failed to Remove Stop File: {e}", log_file)
            raise KeyboardInterrupt("Stopped by Control File")
    else:
        stop_detect_count = 0

    return stop_detect_count


def _supervise_error_events() -> None:
    """
    エラーキュー回収・監督処理
    Raises:
        KeyboardInterrupt: supervisor_action が "shutdown_streamcore" または "stop_operator" の場合
    """
    events = ErrorIF.drain_events()
    for ev in events:
        rule = ErrorIF.apply_policy(ev)
        sig  = ev["signature"]
        
        # 頻発判定
        freq_thr = rule.get("frequent", {}).get("threshold", {})
        freq_window = freq_thr.get("window_sec", 60)
        freq_count  = freq_thr.get("count", 5)
        is_freq = ErrorIF.is_frequent(sig, window_sec=freq_window, count=freq_count)
        
        # 監督ログ出力
        sup_log = ErrorIF.format_supervisor_log(
            ev, rule, frequent=is_freq,
            window_sec=freq_window, count=freq_count)
        base.output_log(MODULE_NAME, sup_log, log_file)
        
        # 頻発時のエスカレーション
        if is_freq and ErrorIF.can_escalate(sig):
            ErrorIF.mark_escalated(sig)
            sup_action = rule.get("frequent", {}).get("supervisor_action", "")
            
            if sup_action == "shutdown_streamcore":
                base.output_log(MODULE_NAME,
                    f"エスカレーション: {sup_action}  signature={sig}", log_file)
                raise KeyboardInterrupt(f"ErrorIF escalation: {sup_action}")
            elif sup_action == "suppress_log":
                ErrorIF.enter_suppression(sig)
            elif sup_action == "stop_operator":
                base.output_log(MODULE_NAME,
                    f"エスカレーション: {sup_action}  signature={sig}", log_file)
                raise KeyboardInterrupt(f"ErrorIF escalation: {sup_action}")
   
    # ログ抑制サマリ出力
    ErrorIF.emit_summary_if_needed()
    return None


def main() -> None:
    """
    メイン処理（起動準備、ワーカー起動、ループ監視、ループ後処理）
    Mod: 2026.10.05    By: IWATA, Y.'s Agent AI（起動準備 _prepare_boot を冒頭で実行。QMODE の起動時検証。A-6 / B-6 / B-3）
    """
    """======== BOOT ========"""
    _prepare_boot()# 二重起動時は KeyboardInterrupt("Already Running") を Operator へ伝える

    try:
        """======== CONFIG ========"""
        policy_path = os.path.join(base.get_setting_dir(), "error_policy.json")
        ErrorIF.init(policy_path=policy_path, log_file=log_file)
        base.output_log(MODULE_NAME, f"ErrorIF initialized. policy={policy_path}", log_file)
        
        if not setting_01: raise FileNotFoundError(f"TriggerSetting-01 load failed or empty. path={setting_01}")
        if not setting_02: raise FileNotFoundError(f"TriggerSetting-02 load failed or empty. path={setting_02}")

        """Column Define"""
        col_define_f = {
            int(k): v for k, v in (setting_01.get("column") or {}).items()
        }
        if not col_define_f: raise ValueError("Find Trigger Column Setting Not Found.")
        
        col_define_f_keys   = [col_define_f[k][0] for k in sorted(col_define_f.keys())]
        col_define_f_dtypes = [col_define_f[k][1] for k in sorted(col_define_f.keys())]
        
        col_define_a_header = {
              0: ["seg_id"    , "int" , "key", "key", "Segment ID"]
            , 1: ["seg_ind"   , "int" , "key", "key", "Segment Row Index"]
            , 2: ["seg_ts"    , "time", "key", "key", "Timestamp When Data Collected"]
            , 3: ["seg_status", "int",  "key", "key", "Segment Process Status"]
        }
        
        col_define_a = {
            **col_define_a_header,
            **{int(k)+len(col_define_a_header): v for k, v in col_define_f.items()}
        }
        
        """Find Trigger (Input Worker) Config"""
        find_trigger_config = setting_01.get("find_trigger") or {}
        if not find_trigger_config: raise ValueError("Find Trigger Setting Not Found.")
        
        mod_find  = importlib.import_module(find_trigger_config["module"])
        func_find = getattr(mod_find, find_trigger_config["func"])
        
        base.output_log(MODULE_NAME, f"find_trigger_config  {find_trigger_config}", log_file)
        
        """Act Trigger (Post-Proc Worker) Config"""
        act_trigger_config = setting_02.get("act_trigger") or {}
        if not act_trigger_config: raise ValueError("Act Trigger Setting Not Found.")
        
        mod_act  = importlib.import_module(act_trigger_config["module"])
        func_act = getattr(mod_act, act_trigger_config["func"])
        
        """Queue Mode (In-Memory / Storage)"""
        _qmode_attr = getattr(func_act, "QMODE", None)
        act_qmode = _qmode_attr if _qmode_attr is not None else "imq"
        if act_qmode not in ("imq", "stq"):
            raise ValueError(f"act_trigger QMODE must be 'imq' or 'stq'. Got: {act_qmode!r}  func={act_trigger_config.get('func')}")
        is_stq = True if act_qmode == "stq" else False
        base.output_log(MODULE_NAME,
            f"act_qmode={act_qmode}  is_stq={is_stq}  ({'declared' if _qmode_attr is not None else 'default: not declared'})", log_file)

        """End Row Handling（CUT_LAST_ROW=True なら終点行を core で除外して渡す。属性なしは False＝全行）"""
        _cut_attr = getattr(func_act, "CUT_LAST_ROW", None)
        cut_last_row = bool(_cut_attr) if _cut_attr is not None else False
        base.output_log(MODULE_NAME,
            f"cut_last_row={cut_last_row}  ({'declared' if _cut_attr is not None else 'default: not declared'})", log_file)
        
        """Monitor Config"""
        LOOP_PERIOD_SEC = 0
        HEARTBEAT_INTERVAL_SEC = 3.0
        CTRL_CHECK_INTERVAL_SEC = 1.0
        
        monitor_config = setting_01.get("monitor") or {}
        if not monitor_config: raise ValueError("Monitor Setting Not Found.")
        base.output_log(MODULE_NAME, f"monitor_config  {monitor_config}", log_file)

        """Timestamp Round Config"""
        _ts_round_ms = int(monitor_config.get("timestamp_round_ms", 1))
        if _ts_round_ms < 0:
            raise ValueError(f"timestamp_round_ms must be >= 0. Got: {_ts_round_ms}")
        base.output_log(MODULE_NAME, f"timestamp_round_ms={_ts_round_ms}  ({'active' if _ts_round_ms > 0 else 'inactive: no rounding'})", log_file)

        """Segment Length Limit Config（0=無制限。先に達した方で強制クローズ）"""
        _seg_max_rows = int(monitor_config.get("seg_max_rows", 0) or 0)
        _seg_max_sec  = float(monitor_config.get("seg_max_sec", 0) or 0)
        if _seg_max_rows < 0 or _seg_max_sec < 0:
            raise ValueError(f"seg_max_rows / seg_max_sec must be >= 0. Got: seg_max_rows={_seg_max_rows}  seg_max_sec={_seg_max_sec}")
        base.output_log(MODULE_NAME, f"seg_max_rows={_seg_max_rows}  seg_max_sec={_seg_max_sec}  (0=unlimited)", log_file)

        """======== START WORKER ========"""
        """入力ワーカー（Find Trigger）起動"""
        input_queue = queue.Queue(maxsize=10000)
        stop_event_find = threading.Event()
        find_trigger_id = find_trigger_config.get("func", "find")
        t_find = threading.Thread(
            target=_worker_wrapper,
            args=(
                func_find,
                stop_event_find,
                find_trigger_id,
                input_queue,
                col_define_f,
                find_trigger_config,
                stop_event_find
            ),
            daemon=True)
        t_find.start()
        
        """後処理ワーカー（Act Trigger）起動"""
        act_queue = queue.Queue(maxsize=10000)
        stop_event_act = threading.Event()
        act_trigger_id = act_trigger_config.get("func", "act")
        t_act = threading.Thread(
            target=_worker_wrapper,
            args=(
                func_act,
                stop_event_act,
                act_trigger_id,
                act_queue,
                col_define_a,
                act_trigger_config,
                stop_event_act
            ),
            daemon=True)
        t_act.start()
    
    except Exception as e:
        base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}", log_file)
        raise  # Operatorへ委ねる（プロセスは落とさない）
    
    """======== MAIN LOOP ========"""
    MAX_LOOP_COUNT = int(1/0.01)*3600*24*10
    
    """波形データ [(timestamp, index, [values]),]"""
    segment_buffer: List[Tuple[dt, int, List[Any]]] = []
    prev_values: Optional[Sequence[Any]] = None
    prev_status: Optional[int] = 0
    seg_range = [None, None]

    # JSONの単一形式/多段形式を内部仕様 {stages:[...]} にラップ（tri_s10_stream_wave_cutout）
    start_spec = cutout.wrap_spec(monitor_config.get("seg_start", {}))
    end_spec   = cutout.wrap_spec(monitor_config.get("seg_end",   {}))
    
    # ステージローカル開始時刻・評価回数カウンタの初期化（状態は tri_s10_stream_wave_cutout 側で持つ）
    cutout.init_state(start_spec, end_spec, _seg_max_rows, _seg_max_sec)
    
    """点種別判定 1:開始点 2:中間点 3:終点 0:非波形"""
    trans_table = cutout.TRANS_TABLE
    base.output_log(MODULE_NAME, f"監視開始  seg_start={monitor_config.get('seg_start')}  seg_end={monitor_config.get('seg_end')}", log_file)
    
    # 直列ステージの成立進捗
    cond_progress = {
        "start": [False] * len(start_spec.get("stages", [])),
        "end":   [False] * len(end_spec.get("stages",   []))
    }

    """======== LOOP INIT ========"""
    loop_counter = 0
    last_heartbeat_time = 0.0
    my_pid = os.getpid()
    stop_detect_count = 0
    last_stop_check = time.time()
    seg_id = 0
    seg_ind = 0
    _seg_summary_reset()
    _seg_summary_metrics(0, 0, 0, "-", "-", "-", "-", "-")# 最初の集計前から Teams 等に STREAM 欄を表示させるための初期値
    base.output_log(MODULE_NAME, f"seg_summary_interval_sec={SEG_SUMMARY_INTERVAL_SEC}", log_file)

    # Prepare Storage Queue DB
    stq_conf = None
    if is_stq:
        # DB File Path, Table Name, Columns
        stq_path = base.get_storage_queue_path()
        tab_name = base.get_storage_queue_tab()
        tab_col  = base.coldef_to_table_col(col_define_a)
        
        # Initialize, Conn Config, SQL
        conn = base.initialize_db(stq_path, tab_name, tab_col)
        _sql = base.stq_prepare_sql(tab_name, tab_col)
        sql_i = _sql["insert_new_seg"]
        sql_d = _sql["delete_by_status"]
        sql_err_cnt = _sql["count_error"]
        sql_err_clr = _sql["clear_error"]
        stq_conf = [conn, sql_i, sql_d, sql_err_cnt, sql_err_clr]
    
    def _on_cutout_segment(seg_copy: List[List[Any]], seg_id: int, seg_ind: int, forced: bool = False) -> None:
        """
        セグメントが切り出されたときに tri_s10_stream_wave_cutout から呼ばれる関数（コールバック）
        集計 → 終点行の除外（CUT_LAST_ROW） → STQ 登録 → キュー送信（満杯時は破棄を記録）→ STQ 掃除 を行う
        forced=True は上限（seg_max_rows / seg_max_sec）による強制クローズ（終点行が無いため除外しない。件数を集計）
        Mod: 2026.10.04    By: IWATA, Y.'s Agent AI（CUT_LAST_ROW による終点行の除外を追加。B-2）
        """
        global STQ_CLEANUP_LAST_TIME, STQ_CLEANUP_PROC_COUNT
        post_queue = act_queue
        if stq_conf:
            conn, sql_i, sql_d, sql_err_cnt, sql_err_clr = stq_conf

        _seg_summary_add(seg_copy)# 集計は切り出したセグメント（終点行を含む）で行う
        if forced:
            SEG_SUMMARY["forced"] += 1

        """終点行の除外（CUT_LAST_ROW=True の Act のみ。強制クローズは除外しない）"""
        if cut_last_row and not forced:
            seg_copy = seg_copy[:-1]
            if not seg_copy:
                base.output_log(MODULE_NAME, f"WARNING  Segment Empty after Cut Last Row. Not Sent. seg_id={seg_id}", log_file)
                return None
            seg_ind = seg_copy[-1][1]
        try:
            if stq_conf: # Storage Queue
                base.stq_insert_new_seg(conn, sql_i, seg_copy)
                STQ_CLEANUP_PROC_COUNT += 1

        except Exception as e:
            ErrorIF.report_event({
                "trigger_id": MODULE_NAME,
                "phase":      "loop",
                "category":   "internal",
                "exc_type":   type(e).__name__,
                "message":    f"Storage Queue Insert Error: {e}",
            })

        try:
            if stq_conf: # In-Memory Queue (Summary Only)
                post_queue.put(
                    [
                        seg_id,
                        seg_ind,
                        seg_copy[0][2],
                        seg_copy[-1][2]
                    ],
                    block=False
                )

            else: # In-Memory Queue
                post_queue.put(
                    seg_copy,
                    block=False
                )

        except queue.Full:# 満杯時
            ErrorIF.report_event({
                "trigger_id": MODULE_NAME,
                "phase":      "loop",
                "category":   "io",# 宛先が受け取れない＝入出力側（A-17）
                "exc_type":   "QueueFull",
                "message":    "Post Queue Full. Segment Dropped.",
            })
            SEG_SUMMARY["dropped"] += 1

        """Clean-up (Status=Processed)"""
        if stq_conf:
            # 削除対象（seg_status=2 Act正常処理済）
            status_to_del = 2
            try:
                (
                    STQ_CLEANUP_LAST_TIME,
                    STQ_CLEANUP_PROC_COUNT,
                    n_deleted,
                    clear_error
                ) = base.stq_cleanup(
                    conn,
                    sql_d,
                    status_to_del,
                    sql_err_cnt,
                    sql_err_clr,
                    STQ_CLEANUP_LAST_TIME,
                    STQ_CLEANUP_PROC_COUNT,
                    STQ_CLEANUP_INTERVAL_SEC,
                    STQ_CLEANUP_INTERVAL_COUNT
                )
                if 0 < n_deleted:
                    base.output_log(MODULE_NAME, f"Storage Queue Clean-up Done. Deleted {n_deleted} Records.", log_file)
                if clear_error:
                    base.output_log(MODULE_NAME, f"Anomaly Queue Clean-up Done.", log_file)

            except Exception as e:
                base.output_log(MODULE_NAME, f"Storage Queue Clean-up Error: {e}", log_file)

    """======== LOOP START ========"""
    try:
        while True:
            current_time = time.time()
            
            """Check Control File to Stop Gently"""
            last_stop_check_sec = current_time - last_stop_check
            if last_stop_check_sec >= CTRL_CHECK_INTERVAL_SEC:
                last_stop_check = current_time
                
                # Check Stop File
                stop_detect_count = _check_stop_control_file(stop_file, stop_detect_count)
            
            """Record Heartbeat"""#### TBD（OSのRAM（tmpfs）を使用）####
            last_heartbeat_sec = current_time - last_heartbeat_time
            if last_heartbeat_sec >= HEARTBEAT_INTERVAL_SEC:
                last_heartbeat_time = current_time
                try:
                    Path(hb_file).touch()
                except Exception:
                    base.output_log(MODULE_NAME, f"Can NOT Record Heartbeat. System Go on.")
                    pass # 書込み異常でも継続を優先
                
                """エラーキュー回収・監督処理"""
                _supervise_error_events()

                """セグメント集計（act_queue 滞留数の最大値を記録し、集計間隔ごとにログ出力）"""
                SEG_SUMMARY["q_max"] = max(SEG_SUMMARY["q_max"], act_queue.qsize())
                if current_time - SEG_SUMMARY_LAST_TIME >= SEG_SUMMARY_INTERVAL_SEC:
                    _seg_summary_emit()
            
            """==== QUEUE PROCESSING ===="""
            """ 1. Receive Input Queue """
            try:
                (
                    index, # Queue Index from Boot
                    value  # Val (list/tuple)
                ) = input_queue.get(timeout=0.1)
                #### for debug ####
                if False:##
                    sys.stdout.write(f'[from find]  index={index}  value={value}\n')#\n\r##
                    sys.stdout.flush()##
                
            except queue.Empty:
                loop_counter += 1
                time.sleep(LOOP_PERIOD_SEC)
                continue

            """ 2. Format Data Buffer """
            timestamp = dt.now()
            record = [
                timestamp,  # TS when Core Received
                index + 1,  # Start in 1
                list(value) # Val (to list)
            ]
            #### for debug ####
            if False:##
                sys.stdout.write(f'\r[formated]  record={type(record)}  {len(record)}  {record}\n')
                sys.stdout.flush()##
            
            """ 3. Call Segment Process Function """
            (
                seg_id,   #out to next in
                seg_ind,  #out to next in
                prev_values,  #out to next in
                prev_status,  #out to next in
                seg_range  #out to next in
            ) = cutout.process_stream_record(
                seg_id,   #out to next in
                seg_ind,  #out to next in
                record,
                segment_buffer,
                _on_cutout_segment,
                prev_values,  #out to next in
                prev_status,  #out to next in
                seg_range,  #out to next in
                (start_spec, end_spec),
                trans_table,
                cond_progress,
                _ts_round_ms
            )
            #### for debug ####
            if False:##
                sys.stdout.write(f'\rloop_counter={loop_counter}  seg_range={seg_range}  seg_id={seg_id}  prev_status={prev_status}  prev_values={prev_values}\n')##
                sys.stdout.flush()##
            if False:##
                _to_see_loc = 2 ##
                if prev_status in (1, 2, 3):##
                    sys.stdout.write(f'{prev_values[_to_see_loc]}  {"#"*min(320, int(prev_values[_to_see_loc]))}\n')##
                    sys.stdout.flush()##
            
            loop_counter += 1
            if MAX_LOOP_COUNT <= loop_counter:
                loop_counter = 0
            
            time.sleep(LOOP_PERIOD_SEC)
        """======== LOOP END ========"""

    except KeyboardInterrupt:
        base.output_log(MODULE_NAME, f"Ctrl+C キー入力を検出しました。監視を停止します\nloop counter={loop_counter}\n", log_file)
    
    except Exception as e:
        base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\nloop counter={loop_counter}\n", log_file)
    
    finally:
        """======== AFTER LOOP ========"""
        base.output_log(MODULE_NAME, f"監視終了  loop_count={loop_counter}", log_file)
        _seg_summary_emit()# 停止までの途中分の集計
        
        # Worker終了（例外時も確実に実行）
        base.output_log(MODULE_NAME, f"Worker停止手順を開始", log_file)
        
        stop_event_find.set() # 入力側を先に停止（以後は post_queue.put しない）
        try:
            base.output_log(MODULE_NAME, f"残キュー処理開始", log_file)
            segment_buffer.clear()# 残バッファを破棄
            act_queue.put(None)# ワーカー数分のセンチネルを投入

            # 未完了タスクが 0 になるまで待機
            join_timeout_sec = 30
            start_join = time.time()
            prev_unfinished = -1            
            base.output_log(MODULE_NAME, f"post_queue の全タスク完了を最大 {join_timeout_sec} 秒待機します...", log_file)
            while act_queue.unfinished_tasks > 0:
                elapsed = time.time() - start_join

                if act_queue.unfinished_tasks != prev_unfinished:
                    base.output_log(MODULE_NAME, f"unfinished_tasks={act_queue.unfinished_tasks}  経過={elapsed:.1f}s", log_file)
                    prev_unfinished = act_queue.unfinished_tasks

                if elapsed > join_timeout_sec:
                    base.output_log(MODULE_NAME, f"警告: post_queue の完了待機がタイムアウトしました。未完了={act_queue.unfinished_tasks}", log_file)
                    break

                time.sleep(0.5)
            
            if act_queue.unfinished_tasks == 0:
                base.output_log(MODULE_NAME, "post_queue の全タスク完了を確認しました。", log_file)
            
            stop_event_act.set()# 念のため Post ワーカーに停止指示を送る（センチネルで停止済みのはず）
            try: t_act.join(timeout=5)
            except Exception: pass
            
            """End Whole Process"""
            time.sleep(2)
        
        except Exception as e:
            base.output_log(MODULE_NAME, f"{e}\n{traceback.format_exc()}\n", log_file)
            
            stop_event_find.set()
            
            # センチネル投入（例外時も確実に終了させる）
            try: act_queue.put(None)
            except Exception: pass
            
            # 未完了タスクを規定時間だけ待機
            start_join = time.time()
            while act_queue.unfinished_tasks > 0 and time.time() - start_join < 30:
                time.sleep(0.5)

            time.sleep(2)
    
    base.output_log(MODULE_NAME, f"残キュー処理完了", log_file)

    """remove heartbeat file"""
    if os.path.exists(hb_file):
        try:
            os.remove(hb_file)
        except Exception:
            base.output_log(MODULE_NAME, f"Can NOT Remove Heartbeat File.", log_file)
            pass
        
    """ErrorIF 終了処理"""
    ErrorIF.shutdown()
    base.output_log(MODULE_NAME, f"ErrorIF shutdown.", log_file)
    
    base.output_log(MODULE_NAME, "StreamCore Stopped Completely.", log_file)


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, "Run Standalone.")
    main()