# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [Queue Template]
# Ver: 06.b.20261005    By: IWATA, Y.'s Agent AI
# Mod: stq_prepare_sql に core と同じく coldef_to_table_col(col_def) を渡すよう統一（B-4）
########################################
a_template_imq_trigger   # インメモリキュー（IMQ）モードの後処理ワーカー Act Trigger テンプレート
a_template_stq_trigger   # ストレージキュー（STQ）モードの後処理ワーカー Act Trigger テンプレート

動作概要
    1. 起動時に列定義（col_def）と個別設定を初期化する
    2. ループ内でキューからセグメントを受信し項目数の整合性を確認する
    3. セグメントの一括処理（波形全体）を実行する（終点行（変化後）は core が除外して渡す。CUT_LAST_ROW 参照）
    4. 続いてレコードごとの個別処理（波形各行）を実行する
    5. STQ モードでは SQLite ストレージキューから行データを取得し処理後にステータスを更新する
    6. stop_event が立ちキューが空になった時点でループを終了する
    7. セグメントの処理が終わるたびに、運用系メトリクス（ACT_Queue）へ登録し、オンデマンド通知を要求する
         last_seg_id         : 最新の処理セグメント id
         total_segments      : 累計の処理セグメント件数
         <列名>[row=<row>]   : notify_values で指定した列（loc）・行（row）の値
       ※ IMQ は項目数の不一致でスキップしたセグメント、STQ は異常になったセグメントを数えない

オンデマンド通知の設定（act_trigger）
    teams_notify : true → セグメント処理ごとにオンデマンド通知を要求する（既定 true。false はメトリクス登録のみ）
                   ※ 実際に送るかは Operator の teams_ondemand 設定による
    notify_values: 通知する値の位置のリスト（既定 []）。例 [{"loc": 6, "row": 0}, {"loc": 6, "row": -1}]
                   loc: 列番号（act 側。先頭4列はセグメントヘッダ）
                   row: セグメント内の行番号（core が終点行を除いた後の0始まり。負の数は後ろから。-1=最後の行）
                   範囲外の位置は値を "-" とする
"""
MODULE_NAME = "ACT_Queue"

"""standard library"""
from   datetime import datetime as dt
import os
import queue
import sys
import threading
import traceback
from   typing   import Any, Dict, List, Tuple, Sequence, Optional

"""external library"""
## Not Using

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
from   src.tri_s01_error_if       import ErrorIF, SIGNAL_STOP

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
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# On-Demand Notify Functions
########################################

def _notify_setup(config: Dict[str, Any], col_def: Dict[int, Any]) -> Tuple[bool, List[Tuple[str, int, int]]]:
    """
    オンデマンド通知の設定を読み、運用系メトリクスへ初期値を登録する
    Mod: 2026.10.02    By: IWATA, Y.'s Agent AI
    Returns:
        (teams_notify, [(メトリクスキー, loc, row), ...])
    """
    teams_notify = bool(config.get("teams_notify", True))
    items: List[Tuple[str, int, int]] = []
    for v in config.get("notify_values", []) or []:
        loc = int(v["loc"])
        row = int(v["row"])
        _def = col_def.get(loc)
        col_name = _def[0] if _def else f"col{loc}"
        items.append((f"{col_name}[row={row}]", loc, row))

    base.metrics_set_last(MODULE_NAME, "last_seg_id", "-")
    base.metrics_add(MODULE_NAME, "total_segments", 0)
    for key, _, _ in items:
        base.metrics_set_last(MODULE_NAME, key, "-")
    return teams_notify, items


def _notify_segment(seg_id: Any, rows: Sequence[Sequence[Any]], teams_notify: bool,
                    items: List[Tuple[str, int, int]]) -> None:
    """
    処理したセグメントを運用系メトリクスへ登録し、オンデマンド通知を要求する（範囲外の位置は "-"）
    Mod: 2026.10.02    By: IWATA, Y.'s Agent AI
    """
    base.metrics_set_last(MODULE_NAME, "last_seg_id", seg_id)
    base.metrics_add(MODULE_NAME, "total_segments", 1)
    for key, loc, row in items:
        try:
            val = rows[row][loc]
        except (IndexError, TypeError):
            val = "-"
        base.metrics_set_last(MODULE_NAME, key, val)
    if teams_notify:
        base.metrics_notify(MODULE_NAME)


########################################
# Act Trigger Functions
########################################

def a_template_imq_trigger(
    post_queue: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    後処理ワーカー（Act Trigger）のテンプレート
    Mod: 2026.10.02    By: IWATA, Y.'s Agent AI（オンデマンド通知を追加）
    """
    TRIGGER_NAME = "a_template_imq_trigger"
    func_name = sys._getframe().f_code.co_name
    func_obj = globals().get(func_name)
    func_qmode = getattr(func_obj, "QMODE", "imq")
    
    """======== LOOP INIT ========"""
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Before Loop Setting.", log_file)
        
        # データ項目定義
        n_col_def = len(col_def)#項目数
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_def={col_def}", log_file)

        """####（個別設定を記述）####"""
        base.output_log(MODULE_NAME, f"act trigger setting={None}", log_file)##

        # オンデマンド通知
        teams_notify, notify_items = _notify_setup(config, col_def)
        base.output_log(MODULE_NAME, f"teams_notify={teams_notify}  notify_values={notify_items}", log_file)
        
    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None
    
    """======== LOOP START ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start Loop...", log_file)
    while True:
        """ キュー受信 """
        try:
            segment = post_queue.get(timeout=1.0)
        
        except queue.Empty:
            exit_loop = (
                stop_event.is_set()      and
                post_queue.qsize() == 0  and
                getattr(post_queue, "unfinished_tasks", 0) == 0
            )
            if exit_loop: break
            continue
        
        if segment is None:
            post_queue.task_done()
            break
        
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} ...Received Queue.")
        
        """ セグメント処理 実行 """

        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start to Process Segment Data...")
        try:
            """項目の整合性（※現状、個数のみ）"""
            n_col_act = len(segment[0])
            if n_col_act != n_col_def:
                base.output_log(MODULE_NAME,
                f"Segment Columns NOT Match with Columns Define: seg_id={segment[0][0]}  Expected:{n_col_def}  Actual:{n_col_act}", log_file)
                continue
            
            """セグメントid"""
            seg_id = segment[0][0]
            
            """終点行（変化後）の除外は core が行う（CUT_LAST_ROW = True）"""
            
            """セグメント一括処理（波形全体）"""
            if 1 < len(segment):
                """####（segment内容）####"""
                #### TBD(C-4) #### 以下の print() はテンプレートの動作確認用。本番トリガーにコピーしたら削除するか、デバッグモード時のみの出力にすること
                print(f"type(segment)={type(segment).__name__}  len={len(segment)}", flush=True)  #### TBD(C-4) ####
                print(f"segment[0]={segment[0]}", flush=True)  #### TBD(C-4) ####
                print(f"segment[-1]={segment[-1]}", flush=True)  #### TBD(C-4) ####
                
                """####（処理内容を記述）####"""
                try:
                    pass####
                except Exception as e:
                    """####（異常時）####"""
                    base.output_log(MODULE_NAME,
                        f"Error in Processing Segment:\n{e}")
                        
            """レコードごと処理（波形の各１行）"""
            for _row, _val in enumerate(segment, start=1):
                """####（record内容）####"""
                #### TBD(C-4) #### 以下の print() はテンプレートの動作確認用。本番トリガーにコピーしたら削除するか、デバッグモード時のみの出力にすること
                if _row <= 3:
                    print("", flush=True)  #### TBD(C-4) ####
                    print(f"row={_row:<3}", flush=True)  #### TBD(C-4) ####
                    print(f"val[0]=SegmentID  {_val[0]}", flush=True)  #### TBD(C-4) ####
                    print(f"val[1]=SegmentIndex  {_val[1]}", flush=True)  #### TBD(C-4) ####
                    print(f"val[2:]=ActualValue", flush=True)  #### TBD(C-4) ####
                    for i, v in enumerate(_val[2:]): print(f"val[{str(i+2).rjust(2)}]  {v}", flush=True)  #### TBD(C-4) ####
                
                elif _row == 4:
                    print("", flush=True)  #### TBD(C-4) ####
                    print(f"...", flush=True)  #### TBD(C-4) ####
                    
                elif _row == len(segment)-1:
                    print("", flush=True)  #### TBD(C-4) ####
                    print(f"row={_row:<3}", flush=True)  #### TBD(C-4) ####
                    print(f"val[0]=SegmentID  {_val[0]}", flush=True)  #### TBD(C-4) ####
                    print(f"val[1]=SegmentIndex  {_val[1]}", flush=True)  #### TBD(C-4) ####
                    print(f"val[2:]=ActualValue", flush=True)  #### TBD(C-4) ####
                    for i, v in enumerate(_val[2:]): print(f"val[{str(i+2).rjust(2)}]  {v}", flush=True)  #### TBD(C-4) ####
                    print("", flush=True)  #### TBD(C-4) ####
                    
                """####（処理内容を記述）####"""
                try:
                    pass####
                except Exception as e:
                    """####（異常時）####"""
                    base.output_log(MODULE_NAME, f"Error in Processing Record [{_row}]:\n{e}")

            """オンデマンド通知（処理したセグメントの id・件数・指定した値）"""
            _notify_segment(seg_id, segment, teams_notify, notify_items)
                    
        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="data", trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break # finally経由でtask_done()後にループ離脱
            continue # ループ継続
        
        finally:
            post_queue.task_done()
    """======== LOOP END ========"""
    
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None

# CUT_LAST_ROW: True = セグメントの終点行（終了条件が成立した行。例: LOT 変化後の行・find の区切り行）を
#               core が除外してから渡す / False = 終点行を含めて全行を渡す（Act 側で扱いを判断する）
#               属性が無い場合は False と同じ扱い。意図を示すため、False の場合も必ず明記すること
#               ※ 上限（seg_max_rows / seg_max_sec）による強制クローズのセグメントは、終点行が無いため除外しない
a_template_imq_trigger.QMODE = "imq"
a_template_imq_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


def a_template_stq_trigger(
    post_queue: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    Act Trigger (STQモード) のテンプレート
    Mod: 2026.10.02    By: IWATA, Y.'s Agent AI（オンデマンド通知を追加）
    """
    TRIGGER_NAME = "a_template_stq_trigger"##
    func_name  = sys._getframe().f_code.co_name
    func_obj   = globals().get(func_name)
    func_qmode = getattr(func_obj, "QMODE", "imq")

    """======== LOOP INIT ========"""
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Before Loop Setting.", log_file)
        
        # データ項目定義
        n_col_def = len(col_def)#項目数
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_def={col_def}", log_file)
        
        # Storage Queue接続設定
        stq_path = base.get_storage_queue_path()
        tab_name = base.get_storage_queue_tab()
        conn     = base.stq_open(stq_path)
        base.output_log(MODULE_NAME, f"queue_mode={func_qmode}  stq_path={stq_path}  tab_name={tab_name}", log_file)
        
        # SQL準備
        sql = base.stq_prepare_sql(tab_name, base.coldef_to_table_col(col_def))# core と同じ型で渡す（B-4）
        sql_r = sql["select_new_seg"]
        sql_u_seg = sql["update_seg_status"]
        sql_u_rec = sql["update_rec_status"]
        sql_seg_err = sql["trans_seg_to_error"]
        sql_rec_err = sql["trans_rec_to_error"]
        
        """####（個別設定を記述）####"""
        dummy_error = config.get("dummy_error", False)####
        base.output_log(MODULE_NAME, f"dummy_error={dummy_error}", log_file)##

        # オンデマンド通知
        teams_notify, notify_items = _notify_setup(config, col_def)
        base.output_log(MODULE_NAME, f"teams_notify={teams_notify}  notify_values={notify_items}", log_file)
        
    except Exception as e:
        ErrorIF.report_exception(
            e,
            phase="startup",
            category="config",
            trigger_id=TRIGGER_NAME,
            log_file=log_file
        )
        stop_event.set()
        return None

    """======== LOOP START ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start Loop...", log_file)
    _dummy_error_seg = None
    while True:
        """キュー受信"""
        try:
            segment = post_queue.get(timeout=1.0)

        except queue.Empty:
            exit_loop = (
                stop_event.is_set()     and
                post_queue.qsize() == 0 and
                getattr(post_queue, "unfinished_tasks", 0) == 0
            )
            if exit_loop: break
            continue

        # センチネル受信時、終了
        if segment is None:
            post_queue.task_done()
            break

        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} ...Received Queue.")
        """セグメント処理"""
        try:
            seg_id, seg_len, seg_ts_start, seg_ts_end = segment
            rows = base.stq_select_new_seg(conn, sql_r, sql_u_seg, seg_id)
            if rows is None:
                base.output_log(MODULE_NAME,
        f"Segment NOT Found or Select Error: seg_id={seg_id}", log_file)
                continue
            
            """項目の整合性（※現状、個数のみ）"""
            n_col_act = len(rows[0])
            if n_col_act != n_col_def:
                base.output_log(MODULE_NAME,
        f"Segment Columns NOT Match with Columns Define: seg_id={seg_id}  Expected:{n_col_def}  Actual:{n_col_act}", log_file)
                continue
            
            """終点行（変化後）の除外は core が行う（CUT_LAST_ROW = True）"""
            
            """セグメント一括処理（波形全体）"""
            _seg_ok = False
            base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start to Process Segments. seg_id={seg_id}")
            """####（segment内容）####"""
            #### TBD(C-4) #### 以下の print() はテンプレートの動作確認用。本番トリガーにコピーしたら削除するか、デバッグモード時のみの出力にすること
            print(f"type(rows)={type(rows).__name__}  len(rows)={len(rows)}", flush=True)  #### TBD(C-4) ####
            print(f"rows[0])={rows[0]}", flush=True)  #### TBD(C-4) ####
            print(f"rows[-1])={rows[-1]}", flush=True)  #### TBD(C-4) ####

            try:
                """####（処理内容を記述）####"""
                pass####
                
                if dummy_error:####
                    _dummy_error_seg = (seg_id % 10) in [7,8,9,0]####
                    if _dummy_error_seg:
                        raise ValueError("__DUMMY__Error Raised in Segment Processing")####

                """####（正常完了）####"""
                base.stq_update_seg(conn, sql_u_seg, seg_id, 2)
                _seg_ok = True

            except Exception as e:
                """####（異常時）####"""
                base.output_log(MODULE_NAME, f"Error in Processing Segment [{seg_id}]:\n{e}")
                try:
                    base.stq_trans_seg_to_error(conn, sql_seg_err, sql_u_seg, seg_id, "Dummy Segment Error")
                except Exception as e_trans:
                    base.output_log(MODULE_NAME, f"Error in Trans Error: {e_trans}", log_file)
                    raise
            
            """レコード個別処理（波形各行）"""
            if not(_dummy_error_seg):####
                base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start to Process Each Rows. seg_id={seg_id}")
                for row_num, row_val in enumerate(rows, start=1):
                    """####（record内容）####"""
                    #### TBD(C-4) #### 以下の print() はテンプレートの動作確認用。本番トリガーにコピーしたら削除するか、デバッグモード時のみの出力にすること
                    if row_num <= 3:##
                            print(f"row_num={row_num}", flush=True)  #### TBD(C-4) ####
                            for _n, _v in enumerate(row_val):
                                print(f" {_n}\t{_v}", flush=True)  #### TBD(C-4) ####
                    
                    try:
                        """####（処理内容を記述）####"""
                        pass####
                        
                        if dummy_error:####
                            _dummy_error_rec = (row_num % 3 == 0) and (str(3) in str(row_num))####
                            if _dummy_error_rec:####
                                raise ValueError("Error Raised in Record Processing")####
    
                        """####（正常完了）####"""
                        base.stq_update_rec(conn, sql_u_rec, seg_id, row_num, 2)
    
                    except Exception as e:
                        """####（異常時）####"""
                        base.output_log(MODULE_NAME, f"__DUMMY__ Error in Processing Record [{row_num}] of Segment [{seg_id}]:\n{e}")
                        try:
                            base.stq_trans_rec_to_error(conn, sql_rec_err, sql_u_rec, seg_id, row_num, "Dummy Record Error")
                        except Exception as e_trans:
                            base.output_log(MODULE_NAME, f"Error in Trans Error: {e_trans}", log_file)
                            raise

            """オンデマンド通知（正常完了したセグメントのみ）"""
            if _seg_ok:
                _notify_segment(seg_id, rows, teams_notify, notify_items)

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="data",
                                            trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue

        finally:
            post_queue.task_done()
    """======== LOOP END ========"""

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None

# CUT_LAST_ROW: True = セグメントの終点行（終了条件が成立した行。例: LOT 変化後の行・find の区切り行）を
#               core が除外してから渡す / False = 終点行を含めて全行を渡す（Act 側で扱いを判断する）
#               属性が無い場合は False と同じ扱い。意図を示すため、False の場合も必ず明記すること
#               ※ 上限（seg_max_rows / seg_max_sec）による強制クローズのセグメントは、終点行が無いため除外しない
a_template_stq_trigger.QMODE = "stq"
a_template_stq_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, f"Run Standalone.")
    base.output_log(MODULE_NAME, f"test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
