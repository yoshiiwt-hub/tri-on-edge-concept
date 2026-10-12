# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [CSV Save]
# Ver: 02.b.20261005    By: IWATA, Y.'s Agent AI
# Mod: 統計まわりを init_stats / calc_stats / export_out_coldef に集約（出力内容は従来どおり。B-9）。端末情報の抽出は tri_s19_util に集約
########################################
a_save_seg_trigger    # セグメントの全行を1CSVファイルへ保存する
a_save_stats_trigger  # セグメントの統計量を算出し、1CSVファイルへ保存する

動作概要
    1. a_save_seg_trigger   : col_def のままCSVヘッダ（列名/型/role/var_type/コメント）を構成する
       a_save_stats_trigger : column_register_map / out_coldef から統計量算出定義を初期化する
    2. ループ内でキューからセグメントを受信し項目数の整合性を確認する
    3. セグメントの終点行（変化後）は core が除外して渡す（CUT_LAST_ROW = True）
    4. a_save_seg_trigger   : セグメントの全行をそのままCSVへ書込む（1セグメント=1ファイル）
       a_save_stats_trigger : build_stats() で統計量を算出し、ヘッダ+1行のCSVへ書込む
    5. device_key_to_add が指定されている場合は端末情報を付与する
    6. ack_ext が指定されている場合は CSV 保存後に ACK ファイル（.done 等）を作成する
    7. stop_event が立ちキューが空になった時点でループを終了する

接続設定（共通）
    save_dir          : CSV 保存先ディレクトリ（system_root からの相対パス、デフォルト: "output"）
    encode            : CSV ファイルの文字エンコーディング（デフォルト: "utf-8"）
    file_pfx          : CSV ファイル名プレフィックス（filename_tokens未指定時のみ使用。
                         デフォルト: a_save_seg_trigger="segments" / a_save_stats_trigger="stats"）
    ack_ext           : ACK ファイル拡張子（空文字列の場合は作成しない、デフォルト: ""）
    device_key_to_add : 端末情報から付与するキーのリスト（デフォルト: []）
    filename_tokens   : ファイル名を組み立てるトークンのリスト（省略可）
                         使える変数: "$999"（連番、999の次は001）| "$seg_start" | "$seg_end" | "$encode"
                         それ以外の文字列はリテラルとして扱う。"_" で連結し ".csv" を付与する
                         （a_save_stats_trigger では $seg_start/$seg_end は stats_dict の
                         seg_ts_start/seg_ts_end を使用）
                         省略時は従来通り "{file_pfx}{連番:03}_{seg_id}_{encode}.csv"

a_save_stats_trigger 専用設定
    column_register_map : 統計量算出用の列マッピング定義（必須）
    out_coldef           : 統計量の出力列定義（必須）
"""
MODULE_NAME = "ACT_Save_Csv"

"""standard library"""
import csv
from   datetime import datetime as dt
import os
import queue
import re
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Tuple, Optional

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
from   src.tri_s01_error_if       import ErrorIF, SIGNAL_STOP
from   src.tri_s10_stream_wave_stat import init_stats, calc_stats, export_out_coldef  # Calc Stats（Act 共通。B-9）
import src.tri_s19_util        as utl

"""system dir, file"""
system_root = base.get_system_root()
log_dir  = base.get_log_dir()
log_file = base.get_log_path()
work_dir = base.get_work_dir()

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# Filename Token Helper（旧stats版の堅牢なフォールバックに統一）
########################################

def _resolve_filename(
    tokens: List[str],
    counter: int,
    seg_start: Any,
    seg_end: Any,
    encode_: str,
) -> str:
    """
    filename_tokens に従ってファイル名を組み立てる。
    使える変数は "$999"（連番、999の次は001） / "$seg_start" / "$seg_end" / "$encode" のみ。
    それ以外の文字列はリテラルとして扱う。トークンは "_" で連結し、拡張子 ".csv" を付ける。
    """
    def _fmt_ts(v: Any) -> str:
        """タイムスタンプを YYYYMMDDHHMMSS + センチ秒2桁 の16桁文字列にする
        datetime型はそのまま整形。文字列（例: '2026-07-14 22:59:15.983000'）はパースしてから整形。
        パースできない場合は、ファイル名に使えない文字（: 空白 - . 等）を除去した文字列を返す
        （フォールバックとしてクラッシュだけは絶対に避ける）。
        """
        d = v
        if not isinstance(d, dt):
            for _fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S.%f", "%Y/%m/%d %H:%M:%S"):
                try:
                    d = dt.strptime(str(v), _fmt)
                    break
                except Exception:
                    d = None
            else:
                d = None

        if isinstance(d, dt):
            return d.strftime("%Y%m%d%H%M%S") + f"{d.microsecond // 10000:02d}"

        # パース失敗時のフォールバック：ファイル名に使えない文字を除去するだけ
        return re.sub(r"[^0-9A-Za-z]", "", str(v))

    parts = []
    for tok in tokens:
        if tok == "$999":
            disp = ((counter - 1) % 999) + 1  # 999の次は001（0は使わない）
            parts.append(f"{disp:03d}")
        elif tok == "$seg_start":
            parts.append(_fmt_ts(seg_start))
        elif tok == "$seg_end":
            parts.append(_fmt_ts(seg_end))
        elif tok == "$encode":
            parts.append(str(encode_))
        else:
            parts.append(str(tok))  # リテラル

    return "_".join(parts) + ".csv"


########################################
# 端末情報付与 Helper（共通）
########################################

def _resolve_device_info_to_add(config: Dict[str, Any]) -> Tuple[List[str], List[Any]]:
    """device_key_to_add で指定されたキーのうち device_info.json に存在するものだけを抽出する（tri_s19_util に集約。B-9）"""
    return utl.get_device_info_to_add(config)


########################################
# 共通保存ループ（a_save_seg_trigger / a_save_stats_trigger の実体）
########################################

def _run_save_trigger(
    mode:       str,  # "seg" または "stats"
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """CSVへ保存する共通ループ。mode で保存内容・ファイル構造だけが変わる"""
    TRIGGER_NAME = "a_save_seg_trigger" if mode == "seg" else "a_save_stats_trigger"

    """======== STARTUP ========"""
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start. mode={mode}", log_file)
        save_dir = os.path.join(system_root, config.get("save_dir", "output"))
        os.makedirs(save_dir, exist_ok=True)
        encode_  = config.get("encode", "utf-8")
        file_pfx = config.get("file_pfx", "segments" if mode == "seg" else "stats")
        ack_ext  = config.get("ack_ext", "")
        filename_tokens = config.get("filename_tokens")  # 省略時は従来のファイル名形式のまま
        base.output_log(MODULE_NAME, f"filename_tokens={filename_tokens}", log_file)

        """データ項目定義"""
        n_col_def = len(col_def)
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_keys={col_keys}", log_file)

        file_head: Optional[List[List[Any]]] = None
        stats_col_names: Optional[List[str]] = None

        if mode == "seg":
            """col_def をそのまま保存し、CSVヘッダを構成する"""
            utl.save_json(col_def, f'{save_dir}/column_define_{encode_}.json', encode_)

            file_head = [
                [col_def[s][0] for s in range(len(col_def.keys()))],  # col_name
                [col_def[s][1] for s in range(len(col_def.keys()))],  # data_type
                [col_def[s][2] for s in range(len(col_def.keys()))],  # num/cat
                [col_def[s][3] for s in range(len(col_def.keys()))],  # exp/obj
                [col_def[s][4] for s in range(len(col_def.keys()))],  # comment
            ]

            device_key_to_add, device_info_to_add = _resolve_device_info_to_add(config)
            if device_key_to_add:
                file_head[0] = file_head[0] + device_key_to_add
                file_head[1] = file_head[1] + ['str'] * len(device_key_to_add)
                file_head[2] = file_head[2] + ['key'] * len(device_key_to_add)
                file_head[3] = file_head[3] + ['key'] * len(device_key_to_add)
                file_head[4] = file_head[4] + ['____'] * len(device_key_to_add)

        else:  # mode == "stats"
            """統計量算出定義"""
            stats_ctx  = init_stats(config, col_def, MODULE_NAME, TRIGGER_NAME)
            out_coldef = stats_ctx.out_coldef

            stats_base_col_names = stats_ctx.base_col_names
            stats_col_names = stats_base_col_names  # ヘッダ用（device列を追加する場合はこちらに追記）

            # 出力列定義を保存（サーバー側等がスキーマ参照できるように）
            export_out_coldef(out_coldef, f'{save_dir}/column_define_{encode_}.json', encode_, MODULE_NAME)

            device_key_to_add, device_info_to_add = _resolve_device_info_to_add(config)
            if device_key_to_add:
                stats_col_names = stats_base_col_names + device_key_to_add

        base.output_log(MODULE_NAME,
            f"device_key_to_add={device_key_to_add}  device_info_to_add={device_info_to_add}", log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== LOOP START ========"""
    file_counter = 1
    while True:
        try:
            segment = post_queue.get(timeout=1.0)

        except queue.Empty:
            if stop_event.is_set() and post_queue.qsize() == 0 and getattr(post_queue, "unfinished_tasks", 0) == 0:
                break
            continue

        if segment is None:
            post_queue.task_done()
            break

        try:
            """項目の整合性（※現状、個数のみ）"""
            n_col_act = len(segment[0])
            if n_col_act != n_col_def:
                base.output_log(MODULE_NAME,
                    f"Segment Columns NOT Match with Columns Define: seg_id={segment[0][0]}  "
                    f"Expected:{n_col_def}  Actual:{n_col_act}", log_file)
                continue

            """セグメントid"""
            seg_id = segment[0][0]

            """終点行（変化後）の除外は core が行う（CUT_LAST_ROW = True）"""

            """空セグメントチェック"""
            if len(segment) == 0:
                base.output_log(MODULE_NAME, f"WARNING  セグメントが空です。スキップします。id={seg_id}", log_file)
                continue

            """空レコードチェック"""
            if (isinstance(segment[0][-1], list)) and (segment[0][-1][0] is None):
                base.output_log(MODULE_NAME, f"WARNING  レコードが空です。スキップします。id={seg_id}", log_file)
                continue

            """ファイル名組立"""
            if mode == "seg":
                if filename_tokens:
                    seg_start = segment[0][0]   # レコードのタイムスタンプ列（record[0]）
                    seg_end   = segment[-1][0]
                    filename = _resolve_filename(filename_tokens, file_counter, seg_start, seg_end, encode_)
                else:
                    filename = f"{file_pfx}{file_counter:03}_{seg_id}_{encode_}.csv"
            else:
                stats_dict = calc_stats(stats_ctx, segment, seg_id)
                row = [stats_dict.get(c) for c in stats_base_col_names]
                if device_info_to_add:
                    row = row + device_info_to_add

                if filename_tokens:
                    seg_start = stats_dict.get("seg_ts_start")
                    seg_end   = stats_dict.get("seg_ts_end")
                    filename = _resolve_filename(filename_tokens, file_counter, seg_start, seg_end, encode_)
                else:
                    filename = f"{file_pfx}{file_counter:03}_{seg_id}_{encode_}.csv"

            filepath = os.path.join(save_dir, filename)
            base.output_log(MODULE_NAME, f"{filename} 保存開始: seg_id={seg_id}", log_file)

            """CSVファイル保存"""
            with open(filepath, "w", newline="", encoding=encode_) as f:
                writer = csv.writer(f)

                if mode == "seg":
                    for i in [0, 4, 1, 2, 3]:
                        writer.writerow(file_head[i])
                    for row_val in segment:
                        output_row = row_val + device_info_to_add if device_info_to_add else row_val
                        writer.writerow(output_row)
                else:
                    writer.writerow(stats_col_names)
                    writer.writerow(row)

            """make Ack file"""
            if ack_ext:
                time.sleep(0.5)
                with open(f"{filepath}.done", "w") as f:
                    pass

            if mode == "seg":
                base.output_log(MODULE_NAME,
                    f"{filename} 保存完了: id={seg_id}  {segment[0][1]}~{segment[-1][1]}", log_file)
            else:
                base.output_log(MODULE_NAME,
                    f"{filename} 保存完了: id={seg_id}  rows={len(segment)}", log_file)# 値が出せなかった項目は calc_stats がログに出す

            file_counter += 1

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break  # finally経由でtask_done()後にループ離脱
            continue  # ループ継続

        finally:
            post_queue.task_done()
    """======== LOOP END ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None


########################################
# Act Trigger Functions（薄いラッパー）
########################################

def a_save_seg_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """セグメントの全行を1CSVファイルへ保存する"""
    return _run_save_trigger("seg", post_queue, col_def, config, stop_event)

a_save_seg_trigger.QMODE = "imq"
a_save_seg_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


def a_save_stats_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """セグメントの統計量を算出し、1CSVファイルへ保存する"""
    return _run_save_trigger("stats", post_queue, col_def, config, stop_event)

a_save_stats_trigger.QMODE = "imq"
a_save_stats_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, f"Run Standalone.")
    base.output_log(MODULE_NAME, f"test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)