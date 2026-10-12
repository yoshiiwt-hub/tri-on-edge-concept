# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [SLMP]
# Ver: 03.b.20261005    By: IWATA, Y.'s Agent AI
# Mod: 初回接続の失敗を ErrorIF へ報告（phase=startup / category=comm。A-21）。統計まわりを tri_s10_stream_wave_stat の init_stats / calc_stats に集約（値が出せなかった項目を共通形式でログに出す。B-9）
########################################
a_slmp_write_trigger  # セグメントデータをSLMPでPLCに書込む

動作概要
    1. キューからセグメントを受信
    2. write_mode="stat" の場合、tri_s10_stream_wave_stat でセグメント統計量を算出し、
       plc_write_map で指定したdevice/labelへそれぞれ書込む
    3. write_mode="seg" の場合、セグメント全行を1行ずつ書込む（統計計算は行わない）
    4. 各エントリの target に従い device（デバイス番地）または label（ラベル名）に書込む
    5. 正常完了後、done_flag を立てる（ハンドシェイク。相手が確認したら落とす想定なので立てたまま）
    6. 通信失敗時はオフラインモードへ移行し、自動再接続（skip_on_offline=true の場合はスキップ）
    7. ループ内で例外が発生した場合（comm/data問わず）、error_flag を立てる
       （ハンドシェイク。同様に立てたまま。comm例外時は接続断済みの可能性が高くベストエフォート）

    検証時は slmp_server.py（standalone, tri_edge_本体と独立）を起動し、
    plc_host=127.0.0.1, plc_port=5010 等で接続して動作確認する。

    GT SoftGOT2000のマイコン接続(Ethernet)・形式8/9(QnA互換3Eフレーム)へ接続する場合:
        - target="label" は使用不可（GT SoftGOT2000の仮想デバイスはD/M/L/R/SM/SDのみ）
        - network_no / pc_no を GT SoftGOT2000側の[通信設定]（Net No. / PC No.）と一致させること
        - デバイス点数・範囲の制限あり（例: ワード単位書込み最大244点、D:0-4095 等）

■ write_mode = "stat"（デフォルト）
    セグメント統計量を tri_s10_stream_wave_stat（CSV保存版 a_save_stats_trigger / TOYOPUC書込み版
    a_toyopuc_write_trigger と同一仕様）で算出し、plc_write_map で指定したdevice/labelへ書込む
    column_register_map / out_coldef : 上記モジュールと同一仕様
        （stat_name: mean min max std count range med iqr sum first last
                     pertl(p) sig_u sig_l(n_sigma) nth(n) mode n_null seg_len const）
    plc_write_map フォーマット（out_coldef の出力キー名をそのまま参照）
        {"out_key": {"target": "device"/"label", "dtype": str, ...(target別の追加キー)}}
        out_key に対応する統計値が None（no_valid_values等）の場合はその書込みをスキップする

■ write_mode = "seg"
    セグメント全行を1行ずつ書込む（統計計算は行わない）
    write_map フォーマット
        {idx: {"src_col": int, "target": str, "dtype": str, ...}}

■ target / dtype 共通
    target = "device" : {"device_type": str, "device_no": int}
    target = "label"  : {"label_name": str, "write_unit": int(word書込み時のみ), "write_len": int}
    dtype  : "bit"（0/1。deviceはRead-Modify-Write安全書込み、labelはネイティブビット書込み）
             "word"（符号なし16bit）| "int16"（符号付き16bit）
             "dword"（符号なし32bit, 2ワード）| "float"（float32, 2ワード）

接続設定
    plc_host               : PLC の IP アドレス（必須）
    plc_port               : SLMP ポート番号（デフォルト: 5010）
    plc_timeout_sec        : 接続・通信タイムアウト秒（デフォルト: 5.0）
    reconnect_interval_sec : 再接続試行間隔秒（デフォルト: 10.0）
    skip_on_offline        : オフライン時にセグメントをスキップするか（デフォルト: true）
    network_no              : 接続先のネットワークNo.（デフォルト: 0x00 = 自局指定）
                              GT SoftGOT2000等、相手機器側の通信設定に値を合わせる場合に指定する。
    pc_no                   : 接続先の局番（デフォルト: 0xFF = 自局指定）
                              GT SoftGOT2000等、相手機器側の通信設定に値を合わせる場合に指定する。

フラグ設定共通フォーマット（done_flag / error_flag で共通。tri_edge_11_find_slmp_bulk_to_rec.py と同一）
    device指定: {"source": "device", "type_no": [device_type, device_no], "value": ...}
    label指定 : {"source": "label",  "name_range": [label_name, byte_length], "value": ...}
                （2要素の場合は unit=1・バイト単位固定。後方互換のデフォルト）
              : {"source": "label",  "name_range": [label_name, unit, length], "value": ...}
                （3要素の場合は unit を明示指定できる。0=ビット単位, 1=バイト単位。
                  ラベルがBit型の場合は unit=0 が必須）

done_flag / error_flag（ハンドシェイク用。どちらも必須。write_mapのdevice/labelとは別に指定する）
    {name}_flag.source / type_no / name_range : 上記フラグ設定共通フォーマットの通り
    {name}_flag.value               : 書込む値（デフォルト: 1）
    ※ 完了フラグ／異常フラグは、書込んだ後こちら側では落とさない
      （相手側が確認したら相手側で落とすハンドシェイク運用を想定）
"""
MODULE_NAME = "ACT_SLMP"

"""standard library"""
from   datetime import datetime  as dt
import os
import queue
import struct
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List

"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()

except ModuleNotFoundError:
    try:
        dir_valid = 'fa_slmp'
        cur_dir  = os.path.dirname(os.path.abspath(__file__))
        dir_name = os.path.basename(cur_dir)
        if dir_name != dir_valid:
            raise FileNotFoundError(f".py MUST be in '{dir_valid}' dir. Current: {dir_name}")

        root_dir = os.path.abspath(os.path.join(cur_dir, "..", ".."))
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
from   src.tri_s01_error_if               import ErrorIF, SIGNAL_STOP
from   src.tri_s10_stream_wave_stat          import init_stats, calc_stats  # Calc Stats（Act 共通。B-9）
import src.fa_slmp.tri_s19_slmp as slmp

"""system dir, file"""
system_root = base.get_system_root()
log_file    = base.get_log_path()
work_dir    = base.get_work_dir()

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# Flag Config Helper（done_flag / error_flag 用。tri_edge_11_find_slmp_bulk_to_rec.py と同じ形式）
########################################

def _parse_flag_block(block: Dict[str, Any], block_name: str) -> Dict[str, Any]:
    """{"source": "device"/"label", "type_no": [device_type, device_no],
    "name_range": [label_name, byte_length] または [label_name, unit, length]} を解決する。
    label の場合、name_range が2要素なら unit=1（バイト単位、後方互換のデフォルト）、
    3要素なら unit を明示指定できる（0=ビット単位, 1=バイト単位）。
    ラベルがBit型の場合は unit=0（ビット単位）を明示指定する必要がある
    （Bit型ラベルをbyte単位で読み書きしようとするとSLMPエラーになる）。"""
    if not isinstance(block, dict):
        raise ValueError(f"{block_name} must be an object: {block!r}")

    source = str(block.get("source", "device")).lower()
    result: Dict[str, Any] = {"source": source}

    if source == "device":
        type_no = block.get("type_no")
        if not (isinstance(type_no, (list, tuple)) and len(type_no) == 2):
            raise ValueError(f"{block_name}.type_no must be [device_type, device_no]: {block!r}")
        result["device_type"] = str(type_no[0]).upper()
        result["device_no"]   = int(type_no[1])

    elif source == "label":
        name_range = block.get("name_range")
        if not (isinstance(name_range, (list, tuple)) and len(name_range) in (2, 3)):
            raise ValueError(
                f"{block_name}.name_range must be [label_name, byte_length] "
                f"or [label_name, unit, length]: {block!r}")
        result["label_name"] = str(name_range[0])
        if len(name_range) == 2:
            result["label_unit"] = 1  # バイト単位（後方互換のデフォルト）
            result["length"]     = int(name_range[1])
        else:
            unit = int(name_range[1])
            if unit not in (0, 1):
                raise ValueError(f"{block_name}.name_range unit must be 0(bit) or 1(byte): {block!r}")
            result["label_unit"] = unit
            result["length"]     = int(name_range[2])

    else:
        raise ValueError(f"{block_name}.source must be 'device' or 'label', got: {source!r}")

    return result


########################################
# HELPER FUNCTIONS
########################################

def _encode_words(val: float, dtype: str) -> List[int]:
    """
    値をPLC書込みワード列に変換する（dtype="bit"はここでは扱わない。_plc_write参照）。
    dtype = "float"  → float32 little-endian → [lo_word, hi_word]
    dtype = "dword"  → uint32 → [lo_word, hi_word]
    dtype = "int16"  → int → 符号付き16bit two's complement → [word]
    dtype = "word"   → int → 符号なし16bit → [word]
    """
    if dtype == "float":
        lo, hi = struct.unpack("<HH", struct.pack("<f", float(val)))
        return [lo, hi]
    if dtype == "dword":
        v = int(round(val)) & 0xFFFFFFFF
        return [v & 0xFFFF, (v >> 16) & 0xFFFF]
    if dtype == "int16":
        return [int(round(val)) & 0xFFFF]
    # "word"
    return [int(round(val)) & 0xFFFF]


def _plc_write(
    plc_client: slmp.SlmpClient,
    entry     : Dict[str, Any],
    val       : Any,
    dtype     : str,
) -> None:
    """
    write_mapエントリに従いPLCに書込む。
    dtype="bit"の場合はword変換を経由せず、専用の安全な書込み方法を使う：
        target="device": device_write_bit_safe（Read-Modify-Writeで対象ビット以外を保護）
        target="label" : label_writeをREAD_UNIT_BITで呼ぶ（ネイティブビット単位書込み。RMW不要）
    """
    target = str(entry.get("target", "device")).lower()
    dtype  = str(dtype).lower()

    if dtype == "bit":
        bit_val = 1 if val else 0
        if target == "device":
            plc_client.device_write_bit_safe(str(entry["device_type"]).upper(), int(entry["device_no"]), bit_val)
        elif target == "label":
            write_len = int(entry.get("write_len", 1))
            plc_client.label_write(str(entry["label_name"]), slmp.READ_UNIT_BIT, write_len, [bit_val])
        else:
            raise ValueError(f"Unknown target: {target!r}  (valid: 'device', 'label')")
        return

    words = _encode_words(float(val), dtype)
    if target == "device":
        device_type = str(entry["device_type"]).upper()
        device_no   = int(entry["device_no"])
        plc_client.device_write(device_type, device_no, words)

    elif target == "label":
        label_name = str(entry["label_name"])
        write_unit = int(entry.get("write_unit", slmp.READ_UNIT_BYTE))
        write_len  = int(entry["write_len"])
        plc_client.label_write(label_name, write_unit, write_len, words)

    else:
        raise ValueError(f"Unknown target: {target!r}  (valid: 'device', 'label')")


########################################
# Act Trigger Function
########################################

def a_slmp_write_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """
    セグメントデータをSLMPでPLCに書込む
    Mod: 2026.06.20    By: IWATA,Y.
    """
    TRIGGER_NAME = "a_slmp_write_trigger"
    _SIG_OFFLINE = f"ConnectionFailed.comm.loop.{TRIGGER_NAME}"

    def _responsive_wait(seconds: float) -> None:
        """stop_event を見ながら細かく待つ（即座に停止へ反応できるように。tri_s11_find_slmp.py 等と同様）"""
        _wait_end = time.time() + seconds
        while time.time() < _wait_end and not stop_event.is_set():
            time.sleep(min(0.05, max(0.0, _wait_end - time.time())))

    def _write_flag(flag_cfg: Dict[str, Any], value: int) -> None:
        """done_flag / error_flag への書込み（device/label両対応。labelはlabel_unit（0=bit/1=byte）に従う）"""
        if flag_cfg["source"] == "device":
            plc_client.device_write_bit_safe(flag_cfg["device_type"], flag_cfg["device_no"], value)
        else:  # "label"
            plc_client.label_write(flag_cfg["label_name"], flag_cfg["label_unit"], flag_cfg["length"], [value])

    """======== LOOP INIT ========"""
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)

        """ col_def """
        n_col_def = len(col_def)
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_keys={col_keys}", log_file)

        """ 書込みモード """
        write_mode = str(config.get("write_mode", "stat")).lower()
        if write_mode not in ("stat", "seg"):
            raise ValueError(f"write_mode must be 'stat' or 'seg', got: {write_mode!r}")
        base.output_log(MODULE_NAME, f"write_mode={write_mode}", log_file)

        stats_ctx = None
        plc_write_map: List[Dict[str, Any]] = []  # write_mode="stat" 用
        write_map: List[Dict[str, Any]] = []      # write_mode="seg" 用

        if write_mode == "stat":
            out_coldef           = config.get("out_coldef")
            plc_write_map_raw    = config.get("plc_write_map", {})
            if not plc_write_map_raw:
                raise ValueError("write_mode='stat': plc_write_map is required.")

            stats_ctx    = init_stats(config, col_def, MODULE_NAME, TRIGGER_NAME)# 必須チェック・コンパイル

            out_keys = {str(out_coldef[k][0]) for k in out_coldef.keys()}
            for out_key, entry in plc_write_map_raw.items():
                if out_key not in out_keys:
                    raise ValueError(
                        f"plc_write_map: out_key={out_key!r} not found in out_coldef "
                        f"(valid: {sorted(out_keys)})")
                target = str(entry.get("target", "device")).lower()
                if target not in ("device", "label"):
                    raise ValueError(f"plc_write_map[{out_key}] target must be 'device' or 'label'")
                if target == "device" and ("device_type" not in entry or "device_no" not in entry):
                    raise ValueError(f"plc_write_map[{out_key}] device_type/device_no required for target='device'")
                if target == "label" and "label_name" not in entry:
                    raise ValueError(f"plc_write_map[{out_key}] label_name required for target='label'")
                if target == "label" and str(entry.get("dtype", "word")).lower() != "bit" and "write_len" not in entry:
                    raise ValueError(f"plc_write_map[{out_key}] write_len required for target='label'")
                new_entry = dict(entry)
                new_entry["out_key"] = out_key
                plc_write_map.append(new_entry)
            base.output_log(MODULE_NAME, f"plc_write_map entries={len(plc_write_map)}", log_file)

        else:  # write_mode == "seg"
            write_map_raw = config.get("write_map", {})
            if not write_map_raw:
                raise ValueError("write_mode='seg': write_map is required.")

            write_map = [v for _, v in sorted(write_map_raw.items(), key=lambda x: int(x[0]))]

            for i, entry in enumerate(write_map):
                src_col = int(entry.get("src_col", -1))
                if not (0 <= src_col < n_col_def):
                    raise ValueError(
                        f"write_map[{i}] src_col={src_col} out of range (0-{n_col_def-1})")
                target = str(entry.get("target", "device")).lower()
                if target not in ("device", "label"):
                    raise ValueError(f"write_map[{i}] target must be 'device' or 'label'")
                if target == "device" and "device_type" not in entry:
                    raise ValueError(f"write_map[{i}] device_type required for target='device'")
                if target == "device" and "device_no" not in entry:
                    raise ValueError(f"write_map[{i}] device_no required for target='device'")
                if target == "label" and "label_name" not in entry:
                    raise ValueError(f"write_map[{i}] label_name required for target='label'")
                if target == "label" and str(entry.get("dtype", "word")).lower() != "bit" and "write_len" not in entry:
                    raise ValueError(f"write_map[{i}] write_len required for target='label'")

            base.output_log(MODULE_NAME, f"write_map entries={len(write_map)}", log_file)

        """ PLC接続設定 """
        plc_host    = str(config.get("plc_host", ""))
        plc_port    = int(config.get("plc_port", 5010))
        plc_timeout = float(config.get("plc_timeout_sec", 5.0))
        reconnect_interval_sec = float(config.get("reconnect_interval_sec", 10.0))
        skip_on_offline = bool(config.get("skip_on_offline", True))

        # network_no / pc_no（省略時は自局指定のデフォルト: network_no=0x00, pc_no=0xFF）
        # GT SoftGOT2000のマイコン接続(Ethernet)等、SLMP対応PLC以外に接続する場合は、
        # 接続先側の[通信設定]（Net No. / PC No.）と一致させる必要がある。
        network_no = int(config.get("network_no", 0x00))
        pc_no      = int(config.get("pc_no", 0xFF))

        if not plc_host:
            raise ValueError("plc_host is required.")

        base.output_log(MODULE_NAME,
            f"plc={plc_host}:{plc_port} network_no={network_no} pc_no={pc_no} "
            f"reconnect_interval={reconnect_interval_sec} "
            f"skip_on_offline={skip_on_offline}", log_file)

        """ 完了フラグ／異常フラグ設定（任意。それぞれ独立にON/OFFできる。ハンドシェイク。
            立てたままにする。相手が確認したら相手側で落とす想定）
            {"enabled": true/false, "source": "device", "type_no": [device_type, device_no], "value": ...}
            {"enabled": true/false, "source": "label",  "name_range": [label_name, byte_length], "value": ...}
        """
        done_flag_raw    = config.get("done_flag")
        done_flag_enabled = bool(done_flag_raw.get("enabled", True)) if done_flag_raw else False
        done_flag_cfg    = None
        done_flag_value  = None
        if done_flag_enabled:
            done_flag_cfg   = _parse_flag_block(done_flag_raw, "done_flag")
            done_flag_value = int(done_flag_raw.get("value", 1))

        error_flag_raw    = config.get("error_flag")
        error_flag_enabled = bool(error_flag_raw.get("enabled", True)) if error_flag_raw else False
        error_flag_cfg    = None
        error_flag_value  = None
        if error_flag_enabled:
            error_flag_cfg   = _parse_flag_block(error_flag_raw, "error_flag")
            error_flag_value = int(error_flag_raw.get("value", 1))

        base.output_log(MODULE_NAME,
            f"done_flag_enabled={done_flag_enabled}  done_flag={done_flag_cfg}  done_flag_value={done_flag_value}  "
            f"error_flag_enabled={error_flag_enabled}  error_flag={error_flag_cfg}  error_flag_value={error_flag_value}",
            log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config",
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """ 初期接続 """
    plc_client = slmp.SlmpClient(plc_host, plc_port, timeout_sec=plc_timeout,
                                  auto_reconnect=False, network_no=network_no, pc_no=pc_no)
    is_online  = False
    try:
        plc_client.connect()
        is_online = True
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} PLC Connected. {plc_host}:{plc_port}", log_file)
    except Exception as e:
        _sig = ErrorIF.report_exception(e, phase="startup", category="comm",  # 初回接続の失敗を報告（A-21）
                                         trigger_id=TRIGGER_NAME, log_file=log_file)
        if _sig == SIGNAL_STOP:
            stop_event.set()
            return None
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Initial Connect FAILED. Go Offline. ({type(e).__name__}: {e})", log_file)

    skipped_seg_count    = 0
    reconnect_fail_count = 0
    last_reconnect_try   = 0.0

    """======== LOOP START ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start Loop...", log_file)
    while True:

        """キュー受信"""
        try:
            segment = post_queue.get(timeout=1.0)

        except queue.Empty:
            """--- オフライン時は再接続を試みる ---"""
            if not is_online:
                _now = time.time()
                if _now - last_reconnect_try >= reconnect_interval_sec:
                    last_reconnect_try = _now
                    try:
                        plc_client.connect()
                        is_online            = True
                        reconnect_fail_count = 0
                        base.output_log(MODULE_NAME,
                            f"{TRIGGER_NAME} Re-Connected. {plc_host}:{plc_port}", log_file)
                    except Exception as e:
                        reconnect_fail_count += 1
                        ErrorIF.report_event({
                            "trigger_id": TRIGGER_NAME,
                            "phase"     : "loop",
                            "category"  : "comm",
                            "exc_type"  : type(e).__name__,
                            "message"   : f"Re-Connect Failed ({reconnect_fail_count} times). {plc_host}:{plc_port}",
                            "signature" : _SIG_OFFLINE,
                        })
                        base.output_log(MODULE_NAME,
                            f"{TRIGGER_NAME} Re-Connect Failed ({reconnect_fail_count}). "
                            f"{plc_host}:{plc_port}  ({type(e).__name__}): {e}", log_file)

            ErrorIF.emit_summary_if_needed()

            if (stop_event.is_set() and
                    post_queue.qsize() == 0 and
                    getattr(post_queue, "unfinished_tasks", 0) == 0):
                break
            continue

        """sentinel → 終了"""
        if segment is None:
            post_queue.task_done()
            break

        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} ...Received Queue.")

        """セグメント処理"""
        try:
            n_col_act = len(segment[0])
            if n_col_act != n_col_def:
                base.output_log(MODULE_NAME,
                    f"Segment Columns NOT Match: seg_id={segment[0][0]}  "
                    f"Expected:{n_col_def}  Actual:{n_col_act}", log_file)
                continue

            seg_id  = segment[0][0]
            # 終点行（変化後）の除外は core が行う（CUT_LAST_ROW = True）

            if len(segment) == 0:
                base.output_log(MODULE_NAME,
                    f"WARNING  空セグメント。スキップ。 seg_id={seg_id}", log_file)
                continue

            """オフライン時の処理"""
            if not is_online:
                if skip_on_offline:
                    skipped_seg_count += 1
                    base.output_log(MODULE_NAME,
                        f"WARNING  OFFLINE - Segment Skipped. "
                        f"seg_id={seg_id}  total_skipped={skipped_seg_count}  "
                        f"{plc_host}:{plc_port}", log_file)
                    continue
                else:
                    base.output_log(MODULE_NAME,
                        f"{TRIGGER_NAME} OFFLINE - Waiting for reconnect... seg_id={seg_id}", log_file)
                    while not is_online and not stop_event.is_set():
                        try:
                            plc_client.connect()
                            is_online            = True
                            reconnect_fail_count = 0
                            base.output_log(MODULE_NAME,
                                f"{TRIGGER_NAME} Reconnected. {plc_host}:{plc_port}", log_file)
                        except Exception as e:
                            reconnect_fail_count += 1
                            base.output_log(MODULE_NAME,
                                f"{TRIGGER_NAME} Re-Connect Failed ({reconnect_fail_count}). "
                                f"Retry in {reconnect_interval_sec}s...", log_file)
                            _responsive_wait(reconnect_interval_sec)
                    if stop_event.is_set():
                        break

            """==== 書込み実行 ===="""
            write_error = False

            # ---- write_mode = "seg": 1行ずつ書込む ----
            if write_mode == "seg":
                for row_num, row in enumerate(segment, start=1):
                    for entry in write_map:
                        src_col = int(entry["src_col"])
                        dtype   = str(entry.get("dtype", "word"))
                        try:
                            raw_val = row[src_col]
                            if raw_val is None:
                                continue
                            _plc_write(plc_client, entry, raw_val, dtype)
                        except (ConnectionResetError, BrokenPipeError, OSError, RuntimeError) as e:
                            _sig = ErrorIF.report_exception(
                                e, phase="loop", category="comm",
                                trigger_id=TRIGGER_NAME, log_file=log_file)
                            try:
                                if error_flag_enabled:
                                    _write_flag(error_flag_cfg, error_flag_value)
                                    base.output_log(MODULE_NAME,
                                        f"{TRIGGER_NAME} Error Flag Written (comm). "
                                        f"{error_flag_cfg}  value={error_flag_value}", log_file)
                            except Exception as e_flag:
                                base.output_log(MODULE_NAME,
                                    f"WARNING  Failed to write error flag after comm error "
                                    f"(PLC likely already disconnected): {type(e_flag).__name__}: {e_flag}", log_file)
                            try: plc_client.disconnect()
                            except Exception: pass
                            is_online            = False
                            reconnect_fail_count = 0
                            last_reconnect_try   = time.time()
                            base.output_log(MODULE_NAME,
                                f"{TRIGGER_NAME} Connection Lost. to Offline. "
                                f"seg_id={seg_id}  row={row_num}", log_file)
                            if _sig == SIGNAL_STOP:
                                stop_event.set()
                            write_error = True
                            break
                        except Exception as e:
                            base.output_log(MODULE_NAME,
                                f"Write Error (row={row_num} src_col={src_col}): {e}", log_file)
                            write_error = True
                            try:
                                if error_flag_enabled:
                                    _write_flag(error_flag_cfg, error_flag_value)
                                    base.output_log(MODULE_NAME,
                                        f"{TRIGGER_NAME} Error Flag Written (data, entry-level). "
                                        f"{error_flag_cfg}  value={error_flag_value}", log_file)
                            except Exception as e_flag:
                                base.output_log(MODULE_NAME,
                                    f"WARNING  Failed to write error flag after entry-level error: "
                                    f"{type(e_flag).__name__}: {e_flag}", log_file)
                    # write_error は個別エントリの失敗を記録するだけ（commエラー以外は残りの
                    # エントリ・行も引き続き試みる。1つでも失敗していれば最終的に error_flag が立つ）
                    if not is_online:
                        break  # comm エラーで接続が切れた場合のみ、残りの行の処理を中断する

            # ---- write_mode = "stat": セグメント統計量を算出して書込む ----
            else:
                stats_dict = calc_stats(stats_ctx, segment, seg_id)
                for entry in plc_write_map:
                    out_key = entry["out_key"]
                    dtype   = str(entry.get("dtype", "word"))
                    try:
                        val = stats_dict.get(out_key)
                        if val is None:
                            warn = stats_dict.get(f"{out_key}_WARN")
                            base.output_log(MODULE_NAME,
                                f"WARNING  No valid value: seg_id={seg_id} "
                                f"out_key={out_key}  ({warn or 'no value'})  Skip write.", log_file)
                            continue
                        _plc_write(plc_client, entry, val, dtype)
                    except (ConnectionResetError, BrokenPipeError, OSError, RuntimeError) as e:
                        _sig = ErrorIF.report_exception(
                            e, phase="loop", category="comm",
                            trigger_id=TRIGGER_NAME, log_file=log_file)
                        try:
                            if error_flag_enabled:
                                _write_flag(error_flag_cfg, error_flag_value)
                                base.output_log(MODULE_NAME,
                                    f"{TRIGGER_NAME} Error Flag Written (comm). "
                                    f"{error_flag_cfg}  value={error_flag_value}", log_file)
                        except Exception as e_flag:
                            base.output_log(MODULE_NAME,
                                f"WARNING  Failed to write error flag after comm error "
                                f"(PLC likely already disconnected): {type(e_flag).__name__}: {e_flag}", log_file)
                        try: plc_client.disconnect()
                        except Exception: pass
                        is_online            = False
                        reconnect_fail_count = 0
                        last_reconnect_try   = time.time()
                        base.output_log(MODULE_NAME,
                            f"{TRIGGER_NAME} Connection Lost. to Offline. seg_id={seg_id}", log_file)
                        if _sig == SIGNAL_STOP:
                            stop_event.set()
                        write_error = True
                        break
                    except Exception as e:
                        base.output_log(MODULE_NAME,
                            f"Write Error (out_key={out_key!r}): {e}", log_file)
                        write_error = True
                        try:
                            if error_flag_enabled:
                                _write_flag(error_flag_cfg, error_flag_value)
                                base.output_log(MODULE_NAME,
                                    f"{TRIGGER_NAME} Error Flag Written (data, entry-level). "
                                    f"{error_flag_cfg}  value={error_flag_value}", log_file)
                        except Exception as e_flag:
                            base.output_log(MODULE_NAME,
                                f"WARNING  Failed to write error flag after entry-level error: "
                                f"{type(e_flag).__name__}: {e_flag}", log_file)
                        # commエラーではないため break しない（残りのエントリも引き続き試みる。
                        # 1つでも失敗していれば最終的に error_flag が立つ）

            if not write_error:
                base.output_log(MODULE_NAME,
                    f"{TRIGGER_NAME} Written. "
                    f"seg_id={seg_id}  rows={len(segment)}  mode={write_mode}", log_file)
                try:
                    if done_flag_enabled:
                        _write_flag(done_flag_cfg, done_flag_value)
                        base.output_log(MODULE_NAME,
                            f"{TRIGGER_NAME} Done Flag Written. {done_flag_cfg}  value={done_flag_value}", log_file)
                except Exception as e_flag:
                    base.output_log(MODULE_NAME,
                        f"WARNING  Failed to write done flag: {type(e_flag).__name__}: {e_flag}", log_file)

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="data",
                                            trigger_id=TRIGGER_NAME, log_file=log_file)
            try:
                if error_flag_enabled:
                    _write_flag(error_flag_cfg, error_flag_value)
                    base.output_log(MODULE_NAME,
                        f"{TRIGGER_NAME} Error Flag Written (data). "
                        f"{error_flag_cfg}  value={error_flag_value}", log_file)
            except Exception as e_flag:
                base.output_log(MODULE_NAME,
                    f"WARNING  Failed to write error flag after data error: "
                    f"{type(e_flag).__name__}: {e_flag}", log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue

        finally:
            post_queue.task_done()

    """======== LOOP END ========"""
    try:
        plc_client.disconnect()
    except Exception:
        pass

    if skipped_seg_count > 0:
        base.output_log(MODULE_NAME,
            f"WARNING  {TRIGGER_NAME} Stopped with {skipped_seg_count} segment(s) skipped (OFFLINE).", log_file)

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None

a_slmp_write_trigger.QMODE = "imq"
a_slmp_write_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)