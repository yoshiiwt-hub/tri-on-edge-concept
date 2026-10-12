# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [SLMP]
# Ver: 02.e.20261005    By: IWATA, Y.'s Agent AI
# Mod: 初回接続の失敗を ErrorIF へ報告（phase=startup / category=comm。A-21）
########################################
f_find_slmp_trigger  # PLCデバイス/ラベルを監視し、条件成立時にデータをキューに流す

動作概要
    1. SlmpClient で MELSEC PLC に TCP 接続（ポート番号 5010 等）
    2. flag_mode=True の場合、トリガーフラグ（デバイス or ラベル）を監視し
       trigger_value と一致した時だけ読出しを許可する（読出し許可ゲート。リセットは行わない）
    3. flag_mode=False の場合、ポーリングごとに常にデータを取得（連続収集）
    4. bulk_read で定義したデバイス/ラベルを一括読出しし、生の値のリスト（bulk_buffers）を作る
    5. column_register_map（src.tri_s19_extract_bitword 仕様）に従って列を組み立て queue_out に送信
    6. 通信失敗時はオフラインモードへ移行し、自動再接続

    ※ 波形の開始・終了判定（seg_start/seg_end）はこのモジュールでは行わない。
       col_def に含めたデータ列を使って stream_core 側（monitor設定）で判定すること。

    検証時は slmp_server.py（standalone, tri_edge_本体と独立）を起動し、
    plc_host=127.0.0.1, plc_port=5010 等で接続して動作確認する。

bulk_read フォーマット（device読出し・label読出しの両方をここに集約する）
    device: {"idx": ["device", device_type, device_no, "bit"/"word", count]}
        count は "word" なら読出しワード数、"bit" なら読出しワード数
        （ビットデバイスは16点/wordにパックされる仕様のため。count*16が実ビット数）
    label : {"idx": ["label", label_name, "bit"/"word", count]}
        count は "word" なら読出しワード数、"bit" なら読出しビット数
        （ラベル側は"bit"指定でネイティブなBit型ラベルを直接ビット単位読出しできる）
    例:
        "0": ["device", "M", 0, "bit",  16]     # M0〜M255（16word=256bit）
        "1": ["device", "D", 100, "word", 10]   # D100〜D109
        "2": ["label",  "g_iTest1", "word", 2]  # ラベル、2word分
        "3": ["label",  "Lbl", "bit", 8]        # ラベル、Bit型8点

column_register_map フォーマット（src.tri_s19_extract_bitword 仕様。TOYOPUC版と共通）
    "col_idx": {
        "sources"   : [{"bulk": bulk_id, "offset": N, "byte": "hi"/"lo"(任意)}, ...],
        "unit_dtype": "bit"/"int1"/"int1_u"/"str1"/"int2"/"int2_u"/"str2"/"int4"/"int4_u"/"float4",
        "scale"     : 任意。int系unit_dtypeを10^scaleで割って小数として扱う
    }
    offset は、"word"系bulkならワード位置、"bit"系bulkならビット位置（0始点）。
    int4/int4_u/float4（2word合成、下位word+上位wordの順）・const・複数sources連結も
    TOYOPUC版と全く同じ書式で使える（詳細は src.tri_s19_extract_bitword を参照）。

接続設定
    plc_host               : PLC の IP アドレス（必須）
    plc_port               : SLMP ポート番号（デフォルト: 5010）
    plc_timeout_sec        : 接続・通信タイムアウト秒（デフォルト: 5.0）
    polling_ms             : ポーリング間隔ミリ秒（デフォルト: 100）
    reconnect_interval_sec : 再接続試行間隔秒（デフォルト: 10.0）

フラグ設定共通フォーマット（trigger_flag）
    device指定: {"source": "device", "type_no": [device_type, device_no], "trigger_value": ...}
    label指定 : {"source": "label",  "name_range": [label_name, byte_length], "trigger_value": ...}
                （2要素の場合は unit=1・バイト単位固定。後方互換のデフォルト）
              : {"source": "label",  "name_range": [label_name, unit, length], "trigger_value": ...}
                （3要素の場合は unit を明示指定できる。0=ビット単位, 1=バイト単位。
                  ラベルがBit型の場合は unit=0 が必須）

トリガーフラグ設定（読出し許可ゲートのみ。リセットは行わない）
    flag_mode    : フラグ監視モード（デフォルト: true）
    trigger_flag : flag_mode=True の場合は必須。上記フラグ設定共通フォーマットの通り
                   trigger_flag.trigger_value : トリガー比較値（デフォルト: 1）。
                   この値と一致する間だけ読出しを許可する
"""
MODULE_NAME = "FIND_SLMP"

"""standard library"""
from   datetime import datetime  as dt
import os
import queue
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Optional

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
from   src.tri_s01_error_if          import ErrorIF, SIGNAL_STOP
from   src.tri_s19_extract_bitword import compile_column_register_map
import src.fa_slmp.tri_s19_slmp   as slmp

"""system dir, file"""
system_root = base.get_system_root()
log_file    = base.get_log_path()
work_dir    = base.get_work_dir()

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# Flag Config Helper（trigger_flag 用。tri_edge_11_find_slmp_bulk_to_rec.py と同じ形式）
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
# Find Trigger Function
########################################

def f_find_slmp_trigger(
    queue_out:  queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """
    MELSEC PLCをSLMPで監視し、条件成立時にデータをキューに流す
    Mod: 2026.08.30    By: IWATA,Y.

    ■ flag_mode=True（デフォルト）
        トリガーフラグ（デバイス or ラベル）を監視し、trigger_valueと一致した時だけ
        bulk_read を取得 → キュー送信（読出し許可ゲート。リセットは行わない）

    ■ flag_mode=False
        ポーリングごとに常にデータを取得しキューに送信（連続収集）

    ※ 波形の開始・終了判定はこのモジュールでは行わない。col_def に含めたデータ列を使って
       stream_core 側（monitor設定）で判定すること。

    ■ bulk_read フォーマット（device読出し・label読出しの両方をここに集約する）
        device: ["device", device_type, device_no, "bit"/"word", count]
        label : ["label",  label_name, "bit"/"word", count]

    ■ column_register_map フォーマット（src.tri_s19_extract_bitword 仕様。TOYOPUC版と共通）
        "col_idx": {
            "sources"   : [{"bulk": bulk_id, "offset": N, "byte": "hi"/"lo"(任意)}, ...],
            "unit_dtype": "bit"/"int1"/"int1_u"/"str1"/"int2"/"int2_u"/"str2"/"int4"/"int4_u"/"float4",
            "scale"     : 任意
        }
        int4/int4_u/float4 は下位word+上位wordの順（slmp.decode_dword/decode_float と同じ並び）
    """
    TRIGGER_NAME = "f_find_slmp_trigger"
    _SIG_OFFLINE = f"ConnectionFailed.comm.loop.{TRIGGER_NAME}"

    # ---- STARTUP ----
    try:
        """ PLC接続設定 """
        plc_host    = config.get("plc_host", "")
        plc_port    = int(config.get("plc_port", 5010))
        plc_timeout = float(config.get("plc_timeout_sec", 5.0))
        reconnect_interval_sec = float(config.get("reconnect_interval_sec", 10.0))

        if not plc_host:
            raise ValueError("plc_host is required.")

        """ トリガーフラグ設定（読出し許可ゲートのみ。リセットは行わない）
            {"source": "device", "type_no": [device_type, device_no], "trigger_value": ...}
            {"source": "label",  "name_range": [label_name, byte_length], "trigger_value": ...}
        """
        flag_mode = config.get("flag_mode", True)
        trigger_flag_cfg: Optional[Dict[str, Any]] = None
        trigger_value: int = 1

        if flag_mode:
            trigger_flag_raw = config.get("trigger_flag")
            if not trigger_flag_raw:
                raise ValueError("trigger_flag is required when flag_mode=True.")
            trigger_flag_cfg = _parse_flag_block(trigger_flag_raw, "trigger_flag")
            trigger_value    = int(trigger_flag_raw.get("trigger_value", 1))

            base.output_log(MODULE_NAME,
                f"flag_mode=True  trigger_flag={trigger_flag_cfg}  "
                f"trigger_value={trigger_value}（読出し許可ゲート。リセットは行わない）", log_file)

        """
        データ読出し設定
        bulk_read（device/labelを1つの設定に集約。src.tri_s19_extract_bitword で共通処理する）
            device: ["device", device_type, device_no, "bit"/"word", count]
            label : ["label",  label_name, "bit"/"word", count]
        """
        bulk_read_raw = config.get("bulk_read", {})
        if not bulk_read_raw:
            raise ValueError("bulk_read is required.")

        bulk_specs: List[Dict[str, Any]] = []  # 読出し実行用。bulk_read_raw のキー順に対応
        for bid, row in sorted(bulk_read_raw.items(), key=lambda x: int(x[0])):
            if not isinstance(row, (list, tuple)) or len(row) < 4:
                raise ValueError(f"bulk_read[{bid}] must be a list. Got: {row!r}")
            kind = str(row[0]).lower()

            if kind == "device":
                if len(row) != 5:
                    raise ValueError(
                        f"bulk_read[{bid}] (device) must be "
                        f"['device', device_type, device_no, 'bit'/'word', count]. Got: {row!r}")
                unit = str(row[3]).lower()
                if unit not in ("bit", "word"):
                    raise ValueError(f"bulk_read[{bid}] unit must be 'bit' or 'word'. Got: {row[3]!r}")
                bulk_specs.append({
                    "kind": "device", "device_type": str(row[1]).upper(),
                    "device_no": int(row[2]), "unit": unit, "count": int(row[4]),
                })

            elif kind == "label":
                unit = str(row[2]).lower()
                if unit not in ("bit", "word"):
                    raise ValueError(f"bulk_read[{bid}] unit must be 'bit' or 'word'. Got: {row[2]!r}")
                bulk_specs.append({
                    "kind": "label", "label_name": str(row[1]),
                    "unit": unit, "count": int(row[3]),
                })

            else:
                raise ValueError(f"bulk_read[{bid}] kind must be 'device' or 'label'. Got: {row[0]!r}")

        base.output_log(MODULE_NAME, f"bulk_specs={bulk_specs}", log_file)

        """column_register_map（src.tri_s19_extract_bitword 仕様。TOYOPUC版と共通）"""
        column_register_map = config.get("column_register_map", {})
        if not column_register_map:
            raise ValueError("column_register_map is required.")
        n_col_def = len(col_def)
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]

        compiled_column_map = compile_column_register_map(
            column_register_map, col_def, bulk_read_raw,
            words_to_int_fn=slmp.words_to_int,
            words_to_float32_fn=slmp.decode_float,
        )
        base.output_log(MODULE_NAME,
            f"compiled_column_map columns={[c for c, _ in compiled_column_map]}", log_file)

        """ ポーリング設定 """
        polling_sec = int(config.get("polling_ms", 100)) / 1000

        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} plc={plc_host}:{plc_port} flag_mode={flag_mode} "
            f"n_bulk_specs={len(bulk_specs)} n_col_def={n_col_def} "
            f"polling_ms={polling_sec*1000:.0f}",
            log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    # ---- HELPER ----

    def _read_bulk_buffers(plc: "slmp.SlmpClient") -> List[List[int]]:
        """
        bulk_specs に従って実際にPLCから読み出し、compile_column_register_map が要求する
        bulk_buffers（[[bulk0の生値...], [bulk1の生値...], ...]）を組み立てる。
        device/labelどちらも、"bit"指定なら個々のbit値(0/1)がフラットに並んだリスト、
        "word"指定ならword値がそのまま並んだリストになる（TOYOPUC版と同じ形）。
        """
        buffers: List[List[int]] = []
        for spec in bulk_specs:
            if spec["kind"] == "device":
                raw_words = plc.device_read_bulk(spec["device_type"], spec["device_no"], spec["count"])
                if spec["unit"] == "bit":
                    buffers.append(slmp.unpack_bits(raw_words, spec["count"] * 16))
                else:
                    buffers.append(raw_words)
            else:  # "label"
                if spec["unit"] == "bit":
                    result = plc.label_read(spec["label_name"], slmp.READ_UNIT_BIT, spec["count"])
                else:
                    result = plc.label_read(spec["label_name"], slmp.READ_UNIT_BYTE, spec["count"] * 2)
                buffers.append(result["values"])
        return buffers

    def _read_trigger_flag() -> Optional[int]:
        """トリガーフラグ値を取得（device/label両対応。deviceはビット安全アクセス。
        labelはlabel_unit（0=bit/1=byte）に従う。読出し許可判定のみに使用）"""
        if trigger_flag_cfg["source"] == "device":
            return plc_client.device_read_bit_safe(trigger_flag_cfg["device_type"], trigger_flag_cfg["device_no"])
        else:  # "label"
            result = plc_client.label_read(
                trigger_flag_cfg["label_name"], trigger_flag_cfg["label_unit"], trigger_flag_cfg["length"])
            return result["values"][0]

    def _responsive_wait(seconds: float) -> None:
        """stop_event を見ながら細かく待つ（即座に停止へ反応できるように。tri_edge_11_find_citl_mdr_native.py 等と同様）"""
        _wait_end = time.time() + seconds
        while time.time() < _wait_end and not stop_event.is_set():
            time.sleep(min(0.05, max(0.0, _wait_end - time.time())))

    # ---- INITIAL CONNECT ----
    plc_client = slmp.SlmpClient(plc_host, plc_port, timeout_sec=plc_timeout, auto_reconnect=False)
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

    """======== MAIN LOOP START ========"""
    reconnect_fail_count = 0
    last_reconnect_try   = 0.0
    count = 0

    while not stop_event.is_set():
        try:
            if not is_online:
                """ オフラインモード：再接続を試みる """
                if time.time() - last_reconnect_try >= reconnect_interval_sec:
                    last_reconnect_try = time.time()
                    try:
                        plc_client.connect()
                        is_online            = True
                        reconnect_fail_count = 0
                        base.output_log(MODULE_NAME,
                            f"{TRIGGER_NAME} Re-Connected. {plc_host}:{plc_port}  Back to Online.", log_file)

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
                time.sleep(2)
                continue

            """ オンラインモード """
            # 1. トリガーフラグ確認（flag_mode=True の場合、device/label両対応。読出し許可ゲート）
            if flag_mode:
                flag_val = _read_trigger_flag()
                if flag_val != trigger_value:
                    _responsive_wait(polling_sec)
                    continue

            # 2. bulk_read（device/label両対応）を一括読出し
            bulk_buffers = _read_bulk_buffers(plc_client)

            # 3. column_register_map（辞書参照なし。コンパイル済み関数を呼ぶだけ）→ キュー送信
            #    （加工なしでそのまま流す。波形の開始・終了判定はstream_core側で行う）
            row = [None] * n_col_def
            for col_idx, col_func in compiled_column_map:
                row[col_idx] = col_func(bulk_buffers)
            queue_out.put((count, row))
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Queued #{count}: {row[:4]}...", log_file)
            count += 1

            _responsive_wait(polling_sec)

        except (ConnectionError, OSError, TimeoutError, RuntimeError) as e:
            _sig = ErrorIF.report_exception(
                e,
                phase="loop",
                category="comm",
                trigger_id=TRIGGER_NAME,
                log_file=log_file,
            )
            try:
                plc_client.disconnect()
            except Exception:
                pass
            is_online            = False
            reconnect_fail_count = 0
            last_reconnect_try   = time.time()
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Connection Lost. to Offline. {plc_host}:{plc_port}", log_file)

            if _sig == SIGNAL_STOP:
                stop_event.set()
                break

            continue

        except Exception as e:
            """通信以外の予期しない例外（バグ等）。オフライン化はせずスキップして継続する"""
            _sig = ErrorIF.report_exception(
                e,
                phase="loop",
                category="data",
                trigger_id=TRIGGER_NAME,
                log_file=log_file,
            )
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break

            continue

    """======== MAIN LOOP END ========"""
    try:
        plc_client.disconnect()
    except Exception:
        pass

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
