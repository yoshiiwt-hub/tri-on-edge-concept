# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [Guard Band Diagnosis]
# Ver: 02.d.20261102    By: IWATA, Y.'s Agent AI
# Mod: モジュール冒頭の Docstring を規約に合わせて簡素化
########################################
波形を、事前に学習したガードバンド（上限・下限の帯）と比べて、異常な波形をエッジで見つける

主要関数:
a_gb_diag_trigger  # 波形をガードバンドと比べて異常を判定し、1セグメント = 1ファイルの CSV で保存する。
_load_gb_def       # ガードバンドの定義 CSV を読み込む（起動時に1回）。

位置づけ: 学習（ガードバンドの作成）は Stored 領域で行い、このモジュールは判定だけを受け持つ。判定結果を画像で見るデモ版は tri_s12_act_diag_gb_viz。
設計意図: 品番・設備状態などの値でモデル（ガードバンド）を切り替え、1台のエッジで複数の品種を判定できるようにする。ガードバンドの定義は起動時に読み込み、セグメントごとにファイルを読まない。pandas / numpy を使わず標準ライブラリだけで実装し、exe を軽く保つ。
前提・制約: 該当するモデルが無く、default_model も無いセグメントは判定しない（警告ログを出す）。
"""
MODULE_NAME = "ACT_GB_Diag"

"""standard library"""
import csv
from   datetime import datetime as dt
import json
import os
import queue
import sys
import threading
import time
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
        if dir_name != 'diag':
            raise FileNotFoundError(f".py MUST be in 'src/diag' dir. Current: {dir_name}")
        
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
import src.tri_s01_error_if       as err
from   src.tri_s01_error_if       import ErrorIF, SIGNAL_STOP
import src.tri_s19_util        as utl

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
# Guard Band Helper
########################################
_GB_DIAG_COLS: List[str] = ["seg_id", "anomaly_wave", "n_points", "n_anomaly_points", "anomaly_indices"]


def _load_gb_def(csv_path: str, seg_index_col: str, gb_col_name: str, encode_: str) -> Dict[int, Tuple[Optional[float], Optional[float]]]:
    """
    ガードバンド定義CSVを読込み、{seg_index: (下限, 上限)} の辞書を返す（標準csvモジュールのみ使用）。
    下限・上限が空欄（_gb_mt_learn_diag.py の offset_trimming で None 化された行）は None のまま保持する
    （None の位置は、判定をスキップする＝そのseg_indexでは異常判定しないことを意味する）。
    """
    import csv as _csv

    col_lw = f"{gb_col_name}_GB_Lw"
    col_up = f"{gb_col_name}_GB_Up"
    gb_bounds: Dict[int, Tuple[Optional[float], Optional[float]]] = {}

    with open(csv_path, "r", newline="", encoding=encode_) as f:
        reader = _csv.DictReader(f)
        if seg_index_col not in (reader.fieldnames or []):
            raise ValueError(f"gb_def_csv に列 {seg_index_col!r} がありません。列一覧={reader.fieldnames}")
        if col_lw not in (reader.fieldnames or []) or col_up not in (reader.fieldnames or []):
            raise ValueError(f"gb_def_csv に列 {col_lw!r}/{col_up!r} がありません。列一覧={reader.fieldnames}")

        for row in reader:
            try:
                seg_index = int(float(row[seg_index_col]))
            except (ValueError, TypeError):
                continue

            def _to_float(s: Any) -> Optional[float]:
                if s is None or str(s).strip() == "":
                    return None
                try:
                    return float(s)
                except ValueError:
                    return None

            gb_bounds[seg_index] = (_to_float(row[col_lw]), _to_float(row[col_up]))

    return gb_bounds


########################################
# Filename Token Helper
########################################
def _resolve_filename(
    tokens: List[str],
    counter: int,
    seg_start: Any,
    seg_end: Any,
    encode_: str,
    ext: str = ".csv",
) -> str:
    """
    filename_tokens に従ってファイル名を組み立てる。
    使える変数は "$999"（連番、999の次は001） / "$seg_start" / "$seg_end" / "$encode" のみ。
    それ以外の文字列はリテラルとして扱う。トークンは "_" で連結し、拡張子（ext）を付ける。
    """
    import re

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

    return "_".join(parts) + ext


########################################
# Act Trigger Functions
########################################

def a_gb_diag_trigger(
    post_queue: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    波形データを事前学習済みガードバンドと比較し、異常判定して1セグメント=1CSVファイルへ保存
    Mod: 2026.07.16    By: IWATA,Y.

    [動作の要点]（旧モジュール Docstring から移動）
        セグメント内の各行（位置 = seg_index）について、target_col_idx の値をガードバンドの上限・下限と比べ、1点でも外れれば異常波形とする
        判定結果 CSV（1セグメント = ヘッダ + 1行）: seg_id・異常波形フラグ・異常点数・異常点のインデックス一覧・使用モデル名
            ※ 異常点の実測値は JSON 出力にのみ含む
        save_normal_wave / save_anomaly_wave が有効な場合、判定結果に応じて波形の生データ（全行）も別に CSV で保存する（tri_s12_act_csv と同じ5行ヘッダー）
    """
    TRIGGER_NAME = "a_gb_diag_trigger"
    func_name = sys._getframe().f_code.co_name
    func_obj = globals().get(func_name)
    func_qmode = getattr(func_obj, "QMODE", "imq")

    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)
        save_dir = os.path.join(system_root, config.get("save_dir", "output"))
        os.makedirs(save_dir, exist_ok=True)
        encode_  = config.get("encode", "utf-8")
        file_pfx = config.get("file_pfx", "gbdiag")
        ack_ext  = config.get("ack_ext", "")
        filename_tokens = config.get("filename_tokens")  # 省略時は従来のファイル名形式のまま
        base.output_log(MODULE_NAME, f"filename_tokens={filename_tokens}", log_file)

        """データ項目定義"""
        n_col_def = len(col_def)#項目数
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_keys={col_keys}", log_file)

        """ モデル選択設定
            model_select_col_idx の値（品番・設備状態等）でモデル（ガードバンド定義）を切り替える。
            model_val_map で「値 → モデルキー」をn対1で対応付ける（複数の値が同じモデルを指してもよい）。
            全モデルは起動時に一括で検証・コンパイルしておき、実行中の都度ファイル読込みは行わない。
            該当モデルが無いセグメントは、default_model が指定されていればそれにフォールバックし、
            無ければ判定をスキップする（安全側に倒す。クラッシュさせない）。
        """
        model_select_col_idx = config.get("model_select_col_idx")
        if model_select_col_idx is None:
            raise ValueError("model_select_col_idx is required")
        model_select_col_idx = base.resolve_act_col(model_select_col_idx, col_def, "model_select_col_idx")  # Find と同じ列番号

        models_cfg = config.get("models")
        if not models_cfg:
            raise ValueError("models is required (at least 1 model)")

        model_val_map_cfg = config.get("model_val_map")
        if not model_val_map_cfg:
            raise ValueError("model_val_map is required (値 → モデルキー の対応)")

        # 値 → モデルキー の逆引き辞書を構築（n対1）。同じ値が複数モデルに重複していないか検証する。
        value_to_model: Dict[str, str] = {}
        for model_key, val_list in model_val_map_cfg.items():
            if str(model_key) not in models_cfg:
                raise ValueError(f"model_val_map のモデルキー {model_key!r} が models にありません: {list(models_cfg.keys())}")
            for raw_val in val_list:
                v = str(raw_val)
                if v in value_to_model and value_to_model[v] != str(model_key):
                    raise ValueError(
                        f"model_val_map の値 {v!r} が複数のモデル（{value_to_model[v]!r} と {model_key!r}）"
                        f"に重複して割り当てられています")
                value_to_model[v] = str(model_key)

        default_model = config.get("default_model")
        if default_model is not None and str(default_model) not in models_cfg:
            raise ValueError(f"default_model={default_model!r} not found in models: {list(models_cfg.keys())}")

        compiled_models: Dict[str, Dict[str, Any]] = {}
        for model_key, model_cfg in models_cfg.items():
            """判定対象列（モデルごとに指定可能）"""
            _target_col_idx = model_cfg.get("target_col_idx")
            if _target_col_idx is None:
                raise ValueError(f"models[{model_key!r}].target_col_idx is required")
            _target_col_idx = base.resolve_act_col(_target_col_idx, col_def, f"models[{model_key!r}].target_col_idx")  # Find と同じ列番号

            """ガードバンド定義CSVの読込み（標準csvモジュールのみ。pandas不使用）"""
            _gb_def_csv = model_cfg.get("gb_def_csv")
            if not _gb_def_csv:
                raise ValueError(f"models[{model_key!r}].gb_def_csv is required")
            _gb_def_csv = base.get_absolute_path(_gb_def_csv)

            _gb_col_name = model_cfg.get("gb_col_name")
            if not _gb_col_name:
                raise ValueError(f"models[{model_key!r}].gb_col_name is required")

            _gb_seg_index_col = model_cfg.get("gb_seg_index_col", "seg_index")

            _gb_bounds = _load_gb_def(_gb_def_csv, _gb_seg_index_col, _gb_col_name, encode_)

            compiled_models[str(model_key)] = {
                "target_col_idx": _target_col_idx,
                "gb_bounds": _gb_bounds,
            }
            base.output_log(MODULE_NAME,
                f"model={model_key!r}  target_col_idx={_target_col_idx}  gb_def_csv={_gb_def_csv}  "
                f"gb_col_name={_gb_col_name}  gb_seg_index_col={_gb_seg_index_col}  entries={len(_gb_bounds)}", log_file)

        base.output_log(MODULE_NAME,
            f"model_select_col_idx={model_select_col_idx}  models={list(compiled_models.keys())}  "
            f"model_val_map={model_val_map_cfg}  default_model={default_model}", log_file)

        """波形の生データ保存オプション（判定結果に応じて、正常/異常それぞれ独立してON/OFF可能）"""
        save_normal_wave  = bool(config.get("save_normal_wave",  False))
        save_anomaly_wave = bool(config.get("save_anomaly_wave", False))
        wave_file_pfx_normal  = config.get("normal_wave_file_pfx",  "gbdiag_wave_normal")
        wave_file_pfx_anomaly = config.get("anomaly_wave_file_pfx", "gbdiag_wave_anomaly")
        base.output_log(MODULE_NAME,
            f"save_normal_wave={save_normal_wave}  save_anomaly_wave={save_anomaly_wave}  "
            f"normal_wave_file_pfx={wave_file_pfx_normal}  anomaly_wave_file_pfx={wave_file_pfx_anomaly}", log_file)

        # 波形生データCSV用のヘッダー（save_csv.py と同じ5行構成: col_name/comment/dtype/num_cat/exp_obj）
        wave_file_head = [
            [col_def[s][0] for s in range(len(col_def.keys()))],  # col_name
            [col_def[s][1] for s in range(len(col_def.keys()))],  # data_type
            [col_def[s][2] for s in range(len(col_def.keys()))],  # num/cat
            [col_def[s][3] for s in range(len(col_def.keys()))],  # exp/obj
            [col_def[s][4] for s in range(len(col_def.keys()))],  # comment
        ]

        # デバイス情報を読込み
        device_key_to_add, device_info_to_add = utl.get_device_info_to_add(config)# tri_s19_util に集約（B-9）
        if device_key_to_add:
            wave_file_head[0] = wave_file_head[0] + device_key_to_add
            wave_file_head[1] = wave_file_head[1] + ['str'] * len(device_key_to_add)
            wave_file_head[2] = wave_file_head[2] + ['key'] * len(device_key_to_add)
            wave_file_head[3] = wave_file_head[3] + ['key'] * len(device_key_to_add)
            wave_file_head[4] = wave_file_head[4] + ['____'] * len(device_key_to_add)
        base.output_log(MODULE_NAME, f"device_key_to_add={device_key_to_add}  device_info_to_add={device_info_to_add}", log_file)
        

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
            if stop_event.is_set() and post_queue.qsize() == 0 and getattr(post_queue, "unfinished_tasks", 0) == 0: break
            continue
        
        if segment is None:
            post_queue.task_done()
            break
        
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
            
            """空セグメントチェック"""
            if len(segment) == 0:
                base.output_log(MODULE_NAME, f"WARNING  セグメントが空です。スキップします。id={seg_id}")
                continue
            
            """空レコードチェック"""
            print(segment[0])
            if (isinstance(segment[0][-1], list)) and (segment[0][-1][0] is None):
                base.output_log(MODULE_NAME, f"WARNING  レコードが空です。スキップします。id={seg_id}")
                continue

            """モデル選択（value_to_mapで値→モデルキーを引き、該当モデルを取得する）"""
            raw_val = str(segment[0][model_select_col_idx])
            model_key = value_to_model.get(raw_val)
            model = compiled_models.get(model_key) if model_key else None
            if model is None and default_model is not None:
                model_key = str(default_model)
                model = compiled_models.get(model_key)
            if model is None:
                base.output_log(MODULE_NAME,
                    f"WARNING  該当モデルが見つかりません。判定をスキップします。"
                    f"id={seg_id}  raw_val={raw_val!r}  models={list(compiled_models.keys())}", log_file)
                continue

            target_col_idx = model["target_col_idx"]
            gb_bounds       = model["gb_bounds"]

            """ガードバンド判定（標準ライブラリのみ。pandas不使用）
               seg_index はセグメント内での位置（0始点）として扱う。
               ガードバンド定義に該当する seg_index が無い、上限/下限が None の位置、
               target_col_idx の値が数値でない位置（設定ミス等）は判定をスキップする
               （_gb_mt_learn_diag.py の offset_trimming 等と同じ扱い）。"""
            anomaly_indices: List[int] = []
            n_type_error = 0
            for seg_index, rec in enumerate(segment):
                val = rec[target_col_idx]
                bounds = gb_bounds.get(seg_index)
                if bounds is None or val is None:
                    continue
                if not isinstance(val, (int, float)):
                    n_type_error += 1
                    continue
                lw, up = bounds
                if lw is not None and val < lw:
                    anomaly_indices.append(seg_index)
                elif up is not None and val > up:
                    anomaly_indices.append(seg_index)

            if n_type_error > 0:
                base.output_log(MODULE_NAME,
                    f"WARNING  target_col_idx={target_col_idx} の値が数値でない点が"
                    f"{n_type_error}件ありました（判定をスキップ）。target_col_idxの設定を"
                    f"見直してください。id={seg_id}  例: {segment[0][target_col_idx]!r}", log_file)

            anomaly_wave     = 1 if anomaly_indices else 0
            n_points         = len(segment)
            n_anomaly_points = len(anomaly_indices)

            """val_dict（判定の詳細情報。CSV保存時に val 列へJSON文字列として格納する）"""
            val_dict: Dict[str, Any] = {
                "model": model_key,
                "n_points": n_points,
                "n_anomaly_points": n_anomaly_points,
                "anomaly_indices": anomaly_indices,
                "anomaly_values": [segment[i][target_col_idx] for i in anomaly_indices],
            }
            if device_info_to_add:
                for k, v in zip(device_key_to_add, device_info_to_add):
                    val_dict[k] = v

            """CSVファイル保存（1セグメント=1ファイル）
               列: seg_id, anomaly, val（valはキーバリューのJSON文字列として1セルに格納）"""
            if filename_tokens:
                seg_start = segment[0][0]   # レコードのタイムスタンプ列（record[0]）
                seg_end   = segment[-1][0]
                filename = _resolve_filename(filename_tokens, file_counter, seg_start, seg_end, encode_, ext=".csv")
            else:
                filename = f"{file_pfx}{file_counter:03}_{seg_id}_{encode_}.csv"
            filepath = os.path.join(save_dir, filename)
            base.output_log(MODULE_NAME, f"{filename} 保存開始: seg_id={seg_id}")

            with open(filepath, "w", newline="", encoding=encode_) as f:
                writer = csv.writer(f)
                writer.writerow(["seg_id", "anomaly", "val"])
                writer.writerow([
                    seg_id,
                    anomaly_wave,
                    json.dumps(val_dict, ensure_ascii=False, default=str),
                ])

            """make Ack file"""
            if ack_ext:
                time.sleep(0.5)
                with open(f"{filepath}.done", "w") as f:
                    pass

            base.output_log(MODULE_NAME,
                f"{filename} 保存完了: id={seg_id}  n_points={n_points}  "
                f"anomaly_wave={anomaly_wave}  n_anomaly_points={n_anomaly_points}  "
                f"anomaly_indices={anomaly_indices}", log_file)

            """波形の生データ保存（判定結果に応じて、正常/異常それぞれ独立してON/OFF）"""
            _should_save_wave = (
                (anomaly_wave == 0 and save_normal_wave) or
                (anomaly_wave == 1 and save_anomaly_wave)
            )
            if _should_save_wave:
                wave_file_pfx = wave_file_pfx_anomaly if anomaly_wave == 1 else wave_file_pfx_normal
                #wave_filename = f"{wave_file_pfx}{file_counter:03}_{seg_id}_{encode_}.csv"
                wave_filename = f"{wave_file_pfx}{os.path.splitext(filename)[0]}.csv"
                wave_filepath = os.path.join(save_dir, wave_filename)

                with open(wave_filepath, "w", newline="", encoding=encode_) as f:
                    writer = csv.writer(f)
                    for i in [0, 4, 1, 2, 3]:
                        writer.writerow(wave_file_head[i])

                    for row_val in segment:
                        output_row = row_val
                        if device_info_to_add:
                            output_row = row_val + device_info_to_add
                        writer.writerow(output_row)

                if ack_ext:
                    time.sleep(0.5)
                    with open(f"{wave_filepath}.done", "w") as f:
                        pass

                base.output_log(MODULE_NAME,
                    f"{wave_filename} 波形保存完了: id={seg_id}  anomaly_wave={anomaly_wave}  "
                    f"n_points={n_points}", log_file)

            file_counter += 1
        
        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="io", trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break  # finally経由でtask_done()後にループ離脱
            continue # ループ継続
        
        finally:
            post_queue.task_done()
    """======== LOOP END ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None

a_gb_diag_trigger.QMODE = "imq"
a_gb_diag_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, f"Run Standalone.")
    base.output_log(MODULE_NAME, f"test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
