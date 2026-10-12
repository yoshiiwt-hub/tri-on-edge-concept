# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [SPC Diagnosis]
# Ver: 02.b.20261005    By: IWATA, Y.'s Agent AI
# Mod: 端末情報の抽出を tri_s19_util.get_device_info_to_add に集約（B-9）
########################################
a_spc_diag_trigger  # セグメント内の値列に対してSPCルール（連続トレンド・ゾーン逸脱等）を判定するAct Trigger

動作概要
    1. 起動時に models（複数モデルの定義）を全て検証・コンパイルする
       （モデルごとに target_col_idx・offset・center・sigma・spc_rules を事前に検証・コンパイルしておく）
    2. ループ内でキューからセグメントを受信し項目数の整合性を確認する
    3. セグメントの終点行（変化後）を除外してから判定する
    4. model_select_col_idx の値で使用するモデルを選択する
       （該当モデルが無ければ default_model にフォールバック。それも無ければ判定をスキップする）
    5. 選択したモデルの target_col_idx の値列（セグメント内、1点=1行）に対して、
       そのモデルのコンパイル済み全ルールを適用する
    6. いずれかのルールに1点でも違反があれば、そのセグメントを異常と判定する
    7. 1セグメントにつき、ヘッダ+1行のCSVファイルを1つ保存する
       （seg_id・異常判定・使用モデル名・ルールごとの違反点数・違反インデックス一覧・違反点の実測値一覧を含む）
       正常/異常で異なるファイル名プレフィックス（normal_file_pfx / anomaly_file_pfx）を使う
    8. device_key_to_add が指定されている場合は端末情報を行に付与して出力する
    9. ack_ext が指定されている場合は CSV 保存後に ACK ファイル（.done 等）を作成する
    10. stop_event が立ちキューが空になった時点でループを終了する

    ※ pandas/numpyは使用しない（TriEdgeのexeレイヤー軽量化方針に合わせ、標準ライブラリのみで実装）
    ※ center/sigma は本トリガーでは固定値のみ対応（学習して自動算出する機能は別途対応予定）

spc_rules フォーマット（リスト。各要素が1つのルール。tri_s19_calc_spc.py 参照）
    {"method": "trend", "n": 6, "direction": "up", "label": "trend_up6"}
        n           : 連続点数（2以上）
        direction   : "up"（上昇のみ）| "down"（下降のみ）| "both"（両方、デフォルト）
    {"method": "zone_out", "m": 3, "n_out": 2, "sigma_level": 2, "label": "zone2of3_2sigma"}
        m           : 判定に使うウィンドウの点数
        n_out       : mのうち、閾値超えとみなす最小点数（同じ側のみでカウント）
        sigma_level : 閾値＝center ± sigma_level*sigma
    "label" キー（省略可）: 結果の識別名。省略時は "{method}_{リスト内のindex}"

接続設定
    save_dir              : CSV 保存先ディレクトリ（system_root からの相対パス、デフォルト: "output"）
    encode                 : CSV ファイルの文字エンコーディング（デフォルト: "utf-8"）
    normal_file_pfx        : 正常判定時のCSVファイル名プレフィックス（デフォルト: "spcdiag_normal"）
    anomaly_file_pfx       : 異常判定時のCSVファイル名プレフィックス（デフォルト: "spcdiag_anomaly"）
    ack_ext                : ACK ファイル拡張子（空文字列の場合は作成しない、デフォルト: ""）
    device_key_to_add      : 端末情報から付与するキーのリスト（デフォルト: []）
    filename_tokens         : ファイル名を組み立てるトークンのリスト（省略可。tri_edge_12_act_save_csv.py 参照）

    model_select_col_idx   : モデル選択に使う列のインデックス（必須。品番・設備状態等が入る列）
    models                  : モデル定義の辞書（必須。1つ以上）
                               {"モデルキー（品番等の文字列）": {
                                   "target_col_idx": ...,
                                   "offset": [前方offset点数, 後方offset点数]（省略可、デフォルト[0,0]）,
                                   "center": ...,
                                   "sigma": ...,
                                   "spc_rules": [...]
                               }, ...}
    default_model           : model_select_col_idx の値がどのモデルにも一致しない場合に
                               フォールバックするモデルキー（省略可）。
                               省略時、該当モデルが無いセグメントは判定をスキップする（警告ログ出力）
"""
MODULE_NAME = "ACT_SPC_Diag"

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
from   src.diag.tri_s19_calc_spc    import compile_spc_rules, check_spc
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

def a_spc_diag_trigger(
    post_queue: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    セグメント内の値列に対してSPCルール（連続トレンド・ゾーン逸脱等）を判定し、1セグメント=1CSVファイルへ保存
    Mod: 2026.07.18    By: IWATA,Y.
    """
    TRIGGER_NAME = "a_spc_diag_trigger"
    func_name = sys._getframe().f_code.co_name
    func_obj = globals().get(func_name)
    func_qmode = getattr(func_obj, "QMODE", "imq")

    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)
        save_dir = os.path.join(system_root, config.get("save_dir", "output"))
        os.makedirs(save_dir, exist_ok=True)
        encode_  = config.get("encode", "utf-8")
        ack_ext  = config.get("ack_ext", "")
        normal_file_pfx  = config.get("normal_file_pfx",  "spcdiag_normal")
        anomaly_file_pfx = config.get("anomaly_file_pfx", "spcdiag_anomaly")
        filename_tokens = config.get("filename_tokens")  # 省略時は従来のファイル名形式のまま
        base.output_log(MODULE_NAME, f"filename_tokens={filename_tokens}", log_file)

        """データ項目定義"""
        n_col_def = len(col_def)#項目数
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_keys={col_keys}", log_file)

        """ モデル選択設定
            model_select_col_idx の値（品番・設備状態等）で判定対象列・中心線・σ・ルールを切り替える。
            全モデルは起動時に一括で検証・コンパイルしておき、実行中の都度再コンパイルは行わない。
            該当モデルが無いセグメントは、default_model が指定されていればそれにフォールバックし、
            無ければ判定をスキップする（安全側に倒す。クラッシュさせない）。
        """
        model_select_col_idx = config.get("model_select_col_idx")
        if model_select_col_idx is None:
            raise ValueError("model_select_col_idx is required")
        model_select_col_idx = int(model_select_col_idx)
        if not (0 <= model_select_col_idx < n_col_def):
            raise ValueError(f"model_select_col_idx={model_select_col_idx} out of range (0-{n_col_def-1})")

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
            _target_col_idx = int(_target_col_idx)
            if not (0 <= _target_col_idx < n_col_def):
                raise ValueError(f"models[{model_key!r}].target_col_idx={_target_col_idx} out of range (0-{n_col_def-1})")

            """セグメントのoffset（判定対象範囲。モデルごとに指定可能）"""
            _offset = model_cfg.get("offset", [0, 0])
            _offset_front, _offset_back = int(_offset[0]), int(_offset[1])
            if _offset_front < 0 or _offset_back < 0:
                raise ValueError(f"models[{model_key!r}].offset must be >= 0. Got: {_offset}")

            """中心線・σ（固定値。モデルごとに指定可能）"""
            _center = model_cfg.get("center")
            _sigma  = model_cfg.get("sigma")
            if _center is None:
                raise ValueError(f"models[{model_key!r}].center is required")
            if _sigma is None:
                raise ValueError(f"models[{model_key!r}].sigma is required")
            _center = float(_center)
            _sigma  = float(_sigma)
            if _sigma <= 0:
                raise ValueError(f"models[{model_key!r}].sigma must be > 0. Got: {_sigma}")

            """SPCルール定義のコンパイル（標準ライブラリのみ。pandas不使用）"""
            _spc_rules_cfg = model_cfg.get("spc_rules")
            if not _spc_rules_cfg:
                raise ValueError(f"models[{model_key!r}].spc_rules is required (at least 1 rule)")
            _compiled_rules = compile_spc_rules(_spc_rules_cfg)
            _rule_labels = [label for label, _ in _compiled_rules]

            compiled_models[str(model_key)] = {
                "target_col_idx": _target_col_idx,
                "offset_front": _offset_front,
                "offset_back":  _offset_back,
                "center": _center,
                "sigma":  _sigma,
                "compiled_rules": _compiled_rules,
                "rule_labels":    _rule_labels,
            }
            base.output_log(MODULE_NAME,
                f"model={model_key!r}  target_col_idx={_target_col_idx}  "
                f"offset=[{_offset_front},{_offset_back}]  center={_center}  sigma={_sigma}  "
                f"rules={_rule_labels}", log_file)

        base.output_log(MODULE_NAME,
            f"model_select_col_idx={model_select_col_idx}  models={list(compiled_models.keys())}  "
            f"model_val_map={model_val_map_cfg}  default_model={default_model}", log_file)

        # デバイス情報を読込み
        device_key_to_add, device_info_to_add = utl.get_device_info_to_add(config)# tri_s19_util に集約（B-9）
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

            """モデル選択（value_to_modelで値→モデルキーを引き、該当モデルを取得する）"""
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

            target_col_idx  = model["target_col_idx"]
            offset_front    = model["offset_front"]
            offset_back     = model["offset_back"]
            center          = model["center"]
            sigma           = model["sigma"]
            compiled_rules  = model["compiled_rules"]
            rule_labels     = model["rule_labels"]

            """SPCルール判定（標準ライブラリのみ。pandas不使用）"""
            values: List[Optional[float]] = [
                rec[target_col_idx] if isinstance(rec[target_col_idx], (int, float)) else None
                for rec in segment
            ]
            n_type_error = sum(1 for rec in segment if not isinstance(rec[target_col_idx], (int, float)))
            if n_type_error > 0:
                base.output_log(MODULE_NAME,
                    f"WARNING  target_col_idx={target_col_idx} の値が数値でない点が"
                    f"{n_type_error}件ありました（判定をスキップ）。target_col_idxの設定を"
                    f"見直してください。id={seg_id}  例: {segment[0][target_col_idx]!r}", log_file)

            """offset適用：前後offset_front/offset_back点を判定対象から除外する。
               None化することで、_rule_trend/_rule_zone_out側の「ウィンドウ内にNoneがあれば
               スキップ」というロジックが自然に働き、除外範囲にかかるウィンドウは無視される。
               （values自体のインデックスは変えないので、結果のインデックスは元のセグメント内位置のまま）"""
            if offset_front > 0:
                for i in range(min(offset_front, len(values))):
                    values[i] = None
            if offset_back > 0:
                for i in range(max(0, len(values) - offset_back), len(values)):
                    values[i] = None

            rule_results = check_spc(values, center, sigma, compiled_rules)
            n_points = len(segment)
            anomaly  = 1 if any(rule_results[label] for label in rule_labels) else 0

            if anomaly == 1:
                _violated = {label: idxs for label, idxs in rule_results.items() if idxs}
                base.output_log(MODULE_NAME,
                    f"!!! SPCルール違反を検出しました !!!  波形ID = {seg_id}  違反ルール = {_violated}", log_file)

            """val_dict（判定の詳細情報。CSV保存時に val 列へJSON文字列として格納する）"""
            val_dict: Dict[str, Any] = {
                "n_points": n_points,
                "model": model_key,
            }
            for label in rule_labels:
                idxs = rule_results[label]
                val_dict[f"{label}_n_violations"] = len(idxs)
                val_dict[f"{label}_indices"] = idxs
                val_dict[f"{label}_values"] = [values[i] for i in idxs]
            if device_info_to_add:
                for k, v in zip(device_key_to_add, device_info_to_add):
                    val_dict[k] = v

            """CSVファイル保存（1セグメント=1ファイル。正常/異常でプレフィックスを分ける）
               列: seg_id, anomaly, val（valはキーバリューのJSON文字列として1セルに格納）"""
            file_pfx = anomaly_file_pfx if anomaly == 1 else normal_file_pfx
            if filename_tokens:
                seg_start = segment[0][0]   # レコードのタイムスタンプ列（record[0]）
                seg_end   = segment[-1][0]
                filename = f"{file_pfx}_" + _resolve_filename(filename_tokens, file_counter, seg_start, seg_end, encode_, ext=".csv")
            else:
                filename = f"{file_pfx}{file_counter:03}_{seg_id}_{encode_}.csv"
            filepath = os.path.join(save_dir, filename)
            base.output_log(MODULE_NAME, f"{filename} 保存開始: seg_id={seg_id}")

            with open(filepath, "w", newline="", encoding=encode_) as f:
                writer = csv.writer(f)
                writer.writerow(["seg_id", "anomaly", "val"])
                writer.writerow([
                    seg_id,
                    anomaly,
                    json.dumps(val_dict, ensure_ascii=False, default=str),
                ])

            """make Ack file"""
            if ack_ext:
                time.sleep(0.5)
                with open(f"{filepath}.done", "w") as f:
                    pass

            base.output_log(MODULE_NAME,
                f"{filename} 保存完了: id={seg_id}  n_points={n_points}  anomaly={anomaly}", log_file)

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

a_spc_diag_trigger.QMODE = "imq"
a_spc_diag_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, f"Run Standalone.")
    base.output_log(MODULE_NAME, f"test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
