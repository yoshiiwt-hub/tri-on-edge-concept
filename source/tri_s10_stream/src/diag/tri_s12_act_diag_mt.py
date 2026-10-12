# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [MT-method Diagnosis]
# Ver: 02.b.20261005    By: IWATA, Y.'s Agent AI
# Mod: 統計まわりを tri_s10_stream_wave_stat の init_stats / calc_stats に集約（値が出せなかった項目を共通形式でログに出す。B-9）。device_key_to_add の値が常に空で端末情報が付与されていなかった不具合を修正（tri_s19_util に集約）
########################################
a_mt_diag_trigger  # セグメントの統計量を算出し、事前学習済み単位空間とのマハラノビス距離で異常判定するAct Trigger

動作概要
    1. 起動時に統計量算出定義（column_register_map / out_coldef、全モデル共通）を初期化する
       （計算エンジンは tri_edge_12_act_store_stats_sqlite 等と共通の tri_s10_stream_wave_stat を使用）
    2. 起動時に models（複数モデルの定義）を全て検証・コンパイルする
       （モデルごとの単位空間定義JSONを事前に読込んでおく。実行中の都度読込みはしない）
    3. ループ内でキューからセグメントを受信し項目数の整合性を確認する
    4. セグメントの終点行（変化後）を除外してから統計量を算出する
    5. model_select_col_idx の値で使用するモデルを選択する
       （該当モデルが無ければ default_model にフォールバック。それも無ければ判定をスキップする）
    6. 選択したモデルの mt_target_col_names で指定した統計量列を並べてベクトル化し、
       そのモデルの単位空間とのマハラノビス距離（m_dis）を計算する（標準ライブラリのみ）
    7. m_dis > border のとき異常と判定する（border未指定の場合は常に正常と判定する）
    8. 1セグメントにつき、ヘッダ+1行のCSVファイルを1つ保存する
       （seg_id・m_dis・border・異常判定・寄与度最大の変数とその値・使用モデル名に加え、
       全変数の元の値（{変数名}_val）・全変数のSN比（{変数名}_snr）を含む）
       正常/異常で異なるファイル名プレフィックス（normal_file_pfx / anomaly_file_pfx）を使う
    9. device_key_to_add が指定されている場合は端末情報を行に付与して出力する
    10. ack_ext が指定されている場合は CSV 保存後に ACK ファイル（.done 等）を作成する
    11. stop_event が立ちキューが空になった時点でループを終了する

    ※ pandas/numpyは使用しない（TriEdgeのexeレイヤー軽量化方針に合わせ、標準ライブラリのみで実装）。
       共分散・逆行列の計算（「学習」フェーズ）はスコープ外。診断（マハラノビス距離の計算）は
       単純な行列×ベクトルの掛け算のみのため、外部ライブラリ無しで実装可能。

単位空間定義JSON（unit_space_json）のフォーマット
    {
        "variables": ["D100_mean", "D100_std", ...],   ("省略可。mt_target_col_namesとの対応検証用)
        "mean"   : [0.12, 0.05, ...],                   (平均ベクトル、長さn)
        "inv_cov": [[1.1, -0.2, ...], ...],             (共分散行列の逆行列、n×n)
        "var"    : [0.02, 0.01, ...]                    (各変数の分散＝共分散行列の対角成分、長さn。寄与度計算に使用)
    }

接続設定
    save_dir             : CSV 保存先ディレクトリ（system_root からの相対パス、デフォルト: "output"）
    encode               : CSV ファイルの文字エンコーディング（デフォルト: "utf-8"）
    normal_file_pfx      : 正常判定時のCSVファイル名プレフィックス（デフォルト: "mtdiag_normal"）
    anomaly_file_pfx     : 異常判定時のCSVファイル名プレフィックス（デフォルト: "mtdiag_anomaly"）
    ack_ext              : ACK ファイル拡張子（空文字列の場合は作成しない、デフォルト: ""）
    device_key_to_add    : 端末情報から付与するキーのリスト（デフォルト: []）
    column_register_map  : 統計量の計算定義（必須。全モデル共通。tri_s10_stream_wave_stat 参照）
    out_coldef            : 出力列の定義（必須。全モデル共通。tri_s10_stream_wave_stat 参照）

    model_select_col_idx  : モデル選択に使う列のインデックス（必須。品番・設備状態等が入る列）
    models                 : モデル定義の辞書（必須。1つ以上）
                              {"モデルキー（品番等の文字列）": {
                                  "mt_target_col_names": [...],
                                  "unit_space_json": ...,
                                  "border": ...（省略可）,
                                  "mt_normalize": ...（省略可、デフォルトfalse）
                              }, ...}
    default_model          : model_select_col_idx の値がどのモデルにも一致しない場合に
                              フォールバックするモデルキー（省略可）。
                              省略時、該当モデルが無いセグメントは判定をスキップする（警告ログ出力）
                             ※ どちらの場合も border は m_dis と直接比較する値のため、
                                trueに切り替えた場合は border の値も見直しが必要
    filename_tokens       : ファイル名を組み立てるトークンのリスト（省略可。tri_edge_12_act_save_csv.py 参照）
"""
MODULE_NAME = "ACT_MT_Diag"

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
from   src.tri_s10_stream_wave_stat  import init_stats, calc_stats  # Calc Stats（Act 共通。B-9）
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
# Stats Helper（tri_edge_12_act_store_stats_sqlite.py と同じ考え方）
########################################
_FIXED_STATS_COLS: List[str] = ["seg_id", "seg_ts_start", "seg_ts_end", "seg_rows"]


def _build_stats_col_names(out_coldef: Dict) -> List[str]:
    """out_coldef から出力列名リストを index 昇順で返す"""
    return [str(out_coldef[idx][0]) for idx in sorted(out_coldef.keys(), key=int)]


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
# MT-method Helper（標準ライブラリのみ。学習フェーズ(共分散・逆行列の計算)は対象外、
# 診断（行列×ベクトルの計算）のみを実装）
########################################
import math as _math


def _load_unit_space(json_path: str) -> Dict[str, Any]:
    """
    単位空間定義JSON（mean, inv_cov, var, variables）を読込む
    Mod: 2026.08.15    By: IWATA,Y.（base.read_json_safely() へ統一。単一エンコード
         決め打ちの json.load() から変更。BOM有無・cp932等のエンコード差異を自動で
         フォールバックする。encode_ 引数は出力CSVのエンコード指定と兼用していたが、
         役割が異なるため分離した（出力側は従来通り encode_ を使用）。）
    """
    data, used_enc = base.read_json_safely(json_path)

    for key in ("mean", "inv_cov", "var"):
        if key not in data:
            raise ValueError(f"unit_space_json に必須キー {key!r} がありません")

    n = len(data["mean"])
    if len(data["inv_cov"]) != n or any(len(row) != n for row in data["inv_cov"]):
        raise ValueError(f"inv_cov のサイズが mean（長さ{n}）と一致しません")
    if len(data["var"]) != n:
        raise ValueError(f"var のサイズが mean（長さ{n}）と一致しません")

    return data


def _mat_vec_mul(mat: List[List[float]], vec: List[float]) -> List[float]:
    """行列×ベクトル（標準ライブラリのみ）"""
    return [sum(mat[i][j] * vec[j] for j in range(len(vec))) for i in range(len(mat))]


def _dot(a: List[float], b: List[float]) -> float:
    """ベクトルの内積"""
    return sum(x * y for x, y in zip(a, b))


def _calc_mahalanobis(
    x_vec: List[float],
    mean: List[float],
    inv_cov: List[List[float]],
    var: List[float],
    variables: Optional[List[str]] = None,
    normalize: bool = False,
) -> Tuple[float, Optional[str], Optional[float], List[Optional[float]]]:
    """
    マハラノビス距離と、寄与度（SN比。全変数分）を計算する
    Mod: 2026.07.17    By: IWATA,Y.（_gb_mt_learn_diag.py の mahalanobis_distance を移植。
         全変数分のSN比リストも返すよう変更）
    Args:
        normalize : True の場合、変数数k（=len(x_vec)）で正規化した「教科書的なMT距離」
                    （m_dis = sqrt((1/k) * (x-mean)^T @ inv_cov @ (x-mean)))）を計算する。
                    False（デフォルト）の場合は、正規化なしの単純なマハラノビス距離
                    （元の _gb_mt_learn_diag.py と同じ式）のまま。
    Returns:
        m_dis      : マハラノビス距離（normalizeにより式が変わる）
        sn_max_col : 寄与度（SN比）が最大の変数名（variables未指定時はインデックスの文字列）
        sn_max_val : その寄与度（SN比）の値
        snr_list   : 全変数分のSN比リスト（x_vec/mean/varと同じ並び順。計算不可の変数はNone）
    """
    diff = [x - m for x, m in zip(x_vec, mean)]
    quad = _dot(diff, _mat_vec_mul(inv_cov, diff))
    if normalize:
        k = len(x_vec)
        quad = quad / k if k > 0 else quad
    m_dis = _math.sqrt(quad) if quad > 0 else 0.0

    sn_max_col: Optional[str] = None
    sn_max_val: Optional[float] = None
    snr_list: List[Optional[float]] = []
    for i, (d, v) in enumerate(zip(diff, var)):
        if v is None or v == 0:
            snr_list.append(None)
            continue
        snr = 10.0 * _math.log10((d ** 2) / v) if (d ** 2) > 0 else float("-inf")
        snr_list.append(snr)
        if sn_max_val is None or snr > sn_max_val:
            sn_max_val = snr
            sn_max_col = variables[i] if variables and i < len(variables) else str(i)

    return m_dis, sn_max_col, sn_max_val, snr_list


########################################
# Act Trigger Functions
########################################

def a_mt_diag_trigger(
    post_queue: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    セグメントの統計量を算出し、事前学習済み単位空間とのマハラノビス距離で異常判定
    Mod: 2026.07.16    By: IWATA,Y.
    """
    TRIGGER_NAME = "a_mt_diag_trigger"
    func_name = sys._getframe().f_code.co_name
    func_obj = globals().get(func_name)
    func_qmode = getattr(func_obj, "QMODE", "imq")

    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)
        save_dir = os.path.join(system_root, config.get("save_dir", "output"))
        os.makedirs(save_dir, exist_ok=True)
        encode_  = config.get("encode", "utf-8")
        normal_file_pfx  = config.get("normal_file_pfx",  "mtdiag_normal")
        anomaly_file_pfx = config.get("anomaly_file_pfx", "mtdiag_anomaly")
        ack_ext  = config.get("ack_ext", "")
        filename_tokens = config.get("filename_tokens")  # 省略時は従来のファイル名形式のまま
        base.output_log(MODULE_NAME, f"filename_tokens={filename_tokens}", log_file)

        """データ項目定義"""
        n_col_def = len(col_def)#項目数
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_keys={col_keys}", log_file)

        """統計量算出定義（store_stats_sqliteと同じ column_register_map / out_coldef）"""
        stats_ctx    = init_stats(config, col_def, MODULE_NAME, TRIGGER_NAME)

        stats_col_names = stats_ctx.base_col_names
        base.output_log(MODULE_NAME, f"stats_col_names({len(stats_col_names)})={stats_col_names}", log_file)

        """ モデル選択設定
            model_select_col_idx の値（品番・設備状態等）で単位空間・閾値を切り替える。
            統計量の計算定義（column_register_map/out_coldef）は全モデル共通。
            全モデルは起動時に一括で検証・コンパイルしておき、実行中の都度ファイル読込みは行わない。
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
            _mt_target_col_names = model_cfg.get("mt_target_col_names")
            if not _mt_target_col_names:
                raise ValueError(f"models[{model_key!r}].mt_target_col_names is required")
            _missing = [c for c in _mt_target_col_names if c not in stats_col_names]
            if _missing:
                raise ValueError(
                    f"models[{model_key!r}].mt_target_col_names に out_coldef 由来の統計量列名として"
                    f"存在しないものがあります: {_missing}  （stats_col_names={stats_col_names}）")

            _unit_space_json = model_cfg.get("unit_space_json")
            if not _unit_space_json:
                raise ValueError(f"models[{model_key!r}].unit_space_json is required")
            _unit_space_json = base.get_absolute_path(_unit_space_json)

            _unit_space  = _load_unit_space(_unit_space_json)
            _mean_vec    = _unit_space["mean"]
            _inv_cov_mat = _unit_space["inv_cov"]
            _var_vec     = _unit_space["var"]
            _variables   = _unit_space.get("variables")

            if len(_mean_vec) != len(_mt_target_col_names):
                raise ValueError(
                    f"models[{model_key!r}].mt_target_col_names の長さ（{len(_mt_target_col_names)}）が "
                    f"unit_space_json の mean の長さ（{len(_mean_vec)}）と一致しません")

            _border = model_cfg.get("border")
            _border = float(_border) if _border is not None else None
            if _border is None:
                base.output_log(MODULE_NAME,
                    f"WARNING  models[{model_key!r}].border が未指定です。閾値不明のため、"
                    f"常に正常（anomaly=0）として扱います。", log_file)

            _mt_normalize = bool(model_cfg.get("mt_normalize", False))

            compiled_models[str(model_key)] = {
                "mt_target_col_names": _mt_target_col_names,
                "mean_vec":    _mean_vec,
                "inv_cov_mat": _inv_cov_mat,
                "var_vec":     _var_vec,
                "variables":   _variables,
                "border":      _border,
                "mt_normalize": _mt_normalize,
            }
            base.output_log(MODULE_NAME,
                f"model={model_key!r}  mt_target_col_names={_mt_target_col_names}  "
                f"unit_space_json={_unit_space_json}  n_vars={len(_mean_vec)}  "
                f"border={_border}  mt_normalize={_mt_normalize}", log_file)

        base.output_log(MODULE_NAME,
            f"model_select_col_idx={model_select_col_idx}  models={list(compiled_models.keys())}  "
            f"model_val_map={model_val_map_cfg}  default_model={default_model}", log_file)

        # デバイス情報を読込み
        device_key_to_add, device_info_to_add = utl.get_device_info_to_add(config)# 従来は値が常に空で付与されていなかった（B-9 で修正）
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

            mt_target_col_names = model["mt_target_col_names"]
            mean_vec     = model["mean_vec"]
            inv_cov_mat  = model["inv_cov_mat"]
            var_vec      = model["var_vec"]
            variables    = model["variables"]
            border       = model["border"]
            mt_normalize = model["mt_normalize"]

            """統計量算出 → MT法（マハラノビス距離）判定"""
            stats_dict = calc_stats(stats_ctx, segment, seg_id)

            x_vec = [stats_dict.get(c) for c in mt_target_col_names]
            _missing_val = [c for c, v in zip(mt_target_col_names, x_vec) if v is None]
            if _missing_val:
                base.output_log(MODULE_NAME,
                    f"WARNING  mt_target_col_names の一部が計算できませんでした（None）。"
                    f"スキップします。id={seg_id}  missing={_missing_val}", log_file)
                continue

            m_dis, sn_max_col, sn_max_val, snr_list = _calc_mahalanobis(
                x_vec, mean_vec, inv_cov_mat, var_vec, variables, normalize=mt_normalize)
            anomaly = (1 if m_dis > border else 0) if border is not None else 0

            """val_dict（判定の詳細情報。CSV保存時に val 列へJSON文字列として格納する）"""
            val_dict: Dict[str, Any] = {
                "model": model_key,
                "m_dis": m_dis,
                "border": border,
                "sn_max_col": sn_max_col,
                "sn_max_val": sn_max_val,
            }
            for v, x, snr in zip(mt_target_col_names, x_vec, snr_list):
                val_dict[f"{v}_val"] = x
                val_dict[f"{v}_snr"] = snr
            if device_info_to_add:
                for k, v in zip(device_key_to_add, device_info_to_add):
                    val_dict[k] = v

            """CSVファイル保存（1セグメント=1ファイル。正常/異常でプレフィックスを分ける）
               列: seg_id, anomaly, val（valはキーバリューのJSON文字列として1セルに格納）"""
            file_pfx = anomaly_file_pfx if anomaly == 1 else normal_file_pfx
            if filename_tokens:
                seg_start = stats_dict.get("seg_ts_start")
                seg_end   = stats_dict.get("seg_ts_end")
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
                f"{filename} 保存完了: id={seg_id}  m_dis={m_dis:.4f}  anomaly={anomaly}  "
                f"sn_max_col={sn_max_col}  sn_max_val={sn_max_val}", log_file)
            
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

a_mt_diag_trigger.QMODE = "imq"
a_mt_diag_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, f"Run Standalone.")
    base.output_log(MODULE_NAME, f"test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
