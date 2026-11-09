# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Supervise Base Module
# Ver: 03.l.20261108    By: IWATA, Y.'s Agent AI
# Mod: pyflakes の指摘を整理（使っていない import・変数、置き換えの無い f-string を削除。動作は変えない）
########################################
全モジュールが共通で使う、パス・設定・ログ・状態の置き場となる基盤

主要関数:
_set_root_dir                              # システムルートと、配下のフォルダ・ファイルのパスを決める（import 時に実行）。
output_log                                 # タイムスタンプとモジュール名を付けてログを出力する。
load_trigger_setting_01 / 02               # トリガー設定ファイルを読み込む（"##" のコメントは除く）。
set_cast_dtype / cast_dtype                # col_def に従って値を型変換する。
resolve_act_col / resolve_act_cols         # Act の設定に書いた列番号を、Act に渡る行の中の位置に変換する。
metrics_* / notify_*                       # 運用系メトリクスと通知要求を、トリガーから Operator へ受け渡す。

位置づけ: 他のすべてのモジュールが最初に import する。パスの決定と設定の読込みをここに集め、各モジュールが個別に解決しないようにする。
設計意図: システムルートは実行ファイル（.exe）またはソースの配置場所から決め、起動したフォルダに左右されないようにする。メトリクス・通知要求・モードの受け渡しはこのモジュールを介し、トリガーと Operator が互いを import しないようにする。
前提・制約: import した時点で、パスの決定と .env の読込みが行われる。オフライン・デバッグのモードは Operator だけが更新する。Storage Queue（stq）は tri_s01_storage_queue が受け持つ。
"""
MODULE_NAME = "BASE"

"""standard library"""
from   datetime import datetime as dt
from   collections import deque
import json
import logging
from   logging.handlers import RotatingFileHandler
import os
import sys
import threading
import time
from   typing   import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

"""external library"""
## NOT Use

"""local module"""
# Base is the Lowest Layer. NOT Import Any Local Modules as to Prevent Dependency Problem.


########################################
# Define System Dir, Files
########################################
_SYSTEM_ROOT = None
_SETTING_DIR = None
_LOG_DIR = None
_WORK_DIR = None
_WORK_DB_DIR = None
_WORK_RUN_DIR = None
_WORK_SPOOL_DIR = None
_DEVICE_INFO_PATH = None
_SETTING_OPERATOR_PATH = None
_SETTING_TRIGGER_01_PATH = None
_SETTING_TRIGGER_02_PATH = None
_STREAM_HEARTBEAT_PATH = None
_CTRL_STOP_STREAM_PATH = None
_CTRL_STATE_OFFLINE_PATH = None
_CTRL_STATE_DEBUG_PATH = None
_CURRENT_LOG_PATH = None
_LOGGERS: Dict[str, logging.Logger] = {}

def _set_root_dir() -> None:
    """Initialize System Directories & Paths"""
    global _SYSTEM_ROOT
    global _SETTING_DIR
    global _LOG_DIR
    global _WORK_DIR
    global _WORK_DB_DIR
    global _WORK_RUN_DIR
    global _WORK_SPOOL_DIR
    global _DEVICE_INFO_PATH
    global _SETTING_OPERATOR_PATH
    global _SETTING_TRIGGER_01_PATH
    global _SETTING_TRIGGER_02_PATH
    global _STREAM_HEARTBEAT_PATH
    global _CTRL_STOP_STREAM_PATH
    global _CTRL_STATE_OFFLINE_PATH
    global _CTRL_STATE_DEBUG_PATH
    global _CURRENT_LOG_PATH
    
    try:
        # 1. Define Root Dir
        is_frozen_exe = bool(getattr(sys, 'frozen', False))
        if is_frozen_exe: # .exe (pyinstaller)
            sys_dir = os.path.dirname(sys.executable)
        else:             # .py
            src_dir = os.path.dirname(os.path.abspath(__file__))
            sys_dir = os.path.dirname(src_dir)
        
        _SYSTEM_ROOT = sys_dir
        
        # 2. Define Sub Directories
        _SETTING_DIR = os.path.join(_SYSTEM_ROOT, "settings")
        _LOG_DIR     = os.path.join(_SYSTEM_ROOT, "log")
        _WORK_DIR    = os.path.join(_SYSTEM_ROOT, "work")
        
        _WORK_DB_DIR    = os.path.join(_WORK_DIR, "db")
        _WORK_RUN_DIR   = os.path.join(_WORK_DIR, "run")
        _WORK_SPOOL_DIR = os.path.join(_WORK_DIR, "spool")

        # 3. Create Directories if not exists
        for d in [
            _SETTING_DIR, _LOG_DIR, _WORK_DIR,
            _WORK_DB_DIR, _WORK_RUN_DIR, _WORK_SPOOL_DIR,
        ]:
            os.makedirs(d, exist_ok=True)
        
        # 4. Define Log File Path
        # tri_{プロセス起動日時 YYYYMMDDHHMMSS}_{pid}.log（名前順＝起動順。PID の再利用で別の日のログに追記しない。E-7）
        pid = os.getpid()
        _CURRENT_LOG_PATH = os.path.join(_LOG_DIR, f"tri_{dt.now().strftime('%Y%m%d%H%M%S')}_{pid}.log")
        
        # 5. Define Config File Paths
        _DEVICE_INFO_PATH        = os.path.join(_SETTING_DIR, "device_info.json")
        _SETTING_OPERATOR_PATH   = os.path.join(_SETTING_DIR, "01_operator.json")
        _SETTING_TRIGGER_01_PATH = os.path.join(_SETTING_DIR, "trigger_setting_01.json")
        _SETTING_TRIGGER_02_PATH = os.path.join(_SETTING_DIR, "trigger_setting_02.json")
        
        # 6. Define System Control File Paths
        _STREAM_HEARTBEAT_PATH = os.path.join(_WORK_RUN_DIR, "_heartbeat_stream_core.triedge")
        _CTRL_STOP_STREAM_PATH = os.path.join(_WORK_RUN_DIR, "_oneshot_stop_stream.triedge_control")
        _CTRL_STATE_OFFLINE_PATH = os.path.join(_WORK_RUN_DIR, "_mode_offline.triedge_control")
        _CTRL_STATE_DEBUG_PATH = os.path.join(_WORK_RUN_DIR, "_mode_debug.triedge_control")

    except Exception as e:
        msg = f"[BASE] _set_root_dir FATAL ERROR: {e}"
        _show_msg(msg)

        # 緊急退避
        target_log = "base_set_root_dir.log"
        try:
            with open(target_log, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception: pass
        sys.exit(1)
    return None


def _get_current_timestamp_str() -> str:
    return dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _show_msg(msg):
    """Show Message BEFORE Defining Logging Func"""
    ts = _get_current_timestamp_str()
    sys.stdout.write(f"{ts}  {msg}\n")
    sys.stdout.flush()
    return None


def _get_logger(log_file: str = "") -> logging.Logger:
    """Get or Create Logger (Singleton)"""
    if not log_file:
        log_file = "CONSOLE_ONLY"

    if log_file in _LOGGERS:
        return _LOGGERS[log_file]

    logger = logging.getLogger(log_file)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.hasHandlers():
        formatter = logging.Formatter("%(message)s")

        # 1. Console Handler
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        logger.addHandler(sh)

        # 2. File Handler
        if log_file != "CONSOLE_ONLY":
            try:
                os.makedirs(os.path.dirname(log_file), exist_ok=True)
                fh = RotatingFileHandler(
                    log_file, 
                    maxBytes=1024*1024*1, # 1MB
                    backupCount=5,        # 5 generations
                    encoding='utf-8'
                )
                fh.setFormatter(formatter)
                logger.addHandler(fh)

            except Exception as e:
                _show_msg(f"[UTIL] Logger Init Error: {e}")

    _LOGGERS[log_file] = logger
    return logger


def _init_log_file(path_: str) -> str:
    """Initialize Logger"""
    full_path = os.path.abspath(path_)
    _get_logger(full_path)
    return full_path


def output_log(tag: str, msg: str, log_file: str = "") -> None:
    """
    Output Log (Console and/or File)
    ログ出力に失敗しても処理は継続する（標準出力と緊急退避先へ書けるだけ書く。従来は sys.exit(1) でプロセス終了）
    Mod: 2026.10.05    By: IWATA, Y.'s Agent AI（A-10）
    """
    try:
        timestamp = _get_current_timestamp_str()
        tag_str   = str('['+tag+']').ljust(7)
        log_msg   = f"{timestamp}  {tag_str}  {msg}"
        
        logger = _get_logger(log_file)
        logger.info(log_msg)

    except Exception as e:
        err_msg = f"[BASE] output_log FATAL ERROR: {e}"
        _show_msg(err_msg)

        # 緊急退避
        if not(log_file):
            log_file = _CURRENT_LOG_PATH
        
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass
        # ログの失敗で処理を止めない（ディスク満杯等でもキュー掃き出し・ハートビート削除・ErrorIF 処理を優先する。A-10）
    return None


def cleanup_old_logs(keep_days: float) -> Tuple[int, int]:
    """
    ログディレクトリ内の古いログファイルを削除する（E-7）
    - 対象: ログディレクトリ直下の tri_*.log*（現行名 tri_{YYYYMMDDHHMMSS}_{pid}.log と旧名 tri_edge_{pid}.log、
            およびそれぞれのローテーション .1〜.5）
    - 条件: 更新日時が keep_days 日より前（ファイル名の日時ではなく更新日時で判定。長時間稼働中のログを消さないため）
    - 除外: 現在のプロセスが書き込んでいるログ（とそのローテーション）
    - keep_days <= 0 の場合は何もしない
    Mod: 2026.10.05    By: IWATA, Y.'s Agent AI
    Returns: (削除した件数, 削除に失敗した件数)
    """
    if not keep_days or keep_days <= 0 or not _LOG_DIR or not os.path.isdir(_LOG_DIR):
        return 0, 0
    border_time = time.time() - float(keep_days) * 24 * 60 * 60
    current_name = os.path.basename(_CURRENT_LOG_PATH or "")
    n_deleted, n_failed = 0, 0
    for name in sorted(os.listdir(_LOG_DIR)):
        if not (name.startswith("tri_") and ".log" in name):
            continue
        if current_name and name.startswith(current_name):
            continue
        path = os.path.join(_LOG_DIR, name)
        try:
            if os.path.isfile(path) and os.path.getmtime(path) < border_time:
                os.remove(path)
                n_deleted += 1
        except Exception as e:
            n_failed += 1
            _show_msg(f"[BASE] cleanup_old_logs: Can NOT Remove {path}  ({e})")
    return n_deleted, n_failed


########################################
# System Dir, Files Accessors
########################################

def get_system_root() -> str:
    """Accessor for System Root"""
    if _SYSTEM_ROOT is None: _set_root_dir()
    return _SYSTEM_ROOT


def get_setting_dir() -> str:
    """Accessor for Setting Directory"""
    if _SETTING_DIR is None: _set_root_dir()
    return _SETTING_DIR


def get_log_dir() -> str:
    """Accessor for Log Directory"""
    if _LOG_DIR is None: _set_root_dir()
    return _LOG_DIR


def get_log_path() -> str:
    """Accessor for Log Path"""
    if _CURRENT_LOG_PATH is None: _set_root_dir()
    return _CURRENT_LOG_PATH


def get_work_dir() -> str:
    """Accessor for Work Directory (Root)"""
    if _WORK_DIR is None: _set_root_dir()
    return _WORK_DIR


def get_work_sub_dirs() -> dict:
    """Get All Work Sub-Dir. 'db' 'run' 'spool' """
    if _WORK_DIR is None: _set_root_dir()
    return {
        "db"   : _WORK_DB_DIR,
        "run"  : _WORK_RUN_DIR,
        "spool": _WORK_SPOOL_DIR
    }


def get_stream_hb_path() -> str:
    """Accessor for Heartbeat of StreamCore"""
    if _STREAM_HEARTBEAT_PATH is None: _set_root_dir()
    return _STREAM_HEARTBEAT_PATH


def get_ctrl_stop_stream_path() -> str:
    """Accessor for Control File to Stop StreamCore Gently"""
    if _CTRL_STOP_STREAM_PATH is None: _set_root_dir()
    return _CTRL_STOP_STREAM_PATH


def get_ctrl_state_offline_path() -> str:
    """Accessor for Control File to Change State to Offline"""
    if _CTRL_STATE_OFFLINE_PATH is None: _set_root_dir()
    return _CTRL_STATE_OFFLINE_PATH


def get_ctrl_state_debug_path() -> str:
    """Accessor for Control File to Change State to Debug"""
    if _CTRL_STATE_DEBUG_PATH is None: _set_root_dir()
    return _CTRL_STATE_DEBUG_PATH


########################################
# Setting File Loader
########################################
_device_info = None
_operator_setting = None
_trigger_setting_01 = None
_trigger_setting_02 = None

def get_absolute_path(path_str: str) -> str:
    """Resolve path with System Root. Return abspath as is. Return relpath join with System Root"""
    if not path_str: return ""
    
    # 1. ユーザーディレクトリ(~)の展開
    target = os.path.expanduser(path_str)
    
    # 2. すでに絶対パスなら、正規化して返す
    if os.path.isabs(target):
        return os.path.normpath(target)
    
    # 3. 相対パスなら、System Root と結合する
    if _SYSTEM_ROOT is None: _set_root_dir()
    return os.path.normpath(os.path.join(_SYSTEM_ROOT, target))


_JSON_ERR_LINE_MAX = 120  # JSON 書式誤りの該当行を表示する最大文字数（超える場合は誤りの位置の前後を切り出す）


def read_json_safely(filepath):
    """
    check encode, new line and read json
    1. 文字コードの判定: utf-8-sig → cp932 → euc-jp の順に、テキストとして読めるかだけを試す
    2. JSON の解析: 読めた文字コードで解析する。書式誤りは別の文字コードで読み直さず、行・列・該当行を付けて報告する
       （従来は書式誤りでも次の文字コードで読み直し、最後は "Encode NOT Match" となって行・列が失われていた）
    Mod: 2026.10.07    By: IWATA, Y.'s Agent AI
    Returns: (内容, 使用した文字コード)
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Setting File Not Found: {filepath}")
    
    encodings = ['utf-8-sig', 'cp932', 'euc-jp']
    text, used_enc, error = None, None, None
    for enc in encodings:
        try:
            with open(filepath, mode='r', encoding=enc) as f:
                text = f.read()
            used_enc = enc
            break
        except UnicodeDecodeError as e:
            error = e
    if used_enc is None:
        raise ValueError(f"Encode NOT Match in {encodings}: {filepath}") from error

    try:
        return json.loads(text), used_enc
    except json.JSONDecodeError as e:
        lines = text.splitlines()
        line_ = lines[e.lineno - 1] if 0 < e.lineno <= len(lines) else ""
        if len(line_) > _JSON_ERR_LINE_MAX:
            start = max(0, e.colno - 1 - _JSON_ERR_LINE_MAX // 2)
            end   = start + _JSON_ERR_LINE_MAX
            line_ = ("..." if start > 0 else "") + line_[start:end] + ("..." if end < len(line_) else "")
        raise ValueError(
            f"Invalid JSON format: {e.msg} (line {e.lineno}, column {e.colno})  file={filepath}  encoding={used_enc}\n"
            f"    line {e.lineno}: {line_.strip()}") from e


def clean_dict_comment(d):
    """Recursively remove keys starting with '##' or '_comment'."""
    if isinstance(d, dict):
        return {
            k: clean_dict_comment(v) 
            for k, v in d.items() 
            if not (str(k).startswith("_comment") or str(k).startswith("##"))
        }
    elif isinstance(d, list):
        return [clean_dict_comment(item) for item in d]
    else:
        return d


def _load_json_setting(path: str, label: str) -> dict:
    """JSON設定ファイルを読み込んでコメントを除去した辞書を返す。失敗時は sys.exit(1)。"""
    try:
        raw_data, used_enc = read_json_safely(path)
        if used_enc != 'utf-8-sig':
            output_log(MODULE_NAME, f"WARNING: {label} is {used_enc}. Use UTF-8N.", _CURRENT_LOG_PATH)
        return clean_dict_comment(raw_data)
    except Exception as e:
        output_log(MODULE_NAME, f"FATAL: Failed to load {label}. {e}", _CURRENT_LOG_PATH)
        sys.exit(1)
    return None


def load_device_info() -> dict:
    """Load Device Info"""
    global _device_info
    if _DEVICE_INFO_PATH is None: _set_root_dir()
    if _device_info is None:
        _device_info = _load_json_setting(_DEVICE_INFO_PATH, "Device Info")
    return _device_info


def load_operator_setting() -> dict:
    """Load Operator Setting"""
    global _operator_setting
    if _SETTING_OPERATOR_PATH is None: _set_root_dir()
    if _operator_setting is None:
        _operator_setting = _load_json_setting(_SETTING_OPERATOR_PATH, "Operator Setting")
    return _operator_setting


def load_trigger_setting_01() -> dict:
    """Load Trigger Setting 01"""
    global _trigger_setting_01
    if _SETTING_TRIGGER_01_PATH is None: _set_root_dir()
    if _trigger_setting_01 is None:
        _trigger_setting_01 = _load_json_setting(_SETTING_TRIGGER_01_PATH, "Trigger Setting 01")
    return _trigger_setting_01


def load_trigger_setting_02() -> dict:
    """Load Trigger Setting 02"""
    global _trigger_setting_02
    if _SETTING_TRIGGER_02_PATH is None: _set_root_dir()
    if _trigger_setting_02 is None:
        _trigger_setting_02 = _load_json_setting(_SETTING_TRIGGER_02_PATH, "Trigger Setting 02")
    return _trigger_setting_02


########################################
# System State Management
########################################
_is_offline = False
_is_debug = False

def _set_mode_is_offline(val: bool) -> None:
    """オフライン状態を更新（operator専用）"""
    global _is_offline
    _is_offline = val
    return None


def get_mode_is_offline() -> bool:
    """オフライン状態を返す（全モジュール向け）"""
    return _is_offline


def _set_mode_is_debug(val: bool) -> None:
    """デバッグ状態を更新（operator専用）"""
    global _is_debug
    _is_debug = val
    return None


def get_mode_is_debug() -> bool:
    """デバッグ状態を返す（全モジュール向け）"""
    return _is_debug


########################################
# Cross-Module Metrics Registry
########################################
# 各トリガー（find/act）が運用系の累計値（処理件数・エラー回数等）を積算し、
# Operatorが定期的に読み出してTeams等へ報告する。
# ※ "metrics" ＝ 運用監視用の値。波形の統計量（"stats"、tri_s10_stream_wave_stat等）とは別概念。
# ・プロセス生涯累計（再起動でリセットされる）。永続化はしない。
# ・複数トリガーが別スレッドから同時に書き込むため、Lockで保護する。
_metrics_lock = threading.Lock()
_metrics: Dict[str, Dict[str, Any]] = {}


def metrics_add(module_name: str, key: str, delta: Union[int, float] = 1) -> None:
    """
    運用系メトリクスを加算する（トリガー側から呼ぶ）
    Mod: 2026.07.08    By: IWATA,Y.
    Args:
        module_name (str): 呼び出し元のMODULE_NAME（例: "FIND_SPO"）
        key         (str): メトリクス項目名（例: "total_items"）
        delta   (int|float): 加算量。デフォルト1
    """
    with _metrics_lock:
        mod_metrics = _metrics.setdefault(module_name, {})
        mod_metrics[key] = mod_metrics.get(key, 0) + delta
    return None


def metrics_set_last(module_name: str, key: str, value: Any) -> None:
    """
    運用系メトリクスを上書きする（「直近エラー内容」等、加算ではなく最新値を残したい場合に使う）
    Mod: 2026.07.08    By: IWATA,Y.
    """
    with _metrics_lock:
        mod_metrics = _metrics.setdefault(module_name, {})
        mod_metrics[key] = value
    return None


def metrics_get_all() -> Dict[str, Dict[str, Any]]:
    """Accessor for Cross-Module Metrics（全モジュール分の運用系メトリクスのスナップショットを返す。コピーを返すためロック外で安全に扱える）"""
    with _metrics_lock:
        return {mod: dict(vals) for mod, vals in _metrics.items()}


########################################
# On-Demand Notify Request Queue
########################################
# トリガー・StreamCore が「今の metrics を通知してほしい」という要求を積み、
# Operator（t_ondemand_notify_worker）が取り出して Teams 等へ送る。
# ・送る内容は、Operator が送信時点で読む metrics のスナップショット（要求には値を持たせない）
# ・上限付き（_NOTIFY_MAXLEN 件）。あふれた古い要求は捨て、件数を数えて取り出し時に返す
# ・要求が入ったら Event を立て、Operator がすぐ気づけるようにする
_NOTIFY_MAXLEN  = 100
_notify_lock    = threading.Lock()
_notify_queue: "deque[Dict[str, Any]]" = deque(maxlen=_NOTIFY_MAXLEN)
_notify_dropped = 0
_notify_event   = threading.Event()


def metrics_notify(module_name: str, message: str = "") -> None:
    """
    運用系メトリクスのオンデマンド通知を要求する（トリガー・StreamCore側から呼ぶ）
    Mod: 2026.10.01    By: IWATA, Y.'s Agent AI
    Args:
        module_name (str): 通知したい metrics のモジュール名（metrics_add 等で使った MODULE_NAME）
        message     (str): 通知に添える文（省略可）
    """
    global _notify_dropped
    with _notify_lock:
        if len(_notify_queue) >= _NOTIFY_MAXLEN:
            _notify_dropped += 1
        _notify_queue.append({"module": module_name, "message": message, "time": time.time()})
        _notify_event.set()
    return None


def notify_pop_all() -> Tuple[List[Dict[str, Any]], int]:
    """
    Accessor for On-Demand Notify（積まれた要求をすべて取り出す。Operator用）
    Returns:
        (要求のリスト（古い順）, 前回取り出し以降にあふれて捨てた件数)
    """
    global _notify_dropped
    with _notify_lock:
        reqs = list(_notify_queue)
        _notify_queue.clear()
        dropped, _notify_dropped = _notify_dropped, 0
        _notify_event.clear()
    return reqs, dropped


def notify_wait(timeout: Optional[float] = None) -> bool:
    """Accessor for On-Demand Notify（要求が入るまで最大 timeout 秒待つ。要求があれば True）"""
    return _notify_event.wait(timeout)


########################################
# System Initialization
########################################
# 1. パスとディレクトリの初期化
_set_root_dir()

# 2. ロガーの初期化
_init_log_file(_CURRENT_LOG_PATH)

# 3. 起動ログ出力
output_log(MODULE_NAME, f"TriEdge Base Module Initialized. PID={os.getpid()}", _CURRENT_LOG_PATH)
output_log(MODULE_NAME, f"System Root         {_SYSTEM_ROOT}", _CURRENT_LOG_PATH)
output_log(MODULE_NAME, f"Log Dir             {_LOG_DIR}", _CURRENT_LOG_PATH)
output_log(MODULE_NAME, f"Log File            {_CURRENT_LOG_PATH}", _CURRENT_LOG_PATH)
output_log(MODULE_NAME, f"Setting Dir         {_SETTING_DIR}", _CURRENT_LOG_PATH)
output_log(MODULE_NAME, f"Device Info         {_DEVICE_INFO_PATH}", _CURRENT_LOG_PATH)
output_log(MODULE_NAME, f"Operator Setting    {_SETTING_OPERATOR_PATH}", _CURRENT_LOG_PATH)
output_log(MODULE_NAME, f"Trigger Setting 01  {_SETTING_TRIGGER_01_PATH}", _CURRENT_LOG_PATH)
output_log(MODULE_NAME, f"Trigger Setting 02  {_SETTING_TRIGGER_02_PATH}", _CURRENT_LOG_PATH)


def load_env_if_exists() -> list:
    """Load .env file into os.environ safely"""
    if _SYSTEM_ROOT is None:
        _set_root_dir()

    ENV_PATH = os.path.join(_SYSTEM_ROOT, "_[CONFIDENTIAL]", ".env")
    if not os.path.isfile(ENV_PATH):
        return None
    
    encodings = ['utf-8-sig', 'cp932', 'euc-jp']
    keys = []
    for enc in encodings:
        try:
            with open(ENV_PATH, 'r', encoding=enc) as f:
                for raw in f:
                    line = raw.strip()
                    to_skip = (
                        not line  or
                        line.startswith('#')  or 
                        line.startswith('REM')  or 
                        '=' not in line
                    )
                    if to_skip: continue
                    
                    key, value = line.split('=', 1)
                    key = key.strip()
                    if not key: continue
                    
                    value = value.strip()
                    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                        value = value[1:-1]
                        
                    os.environ[key] = value
                    keys.append(key)
                return keys
                    
        except Exception:
            continue
    return keys

_ret = load_env_if_exists()
if _ret:
    output_log(MODULE_NAME, f"Env File Detected, Loaded to os.environ {_ret}", _CURRENT_LOG_PATH)


########################################
# Segment Column Functions
########################################

"""
Act に渡る行の並び: [seg_id, seg_ind, seg_ts, seg_status] + Find の列（col_def の並び）
Act の設定で列を指定するときは、Find と同じ列番号（0 始まり）で書き、先頭の固定4列は名前で書く。
行の中の位置への変換（Find の列番号 + 4）は resolve_act_col が行う（各 Act は起動時に1回変換する）
"""
SEG_HEADER_COLS: Tuple[str, ...] = ("seg_id", "seg_ind", "seg_ts", "seg_status")


def resolve_act_col(value: Any, col_def: Union[Dict[int, Any], Sequence[Any]], key: str = "") -> int:
    """
    Act の設定に書いた列の指定を、Act に渡る行の中の位置（0 始まり）に変換する（起動時に呼ぶ）
      整数、または数字の文字列（例 5 / "5"） : Find の列番号とみなし、+ len(SEG_HEADER_COLS) した位置を返す
      "seg_id" / "seg_ind" / "seg_ts" / "seg_status" : 先頭の固定4列の位置（0～3）を返す
    col_def: Act に渡される col_def（固定4列 + Find の列）、またはその列名のリスト。Find の列数は len(col_def) - 4 で求める
    key    : エラーメッセージに出す設定キー名
    範囲外・true/false・小数・それ以外の文字列は ValueError（起動時に止める）
    Mod: 2026.10.21    By: IWATA, Y.'s Agent AI
    """
    n_head = len(SEG_HEADER_COLS)
    n_find = len(col_def) - n_head
    _key = f"config[{key!r}]" if key else "column"

    if isinstance(value, str):
        v = value.strip()
        if v in SEG_HEADER_COLS:
            return SEG_HEADER_COLS.index(v)
        if v.isdigit():
            value = int(v)
        else:
            raise ValueError(
                f"{_key}={value!r} is invalid. Use Find column index (0-{n_find - 1}) "
                f"or one of {list(SEG_HEADER_COLS)}")

    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(
            f"{_key}={value!r} is invalid. Use Find column index (0-{n_find - 1}) "
            f"or one of {list(SEG_HEADER_COLS)}")
    if not (0 <= value < n_find):
        raise ValueError(f"{_key}={value} (Find column index) out of range (0-{n_find - 1})")
    return value + n_head


def resolve_act_cols(values: Any, col_def: Union[Dict[int, Any], Sequence[Any]], key: str = "") -> List[int]:
    """
    resolve_act_col のリスト版。単一の値（リストでない）は1要素のリストとして扱う
    Mod: 2026.10.21    By: IWATA, Y.'s Agent AI
    """
    if values is None:
        return []
    if not isinstance(values, (list, tuple)):
        values = [values]
    return [resolve_act_col(v, col_def, key) for v in values]


def set_cast_dtype(
    col_def: Dict[int, Any]
    , config: Dict[str, Any]
    ) -> Dict[int, Tuple[Callable, Any]]:
    """
    col_defから、列ごとの型変換マップを生成
    既定値（time_default / int_default / float_default / str_default）は、値が無い・変換できない場合に入る値。
    null（None）を指定すると欠損を None のまま渡す（Act の統計で既定値が有効値として集計されないようにする場合に使う）
    Mod: 2026.10.06    By: IWATA, Y.'s Agent AI（time_default の null 指定に対応。従来は strptime(None) で起動時エラー）
    """
    
    # load config
    time_format = config.get("time_format", "%Y-%m-%d %H:%M:%S.%f")
    _time_default = config.get("time_default", "1970-01-01 00:00:00.000000")
    time_default  = dt.strptime(_time_default, "%Y-%m-%d %H:%M:%S.%f") if _time_default is not None else None
    int_default   = config.get("int_default", -999)
    float_default = config.get("float_default", -999.9)
    str_default   = config.get("str_default", "____")
    
    # mapping
    type_map = {
        "time" : lambda v: dt.strptime(v, time_format) if isinstance(v, str) else v,
        "int"  : int,
        "float": float,
        "str"  : str,
    }
    default_map = {
        "time" : time_default ,
        "int"  : int_default  ,
        "float": float_default,
        "str"  : str_default  ,
    }
    cast_plan = {}
    for i in sorted(col_def.keys()):
        target_type = col_def[i][1]
        cast_plan[i] = (
            type_map.get(target_type, str),
            default_map.get(target_type)
        )
    
    return cast_plan


def cast_dtype(
    values: list,
    cast_plan: Dict[int, Tuple[Callable, Any]],
    ) -> Tuple[list, int]:
    """
    cast_planに従い、リストの各要素を型変換
    Mod: 2026.04.17    By: IWATA,Y.
    """
    casted_values = []
    fail_count = 0
    for i, val in enumerate(values):
        cast_func, default_val = cast_plan[i]
        result = default_val
        try:
            if val is not None and val != "":
                result = cast_func(val)
                
        except (ValueError, TypeError):
            fail_count += 1
            
        casted_values.append(result)
    return casted_values, fail_count


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    print("\nload_device_info")
    for k, v in load_device_info().items(): print(f"['{k}']: {v}")
    print("\nos.environ")
    if _ret: print(_ret)
    print(os.environ)
    print("\nload_operator_setting")
    for k, v in load_operator_setting().items(): print(f"['{k}']: {v}")
    print("\nload_trigger_setting_01")
    for k, v in load_trigger_setting_01().items(): print(f"['{k}']: {v}")
    print("\nload_trigger_setting_02")
    for k, v in load_trigger_setting_02().items(): print(f"['{k}']: {v}")
