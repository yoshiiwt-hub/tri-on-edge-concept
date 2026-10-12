# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Operator Module
# Ver: 04.m.20261005    By: IWATA, Y.'s Agent AI
# Mod: 古いログの定期削除（log_retention.keep_days。E-7）、監視ディレクトリの未検出・読取エラー表示（E-1）、IP 取得スタブと Teams の (IP) 欄を削除（C-2）
########################################

動作概要
    1. 01_operator.json の設定に従いシステム全体を制御するオペレータモジュール
    2. stream_core（tri_s10_stream_core）を子スレッドで起動・監視
    3. CPU / ディスク / メモリ使用率を定期取得してシステムリソースを監視
    4. 停止ファイル・ハートビートファイルを監視し、異常時はストリームを再起動
    5. Webhook（Teams 等）へのアラート通知機能を提供
    ※ log_retention.keep_days（日）を設定すると、起動時と1日1回、更新日時が古いログファイルを削除する（未設定・0 は削除しない）
       （定期通知: t_state_monitor_worker、オンデマンド通知: t_ondemand_notify_worker。宛先は別）
    6. スタンドアロン実行（__main__）時はオペレータループを直接起動
"""
MODULE_NAME = "OPERATOR"

"""standard library"""
import ctypes
from   datetime import datetime as dt
import json
import os
from   pathlib import Path
import platform
import re              # Teams統計情報のdiff_keys（"*"のみをワイルドカードとして扱うパターン一致）用
import shutil         # ディスク容量
import ssl             # Webhook（自己署名証明書環境向け、検証スキップオプション用）
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
import urllib.error   # Webhook
import urllib.parse   # Webhook OAuth token取得
import urllib.request # Webhook

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
import src.tri_s10_stream_core as stream
import src.tri_s01_error_if       as err

"""system dir, file"""
system_root = base.get_system_root()

log_dir  = base.get_log_dir()
log_file = base.get_log_path()
work_dir = base.get_work_dir()
work_db_dir    = base.get_work_sub_dirs().get("db"   , os.path.join(work_dir, "db"))
work_run_dir   = base.get_work_sub_dirs().get("run"  , os.path.join(work_dir, "run"))
work_spool_dir = base.get_work_sub_dirs().get("spool", os.path.join(work_dir, "spool"))
base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__=='__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)

"""device info"""
device_info = base.load_device_info()
base.output_log(MODULE_NAME, f"device_info={device_info}", log_file)

"""setting file"""
setting = base.load_operator_setting()
base.output_log(MODULE_NAME, f"setting={setting}", log_file)


########################################
# Operator Functions
########################################


# ======== CPU ========
_prev_cpu = None

def _read_cpu_times() -> dict:
    try:
        _os = platform.system()
        if _os == "Linux":
            with open("/proc/stat") as f:
                # user, nice, system, idle, iowait, irq, softirq
                v = list(map(int, f.readline().split()[1:8]))
            return {"idle": v[3], "total": sum(v)}
    
        elif _os == "Windows":
            class _FT(ctypes.Structure):
                _fields_ = [("lo", ctypes.c_ulong), ("hi", ctypes.c_ulong)]
            idle, kernel, user = _FT(), _FT(), _FT()
            ctypes.windll.kernel32.GetSystemTimes(
                ctypes.byref(idle),
                ctypes.byref(kernel),
                ctypes.byref(user)
            )   
            # kernel には idle が含まれる
            idle_t  = (idle.hi   << 32) | idle.lo
            total_t = ((kernel.hi << 32) | kernel.lo) + ((user.hi << 32) | user.lo)
            
            return {"idle": idle_t, "total": total_t}
    except Exception as e:
        return {"msg": str(e), "idle": 0, "total": 0}
    
    return {"msg": f"Unsupported OS: {_os}", "idle": 0, "total": 0}


def get_cpu_percent() -> str:
    global _prev_cpu
    
    curr = _read_cpu_times()
    if not curr or not _prev_cpu:
        _prev_cpu = curr
        return "-"   # 初回は差分なし
    
    d_idle  = curr["idle"]  - _prev_cpu["idle"]
    d_total = curr["total"] - _prev_cpu["total"]
    _prev_cpu = curr
    if d_total == 0:
        return f"-- ({curr.get('msg', 'no delta')})"
    
    return f"{round((1 - d_idle / d_total) * 100, 1)}%"


# ======== MEMORY ========
def get_mem_info() -> str:
    _os = platform.system()
    try:
        if _os == "Linux":
            info = {}
            with open("/proc/meminfo") as f:
                for line in f:
                    k, v = line.split(":")
                    info[k.strip()] = int(v.strip().split()[0]) * 1024
            total = info.get("MemTotal", 0)
            avail = info.get("MemAvailable", 0)
            used  = total - avail
            pct   = round(used / total * 100, 1) if total else 0.0

        elif _os == "Windows":
            class _MEM(ctypes.Structure):
                _fields_ = [("dwLength",     ctypes.c_ulong),
                             ("dwMemoryLoad", ctypes.c_ulong),
                             ("ullTotalPhys", ctypes.c_ulonglong),
                             ("ullAvailPhys", ctypes.c_ulonglong),
                             ("ullTotalPageFile", ctypes.c_ulonglong),
                             ("ullAvailPageFile", ctypes.c_ulonglong),
                             ("ullTotalVirtual",  ctypes.c_ulonglong),
                             ("ullAvailVirtual",  ctypes.c_ulonglong),
                             ("ullAvailExtVirtual",ctypes.c_ulonglong)]
            s = _MEM(dwLength=ctypes.sizeof(_MEM))
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(s))
            total = s.ullTotalPhys
            used  = total - s.ullAvailPhys
            pct   = round(used / total * 100, 1) if total else 0.0

        else:
            return "-"

        total_gb = round(total / (1024**3), 1)
        used_gb  = round(used  / (1024**3), 1)
        return f"{used_gb}GB / {total_gb}GB ({pct}%)"

    except Exception:
        return "-"


def get_dir_size_and_count(path: str, errors: "Optional[List[str]]" = None) -> "Tuple[int, int]":
    """
    指定ディレクトリ以下の合計サイズ（バイト）とファイル数を、まとめて再帰的に取得する。
    Mod: 2026.10.05    By: IWATA, Y.'s Agent AI（errors を渡すと、読み取れなかった箇所の理由を追記する。E-1）
    （旧 get_dir_size() はサイズのみ返していたが、ディレクトリごとのファイル数も
      Teams等へ報告したいとの要望があったため、1回の走査でまとめて集計するよう統合した）
    """
    total_size  = 0
    total_count = 0
    try:
        with os.scandir(path) as it:  # os.scandir は os.walk より高速でメモリ効率が良い
            for entry in it:
                if entry.is_file():
                    total_size  += entry.stat().st_size
                    total_count += 1
                elif entry.is_dir():
                    _s, _c = get_dir_size_and_count(entry.path, errors)
                    total_size  += _s
                    total_count += _c

    except Exception as e:
        # 権限エラー等は計測可能な範囲で返す（errors を渡された場合は理由を残す）
        if errors is not None:
            errors.append(f"{path}: {type(e).__name__}: {e}")

    return total_size, total_count


def get_dir_size(path: str) -> int:
    """指定ディレクトリ以下の合計サイズを再帰的に取得（後方互換のための薄いラッパー）"""
    size, _count = get_dir_size_and_count(path)
    return size


_DEFAULT_WATCH_DIRS = [
    {"name": "Log Dir",  "path": "log"},
    {"name": "Work Dir", "path": "work"},
]


def _resolve_watch_dir_path(path_: str) -> str:
    """
    watch_dirs の path を絶対パスへ変換する。
    "log"/"work" は base._LOG_DIR/_WORK_DIR（既存の(Log Dir)/(Work Dir)相当）に対応させ、
    それ以外は system_root からの相対パスとして解決する（base.get_absolute_path があれば使う。
    無ければ os.path.join でフォールバックする）。
    Mod: 2026.08.15    By: IWATA,Y.
    """
    if path_ == "log":
        return base._LOG_DIR
    if path_ == "work":
        return base._WORK_DIR
    if os.path.isabs(path_):
        return path_
    try:
        return base.get_absolute_path(path_)
    except AttributeError:
        return os.path.join(system_root, path_)


def get_system_metrics(watch_dirs: "Optional[List[Dict[str, str]]]" = None) -> dict:
    """
    システムリソース使用状況を計測して返す（副作用なし。registryへの登録は呼び出し側の責務）
    Mod: 2026.07.08    By: IWATA,Y.
    Mod: 2026.08.15    By: IWATA,Y.  watch_dirs（監視対象ディレクトリのリスト）を引数化し、
         各ディレクトリの「サイズ + ファイル数」を計測して metrics["dirs"] に格納するようにした。
         省略時は _DEFAULT_WATCH_DIRS（Log Dir / Work Dir）を使い、従来の (Log Dir)/(Work Dir)
         表示と後方互換にしている。
    Mod: 2026.10.05    By: IWATA, Y.'s Agent AI  存在しないディレクトリは "not found"、読み取れない箇所があれば
         "(read error N)" を付けて表示し理由をログに残す（従来はどちらも 0 MB / 0 files 等と区別がつかなかった。E-1）
    """
    if watch_dirs is None:
        watch_dirs = _DEFAULT_WATCH_DIRS

    metrics = {}
    try:
        # 1. CPU
        cpu_pct = get_cpu_percent()
        metrics["cpu"] = f"{cpu_pct}"
        
        # 2. Memory
        mem = get_mem_info()
        metrics["mem"] = mem
        
        # 3. Disk (Drive Info)
        total, used, free = shutil.disk_usage(system_root)
        disk_total_gb = round(total / (1024**3), 1)
        disk_used_gb  = round(used  / (1024**3), 1)
        disk_pct      = round((used / total) * 100, 1)
        metrics["disk"] = f"{disk_used_gb}GB / {disk_total_gb}GB ({disk_pct}%)"

        # 4. 監視対象ディレクトリ（サイズ + ファイル数）
        dirs_result: Dict[str, str] = {}
        for entry in watch_dirs:
            _name = entry.get("name", entry.get("path", "?"))
            _path = _resolve_watch_dir_path(entry.get("path", ""))
            if not os.path.isdir(_path):
                # 存在しないディレクトリを 0 MB / 0 files と誤って報告しない（E-1）
                dirs_result[_name] = "not found"
                continue
            _errors: List[str] = []
            _size, _count = get_dir_size_and_count(_path, _errors)
            dirs_result[_name] = f"{round(_size / (1024**2), 1)} MB / {_count} files"
            if _errors:
                dirs_result[_name] += f"  (read error {len(_errors)})"
                base.output_log(MODULE_NAME, f"Metrics Dir Read Error: {_name}  {_errors[:3]}", log_file)
        metrics["dirs"] = dirs_result
    
    except Exception as e:
        base.output_log(MODULE_NAME, f"Metrics Error: {e}")
        return {"cpu": "-", "mem": "-", "disk": "-", "dirs": {}}
    
    return metrics


########################################
# Webhook OAuth (Entra ID Client Credentials)
########################################
# IT部門からの案内: incoming webhook廃止に伴い、Power Automateの
# Microsoft Teams Webhookコネクタへ切替。あわせて発行されたクライアントID・
# シークレットを使い、Entra IDのクライアントクレデンシャルズフローで
# アクセストークンを取得し、Webhook呼び出し時にBearerトークンとして付与する。
#
# 秘匿情報（tenant_id, client_id, client_secret）は環境変数で管理する
# （WEBHOOK_URLと同じ方針。設定ファイルには書かない）。
#
# トークン有効期限は通常1時間。interval_sec=30等の高頻度送信でも
# 毎回取り直さないよう、有効期限内はキャッシュを使い回す。
# 定期通知とオンデマンド通知で別の認証情報を使えるよう、キャッシュは (tenant_id, client_id) ごとに持つ。
_webhook_token_lock  = threading.Lock()
_webhook_token_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}

# 自己署名証明書環境（社内プロキシによるSSLインスペクション等）向けの、検証スキップ用コンテキスト。
# 01_operator.json の teams_notification.skip_ssl_verify が true の場合のみ使用する
# （tri_s19_ms_spo.py の session.verify = False と同じ位置づけの、環境対応オプション）。
_INSECURE_SSL_CONTEXT = ssl.create_default_context()
_INSECURE_SSL_CONTEXT.check_hostname = False
_INSECURE_SSL_CONTEXT.verify_mode = ssl.CERT_NONE


def _build_webhook_opener(proxy_url: str = "", skip_ssl_verify: bool = False) -> "urllib.request.OpenerDirector":
    """Webhook呼び出し用のurllib openerを組み立てる（プロキシ・SSL検証スキップの両対応）"""
    handlers = []
    if proxy_url:
        handlers.append(urllib.request.ProxyHandler({'http': proxy_url, 'https': proxy_url}))
    if skip_ssl_verify:
        handlers.append(urllib.request.HTTPSHandler(context=_INSECURE_SSL_CONTEXT))
    return urllib.request.build_opener(*handlers)


def get_webhook_token(tenant_id: str, client_id: str, client_secret: str, proxy_url: str = "",
                       skip_ssl_verify: bool = False) -> str:
    """
    Entra IDからWebhook呼び出し用アクセストークンを取得する（キャッシュ有効期限内は再取得しない）
    Mod: 2026.10.01    By: IWATA, Y.'s Agent AI（キャッシュを (tenant_id, client_id) ごとに分けた）
    Args:
        tenant_id, client_id, client_secret: Entra IDアプリ登録の情報（環境変数から渡す想定）
        proxy_url: 社内プロキシ（省略可）
        skip_ssl_verify: True の場合、SSL証明書検証をスキップする（社内プロキシのSSLインスペクションで
                          自己署名証明書エラーになる環境向けの一時的な回避策。既定は False＝通常通り検証する）
    Returns:
        str: アクセストークン（取得失敗時は空文字）
    """
    with _webhook_token_lock:
        cache = _webhook_token_cache.get((tenant_id, client_id), {})
        if cache.get("access_token") and time.time() < cache.get("expires_at", 0.0):
            return cache["access_token"]

        if not (tenant_id and client_id and client_secret):
            return ""

        try:
            token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            body = urllib.parse.urlencode({
                "grant_type"   : "client_credentials",
                "client_id"    : client_id,
                "client_secret": client_secret,
                "scope"        : "https://service.flow.microsoft.com//.default",  # スラッシュ2連続が正（ドキュメント通り）
            }).encode("utf-8")

            opener = _build_webhook_opener(proxy_url, skip_ssl_verify)

            req = urllib.request.Request(
                token_url, data=body,
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            with opener.open(req, timeout=10) as res:
                token_res = json.loads(res.read().decode("utf-8"))

            access_token = token_res.get("access_token", "")
            expires_in   = float(token_res.get("expires_in", 3600))
            if access_token:
                _webhook_token_cache[(tenant_id, client_id)] = {
                    "access_token": access_token,
                    "expires_at"  : time.time() + expires_in - 60,  # 60秒の安全マージン
                }
            return access_token

        except Exception as e:
            base.output_log(MODULE_NAME, f"Webhook Token Error: {e}")
            return ""


def send_teams_card(webhook_url: str, proxy_url: str, card_data: dict,
                     webhook_token: str = "", skip_ssl_verify: bool = False) -> bool:
    """Teams Webhook にMessageCardを送信する（Requestsライブラリを使わず、urllibのみでProxy対応）
    webhook_token指定時は Authorization: Bearer ヘッダーを付与する（Entra ID認証必須の環境向け）
    skip_ssl_verify: True の場合、SSL証明書検証をスキップする（get_webhook_tokenと同じ位置づけ）
    Mod: 2026.10.01    By: IWATA, Y.'s Agent AI（複数スレッドから送るため、install_opener（プロセス全体の設定）を
         やめて opener.open() で送るよう変更。送信できたかを bool で返す）
    """
    if not webhook_url:
        return False

    try:
        opener = _build_webhook_opener(proxy_url, skip_ssl_verify)
        json_data = json.dumps(card_data).encode("utf-8")  # BOM無し（BOM付きだとAPI Management等のゲートウェイでリクエストが拒否される場合がある）
        headers = {"Content-Type": "application/json"}
        if webhook_token:
            headers["Authorization"] = f"Bearer {webhook_token}"
        req = urllib.request.Request(
            webhook_url, 
            data=json_data, 
            headers=headers
        )
        with opener.open(req, timeout=10) as res: pass
    
    except Exception as e:
        base.output_log(MODULE_NAME, f"Teams Send Error: {e}")
        return False
    return True


def _match_key_patterns(patterns: Iterable[str], key: str) -> bool:
    """
    キーがパターンのいずれかに一致するかを判定する（diff_keys / notify_keys 共用）。
    パターン中の "*" だけをワイルドカード（任意文字列）として扱い、
    それ以外の文字（"["/"]" 等、メトリクスキーで実際に使われる文字も含む）はリテラル一致とする。
    （標準の fnmatch は "[" "]" を文字クラスとして特殊解釈してしまい、
      "items[site/list]" のような実際のキー命名と相性が悪いため、あえて自前実装にしている）
    Mod: 2026.10.01    By: IWATA, Y.'s Agent AI（t_state_monitor_worker 内の _is_diff_key から切り出し）
    """
    for pat in patterns:
        regex = "^" + "(?:.*)".join(re.escape(p) for p in pat.split("*")) + "$"
        if re.match(regex, key):
            return True
    return False


def _join_fact_sections(*sections: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """
    セクション（factリスト）同士を、空行代わりのスペーサー行を挟んで結合する。
    通常の空文字factは高さ0で潰れてしまい見た目に空行として現れなかったため、
    半角スペースを1つだけ入れたfactをスペーサーとして使っている。
    空のセクション（device_info_keys未設定時のDevice Info等）はスペーサーごと省略する。
    Mod: 2026.10.01    By: IWATA, Y.'s Agent AI（t_state_monitor_worker 内から切り出し。動作は変更なし）
    """
    _spacer = {"name": " ", "value": " "}
    non_empty = [s for s in sections if s]
    result: List[Dict[str, str]] = []
    for i, sec in enumerate(non_empty):
        if i > 0:
            result.append(_spacer)
        result.extend(sec)
    return result


def _device_info_facts(device_info_keys: Sequence[str]) -> List[Dict[str, str]]:
    """"Device Info" セクションの facts を作る（device_info_keys が空ならセクションごと省略＝空リスト）"""
    facts: List[Dict[str, str]] = []
    if device_info_keys:
        facts.append({"name": "Device Info", "value": ""})
        for _k in device_info_keys:
            facts.append({"name": f"- {_k}", "value": str(device_info.get(_k, "-"))})
    return facts


def make_payload(title: str, text: str, color: str = "0076D7",
                 facts: Optional[List[Dict[str, str]]] = None,
                 links: Optional[Dict[str, str]] = None) -> dict:
    """
    Teams 用の MessageCard を作る
    Mod: 2026.10.01    By: IWATA, Y.'s Agent AI（t_state_monitor_worker 内から切り出し、links を引数にした。動作は変更なし）
    """
    links = links or {}
    return {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": color,
        "summary": title,
        "sections": [{
            "activityTitle": title,
            "activitySubtitle": f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}",
            "text": text,
            "facts": facts or [],
            "markdown": True
        }],
        "potentialAction": [
            {
                "@type": "OpenUri",
                "name": "管理画面",
                "targets": [{"os": "default", "uri": links.get("control_panel", "#")}]
            },
            {
                "@type": "OpenUri",
                "name": "リリースノート",
                "targets": [{"os": "default", "uri": links.get("release_note", "#")}]
            }
        ]
    }


def t_metrics_collector_worker(stop_event: threading.Event) -> None:
    """
    システムリソース（CPU/Mem/Disk）を定期計測し、metricsレジストリへ登録するスレッド
    Mod: 2026.07.08    By: IWATA,Y.
    ※ Teamsへの送信（t_state_monitor_worker）とは、タイミング・処理（関数）ともに完全に分離する。
      このスレッドは「計測して登録する」だけで、Teams送信や表示整形には一切関与しない。
    """
    THREAD_NAME = "MetricsCollector"
    base.output_log(MODULE_NAME, f"{THREAD_NAME} Started.")

    try:
        metrics_conf  = setting.get("metrics_collector", {})
        interval_sec  = float(metrics_conf.get("interval_sec", 60))  # デフォルト1分（Teams送信の1時間とは独立）
        watch_dirs    = metrics_conf.get("watch_dirs", _DEFAULT_WATCH_DIRS)
        base.output_log(MODULE_NAME, f"{THREAD_NAME} interval_sec={interval_sec}  watch_dirs={watch_dirs}")

        # 古いログの削除（log_retention.keep_days 日。未設定・0 は削除しない。E-7）
        log_keep_days = float((setting.get("log_retention", {}) or {}).get("keep_days", 0) or 0)
        base.output_log(MODULE_NAME, f"{THREAD_NAME} log_retention keep_days={log_keep_days}  (0=disabled)", log_file)
    except Exception as e:
        base.output_log(MODULE_NAME, f"{THREAD_NAME} Init Error: {e}")
        return

    LOG_CLEANUP_INTERVAL_SEC = 24 * 60 * 60  # 1日1回（起動直後に1回目を実行）
    last_collect = 0.0
    last_log_cleanup = 0.0
    while not stop_event.is_set():
        if log_keep_days > 0 and time.time() - last_log_cleanup >= LOG_CLEANUP_INTERVAL_SEC:
            try:
                n_deleted, n_failed = base.cleanup_old_logs(log_keep_days)
                if n_deleted or n_failed:
                    base.output_log(MODULE_NAME,
                        f"{THREAD_NAME} Old Logs Cleaned. deleted={n_deleted}  failed={n_failed}  keep_days={log_keep_days}", log_file)
            except Exception as e:
                base.output_log(MODULE_NAME, f"{THREAD_NAME} Log Cleanup Error: {e}", log_file)
            last_log_cleanup = time.time()

        if time.time() - last_collect >= interval_sec:
            try:
                metrics = get_system_metrics(watch_dirs)
                for k, v in metrics.items():
                    base.metrics_set_last("SYSTEM", k, v)
            except Exception as e:
                base.output_log(MODULE_NAME, f"{THREAD_NAME} Collect Error: {e}")
            last_collect = time.time()

        time.sleep(1)

    base.output_log(MODULE_NAME, f"{THREAD_NAME} Stopped.")
    return None


def t_state_monitor_worker(stop_event: threading.Event) -> None:
    """
    システムの状態をモニターしTeamsへ通知を送るスレッド
    Mod: 2026.10.01    By: IWATA, Y.'s Agent AI（カード作成・キー判定などの内部関数をモジュールの関数として外に出した）
    """
    THREAD_NAME = "StateMonitor"

    base.output_log(MODULE_NAME, f"{THREAD_NAME} Started.")
    try:
        teams_conf = setting.get("teams_notification", {})
        enabled     = teams_conf.get("enabled", False)
        webhook_url = os.environ.get("WEBHOOK_URL", "")
        send_teams = enabled and webhook_url
        if not(send_teams):
            base.output_log(MODULE_NAME, f"{THREAD_NAME} Disabled or No URL.")

        # Entra ID クライアントクレデンシャルズ（IT部門の案内：incoming webhook廃止 → Teams Webhookコネクタ移行に伴う認証情報）
        # 秘匿情報のため環境変数管理。未設定（空文字）なら従来通りBearerヘッダー無しで送信する。
        webhook_tenant_id     = os.environ.get("WEBHOOK_TENANT_ID", "")
        webhook_client_id     = os.environ.get("WEBHOOK_CLIENT_ID", "")
        webhook_client_secret = os.environ.get("WEBHOOK_CLIENT_SECRET", "")
        use_webhook_oauth     = bool(webhook_tenant_id and webhook_client_id and webhook_client_secret)
        base.output_log(MODULE_NAME, f"{THREAD_NAME} use_webhook_oauth={use_webhook_oauth}")
        
        proxy_url = os.environ.get("PROXY_URL", "")
        interval  = teams_conf.get("interval_sec", 3600)
        links     = teams_conf.get("links", {})
        title_pfx = teams_conf.get("title_pfx", "")

        # 自己署名証明書環境（社内プロキシのSSLインスペクション等）向け
        skip_ssl_verify = bool(teams_conf.get("skip_ssl_verify", False))
        base.output_log(MODULE_NAME, f"{THREAD_NAME} skip_ssl_verify={skip_ssl_verify}")

        # ---- 統計情報のうち、差分表示（前回Teams投稿時からの増分）したいキー ----
        # 形式: {"モジュール名": ["キー名またはfnmatchパターン", ...], ...}
        # 例: {"FIND_SPO": ["items[*]", "total_items"], "ACT_S3": ["total_uploaded_files"]}
        # 指定が無いキーは、従来通り「起動からの累計値」をそのまま表示する
        diff_keys_conf = teams_conf.get("diff_keys", {})
        base.output_log(MODULE_NAME, f"{THREAD_NAME} diff_keys={diff_keys_conf}")

        # ---- "Device Info" セクションに表示する device_info のキー一覧 ----
        # 例: ["dev_id", "dev_name"]。省略時は何も表示しない（後方互換）
        device_info_keys = teams_conf.get("device_info_keys", [])
        base.output_log(MODULE_NAME, f"{THREAD_NAME} device_info_keys={device_info_keys}")

    except Exception as e:
        base.output_log(MODULE_NAME, f"{THREAD_NAME} Init Error: {e}")
        return
    
    def _is_diff_key(mod_name: str, key: str) -> bool:
        """指定キーが diff_keys 設定に該当するかを判定する（判定は _match_key_patterns）"""
        return _match_key_patterns(diff_keys_conf.get(mod_name, []), key)
    
    """起動通知"""
    if send_teams:
        start_card = make_payload(
            f"{title_pfx} TriEdge System Started", 
            "Operator Module has Started.", 
            "00FF00",
            [
                {"name": "Host"   , "value": os.environ.get('COMPUTERNAME', 'Unknown')},
                {"name": "端末"   , "value": f"{device_info['dev_id']}  {device_info['dev_name']}  {device_info['dev_ip']}"},
                {"name": "設備", "value": f"{device_info['setsubi_id']}  {device_info['setsubi_name']}"},
                {"name": "工程"  , "value": f"{device_info['kotei_id']}  {device_info['kotei_name']}"},
                {"name": "建屋" , "value": f"{device_info['tateya_id']}  {device_info['tateya_name']}"},
                {"name": "最寄り柱", "value": f"{device_info['hashira_id']}  {device_info['hashira_name']}"},
            ],
            links
        )
        _tok = get_webhook_token(webhook_tenant_id, webhook_client_id, webhook_client_secret, proxy_url, skip_ssl_verify) if use_webhook_oauth else ""
        send_teams_card(webhook_url, proxy_url, start_card, webhook_token=_tok, skip_ssl_verify=skip_ssl_verify)
        base.output_log(MODULE_NAME, f"System Started  {start_card}", log_file)

    """======== loop init ========"""
    loop_start_time = time.time()
    count = 0
    prev_metrics_snapshot: Dict[str, Dict[str, Any]] = {}  # diff_keys差分計算用（前回Teams投稿時点のスナップショット）
    last_mode_check     = 0.0
    mode_check_interval = 2.0
    offline_file = base.get_ctrl_state_offline_path()
    is_offline   = False
    base._set_mode_is_offline(is_offline)
    debug_file = base.get_ctrl_state_debug_path()
    is_debug   = False
    base._set_mode_is_debug(is_debug)
    
    """======== loop start ========"""
    while not stop_event.is_set():
        
        """Check Mode"""
        if time.time() - last_mode_check >= mode_check_interval:
            last_mode_check = time.time()
            
            # Offline
            is_offline_now = Path(offline_file).exists()
            if is_offline != is_offline_now:
                is_offline = is_offline_now
                base._set_mode_is_offline(is_offline)
                _mode_str = "OFFLINE" if is_offline else "ONLINE"
                base.output_log(MODULE_NAME, f"Mode Change Detected. Current Mode: {_mode_str}", log_file)
            
            # Debug
            is_debug_now = Path(debug_file).exists()
            if is_debug != is_debug_now:
                is_debug = is_debug_now
                base._set_mode_is_debug(is_debug)
                _mode_str = "DEBUG" if is_debug else "ACTUAL"
                base.output_log(MODULE_NAME, f"Mode Change Detected. Current Mode: {_mode_str}", log_file)
        
        """Resource Monitor"""
        if (send_teams and count >= interval):
            # 計測は行わない。metrics_collector（別スレッド・別タイミング）が登録した値を読むだけ
            all_metrics = base.metrics_get_all()
            res = all_metrics.get("SYSTEM", {"cpu": "-", "mem": "-", "disk": "-", "dirs": {}})

            # ---- "Device Info" セクション（device_info_keys設定に無ければセクションごと省略） ----
            device_info_facts = _device_info_facts(device_info_keys)

            # ---- "Resource Monitor" セクション ----
            # dirs は metrics_collector.watch_dirs で設定した各ディレクトリの「サイズ + ファイル数」。
            # 未設定時は _DEFAULT_WATCH_DIRS（Log Dir / Work Dir）が入るため、既存の表示と後方互換
            resource_facts = [
                {"name": "Resource Monitor", "value": ""},
                {"name": "- CPU", "value": res["cpu"]},
                {"name": "- Memory", "value": res["mem"]},
                {"name": "- Storage (Drive)", "value": res["disk"]},
            ]
            for _dir_name, _dir_value in res.get("dirs", {}).items():
                resource_facts.append({"name": f"- ({_dir_name})", "value": _dir_value})

            # ---- "Trigger Metrics" セクション ----
            metrics_facts = [{"name": "Trigger Metrics", "value": ""}]
            for mod_name, mod_metrics in sorted(all_metrics.items()):
                if mod_name == "SYSTEM":
                    continue  # CPU/Memory/Storageは上のResource Monitorセクションで表示済みのため、ここでは重複させない
                prev_mod_metrics = prev_metrics_snapshot.get(mod_name, {})

                # ---- 各キーの表示用文字列（diff_keys該当なら差分、それ以外は累計）を先に計算 ----
                _val_str: Dict[str, str] = {}
                for k, v in mod_metrics.items():
                    if _is_diff_key(mod_name, k):
                        # diff_keys設定に該当するキーは、前回Teams投稿時点からの差分を表示する
                        # （累計値そのものは base 側のレジストリに残り続けるので、表示だけを差分にする）
                        prev_v = prev_mod_metrics.get(k, 0)
                        try:
                            _val_str[k] = f"+{v - prev_v}"
                        except TypeError:
                            # 数値でないキーをdiff_keysに指定してしまった場合は、差分にできないため
                            # そのまま表示する（設定ミスに気付けるよう、その旨を明示）
                            _val_str[k] = f"{v}(差分不可)"
                    else:
                        _val_str[k] = f"{v}"

                # ---- "prefix[識別子]" 形式のキー（items[site/list] 等）を識別子ごとにペアリングし、
                # 例えば items[site/list] と latest_item_date[site/list] を1行にまとめて見やすくする ----
                _bracket_groups: Dict[str, Dict[str, str]] = {}  # 識別子 -> {prefix: 表示文字列}
                _bracket_order: List[str] = []  # 識別子の初出順（表示順の安定化のため）
                _lines: List[Tuple[str, str]] = []

                for k, v in mod_metrics.items():
                    m = re.match(r'^(\w+)\[(.+)\]$', k)
                    if m:
                        prefix, ident = m.group(1), m.group(2)
                        if ident not in _bracket_groups:
                            _bracket_groups[ident] = {}
                            _bracket_order.append(ident)
                        _bracket_groups[ident][prefix] = _val_str[k]
                    else:
                        _lines.append((k, _val_str[k]))

                for ident in _bracket_order:
                    parts = _bracket_groups[ident]
                    items_part = parts.get("items")
                    date_part  = parts.get("latest_item_date")
                    if items_part is not None and date_part is not None:
                        _lines.append((f"items[{ident}]", f"{items_part}　,　{date_part}"))
                    else:
                        # ペアの片方しか無い場合は、キーごとに個別の行として出す
                        for prefix, val in parts.items():
                            _lines.append((f"{prefix}[{ident}]", val))

                # ---- モジュール名を見出し行として出し、各メトリクスは1行=1つのfactエントリにする ----
                # 実機で確認したところ、facts の value 内で改行（"\n"や"  \n"）やMarkdown太字（**text**）は
                # レンダリングされず、単に折り返しで長い1行に見えてしまうことが分かった。
                # facts配列自体を複数エントリに分ければ、Teams側が本来持っているname/value表形式の
                # 行として確実に改行されるため、こちらの方式に変更した。
                # また、factsの name 先頭の半角スペースはTeams側で無視されて消えるため、
                # インデント代わりに "- " プレフィックスを使っている。
                metrics_facts.append({"name": mod_name, "value": ""})
                for _line_name, _line_value in _lines:
                    metrics_facts.append({"name": f"- {_line_name}", "value": _line_value})

            # ペイロード作成
            uptime_hour = round((time.time() - loop_start_time) / 3600, 1)
            hb_card = make_payload(
                f"{title_pfx} TriEdge Resource Monitor [{device_info['dev_name']} / {device_info['dev_ip']}]",
                f"System Running Normally. (pid={os.getpid()}  uptime={uptime_hour} hours)", 
                "0076D7",
                _join_fact_sections(device_info_facts, resource_facts, metrics_facts),
                links
            )
            
            # Teams通知
            _tok = get_webhook_token(webhook_tenant_id, webhook_client_id, webhook_client_secret, proxy_url, skip_ssl_verify) if use_webhook_oauth else ""
            send_teams_card(webhook_url, proxy_url, hb_card, webhook_token=_tok, skip_ssl_verify=skip_ssl_verify)

            # 次回の差分計算のため、今回投稿した時点のスナップショットを保存
            prev_metrics_snapshot = all_metrics

            base.output_log(MODULE_NAME, f"Resource Monitor (CPU:{res['cpu']} Mem:{res['mem']} Disk:{res['disk']})", log_file)
            base.output_log(MODULE_NAME, f"Trigger Metrics: {all_metrics}", log_file)
            count = 0
        
        count += 1
        time.sleep(1)
    """======== loop end ========"""
    
    """終了通知"""
    if send_teams:
        stop_card = make_payload(
            f"{title_pfx} TriEdge System Stopped",
            "Operator Module has Stopped.",
            "FFA500",
            links=links
        )
        _tok = get_webhook_token(webhook_tenant_id, webhook_client_id, webhook_client_secret, proxy_url, skip_ssl_verify) if use_webhook_oauth else ""
        send_teams_card(webhook_url, proxy_url, stop_card, webhook_token=_tok, skip_ssl_verify=skip_ssl_verify)
    base.output_log(MODULE_NAME, f"{THREAD_NAME} Stopped.", log_file)
    return None


def t_ondemand_notify_worker(stop_event: threading.Event) -> None:
    """
    オンデマンド通知要求（base.metrics_notify）をモニターし、Teamsへ送るスレッド
    Mod: 2026.10.01    By: IWATA, Y.'s Agent AI
    ・宛先は定期通知（t_state_monitor_worker）とは別（環境変数 WEBHOOK_URL_ONDEMAND）
    ・同じモジュールの要求は min_interval_sec 内に1回だけ、送信時点の最新の metrics で送る
    ・送る metrics のキーは notify_keys で絞り込む（指定の無いモジュールは全キー）
    ・オフラインモード時は送らずにログだけ残す。送信失敗もログのみ（再送しない）
    ・停止時は保留中の要求を送ってから終了する
    """
    THREAD_NAME = "OnDemandNotify"
    base.output_log(MODULE_NAME, f"{THREAD_NAME} Started.")

    try:
        conf      = setting.get("teams_ondemand", {})
        base_conf = setting.get("teams_notification", {})  # 未指定の共通項目は定期通知の設定を使う
        enabled     = bool(conf.get("enabled", False))
        webhook_url = os.environ.get("WEBHOOK_URL_ONDEMAND", "")
        if not (enabled and webhook_url):
            base.output_log(MODULE_NAME, f"{THREAD_NAME} Disabled or No URL.")
            return

        # 認証情報はオンデマンド専用を優先し、1つも設定が無ければ定期通知と同じものを使う
        webhook_tenant_id     = os.environ.get("WEBHOOK_ONDEMAND_TENANT_ID", "")
        webhook_client_id     = os.environ.get("WEBHOOK_ONDEMAND_CLIENT_ID", "")
        webhook_client_secret = os.environ.get("WEBHOOK_ONDEMAND_CLIENT_SECRET", "")
        if not (webhook_tenant_id or webhook_client_id or webhook_client_secret):
            webhook_tenant_id     = os.environ.get("WEBHOOK_TENANT_ID", "")
            webhook_client_id     = os.environ.get("WEBHOOK_CLIENT_ID", "")
            webhook_client_secret = os.environ.get("WEBHOOK_CLIENT_SECRET", "")
        use_webhook_oauth = bool(webhook_tenant_id and webhook_client_id and webhook_client_secret)

        proxy_url        = os.environ.get("PROXY_URL", "")
        min_interval_sec = float(conf.get("min_interval_sec", 5))
        notify_keys      = conf.get("notify_keys", {})
        title_pfx        = conf.get("title_pfx", base_conf.get("title_pfx", ""))
        skip_ssl_verify  = bool(conf.get("skip_ssl_verify", base_conf.get("skip_ssl_verify", False)))
        device_info_keys = conf.get("device_info_keys", base_conf.get("device_info_keys", []))
        links            = conf.get("links", base_conf.get("links", {}))
        base.output_log(MODULE_NAME, f"{THREAD_NAME} use_webhook_oauth={use_webhook_oauth}  min_interval_sec={min_interval_sec}  "
                                     f"notify_keys={notify_keys}  skip_ssl_verify={skip_ssl_verify}")

    except Exception as e:
        base.output_log(MODULE_NAME, f"{THREAD_NAME} Init Error: {e}")
        return

    def _send(mod_name: str, messages: List[str]) -> None:
        """1モジュール分の通知を送る（metrics は送信時点のスナップショット）"""
        if base.get_mode_is_offline():
            base.output_log(MODULE_NAME, f"{THREAD_NAME} Skipped (OFFLINE): {mod_name}", log_file)
            return

        mod_metrics = base.metrics_get_all().get(mod_name, {})
        patterns = notify_keys.get(mod_name)
        values = {k: v for k, v in mod_metrics.items() if patterns is None or _match_key_patterns(patterns, k)}

        metrics_facts = [{"name": mod_name, "value": ""}]
        for k, v in values.items():
            metrics_facts.append({"name": f"- {k}", "value": str(v)})

        text = "\n\n".join(messages) if messages else f"On-Demand Notice from {mod_name}."
        card = make_payload(
            f"{title_pfx} TriEdge Notice [{device_info.get('dev_name', '-')}] {mod_name}",
            text,
            "0076D7",
            _join_fact_sections(_device_info_facts(device_info_keys), metrics_facts),
            links
        )
        _tok = get_webhook_token(webhook_tenant_id, webhook_client_id, webhook_client_secret, proxy_url, skip_ssl_verify) if use_webhook_oauth else ""
        if send_teams_card(webhook_url, proxy_url, card, webhook_token=_tok, skip_ssl_verify=skip_ssl_verify):
            base.output_log(MODULE_NAME, f"{THREAD_NAME} Sent: {mod_name} {values}", log_file)
        else:
            base.output_log(MODULE_NAME, f"{THREAD_NAME} Send Failed: {mod_name} {values}", log_file)

    def _collect(pending: Dict[str, List[str]]) -> None:
        """base に積まれた要求を取り出し、モジュールごとにまとめる（message は重複を除いて残す）"""
        reqs, dropped = base.notify_pop_all()
        if dropped:
            base.output_log(MODULE_NAME, f"{THREAD_NAME} Request Queue Overflow. dropped={dropped}", log_file)
        for r in reqs:
            msgs = pending.setdefault(r["module"], [])
            if r["message"] and r["message"] not in msgs:
                msgs.append(r["message"])

    """======== loop init ========"""
    pending: Dict[str, List[str]] = {}    # モジュール名 -> 添える message（送信待ち）
    last_sent: Dict[str, float]   = {}    # モジュール名 -> 前回送信時刻

    """======== loop start ========"""
    while not stop_event.is_set():
        # 要求が入るか、保留中の要求の送信時刻になるまで待つ（停止確認のため最大1秒）
        wait_sec = 1.0
        if pending:
            now = time.time()
            wait_sec = min(wait_sec, max(0.0, min(last_sent.get(m, 0.0) + min_interval_sec - now for m in pending)))
        base.notify_wait(wait_sec)

        try:
            _collect(pending)
            now = time.time()
            for mod_name in list(pending):
                if now - last_sent.get(mod_name, 0.0) >= min_interval_sec:
                    _send(mod_name, pending.pop(mod_name))
                    last_sent[mod_name] = time.time()
        except Exception as e:
            base.output_log(MODULE_NAME, f"{THREAD_NAME} Error: {e}", log_file)
    """======== loop end ========"""

    """停止時：保留中の要求を送る"""
    try:
        _collect(pending)
        for mod_name in list(pending):
            _send(mod_name, pending.pop(mod_name))
    except Exception as e:
        base.output_log(MODULE_NAME, f"{THREAD_NAME} Error: {e}", log_file)
    base.output_log(MODULE_NAME, f"{THREAD_NAME} Stopped.", log_file)
    return None


########################################
# Main Loop
########################################

def main_loop():
    """
    Stream Coreの稼働監視・自動再起動ループ
    Mod: 2026.10.01    By: IWATA, Y.'s Agent AI（オンデマンド通知スレッドの起動・停止を追加）
    """
    base.output_log(MODULE_NAME, f"Operator Started. pid={os.getpid()}")
    
    """Settings"""
    RAPID_BORDER_SEC   = 15
    MAX_RAPID_RETRIES  =  3
    
    """==== Start State Monitor Thread ===="""
    stop_event = threading.Event()
    t_state = threading.Thread(target=t_state_monitor_worker, args=(stop_event,), daemon=True)
    t_state.start()

    t_metrics = threading.Thread(target=t_metrics_collector_worker, args=(stop_event,), daemon=True)
    t_metrics.start()

    t_ondemand = threading.Thread(target=t_ondemand_notify_worker, args=(stop_event,), daemon=True)
    t_ondemand.start()
    
    """======== loop init ========"""
    last_crash_time    = None
    retry_count        = 0
    rapid_crash_count  = 0
    """======== loop start ========"""
    try:
        while True:
            """==== Start StreamCore ===="""
            try:
                base.output_log(MODULE_NAME, "Launching Stream Core...")
                stream.main()
                
                base.output_log(MODULE_NAME, "Stream Core Stopped Normally.")
                break # 正常終了

            except KeyboardInterrupt as ki:
                ki_msg = str(ki)
                if "ErrorIF escalation" in ki_msg:
                    base.output_log(MODULE_NAME, f"Stream Core Stopped by ErrorIF Escalation: {ki_msg}", log_file)
                elif "Already Running" in ki_msg:
                    base.output_log(MODULE_NAME, "Stream Core Already Running. Skip Boot.", log_file)
                elif "Stopped by Control File" in ki_msg:
                    base.output_log(MODULE_NAME, "Stream Core Stopped by Control File.", log_file)                
                else:
                    base.output_log(MODULE_NAME, "Stream Core Stopped by User.", log_file)
                
                break
            
            ## === 異常終了時は再起動 (ReBoot) ===
            except Exception as e:
                current_time  = dt.now()
                retry_count  += 1
                
                if last_crash_time is not None:
                    elapsed_sec = (current_time - last_crash_time).total_seconds()
                    
                    if elapsed_sec < RAPID_BORDER_SEC:
                        rapid_crash_count += 1
                        base.output_log(MODULE_NAME, f"Rapid Crash Detected! ({rapid_crash_count}/{MAX_RAPID_RETRIES}) Interval={elapsed_sec:.1f}s")
                        
                    else:
                        rapid_crash_count  = 0
                        base.output_log(MODULE_NAME, f"Rapid Crash Counter Reset. Interval={elapsed_sec:.1f}s")
                
                last_crash_time = current_time
                
                """too many rapid crash"""
                if rapid_crash_count >= MAX_RAPID_RETRIES:
                    base.output_log(MODULE_NAME, f"FATAL: Too Many Rapid Crashes ({rapid_crash_count} times). System Giving Up.")
                    base.output_log(MODULE_NAME, f"Last Error: {e}")
                    #### TBD #### ここでTeamsに通知してもいいかも！
                    break
                
                base.output_log(MODULE_NAME, f"Stream Core Anomaly Crashed! Try to Reboot. (Retry: {retry_count}) Error: {e}")
                base.output_log(MODULE_NAME, traceback.format_exc())
                #### TBD #### ここでTeamsに通知してもいいかも！
                
                time.sleep(5) # 負荷抑制
                continue
        """======== loop end ========"""
    finally:
        base.output_log(MODULE_NAME, "Operator initiating shutdown sequence...")

        # 0. ErrorIF セーフティネット（StreamCore側でshutdown済みのはずだが、異常終了時の保険）
        try:
            err.ErrorIF.shutdown()
        except Exception:
            pass
        
        # 1. Teams通知スレッドのループを抜けるようにフラグを立てる
        stop_event.set()
        
        try:
            # 2. スレッドが完全に終了するのを待つ（終了通知の送信は
            #    Entra IDトークン取得＋Power Automateへの投稿、と2回のネットワーク通信を伴うため
            #    社内プロキシ経由だと5秒では終わらない場合がある。daemon threadは
            #    タイムアウトでmain_loop()が抜けると強制終了されてしまうため、余裕を持たせる）
            base.output_log(MODULE_NAME, "Waiting for Teams Worker to stop...")
            t_state.join(timeout=15.0)
            if t_state.is_alive():
                base.output_log(MODULE_NAME, "Warning: Teams Worker did not stop in time. Shutdown notice may not have been sent.")

            t_metrics.join(timeout=5.0)

            # オンデマンド通知は停止時に保留分（StreamCore 停止時の集計等）を送るため、定期通知と同じく余裕を持たせる
            t_ondemand.join(timeout=15.0)
            if t_ondemand.is_alive():
                base.output_log(MODULE_NAME, "Warning: OnDemand Notify Worker did not stop in time. Pending notices may not have been sent.")
            
        except Exception as e:
            base.output_log(MODULE_NAME, f"Warning: Teams Worker join error: {e}")
    
    base.output_log(MODULE_NAME, "Operator Stopped Completely.")


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    main_loop()