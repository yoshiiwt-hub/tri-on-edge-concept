# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger Module [Alive]
# Ver: 01.a.20261008    By: IWATA, Y.'s Agent AI
# Mod: 新規作成（機器の死活監視。TCP 接続確認の結果を1機器1行で送る）
########################################
f_check_alive_trigger  # 設定した機器へ TCP 接続を試み、死活（OK/NG）を1機器1行でキューに送る Find Trigger

動作概要
    1. 起動時に監視対象（targets）を検証する（key は必須・重複不可。誤りは起動時エラー）
    2. interval_sec ごとに、全機器へ並列（スレッド）で TCP 接続を試みる（ping は使わない）
    3. 結果を targets の設定順に1機器1行で送り、最後に区切り行（'____'）を送る（1周期 = 1セグメント）
    4. NG の機器はデータとして送る（ErrorIF へは報告しない。ErrorIF は設定・起動時の誤りのみ）
    5. stop_event が立つまでループを継続する

判定（method = "tcp"。将来 method を追加できるよう設定キーを用意）
    接続成功                       : OK（detail="connected"）
    接続拒否（RST）                : refused_is_ok が true なら OK、false なら NG（detail="refused"）
                                     ※ RST が返る ＝ 機器（OS）は応答している、とみなす
    タイムアウト・到達不可・名前解決失敗 : NG（detail="timeout" / "unreachable: ..." / "dns_error: ..." 等）
    NG のときは retry 回まで再試行し、1回でも OK なら OK とする

設定（find_trigger）
    targets      : 監視対象のリスト（必須）
                   [{"key": "dev01", "name": "機器１", "host": "192.168.0.10", "port": 502}, ...]
                     key  : 機器の識別子（必須・重複不可。Act のテーブルの主キーになる）
                     name : 機器名（省略時は key）
                     host : IP アドレスまたはホスト名（必須）
                     port : TCP ポート（省略時は default_port）
    default_port : targets で port を省略したときのポート（デフォルト: なし ＝ port 必須）
    method       : 判定方式（デフォルト: "tcp"。現在は "tcp" のみ）
    interval_sec : 監視周期 秒（デフォルト: 60）。確認に周期より長くかかった場合は、終わり次第つぎの周期を始める
    timeout_sec  : 1回の接続タイムアウト 秒（デフォルト: 3）
    retry        : NG のときの再試行回数（デフォルト: 1）
    refused_is_ok: 接続拒否（RST）を OK とみなすか（デフォルト: true）
    max_workers  : 並列に確認する機器数の上限（デフォルト: 16）
    VERBOSE      : 結果をコンソールに表示する（デフォルト: false）

送る列（column の列名で値を割り当てる。並び順は自由。下記以外の列名は起動時エラー）
    device_key  : str   機器の識別子（targets の key）
    device_name : str   機器名
    status      : str   "OK" / "NG"
    checked_at  : time  確認した日時（機器ごと）
    host        : str   接続先
    port        : int   ポート
    rtt_ms      : float 接続にかかった時間 ミリ秒（NG のときは None）
    detail      : str   判定の詳細（"connected" / "refused" / "timeout" 等）
"""
MODULE_NAME = "FIND_Alive"

"""standard library"""
from   concurrent.futures import ThreadPoolExecutor
from   datetime import datetime as dt
import os
import queue
import socket
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Optional, Tuple

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
from   src.tri_s01_error_if       import ErrorIF, SIGNAL_STOP

"""system dir, file"""
system_root = base.get_system_root()
log_dir  = base.get_log_dir()
log_file = base.get_log_path()
work_dir = base.get_work_dir()

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__=='__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# Alive Check Functions
########################################

ALIVE_METHODS = ("tcp",)

# 送る列（column の列名 → 値の取り出し方は _make_row で対応付ける）
ALIVE_COLUMNS = ("device_key", "device_name", "status", "checked_at", "host", "port", "rtt_ms", "detail")

SEPARATOR = "____"


def parse_alive_targets(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    targets を検証し、[{"key","name","host","port"}, ...] に整えて返す（起動時に1回）
    key 未設定・重複、host 未設定、port の誤りは ValueError
    Mod: 2026.10.08    By: IWATA, Y.'s Agent AI
    """
    targets = config.get("targets")
    if not isinstance(targets, list) or not targets:
        raise ValueError("config['targets'] は1件以上のリストで指定してください。"
                         ' 例: [{"key": "dev01", "name": "機器１", "host": "192.168.0.10", "port": 502}]')

    default_port = config.get("default_port")
    result: List[Dict[str, Any]] = []
    seen: Dict[str, int] = {}
    for i, t in enumerate(targets):
        if not isinstance(t, dict):
            raise ValueError(f"targets[{i}] は {{\"key\",\"name\",\"host\",\"port\"}} の形で指定してください。 値: {t!r}")

        key = t.get("key")
        if key is None or str(key).strip() == "":
            raise ValueError(f"targets[{i}] に key がありません（key は必須）。 値: {t!r}")
        key = str(key).strip()
        if key == SEPARATOR:
            raise ValueError(f"targets[{i}] の key に区切り文字 '{SEPARATOR}' は使えません。")
        if key in seen:
            raise ValueError(f"targets[{i}] の key '{key}' が targets[{seen[key]}] と重複しています（key は重複不可）。")
        seen[key] = i

        host = t.get("host")
        if host is None or str(host).strip() == "":
            raise ValueError(f"targets[{i}] (key='{key}') に host がありません（host は必須）。")

        port = t.get("port", default_port)
        if port is None:
            raise ValueError(f"targets[{i}] (key='{key}') に port がありません（port か default_port を指定してください）。")
        try:
            port = int(port)
        except (TypeError, ValueError):
            raise ValueError(f"targets[{i}] (key='{key}') の port が数値ではありません。 値: {port!r}")
        if not (0 < port < 65536):
            raise ValueError(f"targets[{i}] (key='{key}') の port が範囲外です（1～65535）。 値: {port}")

        name = t.get("name")
        name = key if name is None or str(name) == "" else str(name)
        result.append({"key": key, "name": name, "host": str(host).strip(), "port": port})
    return result


def parse_alive_columns(col_def: Dict[int, Any]) -> List[str]:
    """
    column の列名を、送る列（ALIVE_COLUMNS）に対応付けて並び順のリストで返す（起動時に1回）
    対応しない列名・device_key が無い場合は ValueError
    Mod: 2026.10.08    By: IWATA, Y.'s Agent AI
    """
    names = [str(col_def[k][0]) for k in sorted(col_def.keys())]
    unknown = [n for n in names if n not in ALIVE_COLUMNS]
    if unknown:
        raise ValueError(f"column に対応しない列名があります: {unknown}  使える列名: {list(ALIVE_COLUMNS)}")
    if "device_key" not in names:
        raise ValueError(f"column に device_key がありません（必須）。 column の列名: {names}")
    return names


def tcp_check_once(host: str, port: int, timeout_sec: float, refused_is_ok: bool) -> Tuple[str, Optional[float], str]:
    """
    TCP 接続を1回試みて (status, rtt_ms, detail) を返す（例外は出さない）
    Mod: 2026.10.08    By: IWATA, Y.'s Agent AI
    """
    t0 = time.perf_counter()
    try:
        sock = socket.create_connection((host, port), timeout=timeout_sec)
        rtt_ms = (time.perf_counter() - t0) * 1000.0
        try:
            sock.close()
        except Exception:
            pass
        return "OK", round(rtt_ms, 3), "connected"

    except ConnectionRefusedError:
        # RST が返った ＝ 機器（OS）は応答している
        rtt_ms = (time.perf_counter() - t0) * 1000.0
        if refused_is_ok:
            return "OK", round(rtt_ms, 3), "refused"
        return "NG", None, "refused"

    except socket.timeout:
        return "NG", None, "timeout"

    except socket.gaierror as e:
        return "NG", None, f"dns_error: {e}"

    except OSError as e:
        return "NG", None, f"unreachable: {type(e).__name__}: {e}"

    except Exception as e:
        return "NG", None, f"error: {type(e).__name__}: {e}"


def check_alive(target: Dict[str, Any], method: str, timeout_sec: float, retry: int,
                refused_is_ok: bool) -> Dict[str, Any]:
    """
    1機器の死活を確認して結果の dict を返す（NG なら retry 回まで再試行。1回でも OK なら OK）
    Mod: 2026.10.08    By: IWATA, Y.'s Agent AI
    """
    status, rtt_ms, detail = "NG", None, "not checked"
    for _ in range(max(0, int(retry)) + 1):
        if method == "tcp":
            status, rtt_ms, detail = tcp_check_once(target["host"], target["port"], timeout_sec, refused_is_ok)
        if status == "OK":
            break
    return {
        "device_key" : target["key"],
        "device_name": target["name"],
        "status"     : status,
        "checked_at" : dt.now(),
        "host"       : target["host"],
        "port"       : target["port"],
        "rtt_ms"     : rtt_ms,
        "detail"     : detail,
    }


def _make_row(result: Dict[str, Any], col_names: List[str]) -> List[Any]:
    """確認結果を column の並びの値リストにする"""
    return [result.get(n) for n in col_names]


########################################
# Find Trigger Functions
########################################

def f_check_alive_trigger(
    queue_out: queue.Queue,
    col_def: Dict[int, Any],
    config: Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """
    設定した機器へ TCP 接続を試み、死活（OK/NG）を1機器1行で送る（1周期ごとに区切り行 '____'）
    Mod: 2026.10.08    By: IWATA, Y.'s Agent AI
    """
    TRIGGER_NAME = "f_check_alive_trigger"

    def _responsive_wait(seconds: float) -> None:
        """stop_event を見ながら細かく待つ（即座に停止へ反応できるように。tri_s11_find_socket.py 等と同様）"""
        _wait_end = time.time() + seconds
        while time.time() < _wait_end and not stop_event.is_set():
            time.sleep(min(0.05, max(0.0, _wait_end - time.time())))

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start.", log_file)
    try:
        VERBOSE = config.get("VERBOSE", False)

        """ Col Define """
        col_names = parse_alive_columns(col_def)
        n_col_def = len(col_names)

        """ Settings """
        targets = parse_alive_targets(config)
        method  = str(config.get("method", "tcp")).lower()
        if method not in ALIVE_METHODS:
            raise ValueError(f"config['method'] が対応していません: {method!r}  使える値: {list(ALIVE_METHODS)}")
        interval_sec  = float(config.get("interval_sec", 60))
        timeout_sec   = float(config.get("timeout_sec", 3))
        retry         = int(config.get("retry", 1))
        refused_is_ok = bool(config.get("refused_is_ok", True))
        max_workers   = int(config.get("max_workers", 16))
        if interval_sec <= 0 or timeout_sec <= 0 or retry < 0 or max_workers < 1:
            raise ValueError(f"設定値が範囲外です: interval_sec={interval_sec}(>0)  timeout_sec={timeout_sec}(>0)"
                             f"  retry={retry}(>=0)  max_workers={max_workers}(>=1)")

        base.output_log(MODULE_NAME,
            f"targets={len(targets)}  method={method}  interval_sec={interval_sec}  timeout_sec={timeout_sec}"
            f"  retry={retry}  refused_is_ok={refused_is_ok}  max_workers={max_workers}  columns={col_names}", log_file)
        for t in targets:
            base.output_log(MODULE_NAME, f"  target: key={t['key']}  name={t['name']}  {t['host']}:{t['port']}", log_file)

    except Exception as e:
        ErrorIF.report_exception(
            e,
            phase="startup",
            category="config",
            trigger_id=TRIGGER_NAME,
            log_file=log_file,
        )
        stop_event.set()
        return None

    """======== MAIN LOOP START ========"""
    executor = ThreadPoolExecutor(max_workers=min(max_workers, len(targets)), thread_name_prefix="AliveCheck")
    count = 0
    try:
        while not stop_event.is_set():
            cycle_start = time.time()
            try:
                # 1. 全機器を並列に確認（結果は targets の設定順）
                futures = [executor.submit(check_alive, t, method, timeout_sec, retry, refused_is_ok) for t in targets]
                results = [f.result() for f in futures]

                # 2. 1機器1行で送り、最後に区切り行を送る（1周期 = 1セグメント）
                for r in results:
                    queue_out.put((count, _make_row(r, col_names)))
                    count += 1
                queue_out.put((count, [SEPARATOR] * n_col_def))
                count += 1

                n_ng = sum(1 for r in results if r["status"] != "OK")
                base.output_log(MODULE_NAME, f"Alive Checked. OK={len(results) - n_ng}  NG={n_ng}")
                if VERBOSE:
                    for r in results:
                        sys.stdout.write(f"  {r['status']:<2} {r['device_key']} ({r['device_name']}) "
                                         f"{r['host']}:{r['port']}  rtt_ms={r['rtt_ms']}  {r['detail']}\n")
                    sys.stdout.flush()

            except Exception as e:
                _sig = ErrorIF.report_exception(
                    e,
                    phase="loop",
                    category="data",
                    trigger_id=TRIGGER_NAME,
                    log_file=log_file
                )
                if _sig == SIGNAL_STOP:
                    stop_event.set()
                    break

            # 3. 次の周期まで待つ（確認に周期より長くかかった場合は待たない）
            _responsive_wait(max(0.0, interval_sec - (time.time() - cycle_start)))

    finally:
        executor.shutdown(wait=True)
    """======== MAIN LOOP END ========"""
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None
