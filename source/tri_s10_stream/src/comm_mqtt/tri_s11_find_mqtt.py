# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger [MQTT]
# Ver: 01.l.20261106    By: IWATA, Y.'s Agent AI
# Mod: 接続できない状態の管理を共通の tri_s01_connect_state（ConnectState）に置き換え。接続の失敗・切断の検知は
#      retry_wait_sec ごとにその場で接続し直し（リトライ）、連続 retry_max 回で not_connectable とする
########################################
MQTT のトピックに届く値を、列にまとめて取り込む入口

主要関数:
f_find_mqtt_trigger  # MQTT ブローカーからメッセージを受信し、キューに流す。

位置づけ: センサーや他システムが MQTT で送る値を受け取る。通信の部品は tri_s19_mqtt を使い、tri_s12_act_mqtt が送ったセグメントも受け取れる。
設計意図: 1トピック = 1列の形と、1トピックに JSON でまとめて届く形の両方を、topic_map の書き方だけで扱えるようにする。外部のブローカーが無い現場でも、組込みブローカーで動かせるようにする。接続に失敗したり切断を検知したら、その場で接続し直し（リトライ）、続けて失敗したら接続できない（not_connectable）とする。接続できない状態の記録・再接続の間隔は tri_s01_connect_state に任せる。
前提・制約: 受信した最新値を polling_ms ごとに送る（push_mode=true なら受信のたびに送る）。payload_format="json_rows" は push_mode=true が必須。
"""
MODULE_NAME = "FIND_MQTT"

"""standard library"""
from   datetime import datetime as dt
import json
import os
import queue
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Optional, Tuple

"""external library"""
import paho.mqtt.client as mqtt

"""[Bootstrap] Resolve Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    sys.stdout.write(f"{dt.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    sys.stdout.flush()

except ModuleNotFoundError:
    try:
        dir_valid = 'comm_mqtt'
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
from src.tri_s01_error_if import ErrorIF, SIGNAL_STOP
from src.tri_s01_connect_state import ConnectState
import src.comm_mqtt.tri_s19_mqtt as mqtt_util

"""system dir, file"""
system_root = base.get_system_root()
log_file    = base.get_log_path()

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={base.get_work_dir()}", log_file)


########################################
# Find Trigger Function
########################################

def f_find_mqtt_trigger(
    queue_out : queue.Queue,
    col_def   : Dict[int, Any],
    config    : Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """
    MQTT ブローカーからメッセージを受信し、キューに流す
    Mod: 2026.08.01    By: IWATA,Y.

    [接続の扱い]
        リトライ: 接続（CONNACK が来ない・拒否）に失敗したり、切断を検知したら、retry_wait_sec ごとに接続し直す（このトリガーの中だけ）
            連続 retry_max 回失敗したら、接続できない（not_connectable）と判定して ConnectState へ報告する（接続できたら回数を 0 に戻す）
        切断の検知: paho のコールバックはトリガー内の印（link_up）だけを変え、ConnectState への報告はトリガーのスレッドから行う
        再接続: 接続できない間は reconnect_interval_sec ごとに試み、結果を ConnectState へ報告する（その間は値を送らない）
        接続の状態（ConnectState が登録。FIND_MQTT）: connect_state / not_connectable_since / reconnect_fail_count / total_not_connectable
    """
    TRIGGER_NAME = "f_find_mqtt_trigger"
    CONNACK_TIMEOUT_SEC = 10.0   # CONNACK を待つ時間

    """======== STARTUP ========"""
    try:
        n_col_def = len(col_def)
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Start. n_col_def={n_col_def}  col_keys={col_keys}", log_file)

        """ブローカー設定"""
        broker_mode   = str(config.get("broker_mode", "external")).lower()
        broker_host   = str(config.get("broker_host", "127.0.0.1"))
        broker_port   = int(config.get("broker_port", 1883))
        client_id     = str(config.get("client_id", "")) or None
        username      = config.get("username")
        password      = config.get("password")
        keepalive     = int(config.get("keepalive", 60))
        qos           = int(config.get("qos", 0))
        reconnect_interval_sec = float(config.get("reconnect_interval_sec", 10.0))
        retry_max              = int(config.get("retry_max", 3))
        retry_wait_sec         = float(config.get("retry_wait_sec", 1.0))
        if retry_max < 1 or retry_wait_sec < 0:
            raise ValueError(f"config['retry_max'] must be >= 1 and config['retry_wait_sec'] >= 0. Got: {retry_max} / {retry_wait_sec}")

        """TLS 設定"""
        tls_enable   = bool(config.get("tls_enable", False))
        tls_ca_cert  = config.get("tls_ca_cert")
        tls_certfile = config.get("tls_certfile")
        tls_keyfile  = config.get("tls_keyfile")
        tls_insecure = bool(config.get("tls_insecure", False))

        if tls_ca_cert:
            tls_ca_cert = base.get_absolute_path(tls_ca_cert)
        if tls_certfile:
            tls_certfile = base.get_absolute_path(tls_certfile)
        if tls_keyfile:
            tls_keyfile = base.get_absolute_path(tls_keyfile)

        """組込みブローカー設定"""
        embedded_allow_anonymous = bool(config.get("embedded_allow_anonymous", True))

        """受信設定"""
        topic_map_raw  = config.get("topic_map", {})
        if not topic_map_raw:
            raise ValueError("topic_map is required. e.g. {\"0\": \"sensor/temp\"}")
        polling_ms     = float(config.get("polling_ms", 100))
        push_mode      = bool(config.get("push_mode", False))
        payload_format = str(config.get("payload_format", "json")).lower()
        payload_key    = config.get("payload_key")
        rows_key       = str(config.get("rows_key", "rows"))
        encode         = str(config.get("encode", "utf-8"))

        if payload_format not in ("json", "raw", "json_rows"):
            raise ValueError(f"payload_format must be 'json', 'raw', or 'json_rows'. Got: {payload_format!r}")

        if payload_format == "json_rows" and not push_mode:
            raise ValueError(
                "payload_format='json_rows' requires push_mode=true "
                "（1メッセージに複数行が入っており、順序通りに個別処理する必要があるため）")

        """topic_map コンパイル: 各エントリは
             文字列              : "トピック名"（従来形式。payload_keyはグローバル設定を使う）
             辞書                : {"topic": "トピック名", "payload_key": "キー名"}（1トピックに
                                    複数列を割り当てたい場合に使う。payload_key省略時はグローバル設定）
           topic_to_entries: {topic: [(col_idx, payload_key), ...]}（1トピックに複数列が対応可能）"""
        topic_to_entries: Dict[str, List[Tuple[int, Optional[str]]]] = {}
        col_topic_map: Dict[int, str] = {}  # ログ表示用（col_idx → topic）

        for col_idx_str, entry in topic_map_raw.items():
            col_idx = int(col_idx_str)
            if not (0 <= col_idx < n_col_def):
                raise ValueError(
                    f"topic_map: col_idx={col_idx} out of range (0-{n_col_def-1})")

            if isinstance(entry, dict):
                if "topic" not in entry:
                    raise ValueError(f"topic_map[{col_idx_str}] must have a 'topic' key: {entry!r}")
                topic  = str(entry["topic"])
                p_key  = entry.get("payload_key", payload_key)  # 省略時はグローバル設定
            else:
                # 後方互換: 文字列そのものをトピック名として扱う（payload_keyはグローバル設定）
                topic  = str(entry)
                p_key  = payload_key

            topic_to_entries.setdefault(topic, []).append((col_idx, p_key))
            col_topic_map[col_idx] = topic

        subscribe_list = [(t, qos) for t in topic_to_entries.keys()]

        """dtype キャストプラン"""
        default_val_config = {
            "time_format"  : config.get("time_format",   "%Y-%m-%d %H:%M:%S.%f"),
            "time_default" : config.get("time_default",  "2999-01-01 00:00:00.000000"),
            "int_default"  : config.get("int_default",   -9999),
            "float_default": config.get("float_default", -9999.9),
            "str_default"  : config.get("str_default",   "__"),
        }
        cast_plan = base.set_cast_dtype(col_def, default_val_config)
        base.output_log(MODULE_NAME,
            f"broker_mode={broker_mode}  broker_port={broker_port}  "
            f"push_mode={push_mode}  payload_format={payload_format}  "
            f"col_topic_map={col_topic_map}  cast_plan={cast_plan}", log_file)
        base.output_log(MODULE_NAME,
            f"retry_max={retry_max}  retry_wait_sec={retry_wait_sec}  "
            f"reconnect_interval_sec={reconnect_interval_sec}", log_file)

        if broker_mode not in ("external", "embedded"):
            raise ValueError(
                f"broker_mode must be 'external' or 'embedded'. Got: {broker_mode}")

    except Exception as e:
        ErrorIF.report_exception(
            e, phase="startup", category="config",
            trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== 共有状態（コールバック ↔ メインループ）========"""
    current_values: List[Any]      = [None] * n_col_def
    current_lock:   threading.Lock = threading.Lock()
    push_queue:     queue.Queue    = queue.Queue()  # push_mode 用の中継キュー
    link_up       = False                            # 接続の印（CONNACK rc=0 で True、切断で False。コールバックが変える）
    connack_event = threading.Event()

    """======== コールバック ========"""

    def _on_connect(client, userdata, flags, rc):
        nonlocal link_up
        if rc == 0:
            link_up = True
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Connected to broker. Subscribing {len(subscribe_list)} topic(s).", log_file)
            client.subscribe(subscribe_list)
        else:
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Connect failed. rc={rc}")   # 失敗の記録は判定側（ConnectState / ErrorIF）に任せ、ここは画面のみ
        connack_event.set()

    def _on_disconnect(client, userdata, rc):
        nonlocal link_up
        link_up = False
        if rc != 0:
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Unexpected disconnect. rc={rc}", log_file)

    def _on_message(client, userdata, msg):
        entries = topic_to_entries.get(msg.topic)
        if not entries:
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} WARNING  Received on unmapped topic: {msg.topic!r} "
                f"(known topics: {list(topic_to_entries.keys())})", log_file)
            return

        if payload_format == "json_rows":
            """1メッセージに複数行がJSON配列でまとまっている形式（act_mqtt.py の
            publish_mode="segment" 等）。配列の各要素を、通常の1行ずつのpush_mode処理と
            同じように順番にpush_queueへ送る（seg_start/seg_end等の区切り検出が、
            分割されていない場合と同じ順序・粒度で動くようにするため）。"""
            try:
                text = msg.payload.decode(encode, errors="replace")
                obj  = json.loads(text)
                if isinstance(obj, list):
                    row_objs = obj
                elif isinstance(obj, dict):
                    row_objs = obj.get(rows_key, [])
                else:
                    row_objs = []
            except Exception as e:
                base.output_log(MODULE_NAME,
                    f"{TRIGGER_NAME} WARNING  json_rows のパースに失敗しました: "
                    f"{type(e).__name__}: {e}", log_file)
                return

            for row_obj in row_objs:
                with current_lock:
                    for col_idx, p_key in entries:
                        val = row_obj.get(p_key) if isinstance(row_obj, dict) else None
                        current_values[col_idx] = val
                    row = list(current_values)
                push_queue.put(row)
            return

        with current_lock:
            for col_idx, p_key in entries:
                val = mqtt_util.parse_payload(msg.payload, payload_format, p_key, encode)
                current_values[col_idx] = val
            row = list(current_values) if push_mode else None
        if push_mode:
            push_queue.put(row)

    def _build_client() -> mqtt.Client:
        return mqtt_util.build_mqtt_client(
            client_id=client_id,
            username=username,
            password=password,
            tls_enable=tls_enable,
            tls_ca_cert=tls_ca_cert,
            tls_certfile=tls_certfile,
            tls_keyfile=tls_keyfile,
            tls_insecure=tls_insecure,
            on_connect=_on_connect,
            on_disconnect=_on_disconnect,
            on_message=_on_message,
        )

    def _responsive_wait(seconds: float) -> None:
        """stop_event を見ながら細かく待つ"""
        end = time.time() + seconds
        while time.time() < end and not stop_event.is_set():
            time.sleep(min(0.05, max(0.0, end - time.time())))

    """======== 組込みブローカー起動 ========"""
    if broker_mode == "embedded":
        broker_host = "127.0.0.1"
        broker_ready = threading.Event()
        mqtt_util.start_embedded_broker(
            port=broker_port,
            allow_anonymous=embedded_allow_anonymous,
            broker_ready=broker_ready,
            stop_event=stop_event,
        )
        # ブローカーが起動するまで最大 15 秒待つ
        if not broker_ready.wait(timeout=15.0):
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Embedded broker did not start within 15s. Continuing anyway.", log_file)
        else:
            time.sleep(0.5)  # ポート確立の安定待ち

    """======== 接続ヘルパー ========"""
    mqtt_client: Optional[mqtt.Client] = None
    fail_count = 0   # 連続失敗の回数（判定に使う。このトリガーの中だけで数える）

    # 接続の状態（判定・リトライはこのトリガー。記録・再接続の間隔・通知は ConnectState）
    conn_state = ConnectState(TRIGGER_NAME, f"{broker_host}:{broker_port}", reconnect_interval_sec, MODULE_NAME, log_file)

    def _drop_client() -> None:
        """クライアントを止めて切断する"""
        nonlocal mqtt_client, link_up
        mqtt_util.teardown_client(mqtt_client)
        mqtt_client = None
        link_up     = False

    def _try_connect() -> Optional[BaseException]:
        """接続を1回試みる（CONNACK を待つ。購読は _on_connect が行う）。成功なら None、失敗ならその例外"""
        nonlocal mqtt_client
        _drop_client()
        try:
            mqtt_client = _build_client()
            connack_event.clear()
            mqtt_client.connect(broker_host, broker_port, keepalive)
            mqtt_client.loop_start()
            if not connack_event.wait(timeout=CONNACK_TIMEOUT_SEC):
                raise TimeoutError(f"CONNACK Timeout. {broker_host}:{broker_port}")
            if not link_up:
                raise ConnectionRefusedError(f"Connect REFUSED (rc!=0). {broker_host}:{broker_port}")
            return None
        except Exception as e:
            _drop_client()
            return e

    def _count_fail(e: BaseException, what: str) -> bool:
        """
        失敗を1回数える。連続 retry_max 回に達したら not_connectable と判定して ConnectState へ報告し、False を返す
        （リトライを続けるなら retry_wait_sec 待って True を返す。リトライは画面のみに出し、ConnectState・ErrorIF へは報告しない）
        """
        nonlocal fail_count
        fail_count += 1
        if fail_count >= retry_max:
            conn_state.set_not_connectable(e)   # 判定：連続 retry_max 回の失敗
            return False
        if stop_event.is_set():
            return False
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Retry ({fail_count}/{retry_max}). {what}  "
            f"{broker_host}:{broker_port}  ({type(e).__name__}: {e})")
        _responsive_wait(retry_wait_sec)
        return True

    def _recover_link() -> None:
        """接続できる状態で切断を検知したら、retry_wait_sec ごとに接続し直す"""
        nonlocal fail_count
        while conn_state.is_connectable() and (mqtt_client is None or not link_up) and not stop_event.is_set():
            e = _try_connect()
            if e is None:
                fail_count = 0
                return
            if not _count_fail(e, "Link Lost."):
                return

    def _reconnect_if_due() -> None:
        """接続できない間、再接続の間隔が過ぎていれば再接続を試み、結果を ConnectState へ報告する"""
        nonlocal fail_count
        if conn_state.should_try_reconnect():
            e = _try_connect()
            if e is None:
                fail_count = 0
                conn_state.set_connectable()
            else:
                conn_state.reconnect_failed(e)

    """======== 初期接続（retry_wait_sec ごとに retry_max 回まで試みる） ========"""
    _init_conn_err: Optional[BaseException] = None
    for _try in range(1, retry_max + 1):
        _init_conn_err = _try_connect()
        if _init_conn_err is None:
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} MQTT Client Connected. {broker_host}:{broker_port}", log_file)
            break
        if _try < retry_max and not stop_event.is_set():
            base.output_log(MODULE_NAME,       # リトライは画面のみ
                f"{TRIGGER_NAME} Connect Retry ({_try}/{retry_max}). {broker_host}:{broker_port}  "
                f"({type(_init_conn_err).__name__}: {_init_conn_err})")
            _responsive_wait(retry_wait_sec)

    if _init_conn_err is not None:
        _sig = ErrorIF.report_exception(_init_conn_err, phase="startup", category="comm",  # 初回接続の失敗を報告（A-21）
                                         trigger_id=TRIGGER_NAME, log_file=log_file)
        if _sig == SIGNAL_STOP:
            stop_event.set()
            return None
        conn_state.set_not_connectable(_init_conn_err, report=False)   # ErrorIF へは上で phase=startup として通知済み

    # null 行を先行送出 — stream_core の初期状態認識用
    queue_out.put((0, [None] * n_col_def))

    """======== MAIN LOOP ========"""
    count = 1

    while not stop_event.is_set():
        try:
            """---- 接続できない間：再接続の間隔ごとに再接続を試みる（値は送らない） ----"""
            if not conn_state.is_connectable():
                _reconnect_if_due()
                if not conn_state.is_connectable():
                    ErrorIF.emit_summary_if_needed()
                    _responsive_wait(min(2.0, reconnect_interval_sec))
                    continue

            """---- 接続できる間に切断を検知したら、その場で接続し直す（リトライ） ----"""
            if mqtt_client is None or not link_up:
                _recover_link()
                continue

            """---- ONLINE (push_mode) ----"""
            if push_mode:
                try:
                    row = push_queue.get(timeout=polling_ms / 1000)
                except queue.Empty:
                    continue

                row_casted, cast_fails = base.cast_dtype(row, cast_plan)
                if cast_fails > 0:
                    ErrorIF.report_event({
                        "signature": "CAST_FAIL",
                        "phase": "loop", "category": "data",
                        "trigger_id": TRIGGER_NAME,
                        "message": f"Cast failed {cast_fails} cols.",
                        "log_file": log_file,
                    })
                queue_out.put((count, row_casted))
                count += 1
                continue

            """---- ONLINE (polling_mode) ----"""
            with current_lock:
                row = list(current_values)

            row_casted, cast_fails = base.cast_dtype(row, cast_plan)
            if cast_fails > 0:
                ErrorIF.report_event({
                    "signature": "CAST_FAIL",
                    "phase": "loop", "category": "data",
                    "trigger_id": TRIGGER_NAME,
                    "message": f"Cast failed {cast_fails} cols.",
                    "log_file": log_file,
                })
            queue_out.put((count, row_casted))
            count += 1
            _responsive_wait(polling_ms / 1000)

        except (ConnectionError, OSError) as e:
            # 通信の失敗：接続を切って失敗に数える（次の周回で接続し直す。連続 retry_max 回で not_connectable）
            _drop_client()
            if conn_state.is_connectable():
                _count_fail(e, "Connection Lost.")
            continue

        except Exception as e:
            _sig = ErrorIF.report_exception(
                e, phase="loop", category="data",
                trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue

    """======== CLEANUP ========"""
    _drop_client()
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
