# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Find Trigger [MQTT]
# Ver: 01.j.20261005    By: IWATA, Y.'s Agent AI
# Mod: 初回接続の失敗を ErrorIF へ報告（phase=startup / category=comm。A-21）
########################################
f_find_mqtt_trigger  # MQTT ブローカーからメッセージを受信し、キューに流す

■ 動作概要
    1. broker_mode が "embedded" の場合、amqtt ベースの組込み MQTT ブローカーを
       バックグラウンドスレッド（asyncio ループ）で起動する
    2. paho-mqtt クライアントでブローカーに接続し、topic_map で定義したトピックを Subscribe
    3. 受信メッセージで current_values の対応列を更新する（コールバック）
    4. polling_ms ごとに current_values の全列を queue_out に送出（ポーリングモード）
       push_mode=true の場合は受信のたびに即座に queue_out に送出
    5. 通信失敗時はオフラインモードへ移行し、reconnect_interval_sec 秒ごとに自動再接続
    6. stop_event が立つまでループを継続し、終了時にブローカー・クライアントをクリーンアップ

■ topic_map フォーマット
    {"col_idx": トピック指定}
    col_idx は column の index（0始点）に一致させること。トピック指定は2通り：
      文字列: "topic/string"
          従来形式。そのトピックの値をそのままこの列に格納する（payload_keyはグローバル設定を使う）
          例: {"0": "sensor/temp", "1": "sensor/humid"}  ← 1トピック=1列（値ごとに独立したトピック）
      辞書  : {"topic": "topic/string", "payload_key": "キー名"}
          1つのトピックに複数の値がJSONでまとまって届く場合、複数の col_idx から
          同じ topic を指定し、それぞれ異なる payload_key で値を取り出せる
          （payload_key省略時はグローバル設定を使う）
          例: セグメント全体を1トピックにJSONでPublishする相手（act_mqtt.pyのa_mqtt_pub_seg_trigger等）
              から受信する場合:
              {
                "0": {"topic": "triedge/segment/out", "payload_key": "Timestamp"},
                "1": {"topic": "triedge/segment/out", "payload_key": "LOT"},
                "2": {"topic": "triedge/segment/out", "payload_key": "Sin"}
              }
              ← 1トピック（設定・グループ単位）に複数列をまとめて割り当てる

■ payload フォーマット（payload_format）
    "json"      : JSON ペイロードをパース。payload_key を指定するとその値を抽出。
                  例: payload='{"value": 1.23}' + payload_key="value" → 1.23
                  payload_key 省略時は JSON テキストを文字列としてそのまま格納
    "raw"       : ペイロードをデコード文字列としてそのまま格納
    "json_rows" : 1メッセージに複数行がJSON配列でまとまっている形式を受信する
                  （act_mqtt.py の publish_mode="segment" で送られたメッセージ等）。
                  payload = {"rows": [{...}, {...}, ...]} または [{...}, {...}, ...]（直接配列）
                  rows_key（デフォルト"rows"）で配列を取り出すキー名を指定する。
                  配列の各要素（行オブジェクト）から、topic_mapのpayload_keyで指定した
                  フィールドを取り出し、1行ずつ順番にpush_queueへ送る。
                  ※ push_mode=true が必須（順序通りに個別処理する必要があるため）

■ 接続設定（共通）
    broker_mode            : "external"（外部ブローカー）or "embedded"（組込みブローカー）
                             デフォルト: "external"
    broker_host            : 接続先ブローカーホスト（broker_mode="external" 時。デフォルト: "127.0.0.1"）
    broker_port            : MQTT ブローカーポート（デフォルト: 1883）
    client_id              : クライアントID（省略時は自動生成）
    username               : 認証ユーザー名（省略可）
    password               : 認証パスワード（省略可）
    keepalive              : キープアライブ秒（デフォルト: 60）
    qos                    : QoS レベル（0/1/2, デフォルト: 0）
    reconnect_interval_sec : 再接続試行間隔秒（デフォルト: 10.0）
    tls_enable             : TLS 接続を有効にするか（デフォルト: false）
    tls_ca_cert            : TLS CA 証明書パス（システムルート相対。tls_enable=true 時に有効）
    tls_certfile           : TLS クライアント証明書パス（システムルート相対。省略可）
    tls_keyfile            : TLS クライアント秘密鍵パス（システムルート相対。省略可）
    tls_insecure           : サーバ証明書のホスト検証を無効化（デフォルト: false）

■ 組込みブローカー設定（broker_mode="embedded"）
    broker_port            : 組込みブローカーの待ち受けポート（デフォルト: 1883）
    embedded_allow_anonymous : 匿名接続を許可するか（デフォルト: true）

■ 受信設定
    topic_map              : 列インデックス（str） → トピック のマッピング（必須）
    polling_ms             : ポーリング間隔ミリ秒（デフォルト: 100）
    push_mode              : true=受信のたびに即時 queue_out 送出（デフォルト: false）
    payload_format         : "json" or "raw" or "json_rows"（デフォルト: "json"）
    payload_key            : JSON から抽出するキー名（省略可。json_rows時は各エントリの
                              payload_keyがそのまま行オブジェクト内のフィールド名になる）
    rows_key                : payload_format="json_rows" 時、配列を取り出すキー名（デフォルト: "rows"）
    encode                 : ペイロードのエンコーディング（デフォルト: "utf-8"）

■ dtype cast 設定（省略時は既定値）
    time_format            : 時刻文字列フォーマット（デフォルト: "%Y-%m-%d %H:%M:%S.%f"）
    time_default           : 時刻変換失敗時の値（デフォルト: "2999-01-01 00:00:00.000000"）
    int_default            : int 変換失敗時の値（デフォルト: -9999）
    float_default          : float 変換失敗時の値（デフォルト: -9999.9）
    str_default            : str 変換失敗時の値（デフォルト: "__"）
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
    """
    TRIGGER_NAME = "f_find_mqtt_trigger"
    _SIG_OFFLINE = f"ConnectionFailed.comm.loop.{TRIGGER_NAME}"

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

    """======== コールバック ========"""

    def _on_connect(client, userdata, flags, rc):
        nonlocal is_online
        if rc == 0:
            is_online = True
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Connected to broker. Subscribing {len(subscribe_list)} topic(s).", log_file)
            client.subscribe(subscribe_list)
        else:
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Connect failed. rc={rc}", log_file)

    def _on_disconnect(client, userdata, rc):
        nonlocal is_online
        is_online = False
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

    """======== 初期接続 ========"""
    mqtt_client: Optional[mqtt.Client] = None
    is_online = False
    try:
        mqtt_client = _build_client()
        mqtt_client.connect(broker_host, broker_port, keepalive)
        mqtt_client.loop_start()
        is_online = True
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} MQTT Client Connected. {broker_host}:{broker_port}", log_file)
    except Exception as e:
        _sig = ErrorIF.report_exception(e, phase="startup", category="comm",  # 初回接続の失敗を報告（A-21）
                                         trigger_id=TRIGGER_NAME, log_file=log_file)
        if _sig == SIGNAL_STOP:
            stop_event.set()
            return None
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Initial Connect FAILED. "
            f"({type(e).__name__}: {e})  Go Offline.", log_file)
        mqtt_util.teardown_client(mqtt_client)
        mqtt_client = None

    # null 行を先行送出 — stream_core の初期状態認識用
    queue_out.put((0, [None] * n_col_def))

    """======== MAIN LOOP ========"""
    reconnect_fail_count = 0
    last_reconnect_try   = time.time()
    count = 1

    while not stop_event.is_set():
        try:
            """---- OFFLINE ----"""
            if not is_online:
                if time.time() - last_reconnect_try >= reconnect_interval_sec:
                    last_reconnect_try = time.time()
                    try:
                        mqtt_util.teardown_client(mqtt_client)
                        mqtt_client = None
                        mqtt_client = _build_client()
                        mqtt_client.connect(broker_host, broker_port, keepalive)
                        mqtt_client.loop_start()
                        is_online            = True
                        reconnect_fail_count = 0
                        base.output_log(MODULE_NAME,
                            f"{TRIGGER_NAME} Re-Connected. Back to Online. "
                            f"{broker_host}:{broker_port}", log_file)

                    except Exception as e:
                        reconnect_fail_count += 1
                        mqtt_util.teardown_client(mqtt_client)
                        mqtt_client = None
                        ErrorIF.report_event({
                            "trigger_id": TRIGGER_NAME,
                            "phase"     : "loop",
                            "category"  : "comm",
                            "exc_type"  : type(e).__name__,
                            "message"   : (
                                f"Re-Connect Failed ({reconnect_fail_count} times). "
                                f"{broker_host}:{broker_port}"
                            ),
                            "signature" : _SIG_OFFLINE,
                        })

                ErrorIF.emit_summary_if_needed()
                time.sleep(2.0)
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
            _sig = ErrorIF.report_exception(
                e, phase="loop", category="comm",
                trigger_id=TRIGGER_NAME, log_file=log_file)
            mqtt_util.teardown_client(mqtt_client)
            mqtt_client = None
            is_online            = False
            reconnect_fail_count = 0
            last_reconnect_try   = time.time()
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Connection Lost. Go Offline. {broker_host}:{broker_port}", log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
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
    mqtt_util.teardown_client(mqtt_client)
    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    print(f"{MODULE_NAME} Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
