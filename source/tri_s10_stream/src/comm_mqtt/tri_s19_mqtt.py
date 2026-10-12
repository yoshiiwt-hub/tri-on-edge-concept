# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# MQTT 通信ユーティリティモジュール
# Ver: 01.e.20261001    By: IWATA, Y.'s Agent AI
# Mod: モジュール名を tri_edge_XX_* から tri_sXX_* 形式に変更（tri_edge_19_mqtt → tri_s19_mqtt）
########################################
start_embedded_broker  # amqtt 組込みブローカーをバックグラウンドスレッドで起動する
build_mqtt_client      # paho-mqtt クライアントを生成・設定して返す（v1.x/v2.x 両対応）
parse_payload          # MQTT ペイロードバイト列を設定に従いデコード・パースする
teardown_client        # paho クライアントのループを停止して切断する

動作概要
    find_trigger / act_trigger が共有する MQTT 通信処理のユーティリティ集。
    1. start_embedded_broker : amqtt ブローカーを asyncio スレッドで起動し、
       broker_ready イベントが立つまで待てるようにする
    2. build_mqtt_client     : paho-mqtt v1.x / v2.x の差異を吸収しつつ、
       TLS・認証・コールバックを設定した Client を返す
    3. parse_payload         : JSON / raw の 2 フォーマットに対応し、
       payload_key 指定時は JSON オブジェクトから値を抽出する
    4. teardown_client       : loop_stop → disconnect を安全に実行する

接続設定パラメータ（build_mqtt_client）
    client_id  : クライアント ID（空文字列で自動生成）
    username   : 認証ユーザー名（None で認証なし）
    password   : 認証パスワード
    tls_enable : TLS を有効にするか（デフォルト: False）
    tls_ca_cert: CA 証明書の絶対パス（tls_enable=True 時に使用）
    tls_certfile: クライアント証明書の絶対パス（省略可）
    tls_keyfile : クライアント秘密鍵の絶対パス（省略可）
    tls_insecure: サーバ証明書のホスト検証を無効化（デフォルト: False）
    on_connect    : on_connect コールバック（省略可）
    on_disconnect : on_disconnect コールバック（省略可）
    on_message    : on_message コールバック（省略可）

parse_payload パラメータ
    raw_bytes      : MQTT ペイロードのバイト列
    payload_format : "json" または "raw"
    payload_key    : JSON から抽出するキー名（省略可）
    encode         : 文字エンコーディング（デフォルト: "utf-8"）
"""
MODULE_NAME = "UTIL_MQTT"

"""standard library"""
import asyncio
from   datetime import datetime as dt
import json
import os
import sys
import threading
import traceback
from   typing   import Any, Callable, Optional

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

"""system dir, file"""
system_root = base.get_system_root()
log_file    = base.get_log_path()

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)


########################################
# Embedded Broker (amqtt)
########################################

def start_embedded_broker(
    port           : int,
    allow_anonymous: bool,
    broker_ready   : threading.Event,
    stop_event     : threading.Event,
) -> threading.Thread:
    """
    amqtt ブローカーをバックグラウンドスレッドで起動する。

    Args:
        port            : MQTT 待受ポート番号
        allow_anonymous : 匿名接続を許可するか
        broker_ready    : ブローカー起動完了時にセットするイベント
        stop_event      : セットされるとブローカーをシャットダウンして終了する

    Returns:
        起動した Thread インスタンス（daemon=True）
    """
    def _thread_func():
        try:
            from amqtt.broker import Broker
        except ImportError as e:
            base.output_log(MODULE_NAME,
                f"amqtt not installed. Cannot start embedded broker. ({e})", log_file)
            broker_ready.set()  # タイムアウト待ちを解除するために set する
            return

        broker_config = {
            "listeners": {
                "default": {
                    "type": "tcp",
                    "bind": f"0.0.0.0:{port}",
                },
            },
            "plugins": {
                "amqtt.plugins.authentication.AnonymousAuthPlugin": {
                    "allow_anonymous": allow_anonymous,
                },
            },
        }

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def _run_broker():
            broker = Broker(broker_config)
            await broker.start()
            base.output_log(MODULE_NAME,
                f"Embedded MQTT Broker Started. port={port}", log_file)
            broker_ready.set()

            while not stop_event.is_set():
                await asyncio.sleep(0.2)

            await broker.shutdown()
            base.output_log(MODULE_NAME,
                f"Embedded MQTT Broker Stopped. port={port}", log_file)

        try:
            loop.run_until_complete(_run_broker())
        except Exception as e:
            base.output_log(MODULE_NAME,
                f"Embedded Broker Error: {type(e).__name__}: {e}", log_file)
            broker_ready.set()  # タイムアウト待ちを解除
        finally:
            loop.close()

    t = threading.Thread(target=_thread_func, daemon=True, name="MQTT-EmbeddedBroker")
    t.start()
    return t


########################################
# paho-mqtt Client Factory
########################################

def build_mqtt_client(
    client_id    : Optional[str]      = None,
    username     : Optional[str]      = None,
    password     : Optional[str]      = None,
    tls_enable   : bool               = False,
    tls_ca_cert  : Optional[str]      = None,
    tls_certfile : Optional[str]      = None,
    tls_keyfile  : Optional[str]      = None,
    tls_insecure : bool               = False,
    on_connect   : Optional[Callable] = None,
    on_disconnect: Optional[Callable] = None,
    on_message   : Optional[Callable] = None,
) -> mqtt.Client:
    """
    paho-mqtt クライアントを生成・設定して返す（v1.x / v2.x 両対応）。

    Args:
        client_id    : クライアント ID（None または "" で自動生成）
        username     : 認証ユーザー名（None で認証なし）
        password     : 認証パスワード
        tls_enable   : TLS を有効にするか
        tls_ca_cert  : CA 証明書の絶対パス
        tls_certfile : クライアント証明書の絶対パス（省略可）
        tls_keyfile  : クライアント秘密鍵の絶対パス（省略可）
        tls_insecure : サーバ証明書のホスト検証を無効化するか
        on_connect   : on_connect コールバック（省略可）
        on_disconnect: on_disconnect コールバック（省略可）
        on_message   : on_message コールバック（省略可）

    Returns:
        設定済みの mqtt.Client インスタンス
    """
    try:
        # paho-mqtt v2.x: CallbackAPIVersion が必要
        from paho.mqtt.client import CallbackAPIVersion
        c = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION1,
            client_id=client_id or "",
            clean_session=True,
        )
    except (ImportError, AttributeError):
        # paho-mqtt v1.x
        c = mqtt.Client(client_id=client_id or "", clean_session=True)

    if username:
        c.username_pw_set(username, password)

    if tls_enable:
        c.tls_set(
            ca_certs=tls_ca_cert,
            certfile=tls_certfile,
            keyfile=tls_keyfile,
        )
        if tls_insecure:
            c.tls_insecure_set(True)

    if on_connect    is not None: c.on_connect    = on_connect
    if on_disconnect is not None: c.on_disconnect = on_disconnect
    if on_message    is not None: c.on_message    = on_message

    return c


########################################
# Payload Parser
########################################

def parse_payload(
    raw_bytes     : bytes,
    payload_format: str            = "json",
    payload_key   : Optional[str]  = None,
    encode        : str            = "utf-8",
) -> Any:
    """
    MQTT ペイロードバイト列を設定に従いデコード・パースする。

    Args:
        raw_bytes     : MQTT ペイロードのバイト列
        payload_format: "json"（JSON パース）または "raw"（文字列そのまま）
        payload_key   : JSON から抽出するキー名
                        省略時は payload_format="json" でも JSON テキストをそのまま返す
        encode        : バイト列のデコードエンコーディング

    Returns:
        パース後の値（str / int / float / 任意の JSON 型 / None）
    """
    text = raw_bytes.decode(encode, errors="replace")
    if payload_format == "json" and payload_key is not None:
        try:
            obj = json.loads(text)
            return obj.get(payload_key) if isinstance(obj, dict) else None
        except Exception:
            return text
    return text  # raw、または payload_key 省略時（JSONテキストをそのまま返す）


########################################
# Client Teardown
########################################

def teardown_client(client: Optional[mqtt.Client]) -> None:
    """
    paho クライアントのネットワークループを停止し、ブローカーとの接続を切断する。

    Args:
        client: mqtt.Client インスタンス。None の場合は何もしない。
    """
    if client is None:
        return
    try:
        client.loop_stop()
    except Exception:
        pass
    try:
        client.disconnect()
    except Exception:
        pass
