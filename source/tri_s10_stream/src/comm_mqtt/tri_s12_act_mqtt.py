# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [MQTT Publish]
# Ver: 02.b.20261005    By: IWATA, Y.'s Agent AI
# Mod: 統計まわりを init_stats / calc_stats / export_out_coldef に集約（出力内容は従来どおり。B-9）。初回接続の失敗を ErrorIF へ報告（phase=startup / category=comm。A-21）
########################################
a_mqtt_pub_seg_trigger    # セグメントの全行を JSON 形式で MQTT トピックへ Publish する
a_mqtt_pub_stats_trigger  # セグメントの統計量を算出し、1件を JSON 形式で MQTT トピックへ Publish する

動作概要
    1. broker_mode="embedded" の場合は tri_s19_mqtt.start_embedded_broker で
       amqtt ブローカーをバックグラウンドスレッドで自動起動する
    2. tri_s19_mqtt.build_mqtt_client で paho-mqtt クライアントを生成・接続する
    3. post_queue からセグメントを受信し、項目数の整合性を確認する
    4. a_mqtt_pub_seg_trigger   : セグメントの全行を1行ずつ JSON 形式で逐次 Publish する
       a_mqtt_pub_stats_trigger : build_stats() で統計量を算出して 1 レコードを Publish する
    5. 接続断時は is_online=False に移行し reconnect_interval_sec 秒ごとに再接続を試みる
    6. skip_on_offline=True の場合はオフライン中のセグメントをスキップしてキューを消費する
    7. stop_event またはキュー Sentinel で終了し、クライアントをクリーンアップする

接続設定（共通）
    broker_mode            : "external"（外部ブローカー）or "embedded"（組込みブローカー）
                             デフォルト: "external"
    broker_host            : 接続先ブローカーホスト（broker_mode="external" 時。デフォルト: "127.0.0.1"）
    broker_port            : MQTT ブローカーポート（デフォルト: 1883）
    client_id              : クライアントID（省略時は自動生成）
    username               : 認証ユーザー名（省略可）
    password               : 認証パスワード（省略可）
    keepalive              : キープアライブ秒（デフォルト: 60）
    reconnect_interval_sec : 再接続インターバル秒（デフォルト: 10.0）
    skip_on_offline        : オフライン時にセグメントをスキップするか（デフォルト: true）
    tls_enable             : TLS 接続を有効にするか（デフォルト: false）
    tls_ca_cert            : TLS CA 証明書パス（システムルート相対）
    tls_certfile           : TLS クライアント証明書パス（システムルート相対。省略可）
    tls_keyfile            : TLS クライアント秘密鍵パス（システムルート相対。省略可）
    tls_insecure           : サーバ証明書のホスト検証を無効化（デフォルト: false）

組込みブローカー設定（broker_mode="embedded"）
    broker_port              : 組込みブローカーの待ち受けポート（デフォルト: 1883）
    embedded_allow_anonymous : 匿名接続を許可するか（デフォルト: true）

Publish 設定（共通）
    publish_topic  : 送信先 MQTT トピック（必須）
    qos            : QoS レベル（0/1/2, デフォルト: 0）
    retain         : retain フラグ（デフォルト: false）
    encode         : JSON エンコーディング（デフォルト: "utf-8"）
    publish_mode   : a_mqtt_pub_seg_trigger（mode="seg"）専用。デフォルト: "row"
                       "row"    : セグメントの各行を1メッセージずつPublishする（従来通り）
                       "segment": セグメント全体を1メッセージ（JSON配列）にまとめてPublishする
                                  {"seg_id":..., "row_count":..., "rows":[{...},{...},...]}
                                  公開ブローカー等、大量の短時間連続Publishでメッセージが
                                  欠落しやすい環境では、こちらの方が安全（メッセージ数が
                                  1/セグメント行数に減るため）
    marker_row_enable: publish_mode="segment" 専用。デフォルト: true
                     rows配列の末尾に「全列"____"」の終端マーカー行を1件追加する。
                     受信側（find_mqtt.py の payload_format="json_rows"）は特別な処理をしなくても、
                     他の行と同じように展開されるだけで、即座にセグメント終了を検出できるように
                     なる（find_mqtt.py側の変更は不要）。falseにすると、次のセグメントの
                     先頭行が届くまで終点を検出できなくなる

a_mqtt_pub_stats_trigger 専用設定
    column_register_map : 統計量算出用の列マッピング定義（必須）
    out_coldef          : 統計量の出力列定義（必須）
    save_dir            : out_coldef を保存するディレクトリ（デフォルト: "output"）
"""
MODULE_NAME = "ACT_MQTT"

"""standard library"""
from   datetime import datetime as dt
import json
import os
import queue
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Optional

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
import src.tri_s01_error_if as err
from   src.tri_s01_error_if import ErrorIF, SIGNAL_STOP
from   src.tri_s10_stream_wave_stat import init_stats, calc_stats, export_out_coldef  # Calc Stats（Act 共通。B-9）
import src.comm_mqtt.tri_s19_mqtt as mqtt_util

"""system dir, file"""
system_root = base.get_system_root()
log_dir     = base.get_log_dir()
log_file    = base.get_log_path()
work_dir       = base.get_work_dir()
work_db_dir    = base.get_work_sub_dirs().get("db"   , os.path.join(work_dir, "db"))
work_run_dir   = base.get_work_sub_dirs().get("run"  , os.path.join(work_dir, "run"))
work_spool_dir = base.get_work_sub_dirs().get("spool", os.path.join(work_dir, "spool"))

base.output_log(MODULE_NAME, f"system_root={system_root}", log_file)
base.output_log(MODULE_NAME, f"log_file={log_file}", log_file)
if __name__ == '__main__':
    base.output_log(MODULE_NAME, f"work_dir={work_dir}", log_file)


########################################
# 共通 Publish ループ（a_mqtt_pub_seg_trigger / a_mqtt_pub_stats_trigger の実体）
########################################

def _run_pub_trigger(
    mode      : str,  # "seg" または "stats"
    post_queue: queue.Queue,
    col_def   : Dict[int, Any],
    config    : Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """MQTT ブローカーへセグメントを Publish する共通ループ。mode で送信内容だけが変わる"""
    TRIGGER_NAME = "a_mqtt_pub_seg_trigger" if mode == "seg" else "a_mqtt_pub_stats_trigger"
    _SIG_OFFLINE = f"ConnectionFailed.comm.loop.{TRIGGER_NAME}"

    """======== STARTUP ========"""
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start. mode={mode}", log_file)

        # データ項目定義
        n_col_def = len(col_def)
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_keys={col_keys}", log_file)

        # 統計マップ（stats モードのみ）
        if mode == "stats":
            stats_ctx    = init_stats(config, col_def, MODULE_NAME, TRIGGER_NAME)

            # out_coldef を保存
            save_dir = os.path.join(system_root, config.get("save_dir", "output"))
            export_out_coldef(stats_ctx.out_coldef,
                os.path.join(save_dir, f"output_column_define_{TRIGGER_NAME}.json"), "utf-8", MODULE_NAME)

        # ブローカー設定
        broker_mode   = str(config.get("broker_mode", "external")).lower()
        broker_host   = str(config.get("broker_host", "127.0.0.1"))
        broker_port   = int(config.get("broker_port", 1883))
        client_id     = str(config.get("client_id", "")) or None
        username      = config.get("username")
        password      = config.get("password")
        keepalive     = int(config.get("keepalive", 60))
        reconnect_interval_sec = float(config.get("reconnect_interval_sec", 10.0))
        skip_on_offline = bool(config.get("skip_on_offline", True))

        if broker_mode not in ("external", "embedded"):
            raise ValueError(
                f"broker_mode must be 'external' or 'embedded'. Got: {broker_mode}")

        # TLS 設定
        tls_enable   = bool(config.get("tls_enable", False))
        tls_ca_cert  = config.get("tls_ca_cert")
        tls_certfile = config.get("tls_certfile")
        tls_keyfile  = config.get("tls_keyfile")
        tls_insecure = bool(config.get("tls_insecure", False))

        if tls_ca_cert:
            tls_ca_cert  = base.get_absolute_path(tls_ca_cert)
        if tls_certfile:
            tls_certfile = base.get_absolute_path(tls_certfile)
        if tls_keyfile:
            tls_keyfile  = base.get_absolute_path(tls_keyfile)

        # 組込みブローカー設定
        embedded_allow_anonymous = bool(config.get("embedded_allow_anonymous", True))

        # Publish 設定
        publish_topic = str(config.get("publish_topic", ""))
        if not publish_topic:
            raise ValueError("publish_topic is required")
        qos    = int(config.get("qos", 0))
        retain = bool(config.get("retain", False))
        encode = str(config.get("encode", "utf-8"))

        # publish_mode（mode="seg" 時のみ有効）
        #   "row"    : セグメントの各行を1メッセージずつPublishする（従来通り・デフォルト）
        #   "segment": セグメント全体を1メッセージ（JSON配列）にまとめてPublishする
        #              （公開ブローカー等、大量の短時間連続Publishでメッセージが欠落しやすい
        #              環境向け。メッセージ数を1/N（N=セグメント行数）に減らせる）
        publish_mode = str(config.get("publish_mode", "row")).lower()
        if mode == "seg" and publish_mode not in ("row", "segment"):
            raise ValueError(f"publish_mode must be 'row' or 'segment'. Got: {publish_mode}")

        # marker_row_enable（publish_mode="segment" 時のみ有効。デフォルト: true）
        #   rows配列の末尾に「全列"____"」の終端マーカー行を1件追加する。
        #   受信側（find_mqtt.py の payload_format="json_rows"）は特別な処理をしなくても、
        #   他の行と同じように展開されるだけで、即座にセグメント終了を検出できるようになる
        #   （find_mqtt.py側の変更は不要）。falseにすると、次のセグメントの先頭行が届くまで
        #   終点を検出できなくなる
        marker_row_enable = bool(config.get("marker_row_enable", True))

        base.output_log(MODULE_NAME,
            f"broker_mode={broker_mode}  broker_host={broker_host}  broker_port={broker_port}  "
            f"publish_topic={publish_topic}  qos={qos}  retain={retain}  "
            f"publish_mode={publish_mode}  marker_row_enable={marker_row_enable}  "
            f"skip_on_offline={skip_on_offline}  reconnect_interval_sec={reconnect_interval_sec}", log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config",
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== 共有オンライン状態 ========"""
    is_online     = False
    connack_event = threading.Event()

    """======== HELPER ========"""

    def _on_connect(client, userdata, flags, rc):
        nonlocal is_online
        if rc == 0:
            is_online = True
            connack_event.set()
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Connected to broker. {broker_host}:{broker_port}", log_file)
        else:
            connack_event.set()
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Connect failed. rc={rc}", log_file)

    def _on_disconnect(client, userdata, rc):
        if rc != 0:
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Unexpected disconnect. rc={rc}", log_file)

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
        )

    def _responsive_wait(seconds: float) -> None:
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
        if not broker_ready.wait(timeout=15.0):
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Embedded broker did not start within 15s. Continuing anyway.", log_file)
        else:
            time.sleep(2.0)

    """======== 初期接続 ========"""
    mqtt_client: Optional[mqtt.Client] = None
    _init_conn_err: Optional[Exception] = None
    try:
        mqtt_client = _build_client()
        connack_event.clear()
        mqtt_client.connect(broker_host, broker_port, keepalive)
        mqtt_client.loop_start()
        if not connack_event.wait(timeout=10.0):
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Initial Connect CONNACK Timeout. Go Offline.", log_file)
            mqtt_util.teardown_client(mqtt_client)
            mqtt_client = None
            is_online = False
            _init_conn_err = TimeoutError(f"Initial Connect CONNACK Timeout. {broker_host}:{broker_port}")
        elif not is_online:
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Initial Connect REFUSED (rc!=0). Go Offline.", log_file)
            mqtt_util.teardown_client(mqtt_client)
            mqtt_client = None
            _init_conn_err = ConnectionRefusedError(f"Initial Connect REFUSED (rc!=0). {broker_host}:{broker_port}")
    except Exception as e:
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Initial Connect FAILED. "
            f"({type(e).__name__}: {e})  Go Offline.", log_file)
        mqtt_util.teardown_client(mqtt_client)
        mqtt_client = None
        _init_conn_err = e

    """初回接続の失敗を報告（タイムアウト・拒否・例外のいずれも。A-21）"""
    if _init_conn_err is not None:
        _sig = ErrorIF.report_exception(_init_conn_err, phase="startup", category="comm",
                                        trigger_id=TRIGGER_NAME, log_file=log_file)
        if _sig == SIGNAL_STOP:
            stop_event.set()
            return None
        is_online = False

    """======== LOOP START ========"""
    reconnect_fail_count = 0
    last_reconnect_try   = time.time()
    skipped_seg_count    = 0

    while True:

        """キュー受信"""
        try:
            segment = post_queue.get(timeout=1.0)

        except queue.Empty:
            # オフライン中は再接続を試みる
            if not is_online and (time.time() - last_reconnect_try) >= reconnect_interval_sec:
                last_reconnect_try = time.time()
                try:
                    mqtt_util.teardown_client(mqtt_client)
                    mqtt_client = None
                    mqtt_client = _build_client()
                    connack_event.clear()
                    mqtt_client.connect(broker_host, broker_port, keepalive)
                    mqtt_client.loop_start()
                    if not (connack_event.wait(timeout=10.0) and is_online):
                        raise ConnectionError("CONNACK not received or connection refused")
                    reconnect_fail_count = 0
                    base.output_log(MODULE_NAME,
                        f"{TRIGGER_NAME} Reconnected. {broker_host}:{broker_port}", log_file)
                except Exception as e:
                    reconnect_fail_count += 1
                    mqtt_util.teardown_client(mqtt_client)
                    mqtt_client = None
                    is_online = False
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
                    base.output_log(MODULE_NAME,
                        f"{TRIGGER_NAME} Re-Connect Failed ({reconnect_fail_count}). "
                        f"{broker_host}:{broker_port}  ({type(e).__name__}: {e})", log_file)

            if (stop_event.is_set() and
                    post_queue.qsize() == 0 and
                    getattr(post_queue, "unfinished_tasks", 0) == 0):
                break
            continue

        """sentinel → 終了"""
        if segment is None:
            post_queue.task_done()
            break

        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} ...Received Queue.")

        """セグメント処理"""
        try:
            """項目の整合性チェック"""
            n_col_act = len(segment[0])
            if n_col_act != n_col_def:
                base.output_log(MODULE_NAME,
                    f"Segment Columns NOT Match: seg_id={segment[0][0]}  "
                    f"Expected:{n_col_def}  Actual:{n_col_act}", log_file)
                continue

            seg_id = segment[0][0]

            """終点行（変化後）の除外は core が行う（CUT_LAST_ROW = True）"""

            """空セグメントチェック"""
            if len(segment) == 0:
                base.output_log(MODULE_NAME,
                    f"WARNING  空セグメント。スキップ。 seg_id={seg_id}", log_file)
                continue

            """空レコードチェック（find_mqtt.py が起動時に送る先行null行等、値がNoneのプレースホルダ行を弾く。
            旧実装は isinstance(segment[0][-1], list) を見ていたが、segment[0][-1]はスカラー値であり
            リストにはならないため、この判定が一度も成立しない不具合があった）"""
            if segment[0][-1] is None:
                base.output_log(MODULE_NAME,
                    f"WARNING  空レコード。スキップ。 seg_id={seg_id}", log_file)
                continue

            """==== オフライン時の処理 ===="""
            if not is_online:
                if skip_on_offline:
                    skipped_seg_count += 1
                    base.output_log(MODULE_NAME,
                        f"WARNING  OFFLINE - Segment Skipped. "
                        f"seg_id={seg_id}  total_skipped={skipped_seg_count}  "
                        f"{broker_host}:{broker_port}", log_file)
                    continue

                else:
                    """ブロック待ち（停止チェック付き）"""
                    base.output_log(MODULE_NAME,
                        f"{TRIGGER_NAME} OFFLINE - Waiting for reconnect... seg_id={seg_id}", log_file)
                    while not is_online and not stop_event.is_set():
                        try:
                            mqtt_util.teardown_client(mqtt_client)
                            mqtt_client = None
                            mqtt_client = _build_client()
                            connack_event.clear()
                            mqtt_client.connect(broker_host, broker_port, keepalive)
                            mqtt_client.loop_start()
                            if not (connack_event.wait(timeout=10.0) and is_online):
                                raise ConnectionError("CONNACK not received or connection refused")
                            reconnect_fail_count = 0
                            base.output_log(MODULE_NAME,
                                f"{TRIGGER_NAME} Reconnected. {broker_host}:{broker_port}", log_file)
                        except Exception as e:
                            reconnect_fail_count += 1
                            mqtt_util.teardown_client(mqtt_client)
                            mqtt_client = None
                            is_online = False
                            base.output_log(MODULE_NAME,
                                f"{TRIGGER_NAME} Re-Connect Failed ({reconnect_fail_count}). "
                                f"Retry in {reconnect_interval_sec}s...", log_file)
                            _responsive_wait(reconnect_interval_sec)

                    if stop_event.is_set():
                        break

            """==== Publish ===="""
            sent_count = 0
            send_error = False

            # ==== mode="seg": 全行 Publish ====
            if mode == "seg" and publish_mode == "row":
                for row_num, row_val in enumerate(segment, start=1):
                    row_dict = {col_keys[i]: row_val[i] for i in range(len(col_keys))}
                    payload  = json.dumps(row_dict, ensure_ascii=False, default=str)

                    try:
                        result = mqtt_client.publish(publish_topic, payload.encode(encode), qos=qos, retain=retain)
                        if result.rc != 0:
                            raise OSError(
                                f"publish() failed. rc={result.rc}  "
                                f"seg_id={seg_id}  row={row_num}")
                        sent_count += 1

                    except Exception as e:
                        _sig = ErrorIF.report_exception(
                            e, phase="loop", category="comm",
                            trigger_id=TRIGGER_NAME, log_file=log_file)
                        mqtt_util.teardown_client(mqtt_client)
                        mqtt_client = None
                        is_online            = False
                        reconnect_fail_count = 0
                        last_reconnect_try   = time.time()
                        base.output_log(MODULE_NAME,
                            f"{TRIGGER_NAME} Connection Lost. to Offline. "
                            f"seg_id={seg_id}  sent={sent_count}/{len(segment)}", log_file)
                        if _sig == SIGNAL_STOP:
                            stop_event.set()
                        send_error = True
                        break

            # ==== mode="seg" かつ publish_mode="segment": セグメント全体を1メッセージでPublish ====
            elif mode == "seg" and publish_mode == "segment":
                row_dicts = [
                    {col_keys[i]: row_val[i] for i in range(len(col_keys))}
                    for row_val in segment
                ]

                """終端マーカー行（marker_row_enable時のみ）
                1メッセージにまとめて送ると、受信側（find_mqtt.py）は次のセグメントの
                先頭行が届くまでLOTの変化に気づけず、今のセグメントの終点を検出できない。
                そこで、rows配列の末尾に「全列"____"」の行を1件追加しておく
                （tri_s11_find_text_csv.py等と同じ"____"境界行の考え方）。
                find_mqtt.py側は特別な処理をしなくても、他の行と同じように展開されるだけで
                自然に終点検出のトリガーとして働く（find_mqtt.py側の変更は不要）"""
                if marker_row_enable:
                    row_dicts.append({k: "____" for k in col_keys})

                seg_payload = {"seg_id": seg_id, "row_count": len(row_dicts), "rows": row_dicts}
                payload = json.dumps(seg_payload, ensure_ascii=False, default=str)

                try:
                    result = mqtt_client.publish(publish_topic, payload.encode(encode), qos=qos, retain=retain)
                    if result.rc != 0:
                        raise OSError(f"publish() failed. rc={result.rc}  seg_id={seg_id}")
                    sent_count = len(row_dicts)  # ログ上は「まとめて送った行数」として扱う（マーカー行含む）

                except Exception as e:
                    _sig = ErrorIF.report_exception(
                        e, phase="loop", category="comm",
                        trigger_id=TRIGGER_NAME, log_file=log_file)
                    mqtt_util.teardown_client(mqtt_client)
                    mqtt_client = None
                    is_online            = False
                    reconnect_fail_count = 0
                    last_reconnect_try   = time.time()
                    base.output_log(MODULE_NAME,
                        f"{TRIGGER_NAME} Connection Lost. to Offline. "
                        f"seg_id={seg_id}  (segment mode, 1 message for {len(segment)} rows)", log_file)
                    if _sig == SIGNAL_STOP:
                        stop_event.set()
                    send_error = True

            # ==== mode="stats": 統計量 1 件 Publish ====
            else:  # mode == "stats"
                stats_dict = calc_stats(stats_ctx, segment, seg_id)
                payload    = json.dumps(stats_dict, ensure_ascii=False, default=str)

                try:
                    result = mqtt_client.publish(publish_topic, payload.encode(encode), qos=qos, retain=retain)
                    if result.rc != 0:
                        raise OSError(
                            f"publish() failed. rc={result.rc}  seg_id={seg_id}")
                    sent_count = 1

                except Exception as e:
                    _sig = ErrorIF.report_exception(
                        e, phase="loop", category="comm",
                        trigger_id=TRIGGER_NAME, log_file=log_file)
                    mqtt_util.teardown_client(mqtt_client)
                    mqtt_client = None
                    is_online            = False
                    reconnect_fail_count = 0
                    last_reconnect_try   = time.time()
                    base.output_log(MODULE_NAME,
                        f"{TRIGGER_NAME} Connection Lost. to Offline. "
                        f"seg_id={seg_id}  (stats mode)", log_file)
                    if _sig == SIGNAL_STOP:
                        stop_event.set()
                    send_error = True

            if not send_error:
                _msg_count = 1 if (mode == "seg" and publish_mode == "segment") else sent_count
                base.output_log(MODULE_NAME,
                    f"{TRIGGER_NAME} Segment Published. "
                    f"seg_id={seg_id}  mode={mode}  publish_mode={publish_mode if mode == 'seg' else '-'}  "
                    f"rows_sent={sent_count}  original_rows={len(segment)}  mqtt_messages={_msg_count}", log_file)

        except Exception as e:
            _sig = ErrorIF.report_exception(e, phase="loop", category="data",
                                            trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                break
            continue

        finally:
            post_queue.task_done()

    """======== LOOP END ========"""
    mqtt_util.teardown_client(mqtt_client)

    if skipped_seg_count > 0:
        base.output_log(MODULE_NAME,
            f"WARNING  {TRIGGER_NAME} Stopped with {skipped_seg_count} segment(s) skipped due to OFFLINE.",
            log_file)

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None


########################################
# Act Trigger Functions（薄いラッパー）
########################################

def a_mqtt_pub_seg_trigger(
    post_queue: queue.Queue,
    col_def   : Dict[int, Any],
    config    : Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """セグメントの全行を JSON 形式で MQTT トピックへ Publish する"""
    return _run_pub_trigger("seg", post_queue, col_def, config, stop_event)

a_mqtt_pub_seg_trigger.QMODE = "imq"
a_mqtt_pub_seg_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


def a_mqtt_pub_stats_trigger(
    post_queue: queue.Queue,
    col_def   : Dict[int, Any],
    config    : Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """セグメントの統計量を算出し、1件を JSON 形式で MQTT トピックへ Publish する"""
    return _run_pub_trigger("stats", post_queue, col_def, config, stop_event)

a_mqtt_pub_stats_trigger.QMODE = "imq"
a_mqtt_pub_stats_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, "Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
