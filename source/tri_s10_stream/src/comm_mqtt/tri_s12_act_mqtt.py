# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [MQTT Publish]
# Ver: 02.e.20261108    By: IWATA, Y.'s Agent AI
# Mod: pyflakes の指摘を整理（使っていない import・変数、置き換えの無い f-string を削除。動作は変えない）
########################################
セグメントの値や統計量を、MQTT のトピックへ送り出す出口

主要関数:
a_mqtt_pub_seg_trigger    # セグメントの全行を JSON にして Publish する。
a_mqtt_pub_stats_trigger  # セグメントの統計量を求め、1件の JSON にして Publish する。
_run_pub_trigger          # 2つの trigger の共通ループ（送る内容だけが異なる）。

位置づけ: 生データを送る場合は seg、統計量だけを送る場合は stats を使う。通信の部品は tri_s19_mqtt、統計量の算出は tri_s10_stream_wave_stat を使う。受け側は tri_s11_find_mqtt で受け取れる。
設計意図: セグメント全体を1メッセージにまとめて送る形（publish_mode="segment"）を用意し、短時間に大量の Publish をするとメッセージが欠落しやすいブローカーでも安全に送れるようにする。まとめて送る場合は終端の行を付け、受け側がすぐにセグメントの終わりを判定できるようにする。接続・Publish に失敗したら、その場で接続し直して送り直し（リトライ）、続けて失敗したら接続できない（not_connectable）とする。接続できない状態の記録・再接続の間隔は tri_s01_connect_state に任せる。
前提・制約: 外部のブローカーが無い場合は、組込みブローカーを起動して使える。
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
from   src.tri_s01_error_if import ErrorIF, SIGNAL_STOP
from   src.tri_s01_connect_state import ConnectState
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
    """MQTT ブローカーへセグメントを Publish する共通ループ。mode で送信内容だけが変わる

    [動作の要点]
        リトライ: 接続（CONNACK が来ない・拒否）・Publish に失敗したら、retry_wait_sec ごとに接続し直して（CONNACK を待つ）、
            送れなかったメッセージから送り直す（このトリガーの中だけ）
            連続 retry_max 回失敗したら、接続できない（not_connectable）と判定して ConnectState へ報告する（送れたら回数を 0 に戻す）
            キューが空の間に切断を検知したときも、同じように接続し直す
        切断の検知: paho のコールバックはトリガー内の印（link_up）だけを変え、ConnectState への報告はトリガーのスレッドから行う
        再接続: 接続できない間は reconnect_interval_sec ごとに試み、結果を ConnectState へ報告する
        接続できない間に届いたセグメント: not_connectable_action = "skip"（送らずにスキップ）/ "wait"（再接続できるまで待つ）
        接続の状態（ConnectState が登録。ACT_MQTT）: connect_state / not_connectable_since / reconnect_fail_count / total_not_connectable
    """
    TRIGGER_NAME = "a_mqtt_pub_seg_trigger" if mode == "seg" else "a_mqtt_pub_stats_trigger"
    CONNACK_TIMEOUT_SEC = 10.0   # CONNACK を待つ時間

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
        retry_max              = int(config.get("retry_max", 3))
        retry_wait_sec         = float(config.get("retry_wait_sec", 1.0))
        not_connectable_action = str(config.get("not_connectable_action", "skip")).strip().lower()
        if retry_max < 1 or retry_wait_sec < 0:
            raise ValueError(f"config['retry_max'] must be >= 1 and config['retry_wait_sec'] >= 0. Got: {retry_max} / {retry_wait_sec}")
        if not_connectable_action not in ("skip", "wait"):
            raise ValueError(f"config['not_connectable_action'] must be 'skip' / 'wait'. Got: {not_connectable_action!r}")

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
            f"retry_max={retry_max}  retry_wait_sec={retry_wait_sec}  reconnect_interval_sec={reconnect_interval_sec}  "
            f"not_connectable_action={not_connectable_action}", log_file)

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config",
                                 trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== 接続の印（paho のコールバックが変える。ConnectState への報告はトリガーのスレッドから） ========"""
    link_up       = False   # CONNACK（rc=0）を受けて True、切断で False
    connack_event = threading.Event()

    """======== HELPER ========"""

    def _on_connect(client, userdata, flags, rc):
        nonlocal link_up
        if rc == 0:
            link_up = True
            connack_event.set()
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Connected to broker. {broker_host}:{broker_port}", log_file)
        else:
            connack_event.set()
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Connect failed. rc={rc}")   # 失敗の記録は判定側（ConnectState / ErrorIF）に任せ、ここは画面のみ

    def _on_disconnect(client, userdata, rc):
        nonlocal link_up
        link_up = False
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
        """接続を1回試みる（CONNACK を待つ）。成功なら None、失敗ならその例外"""
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

    def _publish_with_retry(payloads: List[bytes], seg_id: Any) -> bool:
        """
        メッセージを順に Publish する。接続・Publish に失敗したら retry_wait_sec ごとに接続し直して、
        送れなかったメッセージから送り直す（リトライ）。送れたら True、not_connectable になったら False
        """
        nonlocal fail_count
        done = 0
        while True:
            e = _try_connect() if (mqtt_client is None or not link_up) else None
            if e is None:
                try:
                    while done < len(payloads):
                        result = mqtt_client.publish(publish_topic, payloads[done], qos=qos, retain=retain)
                        if result.rc != 0:
                            raise OSError(
                                f"publish() failed. rc={result.rc}  seg_id={seg_id}  message={done + 1}/{len(payloads)}")
                        done += 1
                    fail_count = 0
                    return True
                except Exception as ex:
                    e = ex
                    _drop_client()
            if not _count_fail(e, f"seg_id={seg_id}  sent={done}/{len(payloads)}"):
                return False

    def _recover_link() -> None:
        """接続できる状態で切断を検知したら（キューが空の間）、retry_wait_sec ごとに接続し直す"""
        nonlocal fail_count
        while conn_state.is_connectable() and (mqtt_client is None or not link_up):
            e = _try_connect()
            if e is None:
                fail_count = 0
                return
            if not _count_fail(e, "Link Lost."):
                return

    def _reconnect_now() -> bool:
        """再接続を1回試み、結果を ConnectState へ報告する。成功なら True"""
        nonlocal fail_count
        e = _try_connect()
        if e is None:
            fail_count = 0
            conn_state.set_connectable()
            return True
        conn_state.reconnect_failed(e)
        return False

    def _reconnect_if_due() -> None:
        """接続できない間、再接続の間隔が過ぎていれば再接続を試みる"""
        if conn_state.should_try_reconnect():
            _reconnect_now()

    """======== 初期接続（retry_wait_sec ごとに retry_max 回まで試みる） ========"""
    _init_conn_err: Optional[BaseException] = None
    for _try in range(1, retry_max + 1):
        _init_conn_err = _try_connect()
        if _init_conn_err is None:
            break
        if _try < retry_max and not stop_event.is_set():
            base.output_log(MODULE_NAME,       # リトライは画面のみ
                f"{TRIGGER_NAME} Connect Retry ({_try}/{retry_max}). {broker_host}:{broker_port}  "
                f"({type(_init_conn_err).__name__}: {_init_conn_err})")
            _responsive_wait(retry_wait_sec)

    """初回接続の失敗を報告（タイムアウト・拒否・例外のいずれも。A-21）"""
    if _init_conn_err is not None:
        _sig = ErrorIF.report_exception(_init_conn_err, phase="startup", category="comm",
                                        trigger_id=TRIGGER_NAME, log_file=log_file)
        if _sig == SIGNAL_STOP:
            stop_event.set()
            return None
        conn_state.set_not_connectable(_init_conn_err, report=False)   # ErrorIF へは上で phase=startup として通知済み

    """======== LOOP START ========"""
    skipped_seg_count = 0

    while True:

        """キュー受信"""
        try:
            segment = post_queue.get(timeout=1.0)

        except queue.Empty:
            # 接続できない間は再接続を試みる（間隔は ConnectState）。接続できる間に切断を検知したら接続し直す
            _reconnect_if_due()
            _recover_link()

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

            """==== 接続できない間は、再接続の間隔ごとに再接続を試みる（セグメントが途切れず届く間も再接続できるように） ===="""
            _reconnect_if_due()

            """==== 接続できない間に届いたセグメントの処理（not_connectable_action） ===="""
            if not conn_state.is_connectable():
                if not_connectable_action == "skip":
                    skipped_seg_count += 1
                    base.output_log(MODULE_NAME,
                        f"WARNING  NOT CONNECTABLE - Segment Skipped. "
                        f"seg_id={seg_id}  total_skipped={skipped_seg_count}  "
                        f"{broker_host}:{broker_port}", log_file)
                    continue

                else:
                    """ブロック待ち（停止チェック付き）"""
                    base.output_log(MODULE_NAME,
                        f"{TRIGGER_NAME} NOT CONNECTABLE - Waiting for reconnect... seg_id={seg_id}", log_file)
                    while not conn_state.is_connectable() and not stop_event.is_set():
                        _reconnect_if_due()
                        if not conn_state.is_connectable():
                            _responsive_wait(min(1.0, reconnect_interval_sec))

                    if stop_event.is_set():
                        break

            """==== Publish するメッセージを作る ===="""
            # ==== mode="seg": 全行を1行ずつ ====
            if mode == "seg" and publish_mode == "row":
                payloads = []
                for row_val in segment:
                    row_dict = {col_keys[i]: row_val[i] for i in range(len(col_keys))}
                    payloads.append(json.dumps(row_dict, ensure_ascii=False, default=str).encode(encode))
                sent_count = len(segment)

            # ==== mode="seg" かつ publish_mode="segment": セグメント全体を1メッセージで ====
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
                payloads   = [json.dumps(seg_payload, ensure_ascii=False, default=str).encode(encode)]
                sent_count = len(row_dicts)  # ログ上は「まとめて送った行数」として扱う（マーカー行含む）

            # ==== mode="stats": 統計量 1 件 ====
            else:  # mode == "stats"
                stats_dict = calc_stats(stats_ctx, segment, seg_id)
                payloads   = [json.dumps(stats_dict, ensure_ascii=False, default=str).encode(encode)]
                sent_count = 1

            """==== Publish（リトライを含む） ===="""
            send_error = not _publish_with_retry(payloads, seg_id)
            if send_error:
                base.output_log(MODULE_NAME,
                    f"WARNING  {TRIGGER_NAME} Segment NOT Published (not connectable). "
                    f"seg_id={seg_id}  mode={mode}", log_file)

            if not send_error:
                _msg_count = len(payloads)
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
    _drop_client()

    if skipped_seg_count > 0:
        base.output_log(MODULE_NAME,
            f"WARNING  {TRIGGER_NAME} Stopped with {skipped_seg_count} segment(s) skipped due to NOT CONNECTABLE.",
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
