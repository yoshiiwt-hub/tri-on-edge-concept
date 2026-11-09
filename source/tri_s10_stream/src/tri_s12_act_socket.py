# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Act Trigger Module [Socket Send]
# Ver: 02.g.20261108    By: IWATA, Y.'s Agent AI
# Mod: pyflakes の指摘を整理（使っていない import・変数、置き換えの無い f-string を削除。動作は変えない）
########################################
セグメントの値や統計量を、TCP ソケットで他のシステムへ送り出す出口

主要関数:
a_send_seg_trigger    # セグメントの全行を、1行ずつ JSON で送信する。
a_send_stats_trigger  # セグメントの統計量を求め、1件の JSON で送信する。
_run_send_trigger     # 2つの trigger の共通ループ（送る内容だけが異なる）。

位置づけ: 受け手は独自のソケットサーバー。
設計意図: 受け手の応答（ACK）を確かめるモードを用意し、受け手が受け取れなかった行を送信済みに数えない。送信・接続に失敗したら、その場で接続し直して送り直し（リトライ）、続けて失敗したら接続できない（not_connectable）とする。接続できなくなったときに送れなかったセグメントは、再接続後に最初から送り直し、取りこぼしを減らす（受け手は seg_id / seg_ind で重複を見分ける）。接続できない状態の記録・再接続の間隔は tri_s01_connect_state に任せる。
前提・制約: 受け手は、区切り文字で終わる JSON を1行ずつ受け取り、ACK を使う場合は "OK" を返すこと。
"""
MODULE_NAME = "ACT_Socket"

"""standard library"""
from   datetime import datetime as dt
import json
import os
import socket
import queue
import sys
import threading
import time
import traceback
from   typing   import Any, Dict, List, Optional

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
from   src.tri_s01_connect_state  import ConnectState
from   src.tri_s10_stream_wave_stat  import init_stats, calc_stats, export_out_coldef  # Calc Stats（Act 共通。B-9）


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
# 共通送信ループ（a_send_seg_trigger / a_send_stats_trigger の実体）
########################################

def _run_send_trigger(
    mode:       str,  # "seg" または "stat"
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event,
) -> None:
    """TCPソケットでサーバへ送信する共通ループ。mode で送信内容だけが変わる

    [動作の要点]（旧モジュール Docstring から移動）
        ack_mode=True: 送信ごとにサーバの応答を区切り文字まで読み、"OK" なら次へ進む
            "OK" 以外（NG）は ErrorIF（phase=loop / category=data）へ報告し、送信済みに数えない（ack_ng_action に従う。A-8）
            応答が timeout_sec 以内に来ない場合は通信異常として扱う（下のリトライの対象）
        リトライ: 送信・接続に失敗したら、retry_wait_sec ごとに接続し直して、そのセグメントを最初の行から送り直す（このトリガーの中だけ）
            連続 retry_max 回失敗したら、接続できない（not_connectable）と判定して ConnectState へ報告する（送れたら回数を 0 に戻す）
            ACK NG は通信できているので、失敗に数えない（ack_ng_action に従う）
        再接続: 接続できない間は reconnect_interval_sec ごとに試み、結果を ConnectState へ報告する
            キューが空のときに加え、セグメントを受け取ったときにも間隔が過ぎていれば試みる
        接続できない間に届いたセグメント: not_connectable_action = "skip"（送らずにスキップ）/ "wait"（再接続できるまで待つ）
        送り直し: 接続できなくなったときに送れなかったセグメントは「再送待ち」として1つ持ち、再接続後に新しいセグメントより先に、最初の行から送り直す
            （resend_max 回まで。超えたら ErrorIF へ報告して破棄。受け取る側は各行の seg_id / seg_ind で重複を見分けられる。A-9）
            停止の合図（None）を受けたとき再送待ちがあれば、最後に1回だけ再接続・送り直しを試み、送れなければ破棄する
        運用系メトリクス（ACT_Socket）: total_sent_segments / total_ack_ng / total_resent_segments /
            total_dropped_segments（送り直しの上限超え・停止時の再送待ち） / total_skipped_segments（接続できない間にスキップ）
            接続の状態（ConnectState が登録）: connect_state / not_connectable_since / reconnect_fail_count / total_not_connectable
    """
    TRIGGER_NAME = "a_send_seg_trigger" if mode == "seg" else "a_send_stats_trigger"

    def _connect(_ip: str, _port: int, _to: float) -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(_to)
        s.connect((_ip, _port))
        return s

    def _responsive_wait(seconds: float) -> None:
        """stop_event を見ながら細かく待つ（即座に停止へ反応できるように。tri_s11_find_slmp.py 等と同様）"""
        _wait_end = time.time() + seconds
        while time.time() < _wait_end and not stop_event.is_set():
            time.sleep(min(0.05, max(0.0, _wait_end - time.time())))

    """======== LOOP INIT ========"""
    try:
        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Start. mode={mode}", log_file)

        # データ項目定義
        n_col_def = len(col_def)
        col_keys  = [col_def[k][0] for k in sorted(col_def.keys())]
        base.output_log(MODULE_NAME, f"n_col_def={n_col_def}  col_keys={col_keys}", log_file)

        # 出力データ項目定義（a_send_stats_trigger のみ）
        if mode == "stat":
            stats_ctx    = init_stats(config, col_def, MODULE_NAME, TRIGGER_NAME)

            # save out_coldef
            save_dir = os.path.join(system_root, config.get("save_dir", "output"))
            export_out_coldef(stats_ctx.out_coldef,
                os.path.join(save_dir, f"output_column_define_{TRIGGER_NAME}.json"), "utf-8", MODULE_NAME)

        # 接続設定
        server_ip   = str(config.get("server_ip", "127.0.0.1"))
        server_port = int(config.get("server_port", 9000))
        timeout_sec = float(config.get("timeout_sec", 5.0))
        reconnect_interval_sec = float(config.get("reconnect_interval_sec", 5.0))
        retry_max              = int(config.get("retry_max", 3))
        retry_wait_sec         = float(config.get("retry_wait_sec", 1.0))
        not_connectable_action = str(config.get("not_connectable_action", "skip")).strip().lower()
        if retry_max < 1 or retry_wait_sec < 0:
            raise ValueError(f"config['retry_max'] must be >= 1 and config['retry_wait_sec'] >= 0. Got: {retry_max} / {retry_wait_sec}")
        if not_connectable_action not in ("skip", "wait"):
            raise ValueError(f"config['not_connectable_action'] must be 'skip' / 'wait'. Got: {not_connectable_action!r}")
        base.output_log(MODULE_NAME,
            f"server_ip={server_ip} server_port={server_port} timeout_sec={timeout_sec} "
            f"retry_max={retry_max} retry_wait_sec={retry_wait_sec} reconnect_interval_sec={reconnect_interval_sec} "
            f"not_connectable_action={not_connectable_action}", log_file)

        # Socket通信設定
        encode   = str(config.get("encode", "utf-8"))
        sep      = str(config.get("sep", "\n"))
        chunk    = int(config.get("chunk", 1024))
        ack_mode = bool(config.get("ack_mode", False))
        ack_ng_action = str(config.get("ack_ng_action", "skip")).strip().lower()
        if ack_ng_action not in ("skip", "retry", "abort_segment"):
            raise ValueError(f"config['ack_ng_action'] must be 'skip' / 'retry' / 'abort_segment'. Got: {ack_ng_action!r}")
        ack_retry  = int(config.get("ack_retry", 3))
        resend_max = int(config.get("resend_max", 3))
        if ack_retry < 0 or resend_max < 0:
            raise ValueError(f"config['ack_retry'] / config['resend_max'] must be >= 0. Got: {ack_retry} / {resend_max}")
        base.output_log(MODULE_NAME,
            f"ack_mode={ack_mode} ack_ng_action={ack_ng_action} ack_retry={ack_retry} resend_max={resend_max}", log_file)

        # 運用系メトリクスの初期値（A-8 / A-9）
        for _key in ("total_sent_segments", "total_ack_ng", "total_resent_segments",
                     "total_dropped_segments", "total_skipped_segments"):
            base.metrics_add(MODULE_NAME, _key, 0)

        # 接続の状態（判定・リトライはこのトリガー。記録・再接続の間隔・通知は ConnectState）
        conn_state = ConnectState(TRIGGER_NAME, f"{server_ip}:{server_port}", reconnect_interval_sec, MODULE_NAME, log_file)

        # 初回接続（retry_wait_sec ごとに retry_max 回まで試みる）
        tcp_socket: Optional[socket.socket] = None
        _err: Optional[BaseException] = None
        for _try in range(1, retry_max + 1):
            try:
                tcp_socket = _connect(server_ip, server_port, timeout_sec)
                _err = None
                base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Connected. {server_ip}:{server_port}", log_file)
                break
            except Exception as e:
                _err = e
                if _try < retry_max:
                    base.output_log(MODULE_NAME,       # リトライは画面のみ
                        f"{TRIGGER_NAME} Connect Retry ({_try}/{retry_max}). {server_ip}:{server_port}  ({type(e).__name__}: {e})")
                    _responsive_wait(retry_wait_sec)
        if _err is not None:
            _sig = ErrorIF.report_exception(_err, phase="startup", category="comm",  # 初回接続の失敗を報告（A-21）
                                             trigger_id=TRIGGER_NAME, log_file=log_file)
            if _sig == SIGNAL_STOP:
                stop_event.set()
                return None
            conn_state.set_not_connectable(_err, report=False)   # ErrorIF へは上で phase=startup として通知済み

    except Exception as e:
        ErrorIF.report_exception(e, phase="startup", category="config", trigger_id=TRIGGER_NAME, log_file=log_file)
        stop_event.set()
        return None

    """======== 送信ヘルパー（A-8 / A-9） ========"""
    fail_count = 0                             # 連続失敗の回数（判定に使う。このトリガーの中だけで数える）
    skipped_seg_count = 0
    ack_buf = ""                               # ACK の受信バッファ（区切り文字の後ろの残りを次の ACK に回す）
    pending: Optional[Dict[str, Any]] = None   # 再送待ちのセグメント {"segment", "seg_id", "resends"}（最大1つ）

    def _drop_socket() -> None:
        """ソケットを閉じる（ACK の受信バッファも捨てる）"""
        nonlocal tcp_socket, ack_buf
        if tcp_socket:
            try:   tcp_socket.close()
            except Exception: pass
        tcp_socket = None
        ack_buf    = ""

    def _try_connect() -> Optional[BaseException]:
        """接続を1回試みる。成功なら None、失敗ならその例外"""
        nonlocal tcp_socket, ack_buf
        try:
            tcp_socket = _connect(server_ip, server_port, timeout_sec)
            ack_buf    = ""
            return None
        except Exception as e:
            tcp_socket = None
            return e

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

    def _recv_ack() -> str:
        """
        ACK を区切り文字まで読む（分かれて届いても、まとめて届いても1件ずつ取り出す。A-8）
        timeout_sec 以内に区切り文字まで届かなければ socket.timeout、相手が切断したら ConnectionResetError
        """
        nonlocal ack_buf
        while sep not in ack_buf:
            data = tcp_socket.recv(chunk)
            if not data:
                raise ConnectionResetError("Server closed connection while waiting ACK.")
            ack_buf += data.decode(encode)
        line, ack_buf = ack_buf.split(sep, 1)
        return line.strip()

    def _send_message(send_str: str) -> bool:
        """
        PUT を1件送る。ack_mode=True なら ACK を読み、"OK" なら True、それ以外（NG）なら False
        通信異常は例外のまま呼び出し側へ（_send_segment が切断として扱う）
        """
        tcp_socket.sendall(("PUT" + sep).encode(encode))
        tcp_socket.sendall(send_str.encode(encode))
        if not ack_mode:
            return True
        ack = _recv_ack()
        if ack == "OK":
            return True
        return _report_ack_ng(ack)

    _ack_ng_ctx: Dict[str, Any] = {}   # _report_ack_ng に渡す送信中の位置（seg_id / row）

    def _report_ack_ng(ack: str) -> bool:
        """ACK NG を ErrorIF（phase=loop / category=data）へ報告し、運用系メトリクスに数える。常に False を返す"""
        ErrorIF.report_event({
            "trigger_id": TRIGGER_NAME,
            "phase"     : "loop",
            "category"  : "data",
            "exc_type"  : "AckNG",
            "message"   : f"ACK NG: seg_id={_ack_ng_ctx.get('seg_id')}  row={_ack_ng_ctx.get('row')}  ack={ack!r}",
            "signature" : f"AckNG.data.loop.{TRIGGER_NAME}",
        })
        base.metrics_add(MODULE_NAME, "total_ack_ng", 1)
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} ACK NG: seg_id={_ack_ng_ctx.get('seg_id')}  row={_ack_ng_ctx.get('row')}  ack={ack!r}", log_file)
        return False

    def _send_segment(segment: List[list], seg_id: Any) -> Dict[str, Any]:
        """
        1セグメントを送る（mode="seg": 全行を1行ずつ / mode="stat": 統計量1件）
        Returns: {"status": "ok" | "aborted" | "disconnected", "sent": 送信済みの件数, "ack_ng": NG の件数}
          ok           : 最後まで送った（NG の行は ack_ng_action に従い、送信済みには数えない）
          aborted      : ack_ng_action="abort_segment" で残りを送らなかった
          disconnected : 送信中に通信に失敗した（ソケットは閉じた。リトライ・判定は _send_with_retry）
        """
        if mode == "seg":
            messages = []
            for row_val in segment:
                row_dict = {col_keys[i]: row_val[i] for i in range(len(col_keys))}
                messages.append(json.dumps(row_dict, ensure_ascii=False, default=str) + sep)
        else:
            stats_dict = calc_stats(stats_ctx, segment, seg_id)
            messages = [json.dumps(stats_dict, ensure_ascii=False, default=str) + sep]

        sent = ack_ng = 0
        for row_num, send_str in enumerate(messages, start=1):
            _ack_ng_ctx.update(seg_id=seg_id, row=row_num if mode == "seg" else "stat")
            try:
                ok = _send_message(send_str)
                tries = 0
                while not ok and ack_ng_action == "retry" and tries < ack_retry:
                    tries += 1
                    ok = _send_message(send_str)
            except Exception as e:
                _drop_socket()   # 通信の失敗（リトライ・判定は _send_with_retry）
                return {"status": "disconnected", "sent": sent, "ack_ng": ack_ng, "error": e,
                        "detail": f"sent={sent}/{len(messages)}" if mode == "seg" else "(stat mode)"}
            if ok:
                sent += 1
                continue
            ack_ng += 1
            if ack_ng_action == "abort_segment":
                return {"status": "aborted", "sent": sent, "ack_ng": ack_ng}
        return {"status": "ok", "sent": sent, "ack_ng": ack_ng}

    def _send_with_retry(segment: List[list], seg_id: Any) -> Dict[str, Any]:
        """
        1セグメントを送る。通信・接続に失敗したら retry_wait_sec ごとに接続し直して、最初の行から送り直す（リトライ）。
        連続 retry_max 回失敗したら not_connectable と判定して ConnectState へ報告し、status="disconnected" を返す
        （送れたら連続失敗の回数を 0 に戻す。リトライは画面のみに出し、ConnectState・ErrorIF へは報告しない）
        """
        nonlocal fail_count
        while True:
            e = _try_connect() if tcp_socket is None else None
            if e is None:
                res = _send_segment(segment, seg_id)
                if res["status"] != "disconnected":
                    fail_count = 0
                    return res
                e = res["error"]
            fail_count += 1
            if fail_count >= retry_max:
                conn_state.set_not_connectable(e)   # 判定：連続 retry_max 回の失敗
                return {"status": "disconnected", "sent": 0, "ack_ng": 0, "error": e}
            if stop_event.is_set():
                return {"status": "disconnected", "sent": 0, "ack_ng": 0, "error": e}
            base.output_log(MODULE_NAME,
                f"{TRIGGER_NAME} Retry ({fail_count}/{retry_max}). seg_id={seg_id}  "
                f"{server_ip}:{server_port}  ({type(e).__name__}: {e})")
            _responsive_wait(retry_wait_sec)

    def _log_result(seg_id: Any, n_messages: int, res: Dict[str, Any], resent: int = 0) -> None:
        if res["status"] == "disconnected":
            return
        base.metrics_add(MODULE_NAME, "total_sent_segments", 1)
        # 正常は画面のみ（ログファイルには書かない。件数は metrics）。ACK NG で中断した場合はログファイルにも残す
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Segment {'Sent' if res['status'] == 'ok' else 'Aborted (ACK NG)'}. "
            f"seg_id={seg_id}  mode={mode}  sent={res['sent']}  ack_ng={res['ack_ng']}  "
            f"original_rows={n_messages}" + (f"  resent={resent}" if resent else ""),
            log_file if res["status"] != "ok" else "")

    def _hold_pending(segment: List[list], seg_id: Any) -> None:
        """接続できなくなって送れなかったセグメントを再送待ちにする（resend_max=0 なら送り直さず破棄）"""
        nonlocal pending
        if resend_max <= 0:
            base.metrics_add(MODULE_NAME, "total_dropped_segments", 1)
            base.output_log(MODULE_NAME,
                f"WARNING  {TRIGGER_NAME} Segment Dropped (resend_max=0). seg_id={seg_id}", log_file)
            return
        pending = {"segment": segment, "seg_id": seg_id, "resends": 0}
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Segment Held for Resend. seg_id={seg_id}  resend_max={resend_max}", log_file)

    def _flush_pending() -> bool:
        """
        接続できる状態なら、再送待ちのセグメントを最初の行から送り直す（リトライを含む）。
        Returns: 再送待ちが無くなった（送れた・上限で破棄した）なら True、まだ残っている（接続できない）なら False
        """
        nonlocal pending
        if pending is None:
            return True
        if not conn_state.is_connectable():
            return False
        pending["resends"] += 1
        seg, sid = pending["segment"], pending["seg_id"]
        base.metrics_add(MODULE_NAME, "total_resent_segments", 1)
        base.output_log(MODULE_NAME,
            f"{TRIGGER_NAME} Resending Segment. seg_id={sid}  resend={pending['resends']}/{resend_max}", log_file)
        res = _send_with_retry(seg, sid)
        if res["status"] != "disconnected":
            _log_result(sid, len(seg) if mode == "seg" else 1, res, pending["resends"])
            pending = None
            return True
        if pending["resends"] >= resend_max:
            ErrorIF.report_event({
                "trigger_id": TRIGGER_NAME,
                "phase"     : "loop",
                "category"  : "comm",
                "exc_type"  : "ResendLimitExceeded",
                "message"   : f"Segment Dropped after {resend_max} resend(s). seg_id={sid}",
            })
            base.metrics_add(MODULE_NAME, "total_dropped_segments", 1)
            base.output_log(MODULE_NAME,
                f"WARNING  {TRIGGER_NAME} Segment Dropped after {resend_max} resend(s). seg_id={sid}", log_file)
            pending = None
            return True
        return False

    """======== LOOP START ========"""
    while True:

        """キュー受信"""
        try:
            segment = post_queue.get(timeout=1.0)

        except queue.Empty:
            # 接続できない間はここで再接続を試みる（間隔は ConnectState）。再接続できたら再送待ちを先に送る
            _reconnect_if_due()
            if conn_state.is_connectable() and pending is not None:
                _flush_pending()

            if (stop_event.is_set() and
                    post_queue.qsize() == 0 and
                    getattr(post_queue, "unfinished_tasks", 0) == 0):
                break
            continue

        """sentinel → 終了（再送待ちがあれば、最後に1回だけ再接続・送り直しを試みる）"""
        if segment is None:
            if pending is not None:
                if not conn_state.is_connectable():
                    _reconnect_now()
                _flush_pending()
            post_queue.task_done()
            break

        base.output_log(MODULE_NAME, f"{TRIGGER_NAME} ...Received Queue.")

        """セグメント処理"""
        try:
            """項目の整合性（個数チェック）"""
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

            """空レコードチェック"""
            if (isinstance(segment[0][-1], list)) and (segment[0][-1][0] is None):
                base.output_log(MODULE_NAME,
                    f"WARNING  空レコード。スキップ。 seg_id={seg_id}", log_file)
                continue

            """==== 接続できない間は、再接続の間隔ごとに再接続を試みる（セグメントが途切れず届く間も再接続できるように） ===="""
            _reconnect_if_due()

            """==== 再送待ちを先に送る（接続できる状態なら。A-9） ===="""
            if conn_state.is_connectable() and pending is not None:
                _flush_pending()

            """==== 接続できない間に届いたセグメントの処理（not_connectable_action） ===="""
            if not conn_state.is_connectable():

                if not_connectable_action == "skip":
                    skipped_seg_count += 1
                    base.metrics_add(MODULE_NAME, "total_skipped_segments", 1)
                    base.output_log(MODULE_NAME,
                        f"WARNING  NOT CONNECTABLE - Segment Skipped. "
                        f"seg_id={seg_id}  total_skipped={skipped_seg_count}  "
                        f"{server_ip}:{server_port}", log_file)
                    continue

                else:
                    """ブロック待ち（停止チェック付き）。再接続できたら再送待ちを先に送る"""
                    base.output_log(MODULE_NAME,
                        f"{TRIGGER_NAME} NOT CONNECTABLE - Waiting for reconnect... seg_id={seg_id}", log_file)
                    while not stop_event.is_set():
                        if not conn_state.is_connectable():
                            _reconnect_if_due()
                            if not conn_state.is_connectable():
                                _responsive_wait(min(1.0, reconnect_interval_sec))
                                continue
                        if _flush_pending() and conn_state.is_connectable():
                            break

                    if stop_event.is_set():
                        break

            """==== Sending（リトライを含む） ===="""
            res = _send_with_retry(segment, seg_id)
            if res["status"] == "disconnected":
                _hold_pending(segment, seg_id)   # 再接続後に最初の行から送り直す（A-9）
            else:
                _log_result(seg_id, len(segment) if mode == "seg" else 1, res)

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
    if pending is not None:
        base.metrics_add(MODULE_NAME, "total_dropped_segments", 1)
        base.output_log(MODULE_NAME,
            f"WARNING  {TRIGGER_NAME} Stopped with a segment waiting for resend (dropped). seg_id={pending['seg_id']}", log_file)

    if tcp_socket:
        try:   tcp_socket.close()
        except Exception: pass

    if skipped_seg_count > 0:
        base.output_log(MODULE_NAME,
            f"WARNING  {TRIGGER_NAME} Stopped with {skipped_seg_count} segment(s) skipped due to NOT CONNECTABLE.", log_file)

    base.output_log(MODULE_NAME, f"{TRIGGER_NAME} Stop.", log_file)
    return None


########################################
# Act Trigger Functions（薄いラッパー）
########################################

def a_send_seg_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """セグメントの全行をJSON形式でTCPソケット送信する"""
    return _run_send_trigger("seg", post_queue, col_def, config, stop_event)

a_send_seg_trigger.QMODE = "imq"
a_send_seg_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


def a_send_stats_trigger(
    post_queue: queue.Queue,
    col_def:    Dict[int, Any],
    config:     Dict[str, Any],
    stop_event: threading.Event
) -> None:
    """セグメントの統計量を算出し、1件をJSON形式でTCPソケット送信する"""
    return _run_send_trigger("stat", post_queue, col_def, config, stop_event)

a_send_stats_trigger.QMODE = "imq"
a_send_stats_trigger.CUT_LAST_ROW = True  # 終点行（変化後）を core が除外して渡す


########################################
# Entry Point (for Debug)
########################################
if __name__ == "__main__":
    base.output_log(MODULE_NAME, "Run Standalone.")
    base.output_log(MODULE_NAME, "test show Log")
    base.output_log(MODULE_NAME, f"test save Log  {log_file}", log_file)
