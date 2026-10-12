# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Sample Socket Server [CSV Reader]
# Ver: 01.k.20261012    By: IWATA, Y.'s Agent AI
# Mod: テストからも使えるよう SocketServer クラスに切り出し（空きポートの自動割当・start/stop・PUT 受信データの保持・
#      接続ごとのスレッド処理）。import しただけでは設定を読まない。CSV のヘッダ行の読み飛ばし（csv_header）を追加
########################################
CSVファイル（または行のリスト）を1行ずつ、ソケットクライアントからの要求に応じてJSON形式で返すサンプルサーバー。
TriOnEdge 本体（tri_s00_supervise_base 等）に依存しない、スタンドアロン実装。

使い方1）デモ用サーバとして起動（Ctrl+C で停止）:
    python socket_server.py                    … 同じフォルダの socket_server.json を読む
    python socket_server.py <設定ファイル.json>

使い方2）テストから使う:
    from sample_server.socket_server import SocketServer
    srv = SocketServer(rows=[["2026-10-12 09:00:00.000000", "RUN", 1.0]], dtypes=["time", "str", "float"])
    srv.start()                 # port=0（既定）なら空いているポートを自動で使う → srv.port
    ...
    srv.wait_puts(3)            # PUT で受け取ったデータが3件たまるまで待つ → srv.puts
    srv.stop()

プロトコル:
    クライアント → サーバー:
        GET / NACK / ACK : separator なしで送信（f_socket_client_trigger の仕様）
        PUT データ       : "PUT\\n" を送信後、JSON文字列 + "\\n" を送信（a_send_seg_trigger / a_send_stats_trigger の仕様）
    サーバー → クライアント:
        GET への応答 : JSON配列 + "\\n"  例: ["2026-10-12 09:00:00.000000", "RUN", 1.0] + "\\n"
        NACK への応答: 直前に返した行を再送
        PUT への応答 : "OK\\n"（put_ack=false なら返さない）

    ※ TCP の特性上、"PUT\\n" と JSON データが同一の recv() で届く場合がある

設定ファイル（JSON。使い方1のとき）:
    ip / port / encode / sep / chunk / VERBOSE
    csv_path   : CSVファイルのパス（設定ファイルからの相対パス、または絶対パス）
    csv_encode : CSVの文字コード（既定: utf-8）
    csv_header : true なら CSV の1行目（ヘッダ行）を読み飛ばす（既定: false）
    put_ack    : PUT に "OK" を返すか（既定: true）。応答を読まないクライアント（act_socket の ack_mode=false）が
                 接続を閉じると、読まれずに残った応答のため接続がリセットされ、最後に送ったデータを取りこぼすことがある。
                 そのようなクライアントには false にする
    mode       : "sequential"（既定。順番に返し、末尾で先頭に戻る） / "random"（GET のたびにランダムな行） /
                 "put_only"（PUT 専用。GET には ERROR を返す。csv_path / column 不要）
    column     : {"0": [列名, dtype, ...], ...}  dtype = "time"（文字列扱い）| "str" | "int" | "float"
                 CSV の列数が column より少ない場合、不足列はデフォルト値で補う
"""
MODULE_NAME = "SAMPLE_SOCKET_SVR"

"""standard library"""
import csv
import json
import pathlib
import random
import socket
import sys
import threading
import time
import traceback
from   datetime import datetime as dt
from   typing   import Any, Dict, List, Optional, Sequence


MODES = ("sequential", "random", "put_only")

# キャストに失敗したときのデフォルト値
DEFAULTS: Dict[str, Any] = {
    "time" : "2999-01-01 00:00:00.000000",
    "int"  : -9999,
    "float": -9999.9,
    "str"  : "__",
}


########################################
# Utility
########################################

def _now_str() -> str:
    return dt.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _log(msg: str) -> None:
    print(f"{_now_str()}  [{MODULE_NAME}]  {msg}", flush=True)


def read_json(filepath: pathlib.Path) -> dict:
    """文字コードを自動判定してJSONを読み込む"""
    last_error = None
    for enc in ["utf-8-sig", "utf-8", "cp932", "euc-jp"]:
        try:
            with open(filepath, mode="r", encoding=enc) as f:
                return json.load(f)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            last_error = e
    raise ValueError(f"JSONを読み込めません（文字コード不一致または書式の誤り）: {filepath}\n{last_error}")


def load_csv(path: pathlib.Path, encode: str = "utf-8", header: bool = False) -> List[List[str]]:
    """CSVを全行読み込んで2次元リストで返す（header=True なら1行目を読み飛ばす）"""
    if not path.exists():
        raise FileNotFoundError(f"CSVファイルが見つかりません: {path}")
    with open(path, encoding=encode, newline="") as f:
        rows = list(csv.reader(f))
    if header:
        rows = rows[1:]
    if not rows:
        raise ValueError(f"CSVにデータ行がありません: {path}")
    return rows


def cast_value(raw: Any, dtype: str) -> Any:
    """値を dtype に従いキャストする。失敗時はデフォルト値を返す"""
    try:
        if dtype == "int":
            return int(float(raw))   # "1.0" のような文字列にも対応
        if dtype == "float":
            return float(raw)
        return str(raw).strip()      # str / time / 未知の dtype は文字列として扱う
    except (ValueError, TypeError):
        return DEFAULTS.get(dtype)


def cast_row(row: Sequence[Any], dtypes: Sequence[str]) -> List[Any]:
    """1行分の値を dtype に従いキャストして返す（不足列はデフォルト値）"""
    return [cast_value(row[i], d) if i < len(row) else DEFAULTS.get(d) for i, d in enumerate(dtypes)]


########################################
# Server
########################################

class SocketServer:
    """
    GET に行を返し、PUT のデータを受け取るサンプルサーバー
    Mod: 2026.10.12    By: IWATA, Y.'s Agent AI

    Args:
        rows    : GET で返す行のリスト（mode="put_only" なら不要）
        dtypes  : 列ごとの dtype（省略時は rows の値をそのまま返す）
        ip      : 待ち受けるアドレス（既定: 127.0.0.1）
        port    : 待ち受けるポート（0 なら空いているポートを自動で使う → self.port）
        mode    : "sequential" / "random" / "put_only"
        put_ack : PUT に "OK" を返すか（応答を読まないクライアントには False にする。設定ファイルの put_ack の説明を参照）
        encode / sep / chunk : 文字コード・区切り文字・受信サイズ
        verbose : 送受信の内容を画面に出す
    """

    def __init__(
        self,
        rows:    Optional[Sequence[Sequence[Any]]] = None,
        dtypes:  Optional[Sequence[str]] = None,
        ip:      str   = "127.0.0.1",
        port:    int   = 0,
        mode:    str   = "sequential",
        put_ack: bool  = True,
        encode:  str   = "utf-8",
        sep:     str   = "\n",
        chunk:   int   = 1024,
        verbose: bool  = False,
    ) -> None:
        mode = str(mode).lower()
        if mode not in MODES:
            raise ValueError(f"mode={mode!r} は不正です。使える値: {list(MODES)}")
        if mode != "put_only" and not rows:
            raise ValueError("rows（GET で返す行）がありません。")
        self.rows    = [list(r) for r in (rows or [])]
        self.dtypes  = list(dtypes) if dtypes else None
        self.mode    = mode
        self.put_ack = bool(put_ack)
        self.encode  = encode
        self.sep     = sep
        self.chunk   = int(chunk)
        self.verbose = bool(verbose)
        self.puts: List[Any] = []                  # PUT で受け取ったデータ（JSON はデコード済み）

        self._lock   = threading.Lock()
        self._halt   = threading.Event()           # ※ threading.Thread の内部名（_stop 等）と重ならない名前
        self._cursor = 0
        self._thread: Optional[threading.Thread] = None

        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind((ip, int(port)))
        self._srv.listen(5)
        self._srv.settimeout(0.2)
        self.ip   = ip
        self.port = self._srv.getsockname()[1]

    # ---- 起動・停止 ----
    def start(self) -> "SocketServer":
        """別スレッドで受付を始める"""
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        """受付を止める"""
        self._halt.set()
        try:
            self._srv.close()
        except OSError:
            pass
        if self._thread is not None:
            self._thread.join(timeout=2)

    def serve_forever(self) -> None:
        """stop() が呼ばれるまで接続を受け付ける（接続ごとにスレッドを立てる）"""
        while not self._halt.is_set():
            try:
                conn, addr = self._srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn.settimeout(0.5)
            threading.Thread(target=self._handle_client, args=(conn, addr), daemon=True).start()

    def wait_puts(self, n: int, timeout_sec: float = 5.0) -> bool:
        """PUT で受け取ったデータが n 件たまるまで待つ。たまれば True"""
        end = time.time() + timeout_sec
        while time.time() < end:
            with self._lock:
                if len(self.puts) >= n:
                    return True
            time.sleep(0.02)
        with self._lock:
            return len(self.puts) >= n

    # ---- 応答 ----
    def _next_row(self) -> List[Any]:
        with self._lock:
            if self.mode == "random":
                row = self.rows[random.randrange(len(self.rows))]
            else:
                row = self.rows[self._cursor % len(self.rows)]
                self._cursor += 1
        return cast_row(row, self.dtypes) if self.dtypes else list(row)

    def _on_put(self, data_str: str) -> None:
        """PUT で受け取ったデータを残す（JSON でなければ文字列のまま）"""
        try:
            data = json.loads(data_str)
        except ValueError:
            data = data_str
        with self._lock:
            self.puts.append(data)
        if self.verbose:
            _log(f"PUT RCV: {data}")

    def _recv(self, conn: socket.socket) -> Optional[bytes]:
        """1回受信する（受信待ちのタイムアウトなら None、相手が切断したら b"" を返す）"""
        try:
            return conn.recv(self.chunk)
        except socket.timeout:
            return None

    def _send(self, conn: socket.socket, text: str) -> bool:
        """返信する。相手が先に切断していても例外にしない（受信済みのデータの処理は続ける）"""
        try:
            conn.sendall(text.encode(self.encode))
            return True
        except OSError:
            return False

    def _reply_commands(self, conn: socket.socket, req_b: bytes, last_sent: Optional[List[Any]]) -> Optional[List[Any]]:
        """GET / NACK / ACK に応じる（separator なし。まとめて届いた GET は回数分返す）。直前に返した行を返す"""
        req = req_b.decode(self.encode, errors="ignore")
        if not req.strip():
            return last_sent
        if self.verbose:
            _log(f"RCV: {req.strip()!r}")
        n_get = req.count("GET")
        if n_get:
            if self.mode == "put_only":
                self._send(conn, "ERROR: put_only mode" + self.sep)
                return last_sent
            for _ in range(n_get):
                last_sent = self._next_row()
                self._send(conn, json.dumps(last_sent, ensure_ascii=False) + self.sep)
                if self.verbose:
                    _log(f"SND: {last_sent}")
        elif "NACK" in req:
            if last_sent is not None:
                self._send(conn, json.dumps(last_sent, ensure_ascii=False) + self.sep)
        elif "ACK" in req:
            pass
        elif self.verbose:
            _log(f"Unknown Request: {req.strip()!r}")
        return last_sent

    def _handle_client(self, conn: socket.socket, addr: Any) -> None:
        """
        1クライアントの GET / NACK / ACK / PUT に応じる
        受信したデータはバッファにためて先頭から処理する（"PUT\\n" + JSON + "\\n" が続けて・まとめて届いても1件ずつ取り出す）
        """
        if self.verbose:
            _log(f"Client Connected: {addr}  mode={self.mode}")
        sep_b = self.sep.encode(self.encode)
        last_sent: Optional[List[Any]] = None
        buf = b""
        try:
            while not self._halt.is_set():
                data = self._recv(conn)
                if data is None:
                    continue
                if not data:
                    break
                buf += data

                while buf:
                    i_put = buf.find(b"PUT")
                    if i_put < 0:
                        # PUT を含まない → コマンドとして処理（末尾が "PUT" の書きかけなら、次の受信まで残す）
                        keep = 2 if buf.endswith(b"PU") else 1 if buf.endswith(b"P") else 0
                        cmd, buf = (buf[:-keep], buf[-keep:]) if keep else (buf, b"")
                        last_sent = self._reply_commands(conn, cmd, last_sent)
                        break
                    # PUT より前はコマンドとして処理
                    if i_put > 0:
                        last_sent = self._reply_commands(conn, buf[:i_put], last_sent)
                        buf = buf[i_put:]
                    # "PUT" + 区切り文字 + JSON + 区切り文字 がそろうまで待つ
                    body = buf[len(b"PUT"):]
                    if body.startswith(sep_b):
                        body = body[len(sep_b):]
                    elif not body or sep_b.startswith(body):
                        break                                  # 区切り文字がまだ届いていない
                    i_end = body.find(sep_b)
                    if i_end < 0:
                        break                                  # JSON の終わりがまだ届いていない
                    self._on_put(body[:i_end].decode(self.encode).strip())
                    if self.put_ack:
                        self._send(conn, "OK" + self.sep)
                    buf = body[i_end + len(sep_b):]

        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        except Exception as e:
            _log(f"Client Handler Error: {type(e).__name__}: {e}\n{traceback.format_exc()}")
        finally:
            try:
                conn.close()
            except OSError:
                pass
            if self.verbose:
                _log(f"Client Disconnected: {addr}")


def server_from_config(cfg_path: pathlib.Path) -> SocketServer:
    """設定ファイル（JSON）から SocketServer を作る（csv_path は設定ファイルからの相対パス、または絶対パス）"""
    cfg  = read_json(cfg_path)
    mode = str(cfg.get("mode", "sequential")).lower()
    rows: List[List[str]] = []
    dtypes: Optional[List[str]] = None
    if mode != "put_only":
        csv_str = cfg.get("csv_path", "")
        if not csv_str:
            raise ValueError("csv_path が設定されていません。")
        csv_p = pathlib.Path(csv_str)
        if not csv_p.is_absolute():
            csv_p = cfg_path.parent / csv_p
        rows   = load_csv(csv_p, cfg.get("csv_encode", "utf-8"), bool(cfg.get("csv_header", False)))
        column = cfg.get("column", {})
        dtypes = [str(v[1]).lower() for _, v in sorted(column.items(), key=lambda x: int(x[0]))] or None
        _log(f"CSV: {csv_p}  rows={len(rows)}  dtypes={dtypes}")
    return SocketServer(
        rows=rows, dtypes=dtypes,
        ip=cfg.get("ip", "0.0.0.0"), port=int(cfg.get("port", 52001)), mode=mode,
        put_ack=bool(cfg.get("put_ack", True)),
        encode=cfg.get("encode", "utf-8"), sep=cfg.get("sep", "\n"), chunk=int(cfg.get("chunk", 1024)),
        verbose=bool(cfg.get("VERBOSE", False)),
    )


########################################
# Entry Point
########################################

if __name__ == "__main__":
    cfg_path = pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else pathlib.Path(__file__).parent / "socket_server.json"
    if not cfg_path.exists():
        _log(f"設定ファイルが見つかりません: {cfg_path}")
        sys.exit(1)
    try:
        server = server_from_config(cfg_path)
    except Exception as e:
        _log(f"起動に失敗しました: {type(e).__name__}: {e}")
        sys.exit(1)
    _log(f"Server Ready. Waiting Connection at {server.ip}:{server.port}  mode={server.mode}  (Ctrl+C で停止)")
    server.start()
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        _log("KeyboardInterrupt. Server Stopped.")
    finally:
        server.stop()
        _log("Server Exit.")
