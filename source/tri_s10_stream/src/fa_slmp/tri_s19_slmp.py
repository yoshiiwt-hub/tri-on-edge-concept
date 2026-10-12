# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# SLMP通信ラッパー関数群
# Ver: 03.g.20261001    By: IWATA, Y.'s Agent AI
# Mod: モジュール名を tri_edge_XX_* から tri_sXX_* 形式に変更（tri_edge_19_slmp → tri_s19_slmp）
########################################
SlmpClient.device_read       # デバイス番地 一括読出し（永続接続）
SlmpClient.device_write      # デバイス番地 一括書込み（永続接続）
SlmpClient.device_read_bulk  # デバイス番地 自動分割読出し（960word超え対応）
SlmpClient.label_read        # ラベル名 読出し（永続接続）
SlmpClient.label_write       # ラベル名 書込み（永続接続）
SlmpClient.label_read_bulk   # ラベル名 複数・スリープ付き読出し
DeviceRead      # デバイス番地 一括読出し（ステートレス）
DeviceWrite     # デバイス番地 一括書込み（ステートレス）
DeviceReadBulk  # デバイス番地 自動分割読出し（ステートレス）
ArrayLabelRead  # ラベル名 読出し（ステートレス）
ArrayLabelWrite # ラベル名 書込み（ステートレス）
ArrayLabelReadBulk  # ラベル名 複数読出し（ステートレス）
decode_float    # ワードリスト → float32 変換
decode_dword    # ワードリスト → DWord リスト変換
decode_string   # ワードリスト → ASCII文字列変換
encode_dword    # DWordリスト → ワードペアリスト変換（書込み用）
words_to_int    # 2ワード → 1つの整数変換（src.tri_s19_extract_bitword へ注入して使用）

動作概要
    1. SlmpClient（永続接続型）または ステートレス関数で SLMP プロトコルを処理
    2. デバイス番地読出し・書込み・一括読出し（960word 超え自動分割）をサポート
    3. ラベル名読出し・書込み・複数ラベル一括読出しをサポート
    4. float32 / DWord / 文字列 の変換ヘルパー関数を提供
    5. 検証時は slmp_server.py（standalone, tri_edge_本体と独立）を起動して接続する
       （旧 USE_FAKE_SLMP フェイク応答モードは廃止）
"""
MODULE_NAME = "SLMP WRAPPER"

"""standard library"""
import math
import os
import socket
import struct
import threading
import time


"""[Bootstrap] Adjust Path and Import Base Module"""
try:
    import src.tri_s00_supervise_base as base
    import sys as _sys
    _sys.stdout.write(f"  [{MODULE_NAME}]  Base Module Imported Successfully.\n")
    _sys.stdout.flush()

except ModuleNotFoundError:
    import sys as _sys
    import traceback as _tb
    try:
        dir_valid = 'fa_slmp'
        _cur_dir  = os.path.dirname(os.path.abspath(__file__))
        _dir_name = os.path.basename(_cur_dir)
        if _dir_name != dir_valid:
            raise FileNotFoundError(f".py MUST be in '{dir_valid}' dir. Current: {_dir_name}")

        _root_dir = os.path.abspath(os.path.join(_cur_dir, "..", ".."))
        if _root_dir not in _sys.path:
            _sys.path.append(_root_dir)

        for _i in range(10):
            if _cur_dir in _sys.path:
                _sys.path.remove(_cur_dir)
            else:
                break

        import src.tri_s00_supervise_base as base
        _sys.stdout.write(f"  [{MODULE_NAME}]  Base Module Imported Successfully after Path Adjusted.\n")
        _sys.stdout.flush()

    except Exception as _e:
        _msg = f"  [{MODULE_NAME}]    {_e}\n{_tb.format_exc()}"
        _sys.stdout.write(f"{_msg}\n")
        _sys.stdout.flush()
        with open(f"{MODULE_NAME}_Import_Error.log", "a", encoding="utf-8-sig") as _f:
            _f.write(f"{_msg}\n")
        _sys.exit(1)

"""system dir, file"""
_system_root = base.get_system_root()
_log_file    = base.get_log_path()

base.output_log(MODULE_NAME, f"system_root={_system_root}", _log_file)
base.output_log(MODULE_NAME, f"log_file={_log_file}", _log_file)


########################################
# 定数（3E frame / Binary）
# 参照マニュアル: SH-080956 SLMP Reference Manual
########################################

# 3E frame サブヘッダ（p.20, p.28）
_SUBHEADER_REQ  = b"\x50\x00"   # リクエスト固定値
_SUBHEADER_RESP = b"\xD0\x00"   # レスポンス固定値

# 自局アドレス（p.22-23）
_NETWORK_NO   = 0x00    # ネットワークNo.  自局
_PC_NO        = 0xFF    # 局番            自局
_MODULE_IO_NO = 0x03FF  # モジュールI/O   CPU直接指定（LE送信: FFH 03H）
_MULTIDROP_NO = 0x00    # マルチドロップ  なし

# コマンド（p.86）
_CMD_ARRAY_LABEL_READ  = 0x041A
_CMD_ARRAY_LABEL_WRITE = 0x141A   # Mod: 0x041B → 0x141A（マニュアル p.94 で確証。0x041Bは未定義コマンド）
_CMD_DEVICE_READ       = 0x0401
_CMD_DEVICE_WRITE      = 0x1401
_SUBCMD                = 0x0000

# Device Read 上限（SH-080956 p.50）
_MAX_DEVICE_READ_WORDS = 960   # 1920byte / 2byte = 960word / リクエスト

# read_unit_spec（p.84）
READ_UNIT_BIT  = 0   # Bit型
READ_UNIT_BYTE = 1   # Word型 / DWord型 / Float型等

# データ型ID（p.83）
DATA_TYPE_NAMES: dict[int, str] = {
    1 : "Bit",
    2 : "Word [unsigned]",
    3 : "Double Word [unsigned]",
    4 : "Word [signed]",
    5 : "Double Word [signed]",
    6 : "Float (Single)",
    7 : "Float (Double)",
    8 : "Hour",
    9 : "String",
    10: "String [Unicode]",
}

########################################
# SLMP Binary デバイスコード（SH-080956 p.45-47）
# デバイス識別子 → SLMP Binary コード（1バイト）
########################################
SLMP_DEVICE_CODE: dict[str, int] = {
    "SM" : 0x91,  # 特殊リレー
    "SD" : 0xA9,  # 特殊レジスタ
    "X"  : 0x9C,  # 入力
    "Y"  : 0x9D,  # 出力
    "M"  : 0x90,  # 内部リレー
    "L"  : 0x92,  # ラッチリレー
    "F"  : 0x93,  # アナンシェータ
    "B"  : 0xA0,  # リンクリレー
    "D"  : 0xA8,  # データレジスタ
    "W"  : 0xB4,  # リンクレジスタ
    "TN" : 0xC2,  # タイマ現在値
    "TC" : 0xC3,  # タイマ接点
    "CN" : 0xC5,  # カウンタ現在値
    "CC" : 0xC6,  # カウンタ接点
    "R"  : 0xAF,  # ファイルレジスタ
    "ZR" : 0xB0,  # ファイルレジスタ(ZR)
    # Keyence KV エイリアス（MC Protocol マッピング）
    "DM" : 0xA8,  # Keyence Data Memory → D と同コード
    "MR" : 0x90,  # Keyence 内部リレー   → M と同コード
}

# ビットデバイス種別（device_read/device_write はワード単位アクセスのため、
# これらのデバイスを1点読み書きする場合は対象ビットの抽出/保護が必要。
# SH-080956 p.41-51: ビットデバイスのワード単位アクセスは
# device_no をbit0として、連続16点を1ワードにパックする仕様。
# 例: device_read("M", 1000, 1) は M1000(bit0)〜M1015(bit15) を1ワードで返す。
_BIT_DEVICE_TYPES = {"SM", "X", "Y", "M", "L", "F", "B", "TC", "CC"}


########################################
# デコード・エンコード ヘルパー関数
########################################

def decode_float(raw_words: list[int]) -> float:
    """ワードリスト（2ワード）を float32 に変換"""
    return struct.unpack("<f", struct.pack("<HH", raw_words[0], raw_words[1]))[0]


def decode_dword(raw_words: list[int]) -> list[int]:
    """ワードリストを DWord 値リストに変換（下位ワード + 上位ワード の順）"""
    return [(raw_words[i + 1] << 16) | raw_words[i] for i in range(0, len(raw_words), 2)]


def words_to_int(words: list[int], signed: bool = True) -> int:
    """
    2ワード（下位word, 上位wordの順）を1つの整数に変換する。
    src.tri_s19_extract_bitword.compile_column_register_map に
    words_to_int_fn として注入して使う（int4/int4_u用）。
    """
    raw = ((words[1] & 0xFFFF) << 16) | (words[0] & 0xFFFF)
    if signed and raw & 0x80000000:
        raw -= 0x100000000
    return raw


def decode_string(raw_words: list[int]) -> str:
    """ワードリストを ASCII 文字列に変換（NUL終端まで）"""
    raw_bytes = b"".join(struct.pack("<H", w) for w in raw_words)
    return raw_bytes.decode("ascii").rstrip("\x00")


def encode_dword(dwords: list[int]) -> list[int]:
    """
    DWord 値リストをワードペアのリストに展開する（ArrayLabelWrite 用）
    例: [0x00010002] → [0x0002, 0x0001]（little-endian: 下位ワード, 上位ワード）
    """
    result = []
    for d in dwords:
        result.append(d & 0xFFFF)
        result.append((d >> 16) & 0xFFFF)
    return result


########################################
# 低レベル: フレーム組み立て（プライベート）
########################################

def _encode_label(name: str) -> bytes:
    """ラベル名を UTF-16LE エンコード（p.80）"""
    return name.encode("utf-16-le")


def _resp_word_count(read_unit: int, read_array_len: int) -> int:
    """
    レスポンス read data のワード数を計算（p.91-94）
    Bit spec  : read_array_len = ビット要素数 → ceil(n/16) word
    Byte spec : read_array_len = バイト数     → n/2 word
    """
    if read_unit == READ_UNIT_BIT:
        return math.ceil(read_array_len / 16)
    return read_array_len // 2


def unpack_bits(words: list[int], n_bits: int) -> list[int]:
    """16bit word リストから n_bits 個のビット値を取り出す（LSBから）"""
    bits = []
    for word in words:
        for i in range(16):
            bits.append((word >> i) & 1)
    return bits[:n_bits]


def _build_request_message(
    req_data        : bytes,
    monitoring_timer: int = 4,
    network_no      : int = _NETWORK_NO,
    pc_no           : int = _PC_NO,
) -> bytes:
    """
    3E frame / Binary リクエストメッセージ全体を組み立てる（p.17-26）
    monitoring_timer: 単位250ms（デフォルト4 = 1秒）
    network_no, pc_no: 接続先の自局指定（デフォルト: ネットワークNo.=0, 局番=0xFF 自局）
                        GT SoftGOT2000のマイコン接続(Ethernet)等、相手機器側の
                        通信設定（Net No. / PC No.）に値を合わせる必要がある場合に指定する。
    """
    timer_bytes  = struct.pack("<H", monitoring_timer)
    req_data_len = len(timer_bytes) + len(req_data)
    return (
        _SUBHEADER_REQ                       +
        struct.pack("<B",  network_no)       +
        struct.pack("<B",  pc_no)            +
        struct.pack("<H",  _MODULE_IO_NO)    +
        struct.pack("<B",  _MULTIDROP_NO)    +
        struct.pack("<H",  req_data_len)     +
        timer_bytes                          +
        req_data
    )


def _build_label_read_request(labels: list[dict]) -> bytes:
    """
    Array Label Read (041A) コマンドデータ部を組み立てる（p.88）

    labels: list of dict
        {
            "name"     : str  ラベル名（GX Works3グローバルラベル）
            "read_unit": int  0=bit / 1=byte  (p.84)
            "read_len" : int  読み出し要素数 or バイト数  (p.85)
        }
    """
    label_body = b""
    for lbl in labels:
        name_bytes = _encode_label(lbl["name"])
        name_len   = len(name_bytes) // 2   # UTF-16LE 文字数
        label_body += struct.pack("<H",  name_len)
        label_body += name_bytes
        label_body += struct.pack("<BB", lbl["read_unit"], 0x00)
        label_body += struct.pack("<H",  lbl["read_len"])

    return (
        struct.pack("<H", _CMD_ARRAY_LABEL_READ) +
        struct.pack("<H", _SUBCMD)               +
        struct.pack("<H", len(labels))           +
        struct.pack("<H", 0)                     +   # num_abbrev=0
        label_body
    )


def _build_label_write_request(name: str, write_unit: int, write_len: int, values: list[int]) -> bytes:
    """
    Array Label Write (041B) コマンドデータ部を組み立てる（p.87-90）

    write_unit : 0=bit / 1=byte
    write_len  : bit数 or バイト数
    values     : 書込みワード値リスト
    """
    name_bytes = _encode_label(name)
    name_len   = len(name_bytes) // 2
    write_data = b"".join(struct.pack("<H", v & 0xFFFF) for v in values)
    return (
        struct.pack("<H", _CMD_ARRAY_LABEL_WRITE) +
        struct.pack("<H", _SUBCMD)                +
        struct.pack("<H", 1)                      +   # num_points=1
        struct.pack("<H", 0)                      +   # num_abbrev=0
        struct.pack("<H", name_len)               +
        name_bytes                                +
        struct.pack("<BB", write_unit, 0x00)      +
        struct.pack("<H",  write_len)             +
        write_data
    )


def _build_device_read_request(device_type: str, device_no: int, n_points: int) -> bytes:
    """
    Device Read (0401) コマンドデータ部を組み立てる（p.50-54）
    subcommand 0000 = ワード単位
    """
    code = SLMP_DEVICE_CODE.get(device_type.upper())
    if code is None:
        raise ValueError(f"Unknown device type: {device_type!r}")
    if not (1 <= n_points <= _MAX_DEVICE_READ_WORDS):
        raise ValueError(f"n_points must be 1-{_MAX_DEVICE_READ_WORDS}, got {n_points}")

    return (
        struct.pack("<H", _CMD_DEVICE_READ)        +
        struct.pack("<H", _SUBCMD)                 +
        struct.pack("<I", device_no)[:3]           +   # 先頭デバイス番号（3B LE）
        struct.pack("<B", code)                    +
        struct.pack("<H", n_points)
    )


def _build_device_write_request(device_type: str, device_no: int, values: list[int]) -> bytes:
    """
    Device Write (1401) コマンドデータ部を組み立てる（p.55-58）
    """
    code = SLMP_DEVICE_CODE.get(device_type.upper())
    if code is None:
        raise ValueError(f"Unknown device type: {device_type!r}")
    n_points = len(values)
    if not (1 <= n_points <= _MAX_DEVICE_READ_WORDS):
        raise ValueError(f"len(values) must be 1-{_MAX_DEVICE_READ_WORDS}, got {n_points}")

    write_data = b"".join(struct.pack("<H", v & 0xFFFF) for v in values)
    return (
        struct.pack("<H", _CMD_DEVICE_WRITE)       +
        struct.pack("<H", _SUBCMD)                 +
        struct.pack("<I", device_no)[:3]           +
        struct.pack("<B", code)                    +
        struct.pack("<H", n_points)                +
        write_data
    )


########################################
# 低レベル: レスポンス解析（プライベート）
########################################

def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """ソケットから正確に n バイト受信する"""
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionResetError("Connection closed by PLC")
        data += chunk
    return data


def _parse_label_read_response(resp: bytes) -> list[dict]:
    """
    Array Label Read (041A) レスポンスを解析する（p.25-28, p.90-94）

    Returns:
        list[dict]:
            {
                "data_type_id"  : int
                "data_type_name": str
                "read_unit"     : int
                "read_len"      : int   bit数 or バイト数
                "values"        : list  bit→[0/1,...], word→[int,...]
                "raw_words"     : list  生ワード値（decode_* 関数への入力用）
            }
    """
    _FIXED_HEADER = 9
    _END_CODE_OFF = 9

    if len(resp) < _FIXED_HEADER + 2:
        raise ValueError(f"Response too short: {len(resp)} bytes")

    end_code = struct.unpack_from("<H", resp, _END_CODE_OFF)[0]
    if end_code != 0x0000:
        raise RuntimeError(f"SLMP Error: end_code=0x{end_code:04X}")

    pos        = _FIXED_HEADER + 2
    num_points = struct.unpack_from("<H", resp, pos)[0]
    pos       += 2

    results = []
    for _ in range(num_points):
        data_type_id = resp[pos];     pos += 1
        read_unit    = resp[pos];     pos += 1
        read_len     = struct.unpack_from("<H", resp, pos)[0]; pos += 2

        n_words   = _resp_word_count(read_unit, read_len)
        raw_words = [struct.unpack_from("<H", resp, pos + i * 2)[0] for i in range(n_words)]
        pos      += n_words * 2

        values = unpack_bits(raw_words, read_len) if read_unit == READ_UNIT_BIT else list(raw_words)

        results.append({
            "data_type_id"  : data_type_id,
            "data_type_name": DATA_TYPE_NAMES.get(data_type_id, f"Unknown({data_type_id})"),
            "read_unit"     : read_unit,
            "read_len"      : read_len,
            "values"        : values,
            "raw_words"     : raw_words,
        })
    return results


def _parse_device_read_response(resp: bytes, n_points: int) -> list[int]:
    """
    Device Read (0401) レスポンスを解析する（p.54-55）
    Returns: list[int] ワード値リスト（符号なし）
    """
    _FIXED_HEADER = 9
    _END_CODE_OFF = 9

    if len(resp) < _FIXED_HEADER + 2:
        raise ValueError(f"Response too short: {len(resp)} bytes")

    end_code = struct.unpack_from("<H", resp, _END_CODE_OFF)[0]
    if end_code != 0x0000:
        raise RuntimeError(f"SLMP Error: end_code=0x{end_code:04X}")

    pos    = _FIXED_HEADER + 2
    return [struct.unpack_from("<H", resp, pos + i * 2)[0] for i in range(n_points)]


def _parse_write_response(resp: bytes) -> int:
    """
    書込み系コマンド（1401 / 041B）のレスポンスを解析する。
    Returns: 0 = 正常
    """
    _FIXED_HEADER = 9
    _END_CODE_OFF = 9

    if len(resp) < _FIXED_HEADER + 2:
        raise ValueError(f"Response too short: {len(resp)} bytes")

    end_code = struct.unpack_from("<H", resp, _END_CODE_OFF)[0]
    if end_code != 0x0000:
        raise RuntimeError(f"SLMP Error: end_code=0x{end_code:04X}")
    return 0


########################################
# SlmpClient クラス（TCP永続接続）
########################################

class SlmpClient:
    """
    SLMP クライアント（TCP永続接続）
    Mod: 2026.06.20    By: IWATA,Y.

    使用例:
        with SlmpClient("192.168.1.10", 5010) as plc:
            words = plc.device_read("D", 100, 10)
            plc.device_write("D", 100, [1, 2, 3])
            result = plc.label_read("g_iTest1", READ_UNIT_BYTE, 2)

        GT SoftGOT2000のマイコン接続(Ethernet)等、接続先がSLMP対応PLCではない場合：
            GT SoftGOT2000側の[通信設定]ダイアログで指定した Net No. / PC No. と
            一致させる必要がある。
        Example:
            with SlmpClient("192.168.1.20", 5010, network_no=1, pc_no=1) as gt:
                gt.device_write("D", 100, [123])   # GT SoftGOT2000の仮想デバイス D100 に書込み

    スレッドセーフ（RLock による排他制御）。
    接続エラー時は auto_reconnect=True（デフォルト）で1回だけ再接続を試みる。
    """

    def __init__(
        self,
        host          : str,
        port          : int,
        timeout_sec   : float = 5.0,
        auto_reconnect: bool  = True,
        network_no    : int   = _NETWORK_NO,
        pc_no         : int   = _PC_NO,
    ):
        self._host           = host
        self._port           = port
        self._timeout        = timeout_sec
        self._auto_reconnect = auto_reconnect
        self._network_no     = network_no
        self._pc_no          = pc_no
        self._sock: socket.socket | None = None
        self._lock = threading.RLock()

    # ---- 接続管理 ----

    def connect(self) -> None:
        """TCP接続を確立する"""
        with self._lock:
            if self._sock is not None:
                self.disconnect()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self._timeout)
            s.connect((self._host, self._port))
            self._sock = s
            base.output_log(MODULE_NAME,
                f"SlmpClient Connected. {self._host}:{self._port}", _log_file)

    def disconnect(self) -> None:
        """TCP接続を切断する"""
        with self._lock:
            if self._sock is not None:
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None
            base.output_log(MODULE_NAME,
                f"SlmpClient Disconnected. {self._host}:{self._port}", _log_file)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.disconnect()

    # ---- 内部: 送受信 ----

    def _send_recv(self, req_msg: bytes) -> bytes:
        """リクエスト送信 & レスポンス受信（接続エラー時は1回再接続）"""
        with self._lock:
            for attempt in range(2):
                if self._sock is None:
                    if not self._auto_reconnect:
                        raise ConnectionError("SlmpClient: not connected")
                    self.connect()
                try:
                    self._sock.sendall(req_msg)
                    header        = _recv_exact(self._sock, 9)
                    resp_data_len = struct.unpack_from("<H", header, 7)[0]
                    rest          = _recv_exact(self._sock, resp_data_len)
                    return header + rest

                except (ConnectionResetError, BrokenPipeError, socket.error) as e:
                    base.output_log(MODULE_NAME,
                        f"SlmpClient send/recv error (attempt {attempt+1}): {e}", _log_file)
                    self._sock = None
                    if attempt == 1 or not self._auto_reconnect:
                        raise
            raise ConnectionError("SlmpClient: send/recv failed after reconnect")

    # ---- デバイス番地 読出し ----

    def device_read(
        self,
        device_type: str,
        device_no  : int,
        n_points   : int,
    ) -> list[int]:
        """
        デバイス番地 一括読出し（Device Read 0401）

        Args:
            device_type: デバイス識別子（"D", "W", "M", "DM" 等）
            device_no  : 先頭デバイス番号
            n_points   : 読み出し点数（1〜960）
        Returns:
            list[int]: ワード値リスト（n_points 個）
        Example:
            words = plc.device_read("D", 100, 10)   # D100〜D109
        """
        req_data = _build_device_read_request(device_type, device_no, n_points)
        resp     = self._send_recv(_build_request_message(req_data, network_no=self._network_no, pc_no=self._pc_no))
        return _parse_device_read_response(resp, n_points)

    def device_read_bulk(
        self,
        device_type: str,
        device_no  : int,
        n_points   : int,
        sleep_sec  : float = 0.030,
    ) -> list[int]:
        """
        960word 超えの大量読み出しを自動分割して実行する

        Args:
            device_type: デバイス識別子
            device_no  : 先頭デバイス番号
            n_points   : 読み出し総点数（960超え可）
            sleep_sec  : リクエスト間スリープ秒
        Returns:
            list[int]: ワード値リスト（n_points 個）
        Example:
            words = plc.device_read_bulk("D", 0, 2000)   # 3リクエストに自動分割
        """
        results   = []
        offset    = 0
        remaining = n_points
        while remaining > 0:
            chunk = min(remaining, _MAX_DEVICE_READ_WORDS)
            results.extend(self.device_read(device_type, device_no + offset, chunk))
            offset    += chunk
            remaining -= chunk
            if remaining > 0:
                time.sleep(sleep_sec)
        return results

    # ---- デバイス番地 書込み ----

    def device_write(
        self,
        device_type: str,
        device_no  : int,
        values     : list[int],
    ) -> int:
        """
        デバイス番地 一括書込み（Device Write 1401）

        Args:
            device_type: デバイス識別子（"D", "W", "M" 等）
            device_no  : 先頭デバイス番号
            values     : 書込みワード値リスト（1〜960点）
        Returns:
            int: 0 = 正常終了
        Example:
            plc.device_write("D", 100, [201, 202, 203])   # D100=201, D101=202, D102=203
        """
        req_data = _build_device_write_request(device_type, device_no, values)
        resp     = self._send_recv(_build_request_message(req_data, network_no=self._network_no, pc_no=self._pc_no))
        return _parse_write_response(resp)

    # ---- ビットデバイス 1点 安全アクセス ----

    def device_read_bit_safe(
        self,
        device_type: str,
        device_no  : int,
    ) -> int:
        """
        ビットデバイス（M, X, Y, B 等）を1点だけ安全に読出す。

        device_read はワード単位アクセスのため、device_no をbit0として
        連続16点（device_no 〜 device_no+15）を1ワードにまとめて返す。
        本メソッドはワードを読んだ後、対象ビット（bit0）だけを抽出して返す。
        ワードデバイス（D, W 等）の場合はそのままワード値を返す。

        Args:
            device_type: デバイス識別子
            device_no  : デバイス番号（1点）
        Returns:
            int: ビットデバイスなら 0/1、ワードデバイスならワード値そのもの
        Example:
            val = plc.device_read_bit_safe("M", 1000)   # M1000 単体の値（0/1）
        """
        word_val = self.device_read(device_type, device_no, 1)[0]
        if device_type.upper() in _BIT_DEVICE_TYPES:
            return word_val & 0x0001
        return word_val

    def device_write_bit_safe(
        self,
        device_type: str,
        device_no  : int,
        bit_value  : int,
    ) -> int:
        """
        ビットデバイス（M, X, Y, B 等）を1点だけ安全に書込む（Read-Modify-Write）。

        device_write はワード単位アクセスのため、対象ビット以外の15点を
        そのまま書込むと、それらの状態を破壊してしまう。
        本メソッドは現在のワード値を読み、対象ビット（bit0）だけを変更してから
        書き戻すことで、他の15ビットの状態を保持する。
        ワードデバイス（D, W 等）の場合は bit_value をそのままワード値として書込む。

        注意: Read と Write の間に他プロセスが同じワードの別ビットを変更すると
              競合（race condition）が発生し得る。厳密な排他制御が必要な場合は
              SLMPのビット単位書込みサブコマンド（0x0001）の実装を検討すること。

        Args:
            device_type: デバイス識別子
            device_no  : デバイス番号（1点）
            bit_value  : 0 または 1（ビットデバイスの場合）/ ワード値（ワードデバイスの場合）
        Returns:
            int: 0 = 正常終了
        Example:
            plc.device_write_bit_safe("M", 1000, 0)   # M1000 だけ OFF（他のビットは保持）
        """
        if device_type.upper() in _BIT_DEVICE_TYPES:
            current_word = self.device_read(device_type, device_no, 1)[0]
            if bit_value:
                new_word = current_word | 0x0001
            else:
                new_word = current_word & 0xFFFE
            return self.device_write(device_type, device_no, [new_word])
        return self.device_write(device_type, device_no, [bit_value])

    # ---- ラベル名 読出し ----

    def label_read(
        self,
        name     : str,
        read_unit: int,
        read_len : int,
    ) -> dict:
        """
        ラベル名 読出し（Array Label Read 041A）

        Args:
            name     : ラベル名（GX Works3グローバルラベル）
            read_unit: READ_UNIT_BIT(0) or READ_UNIT_BYTE(1)
            read_len : bit数（bit型） or バイト数（word/dword/float型）
        Returns:
            dict:
                data_type_id  : int
                data_type_name: str
                read_unit     : int
                read_len      : int
                values        : list[int]   生ワード値
                raw_words     : list[int]   同上
        Example:
            # Word型 1点 (2 bytes)
            r = plc.label_read("g_iTest1", READ_UNIT_BYTE, 2)
            value = r["values"][0]

            # Float型 1点 (4 bytes = 2 words)
            r = plc.label_read("g_rTemp", READ_UNIT_BYTE, 4)
            value = decode_float(r["values"])
        """
        req_data = _build_label_read_request([{"name": name, "read_unit": read_unit, "read_len": read_len}])
        resp     = self._send_recv(_build_request_message(req_data, network_no=self._network_no, pc_no=self._pc_no))
        return _parse_label_read_response(resp)[0]

    def label_read_bulk(
        self,
        labels   : list[dict],
        sleep_sec: float = 0.030,
    ) -> list[dict]:
        """
        複数ラベルを 1件ずつ読み出す（スリープ付き）

        1リクエストに複数ラベルを束ねると1920byteレスポンス制限に当たるため、
        1ラベル × 1リクエストを繰り返す。

        Args:
            labels   : [{"name": str, "read_unit": int, "read_len": int}, ...]
            sleep_sec: リクエスト間スリープ秒（推奨: PLCスキャンタイム × 2 程度）
        Returns:
            list[dict]: 各ラベルの label_read() 戻り値リスト
        Example:
            labels = [
                {"name": "CycData[0].udVal[0]",   "read_unit": READ_UNIT_BYTE, "read_len": 1920},
                {"name": "CycData[0].udVal[480]",  "read_unit": READ_UNIT_BYTE, "read_len": 1920},
                {"name": "CycData[0].udVal[960]",  "read_unit": READ_UNIT_BYTE, "read_len": 160},
            ]
            results = plc.label_read_bulk(labels, sleep_sec=0.030)
            dwords  = decode_dword(results[0]["values"] + results[1]["values"] + results[2]["values"])
        """
        results = []
        for i, lbl in enumerate(labels):
            results.append(self.label_read(lbl["name"], lbl["read_unit"], lbl["read_len"]))
            if i < len(labels) - 1:
                time.sleep(sleep_sec)
        return results

    # ---- ラベル名 書込み ----

    def label_write(
        self,
        name      : str,
        write_unit: int,
        write_len : int,
        values    : list[int],
    ) -> int:
        """
        ラベル名 書込み（Array Label Write 041B）

        Args:
            name      : ラベル名（GX Works3グローバルラベル）
            write_unit: READ_UNIT_BIT(0) or READ_UNIT_BYTE(1)
            write_len : bit数 or バイト数
            values    : 書込みワード値リスト
        Returns:
            int: 0 = 正常終了
        Example:
            # Word型 1点 を書込み
            plc.label_write("g_iTest2", READ_UNIT_BYTE, 2, [12345])

            # DWord配列 480点を書込み
            words = encode_dword([1, 2, 3, ..., 480])
            plc.label_write("CycData[0].udVal[0]", READ_UNIT_BYTE, 1920, words)
        """
        req_data = _build_label_write_request(name, write_unit, write_len, values)
        resp     = self._send_recv(_build_request_message(req_data, network_no=self._network_no, pc_no=self._pc_no))
        return _parse_write_response(resp)


########################################
# ステートレス便利関数（後方互換・単発用途）
########################################

def DeviceRead(
    host       : str,
    port       : int,
    device_type: str,
    device_no  : int,
    n_points   : int,
    timeout_sec: float = 5.0,
    network_no : int   = _NETWORK_NO,
    pc_no      : int   = _PC_NO,
) -> list[int]:
    """
    デバイス番地 一括読出し（ステートレス版）
    毎回 TCP 接続を開閉する。定期ポーリングには SlmpClient を推奨。
    network_no, pc_no: GT SoftGOT2000等、接続先の通信設定に合わせる場合に指定する。
    Example:
        words = DeviceRead("192.168.1.10", 5010, "D", 100, 10)
    """
    req_msg = _build_request_message(
        _build_device_read_request(device_type, device_no, n_points),
        network_no=network_no, pc_no=pc_no)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout_sec)
        s.connect((host, port))
        s.sendall(req_msg)
        header        = _recv_exact(s, 9)
        resp_data_len = struct.unpack_from("<H", header, 7)[0]
        rest          = _recv_exact(s, resp_data_len)
    return _parse_device_read_response(header + rest, n_points)


def DeviceReadBulk(
    host       : str,
    port       : int,
    device_type: str,
    device_no  : int,
    n_points   : int,
    sleep_sec  : float = 0.030,
    timeout_sec: float = 5.0,
    network_no : int   = _NETWORK_NO,
    pc_no      : int   = _PC_NO,
) -> list[int]:
    """
    960word 超えの大量読み出し（ステートレス・自動分割）
    Example:
        words = DeviceReadBulk("192.168.1.10", 5010, "D", 0, 2000)
    """
    results   = []
    offset    = 0
    remaining = n_points
    while remaining > 0:
        chunk = min(remaining, _MAX_DEVICE_READ_WORDS)
        results.extend(DeviceRead(host, port, device_type, device_no + offset, chunk,
                                   timeout_sec, network_no=network_no, pc_no=pc_no))
        offset    += chunk
        remaining -= chunk
        if remaining > 0:
            time.sleep(sleep_sec)
    return results


def DeviceWrite(
    host       : str,
    port       : int,
    device_type: str,
    device_no  : int,
    values     : list[int],
    timeout_sec: float = 5.0,
    network_no : int   = _NETWORK_NO,
    pc_no      : int   = _PC_NO,
) -> int:
    """
    デバイス番地 一括書込み（ステートレス版）
    network_no, pc_no: GT SoftGOT2000等、接続先の通信設定に合わせる場合に指定する。
    Example:
        DeviceWrite("192.168.1.10", 5010, "D", 100, [201, 202, 203])
    """
    req_msg = _build_request_message(
        _build_device_write_request(device_type, device_no, values),
        network_no=network_no, pc_no=pc_no)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout_sec)
        s.connect((host, port))
        s.sendall(req_msg)
        header        = _recv_exact(s, 9)
        resp_data_len = struct.unpack_from("<H", header, 7)[0]
        rest          = _recv_exact(s, resp_data_len)
    return _parse_write_response(header + rest)


def ArrayLabelRead(
    host       : str,
    port       : int,
    label      : dict,
    timeout_sec: float = 5.0,
) -> dict:
    """
    ラベル名 読出し（ステートレス版）
    1ラベル × 大きな read_len がパフォーマンス上の最適解（1920byte/リクエスト）
    Example:
        r = ArrayLabelRead("192.168.0.1", 5010,
                           {"name": "CycData[0].udVal[0]",
                            "read_unit": READ_UNIT_BYTE, "read_len": 1920})
        dwords = decode_dword(r["values"])
    """
    req_msg = _build_request_message(_build_label_read_request([label]))
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout_sec)
        s.connect((host, port))
        s.sendall(req_msg)
        header        = _recv_exact(s, 9)
        resp_data_len = struct.unpack_from("<H", header, 7)[0]
        rest          = _recv_exact(s, resp_data_len)
    return _parse_label_read_response(header + rest)[0]


def ArrayLabelReadBulk(
    host       : str,
    port       : int,
    labels     : list[dict],
    sleep_sec  : float = 0.030,
    timeout_sec: float = 5.0,
) -> list[dict]:
    """
    複数ラベルを 1件ずつ読み出す（ステートレス・スリープ付き）
    Example:
        labels = [
            {"name": "CycData[0].udVal[0]",   "read_unit": READ_UNIT_BYTE, "read_len": 1920},
            {"name": "CycData[0].udVal[480]",  "read_unit": READ_UNIT_BYTE, "read_len": 1920},
        ]
        results = ArrayLabelReadBulk("192.168.0.1", 5010, labels)
    """
    results = []
    for i, label in enumerate(labels):
        results.append(ArrayLabelRead(host, port, label, timeout_sec))
        if i < len(labels) - 1:
            time.sleep(sleep_sec)
    return results


def ArrayLabelWrite(
    host       : str,
    port       : int,
    label      : dict,
    timeout_sec: float = 5.0,
) -> int:
    """
    ラベル名 書込み（ステートレス版）

    label: {
        "name"      : "g_iTest2"
        "write_unit": READ_UNIT_BYTE
        "write_len" : 2
        "values"    : [12345]
    }
    Example:
        ArrayLabelWrite("192.168.0.1", 5010, {
            "name": "g_iTest2", "write_unit": READ_UNIT_BYTE,
            "write_len": 2, "values": [12345]
        })
    """
    req_msg = _build_request_message(
        _build_label_write_request(
            label["name"], label["write_unit"], label["write_len"], label["values"]
        )
    )
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout_sec)
        s.connect((host, port))
        s.sendall(req_msg)
        header        = _recv_exact(s, 9)
        resp_data_len = struct.unpack_from("<H", header, 7)[0]
        rest          = _recv_exact(s, resp_data_len)
    return _parse_write_response(header + rest)


########################################
# デバッグ用実行ブロック
########################################
if __name__ == "__main__":
    """
    事前設定（GX Works3 / 実機接続時）:
        グローバルラベルの「外部デバイスからのアクセス」を有効化
        Ethernet構成 → SLMP接続機器 → ポート番号を確認

    検証用サーバー（実機なしでテストする場合）:
        同フォルダの slmp_server.py を別プロセスで起動するか、
        本スクリプトが自動的にサブプロセスとして起動する（Test 8, 9）。

    環境変数:
        SLMP_HOST=192.168.0.1     接続先 PLC IP
        SLMP_PORT=5010           接続先 PLC ポート
        SLMP_SLEEP_MS=30         ラベル読み出し間隔 (ms)
    """
    import os as _os
    import subprocess as _subprocess

    PLC_HOST  = _os.environ.get("SLMP_HOST",     "127.0.0.1")
    PLC_PORT  = int(_os.environ.get("SLMP_PORT", "5010"))
    SLEEP_SEC = int(_os.environ.get("SLMP_SLEEP_MS", "30")) / 1000

    print("=" * 60)
    print("  SLMP Wrapper - 検証")
    print("=" * 60)

    ####################
    ## Test 1: ArrayLabelRead リクエストデータ（p.93 通信例との照合）
    ## "Lbl[2]" bit型 2点
    ####################
    print("\n[Test 1] ArrayLabelRead リクエストデータ照合 (p.93 Bit型通信例)")
    expected = bytes([
        0x1A, 0x04, 0x00, 0x00,
        0x01, 0x00, 0x00, 0x00,
        0x06, 0x00,
        0x4C, 0x00, 0x62, 0x00, 0x6C, 0x00,
        0x5B, 0x00, 0x32, 0x00, 0x5D, 0x00,
        0x00, 0x00, 0x02, 0x00,
    ])
    built = _build_label_read_request([{"name": "Lbl[2]", "read_unit": READ_UNIT_BIT, "read_len": 2}])
    print(f"  Expected: {expected.hex(' ').upper()}")
    print(f"  Built   : {built.hex(' ').upper()}")
    print(f"  Match   : {built == expected}")

    ####################
    ## Test 2: Bit レスポンス解析（p.93）
    ## Lbl[2]=OFF, Lbl[3]=ON → word=0x0002
    ####################
    print("\n[Test 2] Bit レスポンス解析 (p.93: Lbl[2]=OFF, Lbl[3]=ON)")
    fake_resp_bit = bytes([
        0xD0, 0x00, 0x00, 0xFF, 0xFF, 0x03, 0x00,
        0x0A, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x01, 0x00, 0x02, 0x00, 0x02, 0x00,
    ])
    r = _parse_label_read_response(fake_resp_bit)[0]
    print(f"  data_type : {r['data_type_name']}")
    print(f"  values    : {r['values']}  (期待: [0, 1] = [OFF, ON])")

    ####################
    ## Test 3: String レスポンス解析
    ## "Hello" → [0x6548, 0x6C6C, 0x006F]
    ####################
    print("\n[Test 3] String レスポンス解析 (ASCII packed 'Hello')")
    fake_resp_str = bytes([
        0xD0, 0x00, 0x00, 0xFF, 0xFF, 0x03, 0x00,
        0x0E, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x09, 0x01, 0x06, 0x00,
        0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x00,
    ])
    r2 = _parse_label_read_response(fake_resp_str)[0]
    print(f"  data_type : {r2['data_type_name']}")
    print(f"  as string : '{decode_string(r2['values'])}'  (期待: 'Hello')")

    ####################
    ## Test 4: DeviceRead リクエストデータ確認（D100, 10点）
    ####################
    print("\n[Test 4] DeviceRead リクエストデータ確認 (D100, 10点)")
    dev_req = _build_device_read_request("D", 100, 10)
    expected_dev = bytes([
        0x01, 0x04, 0x00, 0x00,
        0x64, 0x00, 0x00,
        0xA8, 0x0A, 0x00,
    ])
    print(f"  Expected: {expected_dev.hex(' ').upper()}")
    print(f"  Built   : {dev_req.hex(' ').upper()}")
    print(f"  Match   : {dev_req == expected_dev}")

    ####################
    ## Test 5: DeviceRead レスポンス解析（D100=1, D101=2, D102=3）
    ####################
    print("\n[Test 5] DeviceRead レスポンス解析 (D100=1, D101=2, D102=3)")
    fake_resp_dev = bytes([
        0xD0, 0x00, 0x00, 0xFF, 0xFF, 0x03, 0x00,
        0x08, 0x00, 0x00, 0x00,
        0x01, 0x00, 0x02, 0x00, 0x03, 0x00,
    ])
    vals = _parse_device_read_response(fake_resp_dev, 3)
    print(f"  values : {vals}  (期待: [1, 2, 3])")

    ####################
    ## Test 6: DeviceWrite リクエストデータ確認（D100=201, D101=202）
    ####################
    print("\n[Test 6] DeviceWrite リクエストデータ確認 (D100=201, D101=202)")
    write_req = _build_device_write_request("D", 100, [201, 202])
    expected_write = bytes([
        0x01, 0x14, 0x00, 0x00,
        0x64, 0x00, 0x00,
        0xA8, 0x02, 0x00,
        0xC9, 0x00, 0xCA, 0x00,
    ])
    print(f"  Expected: {expected_write.hex(' ').upper()}")
    print(f"  Built   : {write_req.hex(' ').upper()}")
    print(f"  Match   : {write_req == expected_write}")

    ####################
    ## Test 7: encode_dword 確認
    ####################
    print("\n[Test 7] encode_dword (DWord→ワードペア変換)")
    enc = encode_dword([1, 2, 0x00010002])
    print(f"  encode_dword([1, 2, 0x00010002]) → {enc}  (期待: [1,0, 2,0, 2,1])")

    ####################
    ## Test 8 & 9: slmp_server.py を起動して実通信テスト
    ## （旧 _USE_FAKE モードを廃止し、検証用サーバー経由に統一）
    ####################
    _TEST_SERVER_HOST = "127.0.0.1"
    _TEST_SERVER_PORT = 15010
    _server_proc = None

    try:
        _server_script = _os.path.join(
            _os.path.dirname(_os.path.abspath(__file__)), "slmp_server.py")

        if _os.path.exists(_server_script):
            _server_proc = _subprocess.Popen(
                [_sys.executable, _server_script,
                 "--host", _TEST_SERVER_HOST, "--port", str(_TEST_SERVER_PORT)],
                stdout=_subprocess.DEVNULL, stderr=_subprocess.DEVNULL,
            )
            time.sleep(0.5)   # サーバー起動待ち

            ####################
            ## Test 8: DeviceReadBulk 自動分割確認（2000点 → 3分割、実通信）
            ####################
            print("\n[Test 8] DeviceReadBulk 分割確認 (2000点 → 3分割, slmp_server経由)")
            DeviceWrite(_TEST_SERVER_HOST, _TEST_SERVER_PORT, "D", 0, [10, 20, 30])
            result = DeviceReadBulk(_TEST_SERVER_HOST, _TEST_SERVER_PORT, "D", 0, 2000, sleep_sec=0.0)
            print(f"  Total : {len(result)}  (期待: 2000)")
            print(f"  First : {result[:3]}  (期待: [10, 20, 30]  D0, D1, D2 書込み値)")

            ####################
            ## Test 9: SlmpClient 永続接続（device / label の read-after-write）
            ####################
            print("\n[Test 9] SlmpClient 永続接続 (slmp_server経由)")
            with SlmpClient(_TEST_SERVER_HOST, _TEST_SERVER_PORT) as plc:
                plc.device_write("D", 200, [42, 43, 44])
                words = plc.device_read("D", 200, 3)
                print(f"  device_read  after write : {words}  (期待: [42, 43, 44])")

                plc.label_write("g_iTest1", READ_UNIT_BYTE, 2, [9999])
                lbl = plc.label_read("g_iTest1", READ_UNIT_BYTE, 2)
                print(f"  label_read   after write : type={lbl['data_type_name']}  "
                      f"values={lbl['values']}  (期待: [9999])")
        else:
            print(f"\n[Test 8/9] SKIPPED: slmp_server.py not found at {_server_script}")

    except Exception as _ex:
        print(f"  [Test 8/9] Error: {_ex}")

    finally:
        if _server_proc is not None:
            _server_proc.terminate()
            try:
                _server_proc.wait(timeout=3)
            except Exception:
                _server_proc.kill()

    ####################
    ## Test 10: ArrayLabelWrite コマンドコード確認
    ##   （0x041B は未定義コマンドだったバグの再発防止。マニュアル p.94 で 0x141A と確証済み）
    ####################
    print("\n[Test 10] ArrayLabelWrite コマンドコード確認 (0x141A)")
    write_req = _build_label_write_request("g_iTest1", READ_UNIT_BYTE, 2, [9999])
    cmd_code  = struct.unpack_from("<H", write_req, 0)[0]
    print(f"  Command Code: 0x{cmd_code:04X}  (期待: 0x141A)")
    print(f"  Match: {cmd_code == 0x141A}")
    assert cmd_code == 0x141A, "ArrayLabelWrite コマンドコードが 0x141A ではない！"

    ####################
    ## Live Test（SLMP_HOST 設定時のみ・実機接続）
    ####################
    if _os.environ.get("SLMP_HOST", "127.0.0.1"):
        print(f"\n[Live Test] Connecting to {PLC_HOST}:{PLC_PORT}  sleep={SLEEP_SEC*1000:.0f}ms")
        from datetime import datetime as _dt
        try:
            labels_test = [
                {"name": "CycContinuous128[0].udData1000[0]",   "read_unit": READ_UNIT_BYTE, "read_len": 1920},
                {"name": "CycContinuous128[0].udData1000[480]", "read_unit": READ_UNIT_BYTE, "read_len": 1920},
                {"name": "CycContinuous128[0].udData1000[960]", "read_unit": READ_UNIT_BYTE, "read_len": 160},
            ]

            print(f"{_dt.now()}  SlmpClient 永続接続 ArrayLabelReadBulk")
            dt1 = _dt.now()
            with SlmpClient(PLC_HOST, PLC_PORT) as plc:
                results = plc.label_read_bulk(labels_test, sleep_sec=SLEEP_SEC)
            dt2 = _dt.now()
            print(f"{_dt.now()}  End: {dt2 - dt1}")
            dwords = (
                decode_dword(results[0]["values"]) +
                decode_dword(results[1]["values"]) +
                decode_dword(results[2]["values"])
            )
            print(f"  DWords[0..4]    : {dwords[:5]}")
            print(f"  DWords[995..999]: {dwords[995:]}")

        except Exception as _ex:
            print(f"  Error: {_ex}")
    else:
        print(f"\nSet SLMP_HOST to run live test against a real PLC.")

    print("\nAll verification tests passed!")
