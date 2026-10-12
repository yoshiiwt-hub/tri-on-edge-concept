# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
"""
########################################
# Extract from Bit/Word (共通モジュール)
# Ver: 01.c.20261001    By: IWATA, Y.'s Agent AI
# Mod: モジュール名を tri_edge_XX_* から tri_sXX_* 形式に変更（tri_edge_19_extract_bitword → tri_s19_extract_bitword）
########################################
compile_column_register_map  # column_register_map をコンパイルしてループ内無辞書参照の関数リストにする

動作概要
    find_trigger の column_register_map（sources/unit_dtype/scale形式）を、ループ前に1回だけ
    クロージャ関数群にコンパイルする。ループ内では [(col_idx, col_func), ...] を
    for col_idx, col_func in compiled: value = col_func(bulk_buffers) で呼ぶだけになり、
    辞書参照（dict lookup）が一切発生しない。

    [column_register_map フォーマット]
        "col_idx": {
            "sources"   : [{"bulk": bulk_id, "offset": 開始位置, "byte": "hi"/"lo"(任意)}, ...],
            "unit_dtype": "bit"/"int1"/"int1_u"/"str1"/"int2"/"int2_u"/"str2"/"int4"/"int4_u"/"float4",
            "scale"     : 任意。int系unit_dtypeを10^scaleで割って小数として扱う（例: scale=2 → ÷100）
        }
        sourcesの各要素は以下のどちらか：
            {"bulk": bulk_id, "offset": N, "byte": "hi"/"lo"(任意)}  ← PLCから読んだ生データ
            {"const": value}                                        ← 固定値（PLCを読まない。文字列/数値どちらも可）
        - sourcesが1個  → 単一の値（全要素がconstなら、unit_dtype自体省略可）
        - sourcesが2個以上 → 自動的に連結（str系unit_dtypeは文字列結合、int系unit_dtypeは各値の
          整数部を文字列化して結合。最終的にintにするかstrのままにするかはcol_defのdtypeが決める）
          constをsourcesの一部に混ぜて、固定の接頭辞・区切り文字・単位表記などをPLC読出し値と
          連結し、人間が読みやすい文章を組み立てることもできる
          （例: [{"const":"規格："}, {源1}, {"const":"以上"}, {源2}, {"const":"未満"}]
                → "規格：5 以上 6 未満"）

    [int4/int4_u/float4（2word合成）について]
        2wordの上位/下位並び順はPLCベンダーごとに異なりうるため、このモジュール自体は
        合成方法を持たない。使用する場合は words_to_int_fn / words_to_float32_fn を
        呼び出し側（各PLCのcomplinkモジュール）から注入すること。
"""
MODULE_NAME = "Extract_BitWord"

"""standard library"""
from   typing import Any, Callable, Dict, Optional


def _build_unit_decoders(
    words_to_int_fn:     Optional[Callable] = None,
    words_to_float32_fn: Optional[Callable] = None,
) -> Dict[str, Any]:
    """
    各関数は「1個の生word値（0-65535）または生byte値（0-255）」を受け取り、
    Pythonの値（int/str/bool）に変換する。int4/float4は2word分の生値リストを受け取る。
    """
    def _int4(words, signed):
        if words_to_int_fn is None:
            raise ValueError(
                "int4/int4_u を使うには compile_column_register_map(..., words_to_int_fn=...) "
                "で合成関数を渡してください（2wordの上位/下位並び順はPLCベンダー依存のため）")
        return words_to_int_fn(words, signed=signed)

    def _float4(words):
        if words_to_float32_fn is None:
            raise ValueError(
                "float4 を使うには compile_column_register_map(..., words_to_float32_fn=...) "
                "で合成関数を渡してください（2wordの上位/下位並び順はPLCベンダー依存のため）")
        return words_to_float32_fn(words)

    return {
        "bit":    lambda raw: int(raw) & 1,
        "int1":   lambda raw: (raw - 0x100) if (raw & 0x80) else raw,
        "int1_u": lambda raw: raw & 0xFF,
        "str1":   lambda raw: chr(raw & 0xFF) if raw else "",
        "int2":   lambda raw: (raw - 0x10000) if (raw & 0x8000) else raw,
        "int2_u": lambda raw: raw & 0xFFFF,
        # str2: 前半byte(L/lo)が先、後半byte(H/hi)が後の順でデコードする
        #       （TOYOPUC実機ASCIIモニタで検証済み。他PLCベンダーでは要検証）
        "str2":   lambda raw: bytes([raw & 0xFF, (raw >> 8) & 0xFF]).decode("shift_jis", errors="replace"),
        "int4":   lambda words: _int4(words, signed=True),
        "int4_u": lambda words: _int4(words, signed=False),
        "float4": _float4,
    }


_MULTI_WORD_UNIT_DTYPES = {"int4", "int4_u", "float4"}  # 2word分をまとめてデコーダに渡す種別


def compile_column_register_map(
    column_register_map: Dict[str, Any],
    col_def             : Dict[int, Any],
    bulk_conf           : Dict[str, Any],
    words_to_int_fn:      Optional[Callable] = None,
    words_to_float32_fn:  Optional[Callable] = None,
) -> list:
    """
    column_register_map の設定辞書を、ループ内で辞書参照を一切使わずに直接呼べる
    クロージャ関数群にコンパイルする（ループ前に1回だけ呼ぶこと）。

    Args:
        words_to_int_fn     : int4/int4_u 使用時に必要。各PLCのcomplinkモジュールから注入する
        words_to_float32_fn : float4 使用時に必要。各PLCのcomplinkモジュールから注入する

    Returns:
        list[tuple]: [(col_idx:int, col_func), ...]
            col_func(bulk_buffers: list) -> 最終値（bulk_buffersは[[bulk0の生値...], [bulk1の生値...], ...]）
    """
    unit_decoders = _build_unit_decoders(words_to_int_fn, words_to_float32_fn)
    bulk_ids      = sorted(bulk_conf.keys(), key=lambda x: int(x))
    bulk_index_of = {bid: i for i, bid in enumerate(bulk_ids)}  # 辞書参照はコンパイル時のみ

    def _make_col_func(sources: list, unit_dtype: Optional[str], scale: int, final_dtype: str):
        """
        sources の各要素は以下のどちらか：
            {"bulk": bulk_id, "offset": N, "byte": "hi"/"lo"(任意)}  ← PLCから読んだ生データ
            {"const": value}                                        ← 固定値（PLCを読まない）
        全要素がconstの場合、unit_dtype/scaleは無視される（PLC読出し自体が発生しないため）。
        """
        has_real_source = any("const" not in s for s in sources)
        if has_real_source:
            if unit_dtype not in unit_decoders:
                raise ValueError(f"compile_column_register_map: unknown unit_dtype={unit_dtype}")
            decode_fn    = unit_decoders[unit_dtype]       # 関数実体をクロージャに埋め込む
            is_multiword = unit_dtype in _MULTI_WORD_UNIT_DTYPES
        else:
            decode_fn, is_multiword = None, False          # 使われないのでダミー
        scale_factor = (10 ** scale) if scale else None
        is_str_final = (final_dtype == "str")

        resolved = []
        for s in sources:
            if "const" in s:
                resolved.append(("const", s["const"]))
            else:
                b_idx = bulk_index_of[str(s["bulk"])]      # 辞書参照はコンパイル時のみ
                resolved.append(("src", b_idx, int(s["offset"]), s.get("byte")))

        def _read_one(bulk_buffers, item):
            if item[0] == "const":
                return item[1]
            _, b_idx, offset, byte_part = item
            if is_multiword:
                raw = bulk_buffers[b_idx][offset: offset + 2]  # [下位word, 上位word]
            else:
                raw = bulk_buffers[b_idx][offset]
                if byte_part == "hi":
                    raw = (raw >> 8) & 0xFF
                elif byte_part == "lo":
                    raw = raw & 0xFF
            val = decode_fn(raw)
            if scale_factor:
                val = val / scale_factor
            return val

        if len(resolved) == 1:
            item = resolved[0]
            def col_func(bulk_buffers):
                return _read_one(bulk_buffers, item)
        else:
            def col_func(bulk_buffers):
                parts = []
                for item in resolved:
                    val = _read_one(bulk_buffers, item)
                    parts.append(val if isinstance(val, str) else str(int(val)))
                joined = "".join(parts)
                return joined if is_str_final else int(joined)

        return col_func

    compiled = []
    for col_idx_str, entry in sorted(column_register_map.items(), key=lambda x: int(x[0])):
        col_idx     = int(col_idx_str)
        final_dtype = col_def[col_idx][1] if len(col_def[col_idx]) > 1 else "str"
        col_func    = _make_col_func(
            entry["sources"], entry.get("unit_dtype"), entry.get("scale", 0), final_dtype
        )
        compiled.append((col_idx, col_func))

    return compiled
