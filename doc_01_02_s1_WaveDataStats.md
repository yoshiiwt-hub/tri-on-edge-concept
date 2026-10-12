# Tri-On-Edge 波形統計量の算出と設定ガイド

- Ver: 01.e.20261006
- By: IWATA, Y.'s Agent AI
- Mod: Find の型変換の既定値（-9999.9 等）が有効値として集計される旨と、null 指定の推奨を追記

---

対象読者
- ユーザ（現場担当者・運用担当者）
- 開発者

---

上位文書
- Tri-On-Edge 波形切出しロジックと設定ガイド

---

## 1. 概要：エッジで「特徴量」に変換

　波形切出し（Segment）で取得した生データは、そのままクラウドやサーバへ送信すると、後処理コスト、通信帯域・ストレージのリソースが大きくなる。

　Tri-On-Edge の波形統計量算出機能は、**エッジ（現場側）で波形を集約・変換してから送信する**ことで、この課題を解決する。

```
【従来】
　生データ（n行） 　　　　　　　　　　　　 → 転送（n行） → 後処理

【Tri-On-Edge 統計量算出】
　生データ（n行） → 統計量算出（n行→1行） → 1行転送（1行） → 後処理
```

### エッジコンピューティングとしての有効性

| 観点 | 効果 |
|------|------|
| **通信コスト削減** | データ量（行数）を圧縮し、通信量を低減。工場ネットワーク・モバイル回線など低帯域環境にも適する |
| **リアルタイム性** | 判定・アラートをエッジで完結し、クラウド往復のレイテンシが不要 |
| **データ品質向上** | Null数・外れ値境界（σ）を除外（クレンジング）。AI/MLの学習に直結する特徴量を抽出 |

---

## 2. 処理の全体像

　後処理（Act Trigger）の一環として、1セグメント（n行）を受け取るたびに統計量を算出。以降、1行のデータセットとして処理を実行

```
┌─ Stream Core ────────────────────────────┐
│  ストリームデータ                          │
│      ↓ 波形切出し                         │
│  segment（N行）→ Act Queue                │
└──────────────────────────────────────────┘
         ↓
┌─ a_socket_client_trigger ────────────────┐
│  segment（n行）                           │
│      ↓                                   │
│  統計量 （1件）                           │
│      ↓                                   │
│  後処理                                   │
└──────────────────────────────────────────┘
```

### 出力 JSON の固定ヘッダ

　統計量に加え、以下のヘッダが常に付与される。

| index | キー | 内容 |
|----|------|------|
| 0 | `seg_id` | セグメントID（発生時刻ベースのユニーク値） |
| 1 | `seg_ts_start` | 波形の開始タイムスタンプ |
| 2 | `seg_ts_end` | 波形の終了タイムスタンプ（**最後のデータ行**の時刻。終了条件が成立した終点行の時刻ではない） |
| 3 | `seg_rows` | 波形の行数（終点行を除いた行数） |

※ 統計を算出する Act Trigger は `CUT_LAST_ROW = True` のため、Stream Core が終点行を除外したセグメントから算出する
（波形切出しガイド「6. 終点行の扱い」参照）。`seg_ts_end - seg_ts_start` は、最初と最後のデータ行の時刻差になる
（終了条件の成立時刻までの長さより、1サンプル周期分短い）。

---

## 3. 統計量一覧

### 3-1. 基本統計（対象：数値列）

| stat 名 | 内容 | 補足 |
|---------|------|------|
| `mean` | 平均値 | float変換できた有効値のみ対象 |
| `med` | 中央値 | 偶数行は中央2値の平均 |
| `min` | 最小値 | |
| `max` | 最大値 | |
| `range` | 範囲（max − min） | 変動幅の把握に |
| `std` | 標準偏差 | 母標準偏差（n で除算） |
| `count` | 有効値件数 | float変換成功数 |
| `n_null` | Null件数 | float変換不可の行数。`count + n_null = seg_rows` |
| `sum` | 合計 | |

※ Find は、値が無い・型変換できない場合に、設定の既定値（`int_default` / `float_default`。省略時は `-9999` / `-9999.9`）を入れて送る。
この既定値は float に変換できるため、**有効値として集計される**（`mean`・`min` 等が歪み、`n_null` にも数えられない）。
統計を算出する場合は、Find（`find_trigger`）の `int_default` / `float_default` を `null` にすることを推奨する。
`null` にすると欠損は None のまま届き、集計から除外され `n_null` に数えられる（`time_default` / `str_default` も `null` を指定できる）。

### 3-2. 分布・外れ値検出（対象：数値列）

| stat 名 | 内容 | params | 補足 |
|---------|------|--------|------|
| `iqr` | 四分位範囲（P75 − P25） | — | 外れ値の影響を受けにくい散布度 |
| `pertl` | パーセンタイル（線形補間） | `p` : int | 有効値: 1, 5, 10, 25, 75, 90, 95, 99 |
| `sig_u` | 上側シグマ境界（mean + n×std） | `n` : float | **1セグメント内**のばらつきの目安 |
| `sig_l` | 下側シグマ境界（mean − n×std） | `n` : float | **1セグメント内**のばらつきの目安 |

※ `sig_u` / `sig_l` は1つのセグメント内の値のばらつき（セグメント内の mean ± n×std）であり、SPC の管理限界（セグメント間＝ロット間・ショット間のばらつきから求める上限・下限）ではない。管理限界として使う場合は、複数セグメントの統計量から別途算出すること（diag_spc 等）。

### 3-3. 任意行取得（対象：数値列・文字列列）

| stat 名 | 内容 | params | 補足 |
|---------|------|--------|------|
| `first` | 先頭行の値（raw値） | — | 数値・文字列どちらも可。float 変換しない |
| `last` | 最終行の値（raw値） | — | 数値・文字列どちらも可。float 変換しない |
| `nth` | n番目の行の値（raw値） | `n` : int | 正数: 1始まり / 負数: 末尾から |
| `mode` | 最頻値 | — | None除外。タイは先着 |

### 3-4. セグメント全体（対象：セグメント行）

| stat 名 | 内容 | src_col_idx | 補足 |
|---------|------|-------------|------|
| `seg_len` | レコード数 | `-1`（固定） | — |
| `const` | 固定値（設定値をそのまま出力） | 任意（`-1` 等） | `[-1, "const", 値]` の形で記述。セグメント内容に依存しない（例: ラベル文字列・ライン名） |

---

## 4. 区間指定スライス

　統計量は**波形全体**に対して計算するだけでなく、**波形の一部区間**に絞って計算できる。

```
[0.0, 0.3]  → 波形の前半30%（立ち上がり区間など）
[0.5, 1.0]  → 波形の後半50%（定常区間など）
[0.2, 0.8]  → 波形の中間60%（過渡区間除去）
```

　スライスは全統計量（`seg_len` を除く）に適用可能。`nth` に組み合わせると、「定常区間に入ってN行目の値」なども取得できる。

```json
[6, "nth", [0.5, 1.0], 1]    ← 後半50%区間の先頭行
[6, "mean", [0.0, 0.3]]      ← 前半30%の平均
[6, "sig_u", [0.3, 0.7], 3]  ← 中間40%での +3σ境界
```

　スライス未指定または不正値（start ≥ end 等）の場合は、全行を対象とする。

---

## 5. 設定ファイルの構造

`trigger_setting_02.json` の `act_trigger` セクションに記述する。

### 5-1. 基本構造

```json
"act_trigger": {
    "module"    : "src.tri_s12_act",
    "func"      : "a_socket_client_trigger",
    "send_stats": true,

    "column_register_map": { ... },
    "out_coldef"         : { ... },
    "save_dir"           : "output"
}
```

| キー | 内容 | デフォルト |
|------|------|-----------|
| `send_stats` | `true`: 統計量送信 / `false`: 全行送信 | `true` |
| `column_register_map` | 統計量の計算定義（後述） | 必須 |
| `out_coldef` | 出力列の定義（後述） | 必須 |
| `save_dir` | `out_coldef` の保存先ディレクトリ | `"output"` |

### 5-2. column_register_map のエントリ形式

```
"index": [src_col_idx, "stat_name"]
"index": [src_col_idx, "stat_name", [start, end]]          ← スライスのみ
"index": [src_col_idx, "stat_name", param]                 ← パラメータのみ
"index": [src_col_idx, "stat_name", [start, end], param]   ← 両方
```

- `src_col_idx`：入力データの列番号（`col_def` のインデックスと対応）
- `stat_name`：統計量名（第3節参照）
- `seg_len` を使う場合のみ `src_col_idx = -1`（列不要の番兵）

### 5-3. out_coldef のエントリ形式

```json
"index": ["出力キー名", "dtype", "role", "var_type", "説明"]
```

- `column_register_map` と同じ `index` で対応させること
- `dtype`：`"float"` / `"int"` / `"str"` / `"time"`
- `role`：`"key"` / `"numetory"` / `"category"`
- `var_type`：`"key"` / `"explanatory"` / `"objective"`

　`out_coldef` は起動時に `save_dir` 配下へ JSON ファイルとして保存される。サーバー側はこのファイルを読み込んでスキーマを構成できる。

---

## 6. 設定例

### パターンA：基本統計量

シナリオ：「主要な統計量を全区間で算出する」

```json
"column_register_map": {
    "0":  [-1, "seg_len"],
    "1":  [6,  "mean"],
    "2":  [6,  "med"],
    "3":  [6,  "min"],
    "4":  [6,  "max"],
    "5":  [6,  "range"],
    "6":  [6,  "std"],
    "7":  [6,  "count"],
    "8":  [6,  "n_null"]
},
"out_coldef": {
    "0":  ["Seg_Len",   "int",   "numetory", "explanatory", "区間長"],
    "1":  ["Sin_mean",  "float", "numetory", "explanatory", "平均値＿全区間"],
    "2":  ["Sin_med",   "float", "numetory", "explanatory", "中央値＿全区間"],
    "3":  ["Sin_min",   "float", "numetory", "explanatory", "最小値＿全区間"],
    "4":  ["Sin_max",   "float", "numetory", "explanatory", "最大値＿全区間"],
    "5":  ["Sin_range", "float", "numetory", "explanatory", "範囲＿全区間"],
    "6":  ["Sin_std",   "float", "numetory", "explanatory", "標準偏差＿全区間"],
    "7":  ["Sin_count", "int",   "numetory", "explanatory", "有効件数＿全区間"],
    "8":  ["Sin_n_null","int",   "numetory", "explanatory", "Null件数＿全区間"]
}
```

### パターンB：分布確認

シナリオ：「パーセンタイル・IQR・シグマ境界で分布形状を把握し、外れ値を検出する」

```json
"column_register_map": {
    "0":  [6,  "pertl", 25],
    "1":  [6,  "med"],
    "2":  [6,  "pertl", 75],
    "3":  [6,  "iqr"],
    "4":  [6,  "sig_u",  3],
    "5":  [6,  "sig_l",  3]
},
"out_coldef": {
    "0":  ["Sin_P25",   "float", "numetory", "explanatory", "第１四分位"],
    "1":  ["Sin_med",   "float", "numetory", "explanatory", "中央値"],
    "2":  ["Sin_P75",   "float", "numetory", "explanatory", "第３四分位"],
    "3":  ["Sin_iqr",   "float", "numetory", "explanatory", "IQR"],
    "4":  ["Sin_sig_u", "float", "numetory", "explanatory", "上方３σ"],
    "5":  ["Sin_sig_l", "float", "numetory", "explanatory", "下方３σ"]
}
```

### パターンC：区間別比較

シナリオ：「波形の前半30%（立ち上がり）と後半50%（定常）を比較する」

```json
"column_register_map": {
    "0":  [6,  "mean", [0.0, 0.3]],
    "1":  [6,  "std",  [0.0, 0.3]],
    "2":  [6,  "mean", [0.5, 1.0]],
    "3":  [6,  "std",  [0.5, 1.0]],
    "4":  [6,  "nth",  [0.5, 1.0], 1]
},
"out_coldef": {
    "0":  ["Sin_mean_rise",    "float", "numetory", "explanatory", "平均値＿立上り30%"],
    "1":  ["Sin_std_rise",     "float", "numetory", "explanatory", "標準偏差＿立上り30%"],
    "2":  ["Sin_mean_steady",  "float", "numetory", "explanatory", "平均値＿定常50%"],
    "3":  ["Sin_std_steady",   "float", "numetory", "explanatory", "標準偏差＿定常50%"],
    "4":  ["Sin_steady_start", "float", "numetory", "explanatory", "定常区間先頭値"]
}
```

### パターンD：カテゴリ列

シナリオ：「LOT番号とクラスラベルを集計する」

```json
"column_register_map": {
    "0":  [5,  "nth",  1],
    "1":  [5,  "nth", -1],
    "2":  [5,  "mode"],
    "3":  [8,  "mode"],
    "4":  [8,  "mean"]
},
"out_coldef": {
    "0":  ["LOT_first", "str",   "key",      "key",         "LOT＿先頭行"],
    "1":  ["LOT_last",  "str",   "key",      "key",         "LOT＿最終行"],
    "2":  ["LOT_mode",  "str",   "key",      "key",         "LOT＿最頻値"],
    "3":  ["CL_mode",   "int",   "category", "objective",   "クラス＿最頻値"],
    "4":  ["CL_mean",   "float", "category", "objective",   "クラス＿平均"]
}
```

---

## 7. エラー時の動作

　設定ミスは**起動時に検出して停止**する。ループ中のデータ問題は**キーにWARNを付与**して記録する。

| 状況 | 動作 |
|------|------|
| `column_register_map` と `out_coldef` のキーが一致しない | 起動停止（ValueError） |
| `src_col_idx` が範囲外 | 起動停止（ValueError） |
| 不明な `stat_name` / 不正な `params` | 起動停止（ValueError） |
| 有効値ゼロ（全行Null等） | `{出力キー}_WARN: "no_valid_values"` |
| スライス結果が空 | `{出力キー}_WARN: "slice_empty"` |
| `nth` が範囲外 | `{出力キー}_WARN: "nth=N out of range"` |

---

まとめ

| 用途 | 推奨パターン |
|------|------------|
| まず動かす | パターンA（基本統計量） |
| 異常検知・管理図 | パターンB（分布確認）+ パターンA |
| 工程の挙動理解 | パターンC（区間別比較） |
| トレーサビリティ | パターンD（カテゴリ列） |
| MT法・機械学習 | パターンA〜C を組み合わせ、`numetory`/`explanatory` 列を特徴量に |


以上
