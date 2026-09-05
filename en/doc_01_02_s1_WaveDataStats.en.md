# Tri-On-Edge Waveform Statistics Calculation and Configuration Guide

- Ver: 01.a.20260814
- By: IWATA,Y.
- Mod: Initial creation

---

Target Audience
- Users (site operators / maintenance staff)
- Developers

---

Parent Document
- Tri-On-Edge Waveform Cut-Out Logic and Configuration Guide

---

## 1. Overview: Converting to "Features" at the Edge

　If the raw data captured by waveform cut-out (Segment) is sent to the cloud or a server as-is, it places a heavy burden on downstream processing cost, communication bandwidth, and storage resources.

　Tri-On-Edge's waveform statistics feature solves this problem by **aggregating and converting the waveform at the edge (on-site) before transmission**.

```
[Conventional]
　Raw data (n rows) 　　　　　　　　　　　　 → Transfer (n rows) → Downstream processing

[Tri-On-Edge Statistics Calculation]
　Raw data (n rows) → Statistics calculation (n rows → 1 row) → Transfer 1 row (1 row) → Downstream processing
```

### Effectiveness as Edge Computing

| Aspect | Effect |
|------|------|
| **Reduced communication cost** | Compresses data volume (row count), reducing communication load. Suitable for low-bandwidth environments such as factory networks and mobile lines |
| **Real-time capability** | Judgment and alerting are completed at the edge, eliminating round-trip latency to the cloud |
| **Improved data quality** | Excludes (cleanses) null counts and outlier boundaries (σ). Extracts features that feed directly into AI/ML training |

---

## 2. Overall Processing Flow

　As part of post-processing (Act Trigger), statistics are calculated each time one segment (n rows) is received. From that point on, processing proceeds as a one-row dataset.

```
┌─ Stream Core ────────────────────────────┐
│  Stream data                              │
│      ↓ Waveform cut-out                   │
│  segment (N rows) → Act Queue             │
└──────────────────────────────────────────┘
         ↓
┌─ a_socket_client_trigger ────────────────┐
│  segment (n rows)                         │
│      ↓                                   │
│  Statistics (1 record)                    │
│      ↓                                   │
│  Downstream processing                    │
└──────────────────────────────────────────┘
```

### Fixed Header of the Output JSON

　In addition to the statistics, the following header is always attached.

| index | key | content |
|----|------|------|
| 0 | `seg_id` | Segment ID (a unique value based on the occurrence time) |
| 1 | `seg_ts_start` | Start timestamp of the waveform |
| 2 | `seg_ts_end` | End timestamp of the waveform |
| 3 | `seg_rows` | Number of rows in the waveform |

---

## 3. List of Statistics

### 3-1. Basic Statistics (target: numeric columns)

| stat name | content | notes |
|---------|------|------|
| `mean` | Mean value | Only valid values that could be converted to float are included |
| `med` | Median | For an even number of rows, the average of the two middle values |
| `min` | Minimum value | |
| `max` | Maximum value | |
| `range` | Range (max − min) | For grasping the width of variation |
| `std` | Standard deviation | Population standard deviation (divided by n) |
| `count` | Count of valid values | Number of rows successfully converted to float |
| `n_null` | Count of nulls | Number of rows that could not be converted to float. `count + n_null = seg_rows` |
| `sum` | Sum | |

### 3-2. Distribution / Outlier Detection (target: numeric columns)

| stat name | content | params | notes |
|---------|------|--------|------|
| `iqr` | Interquartile range (P75 − P25) | — | A measure of spread that is less affected by outliers |
| `perctl` | Percentile | `p` : int | Valid values: 1, 5, 10, 25, 75, 90, 95, 99 |
| `sig_u` | Upper sigma boundary (mean + n×std) | `n` : float | Used as an upper control limit |
| `sig_l` | Lower sigma boundary (mean − n×std) | `n` : float | Used as a lower control limit |

### 3-3. Arbitrary Row Retrieval (target: numeric and string columns)

| stat name | content | params | notes |
|---------|------|--------|------|
| `nth` | Value of the nth row (raw value) | `n` : int | Positive: 1-indexed / Negative: counted from the end |
| `mode` | Mode | — | Excludes None. Ties go to the first occurrence |

### 3-4. Whole-Segment (target: segment rows)

| stat name | content | src_col_idx | notes |
|---------|------|-------------|------|
| `seg_len` | Number of records | `-1` (fixed) | — |

---

## 4. Range-Specified Slicing

　Statistics can be calculated not only over the **entire waveform**, but also restricted to a **partial range of the waveform**.

```
[0.0, 0.3]  → the first 30% of the waveform (e.g., rise interval)
[0.5, 1.0]  → the last 50% of the waveform (e.g., steady-state interval)
[0.2, 0.8]  → the middle 60% of the waveform (excluding transients)
```

　A slice can be applied to any statistic (except `seg_len`). Combined with `nth`, this also allows retrieving values such as "the Nth row after entering the steady-state interval."

```json
[6, "nth", [0.5, 1.0], 1]    ← the first row of the last-50% range
[6, "mean", [0.0, 0.3]]      ← mean of the first 30%
[6, "sig_u", [0.3, 0.7], 3]  ← +3σ boundary over the middle 40%
```

　If no slice is specified, or the value is invalid (e.g., start ≥ end), all rows are targeted.

---

## 5. Configuration File Structure

Written in the `act_trigger` section of `trigger_setting_02.json`.

### 5-1. Basic Structure

```json
"act_trigger": {
    "module"    : "src.tri_edge_12_act",
    "func"      : "a_socket_client_trigger",
    "send_stats": true,

    "column_register_map": { ... },
    "out_coldef"         : { ... },
    "save_dir"           : "output"
}
```

| Key | Content | Default |
|------|------|-----------|
| `send_stats` | `true`: send statistics / `false`: send all rows | `true` |
| `column_register_map` | Definition of how statistics are computed (described below) | required |
| `out_coldef` | Definition of output columns (described below) | required |
| `save_dir` | Save directory for `out_coldef` | `"output"` |

### 5-2. Entry Format of column_register_map

```
"index": [src_col_idx, "stat_name"]
"index": [src_col_idx, "stat_name", [start, end]]          ← slice only
"index": [src_col_idx, "stat_name", param]                 ← param only
"index": [src_col_idx, "stat_name", [start, end], param]   ← both
```

- `src_col_idx`: the column index of the input data (corresponds to the index in `col_def`)
- `stat_name`: the name of the statistic (see Section 3)
- When using `seg_len`, set `src_col_idx = -1` (a sentinel value, since no column is needed)

### 5-3. Entry Format of out_coldef

```json
"index": ["output key name", "dtype", "role", "var_type", "description"]
```

- Must correspond to the same `index` as `column_register_map`
- `dtype`: `"float"` / `"int"` / `"str"` / `"time"`
- `role`: `"key"` / `"numetory"` / `"category"`
- `var_type`: `"key"` / `"explanatory"` / `"objective"`

　`out_coldef` is saved as a JSON file under `save_dir` at startup. The server side can read this file to construct the schema.

---

## 6. Configuration Examples

### Pattern A: Basic Statistics

Scenario: "Calculate the main statistics over the entire range."

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

### Pattern B: Distribution Check

Scenario: "Understand the shape of the distribution and detect outliers using percentiles, IQR, and sigma boundaries."

```json
"column_register_map": {
    "0":  [6,  "perctl", 25],
    "1":  [6,  "med"],
    "2":  [6,  "perctl", 75],
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

### Pattern C: Comparison Between Ranges

Scenario: "Compare the first 30% (rise) of the waveform with the last 50% (steady-state)."

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

### Pattern D: Category Columns

Scenario: "Aggregate the LOT number and class label."

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

## 7. Behavior on Error

　Configuration mistakes are **detected and halted at startup**. Data problems that occur during the loop are **recorded by attaching WARN to the key**.

| Situation | Behavior |
|------|------|
| Keys of `column_register_map` and `out_coldef` do not match | Startup halted (ValueError) |
| `src_col_idx` is out of range | Startup halted (ValueError) |
| Unknown `stat_name` / invalid `params` | Startup halted (ValueError) |
| Zero valid values (e.g., all rows are null) | `{output key}_WARN: "no_valid_values"` |
| Slice result is empty | `{output key}_WARN: "slice_empty"` |
| `nth` is out of range | `{output key}_WARN: "nth=N out of range"` |

---

Summary

| Use Case | Recommended Pattern |
|------|------------|
| Get it running first | Pattern A (basic statistics) |
| Anomaly detection / control charts | Pattern B (distribution check) + Pattern A |
| Understanding process behavior | Pattern C (comparison between ranges) |
| Traceability | Pattern D (category columns) |
| MT method / machine learning | Combine Patterns A–C, using the `numetory`/`explanatory` columns as features |

End of document
