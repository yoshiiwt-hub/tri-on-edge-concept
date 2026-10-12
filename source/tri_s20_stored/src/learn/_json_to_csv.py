# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
import csv
import glob
import json

INPUT_FILES = "output/Mt_Diag_*.json"
OUTPUT_FILE = "Mt_Diag_merged.csv"
_encoding = "utf-8-sig"

json_files = glob.glob(INPUT_FILES)
if not json_files:
    raise RuntimeError("JSONファイルが見つかりません")

rows = []
fieldnames = set()

for json_file in json_files:
    with open(json_file, "r", encoding=_encoding) as f:
        data = json.load(f)

    # ルート階層
    row = {}
    for key, value in data.items():
        if key != "val":
            row[key] = value

    # val階層を展開
    if isinstance(data.get("val"), dict):
        row.update(data["val"])

    row["file_name"] = json_file

    rows.append(row)
    fieldnames.update(row.keys())

# ヘッダ順を安定化
fieldnames = sorted(fieldnames)

with open(OUTPUT_FILE, "w", newline="", encoding=_encoding) as f:
    writer = csv.DictWriter(
        f,
        fieldnames=fieldnames,
        extrasaction="ignore"
    )

    writer.writeheader()
    writer.writerows(rows)

print(f"CSV出力完了: {len(rows)}件")