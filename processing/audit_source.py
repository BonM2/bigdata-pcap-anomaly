"""Read-only, streaming source audit; no Spark, upload or CSV edits."""

import argparse
import csv
import hashlib
import json
import math
import re
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from processing.config import DEFAULT_SCHEMA_PATH, PROJECT_ROOT, RATES, TCP_FLAGS, WINDOW_FLAGS, load_contract


def audit_file(path, contract):
    path = Path(path)
    digest = hashlib.sha256()
    fields = contract["fields"]
    stats = [Counter() for _ in fields]
    examples = [{} for _ in fields]
    ranges = [[None, None] for _ in fields]
    labels = Counter()
    widths = Counter()
    records = malformed = decoding = replacements = duplicates_differ = hard_rows = soft_rows = 0
    integer = re.compile(r"^[+-]?[0-9]+$")

    def issue(index, code, token):
        stats[index][code] += 1
        examples[index].setdefault(code, token)

    with path.open("rb") as stream:
        header_bytes = stream.readline()
        digest.update(header_bytes)
        headers = next(csv.reader([header_bytes.decode("utf-8-sig")], strict=True))
        expected = [f["source_header_exact"] for f in fields]
        header_matches = headers == expected
        for raw_line in stream:
            digest.update(raw_line)
            records += 1
            try:
                line = raw_line.decode("utf-8")
            except UnicodeDecodeError:
                decoding += 1
                line = raw_line.decode("utf-8", errors="replace")
            try:
                row = next(csv.reader([line], strict=True))
            except csv.Error:
                malformed += 1
                widths["CSV_SYNTAX"] += 1
                continue
            widths[str(len(row))] += 1
            if len(row) != 79:
                malformed += 1
                continue
            if row[34] != row[55]:
                duplicates_differ += 1
            labels[row[78]] += 1
            hard = not row[78].strip()
            soft = "\ufffd" in row[78]
            replacements += int(soft)
            if hard:
                issue(78, "empty_label", row[78])
            if soft:
                issue(78, "encoding_replacement", row[78])
            for index in range(78):
                field = fields[index]
                name, kind = field["name"], field["cleaned_target_type"]
                core, rate = field["in_cleaned_core"], name in RATES
                token = row[index].strip()
                bad, sentinel = False, False
                if not token:
                    issue(index, "empty", row[index])
                    bad = True
                else:
                    try:
                        if kind in {"integer", "long"}:
                            if not integer.fullmatch(token):
                                raise ValueError("invalid integer grammar")
                            value = int(token)
                            bits = 32 if kind == "integer" else 64
                            if not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
                                raise ValueError("overflow")
                        else:
                            value = float(token)
                    except (ValueError, OverflowError):
                        issue(index, "cast_failure" if core else "non_numeric", token)
                        bad = True
                    else:
                        if isinstance(value, float) and math.isnan(value):
                            issue(index, "nan", token)
                            bad = True
                        elif isinstance(value, float) and math.isinf(value):
                            issue(index, "positive_infinity" if value > 0 else "negative_infinity", token)
                            bad = True
                        else:
                            stats[index]["finite"] += 1
                            if isinstance(value, float) and not value.is_integer():
                                stats[index]["fractional"] += 1
                            if ranges[index][0] is None or value < ranges[index][0]:
                                ranges[index][0] = value
                            if ranges[index][1] is None or value > ranges[index][1]:
                                ranges[index][1] = value
                            if value < 0:
                                if name in WINDOW_FLAGS and value == -1:
                                    issue(index, "sentinel_minus_one", token)
                                    sentinel = True
                                else:
                                    issue(index, "negative", token)
                                    bad = True
                            if name == "destination_port" and not 0 <= value <= 65535:
                                issue(index, "out_of_range", token)
                                bad = True
                            if name in TCP_FLAGS and value not in (0, 1):
                                issue(index, "invalid_flag", token)
                                bad = True
                if core:
                    hard = hard or (bad and not rate)
                    soft = soft or (bad and rate) or sentinel
            hard_rows += int(hard)
            soft_rows += int(soft and not hard)

    return {
        "source_file": path.name, "input_bytes": path.stat().st_size,
        "source_sha256": digest.hexdigest(), "input_records": records,
        "header_columns": len(headers), "header_matches_exact": header_matches,
        "header_differences": [{"index": i, "actual": a, "expected": b}
                               for i, (a, b) in enumerate(zip(headers, expected)) if a != b],
        "duplicate_headers": {name: [i + 1 for i, h in enumerate(headers) if h.strip() == name]
                              for name, count in Counter(h.strip() for h in headers).items() if count > 1},
        "duplicate_header_value_mismatches": duplicates_differ,
        "row_width_distribution": dict(widths), "malformed_records": malformed,
        "utf8_decode_error_records": decoding, "label_replacement_records": replacements,
        "label_distribution": dict(labels),
        "policy_preview": {"valid_records": records - malformed - hard_rows,
                           "quarantined_records": hard_rows, "rejected_records": malformed,
                           "valid_records_with_soft_issues": soft_rows},
        "fields": {f["name"]: {"target_type": f["cleaned_target_type"],
                              "counts": dict(stats[i]), "examples": examples[i],
                              "min": ranges[i][0], "max": ranges[i][1]}
                   for i, f in enumerate(fields)},
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=PROJECT_ROOT / "data/raw/cicids2017")
    parser.add_argument("--report", type=Path, default=PROJECT_ROOT / "docs/source-audit.local.json")
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be positive")
    contract = load_contract()
    files = sorted(args.source_dir.glob("*.csv"))
    expected = set(contract["source"]["files"])
    actual = {p.name for p in files}
    if expected != actual:
        raise SystemExit(f"File set mismatch: missing={sorted(expected - actual)}, unexpected={sorted(actual - expected)}")
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(audit_file, str(path), contract) for path in files]
        results = []
        for future in futures:
            result = future.result()
            results.append(result)
            print(f"Audited {result['source_file']}: {result['input_records']} records", flush=True)
    report = {
        "audit_timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "scope": "Read-only local CSV audit; policy preview is not a Spark processing result",
        "schema_sha256": hashlib.sha256(DEFAULT_SCHEMA_PATH.read_bytes()).hexdigest(),
        "contract_sha256": hashlib.sha256((PROJECT_ROOT / "data/schemas/feature_dictionary.md").read_bytes()).hexdigest(),
        "source_directory": str(args.source_dir.resolve()),
        "input_records": sum(r["input_records"] for r in results),
        "input_bytes": sum(r["input_bytes"] for r in results),
        "structurally_ready_for_upload": all(r["header_matches_exact"] and r["malformed_records"] == 0 for r in results),
        "files": results,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Audit report: {args.report}; total records={report['input_records']}")


if __name__ == "__main__":
    main()
