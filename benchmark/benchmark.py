"""
Very basic benchmark: run AskQL against BIRD's gold question/SQL pairs for the
"retails" database (TPC-H schema) and check if its results match the gold SQL's.

Run download_dataset.py first to fetch the database and gold questions.
"""

import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
from huggingface_hub import hf_hub_download

from askQL import BasicSQLAgent

GOLD_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gold_questions.json")
RESULTS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results.csv")


def normalize(rows):
    def norm(v):
        return round(v, 2) if isinstance(v, float) else v

    return sorted(tuple(norm(v) for v in row) for row in rows)


def main():
    with open(GOLD_PATH) as f:
        gold = json.load(f)

    database_path = hf_hub_download(
        repo_id="ucalyptus/birdbench-duckdb", repo_type="dataset", filename="train/retails.duckdb"
    )
    agent = BasicSQLAgent(database_path=database_path)
    conn = duckdb.connect(database_path, read_only=True)

    passed = 0
    skipped = 0
    rows_out = []
    for i, row in enumerate(gold, 1):
        try:
            expected = normalize(conn.execute(row["sql"]).fetchall())
        except Exception as exc:
            print(f"[{i}/{len(gold)}] SKIP (gold SQL incompatible with DuckDB: {exc})")
            skipped += 1
            rows_out.append(
                {"question": row["question"], "gold_sql": row["sql"], "askql_sql": "", "pass": "skipped"}
            )
            continue

        result = agent.query(row["question"])
        actual = normalize([tuple(r.values()) for r in result["raw_results"]])
        match = result["success"] and actual == expected
        passed += match

        print(f"[{i}/{len(gold)}] {'PASS' if match else 'FAIL'}")

        rows_out.append(
            {
                "question": row["question"],
                "gold_sql": row["sql"],
                "askql_sql": result["sql"],
                "pass": match,
            }
        )

    with open(RESULTS_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["question", "gold_sql", "askql_sql", "pass"])
        writer.writeheader()
        writer.writerows(rows_out)

    scored = len(gold) - skipped
    print(f"Passed: {passed}/{scored} (skipped {skipped} with DuckDB-incompatible gold SQL)")
    print(f"Results written to {RESULTS_PATH}")


if __name__ == "__main__":
    main()