"""
Download the BIRD "retails" database (TPC-H schema) and its gold question/SQL
pairs, and save the pairs locally so benchmark.py doesn't need to re-fetch them.
"""

import json
import os

from datasets import load_dataset
from huggingface_hub import hf_hub_download

GOLD_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gold_questions.json")


def main():
    database_path = hf_hub_download(
        repo_id="ucalyptus/birdbench-duckdb", repo_type="dataset", filename="train/retails.duckdb"
    )
    print(f"Database cached at: {database_path}")

    gold = load_dataset("xu3kev/BIRD-SQL-data-train", split="train")
    gold = gold.filter(lambda r: r["db_id"] == "retails")

    pairs = [{"question": r["question"], "sql": r["SQL"]} for r in gold]
    with open(GOLD_PATH, "w") as f:
        json.dump(pairs, f, indent=2)

    print(f"Saved {len(pairs)} gold question/SQL pairs to {GOLD_PATH}")


if __name__ == "__main__":
    main()
