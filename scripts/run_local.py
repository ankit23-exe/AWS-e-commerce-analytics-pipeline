"""Run the full Glue transformation logic locally with Spark on the sample data.

    python scripts/run_local.py                      # synthetic sample
    python scripts/run_local.py --raw path/to/raw    # your own copy of the Olist data

Outputs go to ./output/{curated,quarantine,quality_reports}. Needs Java 11 or 17 and `pip install pyspark`.
"""
import argparse
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "glue"))

from pyspark.sql import SparkSession  # noqa: E402

import clean_transform as ct  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", default=str(ROOT / "data" / "sample" / "raw"))
    parser.add_argument("--max-bad-ratio", type=float, default=0.10,
                        help="the tiny sample is deliberately dirty, so the default is looser than in Glue (0.05)")
    args = parser.parse_args()

    out = ROOT / "output"
    shutil.rmtree(out, ignore_errors=True)

    spark = (
        SparkSession.builder.master("local[2]").appName("olist-local")
        .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    report = ct.run_pipeline(
        spark, args.raw, str(out / "curated"), str(out / "quarantine"),
        str(out / "quality_reports"), run_id="local", max_bad_ratio=args.max_bad_ratio,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    print(f"\nOutputs written to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
