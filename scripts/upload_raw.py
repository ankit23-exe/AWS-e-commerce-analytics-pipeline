"""Upload the Olist CSVs to the raw zone, then drop the _READY marker that starts the pipeline.

    python scripts/upload_raw.py --bucket <lake_bucket> --src ~/Downloads/olist

Expects the original Kaggle file names in --src. Only the 6 tables the pipeline uses are uploaded.
Credentials come from your normal AWS profile or environment, nothing is stored in this repo.
"""
import argparse
import sys
from pathlib import Path

import boto3

FILES = {
    "customers": "olist_customers_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--src", required=True, help="folder that contains the Olist CSV files")
    parser.add_argument("--no-trigger", action="store_true", help="upload only, do not start the pipeline")
    args = parser.parse_args()

    src = Path(args.src).expanduser()
    missing = [name for name in FILES.values() if not (src / name).exists()]
    if missing:
        sys.exit(f"Missing files in {src}: {', '.join(missing)}")

    s3 = boto3.client("s3")
    for table, filename in FILES.items():
        key = f"raw/{table}/{filename}"
        print(f"uploading {filename} -> s3://{args.bucket}/{key}")
        s3.upload_file(str(src / filename), args.bucket, key)

    if args.no_trigger:
        print("Upload done. Create raw/_READY later to start the pipeline.")
    else:
        s3.put_object(Bucket=args.bucket, Key="raw/_READY", Body=b"")
        print("Uploaded raw/_READY -> the Lambda starts the Glue job.")


if __name__ == "__main__":
    main()
