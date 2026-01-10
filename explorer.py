import logging
import boto3
import os
import argparse
import pandas as pd
import sys
import io
import warnings

# ---------------------------------------------------------
# Global warning suppression (pandas, boto3, numpy, etc.)
# ---------------------------------------------------------
warnings.filterwarnings("ignore")

# ---------------------------------------------------------
# Logging setup
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger(__name__)

# Silence boto3/botocore internal logs
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)


# ---------------------------------------------------------
# Custom argparse to avoid messy stderr output
# ---------------------------------------------------------
class CleanArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        logger.error("Argument error: %s", message)
        sys.exit(2)


# ---------------------------------------------------------
# Data loading
# ---------------------------------------------------------
def load_data(path: str) -> pd.DataFrame:
    logger.info("Loading data from %s", path)
    try:
        return pd.read_csv(path)
    except Exception as e:
        logger.error("Failed to load CSV file '%s': %s", path, e)
        sys.exit(1)


# ---------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------
def print_summary(df: pd.DataFrame) -> None:
    logger.info("Generating summary statistics")

    # Capture df.info()
    buffer = io.StringIO()
    df.info(buf=buffer)
    info_output = buffer.getvalue()
    logger.info("DataFrame info:\n%s", info_output)

    # Capture df.describe()
    describe_output = df.describe(include="all").to_string()
    logger.info("Summary statistics:\n%s", describe_output)


# ---------------------------------------------------------
# Column description
# ---------------------------------------------------------
def describe_column(df: pd.DataFrame, column: str) -> None:
    if column not in df.columns:
        logger.error("Column '%s' not found. Available columns: %s", column, list(df.columns))
        sys.exit(1)

    logger.info("Describing column: %s", column)
    describe_output = df[column].describe().to_string()
    logger.info("\n%s", describe_output)


# ---------------------------------------------------------
# Filtering
# ---------------------------------------------------------
def filter_rows(df: pd.DataFrame, expression: str) -> pd.DataFrame:
    logger.info("Applying filter: %s", expression)
    try:
        return df.query(expression)
    except Exception as e:
        logger.error("Invalid filter expression '%s': %s", expression, e)
        sys.exit(1)


# ---------------------------------------------------------
# S3 Upload
# ---------------------------------------------------------
def upload_to_s3(local_path: str, bucket: str, key: str) -> None:
    logger.info("Uploading %s to s3://%s/%s", local_path, bucket, key)
    s3 = boto3.client("s3")

    try:
        s3.upload_file(local_path, bucket, key)
        logger.info("Upload successful")
    except Exception as e:
        logger.error("S3 upload failed: %s", e)
        sys.exit(1)


# ---------------------------------------------------------
# Main CLI
# ---------------------------------------------------------
def main():
    parser = CleanArgumentParser(description="CSV Data Explorer")
    parser.add_argument("--file", required=True, help="Path to the CSV file")
    parser.add_argument("--summary", action="store_true", help="Print summary statistics")
    parser.add_argument("--column", help="Column name to describe")
    parser.add_argument("--filter", help="Filter expression, e.g. 'heart_rate>120'")
    parser.add_argument("--out", help="Path to save filtered CSV")
    parser.add_argument("--upload", help="Upload output file to S3 bucket (bucket/key)")

    args = parser.parse_args()

    # Load data
    df = load_data(args.file)

    # Summary
    if args.summary:
        print_summary(df)

    # Column description
    if args.column:
        describe_column(df, args.column)

    # Filtering
    if args.filter:
        df = filter_rows(df, args.filter)
        logger.info("Filtered rows (first 10):\n%s", df.head(10).to_string())

    # Saving
    if args.out:
        try:
            df.to_csv(args.out, index=False)
            logger.info("Data saved to %s", args.out)
        except Exception as e:
            logger.error("Failed to save output CSV '%s': %s", args.out, e)
            sys.exit(1)

    # Uploading
    if args.upload:
        if not args.out or not os.path.exists(args.out):
            logger.error("You must specify --out and save a file before uploading to S3")
            sys.exit(1)

        try:
            bucket, key = args.upload.split("/", 1)
            upload_to_s3(args.out, bucket, key)
        except ValueError:
            logger.error("--upload must be in the format 'bucket/key'")
            sys.exit(1)


if __name__ == "__main__":
    main()
