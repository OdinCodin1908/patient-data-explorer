import argparse
import pandas as pd


def load_data(path: str) -> pd.DataFrame:
    return pd.read_csv(path)


def print_summary(df: pd.DataFrame) -> None:
    print("Basic info:")
    print(df.info())
    print("\nSummary stats:")
    print(df.describe(include="all"))


def describe_column(df: pd.DataFrame, column: str) -> None:
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found. Available: {list(df.columns)}")
    print(f"Summary for column: {column}")
    print(df[column].describe())


def filter_rows(df: pd.DataFrame, expression: str) -> pd.DataFrame:
    return df.query(expression)


def main():
    parser = argparse.ArgumentParser(description="CSV Data Explorer")
    parser.add_argument("--file", required=True, help="Path to the CSV file")
    parser.add_argument("--summary", action="store_true", help="Print summary statistics")
    parser.add_argument("--column", help="Column name to describe")
    parser.add_argument("--filter", help="Filter expression, e.g. 'heart_rate>120'")
    parser.add_argument("--out", help="Path to save filtered CSV")
    args = parser.parse_args()

    df = load_data(args.file)

    if args.summary:
        print_summary(df)

    if args.column:
        describe_column(df, args.column)

    if args.filter:
        filtered = filter_rows(df, args.filter)
        print("\nFiltered rows (first 10):")
        print(filtered.head(10))

        if args.out:
            filtered.to_csv(args.out, index=False)
            print(f"Filtered data saved to {args.out}")


if __name__ == "__main__":
    main()
