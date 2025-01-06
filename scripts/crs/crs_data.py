# Define constants for default settings
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
from pandas import DataFrame

from scripts.config import Paths
from scripts.logger import logger

from oda_reader import bulk_download_crs


def download_crs():
    """Download CRS data from the OECD website."""
    bulk_download_crs(save_to_path=Paths.raw_data)


DEFAULT_FILTERS = [("donor_code", "in", [901, 903, 905])]
DEFAULT_COLUMNS = [
    "year",
    "donor_code",
    "donor_name",
    "recipient_code",
    "recipient_name",
    "project_number",
    "usd_commitment",
    "usd_disbursement",
    "usd_received",
    "project_title",
    "purpose_code",
    "purpose_name",
    "commitment_date",
    "type_repayment",
    "number_repayment",
    "interest1",
    "interest2",
    "repaydate1",
    "repaydate2",
    "usd_interest",
    "usd_outstanding",
    "usd_arrears_principal",
    "usd_arrears_interest",
]


def read_parquet_file(
    file_path: Path, filters: List[tuple] = None, columns: List[str] = None
) -> DataFrame:
    """Read a Parquet file with specified filters and columns.

    Args:
        file_path (Path): Path to the Parquet file.
        filters (List[tuple]): Filters to apply when loading the file.
        columns (List[str]): Columns to load from the file.

    Returns:
        DataFrame: Filtered and selected data as a DataFrame.
    """
    logger.info(f"Reading data from {file_path}")
    return pd.read_parquet(file_path, filters=filters, columns=columns)


def split_project_number(df: DataFrame, column: str = "project_number") -> DataFrame:
    """Split the project_number column into separate components.

    Args:
        df (DataFrame): DataFrame containing the ProjectNumber column.
        column (str): Name of the column to split.

    Returns:
        DataFrame: DataFrame with split project_id, loan_number, and crs_entry columns.
    """
    logger.info(f"Splitting '{column}' into components.")
    split_columns = ["project_id", "loan_or_credit_number", "crs_entry"]
    df[split_columns] = df[column].str.split(".", expand=True)
    return df.drop(columns=["crs_entry"])


def deduplicate_crs_data(df: DataFrame) -> DataFrame:
    """Deduplicate CRS data based on specific columns.

    Args:
        df (DataFrame): Input DataFrame to deduplicate.

    Returns:
        DataFrame: Deduplicated DataFrame.
    """
    logger.info("Deduplicating CRS data.")
    df = df.rename(columns={"commitment_date": "board_approval_date"})
    df = df.groupby(
        [
            "project_id",
            "loan_or_credit_number",
            "recipient_code",
            "board_approval_date",
        ],
        dropna=False,
        observed=True,
    ).agg(
        {
            "interest1": "max",
            "interest2": "max",
            "usd_interest": "sum",
            "usd_received": "sum",
            "usd_commitment": "sum",
            "usd_disbursement": "sum",
        }
    )

    df["interest_rate"] = np.fmax(df["interest1"], df["interest2"]) / 1000
    return df.reset_index().drop(columns=["interest1", "interest2"])


def drop_missing_interest(df: DataFrame) -> DataFrame:
    """Drop rows with missing interest rates.

    Args:
        df (DataFrame): Input DataFrame to deduplicate.

    Returns:
        DataFrame: DataFrame with missing interest rates removed.
    """
    logger.info("Dropping rows with missing interest rates.")
    return df.dropna(subset=["interest_rate"])


def load_and_process_crs_data(
    file_path: Path,
    filters: List[tuple] = None,
    columns: List[str] = None,
) -> DataFrame:
    """Load and preprocess CRS data.

    Combines reading, splitting, and deduplication steps into a single pipeline.

    Args:
        file_path (Path): Path to the CRS Parquet data file.
        filters (List[tuple]): Filters to apply when loading the file.
        columns (List[str]): Columns to load from the file.

    Returns:
        DataFrame: Preprocessed CRS data.
    """
    logger.info("Loading and processing CRS data.")
    df = read_parquet_file(file_path, filters, columns).astype(
        {"interest1": "float", "interest2": "float"}
    )
    df = split_project_number(df)
    df = deduplicate_crs_data(df)
    df = drop_missing_interest(df)
    return df


if __name__ == "__main__":
    crs_data_path = Path(Paths.raw_data / "CRS.parquet")
    if not crs_data_path.exists():
        download_crs()
    crs_data = load_and_process_crs_data(
        file_path=crs_data_path,
        filters=DEFAULT_FILTERS,
        columns=DEFAULT_COLUMNS,
    )
