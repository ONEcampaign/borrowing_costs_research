# Define constants for default settings
from pathlib import Path
from typing import List

import pandas as pd
from pandas import DataFrame

from scripts.config import Paths
from scripts.logger import logger

from oda_reader import bulk_download_crs


def download_crs():
    """Download CRS data from the OECD website."""
    bulk_download_crs(save_to_path=Paths.raw_data)


DEFAULT_FILTERS = [("DonorCode", "in", [901, 903, 905])]
DEFAULT_COLUMNS = [
    "Year",
    "DonorCode",
    "DonorName",
    "RecipientCode",
    "RecipientName",
    "ProjectNumber",
    "USD_Commitment",
    "USD_Disbursement",
    "USD_Received",
    "ProjectTitle",
    "PurposeCode",
    "PurposeName",
    "CommitmentDate",
    "TypeRepayment",
    "NumberRepayment",
    "Interest1",
    "Interest2",
    "Repaydate1",
    "Repaydate2",
    "USD_Interest",
    "USD_Outstanding",
    "USD_Arrears_Principal",
    "USD_Arrears_Interest",
]


def read_parquet_file(
    file_path: Path, filters: List[tuple], columns: List[str]
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


def split_project_number(df: DataFrame, column: str = "ProjectNumber") -> DataFrame:
    """Split the ProjectNumber column into separate components.

    Args:
        df (DataFrame): DataFrame containing the ProjectNumber column.
        column (str): Name of the column to split.

    Returns:
        DataFrame: DataFrame with split project_id, loan_number, and crs_entry columns.
    """
    logger.info(f"Splitting '{column}' into components.")
    split_columns = ["project_id", "loan_number", "crs_entry"]
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
    return (
        df.sort_values(["Year", "DonorCode", "RecipientCode", "project_id"])
        .drop_duplicates(
            subset=["DonorCode", "RecipientName", "project_id", "loan_number"],
            keep="last",
        )
        .reset_index(drop=True)
    )


def load_and_process_crs_data(
    file_path: Path,
    filters: List[tuple],
    columns: List[str],
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
    df = read_parquet_file(file_path, filters, columns)
    df = split_project_number(df)
    df = deduplicate_crs_data(df)
    return df


if __name__ == "__main__":
    crs_data_path = Path(Paths.raw_data / "crs_data.parquet")
    if not crs_data_path.exists():
        download_crs()
    crs_data = load_and_process_crs_data(
        file_path=crs_data_path, filters=DEFAULT_FILTERS, columns=DEFAULT_COLUMNS
    )
