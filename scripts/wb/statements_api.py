"""Module for fetching and processing World Bank interest data from the DataCatalog API."""

from pathlib import Path
from typing import List, Optional, Callable

import pandas as pd
import requests

from scripts import config

BASE_URL: str = "https://datacatalogapi.worldbank.org/dexapps/fone/api/apiservice"


def fetch_paginated_data(
    dataset_id: str,
    resource_id: str,
    select_fields: Optional[List[str]] = None,
    filter_expression: Optional[str] = None,
    max_records: int = 1000,
    data_type: str = "json",
) -> pd.DataFrame:
    """Fetches data from the World Bank API with pagination.

    Args:
        dataset_id: Identifier for the dataset.
        resource_id: Identifier for the resource within the dataset.
        select_fields: Fields to select from the dataset.
        filter_expression: Filter expression for the query, if any.
        max_records: Maximum records per request. Defaults to 1000.
        data_type: The response data type, typically 'json'. Defaults to 'json'.

    Returns:
        A single Pandas DataFrame containing all pages of data.
    """
    # Build the core request parameters; only 'skip' changes per page
    request_params = {
        "datasetId": dataset_id,
        "resourceId": resource_id,
        "type": data_type,
        "top": max_records,
    }

    if select_fields:
        request_params["select"] = ",".join(select_fields)
    if filter_expression:
        request_params["filter"] = filter_expression

    all_data = []
    skip = 0

    while True:
        # Update skip for pagination
        request_params["skip"] = skip

        response = requests.get(BASE_URL, params=request_params)
        response.raise_for_status()

        payload = response.json()
        data_batch = payload.get("data", [])
        all_data.extend(data_batch)

        # If the returned data is smaller than max_records, we've reached the last page
        if len(data_batch) < max_records:
            break

        skip += max_records

    return pd.DataFrame(all_data)


def clean_ida_ibrd_response(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and standardizes fields commonly used in both IDA and IBRD data.

    - Converts known date columns (e.g. 'end_of_period', 'board_approval_date') to datetime.
    - Renames columns for consistency (e.g. 'end_of_period' -> 'period').
    - Merges 'repaid_to_ida_us_' and 'repaid_to_ibrd' into a single 'repayment' column if present.
    - Renames 'disbursed_amount_us_' to 'disbursed_amount'.

    Args:
        df: Raw DataFrame from the World Bank API.

    Returns:
        A cleaned, standardized DataFrame.
    """
    # Convert date columns if they exist
    if "end_of_period" in df.columns:
        df["end_of_period"] = pd.to_datetime(
            df["end_of_period"], format="%d-%b-%Y", errors="coerce"
        )
    if "board_approval_date" in df.columns:
        df["board_approval_date"] = pd.to_datetime(
            df["board_approval_date"], format="%d-%b-%Y", errors="coerce"
        )

    # Rename columns if they exist
    rename_map = {
        "end_of_period": "period",
        "repaid_to_ida_us_": "repayment",
        "repaid_to_ibrd": "repayment",
    }

    return df.rename(columns=rename_map)


def download_and_save_data(
    dataset_id: str,
    resource_id: str,
    select_fields: Optional[List[str]],
    filter_expression: Optional[str],
    max_records: int,
    output_filepath: Path,
) -> None:
    """Downloads data from the API in paginated form, cleans it, and saves to Parquet.

    Args:
        dataset_id: The dataset identifier for the API.
        resource_id: The resource identifier for the API.
        select_fields: A list of fields to select (passed to 'select' parameter).
        filter_expression: A string representing the filter condition (passed to 'filter' parameter).
        max_records: Number of records to fetch per page.
        output_filepath: Local file path where the resulting Parquet file will be saved.
        cleaning_func: Optional function to clean or transform the data before saving.
    """
    df = fetch_paginated_data(
        dataset_id=dataset_id,
        resource_id=resource_id,
        select_fields=select_fields,
        filter_expression=filter_expression,
        max_records=max_records,
    )

    df = clean_ida_ibrd_response(df)

    df.reset_index(drop=True).to_parquet(output_filepath)


def download_ida_interest() -> None:
    """Fetches IDA interest data, cleans it, and writes to Parquet."""
    dataset_id = "DS00976"
    resource_id = "RS00906"
    output_path = config.Paths.raw_data / "ida_interest.parquet"

    download_and_save_data(
        dataset_id=dataset_id,
        resource_id=resource_id,
        select_fields=None,
        filter_expression=None,  # e.g. "end_of_period>='01-Sep-2024'",
        max_records=100_000,
        output_filepath=output_path,
    )


def download_ibrd_interest() -> None:
    """Fetches IBRD interest data, cleans it, and writes to Parquet."""
    dataset_id = "DS00975"
    resource_id = "RS00905"
    output_path = config.Paths.raw_data / "ibrd_interest.parquet"

    download_and_save_data(
        dataset_id=dataset_id,
        resource_id=resource_id,
        select_fields=None,
        filter_expression=None,  # e.g. "end_of_period>='01-Sep-2024'",
        max_records=100_000,
        output_filepath=output_path,
    )


def zero_to_nan(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df[column] = df[column].replace(0, pd.NA)

    return df


def get_ida_interest() -> pd.DataFrame:
    """Loads and aggregates IDA interest data from local Parquet.

    Returns:
        A DataFrame summarizing IDA interest.
    """
    ida_path = config.Paths.raw_data / "ida_interest.parquet"
    ida = pd.read_parquet(ida_path)

    idx_cols = [
        "credit_number",
        "board_approval_date",
        "country_code",
        "project_id",
        "project_name",
    ]

    ida_summary = (
        ida.groupby(idx_cols, dropna=False, observed=True)[
            ["country", "service_charge_rate", "original_principal_amount_us_"]
        ]
        .max()
        .reset_index()
        .drop_duplicates(
            subset=["credit_number", "country_code", "project_id"], keep="last"
        )
        .pipe(zero_to_nan, column="service_charge_rate")
    )

    return ida_summary


def get_ibrd_interest() -> pd.DataFrame:
    """Loads and aggregates IBRD interest data from local Parquet.

    Returns:
        A DataFrame summarizing IBRD interest.
    """
    ibrd_path = config.Paths.raw_data / "ibrd_interest.parquet"
    ibrd = pd.read_parquet(ibrd_path)

    idx_cols = [
        "loan_number",
        "board_approval_date",
        "country_code",
        "project_id",
        "project_name_",
        "loan_type",
    ]

    ibrd_summary = (
        ibrd.groupby(idx_cols, dropna=False, observed=True)[
            ["country", "interest_rate", "original_principal_amount"]
        ]
        .max()
        .reset_index()
        .drop_duplicates(
            subset=["loan_number", "country_code", "project_id"], keep="last"
        )
        .pipe(zero_to_nan, column="interest_rate")
    )

    return ibrd_summary


if __name__ == "__main__":
    # download_ida_interest()
    # download_ibrd_interest()

    # Just interest rate data
    ida_summary = get_ida_interest()
    ibrd_summary = get_ibrd_interest()
