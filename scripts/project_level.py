import logging

import pandas as pd
from bblocks import convert_id

from scripts.config import Paths
from scripts.crs.crs_data import (
    load_and_process_crs_data,
    DEFAULT_FILTERS,
    DEFAULT_COLUMNS,
)
from scripts.logger import logger
from scripts.wb.statements_api import get_ida_interest, get_ibrd_interest

cc_loger = logging.getLogger("country_converter")
cc_loger.setLevel(logging.ERROR)


def get_crs_reported_projects() -> pd.DataFrame:
    # Load CRS data
    crs_data = load_and_process_crs_data(
        file_path=Paths.raw_data / "CRS.parquet",
        filters=DEFAULT_FILTERS,
        columns=DEFAULT_COLUMNS,
    )

    # Add ISO3 codes
    crs_data["iso3"] = convert_id(
        crs_data["recipient_code"], from_type="DACCode", to_type="ISO3"
    )

    # Convert dates to datetime
    crs_data["board_approval_date"] = pd.to_datetime(crs_data["board_approval_date"])

    return (
        crs_data.dropna(subset=["interest_rate"])
        .reset_index(drop=True)
        .drop(
            columns=[
                "recipient_code",
                "usd_interest",
                "usd_received",
                "usd_commitment",
                "usd_disbursement",
            ]
        )
    )


def get_project_level_data() -> pd.DataFrame:
    # Load ida data
    ida_summary = get_ida_interest().assign(source="IDA")

    # load ibrd data
    ibrd_summary = get_ibrd_interest().assign(source="IBRD")

    # combine
    data = pd.concat([ida_summary, ibrd_summary], ignore_index=True)

    # add iso3 codes
    data["iso3"] = convert_id(data["country"], from_type="regex", to_type="ISO3")

    return data


def filter_keep_only_missing_interest(data: pd.DataFrame) -> pd.DataFrame:
    """Keep projects with missing interest rates."""
    # Filter to keep only projects with missing interest rates
    data = data.query("interest_rate.isna()").drop(columns=["interest_rate"])

    return data


def add_available_interest_rate_data_from_crs(
    wb_data: pd.DataFrame, crs_data
) -> pd.DataFrame:
    data = wb_data.merge(
        crs_data,
        left_on=["project_id", "loan_or_credit_number", "iso3", "board_approval_date"],
        right_on=["project_id", "loan_or_credit_number", "iso3", "board_approval_date"],
        how="left",
    )

    return data


def get_project_level_interest():
    """"""

    # Get project level data from both sources. WB data is prioritised
    project_level_data = get_project_level_data()
    crs_project_data = get_crs_reported_projects()

    # Store full total number of projects
    total_projects = len(project_level_data)

    # Filter to keep only projects with missing interest rates
    missing_interest = filter_keep_only_missing_interest(data=project_level_data)

    # Log the number of projects with missing interest rates
    logger.info(
        f"Total number of projects with missing interest rates: {len(missing_interest)}"
    )

    # To avoid duplicates, drop projects with missing interest rates from the WB data
    project_level_data = project_level_data.loc[lambda d: d.interest_rate.notna()]

    # Merge the two datasets. WB data is prioritised. The resulting data may still contain
    # missing interest rates for projects (i.e. where the CRS data is also missing)
    missing_interest = add_available_interest_rate_data_from_crs(
        wb_data=missing_interest, crs_data=crs_project_data
    )

    # Drop missing interest data from the merged data
    missing_interest = missing_interest.loc[lambda d: d.interest_rate.notna()]

    # Combine the two datasets
    combined_data = pd.concat([project_level_data, missing_interest], ignore_index=True)

    # Number of projects with missing interest rates
    projects_with_data = len(combined_data)
    missing = total_projects - projects_with_data
    logger.info(f"Total number of projects with interest data: {projects_with_data}")

    logger.info(
        f"Still missing {missing} ({round(100 * missing / total_projects, 1)}%) out of "
        f" {total_projects} projects after merging CRS data."
    )

    return combined_data


if __name__ == "__main__":
    df = get_project_level_interest()
    df.to_csv(Paths.output / "projects" / "wb_projects_interest.csv", index=False)
    logger.info("Project level interest data saved.")
