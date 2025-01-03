import numpy as np
import pandas as pd

from scripts.config import Paths
from scripts.ids.interest import get_average_interest
from bblocks_data_importers import InternationalDebtStatistics

from scripts.ids.tools import imf_emde
from scripts.logger import logger


def _download_and_clean_interest_data(country: str) -> pd.DataFrame:
    """Download and clean interest data for all countries."""
    try:
        logger.info(f"Downloading data for {country}")
        df = get_average_interest(countries=[country])

    except:
        logger.warning(f"Failed to download data for {country}")
        # return empty dataframe with the same columns as the output
        return pd.DataFrame(
            columns=[
                "entity_name",
                "counterpart_name",
                "year",
                "indicator_code",
                "value",
            ]
        )

    # Add "(private)" to counterpart_name when indicator_code is "DT.INR.PRVT" else add "(official)"
    df["counterpart_name"] = np.where(
        df["indicator_code"] == "DT.INR.PRVT",
        df["counterpart_name"] + " (private)",
        df["counterpart_name"] + " (official)",
    )

    df = (
        df.loc[lambda d: d.value != 0]
        .drop(columns="indicator_code")
        .sort_values(
            ["counterpart_name", "year"],
            ascending=(True, False),
        )
    )

    return df


def download_interest_data_per_african_country():
    """Download interest data for each African country, and save it to a CSV file."""

    # Get the list of African countries
    african_countries = InternationalDebtStatistics().get_african_countries()

    # Download interest data for each African country
    for country in african_countries:
        df = _download_and_clean_interest_data(country=country)
        start_year, end_year = df.year.min(), df.year.max()
        df.to_csv(
            Paths.output
            / "interest_rates"
            / "africa"
            / f"{country}_{start_year}_{end_year}.csv",
            index=False,
        )


def download_interest_data_per_emde_country():
    """Download interest data for each EMDE country (excluding African countries),
    and save it to a CSV file."""

    # Get the list of African countries
    african_countries = InternationalDebtStatistics().get_african_countries()

    # Get the list of EMDE countries
    emde = imf_emde()

    # EMDE not African
    emde_not_african = list(set(emde) - set(african_countries))

    # Download interest data for each African country
    for country in emde_not_african:
        df = _download_and_clean_interest_data(country=country)
        start_year, end_year = df.year.min(), df.year.max()
        df.to_csv(
            Paths.output
            / "interest_rates"
            / "emde_non_african"
            / f"{country}_{start_year}_{end_year}.csv",
            index=False,
        )


if __name__ == "__main__":
    download_interest_data_per_african_country()
    download_interest_data_per_emde_country()
