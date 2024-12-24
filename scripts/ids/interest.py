from typing import Optional

import pandas as pd

from scripts.ids.clean_data import get_clean_data

INTEREST_RATE_INDICATORS: dict = {
    "DT.INR.OFFT": "Official",
    "DT.INR.PRVT": "Private",
}


INTEREST_PAYMENTS_INDICATORS: dict = {
    "DT.INT.BLAT.CD": "Bilateral",
    "DT.INT.MLAT.CD": "Multilateral",
    "DT.INT.PBND.CD": "Private",
    "DT.INT.PCBK.CD": "Private",
    "DT.INT.PROP.CD": "Private",
}


def get_average_interest(
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    countries: Optional[list[str]] = None,
    counterparts: Optional[list[str]] = None,
) -> pd.DataFrame:
    """Get data with the weighted average interest rate for each country/counterpart_area pair."""
    return get_clean_data(
        start_year=start_year,
        end_year=end_year,
        indicators=INTEREST_RATE_INDICATORS,
        countries=countries,
        counterparts=counterparts,
    )


def get_interest_payments(
    start_year: int,
    end_year: int,
    counterparts: list[str] = None,
) -> pd.DataFrame:
    """Get data with the interest payments for each country/counterpart_area pair."""
    return get_clean_data(
        start_year=start_year,
        end_year=end_year,
        indicators=INTEREST_PAYMENTS_INDICATORS,
        counterparts=counterparts,
    )
