import pandas as pd

from scripts.ids.grace import get_grace
from scripts.ids.interest import get_average_interest, get_interest_payments
from scripts.ids.commitments import get_commitments
from scripts.ids.maturity import get_maturities


def get_merged_rates_commitments_payments_data(
    start_year: int, end_year: int, counterparts: list[str] = None
) -> pd.DataFrame:
    """Get the data with the interest rate, the commitments, the grace period and the maturities.

    the resulting data identifies:
    - interest rates as "value_rate"
    - commitments as "value_commitments"
    - grace period as "value_grace"
    - maturities as "value_maturities"

    The data is merged on the following columns:
    - country
    - counterpart_area
    - continent
    - income_level
    - year

    """

    rate = get_average_interest(
        start_year=start_year, end_year=end_year, counterparts=counterparts
    )
    commitments = get_commitments(
        start_year=start_year, end_year=end_year, counterparts=counterparts
    )
    payments = get_interest_payments(
        start_year=start_year, end_year=end_year, counterparts=counterparts
    )

    idx = ["year", "entity_name", "counterpart_name", "continent", "income_level"]

    # merge the data and keep only rows with positive commitments
    df = (
        pd.merge(commitments, rate, on=idx, how="left", suffixes=("_commitments", ""))
        .merge(payments, on=idx, how="left", suffixes=("", "_payments"))
        .rename(columns={"value": "value_rate"})
        .loc[lambda d: d.value_commitments > 0]
    )

    return df


def get_merged_rates_commitments_grace_maturities_data(
    start_year: int,
    end_year: int,
    counterparts: list[str] = None,
) -> pd.DataFrame:
    """Get the data with the interest rate, the commitments, the grace period and the maturities.

    the resulting data identifies:
    - interest rates as "value_rate"
    - commitments as "value_commitments"
    - grace period as "value_grace"
    - maturities as "value_maturities"

    The data is merged on the following columns:
    - country
    - counterpart_area
    - continent
    - income_level
    - year

    """

    rate = get_average_interest(
        start_year=start_year, end_year=end_year, counterparts=counterparts
    )
    commitments = get_commitments(
        start_year=start_year, end_year=end_year, counterparts=counterparts
    )
    grace = get_grace(
        start_year=start_year, end_year=end_year, counterparts=counterparts
    )
    maturities = get_maturities(
        start_year=start_year, end_year=end_year, counterparts=counterparts
    )

    idx = ["year", "entity_name", "counterpart_name", "continent", "income_level"]

    # merge the data and keep only rows with positive commitments
    df = (
        pd.merge(commitments, rate, on=idx, how="left", suffixes=("_commitments", ""))
        .merge(grace, on=idx, how="left", suffixes=("", "_grace"))
        .merge(maturities, on=idx, how="left", suffixes=("", "_maturities"))
        .rename(columns={"value": "value_rate"})
        .loc[lambda d: d.value_commitments > 0]
    )

    return df
