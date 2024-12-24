import pandas as pd

from scripts.ids.clean_data import get_clean_data


MATURITY_INDICATOR: str = "DT.MAT.DPPG"


def get_maturities(
    start_year: int,
    end_year: int,
    counterparts: list[str] = None,
    update_data: bool = False,
) -> pd.DataFrame:
    """Get data with the maturities for each country/counterpart_area pair."""
    return get_clean_data(
        start_year=start_year,
        end_year=end_year,
        indicators=MATURITY_INDICATOR,
        counterparts=counterparts,
    )
