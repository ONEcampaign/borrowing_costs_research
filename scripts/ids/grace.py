import pandas as pd

from scripts.ids.clean_data import get_clean_data

GRACE_PERIOD_INDICATOR: str = "DT.GPA.DPPG"


def get_grace(
    start_year: int,
    end_year: int,
    counterparts: list[str] = None,
) -> pd.DataFrame:
    """Get data with the grace period for each country/counterpart_area pair."""
    return get_clean_data(
        start_year=start_year,
        end_year=end_year,
        indicators=GRACE_PERIOD_INDICATOR,
        counterparts=counterparts,
    )
