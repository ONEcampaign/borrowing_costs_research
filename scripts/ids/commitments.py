import pandas as pd

from scripts.ids.clean_data import get_clean_data

COMMITMENTS_INDICATORS = {
    "DT.COM.BLAT.CD": "Bilateral",
    "DT.COM.MLAT.CD": "Multilateral",
    "DT.COM.PRVT.CD": "Private",
}


def get_commitments(
    start_year: int,
    end_year: int,
    counterparts: list[str] = None,
) -> pd.DataFrame:
    """Get data with the commitments for each country/counterpart_area pair."""
    data = get_clean_data(
        start_year=start_year,
        end_year=end_year,
        indicators=COMMITMENTS_INDICATORS,
        counterparts=counterparts,
    )

    return data
