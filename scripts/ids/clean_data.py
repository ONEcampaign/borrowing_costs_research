import pandas as pd
from bblocks import add_income_level_column, convert_id, set_bblocks_data_path
from bblocks_data_importers import InternationalDebtStatistics

from scripts.config import Paths

set_bblocks_data_path(Paths.raw_data)


def _clean_counterpart_area(df: pd.DataFrame) -> pd.DataFrame:
    """Remove the non-breaking space from the counterpart_area column
    and harmomise the names of the counterpart areas (when possible)."""
    return df.assign(
        counterpart_name=lambda d: d.counterpart_name.apply(
            lambda r: r.replace("\xa0", "")
        )
    ).assign(
        counterpart_name=lambda d: convert_id(
            d["counterpart_name"], from_type="regex", to_type="name_short"
        ).map(lambda x: ", ".join(x) if isinstance(x, list) else str(x))
    )


def _add_continent(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(
        continent=lambda d: convert_id(
            d.entity_name, from_type="regex", to_type="continent"
        )
    )


def _clean_indicators(
    df: pd.DataFrame,
    filter_columns: bool = True,
    counterparts: list = None,
) -> pd.DataFrame:
    """Clean the IDS data.

    Optionally filter counterparts to keep only those in study_counterparts.
    Optionally filter columns to keep only the ones needed for the analysis.

    """

    df = (
        df.pipe(_clean_counterpart_area)
        .pipe(add_income_level_column, id_column="entity_name", id_type="regex")
        .pipe(_add_continent)
        .dropna(subset=["income_level"])
    )

    if counterparts is not None:
        if isinstance(counterparts, str):
            counterparts = [counterparts]
        df = df.query(f"counterpart_name in {counterparts}")

    if filter_columns:
        df = df.filter(
            [
                "entity_code",
                "entity_name",
                "counterpart_name",
                "income_level",
                "continent",
                "indicator_code",
                "year",
                "value",
            ],
            axis=1,
        )

    return df.reset_index(drop=True)


def get_clean_data(
    start_year,
    end_year,
    indicators: list | dict | str,
    countries: list = None,
    counterparts: list | dict = None,
) -> pd.DataFrame:
    """Get indicator data for each country/counterpart_area pair."""

    # Create IDS object
    ids = InternationalDebtStatistics()

    # Filter years
    if start_year is not None and end_year is not None:
        ids.set_years(years=range(start_year, end_year + 1))

    # Filter countries
    if countries is not None:
        ids.set_economies(economies=countries)

    df = ids.get_data(series=indicators)

    # Get data and clean it
    df = df.pipe(_clean_indicators, counterparts=counterparts)

    # Make sure only the right indicators are kept for each counterpart
    if isinstance(indicators, dict):
        condition = ""
        indicator_types = {v: k for k, v in indicators.items()}
        if counterparts is not None:
            for counterpart, indicator_type in counterparts.items():
                condition += (
                    f"(counterpart_name == '{counterpart}' and "
                    f"indicator_code == '{indicator_types[indicator_type]}') or "
                )

            df = df.query(condition[:-4]).reset_index(drop=True)

    return df
