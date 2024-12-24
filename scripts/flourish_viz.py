import pandas as pd

from scripts.config import Paths


def line_chart_explore_interest():
    """Reads all interest rates data and pivots entity_name"""
    dfs = []

    # Identify all csv files inside interest_rates (and subdirectories)
    for file in Paths.output.rglob("interest_rates/**/*.csv"):
        df = pd.read_csv(file)
        dfs.append(df)

    data = pd.concat(dfs).drop(
        columns=["indicator_code", "entity_code", "income_level", "continent"]
    )

    data = data.pivot(
        index=["counterpart_name", "year"],
        columns="entity_name",
        values="value",
    ).reset_index()

    data.to_csv(Paths.output / "visualisations" / "interest_rates.csv", index=False)


if __name__ == "__main__":
    line_chart_explore_interest()
