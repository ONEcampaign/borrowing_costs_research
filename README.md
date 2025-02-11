# Cost of borrowing

## Python dependencies
This project uses `poetry` to manage python dependencies. It requires `python>=3.10`.

After creating a virtual environment for this project, you can install project dependencies
by running the following command with your terminal at the project root.

```bash
poetry install
```

## Project structure

The `scripts` folder contains the different scripts and tools to conduct the analysis.

The `raw_data` folder stores raw data files.

The `output` folder stores the clean files. Currently, it contains an
[interest_rates](./output/interest_rates/) folder with country-level interest rate data for each African country, and for each EMEA country.

---

## Interest Rates data

The [ids](./scripts/ids) module contains different tools to extract, clean, and save data from the International Debt Statistics database.

The scripts are organised based on the type of data they extract. For example, [interest](./scripts/ids/interest.py) contains tools to extract interest (rates and payments) data.

The [clean_data](./scripts/ids/clean_data.py) scripts handles getting data via the API. It leverages ONE's `bblocks_importers` package for that purpose. It also adds additional information like continent name and income level, and harmonises entity names.

The [tools](./scripts/ids/tools.py) script contains a series of tools to create aggregates and further process the data. These scripts come from other ONE analysis, but are not currently used in this project.

### Indicators

#### Interest rate
For this analysis we use two indicators from the International Debt Statistics:

**`DT.INR.OFFT`: Average interest on new external debt commitments, official (%).**

Interest represents the average interest rate on all new public and publicly guaranteed loans contracted during the year. To obtain the average, the interest rates for all public and publicly guaranteed loans have been weighted by the amounts of the loans. Debt from official creditors includes loans from international organizations (multilateral loans) and loans from governments (bilateral loans). Loans from international organization include loans and credits from the World Bank, regional development banks, and other multilateral and intergovernmental agencies. Excluded are loans from funds administered by an international organization on behalf of a single donor government; these are classified as loans from governments. Government loans include loans from governments and their agencies (including central banks), loans from autonomous bodies, and direct loans from official export credit agencies.

**Note**: when exported, we flag data from this indicator with an "(official)"" suffix. By definition, data from Multilateral lenders is "official". However, data from bilateral lenders may be "official" (per the definition above) or "private" (based on the definition below). Data from private lenders is always "private".

**`DT.INR.OFFT`: Average interest on new external debt commitments, private (%)**

Interest represents the average interest rate on all new public and publicly guaranteed loans contracted during the year. To obtain the average, the interest rates for all public and publicly guaranteed loans have been weighted by the amounts of the loans. Debt from private creditors include bonds that are either publicly issued or privately placed; commercial bank loans from private banks and other private financial institutions; and other private credits from manufacturers, exporters, and other suppliers of goods, and bank credits covered by a guarantee of an export credit agency.


#### Missing data
Data may not be available for every country for every year. This could mean that the data wasn't reported by the country, or that no new debt was agreed in that given year.

A few countries have no data in the IDS database:

**African countries**
- Equatorial Guinea
- Libya
- South Sudan
- Seychelles

**Other EMDE**
- United Arab Emirates
- Antigua and Barbuda
- Bulgaria
- Bahrain
- Bahamas
- Barbados
- Brunei Darussalam
- Bhutan
- Chile
- FS Micronesia
- Hungary
- Kiribati
- Saint Kitts and Nevis
- Kwait
- Marshall Islands
- Malaysia
- Nauru
- Oman
- Panama
- Palau
- Poland
- Palestine
- Qatar
- Romania
- Russian Federation
- Saudi Arabia
- Tuvalu
- Uruguay
- Venezuela


---

## Project-level World Bank data

Interest rate information is available for a large number of World Bank projects via two different datasets:
- [IDA statement of Credits, Grants, and Guarantees](https://financesone.worldbank.org/ida-statement-of-credits-grants-and-guarantees-historical-data/DS00976)
- [IBRD statement of Loans and Guarantees](https://datacatalog.worldbank.org/search/dataset/0037715/IBRD-Statement-Of-Loans-and-Guarantees---Historical-Data)

The [scripts](./scripts/wb/statements_api.py) use the relevant API to extract, clean and harmonise the data.

The two historical datasets are only available from 2011. But they include projects that were approved by the board as far back as 1947. 

Currently, the data is current as of November 2024.

Interest rate data is available for 15,747 projects.

## Missing interest rates

According to the World Bank: "Current Interest rate or service charge applied to loan. For loans that could have more than one interest rate (e.g. FSL or SCL fixed rate loans), the interest rate is shown as “0”.

Data is therefore missing for 5,304 projects.

Project-level data used to be reported by the World Bank to the OECD Development Assistance Committee via the Creditor Reporting System dataset. The World Bank has not reported interest rate data (or loan terms data) since 2018.

Based on the pre-2018 data, some gaps can be filled using the OECD data.

The tools inside the [crs](./scripts/crs/crs_data.py) module help with downloading and processing the CRS data.

The [project_level](./scripts/project_level.py) scripts deal with filling the gaps in World Bank data to the extent possible. This method allows us to add interest rate data for an additional 2,640 projects.

That means that 12.6% of projects still have missing interest rates. Future research will focus on estimating those rates using data reported via the International Debt Statistics (IDS) dataset.