# EU Supply Chain Emissions Lakehouse

End-to-end Databricks project estimating and forecasting industrial greenhouse-gas emissions across EU countries and sectors, built on public Eurostat data using a medallion (bronze → silver → gold) architecture on Databricks Free Edition.

**Business question:** Which country–sector combinations are emissions hotspots relative to their economic output, and how are they trending? This is the starting point for Scope 3 supplier risk screening.

## Architecture

```
Eurostat SDMX API ──► UC Volume (raw .tsv.gz) ──► Bronze Delta ──► Silver Delta ──► Gold Delta ──► AI/BI Dashboard + Genie
                                                                                     └──► MLflow forecast model
```

| Layer  | Table                                   | Contents                                                      |
|--------|-----------------------------------------|---------------------------------------------------------------|
| Bronze | `bronze.eurostat_air_emissions_raw`     | Raw air emissions accounts by NACE activity (env_ac_ainah_r2) |
| Bronze | `bronze.eurostat_gva_raw`               | Raw gross value added by industry (nama_10_a64)               |
| Silver | `silver.air_emissions`                  | Long format: country, sector, pollutant, year, value, flags   |
| Silver | `silver.gva`                            | Long format: country, sector, year, GVA (EUR m)               |
| Gold   | `gold.emissions_intensity`              | tCO2e per EUR m GVA by country × sector × year                |
| Gold   | `gold.hotspots`                         | Ranked hotspots + YoY trend                                   |

## Repo structure

```
eu-emissions-lakehouse/
├── README.md
├── notebooks/
│   ├── 00_setup.py                    # schemas + volume
│   ├── 01_bronze_ingest_eurostat.py   # API → volume → bronze Delta
│   ├── 02_silver_transform.py         # (next) reshape, clean, data-quality checks
│   ├── 03_gold_metrics.py             # (next) intensity + hotspots
│   └── 04_forecast_mlflow.py          # (next) sector emissions forecast
├── pipelines/                         # (later) Lakeflow declarative pipeline version
├── dashboards/                        # exported AI/BI dashboard JSON
├── docs/
│   └── architecture.png
└── .gitignore
```

## Data sources (public)

- Eurostat `env_ac_ainah_r2`: Air emissions accounts by NACE Rev. 2 activity
- Eurostat `nama_10_a64`: National accounts aggregates by industry (up to NACE A*64)

Pulled via the Eurostat SDMX 2.1 API:
`https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/<code>?format=TSV&compressed=true`

## How to run

1. Create a Databricks Free Edition workspace.
2. Workspace → Create → Git folder → paste this repo's URL.
3. Run `notebooks/00_setup.py`, then `notebooks/01_bronze_ingest_eurostat.py` on serverless compute.
4. If the download cell fails (outbound network blocked), download the two `.tsv.gz` files from the URLs above in your browser and upload them to the volume path printed by the notebook, then re-run with `download = false`.

## Notes

- Built on Databricks Free Edition (serverless only, fair-usage quota). Non-commercial portfolio project.
