# NPRZ Data Preparation Pipeline

R pipeline that processes CBS (Statistics Netherlands) open data and CBS microdata into the parquet and GeoJSON files served by the web app. All outputs go directly to `../app-development/static/data/`.

## Scripts

```
R_scripts/preparing_data/
├── 01_setup_and_process.R     # Main pipeline — CBS kerncijfers, grids, node summaries
├── 03_process_example_data.R  # Study area boundary, PC4 centroids, inner boundary codes
└── utils.R                    # Shared helper functions used by both scripts
```

Run `01_setup_and_process.R` first, then `03_process_example_data.R`.

## Prerequisites

```r
install.packages(c(
  "tidyverse", "sf", "arrow", "duckdb",
  "cbsodataR", "nngeo", "units"
))
```

- R ≥ 4.1
- CBS microdata access is required for the employment node and commuting flow outputs. The CBS kerncijfers (open statistics) can be downloaded without credentials.

## What each script produces

### `01_setup_and_process.R`

Downloads and processes CBS Kerncijfers Wijken en Buurten 2024 and CBS grid statistics. Outputs:

| File | Description |
|------|-------------|
| `buurt_2024.geojson` + `buurt_2024_stats.parquet` | Neighbourhood boundaries and statistics |
| `wijk_2024.geojson` + `wijk_2024_stats.parquet` | District boundaries and statistics |
| `gemeente_2024.geojson` + `gemeente_2024_stats.parquet` | Municipality boundaries and statistics |
| `pc4_zh_2024.geojson` + `pc4_zh_2024_stats.parquet` | PC4 postcode boundaries and statistics (Zuid-Holland) |
| `grid_100m_rijnmond.geojson` + `grid_100m_rijnmond_stats.parquet` | 100m grid cells clipped to Rijnmond |
| `grid_500m_rijnmond.geojson` + `grid_500m_rijnmond_stats.parquet` | 500m grid cells clipped to Rijnmond |
| `nodes_summary_pc4.parquet` + `nodes_summary_gem.parquet` | Employment totals from microdata by PC4 and gemeente |
| `nodes_demo_inkomen_pc4.parquet` + `nodes_demo_inkomen_gem.parquet` | Income distribution of workers |
| `nodes_demo_opleiding_pc4.parquet` + `nodes_demo_opleiding_gem.parquet` | Education level of workers |
| `edges_woonwerk_pc4.parquet` + `edges_woonwerk_gem.parquet` | Home→work commuting flows |
| `edges_werkwerk_pc4.parquet` + `edges_werkwerk_gem.parquet` | Job-to-job moves |
| `edges_migration_pc4.parquet` + `edges_migration_gem.parquet` | Residential moves |
| `edges_woonwerk_ink_opl_pc4.parquet` | Commuting flows with income and education breakdown (PC4) |
| `pc4_supplementary_stats.parquet` | CBS stats for outer PC4s that appear in flow data but outside the main study area |

### `03_process_example_data.R`

Derives spatial metadata used by the app's boundary clip and model logic. Outputs:

| File | Description |
|------|-------------|
| `rotterdam_boundary.geojson` | Inner study area boundary (dashed red line on map) |
| `outer_donut.geojson` | Middle boundary minus inner area (for "both" extent mode clipping) |
| `pc4_centroids.json` | PC4 centroid coordinates for flow line rendering and haversine distance |
| `gemeente_centroids.json` | Gemeente centroid coordinates for gemeente-level flow lines |

This script also produces the `INNER_PC4_CODES` list pasted into `config.ts` — re-run and update if the study area boundary changes.

## Data sources

| Dataset | Source | Notes |
|---------|--------|-------|
| Kerncijfers Wijken en Buurten 2024 | CBS OData (dataset 85984NED) | Open, downloaded via `cbsodataR` |
| Vierkantstatistieken 100m/500m 2024 | https://download.cbs.nl/ | Open, ~200 MB download |
| PC4 postcodes 2024 | https://download.cbs.nl/postcode/ | Open |
| Wijken/Buurten geometries | CBS Geodata | Open |
| Employment microdata (nodes, flows) | CBS microdata — restricted access | Requires institutional access via CBS Remote Access |

All CBS data is © Statistics Netherlands. Attribution required in derived products.

## Column naming notes

CBS uses different column names for the same concept at different spatial scales. The `columnAt` field in each `Variable` entry in `config.ts` maps scale keys to actual parquet column names. Key differences:

- Grid/PC4: `aantal_mannen` → Admin: `mannen`
- Grid/PC4: `percentage_geb_nederland_herkomst_nederland` → Admin: `percentage_met_herkomstland_nederland`
- Grid/PC4: `gemiddelde_huishoudensgrootte` → Admin: `gemiddelde_huishoudsgrootte`

See `config.ts` comments and `docs/DATA_SCHEMA.md` for the full mapping.

## Runtime expectations

- **First run of `01_setup_and_process.R`:** 20–60 minutes (downloads ~500 MB, large geometry operations)
- **Subsequent runs:** 5–15 minutes (reuses cached downloads from `data/raw/`)
- **`03_process_example_data.R`:** 5–10 minutes

## Troubleshooting

**`object 'WijkenEnBuurten' not found`**
Delete `data/raw/kerncijfers/kwb_2024_raw.rds` and re-run — stale cache from an older dataset version.

**Geometry operations hang**
Expected for national-scale wijk/gemeente joins — allow 5+ minutes. Watch for "Geometry rows before/after filter" messages in the console.

**Parquet column not found in the app**
Check the actual column name in R with `glimpse(your_df)` or `names(your_df)`. CBS column names vary between dataset versions and spatial scales. Update `columnAt` in `config.ts` accordingly.

**`outer_donut.geojson` is empty or malformed**
Re-run section of `03_process_example_data.R` that generates the donut. This file is used by the "both" extent mode in the app and must be a valid polygon-with-hole covering the middle boundary minus the inner boundary.

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — data model and design decisions
- [`docs/DATA_SCHEMA.md`](docs/DATA_SCHEMA.md) — parquet column reference by scale
- [`docs/ADDING_DATASETS.md`](docs/ADDING_DATASETS.md) — step-by-step guide for adding new variables or scales