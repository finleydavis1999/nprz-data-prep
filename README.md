# NPRZ Spatial Analysis App

Research tool for spatial analysis of labour markets in the Rotterdam Zuid (NPRZ) area, developed at the Spatial Networks Lab, KU Leuven. The app enables researchers and urban analysts to explore demographic and employment patterns across multiple spatial scales, visualise commuting flows, and fit spatial interaction models — all running in the browser with no backend.

## Repository structure

```
nprz_project/
├── app-development/     # SvelteKit web application
├── R_scripts/           # Data preparation pipeline (CBS data → parquets)
├── .gitignore
└── README.md            # This file
```

## Branches

| Branch | Purpose |
|--------|---------|
| `finley/working-state` | Stable reference — do not modify |
| `finley/restructure` | Current development branch |

## Quick start

```bash
cd app-development
npm install
npm run dev
# Open http://localhost:5173
```

The app loads pre-generated parquet files from `app-development/static/data/`. No R setup needed to run the app.

To regenerate data from source, see [`R_scripts/README.md`](R_scripts/README.md).

## What the app does

The NPRZ Spatial Explorer has four tabs:

- **Nodal** — choropleth display of 60+ CBS demographic, housing, income, and employment variables at six spatial scales (100m grid, 500m grid, buurt, PC4, wijk, gemeente). Supports study area / wider region / both extent modes and per-km² or per-1000-population normalisation.
- **Edges** — OD flow lines for home→work commuting, job-to-job moves, and residential migration at gemeente and PC4 level. Period selection and income/education filters.
- **Calculate** — multi-term expression builder (up to four terms, mixed variables and constants) rendered as a comparative green choropleth.
- **Model** — log-linear OLS gravity model fitted to PC4 commuting flows, with distance decay, optional covariates, and residuals drawn as coloured flow lines. Also supports nodal (area-level) OLS regression.

## Data sources

All statistical data is © Statistics Netherlands (CBS). Attribution required in derived products.

- CBS Vierkantstatistieken (100m/500m grids)
- CBS Kerncijfers Wijken en Buurten 2024 (buurt/wijk/gemeente)
- CBS PC4 postcodes, Zuid-Holland
- CBS microdata — employment nodes and commuting flows (restricted access, processed via R pipeline)

## Tech stack

SvelteKit 5 · MapLibre GL JS · DuckDB-WASM · TypeScript

## License

Code: to be confirmed. Data: © CBS — see above.