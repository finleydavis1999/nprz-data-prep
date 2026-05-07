# NPRZ App — Development Guide

A static SvelteKit web app for spatial labour market analysis in the Rotterdam Zuid (NPRZ) area. Runs entirely in the browser — DuckDB-WASM queries pre-computed parquet files over HTTP, MapLibre GL JS renders the map. No backend, no server-side processing.

## Prerequisites

- Node.js ≥ 18

## Getting started

```bash
cd app-development
npm install
npm run dev
# Open http://localhost:5173
```

Data files are already in `static/data/` and are committed to the repository. No R setup is needed to run or develop the app.

## Source structure

```
src/
├── lib/
│   ├── config.ts        # All scales, variables, edge datasets, colour ramps,
│   │                    # and helper functions. Edit here to add new data.
│   ├── types.ts         # TypeScript interfaces (Scale, Variable, EdgeDataset, etc.)
│   ├── classify.ts      # Quantile break computation and feature classification
│   ├── format.ts        # Number formatting (fmt) and variable grouping
│   ├── db.ts            # DuckDB-WASM init, serialised query queue, fetchRows()
│   ├── map.ts           # MapLibre init, layer registration, clip expressions
│   ├── edges.ts         # OD flow line loading and rendering
│   ├── nodal.ts         # Choropleth classification and feature-state rendering
│   ├── calculator.ts    # Multi-term expression evaluation
│   ├── model.ts         # OLS solver, gravity model, nodal model, residuals
│   ├── popup.ts         # Map click popup content builders
│   └── index.ts         # Barrel re-export (import everything from '$lib')
└── routes/
    └── +page.svelte     # App shell — reactive state, runUpdate(), HTML template
```

The `.svelte` file is intentionally kept thin. All business logic lives in the `.ts` modules. Svelte 5 runes (`$state`, `$derived`, `$effect`) must stay in `.svelte` files — they cannot be used in plain TypeScript.

## Adding a new variable

1. Confirm the column name in the relevant parquet (check via DuckDB or R `glimpse()`)
2. Add an entry to `VARIABLES` in `src/lib/config.ts`:
   ```ts
   {
     key: 'my_variable',
     label: 'My variable label',
     group: 'Population',
     canNormalise: true,
     availableAt: ['buurt', 'wijk', 'gemeente'],
     columnAt: {
       buurt:    'cbs_column_name_buurt',
       wijk:     'cbs_column_name_wijk',
       gemeente: 'cbs_column_name_gem',
     },
   }
   ```
3. The variable will appear automatically in the Nodal tab dropdown, grouped by `group`.

CBS uses different column names for the same concept at different scales — `columnAt` maps each scale key to the actual parquet column. See existing entries in `config.ts` for examples.

## Adding a new spatial scale

1. Add GeoJSON and stats parquet to `static/data/`
2. Add a `Scale` entry to `INNER_SCALES` or `OUTER_SCALES` in `config.ts`
3. The scale chip will appear automatically in the UI for any variable that lists it in `availableAt`

## Adding a new edge dataset

1. Add flow parquet(s) to `static/data/`
2. Add an `EdgeDataset` entry to `EDGE_DATASETS` in `config.ts`
3. Add centroid JSON if the scale is new
4. The dataset will appear in the Edges tab automatically

## Architecture notes

### Data fetching
`fetchRows(varKey, scaleKey, norm)` in `db.ts` is the universal data fetcher. It routes to the correct parquet based on the variable's `source` field:

| `source` value | Parquet routed to |
|---------------|------------------|
| *(none)*      | CBS stats parquet for the scale |
| `nodes_emp`   | `nodes_summary_{pc4\|gem}.parquet` |
| `nodes_ink`   | `nodes_demo_inkomen_{pc4\|gem}.parquet` |
| `nodes_opl`   | `nodes_demo_opleiding_{pc4\|gem}.parquet` |
| `flows`       | `edges_woonwerk_{pc4\|gem}.parquet` |

### Query serialisation
DuckDB-WASM throws a `DataCloneError` when two queries run concurrently on the same connection. All queries go through `queuedQuery()` in `db.ts`, which serialises them via a promise chain.

### Choropleth rendering
MapLibre feature state is used rather than rebuilding GeoJSON on every variable change. Each feature gets a `cls` property (−1 = no data, 0–3 = quantile class). The colour expression in `map.ts` maps `cls` to the colour ramp at paint time.

### "Both" extent mode
When the user selects both a study-area scale and a wider-region scale simultaneously, break points are computed from the union of both datasets so colours are directly comparable. MapLibre filter expressions clip the outer layer to exclude the inner boundary area and vice versa.

## Known issues

- **"Both" extent mode:** outer PC4 polygons can visually bleed into the study area. Requires `outer_donut.geojson` (polygon with hole) to be regenerated from R.
- **Choropleth popup:** shows feature class only, not the actual variable value. Variable value lookup on click is not yet implemented.
- **Residual flow lines:** not currently clickable for popup detail.
- **Combined ink+opl filter on edges:** can occasionally cause a `DataCloneError` if queries overlap.

## Production build

```bash
npm run build
# Preview locally:
npm run preview
```

The build output is a fully static site — deploy to Vercel, Netlify, or any static host. No adapter configuration needed for static deployment.