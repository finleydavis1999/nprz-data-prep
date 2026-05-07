# Adding New Datasets

## Adding a new variable (CBS stats column)

CBS variables are columns in the existing scale parquets. No new files needed — just register the variable in the frontend config.

1. **Find the column name** — open the parquet in R and run `glimpse()` or `names()`. Column names differ between scales (see `DATA_SCHEMA.md`).

2. **Add to `VARIABLES` in `app-development/src/lib/config.ts`:**

```ts
{
  key: 'my_variable',          // unique identifier, used in code
  label: 'My variable label',  // shown in UI dropdown
  group: 'Housing',            // optgroup in dropdown
  canNormalise: true,          // whether per-km² / per-1000 makes sense
  availableAt: ['buurt', 'wijk', 'gemeente'],  // scales that have this column
  columnAt: {
    buurt:    'cbs_column_buurt',
    wijk:     'cbs_column_wijk',
    gemeente: 'cbs_column_gem',
  },
},
```

3. **Test** — `npm run dev`, select the variable, check it renders. Watch browser console for `[fetchRows]` warnings about missing columns.

### Variables that are percentages
Set `canNormalise: false` — normalising a percentage by population or area is meaningless.

### Variables only available at some scales
Only list the scales in `availableAt` where the column exists. The UI will show scale chips only for those scales.

### Variables from node summary parquets
Set `source: 'nodes_emp'` and make the key match `nodes_{column}` where `{column}` is the actual parquet column (e.g. `nodes_banen_werk` → column `banen_werk`). No `columnAt` needed.

---

## Adding a new spatial scale

1. **Prepare in R:**
   - GeoJSON with geometry in EPSG:4326, `promoteId`-compatible feature ID column
   - Stats parquet with the same ID column and variable columns

2. **Place files in `app-development/static/data/`:**
   - `{scale}_{year}.geojson`
   - `{scale}_{year}_stats.parquet`

3. **Register in `config.ts`:**

```ts
// Add to INNER_SCALES (study area) or OUTER_SCALES (wider region):
{
  key: 'my_scale',
  label: 'My Scale',
  geojson: 'my_scale_2024.geojson',
  stats:   'my_scale_2024_stats.parquet',
  id:      'my_id_column',   // must match GeoJSON feature property AND parquet column
  type:    'polygon',        // 'polygon' or 'point'
},
```

4. **Add layers in `map.ts`** — if your scale is a point layer (grid-style), add a `symbol` layer entry like the existing `100m` and `500m` entries. Polygon scales are handled automatically.

5. **Add to `availableAt`** in any variables that have data for this scale.

---

## Adding a new edge dataset

Edge datasets are OD flow parquets with columns: `origin_id`, `destination_id`, `periode`, `flow_value`.

1. **Place parquets in `static/data/`:**
   - `edges_{name}_{scale}.parquet` — full flows
   - `edges_{name}_summary_{scale}.parquet` — per-origin totals (optional, not currently queried)

2. **Add centroids JSON** if the scale is new — array of `{id, lng, lat}` objects.

3. **Register in `EDGE_DATASETS` in `config.ts`:**

```ts
{
  key:          'my_dataset_gem',
  label:        'My dataset (gemeente)',
  description:  'Brief description for UI',
  flows:        'edges_my_dataset_gem.parquet',
  flowSummary:  'edges_my_dataset_summary_gem.parquet',
  scaleKey:     'gemeente',
  colour:       '#e63946',
  periods:      ['period1', 'period2'],
  periodLabels: { 'period1': '2007–2012', 'period2': '2012–2017' },
  defaultPeriod: 'period2',
  idCols: { origin: 'origin_id', destination: 'destination_id', period: 'periode' },
  hasBreakdown: false,  // true only if breakdown parquet with ink/opl columns exists
},
```

4. The dataset will appear in the Edges tab automatically. No changes to `edges.ts` or `+page.svelte` needed.

---

## Adding income/education breakdown to an edge dataset

Breakdown filtering requires a separate parquet with `inks` (income category 1–5) and `opl` (education level 1–3) columns in addition to the standard flow columns.

1. Generate `edges_{name}_ink_opl_{scale}.parquet` in R
2. Set `hasBreakdown: true` in the `EdgeDataset` config entry
3. Update the breakdown parquet filename in `loadEdgeLayer()` in `edges.ts` if the naming pattern differs from `edges_woonwerk_ink_opl_{scale}.parquet`

---

## Updating the inner boundary PC4 list

The `INNER_PC4_CODES` array in `config.ts` defines which PC4s are considered part of the inner study area. This controls the "both" extent mode clipping and the study-area filter toggle.

After changing the study area in R:
1. Run `03_process_example_data.R`
2. In R: `sort(as.integer(pc4_inner$postcode))` — copy the output
3. Paste into `INNER_PC4_CODES` in `config.ts`
4. Update `INNER_GEMEENTE_CODES` and `INNER_GM_NUMS` similarly if gemeente boundaries changed