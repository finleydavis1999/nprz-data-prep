# Data Schema
## Spatial Layers (GeoJSON)
Served from `app-development/static/data/`. Pattern: `{scale}_{year}.geojson`
| File | ID column used by MapLibre |
|------|---------------------------|
| `grid_100m_rijnmond.geojson` | `crs28992res100m` |
| `grid_500m_rijnmond.geojson` | `crs28992res500m` |
| `buurt_2024.geojson` | `buurtcode` |
| `wijk_2024.geojson` | `wijkcode` |
| `gemeente_2024.geojson` | `gemeentecode` |
| `pc4_zh_2024.geojson` | `postcode` |
| `rotterdam_boundary.geojson` | boundary line only |
Rules:
- All geometry in EPSG:4326
- GeoJSON feature `id` must match the ID column (via MapLibre `promoteId`)
- Grid layers export centroids (Points); admin layers export polygons
## Stats Tables (Parquet)
Pattern: `{scale}_{year}_stats.parquet`. Contains ID column + variable columns.
## Edge / Flow Data (Parquet)
| File | Description |
|------|-------------|
| `flows.parquet` | Full flows: origin_id, destination_id, flow_value, variable_name, year |
| `flow_summary.parquet` | Per-origin totals: origin_id, total_outflow, n_destinations |
Rules:
- `origin_id` and `destination_id` must match IDs in a spatial layer
- `flow_value` >= 0; suppressed values = NA
