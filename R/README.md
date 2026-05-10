# R data pipeline

Converts source SQLite + geo files into the parquet + GeoJSON + manifest layout consumed by the SvelteKit app.

## Inputs (`raw-data/`, gitignored)

- `nodes-2018.sqlite` — node datasets (demographics; banen-werk and banen-woon to follow)
- `geo-data/CBS-PC4-2017-v3/CBS_PC4_2017_v3.shp` — PC4 postcode polygons (RD/EPSG:28992)
- `geo-data/cbsgebiedsindelingen2025.gpkg` — CBS administrative boundaries (gemeente_gegeneraliseerd, 2025)

## Outputs (`static/data/`, gitignored)

- `manifest.json` — single source of truth for datasets, fields, geo paths
- `parquet/<dataset>-<scale>.parquet` — one file per (dataset, scale)
- `geo/{pc4,gemeenten}.geojson` — WGS84, simplified, for MapLibre
- `geo/{pc4,gemeenten}.topo.json` — RD/EPSG:28992, simplified, for d3-geo print

## Run

```
npm run data        # = Rscript R/build.R
```

## R dependencies

```r
install.packages(c("DBI", "duckdb", "sf", "dplyr", "jsonlite", "rmapshaper", "geojsonio"))
```

`rmapshaper` and `geojsonio` both depend on `V8`. Simplification uses the mapshaper JS lib bundled inside `rmapshaper` (no separate npm install).

## Notes

- Gemeente polygons (`gemeente_gegeneraliseerd`) include water bodies (IJsselmeer, Markermeer, Oosterschelde, Waddenzee) as part of municipal territory. No water mask is applied.
- PC4 codes are stored as 4-digit zero-padded strings (`"1011"`); gemeente codes use the CBS `statcode` form (`"GM0014"`).
- Edge datasets and additional node datasets are scaffolded for later phases.
