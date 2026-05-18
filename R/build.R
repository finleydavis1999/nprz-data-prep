# Orchestrator: `npm run data` → `Rscript R/build.R`
#
# Builds:
#   static/data/manifest.json
#   static/data/parquet/<dataset>-<scale>.parquet
#   static/data/geo/{pc4,gemeenten}.{geojson,topo.json}
#
# Run from the project root.
if (!file.exists("R/build.R")) {
  stop("Run from the project root: Rscript R/build.R")
}

source("R/_manifest.R")
source("R/geo/pc4.R")
source("R/geo/gemeenten.R")
source("R/nodes/demographics.R")
source("R/nodes/banen-werk.R")
source("R/nodes/banen-woon.R")
source("R/edges/ovin.R")
source("R/nodes/cbs-census.R")
source("R/edges/woonwerk.R")
source("R/edges/werkwerk.R")
source("R/edges/migration.R")
source("R/geo/buurt.R")

# NOTE: most build functions require restricted CBS microdata in raw-data/.
# Exception: source("R/nodes/cbs-census.R") downloads public CBS data
# automatically and can be run on any machine.

cat("=== building geo ===\n")
geo <- list(
  pc4   = build_pc4(),
  gem   = build_gemeenten(),
  buurt = build_buurt()
)

cat("\n=== building node datasets ===\n")
datasets <- list(
  demographics        = build_demographics(),
  `banen-werk`        = build_banen_werk(),
  `banen-woon`        = build_banen_woon(),
  `cbs-census-pc4`    = build_cbs_census_pc4(),
  `cbs-census-gem`    = build_cbs_census_gem(),
  `cbs-census-gem-grid` = build_cbs_census_gem_grid()
)

cat("\n=== building flow datasets ===\n")
flows <- list(
  ovin = build_ovin(),
  woonwerk = build_woonwerk(),
  werkwerk = build_werkwerk(),
  migration = build_migration()
)

cat("\n=== writing manifest ===\n")
write_manifest(datasets, geo, flows)
cat("\nDone.\n")
