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
# Phase 1 follow-ups (scaffolded later):
# source("R/nodes/banen-werk.R")
# source("R/nodes/banen-woon.R")

cat("=== building geo ===\n")
geo <- list(
  pc4 = build_pc4(),
  gem = build_gemeenten()
)

cat("\n=== building node datasets ===\n")
datasets <- list(
  demographics = build_demographics()
)

cat("\n=== writing manifest ===\n")
write_manifest(datasets, geo)
cat("\nDone.\n")
