# Assemble + write static/data/manifest.json from per-dataset / per-geo build results.
suppressPackageStartupMessages(library(jsonlite))

write_manifest <- function(datasets, geo) {
  manifest <- list(
    version  = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    datasets = datasets,
    geo      = geo
  )
  out <- "static/data/manifest.json"
  dir.create(dirname(out), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(manifest, out, auto_unbox = TRUE, pretty = TRUE)
  cat("wrote", out, "\n")
}
