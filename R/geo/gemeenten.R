# Gemeente polygons → simplified WGS84 GeoJSON + simplified RD TopoJSON.
#
# Uses CBS gemeente_gegeneraliseerd (342 features, 2025 boundaries). Polygons
# include water bodies (IJsselmeer, Markermeer, Oosterschelde, Waddenzee) as
# part of municipal territory — accepted as-is per project decision.
suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
})
source("R/lib/geo.R")

build_gemeenten <- function() {
  src <- "raw-data/geo-data/cbsgebiedsindelingen2025.gpkg"
  gem <- sf::read_sf(src, layer = "gemeente_gegeneraliseerd") |>
    dplyr::transmute(area_code = statcode, name = statnaam)

  simplify_to_geojson_and_topojson(
    gem,
    geojson_out  = "static/data/geo/gemeenten.geojson",
    topojson_out = "static/data/geo/gemeenten.topo.json",
    keep_pct     = 15
  )

  # Centroids (point-on-surface for safety with multi-part / coastal polygons)
  # in EPSG:4326 — used by the flow layer to draw OD curves.
  pts <- gem |>
    sf::st_point_on_surface() |>
    sf::st_transform(4326)
  coords <- sf::st_coordinates(pts)
  centroids <- setNames(
    lapply(seq_len(nrow(coords)), function(i) c(coords[i, "X"], coords[i, "Y"])),
    pts$area_code
  )
  out <- "static/data/geo/gem-centroids.json"
  dir.create(dirname(out), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(centroids, out, auto_unbox = FALSE, digits = 6)
  cat("wrote", out, "(", length(centroids), "centroids )\n")

  list(
    geojson   = "geo/gemeenten.geojson",
    topojson  = "geo/gemeenten.topo.json",
    centroids = "geo/gem-centroids.json",
    idProp    = "area_code"
  )
}
