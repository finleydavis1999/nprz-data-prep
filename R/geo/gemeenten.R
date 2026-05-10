# Gemeente polygons → simplified WGS84 GeoJSON + simplified RD TopoJSON.
#
# Uses CBS gemeente_gegeneraliseerd (342 features, 2025 boundaries). Polygons
# include water bodies (IJsselmeer, Markermeer, Oosterschelde, Waddenzee) as
# part of municipal territory — accepted as-is per project decision.
suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
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

  list(
    geojson  = "geo/gemeenten.geojson",
    topojson = "geo/gemeenten.topo.json",
    idProp   = "area_code"
  )
}
