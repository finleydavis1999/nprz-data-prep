# PC4 polygons → simplified WGS84 GeoJSON + simplified RD TopoJSON.
suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
})
source("R/lib/geo.R")

build_pc4 <- function() {
  src <- "raw-data/geo-data/CBS-PC4-2017-v3/CBS_PC4_2017_v3.shp"
  pc4 <- sf::read_sf(src, query = "SELECT PC4 FROM CBS_PC4_2017_v3") |>
    dplyr::transmute(area_code = sprintf("%04d", as.integer(PC4)))

  simplify_to_geojson_and_topojson(
    pc4,
    geojson_out  = "static/data/geo/pc4.geojson",
    topojson_out = "static/data/geo/pc4.topo.json",
    keep_pct     = 6
  )

  list(
    geojson  = "geo/pc4.geojson",
    topojson = "geo/pc4.topo.json",
    idProp   = "area_code"
  )
}
