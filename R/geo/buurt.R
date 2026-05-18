# Buurt polygons → simplified WGS84 GeoJSON.
# Uses CBS wijkbuurtkaart 2024 v2 (buurten layer).
# Output: static/data/geo/buurt.geojson
suppressPackageStartupMessages({
  library(sf)
  library(dplyr)
  library(jsonlite)
})
source("R/lib/geo.R")

build_buurt <- function() {
  zip  <- "raw-data/cbs/wijkbuurtkaart_2024_v2.zip"
  extr <- "raw-data/cbs/wijkbuurtkaart_2024"
  gpkg <- file.path(extr, "WijkBuurtkaart_2024_v2/wijkenbuurten_2024_v2.gpkg")

  if (!file.exists(zip)) {
    cat("Downloading CBS wijkbuurtkaart 2024...\n")
    download.file(
      "https://geodata.cbs.nl/files/Wijkenbuurtkaart/WijkBuurtkaart_2024_v2.zip",
      zip, mode = "wb"
    )
  }
  if (!dir.exists(extr)) unzip(zip, exdir = extr)

  buurt_sf <- sf::read_sf(gpkg, layer = "buurten") |>
    sf::st_make_valid() |>
    dplyr::transmute(
      area_code = buurtcode,
      name      = buurtnaam
    )

  simplify_to_geojson_and_topojson(
    buurt_sf,
    geojson_out  = "static/data/geo/buurt.geojson",
    topojson_out = "static/data/geo/buurt.topo.json",
    keep_pct     = 15
  )

  # Centroids
  pts84 <- buurt_sf |> sf::st_point_on_surface() |> sf::st_transform(4326)
  coords84 <- sf::st_coordinates(pts84)
  centroids84 <- setNames(
    lapply(seq_len(nrow(coords84)), function(i) c(coords84[i, "X"], coords84[i, "Y"])),
    pts84$area_code
  )
  out84 <- "static/data/geo/buurt-centroids.json"
  dir.create(dirname(out84), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(centroids84, out84, auto_unbox = FALSE, digits = 6)
  cat("wrote", out84, "(", length(centroids84), "centroids )\n")

  pts_rd <- buurt_sf |> sf::st_point_on_surface() |> sf::st_transform(28992)
  coords_rd <- sf::st_coordinates(pts_rd)
  centroids_rd <- setNames(
    lapply(seq_len(nrow(coords_rd)), function(i) c(coords_rd[i, "X"], coords_rd[i, "Y"])),
    pts_rd$area_code
  )
  out_rd <- "static/data/geo/buurt-centroids-rd.json"
  jsonlite::write_json(centroids_rd, out_rd, auto_unbox = FALSE, digits = 1)
  cat("wrote", out_rd, "(", length(centroids_rd), "centroids )\n")

  list(
    geojson     = "geo/buurt.geojson",
    topojson    = "geo/buurt.topo.json",
    centroids   = "geo/buurt-centroids.json",
    centroidsRd = "geo/buurt-centroids-rd.json",
    idProp      = "area_code"
  )
}