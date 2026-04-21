# =============================================================================
# utils.R
# Shared helper functions for the NPRZ data pipeline
# Source at top of other scripts: source("R_scripts/preparing_data/utils.R")
# =============================================================================
# Drop suppressed CBS values (-99995, -99997, etc.) replacing with NA
drop_suppressed <- function(df, label = "") {
  numeric_cols <- names(df)[sapply(df, is.numeric)]
  for (col in numeric_cols) {
    df[[col]][df[[col]] %in% c(-99995, -99997, -99994, -99993, -99999)] <- NA
  }
  message(sprintf("[utils] %s: suppressed values replaced with NA", label))
  df
}
# Build Rijnmond boundary polygon from gemeente geometry file
# gpkg_path : path to .gpkg file containing a "gemeenten" layer
# gm_codes  : character vector of GM codes e.g. c("GM0599", ...)
build_rijnmond_boundary <- function(gpkg_path, gm_codes) {
  gemeenten <- sf::st_read(gpkg_path, layer = "gemeenten", quiet = TRUE)
  boundary  <- gemeenten |>
    dplyr::filter(gemeentecode %in% gm_codes) |>
    sf::st_union() |>
    sf::st_make_valid()
  message(sprintf("[utils] Boundary built from %d municipalities", length(gm_codes)))
  boundary
}
# Export one administrative scale: GeoJSON (geometry) + Parquet (stats)
# joined_sf   : sf object already joined and reprojected to WGS84
# id_col      : column name used as feature ID (e.g. "buurtcode")
# scale_name  : output file prefix  (e.g. "buurt_2024")
# export_dir  : destination folder  (app-development/static/data)
export_admin_scale <- function(joined_sf, id_col, scale_name, export_dir) {
  if (!dir.exists(export_dir)) dir.create(export_dir, recursive = TRUE)
  # GeoJSON — id column + simplified polygon only
  geom_only <- sf::st_sf(
  setNames(list(joined_sf[[id_col]]), id_col),
  geometry = sf::st_geometry(joined_sf),
  crs = 4326
)

geom_simple <- sf::st_simplify(geom_only, dTolerance = 0.001,
                                preserveTopology = TRUE)
geom_simple <- sf::st_make_valid(geom_simple)
geom_simple$id <- geom_simple[[id_col]] # Add top-level id field so MapLibre promoteId works reliably

geojson_path <- file.path(export_dir, paste0(scale_name, ".geojson"))

sf::st_write(geom_simple, geojson_path, driver = "GeoJSON",
             layer_options = "ID_FIELD=id", delete_dsn = TRUE)

  message(sprintf("[utils] GeoJSON exported: %s", geojson_path))
  # Parquet — all stats columns, no geometry
  stats_df <- sf::st_drop_geometry(joined_sf) |> as.data.frame()
  parquet_path <- file.path(export_dir, paste0(scale_name, "_stats.parquet"))
  arrow::write_parquet(stats_df, parquet_path)
  message(sprintf("[utils] Parquet exported: %s", parquet_path))
}
# Validate flow/edge data before export
# flows_df  : data frame with commuting/flow data
# node_ids  : optional vector of valid spatial IDs to check against
# label     : name used in log messages
validate_flows <- function(flows_df, node_ids = NULL, label = "flows") {
  required <- c("origin_id", "destination_id", "flow_value")
  missing  <- setdiff(required, names(flows_df))
  if (length(missing) > 0)
    stop(sprintf("[validate_flows] Missing columns: %s", paste(missing, collapse = ", ")))
  n_na <- sum(is.na(flows_df$flow_value))
  if (n_na > 0)
    message(sprintf("[validate_flows] %s: %d NA flow values", label, n_na))
  if (!is.null(node_ids)) {
    bad_o <- sum(!flows_df$origin_id      %in% node_ids)
    bad_d <- sum(!flows_df$destination_id %in% node_ids)
    if (bad_o > 0) message(sprintf("[validate_flows] %s: %d unmatched origins",      label, bad_o))
    if (bad_d > 0) message(sprintf("[validate_flows] %s: %d unmatched destinations", label, bad_d))
  }
  message(sprintf("[validate_flows] %s: %d rows validated", label, nrow(flows_df)))
  flows_df
}
