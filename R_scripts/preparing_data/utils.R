# =============================================================================
# utils.R
# Shared helper functions for the NPRZ data pipeline
# Source at top of other scripts: source("R_scripts/preparing_data/utils.R")
#
# Functions:
#   drop_suppressed()       — replace CBS suppressed values with NA
#   export_admin_scale()    — export GeoJSON + Parquet for an admin scale
#   build_rijnmond_boundary() — build boundary polygon from gemeente codes
#   validate_flows()        — validate edge/flow data schema
#   safe_write_parquet()    — write parquet without Windows memory-map lock
#   safe_write_geojson()    — write GeoJSON without Windows memory-map lock
# =============================================================================

# ── Replace CBS suppressed values with NA ─────────────────────────────────────
# CBS uses -99995, -99997 etc. for suppressed cells (privacy threshold < 10)
# Call this after loading any CBS stats data
drop_suppressed <- function(df, label = "") {
  suppressed <- c(-99995, -99997, -99994, -99993, -99999)
  numeric_cols <- names(df)[sapply(df, is.numeric)]
  for (col in numeric_cols) {
    df[[col]][df[[col]] %in% suppressed] <- NA
  }
  if (nchar(label) > 0)
    message(sprintf("[utils] %s: suppressed values replaced with NA", label))
  df
}

# ── Safe write: avoids Windows memory-map file lock (error 1224) ──────────────
# Always writes to .tmp file first, then renames — never overwrites in place
safe_write_parquet <- function(df, path) {
  tmp <- paste0(path, ".tmp")
  arrow::write_parquet(df, tmp)
  if (file.exists(path)) file.remove(path)
  file.rename(tmp, path)
  message(sprintf("[utils] Parquet: %s (%.1f KB, %d rows)",
                  basename(path), file.size(path)/1024, nrow(df)))
}

safe_write_geojson <- function(sf_obj, path, layer_options = "ID_FIELD=id") {
  tmp <- paste0(path, ".tmp.geojson")
  if (length(layer_options) == 0 || identical(layer_options, character(0))) {
    sf::st_write(sf_obj, tmp, driver = "GeoJSON",
                 delete_dsn = TRUE, quiet = TRUE)
  } else {
    sf::st_write(sf_obj, tmp, driver = "GeoJSON",
                 layer_options = layer_options, delete_dsn = TRUE, quiet = TRUE)
  }
  if (file.exists(path)) file.remove(path)
  file.rename(tmp, path)
  message(sprintf("[utils] GeoJSON: %s (%.1f KB, %d features)",
                  basename(path), file.size(path)/1024, nrow(sf_obj)))
}

# ── Export one administrative scale: GeoJSON (geometry) + Parquet (stats) ─────
# joined_sf   : sf object already joined, reprojected to WGS84
# id_col      : column name used as feature ID (e.g. "buurtcode")
# scale_name  : output file prefix (e.g. "buurt_2024")
# export_dir  : destination folder (app-development/static/data)
# tolerance   : st_simplify tolerance in decimal degrees
#
# Notes:
# - Water-only features (water == "WATER") are removed before export
# - Raw OData CamelCase columns (_1, _2 suffix) are removed from stats
# - GeoJSON includes top-level `id` field for MapLibre promoteId
export_admin_scale <- function(joined_sf, id_col, scale_name, export_dir,
                                tolerance = 0.001) {
  if (!dir.exists(export_dir)) dir.create(export_dir, recursive = TRUE)

  # Remove water-only features
  if ("water" %in% names(joined_sf)) {
    n_before <- nrow(joined_sf)
    joined_sf <- joined_sf[is.na(joined_sf$water) | joined_sf$water != "WATER", ]
    if (nrow(joined_sf) < n_before)
      message(sprintf("[utils] %s: removed %d water features",
                      scale_name, n_before - nrow(joined_sf)))
  }

  # GeoJSON: id column + simplified polygon only
  geom_only <- sf::st_sf(
    setNames(list(joined_sf[[id_col]]), id_col),
    geometry = sf::st_geometry(joined_sf),
    crs = 4326
  )
  geom_simple <- sf::st_simplify(geom_only, dTolerance = tolerance,
                                  preserveTopology = TRUE)
  geom_simple <- sf::st_make_valid(geom_simple)
  geom_simple$id <- geom_simple[[id_col]]

  geojson_path <- file.path(export_dir, paste0(scale_name, ".geojson"))
  safe_write_geojson(geom_simple, geojson_path)

  # Parquet: all stats columns, no geometry
  # Remove raw OData CamelCase duplicate columns (e.g. Gemeentenaam_1)
  stats_df <- sf::st_drop_geometry(joined_sf) |>
    as.data.frame() |>
    dplyr::select(-dplyr::matches("_\\d+$"))

  parquet_path <- file.path(export_dir, paste0(scale_name, "_stats.parquet"))
  safe_write_parquet(stats_df, parquet_path)
}

# ── Build Rijnmond boundary from gemeente geometry file ───────────────────────
# gpkg_path : path to CBS WijkBuurtkaart .gpkg file
# gm_codes  : character vector of GM codes e.g. c("GM0599", "GM0489", ...)
# Returns   : sfc geometry in the CRS of the source file (RD New / EPSG:28992)
build_rijnmond_boundary <- function(gpkg_path, gm_codes) {
  gemeenten <- sf::st_read(gpkg_path, layer = "gemeenten", quiet = TRUE)

  missing <- gm_codes[!gm_codes %in% gemeenten$gemeentecode]
  if (length(missing) > 0)
    warning(sprintf("[utils] Missing gemeente codes: %s",
                    paste(missing, collapse = ", ")))

  boundary <- gemeenten |>
    dplyr::filter(gemeentecode %in% gm_codes) |>
    sf::st_union() |>
    sf::st_make_valid()

  message(sprintf("[utils] Boundary built from %d/%d municipalities",
                  sum(gm_codes %in% gemeenten$gemeentecode), length(gm_codes)))
  boundary
}

# ── Validate flow/edge data before export ─────────────────────────────────────
# flows_df  : data frame with commuting/flow data
# node_ids  : optional vector of valid spatial IDs to check against
# label     : name used in log messages
validate_flows <- function(flows_df, node_ids = NULL, label = "flows") {
  required <- c("origin_id", "destination_id", "flow_value")
  missing  <- setdiff(required, names(flows_df))
  if (length(missing) > 0)
    stop(sprintf("[validate_flows] Missing columns: %s",
                 paste(missing, collapse = ", ")))

  n_na <- sum(is.na(flows_df$flow_value))
  if (n_na > 0)
    message(sprintf("[validate_flows] %s: %d NA flow values", label, n_na))

  n_neg <- sum(flows_df$flow_value < 0, na.rm = TRUE)
  if (n_neg > 0)
    message(sprintf("[validate_flows] %s: %d negative flow values", label, n_neg))

  if (!is.null(node_ids)) {
    bad_o <- sum(!flows_df$origin_id      %in% node_ids)
    bad_d <- sum(!flows_df$destination_id %in% node_ids)
    if (bad_o > 0)
      message(sprintf("[validate_flows] %s: %d unmatched origins",      label, bad_o))
    if (bad_d > 0)
      message(sprintf("[validate_flows] %s: %d unmatched destinations", label, bad_d))
  }

  message(sprintf("[validate_flows] %s: %d rows validated", label, nrow(flows_df)))
  flows_df
}