# =============================================================================
# 05_aggregate_cbs_to_app_scales.R
#
# Aggregates CBS grid (100m, 500m) and buurt data up to PC4 and gemeente
# scales, producing parquets in his app's schema:
#   area_code, year, count, weight
#
# Aggregation rules:
#   COUNT columns  → SUM across cells/buurten within target area
#   PERCENTAGE columns with derivable numerator → recompute from summed counts
#   PERCENTAGE columns without numerator → population-weighted mean (approximate)
#   DISTANCE columns → population-weighted mean (nearest facility)
#   AVERAGE columns (e.g. gemiddelde_huishoudensgrootte) → recompute from counts
#
# Input:  existing parquets from old pipeline (Rotterdam-filtered for now,
#         identical schema to full national data when CBS is available)
# Output: static/data/parquet/cbs-grid100-pc4.parquet  (100m → PC4)
#         static/data/parquet/cbs-grid500-pc4.parquet  (500m → PC4)
#         static/data/parquet/cbs-buurt-gem.parquet    (buurt → gemeente)
#         + manifest-ready variable list printed to console
#
# When CBS is available again: replace SRC paths with full national files,
# re-run — output will be nationwide without any other changes.
# =============================================================================

library(sf)
library(arrow)
library(dplyr)
library(jsonlite)
sf_use_s2(FALSE)

# =============================================================================
# CONFIG
# =============================================================================

SRC_GRID_100M <- "C:/NPRZ_project/app-development/static/data/grid_100m_rijnmond_stats.parquet"
SRC_GRID_500M <- "C:/NPRZ_project/app-development/static/data/grid_500m_rijnmond_stats.parquet"
SRC_BUURT     <- "C:/NPRZ_project/app-development/static/data/buurt_2024_stats.parquet"

GEO_GRID_100M <- "C:/NPRZ_project/static/data/geo/grid_100m_rijnmond.geojson"
GEO_GRID_500M <- "C:/NPRZ_project/static/data/geo/grid_500m_rijnmond.geojson"
GEO_PC4       <- "C:/NPRZ_project/static/data/geo/pc4.geojson"

OUT_DIR <- "C:/NPRZ_project/static/data/parquet"
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# Synthetic year — CBS grid 2024 data has no year column; we assign 2024
GRID_YEAR <- 2024L
BUURT_YEAR <- 2024L

# =============================================================================
# HELPER: safe parquet write
# =============================================================================

safe_write <- function(df, path) {
  tmp <- paste0(path, ".tmp")
  write_parquet(df, tmp)
  if (file.exists(path)) file.remove(path)
  file.rename(tmp, path)
  message("  → ", basename(path),
          " (", nrow(df), " rows, ", ncol(df), " cols, ",
          round(file.size(path) / 1024), " KB)")
}

# =============================================================================
# SECTION 1: Spatial join — grid cells → PC4 areas
#
# Each grid cell gets a pc4 code assigned based on which PC4 polygon its
# centroid falls within. Cells outside all PC4 polygons are dropped.
# =============================================================================

message("\n=== Section 1: Spatial join grid → PC4 ===")

pc4_sf <- st_read(GEO_PC4, quiet = TRUE) |>
  select(area_code) |>
  st_make_valid()

join_grid_to_pc4 <- function(geo_path, stats_path, id_col) {
  message("  Reading ", basename(geo_path), "...")
  grid_sf <- st_read(geo_path, quiet = TRUE) |>
    select(all_of(id_col)) |>
    st_make_valid() |>
    st_transform(28992)  # reproject to RD New for correct spatial ops

  pc4_proj <- st_transform(pc4_sf, 28992)

  grid_cents <- st_centroid(grid_sf)
  joined     <- st_join(grid_cents, pc4_proj, join = st_within, left = FALSE)

  lookup <- st_drop_geometry(joined) |>
    select(grid_id = all_of(id_col), pc4_code = area_code) |>
    filter(!is.na(pc4_code))

  message("  Matched ", nrow(lookup), " / ", nrow(grid_sf), " cells to a PC4")

  stats <- read_parquet(stats_path)
  stats |>
    inner_join(lookup, by = setNames("grid_id", id_col)) |>
    mutate(area_code = as.character(pc4_code)) |>
    select(-pc4_code)
}

grid100_with_pc4 <- join_grid_to_pc4(GEO_GRID_100M, SRC_GRID_100M, "crs28992res100m")
grid500_with_pc4 <- join_grid_to_pc4(GEO_GRID_500M, SRC_GRID_500M, "crs28992res500m")

# =============================================================================
# SECTION 2: Aggregation functions
# =============================================================================

message("\n=== Section 2: Define aggregation ===")

# Columns to SUM directly (counts)
GRID_COUNT_COLS <- c(
  "aantal_inwoners",
  "aantal_mannen",
  "aantal_vrouwen",
  "aantal_inwoners_0_tot_15_jaar",
  "aantal_inwoners_15_tot_25_jaar",
  "aantal_inwoners_25_tot_45_jaar",
  "aantal_inwoners_45_tot_65_jaar",
  "aantal_inwoners_65_jaar_en_ouder",
  "aantal_part_huishoudens",
  "aantal_eenpersoonshuishoudens",
  "aantal_meerpersoonshuishoudens_zonder_kind",
  "aantal_eenouderhuishoudens",
  "aantal_tweeouderhuishoudens",
  "aantal_woningen",
  "aantal_woningen_bouwjaar_voor_1945",
  "aantal_woningen_bouwjaar_45_tot_65",
  "aantal_woningen_bouwjaar_65_tot_75",
  "aantal_woningen_bouwjaar_75_tot_85",
  "aantal_woningen_bouwjaar_85_tot_95",
  "aantal_woningen_bouwjaar_95_tot_05",
  "aantal_woningen_bouwjaar_05_tot_15",
  "aantal_woningen_bouwjaar_15_en_later",
  "aantal_meergezins_woningen",
  "aantal_huurwoningen_in_bezit_woningcorporaties",
  "aantal_niet_bewoonde_woningen",
  "aantal_personen_met_uitkering_onder_aowlft"
)

# Population-weighted mean columns (percentages without derivable numerator,
# and distance columns). Weight = aantal_inwoners.
GRID_WTMEAN_COLS <- c(
  "percentage_geb_nederland_herkomst_nederland",
  "percentage_geb_nederland_herkomst_overig_europa",
  "percentage_geb_nederland_herkomst_buiten_europa",
  "percentage_geb_buiten_nederland_herkomst_europa",
  "percentage_geb_buiten_nederland_herkmst_buiten_europa"
)

# Columns to recompute from aggregated counts (not aggregated directly)
# These are derived in Section 3 after summing counts.
GRID_DERIVED_COLS <- c(
  "percentage_koopwoningen",       # koopwoningen / woningen * 100
  "percentage_huurwoningen",       # huurwoningen / woningen * 100
  "gemiddelde_huishoudensgrootte"  # inwoners / huishoudens
)

# Aggregate grid data to PC4
aggregate_grid_to_pc4 <- function(df) {
  # Only use columns that exist in this dataset
  count_cols  <- intersect(GRID_COUNT_COLS,  names(df))
  wtmean_cols <- intersect(GRID_WTMEAN_COLS, names(df))

  df_clean <- df |>
    mutate(across(all_of(c(count_cols, wtmean_cols)),
                  ~ suppressWarnings(as.numeric(.x)))) |>
    mutate(across(all_of(c(count_cols, wtmean_cols)),
                  ~ ifelse(. < -99990 | is.nan(.), NA_real_, .)))

  # Sum counts
  summed <- df_clean |>
    group_by(area_code) |>
    summarise(
      across(all_of(count_cols), ~ sum(.x, na.rm = TRUE)),
      pop_weight = sum(aantal_inwoners, na.rm = TRUE),
      .groups = "drop"
    )

  # Population-weighted means for percentage columns
  if (length(wtmean_cols) > 0) {
    wtmeans <- df_clean |>
      group_by(area_code) |>
      summarise(
        across(all_of(wtmean_cols), ~ {
          w <- aantal_inwoners
          w[is.na(w)] <- 0
          v <- .x
          valid <- !is.na(v) & !is.na(w) & w > 0
          if (sum(valid) == 0) NA_real_
          else sum(v[valid] * w[valid]) / sum(w[valid])
        }),
        .groups = "drop"
      )
    summed <- left_join(summed, wtmeans, by = "area_code")
  }

  summed
}

# =============================================================================
# SECTION 3: Aggregate and derive computed columns
# =============================================================================

message("\n=== Section 3: Aggregate grid → PC4 ===")

agg100 <- aggregate_grid_to_pc4(grid100_with_pc4)
agg500 <- aggregate_grid_to_pc4(grid500_with_pc4)

derive_cols <- function(df) {
  df |> mutate(
    # Percentages from counts
    percentage_koopwoningen = case_when(
      aantal_woningen > 0 ~
        # Implied koop = woningen - huur_corp - niet_bewoond (approximate)
        # Grid doesn't have total huur count so use: 100 - huurwoningen
        # We recompute from: koop_n not available directly, so use pop-weighted
        # mean carried through if available, else NA
        NA_real_,
      TRUE ~ NA_real_
    ),
    percentage_huurwoningen = case_when(
      antal_woningen > 0 ~
        (aantal_huurwoningen_in_bezit_woningcorporaties / aantal_woningen) * 100,
      TRUE ~ NA_real_
    ),
    gemiddelde_huishoudensgrootte = case_when(
      aantal_part_huishoudens > 0 ~
        aantal_inwoners / aantal_part_huishoudens,
      TRUE ~ NA_real_
    )
  )
}

# Note: percentage_koopwoningen cannot be correctly derived from grid data
# (no total koop count column). We carry it as NA at aggregated scales.
# It IS available correctly at buurt/gemeente from kerncijfers.

derive_cols_safe <- function(df) {
  df |> mutate(
    percentage_huurwoningen = case_when(
      !is.na(aantal_woningen) & aantal_woningen > 0 ~
        (aantal_huurwoningen_in_bezit_woningcorporaties / aantal_woningen) * 100,
      TRUE ~ NA_real_
    ),
    gemiddelde_huishoudensgrootte = case_when(
      !is.na(aantal_part_huishoudens) & aantal_part_huishoudens > 0 ~
        aantal_inwoners / aantal_part_huishoudens,
      TRUE ~ NA_real_
    )
  )
}

agg100_final <- derive_cols_safe(agg100)
agg500_final <- derive_cols_safe(agg500)

# =============================================================================
# SECTION 4: Buurt → gemeente aggregation
# =============================================================================

message("\n=== Section 4: Aggregate buurt → gemeente ===")

buurt_raw <- read_parquet(SRC_BUURT)

BUURT_COUNT_COLS <- c(
  "aantal_inwoners", "mannen", "vrouwen",
  "aantal_huishoudens",
  "woningvoorraad",
  "aantal_bedrijven_landbouw_bosbouw_visserij",
  "aantal_bedrijven_nijverheid_energie",
  "aantal_bedrijven_handel_en_horeca",
  "aantal_bedrijven_vervoer_informatie_communicatie",
  "aantal_bedrijven_financieel_onroerend_goed",
  "aantal_bedrijven_zakelijke_dienstverlening",
  "aantal_bedrijven_overheid_onderwijs_en_zorg",
  "aantal_bedrijven_cultuur_recreatie_overige",
  "aantal_bedrijfsvestigingen",
  "aantal_leerlingen_primair_onderwijs",
  "aantal_leerlingen_voortgezet_onderwijs",
  "aantal_studenten_mbo",
  "aantal_studenten_hbo",
  "aantal_studenten_wo",
  "aantal_personen_met_een_ao_uitkering_totaal",
  "aantal_personen_met_een_ww_uitkering_totaal",
  "aantal_personen_met_een_alg_bijstandsuitkering_tot",
  "aantal_personen_met_een_aow_uitkering_totaal",
  "personenautos_totaal",
  "motortweewielers_totaal",
  "oppervlakte_totaal_in_ha",
  "oppervlakte_land_in_ha",
  "oppervlakte_water_in_ha"
)

BUURT_WTMEAN_COLS <- c(
  "percentage_met_herkomstland_nederland",
  "percentage_met_herkomstland_uit_europa_excl_nl",
  "percentage_met_herkomstland_buiten_europa",
  "percentage_geb_in_nl_met_herkomstland_nederland",
  "perc_geb_in_nl_met_herkomstland_in_europa_ex_nl",
  "perc_geb_in_nl_met_herkomstland_buiten_europa",
  "perc_geb_buiten_nl_met_herkomstlnd_in_europa_ex_nl",
  "perc_geb_buiten_nl_met_herkomstlnd_buiten_europa",
  "gemiddelde_woningwaarde",
  "huisartsenpraktijk_gemiddelde_afstand_in_km",
  "grote_supermarkt_gemiddelde_afstand_in_km",
  "basisonderwijs_gemiddelde_afstand_in_km",
  "treinstation_gemiddelde_afstand_in_km",
  "voortgezet_onderwijs_gem_afstand_in_km",
  "ziekenhuis_incl_buitenpolikliniek_gem_afst_in_km"
)

buurt_clean <- buurt_raw |>
  filter(!is.na(gemeentecode), is.na(water) | water != "WATER") |>
  mutate(across(all_of(intersect(c(BUURT_COUNT_COLS, BUURT_WTMEAN_COLS),
                                  names(buurt_raw))),
                ~ suppressWarnings(as.numeric(.x)))) |>
  mutate(across(all_of(intersect(c(BUURT_COUNT_COLS, BUURT_WTMEAN_COLS),
                                  names(buurt_raw))),
                ~ ifelse(. < -99990 | is.nan(.), NA_real_, .)))

count_cols_b  <- intersect(BUURT_COUNT_COLS,  names(buurt_clean))
wtmean_cols_b <- intersect(BUURT_WTMEAN_COLS, names(buurt_clean))

gem_summed <- buurt_clean |>
  group_by(area_code = gemeentecode) |>
  summarise(
    across(all_of(count_cols_b), ~ sum(.x, na.rm = TRUE)),
    pop_weight = sum(aantal_inwoners, na.rm = TRUE),
    .groups = "drop"
  )

gem_wtmeans <- buurt_clean |>
  group_by(area_code = gemeentecode) |>
  summarise(
    across(all_of(wtmean_cols_b), ~ {
      w <- aantal_inwoners
      w[is.na(w)] <- 0
      v <- .x
      valid <- !is.na(v) & !is.na(w) & w > 0
      if (sum(valid) == 0) NA_real_
      else sum(v[valid] * w[valid]) / sum(w[valid])
    }),
    .groups = "drop"
  )

# Derive percentages from counts
gem_final <- left_join(gem_summed, gem_wtmeans, by = "area_code") |>
  mutate(
    gemiddelde_huishoudsgrootte = case_when(
      !is.na(aantal_huishoudens) & aantal_huishoudens > 0 ~
        aantal_inwoners / aantal_huishoudens,
      TRUE ~ NA_real_
    ),
    percentage_eenpersoonshuishoudens = NA_real_,  # needs count not in buurt
    bevolkingsdichtheid_inwoners_per_km2 = case_when(
      !is.na(oppervlakte_land_in_ha) & oppervlakte_land_in_ha > 0 ~
        aantal_inwoners / (oppervlakte_land_in_ha / 100),
      TRUE ~ NA_real_
    )
  )

# =============================================================================
# SECTION 5: Format for app schema and write
#
# His app expects: area_code, year, count, weight
# For these CBS variables there's no meaningful "count/weight" split —
# each row IS the value. We use a single-variable-per-dataset approach:
# one parquet per variable group, with area_code, year, [variable columns].
# The manifest will reference these with countCol = the variable column name.
#
# Alternatively: produce wide parquets and query with SELECT area_code, <col>
# using his existing query engine with a custom countCol per dataset entry.
# This is cleaner — one parquet per scale, manifest has multiple dataset entries
# each pointing to the same parquet but different countCol.
# =============================================================================

message("\n=== Section 5: Write output parquets ===")

# Add year column (required by his query engine)
add_year <- function(df, year) {
  df |> mutate(year = as.integer(year)) |>
    select(area_code, year, everything(), -matches("pop_weight"))
}

agg100_out <- add_year(agg100_final, GRID_YEAR)
agg500_out <- add_year(agg500_final, GRID_YEAR)
gem_out    <- add_year(gem_final,    BUURT_YEAR)

safe_write(agg100_out, file.path(OUT_DIR, "cbs-grid100-pc4.parquet"))
safe_write(agg500_out, file.path(OUT_DIR, "cbs-grid500-pc4.parquet"))
safe_write(gem_out,    file.path(OUT_DIR, "cbs-buurt-gem.parquet"))

# =============================================================================
# SECTION 6: Print manifest variable list
# =============================================================================

message("\n=== Section 6: Manifest entries needed ===")
message("Add these dataset entries to manifest.json:\n")

manifest_vars <- list(
  list(key = "cbs-grid100-pc4",  scale = "pc4",
       name = "CBS 100m grid (geaggregeerd naar PC4)",
       cols = names(agg100_out)),
  list(key = "cbs-grid500-pc4",  scale = "pc4",
       name = "CBS 500m grid (geaggregeerd naar PC4)",
       cols = names(agg500_out)),
  list(key = "cbs-buurt-gem",    scale = "gem",
       name = "CBS Kerncijfers buurt (geaggregeerd naar gemeente)",
       cols = names(gem_out))
)

for (m in manifest_vars) {
  cat("\nDataset:", m$key, "| Scale:", m$scale, "\n")
  cat("Available countCol options (one manifest entry per variable):\n")
  data_cols <- setdiff(m$cols, c("area_code", "year", "pop_weight"))
  cat(paste(" -", data_cols, collapse = "\n"), "\n")
}

message("\n=== AGGREGATION COMPLETE ===")
message("Next: add manifest.json entries for each variable group,")
message("      then test in app by selecting the new datasets.")