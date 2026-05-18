# R/nodes/cbs-census.R
#
# Aggregates CBS open data to PC4, gemeente, and buurt scales.
#
# Produces:
#   static/data/parquet/cbs-census-pc4.parquet
#     Source:  CBS 100m grid → PC4
#     Years:   2024 only
#     Note:    Multi-year deferred — spatial join ~30 min/year (work item)
#     Note:    Origin % unavailable — CBS suppresses at cell level
#
#   static/data/parquet/cbs-census-gem.parquet
#     Source:  CBS KWB buurt → gemeente (aggregated)
#     Years:   2015, 2017, 2022, 2023, 2024, 2025
#     Note:    Origin % available 2024/2025 only (geboorteland/herkomst schema)
#              2015-2023 use Westers/NietWesters — incompatible classification
#     Note:    Origin % aggregation bug open (work item) — shows NA at gem level
#
#   static/data/parquet/cbs-census-gem-grid.parquet
#     Source:  CBS 100m grid → gemeente
#     Years:   2024 only
#     Note:    Adds household composition/gender/age not in KWB at gem scale
#     Note:    Multi-year deferred — spatial join ~30 min/year (work item)
#     Note:    Origin % unavailable — CBS suppresses at cell level
#
#   static/data/parquet/cbs-census-buurt.parquet
#     Source:  CBS KWB buurt (native resolution)
#     Years:   2015, 2017, 2022, 2023, 2024, 2025
#     Note:    Origin % available 2024/2025 only
#     Note:    Distance variables: huisarts/supermarkt/school 2017+,
#              treinstation/ziekenhuis/VO 2015/2017 only (dropped from KWB 2022+)
#
# Data sources (auto-downloaded to raw-data/cbs/ on first run):
#   CBS 100m grid 2024  — download.cbs.nl/vierkant/100/
#   CBS KWB per year    — CBS OData API (table IDs in .KWB_YEARS below)
#
# To run standalone:
#   setwd("/path/to/project")
#   source("R/nodes/cbs-census.R")
#   build_cbs_census_pc4()
#   build_cbs_census_gem()
#   build_cbs_census_gem_grid()
#   build_cbs_census_buurt()
# =============================================================================

suppressPackageStartupMessages({
  library(sf)
  library(arrow)
  library(dplyr)
  library(cbsodataR)
})
sf_use_s2(FALSE)

# ── Constants ─────────────────────────────────────────────────────────────────

# Only 2024 for grid — multi-year spatial join deferred (see header notes).
.GRID_YEARS <- list(
  `2024` = list(
    url            = "https://download.cbs.nl/vierkant/100/2025-cbs_vk100_2024_v1.zip",
    zip            = "cbs_vk100_2024.zip",
    has_origin_pct = TRUE,
    has_proximity  = TRUE
  )
)

# Separate CBS OData table ID per year.
.KWB_YEARS <- list(
  `2015` = "83220NED",
  `2017` = "83765NED",
  `2022` = "85318NED",
  `2023` = "85618NED",
  `2024` = "85984NED",
  `2025` = "86165NED"
)

# ── Helpers ───────────────────────────────────────────────────────────────────

# Replace CBS sentinel values (< -99990) with NA.
.clean_sentinel <- function(x) {
  ifelse(is.numeric(x) & x < -99990, NA_real_, x)
}

# Population-weighted mean; NA if no valid weight/value pairs.
.wtmean <- function(values, weights) {
  w  <- ifelse(is.na(weights) | weights <= 0, 0, weights)
  ok <- !is.na(values) & w > 0
  if (!any(ok)) return(NA_real_)
  sum(values[ok] * w[ok]) / sum(w[ok])
}

# Null-coalescing: returns `a` unless all values are NA, then `b`.
# Used to select the correct KWB column across years with shifting suffixes.
`%||%` <- function(a, b) {
  if (length(a) == 0 || all(is.na(a))) b else a
}

.cbs_raw_dir <- function() {
  d <- file.path("raw-data", "cbs")
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

# ── Grid download / cache ─────────────────────────────────────────────────────

.get_grid_gpkg <- function(year_str) {
  cfg  <- .GRID_YEARS[[year_str]]
  zip  <- file.path(.cbs_raw_dir(), cfg$zip)
  extr <- file.path(.cbs_raw_dir(), paste0("grid_100m_", year_str))

  if (!file.exists(zip)) {
    cat(sprintf("Downloading CBS 100m grid %s (large, may take minutes)...\n", year_str))
    download.file(cfg$url, destfile = zip, mode = "wb")
    cat("  Done:", round(file.size(zip) / 1e6), "MB\n")
  }
  if (!dir.exists(extr)) {
    cat(sprintf("Extracting %s grid...\n", year_str))
    unzip(zip, exdir = extr)
  }

  # GeoPackage (2017+) or Shapefile (2015 uses older format).
  gpkg <- list.files(extr, pattern = "\\.gpkg$", full.names = TRUE, recursive = TRUE)[1]
  if (is.na(gpkg)) {
    gpkg <- list.files(extr, pattern = "\\.shp$", full.names = TRUE, recursive = TRUE)[1]
  }
  if (is.na(gpkg)) stop("No .gpkg or .shp found after extracting ", zip)
  gpkg
}

# ── KWB download / cache ──────────────────────────────────────────────────────

.get_kwb <- function(year_str) {
  table_id <- .KWB_YEARS[[year_str]]
  path     <- file.path(.cbs_raw_dir(), paste0("kwb_", year_str, ".rds"))
  if (!file.exists(path)) {
    cat(sprintf("Downloading KWB %s (table %s)...\n", year_str, table_id))
    kwb <- cbsodataR::cbs_get_data(table_id)
    saveRDS(kwb, path)
    cat("  Saved:", nrow(kwb), "rows\n")
  }
  readRDS(path)
}

# ── Spatial lookups ───────────────────────────────────────────────────────────

.build_pc4_lookup <- function(grid_sf) {
  pc4_sf <- sf::st_read("static/data/geo/pc4.geojson", quiet = TRUE) |>
    dplyr::select(area_code) |>
    sf::st_transform(28992) |>
    sf::st_make_valid()

  grid_cents <- sf::st_centroid(grid_sf)
  joined     <- sf::st_join(grid_cents, pc4_sf, join = sf::st_within, left = FALSE)
  sf::st_drop_geometry(joined) |>
    dplyr::select(grid_id = crs28992res100m, area_code) |>
    dplyr::filter(!is.na(area_code))
}

.build_gem_lookup <- function(grid_sf) {
  gem_sf <- sf::st_read("static/data/geo/gemeenten.geojson", quiet = TRUE) |>
    dplyr::select(area_code) |>
    sf::st_transform(28992) |>
    sf::st_make_valid()

  grid_cents <- sf::st_centroid(grid_sf)
  joined     <- sf::st_join(grid_cents, gem_sf, join = sf::st_within, left = FALSE)
  sf::st_drop_geometry(joined) |>
    dplyr::select(grid_id = crs28992res100m, area_code) |>
    dplyr::filter(!is.na(area_code))
}

# ── Grid aggregation ──────────────────────────────────────────────────────────

# Aggregate one grid year to a target scale.
# `lookup` has columns: grid_id, area_code.
# Returns a data frame with area_code, year, and aggregated columns.
.aggregate_grid_year <- function(year_str, lookup) {
  cfg     <- .GRID_YEARS[[year_str]]
  gpkg    <- .get_grid_gpkg(year_str)
  cat(sprintf("  Reading %s grid...\n", year_str))
  grid_sf <- sf::st_read(gpkg, quiet = TRUE) |> sf::st_make_valid()

  # 2015 shapefile uses short uppercase column names; 2017+ uses long Dutch names.
  is_old_schema <- "C28992R100" %in% names(grid_sf)
  id_col        <- if (is_old_schema) "C28992R100" else "crs28992res100m"

  stats <- sf::st_drop_geometry(grid_sf) |>
    dplyr::rename(grid_id = !!id_col) |>
    dplyr::inner_join(lookup, by = "grid_id") |>
    dplyr::mutate(dplyr::across(where(is.numeric), .clean_sentinel))

  if (is_old_schema) {
    stats <- stats |> dplyr::filter(!is.na(INWONER) & INWONER > 0)

    out <- stats |>
      dplyr::group_by(area_code) |>
      dplyr::summarise(
        aantal_inwoners                                = sum(INWONER,    na.rm = TRUE),
        aantal_mannen                                  = sum(MAN,        na.rm = TRUE),
        aantal_vrouwen                                 = sum(VROUW,      na.rm = TRUE),
        aantal_inwoners_0_tot_15_jaar                  = sum(INW_014,    na.rm = TRUE),
        aantal_inwoners_15_tot_25_jaar                 = sum(INW_1524,   na.rm = TRUE),
        aantal_inwoners_25_tot_45_jaar                 = sum(INW_2544,   na.rm = TRUE),
        aantal_inwoners_45_tot_65_jaar                 = sum(INW_4564,   na.rm = TRUE),
        aantal_inwoners_65_jaar_en_ouder               = sum(INW_65PL,   na.rm = TRUE),
        aantal_part_huishoudens                        = sum(AANTAL_HH,  na.rm = TRUE),
        aantal_eenpersoonshuishoudens                  = sum(TOTHH_EENP, na.rm = TRUE),
        aantal_meerpersoonshuishoudens_zonder_kind     = sum(TOTHH_MPZK, na.rm = TRUE),
        aantal_eenouderhuishoudens                     = sum(HH_EENOUD,  na.rm = TRUE),
        aantal_tweeouderhuishoudens                    = sum(HH_TWEEOUD, na.rm = TRUE),
        aantal_woningen                                = sum(WONING,     na.rm = TRUE),
        aantal_huurwoningen_in_bezit_woningcorporaties = sum(WON_HCORP,  na.rm = TRUE),
        aantal_niet_bewoonde_woningen                  = sum(WON_NBEW,   na.rm = TRUE),
        aantal_personen_met_uitkering_onder_aowlft     = sum(UITKMINAOW, na.rm = TRUE),
        # Origin %: not published in 2015 schema.
        percentage_geb_nederland_herkomst_nederland           = NA_real_,
        percentage_geb_nederland_herkomst_overig_europa       = NA_real_,
        percentage_geb_nederland_herkomst_buiten_europa       = NA_real_,
        percentage_geb_buiten_nederland_herkomst_europa       = NA_real_,
        percentage_geb_buiten_nederland_herkmst_buiten_europa = NA_real_,
        .groups = "drop"
      )

  } else {
    stats <- stats |>
      dplyr::filter(!is.na(aantal_inwoners) & aantal_inwoners > 0) |>
      dplyr::mutate(dplyr::across(
        dplyr::starts_with("percentage_geb_"),
        ~ ifelse(. == 0, NA_real_, .)
      ))

    out <- stats |>
      dplyr::group_by(area_code) |>
      dplyr::summarise(
        aantal_inwoners =
          sum(aantal_inwoners, na.rm = TRUE),
        aantal_mannen =
          sum(aantal_mannen, na.rm = TRUE),
        aantal_vrouwen =
          sum(aantal_vrouwen, na.rm = TRUE),
        aantal_inwoners_0_tot_15_jaar =
          sum(aantal_inwoners_0_tot_15_jaar, na.rm = TRUE),
        aantal_inwoners_15_tot_25_jaar =
          sum(aantal_inwoners_15_tot_25_jaar, na.rm = TRUE),
        aantal_inwoners_25_tot_45_jaar =
          sum(aantal_inwoners_25_tot_45_jaar, na.rm = TRUE),
        aantal_inwoners_45_tot_65_jaar =
          sum(aantal_inwoners_45_tot_65_jaar, na.rm = TRUE),
        aantal_inwoners_65_jaar_en_ouder =
          sum(aantal_inwoners_65_jaar_en_ouder, na.rm = TRUE),
        aantal_part_huishoudens =
          sum(aantal_part_huishoudens, na.rm = TRUE),
        aantal_eenpersoonshuishoudens =
          sum(aantal_eenpersoonshuishoudens, na.rm = TRUE),
        aantal_meerpersoonshuishoudens_zonder_kind =
          sum(aantal_meerpersoonshuishoudens_zonder_kind, na.rm = TRUE),
        aantal_eenouderhuishoudens =
          sum(aantal_eenouderhuishoudens, na.rm = TRUE),
        aantal_tweeouderhuishoudens =
          sum(aantal_tweeouderhuishoudens, na.rm = TRUE),
        aantal_woningen =
          sum(aantal_woningen, na.rm = TRUE),
        aantal_huurwoningen_in_bezit_woningcorporaties =
          sum(aantal_huurwoningen_in_bezit_woningcorporaties, na.rm = TRUE),
        aantal_niet_bewoonde_woningen =
          sum(aantal_niet_bewoonde_woningen, na.rm = TRUE),
        aantal_personen_met_uitkering_onder_aowlft =
          sum(aantal_personen_met_uitkering_onder_aowlft, na.rm = TRUE),
        percentage_geb_nederland_herkomst_nederland =
          if (cfg$has_origin_pct)
            .wtmean(percentage_geb_nederland_herkomst_nederland, aantal_inwoners)
          else NA_real_,
        percentage_geb_nederland_herkomst_overig_europa =
          if (cfg$has_origin_pct)
            .wtmean(percentage_geb_nederland_herkomst_overig_europa, aantal_inwoners)
          else NA_real_,
        percentage_geb_nederland_herkomst_buiten_europa =
          if (cfg$has_origin_pct)
            .wtmean(percentage_geb_nederland_herkomst_buiten_europa, aantal_inwoners)
          else NA_real_,
        percentage_geb_buiten_nederland_herkomst_europa =
          if (cfg$has_origin_pct)
            .wtmean(percentage_geb_buiten_nederland_herkomst_europa, aantal_inwoners)
          else NA_real_,
        percentage_geb_buiten_nederland_herkmst_buiten_europa =
          if (cfg$has_origin_pct)
            .wtmean(percentage_geb_buiten_nederland_herkmst_buiten_europa, aantal_inwoners)
          else NA_real_,
        .groups = "drop"
      )
  }

  out |>
    dplyr::mutate(
      year      = as.integer(year_str),
      area_code = as.character(area_code),
      gemiddelde_huishoudensgrootte = dplyr::case_when(
        !is.na(aantal_part_huishoudens) & aantal_part_huishoudens > 0 ~
          aantal_inwoners / aantal_part_huishoudens,
        TRUE ~ NA_real_
      ),
      percentage_huurwoningen = dplyr::case_when(
        !is.na(aantal_woningen) & aantal_woningen > 0 ~
          (aantal_huurwoningen_in_bezit_woningcorporaties / aantal_woningen) * 100,
        TRUE ~ NA_real_
      )
    ) |>
    dplyr::select(area_code, year, dplyr::everything())
}

# ── PC4 build ─────────────────────────────────────────────────────────────────

build_cbs_census_pc4 <- function() {
  cat("\n=== CBS census -> PC4 ===\n")

  cat("Building PC4 spatial lookup from 2024 grid geometry...\n")
  gpkg_ref <- .get_grid_gpkg("2024")
  grid_ref <- sf::st_read(gpkg_ref, quiet = TRUE) |>
    sf::st_make_valid() |>
    dplyr::select(crs28992res100m)
  lookup <- .build_pc4_lookup(grid_ref)
  cat(" ", nrow(lookup), "grid cells matched to PC4\n")
  rm(grid_ref)

  all_years <- dplyr::bind_rows(lapply(names(.GRID_YEARS), function(yr) {
    cat(sprintf("\nProcessing year %s...\n", yr))
    .aggregate_grid_year(yr, lookup)
  }))

  all_years <- all_years |>
    dplyr::mutate(area_code = sprintf("%04d", as.integer(area_code)))

  out_path <- "static/data/parquet/cbs-census-pc4.parquet"
  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(all_years, out_path, compression = "zstd", compression_level = 3)
  cat("\nwrote", out_path, "(", file.size(out_path) %/% 1024, "KB,",
      nrow(all_years), "rows,", length(.GRID_YEARS), "years )\n")

  .cbs_pc4_manifest()
}

# ── Gemeente grid build ───────────────────────────────────────────────────────

build_cbs_census_gem_grid <- function() {
  cat("\n=== CBS census grid -> gemeente ===\n")

  cat("Building gemeente spatial lookup from 2024 grid geometry...\n")
  gpkg_ref <- .get_grid_gpkg("2024")
  grid_ref <- sf::st_read(gpkg_ref, quiet = TRUE) |>
    sf::st_make_valid() |>
    dplyr::select(crs28992res100m)
  lookup <- .build_gem_lookup(grid_ref)
  cat(" ", nrow(lookup), "grid cells matched to gemeente\n")
  rm(grid_ref)

  all_years <- dplyr::bind_rows(lapply(names(.GRID_YEARS), function(yr) {
    cat(sprintf("\nProcessing year %s...\n", yr))
    .aggregate_grid_year(yr, lookup)
  }))

  all_years <- all_years |>
    dplyr::mutate(
      area_code = dplyr::case_when(
        startsWith(area_code, "GM") ~ area_code,
        TRUE ~ paste0("GM", sprintf("%04d", as.integer(area_code)))
      )
    )

  out_path <- "static/data/parquet/cbs-census-gem-grid.parquet"
  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(all_years, out_path, compression = "zstd", compression_level = 3)
  cat("\nwrote", out_path, "(", file.size(out_path) %/% 1024, "KB,",
      nrow(all_years), "rows,", length(.GRID_YEARS), "years )\n")

  invisible(all_years)
}

# ── KWB column selection ──────────────────────────────────────────────────────

# Selects and renames KWB columns to canonical names for all years.
# Handles shifting column suffix numbers across CBS table versions using %||%.
# Returns a data frame with one row per region (buurt, wijk, or gemeente).
.kwb_select_buurt <- function(kwb_raw, year_int) {

  g <- function(col) {
    if (col %in% names(kwb_raw)) kwb_raw[[col]] else rep(NA_real_, nrow(kwb_raw))
  }

  df <- data.frame(
    kwb_code        = trimws(kwb_raw$WijkenEnBuurten),
    soort_regio     = trimws(kwb_raw$SoortRegio_2),
    aantal_inwoners = g("AantalInwoners_5"),
    stringsAsFactors = FALSE
  )

  # ── Social security ────────────────────────────────────────────────────────
  # 2024/2025: _87/_88/_89/_90
  # 2022/2023: _83/_84/_85/_86
  # 2015/2017: _74/_75/_76/_77
  df$bijstand <- g("PersonenPerSoortUitkeringBijstand_87") %||%
                 g("PersonenPerSoortUitkeringBijstand_83") %||%
                 g("PersonenPerSoortUitkeringBijstand_74")
  df$ao       <- g("PersonenPerSoortUitkeringAO_88")  %||%
                 g("PersonenPerSoortUitkeringAO_84")  %||%
                 g("PersonenPerSoortUitkeringAO_75")
  df$ww       <- g("PersonenPerSoortUitkeringWW_89")  %||%
                 g("PersonenPerSoortUitkeringWW_85")  %||%
                 g("PersonenPerSoortUitkeringWW_76")
  df$aow      <- g("PersonenPerSoortUitkeringAOW_90") %||%
                 g("PersonenPerSoortUitkeringAOW_86") %||%
                 g("PersonenPerSoortUitkeringAOW_77")

  # ── Education ──────────────────────────────────────────────────────────────
  # MBO/HBO/WO student columns absent in 2015/2017 -> NA.
  df$leerl_po <- g("LeerlingenPo_62")             %||% g("LeerlingenPo_61")
  df$leerl_vo <- g("LeerlingenVoInclVavo_63")     %||% g("LeerlingenVo_62")
  df$stud_mbo <- g("StudentenMboExclExtranei_64") %||% g("StudentenMbo_63")
  df$stud_hbo <- g("StudentenHbo_65")             %||% g("StudentenHbo_64")
  df$stud_wo  <- g("StudentenWo_66")              %||% g("StudentenWo_65")

  # ── Housing & cars ─────────────────────────────────────────────────────────
  # 2024/2025: Personenauto_104, OppervlakteLand_116, WOZ_39
  # 2022/2023: Personenauto_100, OppervlakteLand_112, WOZ_35
  # 2017:      Personenauto_86,  OppervlakteLand_100, WOZ_35
  # 2015:      Personenauto_86,  OppervlakteLand_101, WOZ absent
  df$personenautos <- g("PersonenautoSTotaal_104") %||%
                      g("PersonenautoSTotaal_100") %||%
                      g("PersonenautoSTotaal_86")
  df$opp_land      <- g("OppervlakteLand_116") %||%
                      g("OppervlakteLand_112") %||%
                      g("OppervlakteLand_100") %||%
                      g("OppervlakteLand_101")
  df$woz_waarde    <- g("GemiddeldeWOZWaardeVanWoningen_39") %||%
                      g("GemiddeldeWOZWaardeVanWoningen_35")

  # ── Origin percentages ─────────────────────────────────────────────────────
  # 2024/2025: geboorteland/herkomst counts -> convert to % via aantal_inwoners.
  # 2015-2023: WestersTotaal/NietWesters classification — incompatible, -> NA.
  n <- g("AantalInwoners_5")
  if (year_int >= 2024) {
    safe_pct <- function(x) ifelse(!is.na(n) & n > 0, (x / n) * 100, NA_real_)
    df$pct_herkomst_nl        <- safe_pct(g("Nederland_17"))
    df$pct_herkomst_eu_ex_nl  <- safe_pct(g("EuropaExclusiefNederland_18"))
    df$pct_herkomst_buiten_eu <- safe_pct(g("BuitenEuropa_19"))
    df$pct_geb_nl_eu_ex_nl    <- safe_pct(g("EuropaExclusiefNederland_21"))
    df$pct_geb_nl_buiten_eu   <- safe_pct(g("BuitenEuropa_22"))
    df$pct_geb_buit_eu_ex_nl  <- safe_pct(g("EuropaExclusiefNederland_23"))
    df$pct_geb_buit_buiten_eu <- safe_pct(g("BuitenEuropa_24"))
  } else {
    df$pct_herkomst_nl        <- NA_real_
    df$pct_herkomst_eu_ex_nl  <- NA_real_
    df$pct_herkomst_buiten_eu <- NA_real_
    df$pct_geb_nl_eu_ex_nl    <- NA_real_
    df$pct_geb_nl_buiten_eu   <- NA_real_
    df$pct_geb_buit_eu_ex_nl  <- NA_real_
    df$pct_geb_buit_buiten_eu <- NA_real_
  }

  # ── Urbanicity ─────────────────────────────────────────────────────────────
  # MateVanStedelijkheid: 1 = zeer sterk stedelijk, 5 = niet stedelijk.
  # 2024/2025: _120/_121  |  2023: _125/_126  |  2022: _116/_117
  # 2017:      _104/_105  |  2015: _105/_106
  df$stedelijkheid  <- g("MateVanStedelijkheid_120") %||%
                       g("MateVanStedelijkheid_125") %||%
                       g("MateVanStedelijkheid_116") %||%
                       g("MateVanStedelijkheid_104") %||%
                       g("MateVanStedelijkheid_105")
  df$opp_addr_dicht <- g("Omgevingsadressendichtheid_121") %||%
                       g("Omgevingsadressendichtheid_126") %||%
                       g("Omgevingsadressendichtheid_117") %||%
                       g("Omgevingsadressendichtheid_105") %||%
                       g("Omgevingsadressendichtheid_106")

  # ── Proximity ──────────────────────────────────────────────────────────────
  # huisarts/supermarkt/school: available 2017+ (absent in 2015 KWB format).
  # treinstation/ziekenhuis/VO: 2015/2017 only (dropped from KWB from 2022).
  # 2024/2025: _110/_111/_113  |  2023: _115/_116/_118  |  2022: _106/_107/_109
  df$afst_huisarts       <- g("AfstandTotHuisartsenpraktijk_110") %||%
                            g("AfstandTotHuisartsenpraktijk_115") %||%
                            g("AfstandTotHuisartsenpraktijk_106") %||%
                            g("AfstandTotHuisartsenpraktijk_94")
  df$afst_supermarkt     <- g("AfstandTotGroteSupermarkt_111")    %||%
                            g("AfstandTotGroteSupermarkt_116")    %||%
                            g("AfstandTotGroteSupermarkt_107")    %||%
                            g("AfstandTotGroteSupermarkt_95")
  df$afst_treinstation   <- g("AfstandTotTreinstation_99")
  df$afst_basisonderwijs <- g("AfstandTotSchool_113")              %||%
                            g("AfstandTotSchool_118")              %||%
                            g("AfstandTotSchool_109")              %||%
                            g("AfstandTotSchoolBasisonderwijs_42") %||%
                            g("AfstandTotSchool_97")
  df$afst_ziekenhuis     <- g("AfstandTotZiekenhuis_46")
  df$afst_vo             <- g("AfstandTotSchoolVoortgezetOnderwijs_44") %||%
                            g("AfstandTotSchool_97")

  df
}

# ── Gemeente KWB build ────────────────────────────────────────────────────────

build_cbs_census_gem <- function() {
  cat("\n=== CBS KWB census -> gemeente (all years) ===\n")

  all_years <- dplyr::bind_rows(lapply(names(.KWB_YEARS), function(yr) {
    cat(sprintf("\nProcessing KWB year %s...\n", yr))
    year_int <- as.integer(yr)
    kwb_raw  <- .get_kwb(yr)

    buurt_df <- .kwb_select_buurt(kwb_raw, year_int) |>
      dplyr::filter(soort_regio == "Buurt") |>
      dplyr::mutate(
        gemeentecode = paste0("GM", substr(kwb_code, 3, 6)),
        dplyr::across(where(is.numeric), .clean_sentinel)
      )

    buurt_df |>
      dplyr::group_by(area_code = gemeentecode) |>
      dplyr::summarise(
        aantal_inwoners =
          sum(aantal_inwoners, na.rm = TRUE),
        aantal_personen_met_een_alg_bijstandsuitkering_tot =
          sum(bijstand, na.rm = TRUE),
        aantal_personen_met_een_ao_uitkering_totaal =
          sum(ao, na.rm = TRUE),
        aantal_personen_met_een_ww_uitkering_totaal =
          sum(ww, na.rm = TRUE),
        aantal_personen_met_een_aow_uitkering_totaal =
          sum(aow, na.rm = TRUE),
        aantal_leerlingen_primair_onderwijs =
          sum(leerl_po, na.rm = TRUE),
        aantal_leerlingen_voortgezet_onderwijs =
          sum(leerl_vo, na.rm = TRUE),
        aantal_studenten_mbo =
          sum(stud_mbo, na.rm = TRUE),
        aantal_studenten_hbo =
          sum(stud_hbo, na.rm = TRUE),
        aantal_studenten_wo =
          sum(stud_wo, na.rm = TRUE),
        personenautos_totaal =
          sum(personenautos, na.rm = TRUE),
        oppervlakte_land_in_ha =
          sum(opp_land, na.rm = TRUE),
        stedelijkheid_gem =
          .wtmean(stedelijkheid, aantal_inwoners),
        omgevingsadressendichtheid =
          .wtmean(opp_addr_dicht, aantal_inwoners),
        gemiddelde_woningwaarde =
          .wtmean(woz_waarde, aantal_inwoners),
        percentage_met_herkomstland_nederland =
          .wtmean(pct_herkomst_nl, aantal_inwoners),
        percentage_met_herkomstland_uit_europa_excl_nl =
          .wtmean(pct_herkomst_eu_ex_nl, aantal_inwoners),
        percentage_met_herkomstland_buiten_europa =
          .wtmean(pct_herkomst_buiten_eu, aantal_inwoners),
        perc_geb_in_nl_met_herkomstland_in_europa_ex_nl =
          .wtmean(pct_geb_nl_eu_ex_nl, aantal_inwoners),
        perc_geb_in_nl_met_herkomstland_buiten_europa =
          .wtmean(pct_geb_nl_buiten_eu, aantal_inwoners),
        perc_geb_buiten_nl_met_herkomstlnd_in_europa_ex_nl =
          .wtmean(pct_geb_buit_eu_ex_nl, aantal_inwoners),
        perc_geb_buiten_nl_met_herkomstlnd_buiten_europa =
          .wtmean(pct_geb_buit_buiten_eu, aantal_inwoners),
        .groups = "drop"
      ) |>
      dplyr::mutate(year = year_int)
  }))

  all_years <- all_years |>
    dplyr::select(area_code, year, dplyr::everything())

  out_path <- "static/data/parquet/cbs-census-gem.parquet"
  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(all_years, out_path, compression = "zstd", compression_level = 3)
  cat("\nwrote", out_path, "(", file.size(out_path) %/% 1024, "KB,",
      nrow(all_years), "rows,", length(.KWB_YEARS), "years )\n")

  .cbs_gem_manifest()
}

# ── Buurt build ───────────────────────────────────────────────────────────────

build_cbs_census_buurt <- function() {
  cat("\n=== CBS KWB census -> buurt (all years) ===\n")

  all_years <- dplyr::bind_rows(lapply(names(.KWB_YEARS), function(yr) {
    cat(sprintf("\nProcessing KWB buurt year %s...\n", yr))
    year_int <- as.integer(yr)
    kwb_raw  <- .get_kwb(yr)

    .kwb_select_buurt(kwb_raw, year_int) |>
      dplyr::filter(soort_regio == "Buurt") |>
      dplyr::mutate(
        area_code = kwb_code,
        year      = year_int,
        dplyr::across(where(is.numeric), .clean_sentinel)
      ) |>
      dplyr::select(
        area_code, year,
        aantal_inwoners,
        bijstand, ao, ww, aow,
        leerl_po, leerl_vo, stud_mbo, stud_hbo, stud_wo,
        personenautos, opp_land, woz_waarde,
        stedelijkheid, opp_addr_dicht,
        pct_herkomst_nl, pct_herkomst_eu_ex_nl, pct_herkomst_buiten_eu,
        pct_geb_nl_eu_ex_nl, pct_geb_nl_buiten_eu,
        pct_geb_buit_eu_ex_nl, pct_geb_buit_buiten_eu,
        afst_huisarts, afst_supermarkt, afst_treinstation,
        afst_basisonderwijs, afst_ziekenhuis, afst_vo
      ) |>
      dplyr::rename(
        aantal_personen_met_een_alg_bijstandsuitkering_tot = bijstand,
        aantal_personen_met_een_ao_uitkering_totaal        = ao,
        aantal_personen_met_een_ww_uitkering_totaal        = ww,
        aantal_personen_met_een_aow_uitkering_totaal       = aow,
        aantal_leerlingen_primair_onderwijs                = leerl_po,
        aantal_leerlingen_voortgezet_onderwijs             = leerl_vo,
        aantal_studenten_mbo                               = stud_mbo,
        aantal_studenten_hbo                               = stud_hbo,
        aantal_studenten_wo                                = stud_wo,
        personenautos_totaal                               = personenautos,
        oppervlakte_land_in_ha                             = opp_land,
        gemiddelde_woningwaarde                            = woz_waarde,
        stedelijkheid_buurt                                = stedelijkheid,
        omgevingsadressendichtheid                         = opp_addr_dicht,
        percentage_met_herkomstland_nederland              = pct_herkomst_nl,
        percentage_met_herkomstland_uit_europa_excl_nl     = pct_herkomst_eu_ex_nl,
        percentage_met_herkomstland_buiten_europa          = pct_herkomst_buiten_eu,
        perc_geb_in_nl_met_herkomstland_in_europa_ex_nl    = pct_geb_nl_eu_ex_nl,
        perc_geb_in_nl_met_herkomstland_buiten_europa      = pct_geb_nl_buiten_eu,
        perc_geb_buiten_nl_met_herkomstlnd_in_europa_ex_nl = pct_geb_buit_eu_ex_nl,
        perc_geb_buiten_nl_met_herkomstlnd_buiten_europa   = pct_geb_buit_buiten_eu,
        afstand_huisartsenpraktijk                         = afst_huisarts,
        afstand_grote_supermarkt                           = afst_supermarkt,
        afstand_treinstation                               = afst_treinstation,
        afstand_basisonderwijs                             = afst_basisonderwijs,
        afstand_ziekenhuis                                 = afst_ziekenhuis,
        afstand_voortgezet_onderwijs                       = afst_vo
      )
  }))

  out_path <- "static/data/parquet/cbs-census-buurt.parquet"
  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(all_years, out_path, compression = "zstd", compression_level = 3)
  cat("\nwrote", out_path, "(", file.size(out_path) %/% 1024, "KB,",
      nrow(all_years), "rows,", length(.KWB_YEARS), "years )\n")

  invisible(all_years)
}

# ── Manifest helpers ──────────────────────────────────────────────────────────

.v <- function(id, label) list(id = id, label = label)

.grid_year_values <- function() {
  lapply(as.integer(names(.GRID_YEARS)), function(y) list(id = y, label = as.character(y)))
}

.kwb_year_values <- function() {
  lapply(as.integer(names(.KWB_YEARS)), function(y) list(id = y, label = as.character(y)))
}

# ── PC4 manifest ──────────────────────────────────────────────────────────────

.cbs_pc4_manifest <- function() {
  list(
    name        = "CBS Vierkantstatistieken (PC4)",
    description = paste(
      "CBS 100m grid geaggregeerd naar PC4. Jaar: 2024.",
      "Huishoudsamenstelling, geslacht, leeftijd, woningen, uitkeringen.",
      "Herkomst-% niet beschikbaar: CBS-geheimhouding op gridniveau."
    ),
    scales       = list(pc4 = "parquet/cbs-census-pc4.parquet"),
    needsGroupBy = FALSE,
    fields       = list(
      year = list(
        type   = "single", label = "Jaar",
        values = .grid_year_values(), default = 2024L
      ),
      variable = list(
        type   = "single", label = "Variabele",
        values = list(
          .v("aantal_inwoners",                                    "Inwoners totaal"),
          .v("aantal_mannen",                                      "Mannen"),
          .v("aantal_vrouwen",                                     "Vrouwen"),
          .v("aantal_inwoners_0_tot_15_jaar",                      "Inwoners 0-15 jaar"),
          .v("aantal_inwoners_15_tot_25_jaar",                     "Inwoners 15-25 jaar"),
          .v("aantal_inwoners_25_tot_45_jaar",                     "Inwoners 25-45 jaar"),
          .v("aantal_inwoners_45_tot_65_jaar",                     "Inwoners 45-65 jaar"),
          .v("aantal_inwoners_65_jaar_en_ouder",                   "Inwoners 65+"),
          .v("aantal_part_huishoudens",                            "Particuliere huishoudens"),
          .v("aantal_eenpersoonshuishoudens",                      "Eenpersoonshuishoudens"),
          .v("aantal_meerpersoonshuishoudens_zonder_kind",         "Meerpersoonshuishoudens z. kind"),
          .v("aantal_eenouderhuishoudens",                         "Eenouderhuishoudens"),
          .v("aantal_tweeouderhuishoudens",                        "Tweeouderhuishoudens"),
          .v("gemiddelde_huishoudensgrootte",                      "Gem. huishoudensgrootte"),
          .v("aantal_woningen",                                    "Woningen totaal"),
          .v("aantal_huurwoningen_in_bezit_woningcorporaties",     "Sociale huurwoningen"),
          .v("percentage_huurwoningen",                            "% Sociale huurwoningen"),
          .v("aantal_niet_bewoonde_woningen",                      "Niet-bewoonde woningen"),
          .v("aantal_personen_met_uitkering_onder_aowlft",         "Uitkeringsontvangers (< AOW)")
        ),
        default = "aantal_inwoners"
      )
    ),
    countCol              = "aantal_inwoners",
    weightCol             = "aantal_inwoners",
    defaultClassification = list(method = "jenks", n = 5L, palette = "YlOrRd")
  )
}

# ── Gemeente KWB manifest ─────────────────────────────────────────────────────

.cbs_gem_manifest <- function() {
  list(
    name        = "CBS Kerncijfers Buurten (Gemeente)",
    description = paste(
      "CBS kerncijfers buurten geaggregeerd naar gemeente.",
      "Jaren: 2015, 2017, 2022, 2023, 2024, 2025.",
      "Herkomst-%: beschikbaar 2024/2025 (geboorteland/herkomst indeling).",
      "2015-2023 gebruiken Westers/NietWesters — niet vergelijkbaar.",
      "Afstandsvariabelen uitgesloten — alleen zinvol op buurtniveau."
    ),
    scales       = list(gem = "parquet/cbs-census-gem.parquet"),
    needsGroupBy = FALSE,
    fields       = list(
      year = list(
        type   = "single", label = "Jaar",
        values = .kwb_year_values(), default = 2024L
      ),
      variable = list(
        type   = "single", label = "Variabele",
        values = list(
          .v("aantal_personen_met_een_alg_bijstandsuitkering_tot",  "Bijstandsontvangers"),
          .v("aantal_personen_met_een_ao_uitkering_totaal",         "AO-uitkeringsontvangers"),
          .v("aantal_personen_met_een_ww_uitkering_totaal",         "WW-uitkeringsontvangers"),
          .v("aantal_personen_met_een_aow_uitkering_totaal",        "AOW-ontvangers"),
          .v("aantal_leerlingen_primair_onderwijs",                 "Leerlingen primair onderwijs"),
          .v("aantal_leerlingen_voortgezet_onderwijs",              "Leerlingen voortgezet onderwijs"),
          .v("aantal_studenten_mbo",                                "Studenten MBO"),
          .v("aantal_studenten_hbo",                                "Studenten HBO"),
          .v("aantal_studenten_wo",                                 "Studenten WO"),
          .v("personenautos_totaal",                                "Personenauto's"),
          .v("oppervlakte_land_in_ha",                              "Oppervlakte land (ha)"),
          .v("stedelijkheid_gem",                                   "Stedelijkheid (gewogen gem.)"),
          .v("omgevingsadressendichtheid",                          "Omgevingsadressendichtheid"),
          .v("gemiddelde_woningwaarde",                             "Gem. woningwaarde"),
          .v("percentage_met_herkomstland_nederland",               "% Herkomst Nederland (2024+)"),
          .v("percentage_met_herkomstland_uit_europa_excl_nl",      "% Herkomst Europa excl. NL (2024+)"),
          .v("percentage_met_herkomstland_buiten_europa",           "% Herkomst buiten Europa (2024+)"),
          .v("perc_geb_in_nl_met_herkomstland_in_europa_ex_nl",     "% Europa-herkomst, NL-geb. (2024+)"),
          .v("perc_geb_in_nl_met_herkomstland_buiten_europa",       "% Buiten-Europa-herkomst, NL-geb. (2024+)"),
          .v("perc_geb_buiten_nl_met_herkomstlnd_in_europa_ex_nl",  "% Europa-herkomst, buitenl.-geb. (2024+)"),
          .v("perc_geb_buiten_nl_met_herkomstlnd_buiten_europa",    "% Buiten-Europa-herkomst, buitenl.-geb. (2024+)")
        ),
        default = "aantal_personen_met_een_alg_bijstandsuitkering_tot"
      )
    ),
    countCol              = "aantal_personen_met_een_alg_bijstandsuitkering_tot",
    weightCol             = "aantal_inwoners",
    defaultClassification = list(method = "jenks", n = 5L, palette = "YlOrRd")
  )
}

# ── Gemeente grid manifest ────────────────────────────────────────────────────

.cbs_gem_grid_manifest <- function() {
  list(
    name        = "CBS Vierkantstatistieken (Gemeente)",
    description = paste(
      "CBS 100m grid geaggregeerd naar gemeente. Jaar: 2024.",
      "Bevat huishoudsamenstelling, geslacht, leeftijd en woningvariabelen",
      "die niet in CBS kerncijfers op gemeenteniveau beschikbaar zijn.",
      "Herkomst-% niet beschikbaar: CBS-geheimhouding op gridniveau."
    ),
    scales       = list(gem = "parquet/cbs-census-gem-grid.parquet"),
    needsGroupBy = FALSE,
    fields       = list(
      year = list(
        type   = "single", label = "Jaar",
        values = .grid_year_values(), default = 2024L
      ),
      variable = list(
        type   = "single", label = "Variabele",
        values = list(
          .v("aantal_inwoners",                                    "Inwoners totaal"),
          .v("aantal_mannen",                                      "Mannen"),
          .v("aantal_vrouwen",                                     "Vrouwen"),
          .v("aantal_inwoners_0_tot_15_jaar",                      "Inwoners 0-15 jaar"),
          .v("aantal_inwoners_15_tot_25_jaar",                     "Inwoners 15-25 jaar"),
          .v("aantal_inwoners_25_tot_45_jaar",                     "Inwoners 25-45 jaar"),
          .v("aantal_inwoners_45_tot_65_jaar",                     "Inwoners 45-65 jaar"),
          .v("aantal_inwoners_65_jaar_en_ouder",                   "Inwoners 65+"),
          .v("aantal_part_huishoudens",                            "Particuliere huishoudens"),
          .v("aantal_eenpersoonshuishoudens",                      "Eenpersoonshuishoudens"),
          .v("aantal_meerpersoonshuishoudens_zonder_kind",         "Meerpersoonshuishoudens z. kind"),
          .v("aantal_eenouderhuishoudens",                         "Eenouderhuishoudens"),
          .v("aantal_tweeouderhuishoudens",                        "Tweeouderhuishoudens"),
          .v("gemiddelde_huishoudensgrootte",                      "Gem. huishoudensgrootte"),
          .v("aantal_woningen",                                    "Woningen totaal"),
          .v("aantal_huurwoningen_in_bezit_woningcorporaties",     "Sociale huurwoningen"),
          .v("percentage_huurwoningen",                            "% Sociale huurwoningen"),
          .v("aantal_niet_bewoonde_woningen",                      "Niet-bewoonde woningen"),
          .v("aantal_personen_met_uitkering_onder_aowlft",         "Uitkeringsontvangers (< AOW)")
        ),
        default = "aantal_inwoners"
      )
    ),
    countCol              = "aantal_inwoners",
    weightCol             = "aantal_inwoners",
    defaultClassification = list(method = "jenks", n = 5L, palette = "YlOrRd")
  )
}