# R/nodes/cbs-census.R
#
# Aggregates CBS open data to PC4 and gemeente scales.
# Produces:
#   static/data/parquet/cbs-census-pc4.parquet
#   static/data/parquet/cbs-census-gem.parquet
#
# Data sources (downloaded automatically to raw-data/cbs/ on first run):
#   CBS 100m grid 2024  — https://download.cbs.nl/vierkant/100/
#   CBS kerncijfers 2024 — CBS OData API (table 85984NED)
#
# To run standalone (e.g. when restricted microdata is unavailable):
#   setwd("/path/to/project")
#   source("R/nodes/cbs-census.R")
#   build_cbs_census_pc4()
#   build_cbs_census_gem()
#
# NOTE: Full pipeline integration into build.R is a work item.
# The OData API returns CamelCase column names with numeric suffixes
# (e.g. AantalInwoners_5) which differ from the processed parquet column
# names used elsewhere. Column mapping is handled explicitly below.
# =============================================================================

suppressPackageStartupMessages({
  library(sf)
  library(arrow)
  library(dplyr)
  library(cbsodataR)
})
sf_use_s2(FALSE)

# ── Helpers ───────────────────────────────────────────────────────────────────

.clean_sentinel <- function(x) {
  ifelse(is.numeric(x) & x < -99990, NA_real_, x)
}

.wtmean <- function(values, weights) {
  w <- ifelse(is.na(weights) | weights < 0, 0, weights)
  ok <- !is.na(values) & w > 0
  if (!any(ok)) return(NA_real_)
  sum(values[ok] * w[ok]) / sum(w[ok])
}

.cbs_raw_dir <- function() {
  d <- file.path("raw-data", "cbs")
  dir.create(d, recursive = TRUE, showWarnings = FALSE)
  d
}

.get_grid_100m_gpkg <- function() {
  zip  <- file.path(.cbs_raw_dir(), "cbs_vk100_2024.zip")
  extr <- file.path(.cbs_raw_dir(), "grid_100m")
  if (!file.exists(zip)) {
    cat("Downloading CBS 100m grid (large file, may take several minutes)...\n")
    download.file(
      "https://download.cbs.nl/vierkant/100/2025-cbs_vk100_2024_v1.zip",
      destfile = zip, mode = "wb"
    )
    cat("  Done:", round(file.size(zip) / 1e6), "MB\n")
  }
  if (!dir.exists(extr)) {
    cat("Extracting 100m grid...\n")
    unzip(zip, exdir = extr)
  }
  gpkg <- list.files(extr, pattern = "\\.gpkg$",
                     full.names = TRUE, recursive = TRUE)[1]
  if (is.na(gpkg)) stop("No .gpkg found after extracting ", zip)
  gpkg
}

.get_kwb <- function() {
  path <- file.path(.cbs_raw_dir(), "kwb_2024.rds")
  if (!file.exists(path)) {
    cat("Downloading CBS kerncijfers via OData (table 85984NED)...\n")
    kwb <- cbsodataR::cbs_get_data("85984NED")
    saveRDS(kwb, path)
    cat("  Saved:", nrow(kwb), "rows\n")
  }
  readRDS(path)
}

# ── PC4 build ──────────────────────────────────────────────────────────────────

build_cbs_census_pc4 <- function() {
  cat("\n=== CBS census -> PC4 ===\n")

  pc4_sf <- sf::st_read("static/data/geo/pc4.geojson", quiet = TRUE) |>
    dplyr::select(area_code) |>
    sf::st_transform(28992) |>
    sf::st_make_valid()

  gpkg <- .get_grid_100m_gpkg()
  cat("Reading 100m grid gpkg...\n")
  grid_sf <- sf::st_read(gpkg, quiet = TRUE) |> sf::st_make_valid()
  cat("Grid cells:", nrow(grid_sf), "\n")

  cat("Spatial join: grid cells -> PC4...\n")
  grid_cents <- sf::st_centroid(grid_sf)
  joined     <- sf::st_join(grid_cents, pc4_sf,
                             join = sf::st_within, left = FALSE)
  lookup <- sf::st_drop_geometry(joined) |>
    dplyr::select(grid_id = crs28992res100m, area_code) |>
    dplyr::filter(!is.na(area_code))
  cat(" ", nrow(lookup), "/", nrow(grid_sf), "cells matched\n")

  stats <- sf::st_drop_geometry(grid_sf) |>
    dplyr::inner_join(lookup, by = c("crs28992res100m" = "grid_id")) |>
    dplyr::mutate(dplyr::across(where(is.numeric), .clean_sentinel))

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
        .wtmean(percentage_geb_nederland_herkomst_nederland, aantal_inwoners),
      percentage_geb_nederland_herkomst_overig_europa =
        .wtmean(percentage_geb_nederland_herkomst_overig_europa, aantal_inwoners),
      percentage_geb_nederland_herkomst_buiten_europa =
        .wtmean(percentage_geb_nederland_herkomst_buiten_europa, aantal_inwoners),
      percentage_geb_buiten_nederland_herkomst_europa =
        .wtmean(percentage_geb_buiten_nederland_herkomst_europa, aantal_inwoners),
      percentage_geb_buiten_nederland_herkmst_buiten_europa =
        .wtmean(percentage_geb_buiten_nederland_herkmst_buiten_europa,
                aantal_inwoners),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      year      = 2024L,
      area_code = sprintf("%04d", as.integer(area_code)),
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

  out_path <- "static/data/parquet/cbs-census-pc4.parquet"
  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(out, out_path, compression = "zstd", compression_level = 3)
  cat("wrote", out_path, "(", file.size(out_path) %/% 1024, "KB,",
      nrow(out), "rows )\n")

  .cbs_pc4_manifest()
}

# ── Gemeente build ─────────────────────────────────────────────────────────────

build_cbs_census_gem <- function() {
  cat("\n=== CBS census -> gemeente ===\n")

  kwb <- .get_kwb()

  # Filter to buurt rows and derive gemeente code from WijkenEnBuurten
  # e.g. "BU04890101" -> "GM0489"
  buurt <- kwb |>
    dplyr::filter(trimws(SoortRegio_2) == "Buurt") |>
    dplyr::mutate(
      gemeentecode = paste0("GM", substr(WijkenEnBuurten, 3, 6)),
      dplyr::across(where(is.numeric), .clean_sentinel)
    )

  out <- buurt |>
    dplyr::group_by(area_code = gemeentecode) |>
    dplyr::summarise(
      aantal_inwoners =
        sum(AantalInwoners_5, na.rm = TRUE),
      aantal_personen_met_een_alg_bijstandsuitkering_tot =
        sum(PersonenPerSoortUitkeringBijstand_87, na.rm = TRUE),
      aantal_personen_met_een_ao_uitkering_totaal =
        sum(PersonenPerSoortUitkeringAO_88, na.rm = TRUE),
      aantal_personen_met_een_ww_uitkering_totaal =
        sum(PersonenPerSoortUitkeringWW_89, na.rm = TRUE),
      aantal_personen_met_een_aow_uitkering_totaal =
        sum(PersonenPerSoortUitkeringAOW_90, na.rm = TRUE),
      aantal_studenten_mbo =
        sum(StudentenMboExclExtranei_64, na.rm = TRUE),
      aantal_studenten_hbo =
        sum(StudentenHbo_65, na.rm = TRUE),
      aantal_studenten_wo =
        sum(StudentenWo_66, na.rm = TRUE),
      aantal_leerlingen_primair_onderwijs =
        sum(LeerlingenPo_62, na.rm = TRUE),
      aantal_leerlingen_voortgezet_onderwijs =
        sum(LeerlingenVoInclVavo_63, na.rm = TRUE),
      personenautos_totaal =
        sum(PersonenautoSTotaal_104, na.rm = TRUE),
      oppervlakte_land_in_ha =
        sum(OppervlakteLand_116, na.rm = TRUE),
      gemiddelde_woningwaarde =
        .wtmean(GemiddeldeWOZWaardeVanWoningen_39, AantalInwoners_5),
      percentage_met_herkomstland_nederland =
        .wtmean(Nederland_17, AantalInwoners_5),
      percentage_met_herkomstland_uit_europa_excl_nl =
        .wtmean(EuropaExclusiefNederland_18, AantalInwoners_5),
      percentage_met_herkomstland_buiten_europa =
        .wtmean(BuitenEuropa_19, AantalInwoners_5),
      perc_geb_in_nl_met_herkomstland_in_europa_ex_nl =
        .wtmean(EuropaExclusiefNederland_21, AantalInwoners_5),
      perc_geb_in_nl_met_herkomstland_buiten_europa =
        .wtmean(BuitenEuropa_22, AantalInwoners_5),
      perc_geb_buiten_nl_met_herkomstlnd_in_europa_ex_nl =
        .wtmean(EuropaExclusiefNederland_23, AantalInwoners_5),
      perc_geb_buiten_nl_met_herkomstlnd_buiten_europa =
        .wtmean(BuitenEuropa_24, AantalInwoners_5),
      .groups = "drop"
    ) |>
    dplyr::mutate(year = 2024L) |>
    dplyr::select(area_code, year, dplyr::everything())

  out_path <- "static/data/parquet/cbs-census-gem.parquet"
  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
  arrow::write_parquet(out, out_path, compression = "zstd", compression_level = 3)
  cat("wrote", out_path, "(", file.size(out_path) %/% 1024, "KB,",
      nrow(out), "rows )\n")

  .cbs_gem_manifest()
}

# ── Manifest entries ───────────────────────────────────────────────────────────

.v <- function(id, label) list(id = id, label = label)

.cbs_pc4_manifest <- function() {
  list(
    name        = "CBS Vierkantstatistieken 2024 (PC4)",
    description = paste(
      "CBS 100m grid statistics aggregated to PC4.",
      "Counts summed; origin percentages population-weighted.",
      "Re-run build_cbs_census_pc4() when CBS data updates."
    ),
    scales = list(pc4 = "parquet/cbs-census-pc4.parquet"),
    fields = list(
      year = list(
        type    = "single", label = "Jaar",
        values  = list(list(id = 2024L, label = "2024")),
        default = 2024L
      ),
      variable = list(
        type   = "single", label = "Variabele",
        values = list(
          .v("aantal_inwoners",                                       "Inwoners totaal"),
          .v("aantal_mannen",                                         "Mannen"),
          .v("aantal_vrouwen",                                        "Vrouwen"),
          .v("aantal_inwoners_0_tot_15_jaar",                         "Inwoners 0-15 jaar"),
          .v("aantal_inwoners_15_tot_25_jaar",                        "Inwoners 15-25 jaar"),
          .v("aantal_inwoners_25_tot_45_jaar",                        "Inwoners 25-45 jaar"),
          .v("aantal_inwoners_45_tot_65_jaar",                        "Inwoners 45-65 jaar"),
          .v("aantal_inwoners_65_jaar_en_ouder",                      "Inwoners 65+"),
          .v("aantal_part_huishoudens",                               "Particuliere huishoudens"),
          .v("aantal_eenpersoonshuishoudens",                         "Eenpersoonshuishoudens"),
          .v("aantal_meerpersoonshuishoudens_zonder_kind",            "Meerpersoonshuishoudens zonder kind"),
          .v("aantal_eenouderhuishoudens",                            "Eenouderhuishoudens"),
          .v("aantal_tweeouderhuishoudens",                           "Tweeouderhuishoudens"),
          .v("aantal_woningen",                                       "Woningen totaal"),
          .v("aantal_huurwoningen_in_bezit_woningcorporaties",        "Sociale huurwoningen"),
          .v("aantal_niet_bewoonde_woningen",                         "Niet-bewoonde woningen"),
          .v("aantal_personen_met_uitkering_onder_aowlft",            "Uitkeringsontvangers (onder AOW)"),
          .v("percentage_geb_nederland_herkomst_nederland",           "% Herkomst Nederland"),
          .v("percentage_geb_nederland_herkomst_overig_europa",       "% Herkomst Europa (NL-geb.)"),
          .v("percentage_geb_nederland_herkomst_buiten_europa",       "% Herkomst buiten Europa (NL-geb.)"),
          .v("percentage_geb_buiten_nederland_herkomst_europa",       "% Herkomst Europa (buitenl.-geb.)"),
          .v("percentage_geb_buiten_nederland_herkmst_buiten_europa", "% Herkomst buiten Europa (buitenl.-geb.)")
        ),
        default = "aantal_inwoners"
      )
    ),
    countCol              = "aantal_inwoners",
    weightCol             = "aantal_inwoners",
    defaultClassification = list(method = "jenks", n = 5L, palette = "YlOrRd")
  )
}

.cbs_gem_manifest <- function() {
  list(
    name        = "CBS Kerncijfers Buurten 2024 (Gemeente)",
    description = paste(
      "CBS kerncijfers buurten 2024, aggregated to gemeente.",
      "Counts summed; percentages and housing value population-weighted.",
      "Distance variables excluded — meaningful only at buurt scale.",
      "Re-run build_cbs_census_gem() when CBS data updates."
    ),
    scales = list(gem = "parquet/cbs-census-gem.parquet"),
    fields = list(
      year = list(
        type    = "single", label = "Jaar",
        values  = list(list(id = 2024L, label = "2024")),
        default = 2024L
      ),
      variable = list(
        type   = "single", label = "Variabele",
        values = list(
          .v("aantal_personen_met_een_alg_bijstandsuitkering_tot", "Bijstandsontvangers"),
          .v("aantal_personen_met_een_ao_uitkering_totaal",        "AO-uitkeringsontvangers"),
          .v("aantal_personen_met_een_ww_uitkering_totaal",        "WW-uitkeringsontvangers"),
          .v("aantal_personen_met_een_aow_uitkering_totaal",       "AOW-ontvangers"),
          .v("aantal_studenten_mbo",                               "Studenten MBO"),
          .v("aantal_studenten_hbo",                               "Studenten HBO"),
          .v("aantal_studenten_wo",                                "Studenten WO"),
          .v("aantal_leerlingen_primair_onderwijs",                "Leerlingen primair onderwijs"),
          .v("aantal_leerlingen_voortgezet_onderwijs",             "Leerlingen voortgezet onderwijs"),
          .v("personenautos_totaal",                               "Personenautos"),
          .v("oppervlakte_land_in_ha",                             "Oppervlakte land (ha)"),
          .v("gemiddelde_woningwaarde",                            "Gem. woningwaarde"),
          .v("percentage_met_herkomstland_nederland",              "% Herkomst Nederland"),
          .v("percentage_met_herkomstland_uit_europa_excl_nl",     "% Herkomst Europa excl. NL"),
          .v("percentage_met_herkomstland_buiten_europa",          "% Herkomst buiten Europa"),
          .v("perc_geb_in_nl_met_herkomstland_in_europa_ex_nl",    "% Europa-herkomst, NL-geb."),
          .v("perc_geb_in_nl_met_herkomstland_buiten_europa",      "% Buiten-Europa-herkomst, NL-geb."),
          .v("perc_geb_buiten_nl_met_herkomstlnd_in_europa_ex_nl", "% Europa-herkomst, buitenl.-geb."),
          .v("perc_geb_buiten_nl_met_herkomstlnd_buiten_europa",   "% Buiten-Europa-herkomst, buitenl.-geb.")
        ),
        default = "aantal_personen_met_een_alg_bijstandsuitkering_tot"
      )
    ),
    countCol              = "aantal_personen_met_een_alg_bijstandsuitkering_tot",
    weightCol             = "aantal_inwoners",
    defaultClassification = list(method = "jenks", n = 5L, palette = "YlOrRd")
  )
}