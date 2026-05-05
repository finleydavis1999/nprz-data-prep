# =============================================================================
# 03_process_example_data.R
# Process example nodal and edge data from SQLite files
#
# Sources (D:/data-finley/data-finley/):
#   nodes-2018.sqlite      — demographics, banen-werk, banen-woon
#   edges-woonwerk-2018.sqlite — home-work commuting flows
#
# Years exported: 2007, 2012, 2015, 2017 (nodes)
#                 20072012, 20122017 (edges — 5-year periods)
#
# Output → app-development/static/data/:
#   nodes_demographics_gem.parquet   — population by age/income/edu/sec
#   nodes_banen_werk_gem.parquet     — jobs at work location by age/income/edu
#   nodes_banen_woon_gem.parquet     — jobs at home location by age/income/edu
#   nodes_summary_gem.parquet        — wide-format totals for map display
#   edges_woonwerk_gem.parquet       — OD flows between gemeenten
#   edges_woonwerk_summary_gem.parquet — per-origin totals for nodal display
#
# GM codes: numeric in source (e.g. 363) → padded to GM0363 format
# =============================================================================

setwd("C:/NPRZ_project")
source("R_scripts/preparing_data/utils.R")

library(DBI)
library(RSQLite)
library(dplyr)
library(arrow)
library(tidyr)

# ── Config ────────────────────────────────────────────────────────────────────
NODES_DB   <- "D:/data-finley/data-finley/nodes-2018.sqlite"
EDGES_DB   <- "D:/data-finley/data-finley/edges-woonwerk-2018.sqlite"
EXPORT_DIR <- "C:/NPRZ_project/app-development/static/data"
WERKWERK_DB  <- "D:/data-finley/data-finley/edges-werkwerk-2018.sqlite"
MIGRATION_DB <- "D:/data-finley/data-finley/edges-migration-2018.sqlite"

TARGET_YEARS_NODES <- c(2007L, 2012L, 2015L, 2017L)
TARGET_YEARS_EDGES <- c("20072012", "20122017")

# Format numeric gemeente code to GM0363
format_gm <- function(x) sprintf("GM%04d", as.integer(x))

# Age category labels (from JS metadata)
AGE_LABELS <- c(
  "1" = "Jonger dan 18",
  "2" = "18-23",
  "3" = "24-29",
  "4" = "30-40",
  "5" = "40-59",
  "6" = "60+"
)

# Income category labels
INK_LABELS <- c(
  "1" = "< 20%",
  "2" = "20-40%",
  "3" = "40-60%",
  "4" = "60-80%",
  "5" = "80-100%"
)

# Education category labels
OPL_LABELS <- c(
  "1" = "Laag",
  "2" = "Midden",
  "3" = "Hoog"
)

# Socioeconomic position labels (demographics only)
SEC_LABELS <- c(
  "1" = "Actief (werkend)",
  "2" = "Uitkering",
  "3" = "Pensioen",
  "4" = "Scholier/student"
)

# ── Connect ───────────────────────────────────────────────────────────────────
con_nodes <- dbConnect(SQLite(), NODES_DB)
con_edges <- dbConnect(SQLite(), EDGES_DB)
message("Connected to SQLite databases")

# ── Helper: query with year filter ───────────────────────────────────────────
query_years <- function(con, table, years, dim_col, dim_null_cols, id_col = "gem") {
  # dim_col: the dimension we are breaking down by (NOT NULL)
  # dim_null_cols: all other dimensions that must be NULL
  
  year_list <- paste(years, collapse = ",")
  null_conditions <- paste(sprintf("AND %s IS NULL", dim_null_cols), collapse = " ")
  
  sql <- sprintf("
    SELECT %s, year, %s, SUM(value) AS value
    FROM %s
    WHERE year IN (%s)
    AND %s IS NOT NULL
    %s
    GROUP BY %s, year, %s
    ORDER BY %s, year, %s
  ", id_col, dim_col, table, year_list,
     dim_col, null_conditions,
     id_col, dim_col, id_col, dim_col)
  
  dbGetQuery(con, sql)
}

query_total <- function(con, table, years, marginal_col, null_cols, id_col = "gem") {
  # Total = marginal on age (NOT NULL), all others NULL
  year_list <- paste(years, collapse = ",")
  null_conditions <- paste(sprintf("AND %s IS NULL", null_cols), collapse = " ")
  
  sql <- sprintf("
    SELECT %s, year, SUM(value) AS total
    FROM %s
    WHERE year IN (%s)
    AND %s IS NOT NULL
    %s
    GROUP BY %s, year
    ORDER BY %s, year
  ", id_col, table, year_list,
     marginal_col, null_conditions,
     id_col, id_col)
  
  dbGetQuery(con, sql)
}

# ── SECTION 1: Banen-werk (jobs at work location) ─────────────────────────────
message("\n=== Section 1: Banen-werk (jobs at work location) ===")

werk_null_cols <- c("opl", "inks", "sectorcat", "soortbaan")

# Total workers per gemeente per year
werk_total <- query_total(con_nodes, "woonwerk_werk_19992018_gem",
                           TARGET_YEARS_NODES, "age", werk_null_cols) |>
  mutate(gemeentecode = format_gm(gem), .keep = "unused") |>
  rename(jaar = year, total_banen_werk = total)

message("  Total rows: ", nrow(werk_total))

# Age breakdown
werk_age <- query_years(con_nodes, "woonwerk_werk_19992018_gem",
                         TARGET_YEARS_NODES, "age", werk_null_cols) |>
  mutate(
    gemeentecode = format_gm(gem),
    age_label    = AGE_LABELS[as.character(age)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

# Income breakdown  
werk_ink <- query_years(con_nodes, "woonwerk_werk_19992018_gem",
                         TARGET_YEARS_NODES, "inks",
                         c("age", "opl", "sectorcat", "soortbaan")) |>
  mutate(
    gemeentecode = format_gm(gem),
    ink_label    = INK_LABELS[as.character(inks)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

# Education breakdown
werk_opl <- query_years(con_nodes, "woonwerk_werk_19992018_gem",
                         TARGET_YEARS_NODES, "opl",
                         c("age", "inks", "sectorcat", "soortbaan")) |>
  mutate(
    gemeentecode = format_gm(gem),
    opl_label    = OPL_LABELS[as.character(opl)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

# Export detailed parquet
banen_werk_out <- list(
  total     = werk_total,
  by_age    = werk_age,
  by_income = werk_ink,
  by_edu    = werk_opl
)

write_parquet(werk_total, file.path(EXPORT_DIR, "nodes_banen_werk_totaal_gem.parquet"))
write_parquet(werk_age,   file.path(EXPORT_DIR, "nodes_banen_werk_leeftijd_gem.parquet"))
write_parquet(werk_ink,   file.path(EXPORT_DIR, "nodes_banen_werk_inkomen_gem.parquet"))
write_parquet(werk_opl,   file.path(EXPORT_DIR, "nodes_banen_werk_opleiding_gem.parquet"))
message("  Exported banen-werk parquets")

# ── SECTION 2: Banen-woon (jobs at home location = employed residents) ────────
message("\n=== Section 2: Banen-woon (employed residents at home location) ===")

woon_null_cols <- c("opl", "inks", "sectorcat", "soortbaan")

woon_total <- query_total(con_nodes, "woonwerk_woon_19992018_gem",
                           TARGET_YEARS_NODES, "age", woon_null_cols) |>
  mutate(gemeentecode = format_gm(gem), .keep = "unused") |>
  rename(jaar = year, total_banen_woon = total)

woon_age <- query_years(con_nodes, "woonwerk_woon_19992018_gem",
                         TARGET_YEARS_NODES, "age", woon_null_cols) |>
  mutate(
    gemeentecode = format_gm(gem),
    age_label    = AGE_LABELS[as.character(age)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

woon_ink <- query_years(con_nodes, "woonwerk_woon_19992018_gem",
                         TARGET_YEARS_NODES, "inks",
                         c("age", "opl", "sectorcat", "soortbaan")) |>
  mutate(
    gemeentecode = format_gm(gem),
    ink_label    = INK_LABELS[as.character(inks)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

woon_opl <- query_years(con_nodes, "woonwerk_woon_19992018_gem",
                         TARGET_YEARS_NODES, "opl",
                         c("age", "inks", "sectorcat", "soortbaan")) |>
  mutate(
    gemeentecode = format_gm(gem),
    opl_label    = OPL_LABELS[as.character(opl)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

write_parquet(woon_total, file.path(EXPORT_DIR, "nodes_banen_woon_totaal_gem.parquet"))
write_parquet(woon_age,   file.path(EXPORT_DIR, "nodes_banen_woon_leeftijd_gem.parquet"))
write_parquet(woon_ink,   file.path(EXPORT_DIR, "nodes_banen_woon_inkomen_gem.parquet"))
write_parquet(woon_opl,   file.path(EXPORT_DIR, "nodes_banen_woon_opleiding_gem.parquet"))
message("  Exported banen-woon parquets")

# ── SECTION 3: Demographics (persoonsgegevens) ────────────────────────────────
message("\n=== Section 3: Demographics (persoonsgegevens) ===")

demo_null_cols <- c("sec", "hh", "opl", "inks")

demo_total <- query_total(con_nodes, "demographics19992018_gem",
                           TARGET_YEARS_NODES, "age", demo_null_cols) |>
  mutate(gemeentecode = format_gm(gem), .keep = "unused") |>
  rename(jaar = year, total_inwoners = total)

demo_age <- query_years(con_nodes, "demographics19992018_gem",
                         TARGET_YEARS_NODES, "age", demo_null_cols) |>
  mutate(
    gemeentecode = format_gm(gem),
    age_label    = AGE_LABELS[as.character(age)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

demo_ink <- query_years(con_nodes, "demographics19992018_gem",
                         TARGET_YEARS_NODES, "inks",
                         c("age", "sec", "hh", "opl")) |>
  mutate(
    gemeentecode = format_gm(gem),
    ink_label    = INK_LABELS[as.character(inks)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

demo_opl <- query_years(con_nodes, "demographics19992018_gem",
                         TARGET_YEARS_NODES, "opl",
                         c("age", "sec", "hh", "inks")) |>
  mutate(
    gemeentecode = format_gm(gem),
    opl_label    = OPL_LABELS[as.character(opl)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

demo_sec <- query_years(con_nodes, "demographics19992018_gem",
                         TARGET_YEARS_NODES, "sec",
                         c("age", "hh", "opl", "inks")) |>
  mutate(
    gemeentecode = format_gm(gem),
    sec_label    = SEC_LABELS[as.character(sec)],
    .keep = "unused"
  ) |>
  rename(jaar = year, n = value)

write_parquet(demo_total, file.path(EXPORT_DIR, "nodes_demo_totaal_gem.parquet"))
write_parquet(demo_age,   file.path(EXPORT_DIR, "nodes_demo_leeftijd_gem.parquet"))
write_parquet(demo_ink,   file.path(EXPORT_DIR, "nodes_demo_inkomen_gem.parquet"))
write_parquet(demo_opl,   file.path(EXPORT_DIR, "nodes_demo_opleiding_gem.parquet"))
write_parquet(demo_sec,   file.path(EXPORT_DIR, "nodes_demo_sec_gem.parquet"))
message("  Exported demographics parquets")

# ── SECTION 4: Summary wide-format for map display ────────────────────────────
# One row per gemeente per year — totals only, plus derived ratios
# This is what the Svelte app displays as choropleth variables
message("\n=== Section 4: Summary wide-format for map display ===")

nodes_summary <- werk_total |>
  left_join(woon_total, by = c("gemeentecode", "jaar")) |>
  left_join(demo_total, by = c("gemeentecode", "jaar")) |>
  mutate(
    # Jobs-to-residents ratio (key for spatial interaction modelling)
    ratio_banen_inwoners = round(total_banen_werk / pmax(total_inwoners, 1), 4),
    # Employment rate proxy (employed residents / total pop)
    ratio_werkenden_inwoners = round(total_banen_woon / pmax(total_inwoners, 1), 4)
  )

write_parquet(nodes_summary, file.path(EXPORT_DIR, "nodes_summary_gem.parquet"))
message("  nodes_summary_gem.parquet: ", nrow(nodes_summary), " rows")
message("  Columns: ", paste(names(nodes_summary), collapse = ", "))

# ── SECTION 5: Woonwerk edges ─────────────────────────────────────────────────
message("\n=== Section 5: Woonwerk edges (home-work flows) ===")

con_edges <- dbConnect(SQLite(), EDGES_DB)

edges_raw <- dbGetQuery(con_edges, "
  SELECT woongem, werkgem, year, SUM(value) AS flow_value
  FROM woonwerk_19992018_gem
  WHERE year IN ('20072012', '20122017')
  AND age IS NOT NULL
  AND opl IS NULL AND inks IS NULL
  AND sectorcat IS NULL AND soortbaan IS NULL
  GROUP BY woongem, werkgem, year
  ORDER BY year, flow_value DESC
")

dbDisconnect(con_edges)

message("  Raw OD pairs: ", nrow(edges_raw))

# Divide by 6 (both periods are 6 collection years: 2007-2012 and 2012-2017)
# Source: JS metadata rowSumCalculation: range[1] - range[0] + 1
edges_out <- edges_raw |>
  mutate(
    origin_id      = format_gm(woongem),
    destination_id = format_gm(werkgem),
    flow_value     = round(flow_value / 6),
    variable_name  = "woonwerk",
    periode        = year
  ) |>
  filter(flow_value > 0) |>
  select(origin_id, destination_id, periode, flow_value, variable_name)

message("  After averaging and filtering zeros: ", nrow(edges_out))
message("  By period:")
print(edges_out |> count(periode))
message("  Top 5 flows (20122017):")
print(edges_out |> filter(periode == "20122017") |> head(5))

# Per-origin summary
edges_summary <- edges_out |>
  group_by(origin_id, periode) |>
  summarise(
    total_outflow  = sum(flow_value, na.rm = TRUE),
    n_destinations = n_distinct(destination_id),
    .groups = "drop"
  )

write_parquet(edges_out,     file.path(EXPORT_DIR, "edges_woonwerk_gem.parquet"))
write_parquet(edges_summary, file.path(EXPORT_DIR, "edges_woonwerk_summary_gem.parquet"))

message("  Exported edges_woonwerk_gem.parquet")
message("  Exported edges_woonwerk_summary_gem.parquet")

# ── SECTION 6: Werkwerk edges (job-to-job moves) ─────────────────────────────
message("\n=== Section 6: Werkwerk edges (job moves) ===")

con_ww <- dbConnect(SQLite(), WERKWERK_DB)

werkwerk_raw <- dbGetQuery(con_ww, "
  SELECT GEMy1, GEMy2, year, SUM(value) AS flow_value
  FROM werkwerk_19992018_gem
  WHERE year IN ('07-12', '12-17')
  AND age IS NOT NULL
  AND opl IS NULL AND inks IS NULL
  AND sectorcat2 IS NULL AND sectorsector IS NULL AND soortbaan IS NULL
  GROUP BY GEMy1, GEMy2, year
  ORDER BY year, flow_value DESC
")

dbDisconnect(con_ww)

message("  Raw OD pairs: ", nrow(werkwerk_raw))

# Both periods are 6 years (2007-2012, 2012-2017)
werkwerk_out <- werkwerk_raw |>
  mutate(
    origin_id      = format_gm(GEMy1),
    destination_id = format_gm(GEMy2),
    flow_value     = flow_value,
    variable_name  = "werkwerk",
    periode        = year
  ) |>
  filter(flow_value > 0) |>
  select(origin_id, destination_id, periode, flow_value, variable_name)

message("  After averaging: ", nrow(werkwerk_out))
print(werkwerk_out |> count(periode))

werkwerk_summary <- werkwerk_out |>
  group_by(origin_id, periode) |>
  summarise(
    total_outflow  = sum(flow_value, na.rm = TRUE),
    n_destinations = n_distinct(destination_id),
    .groups = "drop"
  )

write_parquet(werkwerk_out,     file.path(EXPORT_DIR, "edges_werkwerk_gem.parquet"))
write_parquet(werkwerk_summary, file.path(EXPORT_DIR, "edges_werkwerk_summary_gem.parquet"))
message("  Exported edges_werkwerk_gem.parquet")

# ── SECTION 7: Migration edges (residential moves) ───────────────────────────
message("\n=== Section 7: Migration edges (residential moves) ===")



con_mig <- dbConnect(SQLite(), MIGRATION_DB)

migration_raw <- dbGetQuery(con_mig, "
  SELECT gemPre, gemPost, year, SUM(value) AS flow_value
  FROM verhuizingen_19992018_gem
  WHERE year IN ('p07-10', 'p11-14', 'p15-18')
  AND age IS NOT NULL
  AND opl IS NULL AND inks IS NULL
  AND sec IS NULL AND hh IS NULL AND inkchanges IS NULL
  GROUP BY gemPre, gemPost, year
  ORDER BY year, flow_value DESC
")

dbDisconnect(con_mig)

message("  Raw OD pairs: ", nrow(migration_raw))

migration_out <- migration_raw |>
  mutate(
    origin_id      = format_gm(gemPre),
    destination_id = format_gm(gemPost),
    flow_value     = flow_value,
    variable_name  = "migration",
    periode        = year
  ) |>
  filter(flow_value > 0) |>
  select(origin_id, destination_id, periode, flow_value, variable_name)

message("  After averaging: ", nrow(migration_out))
print(migration_out |> count(periode))

migration_summary <- migration_out |>
  group_by(origin_id, periode) |>
  summarise(
    total_outflow  = sum(flow_value, na.rm = TRUE),
    n_destinations = n_distinct(destination_id),
    .groups = "drop"
  )

write_parquet(migration_out,     file.path(EXPORT_DIR, "edges_migration_gem.parquet"))
write_parquet(migration_summary, file.path(EXPORT_DIR, "edges_migration_summary_gem.parquet"))
message("  Exported edges_migration_gem.parquet")

# ── SECTION 8: Disconnect & summary ──────────────────────────────────────────
dbDisconnect(con_nodes)
dbDisconnect(con_edges)

message("\n========================================")
message("=== PIPELINE COMPLETE ===")
message("========================================")
message("\nFiles exported to: ", EXPORT_DIR)
message("\nNode files (map display):")
message("  nodes_summary_gem.parquet         — totals + ratios, all years")
message("Node files (breakdown/analysis):")
message("  nodes_banen_werk_totaal_gem        — jobs at work location")
message("  nodes_banen_werk_leeftijd_gem      — by age")
message("  nodes_banen_werk_inkomen_gem       — by income")
message("  nodes_banen_werk_opleiding_gem     — by education")
message("  nodes_banen_woon_totaal_gem        — employed residents")
message("  nodes_banen_woon_leeftijd_gem      — by age")
message("  nodes_banen_woon_inkomen_gem       — by income")
message("  nodes_banen_woon_opleiding_gem     — by education")
message("  nodes_demo_totaal_gem              — total population")
message("  nodes_demo_leeftijd_gem            — by age")
message("  nodes_demo_inkomen_gem             — by income")
message("  nodes_demo_opleiding_gem           — by education")
message("  nodes_demo_sec_gem                 — by socioeconomic position")
message("\nEdge files:")
message("  edges_woonwerk_gem.parquet         — OD flows, two periods")
message("  edges_woonwerk_summary_gem.parquet — per-origin totals")
message("\nNext: add nodes_summary_gem variables to config.ts VARIABLES array")
message("      update EDGE_DATASETS in config.ts to include edges_woonwerk_gem")

# =============================================================================
# SECTIONS 8-12: PC4-level nodes, edges, and ODiN pipeline stub
# Append these to 03_process_example_data.R after Section 7
#
# Section 8:  PC4 node data (demographics, banen-werk, banen-woon)
# Section 9:  PC4 node summary (wide-format for map display)
# Section 10: PC4 edge data (woonwerk, werkwerk, migration)
# Section 11: PC4 centroids (for flow line rendering)
# Section 12: ODiN pipeline stub (runs when SQLite exists)
# =============================================================================

# These libraries should already be loaded from sections 1-7.
# Listed here for clarity if running sections 8-12 standalone.
library(sf)
library(dplyr)
library(arrow)
library(DBI)
library(RSQLite)
library(jsonlite)
sf_use_s2(FALSE)

# ── Config additions ───────────────────────────────────────────────────────────
# Path to ODiN SQLite (update when data arrives)
ODIN_DB <- "D:/data-finley/data-finley/edges-ovin-2022.sqlite"

# Inner boundary PC4 codes — centroid-within filter.
# A PC4 area is "in" the study area only if its centroid falls within the
# inner boundary polygon. This gives a clean cut without including neighbouring
# PC4 areas that merely touch or overlap the boundary edge.
inner_boundary <- st_read(
  file.path(EXPORT_DIR, "rotterdam_boundary.geojson"), quiet = TRUE)
pc4_sf_geom <- st_read(
  file.path(EXPORT_DIR, "pc4_zh_2024.geojson"), quiet = TRUE)

# Reproject both to same CRS for spatial operations
inner_proj  <- st_transform(inner_boundary, st_crs(pc4_sf_geom))

# Centroid-within: only PC4 areas whose centroid is inside the boundary
pc4_cents_geom <- st_centroid(pc4_sf_geom)
inside         <- st_within(pc4_cents_geom, inner_proj, sparse = FALSE)[, 1]
pc4_inner      <- pc4_sf_geom[inside, ]

pc4_study <- as.integer(pc4_inner$postcode)

message("Inner boundary PC4 codes (centroid-within): ", length(pc4_study))
message("Range: ", min(pc4_study), " – ", max(pc4_study))

# For edge filtering we want flows that START or END in the study area.
# Flows between two non-study-area PC4s are excluded even if they pass through.
pc4_in <- paste(pc4_study, collapse = ",")

# Age / income / education / sec labels (same as gemeente sections)
AGE_LABELS <- c("1"="Jonger dan 18","2"="18-23","3"="24-29",
                "4"="30-40","5"="40-59","6"="60+")
INK_LABELS <- c("1"="< 20%","2"="20-40%","3"="40-60%",
                "4"="60-80%","5"="80-100%")
OPL_LABELS <- c("1"="Laag","2"="Midden","3"="Hoog")
SEC_LABELS <- c("1"="Actief (werkend)","2"="Uitkering",
                "3"="Pensioen","4"="Scholier/student")

# Re-open connections (may have been closed in earlier sections)
con_nodes <- dbConnect(SQLite(), NODES_DB)

# ── SECTION 8: PC4 node data ───────────────────────────────────────────────────
message("\n=== Section 8: PC4 node data ===")

# Helper: query PC4 nodes with dimension breakdown
query_pc4_years <- function(con, table, years, dim_col, null_cols,
                             id_col = "pc4") {
  year_list <- paste(years, collapse = ",")
  null_conds <- paste(sprintf("AND %s IS NULL", null_cols), collapse = " ")
  sql <- sprintf("
    SELECT %s, year, %s, SUM(value) AS value
    FROM %s
    WHERE year IN (%s)
    AND %s IS NOT NULL
    %s
    GROUP BY %s, year, %s
    ORDER BY %s, year, %s
  ", id_col, dim_col, table, year_list,
     dim_col, null_conds,
     id_col, dim_col, id_col, dim_col)
  dbGetQuery(con, sql)
}

query_pc4_total <- function(con, table, years, marginal_col, null_cols,
                             id_col = "pc4") {
  year_list <- paste(years, collapse = ",")
  null_conds <- paste(sprintf("AND %s IS NULL", null_cols), collapse = " ")
  sql <- sprintf("
    SELECT %s, year, SUM(value) AS total
    FROM %s
    WHERE year IN (%s)
    AND %s IS NOT NULL
    %s
    GROUP BY %s, year
    ORDER BY %s, year
  ", id_col, table, year_list,
     marginal_col, null_conds,
     id_col, id_col)
  dbGetQuery(con, sql)
}

TARGET_YEARS_NODES <- c(2007L, 2012L, 2015L, 2017L)

# ── 8a: Demographics PC4 ──────────────────────────────────────────────────────
message("  Demographics PC4...")
demo_null_pc4 <- c("sec", "hh", "opl", "inks")

demo_pc4_total <- query_pc4_total(
  con_nodes, "demographics19992018_pc",
  TARGET_YEARS_NODES, "age", demo_null_pc4) |>
  mutate(postcode = as.integer(pc4), .keep = "unused") |>
  rename(jaar = year, total_inwoners = total)

demo_pc4_age <- query_pc4_years(
  con_nodes, "demographics19992018_pc",
  TARGET_YEARS_NODES, "age", demo_null_pc4) |>
  mutate(postcode = as.integer(pc4),
         age_label = AGE_LABELS[as.character(age)], .keep = "unused") |>
  rename(jaar = year, n = value)

demo_pc4_ink <- query_pc4_years(
  con_nodes, "demographics19992018_pc",
  TARGET_YEARS_NODES, "inks", c("age","sec","hh","opl")) |>
  mutate(postcode = as.integer(pc4),
         ink_label = INK_LABELS[as.character(inks)], .keep = "unused") |>
  rename(jaar = year, n = value)

demo_pc4_opl <- query_pc4_years(
  con_nodes, "demographics19992018_pc",
  TARGET_YEARS_NODES, "opl", c("age","sec","hh","inks")) |>
  mutate(postcode = as.integer(pc4),
         opl_label = OPL_LABELS[as.character(opl)], .keep = "unused") |>
  rename(jaar = year, n = value)

demo_pc4_sec <- query_pc4_years(
  con_nodes, "demographics19992018_pc",
  TARGET_YEARS_NODES, "sec", c("age","hh","opl","inks")) |>
  mutate(postcode = as.integer(pc4),
         sec_label = SEC_LABELS[as.character(sec)], .keep = "unused") |>
  rename(jaar = year, n = value)

write_parquet(demo_pc4_total, file.path(EXPORT_DIR, "nodes_demo_totaal_pc4.parquet"))
write_parquet(demo_pc4_age,   file.path(EXPORT_DIR, "nodes_demo_leeftijd_pc4.parquet"))
write_parquet(demo_pc4_ink,   file.path(EXPORT_DIR, "nodes_demo_inkomen_pc4.parquet"))
write_parquet(demo_pc4_opl,   file.path(EXPORT_DIR, "nodes_demo_opleiding_pc4.parquet"))
write_parquet(demo_pc4_sec,   file.path(EXPORT_DIR, "nodes_demo_sec_pc4.parquet"))
message("  Exported demographics PC4 parquets")

# ── 8b: Banen-werk PC4 ───────────────────────────────────────────────────────
message("  Banen-werk PC4...")
werk_null_pc4 <- c("opl","inks","sectorcat","soortbaan")

werk_pc4_total <- query_pc4_total(
  con_nodes, "woonwerk_werk_19992018_pc",
  TARGET_YEARS_NODES, "age", werk_null_pc4) |>
  mutate(postcode = as.integer(pc4), .keep = "unused") |>
  rename(jaar = year, total_banen_werk = total)

werk_pc4_age <- query_pc4_years(
  con_nodes, "woonwerk_werk_19992018_pc",
  TARGET_YEARS_NODES, "age", werk_null_pc4) |>
  mutate(postcode = as.integer(pc4),
         age_label = AGE_LABELS[as.character(age)], .keep = "unused") |>
  rename(jaar = year, n = value)

werk_pc4_ink <- query_pc4_years(
  con_nodes, "woonwerk_werk_19992018_pc",
  TARGET_YEARS_NODES, "inks", c("age","opl","sectorcat","soortbaan")) |>
  mutate(postcode = as.integer(pc4),
         ink_label = INK_LABELS[as.character(inks)], .keep = "unused") |>
  rename(jaar = year, n = value)

werk_pc4_opl <- query_pc4_years(
  con_nodes, "woonwerk_werk_19992018_pc",
  TARGET_YEARS_NODES, "opl", c("age","inks","sectorcat","soortbaan")) |>
  mutate(postcode = as.integer(pc4),
         opl_label = OPL_LABELS[as.character(opl)], .keep = "unused") |>
  rename(jaar = year, n = value)

write_parquet(werk_pc4_total, file.path(EXPORT_DIR, "nodes_banen_werk_totaal_pc4.parquet"))
write_parquet(werk_pc4_age,   file.path(EXPORT_DIR, "nodes_banen_werk_leeftijd_pc4.parquet"))
write_parquet(werk_pc4_ink,   file.path(EXPORT_DIR, "nodes_banen_werk_inkomen_pc4.parquet"))
write_parquet(werk_pc4_opl,   file.path(EXPORT_DIR, "nodes_banen_werk_opleiding_pc4.parquet"))
message("  Exported banen-werk PC4 parquets")

# ── 8c: Banen-woon PC4 ───────────────────────────────────────────────────────
message("  Banen-woon PC4...")
woon_null_pc4 <- c("opl","inks","sectorcat","soortbaan")

woon_pc4_total <- query_pc4_total(
  con_nodes, "woonwerk_woon_19992018_pc",
  TARGET_YEARS_NODES, "age", woon_null_pc4) |>
  mutate(postcode = as.integer(pc4), .keep = "unused") |>
  rename(jaar = year, total_banen_woon = total)

woon_pc4_age <- query_pc4_years(
  con_nodes, "woonwerk_woon_19992018_pc",
  TARGET_YEARS_NODES, "age", woon_null_pc4) |>
  mutate(postcode = as.integer(pc4),
         age_label = AGE_LABELS[as.character(age)], .keep = "unused") |>
  rename(jaar = year, n = value)

woon_pc4_ink <- query_pc4_years(
  con_nodes, "woonwerk_woon_19992018_pc",
  TARGET_YEARS_NODES, "inks", c("age","opl","sectorcat","soortbaan")) |>
  mutate(postcode = as.integer(pc4),
         ink_label = INK_LABELS[as.character(inks)], .keep = "unused") |>
  rename(jaar = year, n = value)

woon_pc4_opl <- query_pc4_years(
  con_nodes, "woonwerk_woon_19992018_pc",
  TARGET_YEARS_NODES, "opl", c("age","inks","sectorcat","soortbaan")) |>
  mutate(postcode = as.integer(pc4),
         opl_label = OPL_LABELS[as.character(opl)], .keep = "unused") |>
  rename(jaar = year, n = value)

write_parquet(woon_pc4_total, file.path(EXPORT_DIR, "nodes_banen_woon_totaal_pc4.parquet"))
write_parquet(woon_pc4_age,   file.path(EXPORT_DIR, "nodes_banen_woon_leeftijd_pc4.parquet"))
write_parquet(woon_pc4_ink,   file.path(EXPORT_DIR, "nodes_banen_woon_inkomen_pc4.parquet"))
write_parquet(woon_pc4_opl,   file.path(EXPORT_DIR, "nodes_banen_woon_opleiding_pc4.parquet"))
message("  Exported banen-woon PC4 parquets")

# ── SECTION 9: PC4 node summary (wide-format for map display) ─────────────────
message("\n=== Section 9: PC4 node summary ===")

nodes_summary_pc4 <- werk_pc4_total |>
  left_join(woon_pc4_total, by = c("postcode", "jaar")) |>
  left_join(demo_pc4_total, by = c("postcode", "jaar")) |>
  mutate(
    ratio_banen_inwoners     = round(total_banen_werk / pmax(total_inwoners, 1), 4),
    ratio_werkenden_inwoners = round(total_banen_woon / pmax(total_inwoners, 1), 4)
  )

write_parquet(nodes_summary_pc4, file.path(EXPORT_DIR, "nodes_summary_pc4.parquet"))
message("  nodes_summary_pc4.parquet: ", nrow(nodes_summary_pc4), " rows")
message("  Columns: ", paste(names(nodes_summary_pc4), collapse = ", "))

dbDisconnect(con_nodes)

# ── SECTION 10: PC4 edge data ──────────────────────────────────────────────────
message("\n=== Section 10: PC4 edge data ===")
message("  Study area filter: ", length(pc4_study), " PC4 codes")
message("  (keeping only flows where origin OR destination is in study area)")

# ── 10a: Woonwerk PC4 ────────────────────────────────────────────────────────
message("  Woonwerk PC4...")
con_ww <- dbConnect(SQLite(), EDGES_DB)

woonwerk_pc4_raw <- dbGetQuery(con_ww, sprintf("
  SELECT woonpostcode AS origin_id,
         werkpostcode AS destination_id,
         year         AS periode,
         SUM(value)   AS flow_value
  FROM woonwerk_19992018_pc
  WHERE year IN ('20072012', '20122017')
  AND age IS NOT NULL
  AND opl IS NULL AND inks IS NULL
  AND sectorcat IS NULL AND soortbaan IS NULL
  AND (woonpostcode IN (%s) OR werkpostcode IN (%s))
  GROUP BY woonpostcode, werkpostcode, year
  ORDER BY year, flow_value DESC
", pc4_in, pc4_in))

dbDisconnect(con_ww)
message("  Raw pairs: ", nrow(woonwerk_pc4_raw))

woonwerk_pc4_out <- woonwerk_pc4_raw |>
  mutate(
    origin_id      = as.integer(origin_id),
    destination_id = as.integer(destination_id),
    flow_value     = round(flow_value / 6),  # annual average
    variable_name  = "woonwerk"
  ) |>
  filter(flow_value > 0)

woonwerk_pc4_summary <- woonwerk_pc4_out |>
  group_by(origin_id, periode) |>
  summarise(
    total_outflow  = sum(flow_value),
    n_destinations = n_distinct(destination_id),
    .groups = "drop"
  )

write_parquet(woonwerk_pc4_out,
              file.path(EXPORT_DIR, "edges_woonwerk_pc4.parquet"))
write_parquet(woonwerk_pc4_summary,
              file.path(EXPORT_DIR, "edges_woonwerk_summary_pc4.parquet"))
message("  After filter + averaging: ", nrow(woonwerk_pc4_out), " rows | ",
        round(file.size(file.path(EXPORT_DIR, "edges_woonwerk_pc4.parquet"))/1024), " KB")

# ── 10b: Werkwerk PC4 ────────────────────────────────────────────────────────
message("  Werkwerk PC4...")
con_ww2 <- dbConnect(SQLite(), WERKWERK_DB)

werkwerk_pc4_raw <- dbGetQuery(con_ww2, sprintf("
  SELECT POSTCODEy1   AS origin_id,
         POSTCODEy2   AS destination_id,
         year         AS periode,
         SUM(value)   AS flow_value
  FROM werkwerk_19992018_pc
  WHERE year IN ('07-12', '12-17')
  AND age IS NOT NULL
  AND opl IS NULL AND inks IS NULL
  AND sectorsector IS NULL AND soortbaan IS NULL
  AND (POSTCODEy1 IN (%s) OR POSTCODEy2 IN (%s))
  GROUP BY POSTCODEy1, POSTCODEy2, year
  ORDER BY year, flow_value DESC
", pc4_in, pc4_in))

dbDisconnect(con_ww2)

werkwerk_pc4_out <- werkwerk_pc4_raw |>
  mutate(
    origin_id      = as.integer(origin_id),
    destination_id = as.integer(destination_id),
    variable_name  = "werkwerk"
  ) |>
  filter(flow_value > 0)

werkwerk_pc4_summary <- werkwerk_pc4_out |>
  group_by(origin_id, periode) |>
  summarise(
    total_outflow  = sum(flow_value),
    n_destinations = n_distinct(destination_id),
    .groups = "drop"
  )

write_parquet(werkwerk_pc4_out,
              file.path(EXPORT_DIR, "edges_werkwerk_pc4.parquet"))
write_parquet(werkwerk_pc4_summary,
              file.path(EXPORT_DIR, "edges_werkwerk_summary_pc4.parquet"))
message("  Werkwerk PC4: ", nrow(werkwerk_pc4_out), " rows")

# ── 10c: Migration PC4 ───────────────────────────────────────────────────────
message("  Migration PC4...")
con_mig <- dbConnect(SQLite(), MIGRATION_DB)

migration_pc4_raw <- dbGetQuery(con_mig, sprintf("
  SELECT pcPre        AS origin_id,
         pcPost       AS destination_id,
         year         AS periode,
         SUM(value)   AS flow_value
  FROM verhuizingen_19992018_pc
  WHERE year IN ('p07-10', 'p11-14', 'p15-18')
  AND age IS NOT NULL
  AND opl IS NULL AND inks IS NULL
  AND sec IS NULL AND hh IS NULL AND inkchanges IS NULL
  AND (pcPre IN (%s) OR pcPost IN (%s))
  GROUP BY pcPre, pcPost, year
  ORDER BY year, flow_value DESC
", pc4_in, pc4_in))

dbDisconnect(con_mig)

migration_pc4_out <- migration_pc4_raw |>
  mutate(
    origin_id      = as.integer(origin_id),
    destination_id = as.integer(destination_id),
    variable_name  = "migration"
  ) |>
  filter(flow_value > 0)

migration_pc4_summary <- migration_pc4_out |>
  group_by(origin_id, periode) |>
  summarise(
    total_outflow  = sum(flow_value),
    n_destinations = n_distinct(destination_id),
    .groups = "drop"
  )

write_parquet(migration_pc4_out,
              file.path(EXPORT_DIR, "edges_migration_pc4.parquet"))
write_parquet(migration_pc4_summary,
              file.path(EXPORT_DIR, "edges_migration_summary_pc4.parquet"))
message("  Migration PC4: ", nrow(migration_pc4_out), " rows")

# ── SECTION 11: PC4 centroids for flow line rendering ─────────────────────────
# Run just section 11 in R — rebuilds pc4_centroids.json with full middle boundary
# Make sure pc4_sf_geom is in your environment first:
# If not: pc4_sf_geom <- st_read("C:/NPRZ_project/app-development/static/data/pc4_zh_2024.geojson", quiet=TRUE)

pc4_all_cents  <- st_centroid(pc4_sf_geom)
pc4_all_coords <- st_coordinates(pc4_all_cents)

pc4_centroids <- data.frame(
  id  = as.integer(pc4_sf_geom$postcode),
  lng = round(pc4_all_coords[, "X"], 6),
  lat = round(pc4_all_coords[, "Y"], 6)
)

write_json(pc4_centroids,
           file.path(export_dir, "pc4_centroids.json"),
           dataframe = "rows")
message("pc4_centroids.json: ", nrow(pc4_centroids), " centroids")

# ── SECTION 12: ODiN pipeline stub ────────────────────────────────────────────
# =============================================================================
# ODiN (OViN) data: survey-based trip data 2004-2022
# Schema from edges-ovin20042022.js:
#   Table:     ovin20042022 (single table, one row per trip)
#   Weight:    factorv  — SUM(factorv) gives weighted trip counts
#   Division:  SUM(factorv) / (n_years * 365) = daily average trips
#   Geography (gemeente): c_vgemf (origin), c_agemf (destination)
#   Geography (PC4):      c_vpcf  (origin), c_apcf  (destination)
#   Key dims:  year (int), c_lft (age 0-99), c_motief (trip purpose 1-9),
#              c_modus (mode 1-4), c_opl (education 1-4,0),
#              c_maatsch (socioeconomic 1-8)
#
# Trip purpose (c_motief):
#   1=Work, 2=Business, 3=Services, 4=Shopping, 5=Education,
#   6=Visit/stay, 7=Social/recreational, 8=Tour/walk, 9=Other
#
# Mode (c_modus):
#   1=Car, 2=Train/bus/tram/metro, 3=Walk/cycle, 4=Other
# =============================================================================

message("\n=== Section 12: ODiN pipeline stub ===")

if (file.exists(ODIN_DB)) {
  message("  ODiN SQLite found — processing...")
  con_odin <- dbConnect(SQLite(), ODIN_DB)

  # Check available years
  years_available <- dbGetQuery(con_odin,
    "SELECT DISTINCT year FROM ovin20042022 ORDER BY year")$year
  message("  Available years: ", paste(range(years_available), collapse = "–"))

  # Define year ranges matching woonwerk periods for consistency
  # 2007-2012 and 2012-2017
  ODIN_PERIODS <- list(
    list(label = "20072012", years = 2007:2012),
    list(label = "20122017", years = 2012:2017)
  )

  # ── Helper: aggregate ODiN flows ────────────────────────────
  # n_years: number of years in period (for daily average division)
  aggregate_odin <- function(con, origin_col, dest_col,
                              year_range, motief_filter = NULL,
                              modus_filter = NULL) {
    motief_cond <- if (!is.null(motief_filter))
      paste("AND c_motief IN (", paste(motief_filter, collapse = ","), ")")
    else ""
    modus_cond  <- if (!is.null(modus_filter))
      paste("AND c_modus IN (", paste(modus_filter, collapse = ","), ")")
    else ""

    year_list <- paste(year_range, collapse = ",")
    n_years   <- length(year_range)

    sql <- sprintf("
      SELECT %s AS origin_id,
             %s AS destination_id,
             SUM(factorv) / (%d * 365.0) AS flow_value
      FROM ovin20042022
      WHERE year IN (%s)
      AND %s IS NOT NULL
      AND %s IS NOT NULL
      %s %s
      GROUP BY %s, %s
      ORDER BY flow_value DESC
    ", origin_col, dest_col,
       n_years,
       year_list,
       origin_col, dest_col,
       motief_cond, modus_cond,
       origin_col, dest_col)

    dbGetQuery(con, sql)
  }

  # ── All trips (gemeente level) ───────────────────────────────
  odin_gem_all <- lapply(ODIN_PERIODS, function(p) {
    rows <- aggregate_odin(con_odin, "c_vgemf", "c_agemf", p$years)
    rows$periode <- p$label
    rows
  }) |> bind_rows() |>
    mutate(
      origin_id      = format_gm(origin_id),
      destination_id = format_gm(destination_id),
      flow_value     = round(flow_value, 1),
      variable_name  = "odin_all"
    ) |>
    filter(flow_value > 0)

  # ── Work trips only (motief = 1) ─────────────────────────────
  odin_gem_work <- lapply(ODIN_PERIODS, function(p) {
    rows <- aggregate_odin(con_odin, "c_vgemf", "c_agemf",
                           p$years, motief_filter = 1)
    rows$periode <- p$label
    rows
  }) |> bind_rows() |>
    mutate(
      origin_id      = format_gm(origin_id),
      destination_id = format_gm(destination_id),
      flow_value     = round(flow_value, 1),
      variable_name  = "odin_work"
    ) |>
    filter(flow_value > 0)

  # ── All trips (PC4 level), filtered to study area ────────────
  odin_pc4_all <- lapply(ODIN_PERIODS, function(p) {
    rows <- aggregate_odin(con_odin, "c_vpcf", "c_apcf", p$years)
    rows$periode <- p$label
    rows
  }) |> bind_rows() |>
    mutate(
      origin_id      = as.integer(origin_id),
      destination_id = as.integer(destination_id),
      flow_value     = round(flow_value, 1),
      variable_name  = "odin_all"
    ) |>
    filter(
      flow_value > 0,
      origin_id %in% pc4_study | destination_id %in% pc4_study
    )

  # ── Summaries ─────────────────────────────────────────────────
  make_summary <- function(df) {
    df |>
      group_by(origin_id, periode) |>
      summarise(
        total_outflow  = sum(flow_value),
        n_destinations = n_distinct(destination_id),
        .groups = "drop"
      )
  }

  write_parquet(odin_gem_all,
                file.path(EXPORT_DIR, "edges_odin_all_gem.parquet"))
  write_parquet(make_summary(odin_gem_all),
                file.path(EXPORT_DIR, "edges_odin_all_summary_gem.parquet"))
  write_parquet(odin_gem_work,
                file.path(EXPORT_DIR, "edges_odin_work_gem.parquet"))
  write_parquet(make_summary(odin_gem_work),
                file.path(EXPORT_DIR, "edges_odin_work_summary_gem.parquet"))
  write_parquet(odin_pc4_all,
                file.path(EXPORT_DIR, "edges_odin_all_pc4.parquet"))
  write_parquet(make_summary(odin_pc4_all),
                file.path(EXPORT_DIR, "edges_odin_all_summary_pc4.parquet"))

  dbDisconnect(con_odin)
  message("  ODiN exports complete")
  message("  Gemeente all trips:  ", nrow(odin_gem_all), " rows")
  message("  Gemeente work trips: ", nrow(odin_gem_work), " rows")
  message("  PC4 all trips:       ", nrow(odin_pc4_all), " rows")

} else {
  message("  ODiN SQLite not found at: ", ODIN_DB)
  message("  Skipping ODiN processing — update ODIN_DB path when data arrives")
  message("  Expected file: edges-ovin-2022.sqlite")
  message("  Pipeline is ready — just point ODIN_DB at the correct path and re-run")
}

# ── SECTION 13: Supplementary PC4 stats for model (outer-area flows) ──────────
message("\n=== Section 13: Supplementary PC4 stats ===")

edges_all <- read_parquet(file.path(EXPORT_DIR, "edges_woonwerk_pc4.parquet"))
pc4_stats_existing <- read_parquet(file.path(EXPORT_DIR, "pc4_zh_2024_stats.parquet"))

all_pcs  <- unique(c(edges_all$origin_id, edges_all$destination_id))
missing  <- all_pcs[!all_pcs %in% pc4_stats_existing$postcode]
message("  Missing PC4s to supplement: ", length(missing))

pc4_gpkg <- list.files(
  file.path(local_data_dir, "raw/pc4/extracted"),
  pattern = "\\.gpkg$", full.names = TRUE, recursive = TRUE)[1]
pc4_nl_full <- st_read(pc4_gpkg, quiet = TRUE)

pc4_missing <- pc4_nl_full |>
  filter(postcode %in% missing) |>
  st_drop_geometry()

pc4_missing_clean <- drop_suppressed(pc4_missing, label = "pc4_supplementary")

shared_cols <- intersect(names(pc4_missing_clean), names(pc4_stats_existing))
pc4_missing_clean <- pc4_missing_clean |> select(all_of(shared_cols))

safe_write_parquet(pc4_missing_clean,
  file.path(EXPORT_DIR, "pc4_supplementary_stats.parquet"))
message("  Exported: ", nrow(pc4_missing_clean), " rows")

# ── Final summary ──────────────────────────────────────────────────────────────
message("\n========================================")
message("=== SECTIONS 8-12 COMPLETE ===")
message("========================================")
message("\nNew files exported to: ", EXPORT_DIR)
message("\nPC4 node files:")
message("  nodes_summary_pc4.parquet         — totals + ratios, all years")
message("  nodes_demo_totaal_pc4             — total population")
message("  nodes_demo_leeftijd_pc4           — by age")
message("  nodes_demo_inkomen_pc4            — by income")
message("  nodes_demo_opleiding_pc4          — by education")
message("  nodes_demo_sec_pc4               — by socioeconomic position")
message("  nodes_banen_werk_totaal_pc4       — jobs at work location")
message("  nodes_banen_werk_leeftijd/inkomen/opleiding_pc4")
message("  nodes_banen_woon_totaal_pc4       — employed residents")
message("  nodes_banen_woon_leeftijd/inkomen/opleiding_pc4")
message("\nPC4 edge files:")
message("  edges_woonwerk_pc4.parquet + summary")
message("  edges_werkwerk_pc4.parquet + summary")
message("  edges_migration_pc4.parquet + summary")
message("  pc4_centroids.json")
message("\nODiN (when data available):")
message("  edges_odin_all_gem.parquet + summary")
message("  edges_odin_work_gem.parquet + summary")
message("  edges_odin_all_pc4.parquet + summary")
message("\nNext: update config.ts EDGE_DATASETS and VARIABLES to include PC4 edges")
message("      and nodes_summary_pc4 variables")