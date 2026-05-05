# ============================================================
# 02_add_edge_data.R
# Load and export commuting flow (edge) data
#
# Current source: NL_allcommutes_edited.xlsx
#   Columns used: source (GM code), sink (GM code), count
#
# Outputs → app-development/static/data/
#   flows.parquet        — origin, destination, flow_value
#   flow_summary.parquet — per-origin totals
#
# Display: gemeente-to-gemeente only (outer scale = gemeente)
# ============================================================

source("R_scripts/preparing_data/utils.R")

setwd("C:/NPRZ_project")

library(readxl)
library(arrow)
library(dplyr)
library(sf)

# ── Config ────────────────────────────────────────────────────
RAW_FILE       <- "C:/NPRZ_project/data/NL_allcommutes_edited.xlsx"
EXPORT_DIR     <- "C:/NPRZ_project/app-development/static/data"
GEMEENTE_STATS <- file.path(EXPORT_DIR, "gemeente_2024_stats.parquet")

# ── Load raw data ─────────────────────────────────────────────
message("Reading: ", basename(RAW_FILE))
raw <- read_excel(RAW_FILE) |>
  select(source, sink, count)

message("Raw rows: ", nrow(raw), " | Columns: ", paste(names(raw), collapse = ", "))
message("Sample source values: ", paste(head(raw$source, 3), collapse = ", "))

# ── Format GM codes ───────────────────────────────────────────
# Ensure codes are in GM0363 format (zero-padded to 4 digits, GM prefix)
# Handles: 363 → "GM0363", "363" → "GM0363", "GM0363" → "GM0363"
format_gm <- function(x) {
  x <- as.character(x)
  # If already has GM prefix, ensure zero-padded to GM + 4 digits
  already_gm <- grepl("^GM", x, ignore.case = TRUE)
  x[already_gm]  <- sprintf("GM%04d", as.integer(sub("GM", "", x[already_gm],
                                                       ignore.case = TRUE)))
  x[!already_gm] <- sprintf("GM%04d", as.integer(x[!already_gm]))
  x
}

flows <- raw |>
  mutate(
    origin_id      = format_gm(source),
    destination_id = format_gm(sink),
    flow_value     = as.numeric(count),
    variable_name  = "commuters",
    year           = 2022L       # update when known
  ) |>
  select(origin_id, destination_id, flow_value, variable_name, year)

message("After formatting — sample origin codes: ",
        paste(head(flows$origin_id, 3), collapse = ", "))

# ── Validate against gemeente spatial layer ───────────────────
# Check origin/destination codes exist in our gemeente stats file
if (file.exists(GEMEENTE_STATS)) {
  gemeente_ids <- read_parquet(GEMEENTE_STATS) |> pull(gemeentecode)
  message("Known gemeente codes: ", length(gemeente_ids))

  bad_origins <- sum(!flows$origin_id %in% gemeente_ids)
  bad_dests   <- sum(!flows$destination_id %in% gemeente_ids)

  if (bad_origins > 0)
    message("WARNING: ", bad_origins, " flows with unmatched origin codes")
  if (bad_dests > 0)
    message("WARNING: ", bad_dests, " flows with unmatched destination codes")
  if (bad_origins == 0 && bad_dests == 0)
    message("✓ All codes match gemeente spatial layer")
} else {
  message("NOTE: gemeente_2024_stats.parquet not found — skipping ID validation")
  message("      Run 01_setup_and_process.R first if needed")
}

# ── Validate schema ───────────────────────────────────────────
flows <- validate_flows(flows, label = "NL_allcommutes")

# ── Summary stats ─────────────────────────────────────────────
message("\nFlow summary:")
message("  Total flows:        ", nrow(flows))
message("  Unique origins:     ", n_distinct(flows$origin_id))
message("  Unique destinations:", n_distinct(flows$destination_id))
message("  Total commuters:    ", format(sum(flows$flow_value, na.rm = TRUE),
                                         big.mark = ","))
message("  Max single flow:    ", format(max(flows$flow_value, na.rm = TRUE),
                                         big.mark = ","))

# ── Per-origin summary ────────────────────────────────────────
flow_summary <- flows |>
  group_by(origin_id) |>
  summarise(
    total_outflow  = sum(flow_value, na.rm = TRUE),
    n_destinations = n_distinct(destination_id),
    .groups = "drop"
  ) |>
  arrange(desc(total_outflow))

message("\nTop 5 origins by outflow:")
print(head(flow_summary, 5))

# ── Export ────────────────────────────────────────────────────
if (!dir.exists(EXPORT_DIR)) dir.create(EXPORT_DIR, recursive = TRUE)

write_parquet(flows,        file.path(EXPORT_DIR, "flows.parquet"))
write_parquet(flow_summary, file.path(EXPORT_DIR, "flow_summary.parquet"))

message("\n✓ Exported:")
message("  flows.parquet        — ", nrow(flows), " rows")
message("  flow_summary.parquet — ", nrow(flow_summary), " rows")
message("  → ", EXPORT_DIR)

# ── Optional: Correct Rotterdam centroid ───────────────────────────────
library(jsonlite)
cents <- fromJSON("C:/NPRZ_project/app-development/static/data/gemeente_centroids.json")
cents$lng[cents$id == "GM0599"] <- 4.479448
cents$lat[cents$id == "GM0599"] <- 51.928931
write_json(cents,
           "C:/NPRZ_project/app-development/static/data/gemeente_centroids.json",
           dataframe = "rows")
message("Rotterdam centroid corrected")