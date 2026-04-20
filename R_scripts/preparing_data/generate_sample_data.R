# =============================================================================
# generate_sample_data.R
# Creates small toy datasets for local development / testing
# Run once to populate data/sample/
# Output: data/sample/commuting_rijnmond_mini.csv
# =============================================================================
set.seed(42)
library(dplyr)
SAMPLE_DIR <- "data/sample"
if (!dir.exists(SAMPLE_DIR)) dir.create(SAMPLE_DIR, recursive = TRUE)
# Realistic Rijnmond buurt codes (Rotterdam GM0599)
buurt_codes <- c(
  "BU05990000", "BU05990001", "BU05990002", "BU05990003", "BU05990004",
  "BU05180000", "BU05180001", "BU05180002", "BU05180003", "BU05180004",
  "BU05990005", "BU05990006", "BU05990007", "BU05990008", "BU05990009"
)
flows <- expand.grid(
  origin_id      = sample(buurt_codes, 10),
  destination_id = sample(buurt_codes, 10),
  stringsAsFactors = FALSE
) |>
  filter(origin_id != destination_id) |>
  slice_sample(n = 100) |>
  mutate(
    flow_value    = sample(10:500, n(), replace = TRUE),
    variable_name = "commuters_2024",
    year          = 2024L
  )
write.csv(flows, file.path(SAMPLE_DIR, "commuting_rijnmond_mini.csv"),
          row.names = FALSE)
message(sprintf("✓ %d toy flows → %s/commuting_rijnmond_mini.csv",
                nrow(flows), SAMPLE_DIR))
