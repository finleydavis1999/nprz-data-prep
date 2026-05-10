# demographics: CBS persoonsgegevens 1999-2018, PC4 + gemeente scales.
# Source: raw-data/nodes-2018.sqlite, tables demographics19992018_{pc,gem}.
# Output: static/data/parquet/demographics-{pc4,gem}.parquet
source("R/lib/parquet.R")

build_demographics <- function() {
  con <- duckdb_with_sqlite("raw-data/nodes-2018.sqlite")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  # PC4: pad pc4 to 4-digit string; cast year REAL -> INTEGER.
  write_parquet_from_query(con, "
    SELECT
      printf('%04d', pc4) AS area_code,
      CAST(year AS INTEGER) AS year,
      age, sec, hh, opl, inks,
      value AS count,
      value AS weight
    FROM src.demographics19992018_pc
    ORDER BY year, age, opl, inks
  ", "static/data/parquet/demographics-pc4.parquet")

  # Gemeente: format integer gem code as 'GM0014' to match CBS gpkg statcode.
  write_parquet_from_query(con, "
    SELECT
      'GM' || printf('%04d', gem) AS area_code,
      CAST(year AS INTEGER) AS year,
      age, sec, hh, opl, inks,
      value AS count,
      value AS weight
    FROM src.demographics19992018_gem
    ORDER BY year, age, opl, inks
  ", "static/data/parquet/demographics-gem.parquet")

  list(
    name        = "Persoonsgegevens 2018",
    description = "CBS microdata 1999-2018, demographic person attributes. Cells <10 suppressed for privacy.",
    scales = list(
      pc4 = "parquet/demographics-pc4.parquet",
      gem = "parquet/demographics-gem.parquet"
    ),
    fields = list(
      year = list(
        type = "single", label = "Jaar",
        values = lapply(1999:2018, function(y) list(id = y, label = as.character(y))),
        default = 2018L,
        min = 1999L, max = 2018L
      ),
      age = list(
        type = "multi", label = "Leeftijd",
        values = list(
          list(id = 1L, label = "12-18"),
          list(id = 2L, label = "18-23"),
          list(id = 3L, label = "24-29"),
          list(id = 4L, label = "30-40"),
          list(id = 5L, label = "40-59"),
          list(id = 6L, label = "60+")
        )
      ),
      sec = list(
        type = "multi", label = "Sociaal-Economische Positie",
        values = list(
          list(id = 1L, label = "Actief (werkend, DGA, zelfstand of overig actief)"),
          list(id = 2L, label = "Ontvanger uitkering (excl. pensioen)"),
          list(id = 3L, label = "Pensioen"),
          list(id = 4L, label = "Scholier/student")
        )
      ),
      hh = list(
        type = "multi", label = "Huishoudtype",
        values = list(
          list(id = 1L, label = "Eenpersoonshuishouden"),
          list(id = 2L, label = "Paar zonder kinderen"),
          list(id = 3L, label = "1-ouder met kinderen"),
          list(id = 4L, label = "Overig")
        )
      ),
      opl = list(
        type = "multi", label = "Opleiding",
        values = list(
          list(id = 1L, label = "Laag (SOI2006 < 40)"),
          list(id = 2L, label = "Midden (SOI2006 39-50)"),
          list(id = 3L, label = "Hoog (SOI2006 > 60)")
        )
      ),
      inks = list(
        type = "multi", label = "Brutoinkomen",
        values = list(
          list(id = 1L, label = "< 20%"),
          list(id = 2L, label = "20-40%"),
          list(id = 3L, label = "40-60%"),
          list(id = 4L, label = "60-80%"),
          list(id = 5L, label = "80-100%")
        )
      )
    ),
    countCol  = "count",
    weightCol = "weight",
    # Single-year selection only — no cross-year aggregation, so divisor = 1.
    yearAggregation = "sum",
    defaultClassification = list(method = "jenks", n = 5L, palette = "YlOrRd")
  )
}
