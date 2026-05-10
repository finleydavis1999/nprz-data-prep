# banen-werk: CBS baangegevens werkadres 2007-2017, PC4 + gemeente scales.
# Source: raw-data/nodes-2018.sqlite, tables woonwerk_werk_19992018_{pc,gem}.
# Output: static/data/parquet/banen-werk-{pc4,gem}.parquet
source("R/lib/parquet.R")

build_banen_werk <- function() {
  con <- duckdb_with_sqlite("raw-data/nodes-2018.sqlite")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  write_parquet_from_query(con, "
    SELECT
      printf('%04d', pc4) AS area_code,
      CAST(year AS INTEGER) AS year,
      age, opl, inks, sectorcat, soortbaan,
      value AS count,
      value AS weight
    FROM src.woonwerk_werk_19992018_pc
    ORDER BY year, age, opl, inks, sectorcat, soortbaan
  ", "static/data/parquet/banen-werk-pc4.parquet")

  write_parquet_from_query(con, "
    SELECT
      'GM' || printf('%04d', gem) AS area_code,
      CAST(year AS INTEGER) AS year,
      age, opl, inks, sectorcat, soortbaan,
      value AS count,
      value AS weight
    FROM src.woonwerk_werk_19992018_gem
    ORDER BY year, age, opl, inks, sectorcat, soortbaan
  ", "static/data/parquet/banen-werk-gem.parquet")

  list(
    name        = "Baangegevens Werkadres",
    description = "CBS microdata 2007-2017, banen geteld op werkadres. Cellen <10 onderdrukt voor privacy.",
    scales = list(
      pc4 = "parquet/banen-werk-pc4.parquet",
      gem = "parquet/banen-werk-gem.parquet"
    ),
    fields = list(
      year = list(
        type = "single", label = "Jaar",
        values = lapply(2007:2017, function(y) list(id = y, label = as.character(y))),
        default = 2017L
      ),
      age = list(
        type = "multi", label = "Leeftijd",
        values = list(
          list(id = 1L, label = "jonger dan 18"),
          list(id = 2L, label = "18-23"),
          list(id = 3L, label = "24-29"),
          list(id = 4L, label = "30-40"),
          list(id = 5L, label = "40-59"),
          list(id = 6L, label = "60+")
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
      ),
      sectorcat = list(
        type = "multi", label = "Sector",
        values = list(
          list(id = 1L, label = "Materiaalgericht: Productie"),
          list(id = 2L, label = "Materiaalgericht: Dienstverlening"),
          list(id = 3L, label = "Informatiegericht: Commercieel"),
          list(id = 4L, label = "Informatiegericht: Publiek (Quartair)"),
          list(id = 5L, label = "Persoonsgericht: Retail, Ambacht, Horeca & Vervoer"),
          list(id = 6L, label = "Persoonsgericht: Zorg, Onderwijs, Cultuur"),
          list(id = 7L, label = "Landbouw"),
          list(id = 8L, label = "Metaal- en maritieme industrie"),
          list(id = 9L, label = "Overig (uitzend & onbekend)")
        )
      ),
      soortbaan = list(
        type = "multi", label = "Soort Baan",
        values = list(
          list(id = 1L, label = "DGA"),
          list(id = 2L, label = "Overig"),
          list(id = 3L, label = "Stagaire, WSW, Oproep, Uitzend")
        )
      )
    ),
    countCol  = "count",
    weightCol = "weight",
    defaultClassification = list(method = "jenks", n = 5L, palette = "YlOrRd")
  )
}
