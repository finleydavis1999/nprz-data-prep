# Woon-Werk: CBS microdata 1999-2017, home → work commute OD edges.
# Source: raw-data/edges-woonwerk-2018.sqlite, tables woonwerk_19992018_{gem,pc}.
# Output: static/data/parquet/woonwerk-edges-{gem,pc4}.parquet
#
# `year` is a *period* (e.g., 20072012) not a calendar year — encoded as a
# small integer id so the existing multi-select filter UI handles it cleanly.
source("R/lib/parquet.R")

build_woonwerk <- function() {
  con <- duckdb_with_sqlite("raw-data/edges-woonwerk-2018.sqlite")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  # Period encoding (must match the manifest entry below).
  period_case <- "CASE year
      WHEN 20072012 THEN 1
      WHEN 20122017 THEN 2
      WHEN 20072017 THEN 3
    END"

  write_parquet_from_query(con, sprintf("
    SELECT
      'GM' || printf('%%04d', CAST(woongem AS INTEGER)) AS o_code,
      'GM' || printf('%%04d', CAST(werkgem AS INTEGER)) AS d_code,
      %s AS year,
      CAST(age      AS INTEGER) AS age,
      CAST(inks     AS INTEGER) AS inks,
      CAST(opl      AS INTEGER) AS opl,
      CAST(sectorcat AS INTEGER) AS sectorcat,
      CAST(soortbaan AS INTEGER) AS soortbaan,
      SUM(value)::DOUBLE AS count,
      SUM(value)::DOUBLE AS weight
    FROM src.woonwerk_19992018_gem
    WHERE woongem IS NOT NULL AND werkgem IS NOT NULL AND year IN (20072012, 20122017, 20072017)
    GROUP BY o_code, d_code, year, age, inks, opl, sectorcat, soortbaan
    ORDER BY year, o_code, d_code
  ", period_case), "static/data/parquet/woonwerk-edges-gem.parquet")

  write_parquet_from_query(con, sprintf("
    SELECT
      printf('%%04d', CAST(woonpostcode AS INTEGER)) AS o_code,
      printf('%%04d', CAST(werkpostcode AS INTEGER)) AS d_code,
      %s AS year,
      CAST(age      AS INTEGER) AS age,
      CAST(inks     AS INTEGER) AS inks,
      CAST(opl      AS INTEGER) AS opl,
      CAST(sectorcat AS INTEGER) AS sectorcat,
      CAST(soortbaan AS INTEGER) AS soortbaan,
      SUM(value)::DOUBLE AS count,
      SUM(value)::DOUBLE AS weight
    FROM src.woonwerk_19992018_pc
    WHERE woonpostcode IS NOT NULL AND werkpostcode IS NOT NULL AND year IN (20072012, 20122017, 20072017)
    GROUP BY o_code, d_code, year, age, inks, opl, sectorcat, soortbaan
    ORDER BY year, o_code, d_code
  ", period_case), "static/data/parquet/woonwerk-edges-pc4.parquet")

  list(
    name        = "Woon-Werk 1999-2017",
    description = "CBS microdata, home → work commute flows aggregated to herkomst-bestemming. Cells <10 suppressed for privacy.",
    scales = list(
      gem = "parquet/woonwerk-edges-gem.parquet",
      pc4 = "parquet/woonwerk-edges-pc4.parquet"
    ),
    fields = list(
      year = list(
        type = "multi", label = "Periode",
        values = list(
          list(id = 1L, label = "2007-2012"),
          list(id = 2L, label = "2012-2017"),
          list(id = 3L, label = "2007-2017")
        )
      ),
      age = list(
        type = "multi", label = "Leeftijd",
        values = list(
          list(id = 1L, label = "< 18"),
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
          list(id = 1L, label = "Laag"),
          list(id = 2L, label = "Midden"),
          list(id = 3L, label = "Hoog")
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
      soortbaan = list(
        type = "multi", label = "Soort baan",
        values = list(
          list(id = 1L, label = "DGA"),
          list(id = 2L, label = "Overig")
        )
      )
    )
  )
}
