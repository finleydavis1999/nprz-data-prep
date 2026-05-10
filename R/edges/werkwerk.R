# Werk-Werk: CBS microdata 1999-2017, job → job moves OD edges.
# Source: raw-data/edges-werkwerk-2018.sqlite.
# Output: static/data/parquet/werkwerk-edges-{gem,pc4}.parquet
source("R/lib/parquet.R")

build_werkwerk <- function() {
  con <- duckdb_with_sqlite("raw-data/edges-werkwerk-2018.sqlite")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  period_case <- "CASE year
      WHEN '07-12' THEN 1
      WHEN '12-17' THEN 2
      WHEN '07-17' THEN 3
    END"

  write_parquet_from_query(con, sprintf("
    SELECT
      'GM' || printf('%%04d', CAST(GEMy1 AS INTEGER)) AS o_code,
      'GM' || printf('%%04d', CAST(GEMy2 AS INTEGER)) AS d_code,
      %s AS year,
      CAST(age      AS INTEGER) AS age,
      CAST(inks     AS INTEGER) AS inks,
      CAST(opl      AS INTEGER) AS opl,
      CAST(soortbaan AS INTEGER) AS soortbaan,
      SUM(value)::DOUBLE AS count,
      SUM(value)::DOUBLE AS weight
    FROM src.werkwerk_19992018_gem
    WHERE GEMy1 IS NOT NULL AND GEMy2 IS NOT NULL AND year IN ('07-12','12-17','07-17')
    GROUP BY o_code, d_code, year, age, inks, opl, soortbaan
    ORDER BY year, o_code, d_code
  ", period_case), "static/data/parquet/werkwerk-edges-gem.parquet")

  write_parquet_from_query(con, sprintf("
    SELECT
      printf('%%04d', CAST(POSTCODEy1 AS INTEGER)) AS o_code,
      printf('%%04d', CAST(POSTCODEy2 AS INTEGER)) AS d_code,
      %s AS year,
      CAST(age      AS INTEGER) AS age,
      CAST(inks     AS INTEGER) AS inks,
      CAST(opl      AS INTEGER) AS opl,
      CAST(soortbaan AS INTEGER) AS soortbaan,
      SUM(value)::DOUBLE AS count,
      SUM(value)::DOUBLE AS weight
    FROM src.werkwerk_19992018_pc
    WHERE POSTCODEy1 IS NOT NULL AND POSTCODEy2 IS NOT NULL AND year IN ('07-12','12-17','07-17')
    GROUP BY o_code, d_code, year, age, inks, opl, soortbaan
    ORDER BY year, o_code, d_code
  ", period_case), "static/data/parquet/werkwerk-edges-pc4.parquet")

  list(
    name        = "Baanverhuizingen 1999-2017",
    description = "CBS microdata, job → job moves between successive observed employments. Cells <10 suppressed for privacy.",
    scales = list(
      gem = "parquet/werkwerk-edges-gem.parquet",
      pc4 = "parquet/werkwerk-edges-pc4.parquet"
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
