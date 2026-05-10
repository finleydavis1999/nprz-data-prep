# Verhuizingen: CBS microdata 1999-2018, residential moves OD edges.
# Source: raw-data/edges-migration-2018.sqlite.
# Output: static/data/parquet/migration-edges-{gem,pc4}.parquet
source("R/lib/parquet.R")

build_migration <- function() {
  con <- duckdb_with_sqlite("raw-data/edges-migration-2018.sqlite")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE))

  period_case <- "CASE year
      WHEN 'p99-02' THEN 1
      WHEN 'p03-06' THEN 2
      WHEN 'p07-10' THEN 3
      WHEN 'p11-14' THEN 4
      WHEN 'p15-18' THEN 5
      WHEN 'p99-08' THEN 6
      WHEN 'p09-18' THEN 7
    END"

  write_parquet_from_query(con, sprintf("
    SELECT
      'GM' || printf('%%04d', CAST(gemPre  AS INTEGER)) AS o_code,
      'GM' || printf('%%04d', CAST(gemPost AS INTEGER)) AS d_code,
      %s AS year,
      CAST(age  AS INTEGER) AS age,
      CAST(hh   AS INTEGER) AS hh,
      CAST(inks AS INTEGER) AS inks,
      CAST(opl  AS INTEGER) AS opl,
      CAST(sec  AS INTEGER) AS sec,
      SUM(value)::DOUBLE AS count,
      SUM(value)::DOUBLE AS weight
    FROM src.verhuizingen_19992018_gem
    WHERE gemPre IS NOT NULL AND gemPost IS NOT NULL AND year IN
      ('p99-02','p03-06','p07-10','p11-14','p15-18','p99-08','p09-18')
    GROUP BY o_code, d_code, year, age, hh, inks, opl, sec
    ORDER BY year, o_code, d_code
  ", period_case), "static/data/parquet/migration-edges-gem.parquet")

  write_parquet_from_query(con, sprintf("
    SELECT
      printf('%%04d', CAST(pcPre  AS INTEGER)) AS o_code,
      printf('%%04d', CAST(pcPost AS INTEGER)) AS d_code,
      %s AS year,
      CAST(age  AS INTEGER) AS age,
      CAST(hh   AS INTEGER) AS hh,
      CAST(inks AS INTEGER) AS inks,
      CAST(opl  AS INTEGER) AS opl,
      CAST(sec  AS INTEGER) AS sec,
      SUM(value)::DOUBLE AS count,
      SUM(value)::DOUBLE AS weight
    FROM src.verhuizingen_19992018_pc
    WHERE pcPre IS NOT NULL AND pcPost IS NOT NULL AND year IN
      ('p99-02','p03-06','p07-10','p11-14','p15-18','p99-08','p09-18')
    GROUP BY o_code, d_code, year, age, hh, inks, opl, sec
    ORDER BY year, o_code, d_code
  ", period_case), "static/data/parquet/migration-edges-pc4.parquet")

  list(
    name        = "Verhuizingen 1999-2018",
    description = "CBS microdata, residential moves between successive observed addresses. Cells <10 suppressed for privacy.",
    scales = list(
      gem = "parquet/migration-edges-gem.parquet",
      pc4 = "parquet/migration-edges-pc4.parquet"
    ),
    fields = list(
      year = list(
        type = "multi", label = "Periode",
        values = list(
          list(id = 1L, label = "1999-2002"),
          list(id = 2L, label = "2003-2006"),
          list(id = 3L, label = "2007-2010"),
          list(id = 4L, label = "2011-2014"),
          list(id = 5L, label = "2015-2018"),
          list(id = 6L, label = "1999-2008"),
          list(id = 7L, label = "2009-2018")
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
      hh = list(
        type = "multi", label = "Huishouden",
        values = list(
          list(id = 1L, label = "Alleenstaand"),
          list(id = 2L, label = "Paar zonder kinderen"),
          list(id = 3L, label = "Paar met kinderen"),
          list(id = 4L, label = "Eenouder"),
          list(id = 5L, label = "Overig")
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
      )
    )
  )
}
