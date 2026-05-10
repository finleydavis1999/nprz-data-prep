# Helpers for materializing SQL query results to parquet via DuckDB's COPY.
suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
})

# Open a DuckDB in-memory connection with a SQLite file ATTACHed read-only.
duckdb_with_sqlite <- function(sqlite_path, alias = "src") {
  con <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbExecute(con, "INSTALL sqlite; LOAD sqlite;")
  DBI::dbExecute(
    con,
    sprintf("ATTACH '%s' AS %s (TYPE sqlite, READ_ONLY)", sqlite_path, alias)
  )
  con
}

# Stream a SELECT directly to parquet (zstd, sorted via the SELECT's ORDER BY).
write_parquet_from_query <- function(con, sql, out_path, row_group_size = 100000L) {
  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
  copy_sql <- sprintf(
    "COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3, ROW_GROUP_SIZE %d)",
    sql, out_path, row_group_size
  )
  DBI::dbExecute(con, copy_sql)
  cat("wrote", out_path, "(", file.size(out_path) %/% 1024, "KB )\n")
  invisible(out_path)
}
