# Adding a New Dataset
1. **Raw data** → `data/raw/` (never edit)
2. **Process in R** → follow pattern in `01_setup_and_process.R`:
   - Load + clip to Rijnmond boundary
   - `drop_suppressed()` to clean suppressed CBS values
   - `export_admin_scale()` to write GeoJSON + Parquet
3. **Files land in** `app-development/static/data/` automatically
4. **Register in frontend** → add to `SCALES` and `VARIABLES` in `+page.svelte`
   (later: `app-development/src/lib/config.ts`)
5. **Test** → `npm run dev`, verify layer appears and data loads
