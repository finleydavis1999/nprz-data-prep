# NPRZ — Interactive Spatial Data Explorer for Urban Analysis & Modelling

An end-to-end platform for exploring Dutch CBS administrative and grid data at multiple spatial scales, enabling urban planners, researchers, and analysts to perform dynamic spatial analysis and demographic modelling.

## Project Goal

Develop a web application that enables users to:
- **Load CBS spatial data** at different administrative scales (100m grids, neighbourhoods, districts, municipalities, postcodes)
- **Dynamically analyze** 34+ demographic, housing, and socioeconomic variables
- **Visualize choropleth maps** with automatic quantile-based classification
- **Export datasets** for further analysis in GIS or modelling tools
- **Support reproducible workflows** from data preparation to web deployment

---

## Quick Start (3 minutes)

### Prerequisites
- **R** ≥ 4.0 (if regenerating data)
- **Node.js** ≥ 18

### 1. View Pre-built App (fastest)
```bash
cd app-development
npm install
npm run dev
# Open http://localhost:5173
```

The app loads pre-generated parquet files from `app-development/static/data/`. No R setup needed!

### 2. Regenerate Data (advanced)
```bash
cd R_scripts/preparing-data
# Open 01_setup_and_process.R in R/RStudio and run it
# Outputs: data/export/*.parquet (6 GeoParquet files)
```

See [R_scripts/preparing-data/README-data-preparation.md](R_scripts/preparing-data/README-data-preparation.md) for detailed pipeline documentation.

---

## Project Structure

```
NPRZ/
├── README.md                                  # Project overview (this file)
├── .gitignore                                 # Git ignore rules
│
├── app-development/                           # 🎨 SvelteKit Web Application
│   ├── README-app-development.md              # App setup & development guide
│   ├── src/routes/+page.svelte                # Interactive map & dashboard
│   ├── static/data/*.parquet                  # Pre-loaded parquet files
│   └── [SvelteKit config files]
│
├── R_scripts/                                 # 📊 Data Preparation Pipeline
│   └── preparing-data/
│       ├── README-data-preparation.md         # Detailed pipeline docs
│       └── 01_setup_and_process.R             # Main script (RUN THIS)
│
└── data/                                      # 💾 Data Outputs
    ├── raw/                                   # Downloaded, not committed
    ├── processed/                             # Intermediate, not committed
    └── export/                                # ✅ COMMITTED outputs
        ├── grid_100m_rijnmond.parquet
        ├── grid_500m_rijnmond.parquet
        ├── buurt_2024.parquet
        ├── wijk_2024.parquet
        ├── gemeente_2024.parquet
        └── pc4_zh_2024.parquet
```

---

## What's Included

### 🌐 Web App (`app-development/`)
- **Interactive choropleth map** powered by MapLibre GL
- **34 variables** across demographic, housing, and socioeconomic categories
- **Dynamic variable switching** with automatic color classification
- **Client-side processing** via DuckDB-WASM (no server needed)
- **Quantile-based legend** that updates with each variable

### 📈 Data Pipeline (`R_scripts/preparing-data/`)
- **Unified 10-section R script** that orchestrates the entire pipeline
- **Downloads CBS data** (grids, geometries, kerncijfers statistics)
- **Spatial filtering** to Rijnmond region (13 municipalities)
- **Multi-scale outputs** from 100m grids to national municipalities
- **Automatic data cleaning** (handles suppressed values, missing data)
- **GeoParquet export** with embedded WKT geometry

### 💾 Prepared Datasets
6 GeoParquet files ready for web consumption:

| File | Scale | Region | Rows | Use Case |
|------|-------|--------|------|----------|
| `grid_100m_rijnmond.parquet` | 100m² | Rijnmond | ~6,000 | Fine-grained analysis |
| `grid_500m_rijnmond.parquet` | 500m² | Rijnmond | ~250 | Regional overview |
| `buurt_2024.parquet` | Neighbourhood | Rijnmond | ~300 | Local planning |
| `wijk_2024.parquet` | District | All NL | ~3,500 | Strategic analysis |
| `gemeente_2024.parquet` | Municipality | All NL | ~344 | Policy-level |
| `pc4_zh_2024.parquet` | Postcode | Zuid-Holland | ~1,500 | Postal data |

---

## Key Features

✅ **Fully Reproducible** — One-click R script generates all outputs  
✅ **Multi-Scale Data** — From 100m grids to national coverage  
✅ **Web-Ready Format** — GeoParquet with embedded geometry  
✅ **Client-Side Processing** — DuckDB-WASM queries in the browser  
✅ **Pre-Loaded Data** — App includes parquet files, no setup needed  
✅ **Modern Stack** — SvelteKit + TypeScript + MapLibre GL  
✅ **Production Deployable** — Static site, works on any host  

---

## Data Sources

All data is **© Statistics Netherlands (CBS)** — attribution required in derived products.

- **Grid Data (100m, 500m, PC4):** https://download.cbs.nl/
- **Administrative Geometries:** CBS Wijkenbuurt 2024 v2
- **Kerncijfers (Statistics):** CBS OData dataset 85984NED
- **Province/Municipality Mapping:** CBS administrative code system

---

## Workflow

```
1. R Script (01_setup_and_process.R)
   ↓
2. Download CBS Data
   ↓
3. Spatial Processing & Filtering
   ↓
4. Generate GeoParquet Files (data/export/)
   ↓
5. Copy to App (app-development/static/data/)
   ↓
6. Run Web App (npm run dev)
   ↓
7. Explore & Analyze
```

---

## Usage Examples

### Exploring the Map
1. Open http://localhost:5173
2. Select a variable from the dropdown (e.g., "Total population")
3. Map updates with color-coded quantile classes
4. Legend shows break values automatically
5. Switch variables to see different patterns

### For Urban Planners
Export data by scale and variable, import into ArcGIS or QGIS for further analysis.

### For Researchers
Use pre-generated datasets for demographic or socioeconomic studies. SQL-query the parquets via DuckDB for custom aggregations.

### For Developers
Modify `+page.svelte` to add new layers, styling, or interactivity. All code is TypeScript with full IDE support.

---

## Next Steps

### Short Term
- [ ] Test all 34 variables in the app
- [ ] Verify data loads correctly on localhost
- [ ] Test layer switching (if implemented)

### Medium Term
- [ ] Deploy to Vercel or Netlify
- [ ] Add layer switcher for different scales
- [ ] Implement data export (CSV, GeoJSON)
- [ ] Add custom color scheme selector

### Long Term
- [ ] Extend to other provinces/all Netherlands
- [ ] Add multi-year temporal analysis (2022–2025)
- [ ] Integrate additional datasets (environmental, economic)
- [ ] Build backend for user-saved analyses

---

## Troubleshooting

**App won't start?**
```bash
cd app-development
npm install
npm run dev
```

**Parquet files not loading?**
- Ensure `app-development/static/data/*.parquet` files exist
- Check browser console (F12) for DuckDB errors

**R script encounters errors?**
- See [R_scripts/preparing-data/README-data-preparation.md](R_scripts/preparing-data/README-data-preparation.md) for troubleshooting
- Ensure R ≥ 4.0 and required packages are installed

---

## Project History

- **Phase 1** ✅ Data pipeline & preparation (complete)
- **Phase 2** 🚀 Interactive web app (current)
- **Phase 3** 📋 Scale & extend (planned)

---

## License

**Data:** © Statistics Netherlands (CBS)  
**Code:** [Add your preferred license here]

---

## Questions?

- **Data Pipeline:** [R_scripts/preparing-data/README-data-preparation.md](R_scripts/preparing-data/README-data-preparation.md)
- **App Development:** [app-development/README-app-development.md](app-development/README-app-development.md)
- **GitHub Repository:** https://github.com/finleydavis1999/nprz-data-prep

---

## Contributors

Built for urban analysis and planning research with CBS open data.