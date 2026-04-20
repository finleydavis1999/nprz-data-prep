# Architecture
## Data Flow
## Key Concepts
- **Scale**: spatial layer (buurt, grid_100m, …). Registered in SCALES.
- **Variable**: column in a stats parquet. Registered in VARIABLES.
- **Computed Variable**: user arithmetic on variables (e.g. A / B). Phase 4.
- **Edge Dataset**: flows parquet (origin → destination → value). Phase 7.
- **Nodal Model**: per-feature result (e.g. Herfindahl index). Phase 5.
- **Spatial-Interaction Model**: per-flow result (e.g. gravity model). Phase 6.
