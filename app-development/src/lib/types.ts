// =============================================================================
// types.ts
// TypeScript interfaces for the NPRZ spatial analysis app
// =============================================================================

export interface Scale {
  key: string;
  label: string;
  geojson: string;           // filename in static/data/
  stats: string;             // filename in static/data/
  id: string;                // feature ID column (used by MapLibre promoteId)
  type: 'point' | 'polygon';
  pointSize?: number;        // metres, only for point/grid layers
}

export interface Variable {
  key: string;               // column name in stats parquet
  label: string;             // human-readable display name
  group: string;             // optgroup label in dropdown
  canNormalise: boolean;     // whether per-km² / per-1000 makes sense
  availableAt: string[];     // scale keys where this variable exists
  unit?: string;             // optional unit label e.g. '€', '%'
}

export interface ComputedVariable {
  key: string;               // generated unique key
  label: string;             // user-supplied display name
  expression: string;        // e.g. "aantal_woningen / aantal_inwoners"
  availableAt: string[];     // inherited from the variables used
}

export interface EdgeDataset {
  key: string;
  label: string;
  flows: string;             // parquet filename: origin_id, destination_id, flow_value
  flowSummary: string;       // parquet filename: origin_id, total_outflow, n_destinations
  scaleKey: string;          // which spatial scale the IDs belong to
}

export type DisplayMode = 'both' | 'inner' | 'outer';
export type Normalisation = 'none' | 'per_km2' | 'per_1000';
