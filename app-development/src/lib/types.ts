// =============================================================================
// types.ts
// =============================================================================

export interface Scale {
  key: string;
  label: string;
  geojson: string;
  stats: string;
  id: string;                // feature ID column for MapLibre promoteId
  type: 'point' | 'polygon';
  pointSize?: number;        // metres, grid layers only
}

export interface Variable {
  key: string;               // logical key, stable across scales
  label: string;
  group: string;
  canNormalise: boolean;
  availableAt: string[];     // scale keys where this variable exists
  // Maps scale key → actual parquet column name.
  // CBS uses different column names for the same concept at different scales.
  // Falls back to `key` if a scale is absent from this map.
  columnAt?: Record<string, string>;
  unit?: string;
  source?: 'nodes' | 'nodes_pc4' | 'nodes_ink' | 'nodes_opl' | 'nodes_ink_pc4' | 'nodes_opl_pc4' | 'flow_summary';
}

export interface EdgeDataset {
  key:           string;
  label:         string;
  description:   string;
  flows:         string;      // parquet filename
  flowSummary:   string;      // parquet filename
  scaleKey:      string;
  colour:        string;      // hex
  periods:       string[];
  periodLabels:  Record<string, string>;
  defaultPeriod: string;
  idCols: {
    origin:      string;
    destination: string;
    period:      string;
  };
  hasBreakdown?: boolean; // supports income/education dimension filtering
}

// inner = inner scales only (100m/500m/buurt)
// outer = outer scales only (pc4/wijk/gemeente)
// both  = one inner + one outer, same variable, same colour ramp
export type SpatialExtent = 'inner' | 'outer' | 'both';

export type Normalisation = 'none' | 'per_km2' | 'per_1000';
export type CalcOperator  = '+' | '-' | '*' | '/';

export interface ActiveEdgeLayer {
  datasetKey:    string;
  period:        string;
  visible:       boolean;
  // Optional dimension filters — null means no filter (all values)
  inkFilter:     string | null;  // income category: '1'–'5' or null
  oplFilter:     string | null;  // education category: '1'–'3' or null
  flowMin?:      number;         // populated after loading, for legend
  flowMax?:      number;
}

// Income and education label maps — shared across edge and node contexts
export const INK_LABELS: Record<string, string> = {
  '1': '< 20%', '2': '20–40%', '3': '40–60%', '4': '60–80%', '5': '80–100%',
};

export const OPL_LABELS: Record<string, string> = {
  '1': 'Laag', '2': 'Midden', '3': 'Hoog',
};