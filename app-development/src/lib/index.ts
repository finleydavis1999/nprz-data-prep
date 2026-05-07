// =============================================================================
// index.ts — barrel re-export for src/lib
//
// Allows imports like: import { fmt, classify } from '$lib'
// instead of: import { fmt } from '$lib/format'; import { classify } from '$lib/classify'
//
// Only re-export the public API of each module — internal helpers stay private.
// =============================================================================

// Core data types
export type {
  Scale, Variable, EdgeDataset,
  SpatialExtent, Normalisation, CalcOperator,
  ActiveEdgeLayer,
  INK_LABELS, OPL_LABELS,
} from './types';

// App configuration (scales, variables, datasets, colour ramps)
export {
  INNER_SCALES, OUTER_SCALES, ALL_SCALES,
  VARIABLES, EDGE_DATASETS,
  COLOURS_BLUE, COLOURS_GREEN, COLOURS_ORANGE, NO_DATA,
  NORMALISATIONS, CALC_OPERATORS,
  INNER_GEMEENTE_CODES, INNER_GM_NUMS, INNER_PC4_CODES,
  findScale, colForScale, varsForScale, sharedScales, scaleChips, isInnerScale,
} from './config';

// Statistical classification
export { quantileBreaks, classify } from './classify';

// Display formatting
export { fmt, groupedVars } from './format';

// DuckDB data access
export { initDuckDB, queuedQuery, dataURL, fetchRows, fetchMap, normSQL } from './db';

// MapLibre map management
export {
  map,
  initMap, colourExpr,
  setLayerVis, setColours,
  clipToInnerBoundary, clearBoundaryClip, clipOuterToExcludeInner,
} from './map';

// Tab-specific logic
export { applyScale, renderNodal }       from './nodal';
export { applyCalc, calcSharedScales, calcLabel } from './calculator';
export { loadEdgeLayer }                 from './edges';
export {
  runGravityModel, runNodalModel,
  drawResiduals, clearResiduals,
} from './model';
export type { ModelResults, NodalModelResults, ModelCovariate } from './model';
export type { CalcTerm, CalcOp } from './calculator';

// Popup content builders
export { buildAreaPopup, buildFlowPopup } from './popup';
export type { PopupInfo } from './popup';