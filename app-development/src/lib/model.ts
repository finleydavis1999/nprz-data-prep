// =============================================================================
// model.ts
//
// Statistical modelling logic for the Model tab.
//
// Contains two model types:
//   1. Gravity model — log-linear OLS on PC4-level OD flows.
//      Predicts log(flow) from log(origin mass) + log(dest mass) + log(distance)
//      + optional covariates. Residuals drawn as coloured flow lines on the map.
//
//   2. Nodal model — cross-sectional OLS on PC4 areas.
//      Predicts a nodal outcome (e.g. commuter outflow) from CBS area covariates.
//      Residuals drawn as a diverging choropleth on the PC4 layer.
//
// OLS implementation uses normal equations (X'X)^-1 X'y with Gauss-Jordan
// matrix inversion. Suitable for small matrices (≤ ~10 variables). VIF is
// computed by regressing each predictor on the others.
//
// All Svelte state stays in page.svelte — functions here receive plain values.
// =============================================================================

import type { Feature, FeatureCollection } from 'geojson';
import { map, setLayerVis, setColours } from './map';
import { queuedQuery, dataURL, fetchRows } from './db';
import { quantileBreaks, classify } from './classify';
import { findScale } from './config';
import { NO_DATA } from './config';

// ── Public types ──────────────────────────────────────────────────────────────

export interface ModelCovariate {
  key:        string;
  label:      string;
  role:       'origin' | 'destination' | 'covariate';
  useLog:     boolean;
  useDensity: boolean; // divide by land area (ha) before optional log
}

export interface ModelResults {
  coefficients: { name: string; coef: number; se: number; t: number }[];
  r2:           number;
  r2adj:        number;
  n:            number;
  vif:          { name: string; vif: number }[];
  residuals:    {
    origin_id: number;
    dest_id:   number;
    observed:  number;
    predicted: number;
    residual:  number;
  }[];
}

export interface NodalModelResults {
  r2:           number;
  r2adj:        number;
  n:            number;
  coefficients: { name: string; coef: number; se: number; t: number }[];
  residuals:    {
    id:        number | string;
    observed:  number;
    predicted: number;
    residual:  number;
  }[];
}

// ── Gravity model ─────────────────────────────────────────────────────────────

/**
 * Fit a log-linear gravity model to PC4-level commuting flows.
 *
 * Model: log(T_ij) = α + β₁·log(Oᵢ) + β₂·log(Dⱼ) + β₃·log(dist^γ) + Σ βₖ·Xₖ + ε
 *
 * Steps:
 *   1. Load PC4 centroids for haversine distance computation
 *   2. Load flows from the woonwerk edge parquet (with optional ink/opl filters)
 *   3. Load node summary and CBS stats for origin/destination attributes
 *   4. Build design matrix and fit OLS
 *   5. Compute VIF for multicollinearity diagnostics
 *   6. Draw residuals as coloured flow lines on the map
 *
 * @returns  ModelResults on success, or throws with a descriptive message
 */
export async function runGravityModel(params: {
  period:       string;
  originKey:    string;
  destKey:      string;
  logOrigin:    boolean;
  logDest:      boolean;
  logDistance:  boolean;
  decayExp:     number;
  covariates:   ModelCovariate[];
  internals:    'exclude' | 'include' | 'only';
  minFlow:      number;
  inkFilter:    string[];
  oplFilter:    string[];
}): Promise<ModelResults> {
  const {
    period, originKey, destKey, logOrigin, logDest, logDistance,
    decayExp, covariates, internals, minFlow, inkFilter, oplFilter,
  } = params;

  // Step 1: Centroids for distance computation
  const centroidsRaw = await fetch(dataURL('pc4_centroids.json')).then(r => r.json());
  const centMap = new Map<number, { lng: number; lat: number }>(
    centroidsRaw.map((c: any) => [Number(c.id), { lng: c.lng, lat: c.lat }])
  );

  // Step 2: Load flows
  const hasFilter   = inkFilter.length > 0 || oplFilter.length > 0;
  const flowFile    = hasFilter
    ? 'edges_woonwerk_ink_opl_pc4.parquet'
    : 'edges_woonwerk_pc4.parquet';
  const inkWhere    = inkFilter.length > 0 ? `AND inks IN (${inkFilter.join(',')})` : '';
  const oplWhere    = oplFilter.length > 0 ? `AND opl IN (${oplFilter.join(',')})` : '';
  const internalWhere = internals === 'exclude' ? 'AND origin_id != destination_id'
                      : internals === 'only'    ? 'AND origin_id = destination_id'
                      : '';

  const flowRes = await queuedQuery(`
    SELECT origin_id, destination_id, SUM(flow_value) AS flow_value
    FROM read_parquet('${dataURL(flowFile)}')
    WHERE periode = '${period}'
    AND flow_value >= ${minFlow}
    ${internalWhere} ${inkWhere} ${oplWhere}
    GROUP BY origin_id, destination_id
  `);
  const flows = flowRes.toArray().map((r: any) => r.toJSON());

  if (flows.length < 10) {
    throw new Error('Too few flows to fit model. Check filters or minimum count.');
  }

  // Step 3: Node summaries and CBS stats
  const nodeRes = await queuedQuery(`
    SELECT postcode, total_banen_werk, total_banen_woon, total_inwoners,
           ratio_banen_inwoners, ratio_werkenden_inwoners
    FROM read_parquet('${dataURL('nodes_summary_pc4.parquet')}')
    WHERE jaar = 2017
  `);
  const nodes = new Map<number, any>(
    nodeRes.toArray().map((r: any) => { const j = r.toJSON(); return [j.postcode, j]; })
  );

  // CBS stats: primary study area + supplementary for outer PC4s in flow data
  const cbsRes = await queuedQuery(`
    SELECT * FROM read_parquet('${dataURL('pc4_zh_2024_stats.parquet')}')
    UNION ALL
    SELECT * FROM read_parquet('${dataURL('pc4_supplementary_stats.parquet')}')
  `);
  const cbsNodes = new Map<number, any>(
    cbsRes.toArray().map((r: any) => { const j = r.toJSON(); return [j.postcode, j]; })
  );

  // Resolve a variable value: node summary first, then CBS column
  const getVal = (pc4: number, key: string): number => {
    const n = nodes.get(pc4);
    const c = cbsNodes.get(pc4);
    const named: Record<string, number> = {
      nodes_banen_werk: n?.total_banen_werk ?? NaN,
      nodes_banen_woon: n?.total_banen_woon ?? NaN,
      nodes_inwoners:   n?.total_inwoners   ?? NaN,
    };
    if (key in named) return named[key];
    const v = c?.[key];
    return (v != null && Number(v) > -99990) ? Number(v) : NaN;
  };

  // Step 4: Build design matrix
  const varNames = [
    'intercept',
    `log(${originKey})`,
    `log(${destKey})`,
    `log(dist^${decayExp})`,
    ...covariates.map(c =>
      `${c.useLog ? 'log' : ''}(${c.label}${c.useDensity ? '/ha' : ''})`
    ),
  ];

  const Xrows: number[][] = [];
  const Yrows: number[]   = [];
  const rawFlows: typeof flows = [];

  for (const flow of flows) {
    const oi = Number(flow.origin_id), di = Number(flow.destination_id);
    const oVal = getVal(oi, originKey);
    const dVal = getVal(di, destKey);
    const dist = _haversine(centMap, oi, di);

    if (!isFinite(oVal) || !isFinite(dVal) || !isFinite(dist) || dist <= 0) continue;
    if (oVal <= 0 || dVal <= 0) continue;

    const y       = Math.log(Math.max(flow.flow_value, 0.5));
    const xOrigin = logOrigin   ? Math.log(oVal) : oVal;
    const xDest   = logDest     ? Math.log(dVal) : dVal;
    const xDist   = logDistance
      ? decayExp * Math.log(dist)
      : Math.pow(dist, decayExp);

    // Covariates
    const covVals: number[] = [];
    let skip = false;
    for (const cov of covariates) {
      let v = getVal(oi, cov.key);
      if (cov.useDensity) {
        const area = getVal(oi, 'land_area_ha');
        v = area > 0 ? v / area : NaN;
      }
      if (cov.useLog) v = v > 0 ? Math.log(v) : NaN;
      if (!isFinite(v)) { skip = true; break; }
      covVals.push(v);
    }
    if (skip) continue;

    Xrows.push([1, xOrigin, xDest, xDist, ...covVals]);
    Yrows.push(y);
    rawFlows.push(flow);
  }

  if (Xrows.length < varNames.length + 5) {
    throw new Error(
      `Too few valid rows (${Xrows.length}) for ${varNames.length} variables.`
    );
  }

  // Step 5: OLS
  const fit = _olsFit(Xrows, Yrows);
  if (!fit) throw new Error('Matrix singular — check for collinear variables.');

  const vifVals = _computeVIF(Xrows);

  // Residuals in log space: positive = more commuting than model expects
  const residuals = rawFlows.map((flow: any, i: number) => ({
    origin_id: Number(flow.origin_id),
    dest_id:   Number(flow.destination_id),
    observed:  Number(flow.flow_value),
    predicted: Math.exp(fit.fitted[i]),
    residual:  Yrows[i] - fit.fitted[i],
  }));

  const results: ModelResults = {
    coefficients: varNames.map((name, i) => ({
      name,
      coef: fit.coef[i],
      se:   fit.se[i],
      t:    fit.coef[i] / fit.se[i],
    })),
    r2:    fit.r2,
    r2adj: fit.r2adj,
    n:     Xrows.length,
    vif:   varNames.slice(1).map((name, i) => ({ name, vif: vifVals[i] ?? NaN })),
    residuals,
  };

  // Step 6: Draw residuals
  await drawResiduals(residuals);

  return results;
}

// ── Nodal model ───────────────────────────────────────────────────────────────

/**
 * Fit a cross-sectional log-linear OLS model at the PC4 level.
 *
 * Model: log(Y_i) = α + Σ βₖ·log(X_ki) + ε
 * where Y is a nodal outcome (e.g. commuter outflow) and X are CBS area variables.
 *
 * Residuals are written as a diverging choropleth (teal = above predicted, red = below).
 *
 * @returns  NodalModelResults on success, or throws with a descriptive message
 */
export async function runNodalModel(params: {
  outcomeKey:    string;
  predictorKeys: string[];
}): Promise<NodalModelResults> {
  const { outcomeKey, predictorKeys } = params;

  // Load outcome variable at PC4 scale
  const outcomeRows = await fetchRows(outcomeKey, 'pc4', 'none');
  if (outcomeRows.length < 10) {
    throw new Error('Too few areas for regression.');
  }

  // Load CBS covariates (study area + supplementary)
  const cbsRes = await queuedQuery(`
    SELECT * FROM read_parquet('${dataURL('pc4_zh_2024_stats.parquet')}')
    UNION ALL
    SELECT * FROM read_parquet('${dataURL('pc4_supplementary_stats.parquet')}')
  `);
  const cbsMap = new Map<number, any>(
    cbsRes.toArray().map((r: any) => {
      const j = r.toJSON(); return [Number(j.postcode), j];
    })
  );

  // Build design matrix (max 5 predictors)
  const predKeys  = predictorKeys.slice(0, 5);
  const Xrows: number[][] = [];
  const Yrows: number[]   = [];
  const ids:   (number | string)[] = [];

  for (const row of outcomeRows) {
    const y = Number(row.value);
    if (!isFinite(y) || y <= -99990) continue;
    const cbs = cbsMap.get(Number(row.id));
    if (!cbs) continue;

    const xs = predKeys.map(k => {
      const v = Number(cbs[k]);
      return (isFinite(v) && v > -99990) ? Math.log(Math.max(v, 0.01)) : NaN;
    });
    if (xs.some(x => !isFinite(x))) continue;

    Xrows.push([1, ...xs]);
    Yrows.push(Math.log(Math.max(y, 0.01)));
    ids.push(row.id);
  }

  if (Xrows.length < predKeys.length + 3) {
    throw new Error(`Too few valid areas (${Xrows.length}).`);
  }

  const fit = _olsFit(Xrows, Yrows);
  if (!fit) throw new Error('Singular matrix — check for collinear predictors.');

  const varNames = ['intercept', ...predKeys];
  const residuals = ids.map((id, i) => ({
    id,
    observed:  Math.exp(Yrows[i]),
    predicted: Math.exp(fit.fitted[i]),
    residual:  Yrows[i] - fit.fitted[i],
  }));

  // Draw diverging choropleth on PC4 layer
  _drawNodalResiduals(residuals);

  return {
    r2:   fit.r2,
    r2adj: fit.r2adj,
    n:    Xrows.length,
    coefficients: varNames.map((name, i) => ({
      name, coef: fit.coef[i], se: fit.se[i], t: fit.coef[i] / fit.se[i],
    })),
    residuals,
  };
}

// ── Residual visualisation ────────────────────────────────────────────────────

/**
 * Draw gravity model residuals as coloured flow lines.
 * Teal (+) = more commuting than model expects.
 * Red  (−) = less commuting than model expects.
 * Top 1500 flows by absolute residual magnitude are shown.
 */
export async function drawResiduals(
  residuals: ModelResults['residuals'],
): Promise<void> {
  if (!map) return;

  const centroidsRaw = await fetch(dataURL('pc4_centroids.json')).then(r => r.json());
  const centMap = new Map<number, [number, number]>(
    centroidsRaw.map((c: any) => [Number(c.id), [c.lng, c.lat] as [number, number]])
  );

  // Top 1500 by absolute residual — keeps rendering performant
  const sorted = [...residuals]
    .sort((a, b) => Math.abs(b.residual) - Math.abs(a.residual))
    .slice(0, 1500);
  const maxAbs = Math.max(...sorted.map(r => Math.abs(r.residual)), 1);

  const features: Feature[] = sorted.flatMap(r => {
    const o = centMap.get(r.origin_id);
    const d = centMap.get(r.dest_id);
    if (!o || !d) return [];
    return [{
      type: 'Feature' as const,
      properties: {
        residual:  r.residual,
        norm:      r.residual / maxAbs,
        observed:  r.observed,
        predicted: r.predicted,
        origin_id: r.origin_id,
        dest_id:   r.dest_id,
      },
      geometry: { type: 'LineString' as const, coordinates: [o, d] },
    }];
  });

  const geojson: FeatureCollection = { type: 'FeatureCollection', features };
  const sid = 'model-residuals-source';
  const lid = 'model-residuals-layer';

  if (map.getSource(sid)) {
    (map.getSource(sid) as maplibregl.GeoJSONSource).setData(geojson);
  } else {
    map.addSource(sid, { type: 'geojson', data: geojson });
    map.addLayer({
      id: lid, type: 'line', source: sid,
      layout: { 'line-join': 'round', 'line-cap': 'round' },
      paint: {
        'line-color': [
          'interpolate', ['linear'], ['get', 'norm'],
          -1,    '#e63946', // strong negative: red
          -0.15, '#f4a261', // weak negative: orange
           0,    '#dddddd', // neutral: light grey
           0.15, '#74c476', // weak positive: light green
           1,    '#2a9d8f', // strong positive: teal
        ],
        'line-opacity': [
          'interpolate', ['linear'],
          ['max', ['get', 'norm'], ['-', 0, ['get', 'norm']]],
          0, 0.1, 0.2, 0.5, 1, 0.85,
        ],
        'line-width': [
          'interpolate', ['linear'],
          ['max', ['get', 'norm'], ['-', 0, ['get', 'norm']]],
          0, 0.3, 0.5, 1.5, 1, 4,
        ],
      },
    });
  }

  setLayerVis(lid, 'visible');
}

/** Hide the gravity model residual flow lines. */
export function clearResiduals(): void {
  setLayerVis('model-residuals-layer', 'none');
}

// ── Private: OLS solver ───────────────────────────────────────────────────────
// Normal equations: β = (X'X)⁻¹ X'y
// Suitable for small matrices (< 10 variables). Uses Gauss-Jordan inversion.

function _matMul(A: number[][], B: number[][]): number[][] {
  const m = A.length, n = B[0].length;
  return Array.from({ length: m }, (_, i) =>
    Array.from({ length: n }, (_, j) =>
      A[i].reduce((s, _, l) => s + A[i][l] * B[l][j], 0)
    )
  );
}

function _matTranspose(A: number[][]): number[][] {
  return A[0].map((_, j) => A.map(row => row[j]));
}

/** Gauss-Jordan matrix inverse. Returns null if matrix is singular. */
function _matInverse(A: number[][]): number[][] | null {
  const n = A.length;
  const M = A.map((row, i) => [
    ...row,
    ...Array.from({ length: n }, (_, j) => i === j ? 1 : 0),
  ]);

  for (let col = 0; col < n; col++) {
    // Partial pivoting for numerical stability
    let maxRow = col;
    for (let row = col + 1; row < n; row++) {
      if (Math.abs(M[row][col]) > Math.abs(M[maxRow][col])) maxRow = row;
    }
    [M[col], M[maxRow]] = [M[maxRow], M[col]];

    if (Math.abs(M[col][col]) < 1e-12) return null; // singular

    const pivot = M[col][col];
    for (let j = 0; j < 2 * n; j++) M[col][j] /= pivot;
    for (let row = 0; row < n; row++) {
      if (row === col) continue;
      const factor = M[row][col];
      for (let j = 0; j < 2 * n; j++) M[row][j] -= factor * M[col][j];
    }
  }

  return M.map(row => row.slice(n));
}

/** Fit OLS and return coefficients, standard errors, R², fitted values. */
function _olsFit(
  X: number[][],
  y: number[],
): { coef: number[]; se: number[]; r2: number; r2adj: number; fitted: number[] } | null {
  const n = y.length, p = X[0].length;
  const Xt   = _matTranspose(X);
  const XtX  = _matMul(Xt, X);
  const XtXi = _matInverse(XtX);
  if (!XtXi) return null;

  const Xty  = _matMul(Xt, y.map(v => [v]));
  const coef = _matMul(XtXi, Xty).map(r => r[0]);

  const fitted = X.map(row => row.reduce((s, v, i) => s + v * coef[i], 0));
  const yMean  = y.reduce((a, b) => a + b, 0) / n;
  const ssTot  = y.reduce((s, v) => s + (v - yMean) ** 2, 0);
  const ssRes  = y.reduce((s, v, i) => s + (v - fitted[i]) ** 2, 0);
  const r2     = 1 - ssRes / ssTot;
  const r2adj  = 1 - (1 - r2) * (n - 1) / (n - p);
  const mse    = ssRes / (n - p);
  const se     = XtXi.map((row, i) => Math.sqrt(Math.max(0, mse * row[i])));

  return { coef, se, r2, r2adj, fitted };
}

/** Variance Inflation Factor for each predictor (column 0 is intercept, skip it). */
function _computeVIF(X: number[][]): number[] {
  const p = X[0].length;
  return Array.from({ length: p - 1 }, (_, k) => {
    const j  = k + 1; // predictor column index (skip intercept)
    const y  = X.map(row => row[j]);
    const Xr = X.map(row => row.filter((_, i) => i !== j));
    const res = _olsFit(Xr, y);
    if (!res || res.r2 >= 1) return 99;
    return 1 / (1 - res.r2);
  });
}

// ── Private: Haversine distance ───────────────────────────────────────────────

/** Great-circle distance in kilometres between two PC4 centroids. */
function _haversine(
  centMap: Map<number, { lng: number; lat: number }>,
  origin:  number,
  dest:    number,
): number {
  const oc = centMap.get(origin), dc = centMap.get(dest);
  if (!oc || !dc) return NaN;
  const R    = 6371;
  const dLat = (dc.lat - oc.lat) * Math.PI / 180;
  const dLng = (dc.lng - oc.lng) * Math.PI / 180;
  const a    = Math.sin(dLat / 2) ** 2
             + Math.cos(oc.lat * Math.PI / 180) * Math.cos(dc.lat * Math.PI / 180)
             * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// ── Private: Nodal residual choropleth ────────────────────────────────────────

/** Write diverging residuals to PC4 feature state and apply a 5-class colour ramp. */
function _drawNodalResiduals(
  residuals: NodalModelResults['residuals'],
): void {
  if (!map) return;
  const DIVERGING = ['#e63946', '#f4a261', '#dddddd', '#74c476', '#2a9d8f'];
  const maxAbs = Math.max(...residuals.map(r => Math.abs(r.residual)), 0.01);

  try { map.removeFeatureState({ source: 'pc4-source' }); } catch (_) {}

  for (const r of residuals) {
    // Map residual to class 0-4: 0=strong negative … 4=strong positive
    const cls = Math.round(2 + (r.residual / maxAbs) * 2);
    map.setFeatureState({ source: 'pc4-source', id: r.id }, { cls });
  }

  setColours('pc4', DIVERGING, 0.72);
  setLayerVis('pc4-fill',    'visible');
  setLayerVis('pc4-outline', 'visible');
}