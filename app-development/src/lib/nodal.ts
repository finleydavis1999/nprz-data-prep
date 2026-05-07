// =============================================================================
// nodal.ts
//
// Choropleth rendering logic for the Nodal tab.
//
// Responsibilities:
//   - Fetch variable data for one or two scales via db.fetchRows()
//   - Compute quantile break points and classify each feature into a colour class
//   - Write classification results to MapLibre feature state (the `cls` property
//     that the colour expression in map.ts reads)
//   - Handle the "both" extent mode, which requires unified break points across
//     both the inner and outer scale so colours are directly comparable
//
// Note: This module has NO Svelte reactive state. All reactive values (varKey,
// scaleKey, normalisation, extent) are passed as plain arguments from page.svelte.
// =============================================================================

import { fetchRows } from './db';
import { quantileBreaks, classify } from './classify';
import { setColours, setLayerVis, clipToInnerBoundary, clearBoundaryClip,
         clipOuterToExcludeInner, map } from './map';
import { COLOURS_BLUE, ALL_SCALES, isInnerScale } from './config';
import type { Normalisation, SpatialExtent } from './types';

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Apply a choropleth variable to a single scale.
 * Fetches data, computes quantile breaks, writes feature states, sets colours.
 *
 * @returns  The computed break points (used by the legend in page.svelte)
 */
export async function applyScale(
  varKey:    string,
  scaleKey:  string,
  norm:      Normalisation,
): Promise<number[]> {
  if (!map) return [];

  const rows   = await fetchRows(varKey, scaleKey, norm);
  const values = rows.map(r => Number(r.value)).filter(v => isFinite(v) && v > -99990);
  const breaks = quantileBreaks(values);

  if (rows.length === 0) {
    console.warn(`[applyScale] No rows for varKey="${varKey}" scale="${scaleKey}"`);
    return [];
  }

  const sourceId = `${scaleKey}-source`;
  for (const row of rows) {
    // promoteId preserves GeoJSON property type:
    //   string codes (BU…/GM…/WK…) stay strings, PC4 numeric codes stay numbers.
    map.setFeatureState(
      { source: sourceId, id: row.id },
      { cls: classify(Number(row.value), breaks) },
    );
  }

  return breaks;
}

/**
 * Orchestrate the full nodal render for the current UI state.
 * Hides all inactive scales, applies clip filters, and calls applyScale()
 * for whichever combination of inner / outer / both is selected.
 *
 * In "both" mode the break points are computed from the union of both scales'
 * values so that colours are directly comparable across the map.
 *
 * @returns  { innerBreaks, outerBreaks } — equal in "both" mode (unified), or
 *           only one is populated in "inner" / "outer" mode.
 */
export async function renderNodal(params: {
  varKey:       string;
  extent:       SpatialExtent;
  innerScaleKey: string;
  outerScaleKey: string;
  norm:         Normalisation;
}): Promise<{ innerBreaks: number[]; outerBreaks: number[] }> {
  const { varKey, extent, innerScaleKey, outerScaleKey, norm } = params;

  const showInner = extent === 'inner' || extent === 'both';
  const showOuter = extent === 'outer' || extent === 'both';
  const innerIsOuter = !isInnerScale(innerScaleKey); // e.g. PC4 used as study-area scale

  // Clear all boundary clips from the previous render
  for (const s of ALL_SCALES) clearBoundaryClip(s.key);

  // ── Both mode: unified colour scale ────────────────────────────────────────
  // The effective outer key is empty if both pickers point to the same scale
  // (shouldn't normally happen but guards against it).
  const effectiveOuterKey =
    showInner && showOuter && innerScaleKey === outerScaleKey ? '' : outerScaleKey;

  if (showInner && showOuter && innerScaleKey && effectiveOuterKey) {
    const [innerRows, outerRows] = await Promise.all([
      fetchRows(varKey, innerScaleKey, norm),
      fetchRows(varKey, effectiveOuterKey, norm),
    ]);

    // Unified breaks across both datasets — same colour = same value range
    const allValues = [...innerRows, ...outerRows]
      .map(r => Number(r.value))
      .filter(v => isFinite(v) && v > -99990);
    const unifiedBreaks = quantileBreaks(allValues);

    _applyRows(innerScaleKey,     innerRows, unifiedBreaks);
    _applyRows(effectiveOuterKey, outerRows, unifiedBreaks);

    setColours(innerScaleKey,     COLOURS_BLUE, 0.72);
    setColours(effectiveOuterKey, COLOURS_BLUE, 0.72);

    // Outer layer: clip to exclude the inner area so polygons don't overlap
    clipOuterToExcludeInner(effectiveOuterKey);
    setLayerVis(`${effectiveOuterKey}-fill`,    'visible');
    setLayerVis(`${effectiveOuterKey}-outline`, 'visible');

    // Inner layer: clip to boundary if it's an outer-type scale (e.g. PC4 as study area)
    if (innerIsOuter) clipToInnerBoundary(innerScaleKey);
    setLayerVis(`${innerScaleKey}-fill`,    'visible');
    setLayerVis(`${innerScaleKey}-outline`, 'visible');

    return { innerBreaks: unifiedBreaks, outerBreaks: unifiedBreaks };
  }

  // ── Single extent ─────────────────────────────────────────────────────────
  let innerBreaks: number[] = [];
  let outerBreaks: number[] = [];

  if (showInner && innerScaleKey) {
    const rows   = await fetchRows(varKey, innerScaleKey, norm);
    const values = rows.map(r => Number(r.value)).filter(v => isFinite(v) && v > -99990);
    innerBreaks  = quantileBreaks(values);
    _applyRows(innerScaleKey, rows, innerBreaks);
    setColours(innerScaleKey, COLOURS_BLUE, 0.72);
    if (innerIsOuter) clipToInnerBoundary(innerScaleKey);
    setLayerVis(`${innerScaleKey}-fill`,    'visible');
    setLayerVis(`${innerScaleKey}-outline`, 'visible');
  }

  if (showOuter && outerScaleKey) {
    const rows   = await fetchRows(varKey, outerScaleKey, norm);
    const values = rows.map(r => Number(r.value)).filter(v => isFinite(v) && v > -99990);
    outerBreaks  = quantileBreaks(values);
    _applyRows(outerScaleKey, rows, outerBreaks);
    setColours(outerScaleKey, COLOURS_BLUE, 0.72);
    setLayerVis(`${outerScaleKey}-fill`,    'visible');
    setLayerVis(`${outerScaleKey}-outline`, 'visible');
  }

  return { innerBreaks, outerBreaks };
}

// ── Private helpers ───────────────────────────────────────────────────────────

/** Write pre-classified feature states for a set of rows. */
function _applyRows(
  scaleKey: string,
  rows:     { id: string | number; value: number }[],
  breaks:   number[],
): void {
  if (!map) return;
  const sourceId = `${scaleKey}-source`;
  for (const row of rows) {
    map.setFeatureState(
      { source: sourceId, id: row.id },
      { cls: classify(Number(row.value), breaks) },
    );
  }
}