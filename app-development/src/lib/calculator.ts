// =============================================================================
// calculator.ts
//
// Expression evaluation logic for the Calculate tab.
//
// The calculator lets users build expressions like "A ÷ B" or "A + B − C"
// from up to four terms, where each term is either a variable (fetched from
// a parquet) or a typed numeric constant. Results are written to MapLibre
// feature state and rendered as a green choropleth layer.
//
// All Svelte state (calcTerms, calcOps, calcScaleKey, etc.) stays in
// page.svelte — this module receives plain values as arguments.
// =============================================================================

import { fetchRows } from './db';
import { quantileBreaks, classify } from './classify';
import { setColours, setLayerVis, map } from './map';
import { COLOURS_GREEN, ALL_SCALES, VARIABLES } from './config';
import type { Normalisation } from './types';

// ── Types (mirrored from page.svelte) ────────────────────────────────────────

export type CalcTerm = { type: 'var'; key: string } | { type: 'const'; value: string };
export type CalcOp   = '+' | '-' | '*' | '/';

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Evaluate a multi-term expression for every feature in the selected scale,
 * write classification results to MapLibre feature state, and return break
 * points for legend display.
 *
 * Operators are applied left-to-right (no precedence — the UI enforces this).
 * Division by zero and NaN propagation produce no-data (NaN) for that feature.
 *
 * @param terms     Ordered list of terms (variables or constants)
 * @param ops       Operators between terms (length = terms.length - 1)
 * @param scaleKey  Which scale to evaluate on
 * @returns         Quantile break points for the legend
 */
export async function applyCalc(
  terms:    CalcTerm[],
  ops:      CalcOp[],
  scaleKey: string,
): Promise<number[]> {
  if (!map) return [];

  // Fetch all variable terms in parallel; constants resolve immediately as null
  const termMaps = await Promise.all(
    terms.map(t =>
      t.type === 'const'
        ? Promise.resolve(null)
        : _fetchMap(t.key, scaleKey),
    )
  );

  // Collect all feature IDs from any variable term
  const allIds = new Set<string | number>();
  for (const m of termMaps) {
    if (m) for (const k of m.keys()) allIds.add(k);
  }

  // Evaluate the expression for each feature
  const rows = [...allIds].map(id => {
    const termValues = terms.map((t, i) => {
      if (t.type === 'const') return parseFloat(t.value);
      return termMaps[i]?.get(id) ?? NaN;
    });

    // Left-to-right evaluation
    let value = termValues[0];
    for (let i = 0; i < ops.length; i++) {
      const next = termValues[i + 1];
      if (!isFinite(value) || !isFinite(next)) { value = NaN; break; }
      const op = ops[i];
      if (op === '/' && next === 0)  { value = NaN; break; }
      if (op === '+') value = value + next;
      else if (op === '-') value = value - next;
      else if (op === '*') value = value * next;
      else                 value = value / next;
    }
    return { id, value };
  });

  // Classify and write feature states
  const values = rows.map(r => r.value).filter(v => isFinite(v));
  const breaks = quantileBreaks(values);
  const sourceId = `${scaleKey}-source`;

  for (const row of rows) {
    map.setFeatureState(
      { source: sourceId, id: row.id },
      { cls: classify(row.value, breaks) },
    );
  }

  setColours(scaleKey, COLOURS_GREEN, 0.72);
  setLayerVis(`${scaleKey}-fill`,    'visible');
  setLayerVis(`${scaleKey}-outline`, 'visible');

  return breaks;
}

/**
 * Compute the set of scale keys that all variable terms have in common.
 * Constants don't constrain the scale — they're compatible with any scale.
 * Returns all scale keys if there are no variable terms.
 */
export function calcSharedScales(terms: CalcTerm[]): string[] {
  const varTerms = terms.filter((t): t is { type: 'var'; key: string } => t.type === 'var');
  if (varTerms.length === 0) return ALL_SCALES.map(s => s.key);

  let shared = VARIABLES.find(v => v.key === varTerms[0].key)?.availableAt ?? [];
  for (const t of varTerms.slice(1)) {
    const avail = VARIABLES.find(v => v.key === t.key)?.availableAt ?? [];
    shared = shared.filter(s => avail.includes(s));
  }
  return shared;
}

/**
 * Build a human-readable label for the current expression, e.g. "Jobs ÷ Population".
 * Used in the legend and as the layer title.
 */
export function calcLabel(terms: CalcTerm[], ops: CalcOp[]): string {
  const opSymbols: Record<string, string> = { '+': '+', '-': '−', '*': '×', '/': '÷' };
  return terms.map((t, i) => {
    const termStr = t.type === 'const'
      ? t.value
      : (VARIABLES.find(v => v.key === t.key)?.label ?? t.key);
    return i < ops.length ? `${termStr} ${opSymbols[ops[i]]}` : termStr;
  }).join(' ');
}

// ── Private helpers ───────────────────────────────────────────────────────────

/** Fetch a variable and return it as a Map<featureId, value>. */
async function _fetchMap(
  varKey:   string,
  scaleKey: string,
): Promise<Map<string | number, number>> {
  const rows = await fetchRows(varKey, scaleKey, 'none');
  return new Map(rows.map(r => [r.id, Number(r.value)]));
}