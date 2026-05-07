// =============================================================================
// classify.ts
//
// Pure statistical helpers for choropleth classification.
// No side effects, no MapLibre or DuckDB dependencies — safe to import anywhere.
// =============================================================================

/**
 * Compute n-1 quantile break points from an array of values.
 * Filters out non-finite values and CBS sentinel values (≤ -99990) before sorting.
 *
 * @param values  Raw numeric values (may contain NaN / sentinel)
 * @param n       Number of classes (default 4 → returns 3 break points)
 * @returns       Sorted array of break values; empty array if too few valid values
 */
export function quantileBreaks(values: number[], n = 4): number[] {
  const sorted = [...values]
    .filter(v => isFinite(v) && v > -99990)
    .sort((a, b) => a - b);

  if (sorted.length < n) return [];

  return Array.from({ length: n - 1 }, (_, i) => {
    const idx = Math.floor(((i + 1) / n) * sorted.length);
    return sorted[Math.min(idx, sorted.length - 1)];
  });
}

/**
 * Assign a value to a class index (0-based) using pre-computed break points.
 *
 * Returns -1 for no-data (null, NaN, or CBS sentinel values ≤ -99990).
 * Note: 0 and small positives are VALID and map to class 0 (lightest colour).
 *
 * @param value   The value to classify
 * @param breaks  Break points from quantileBreaks()
 * @returns       Class index (-1 = no data, 0 = lowest class, breaks.length = highest)
 */
export function classify(value: number, breaks: number[]): number {
  if (value == null || !isFinite(value) || value <= -99990) return -1;
  for (let i = 0; i < breaks.length; i++) {
    if (value <= breaks[i]) return i;
  }
  return breaks.length;
}