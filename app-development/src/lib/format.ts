// =============================================================================
// format.ts
//
// Pure display/formatting utilities used across tabs.
// No side effects, no external dependencies.
// =============================================================================

import { VARIABLES } from './config';

/**
 * Format a number for legend and tooltip display.
 * Automatically scales to k / M suffixes for large values,
 * and shows enough decimal places for small values.
 */
export function fmt(v: number): string {
  if (!isFinite(v)) return '–';
  if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (Math.abs(v) >= 1e3) return `${(v / 1e3).toFixed(1)}k`;
  if (Math.abs(v) >= 100) return v.toFixed(0);
  if (Math.abs(v) >= 10)  return v.toFixed(1);
  if (Math.abs(v) >= 1)   return v.toFixed(2);
  return v.toFixed(3); // small percentages like 0.02% get 3 decimal places
}

/**
 * Group all VARIABLES by their `group` field.
 * Returns an ordered Map<groupName, Variable[]> suitable for <optgroup> rendering.
 */
export function groupedVars(): Map<string, typeof VARIABLES> {
  const grouped = new Map<string, typeof VARIABLES>();
  for (const v of VARIABLES) {
    if (!grouped.has(v.group)) grouped.set(v.group, []);
    grouped.get(v.group)!.push(v);
  }
  return grouped;
}