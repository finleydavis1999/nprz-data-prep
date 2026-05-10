// Classification helpers — produce n+1 break boundaries from a numeric array.
import { ckmeans, quantile } from 'simple-statistics';

// `values` should be filtered to finite numbers before calling.
// Returns `[min, b1, b2, …, b_{n-1}, max]` (length n+1).
export function classify(values, { method = 'jenks', n = 5, manual = null } = {}) {
	if (manual && Array.isArray(manual)) return [...manual];
	if (values.length === 0) return null;
	const sorted = [...values].sort((a, b) => a - b);
	switch (method) {
		case 'jenks':
			return jenks(sorted, n);
		case 'quantile':
			return quantileBreaks(sorted, n);
		case 'equal':
			return equalBreaks(sorted, n);
		default:
			throw new Error(`unknown classification method: ${method}`);
	}
}

function jenks(sorted, n) {
	// Degenerate: too few unique points → fall back to equal interval.
	const unique = new Set(sorted);
	if (unique.size < n) return equalBreaks(sorted, n);
	const clusters = ckmeans(sorted, n);
	const breaks = [sorted[0]];
	for (const c of clusters) breaks.push(c[c.length - 1]);
	return breaks;
}

function quantileBreaks(sorted, n) {
	const breaks = [sorted[0]];
	for (let i = 1; i < n; i++) breaks.push(quantile(sorted, i / n));
	breaks.push(sorted[sorted.length - 1]);
	return breaks;
}

function equalBreaks(sorted, n) {
	const min = sorted[0];
	const max = sorted[sorted.length - 1];
	const step = (max - min) / n;
	const breaks = [];
	for (let i = 0; i <= n; i++) breaks.push(min + step * i);
	return breaks;
}
