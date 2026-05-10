// Build a MapLibre paint expression from breaks + colors.
//
// `breaks` has length n+1 (boundaries); `colors` has length n.
// Falls back to `nullColor` when feature-state value is null/missing.
// The `step` expression maps:
//   value < breaks[1]            -> colors[0]
//   breaks[1] <= value < breaks[2] -> colors[1]
//   …
//   value >= breaks[n-1]         -> colors[n-1]
export function stepExpression({ breaks, colors, nullColor = '#eee' }) {
	if (breaks.length !== colors.length + 1) {
		throw new Error(
			`stepExpression: expected breaks.length === colors.length + 1 (got ${breaks.length} vs ${colors.length})`
		);
	}
	const step = ['step', ['feature-state', 'value'], colors[0]];
	for (let i = 1; i < colors.length; i++) {
		step.push(breaks[i], colors[i]);
	}
	return ['case', ['==', ['feature-state', 'value'], null], nullColor, step];
}
