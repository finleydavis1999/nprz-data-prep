// Build a MapLibre paint expression from breaks + colors.
//
// `breaks` has length n+1 (boundaries); `colors` has length n.
// `input` is the maplibre expression to read the value from; default is
// ['feature-state', 'value'] (used by the choropleth). Pass ['get','value']
// for layers that carry the value on geojson properties (e.g. flow lines).
// Falls back to `nullColor` when the input value is null/missing.
export function stepExpression({
	breaks,
	colors,
	nullColor = '#eee',
	input = ['feature-state', 'value']
}) {
	if (breaks.length !== colors.length + 1) {
		throw new Error(
			`stepExpression: expected breaks.length === colors.length + 1 (got ${breaks.length} vs ${colors.length})`
		);
	}
	const step = ['step', input, colors[0]];
	for (let i = 1; i < colors.length; i++) {
		step.push(breaks[i], colors[i]);
	}
	return ['case', ['==', input, null], nullColor, step];
}
