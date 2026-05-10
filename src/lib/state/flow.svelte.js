// Flow layer state — singletons read by `+page.svelte` to drive `runFlows(...)`
// and the FlowLayer paint expressions. Written by FlowDataPicker / Filters
// (data side) and ClassificationControls (cartography side).
//
// Reactivity rule: when mutating `filters`, assign a new object
// (`flow.filters = { ...flow.filters, [k]: v }`) — deep mutation isn't tracked.
class FlowState {
	enabled = $state(false);
	dataset = $state('ovin');
	scale = $state('gem');
	// Inclusive year range. yearMin === yearMax = single year.
	yearMin = $state(2018);
	yearMax = $state(2018);
	filters = $state({});
	// Client-side filter: only render flows whose post-aggregation value is
	// >= minWeight. Updates do not re-query DuckDB.
	minWeight = $state(0);
	includeSelfLoops = $state(false);
}

class FlowCartographyState {
	method = $state('quantile');
	n = $state(5);
	palette = $state('YlOrRd');
	widthMin = $state(0.5);
	widthMax = $state(8);
	opacity = $state(0.75);
	curvature = $state(0.2);
}

export const flow = new FlowState();
export const flowCartography = new FlowCartographyState();
