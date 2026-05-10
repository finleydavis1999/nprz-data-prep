// Singleton selection state — what the user is currently mapping (node side).
// Read by `+page.svelte` to drive `runChoropleth(...)` and the legend/histogram.
// Written by ScaleToggle, DatasetPicker, YearPicker, CategoryFilters.
//
// Node datasets are single-year only (manifest field `year.type === 'single'`).
class SelectionState {
	dataset = $state('demographics');
	scale = $state('gem');
	year = $state(2018);
	// Multi-select filters keyed by field id, value lists of integer category ids.
	// Empty / missing key = no filter on that field.
	filters = $state({});
}

export const selection = new SelectionState();
