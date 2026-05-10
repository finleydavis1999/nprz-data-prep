// Singleton selection state — what the user is currently mapping.
// Read by `+page.svelte` to drive `runChoropleth(...)` and the legend/histogram.
// Written by ScaleToggle, DatasetPicker, YearPicker, CategoryFilters.
class SelectionState {
	dataset = $state('demographics');
	scale = $state('pc4');
	year = $state(2018);
	// Multi-select filters keyed by field id, value lists of integer category ids.
	// Empty / missing key = no filter on that field.
	filters = $state({});
}

export const selection = new SelectionState();
