<script>
	import './layout.css';
	import favicon from '$lib/assets/favicon.svg';
	import { manifestState } from '$lib/state/manifest.svelte.js';
	import { queryResult } from '$lib/state/query-result.svelte.js';
	import { selection } from '$lib/state/selection.svelte.js';
	import { layers } from '$lib/state/layers.svelte.js';

	let { children } = $props();

	// Boot manifest once, then re-run the choropleth query whenever any
	// selection field changes. Lifted to the layout so /  and /print share
	// the same `queryResult.data` without redundant fetches.
	$effect(() => {
		manifestState.ensureLoaded();
		layers.load();
	});
	$effect(() => {
		if (!manifestState.data) return;
		// Touch reactive deps so the effect re-runs on selection changes.
		void selection.dataset;
		void selection.scale;
		void selection.year;
		void selection.filters;
		queryResult.refresh();
	});
	// Re-run all saved layers when the manifest is ready and whenever the
	// active scale changes (area_codes differ across scales, so cached
	// results from the other scale are invalid).
	$effect(() => {
		if (!manifestState.data) return;
		void selection.scale;
		layers.refreshAll();
	});
</script>

<svelte:head><link rel="icon" href={favicon} /></svelte:head>
{@render children()}
