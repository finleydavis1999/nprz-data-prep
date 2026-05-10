<script>
	import './layout.css';
	import favicon from '$lib/assets/favicon.svg';
	import { manifestState } from '$lib/state/manifest.svelte.js';
	import { queryResult } from '$lib/state/query-result.svelte.js';
	import { selection } from '$lib/state/selection.svelte.js';

	let { children } = $props();

	// Boot manifest once, then re-run the choropleth query whenever any
	// selection field changes. Lifted to the layout so /  and /print share
	// the same `queryResult.data` without redundant fetches.
	$effect(() => {
		manifestState.ensureLoaded();
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
</script>

<svelte:head><link rel="icon" href={favicon} /></svelte:head>
{@render children()}
