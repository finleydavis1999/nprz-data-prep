// Singleton holding the latest choropleth result. Driven by `+layout.svelte`
// (one $effect that watches `selection.*`); read by `/` and `/print`.
import { runChoropleth } from '$lib/data/query.js';
import { selection } from './selection.svelte.js';

class QueryResult {
	data = $state(new Map());
	loading = $state(false);
	error = $state(/** @type {string | null} */ (null));
	lastMs = $state(/** @type {number | null} */ (null));

	async refresh() {
		this.loading = true;
		this.error = null;
		const t0 = performance.now();
		try {
			this.data = await runChoropleth({
				dataset: selection.dataset,
				scale: selection.scale,
				year: selection.year,
				filters: selection.filters
			});
			this.lastMs = Math.round(performance.now() - t0);
		} catch (e) {
			this.error = e?.message ?? String(e);
		} finally {
			this.loading = false;
		}
	}
}

export const queryResult = new QueryResult();
