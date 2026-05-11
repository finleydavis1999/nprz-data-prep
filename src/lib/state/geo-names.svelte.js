// Per-scale area_code → display name lookup + point-in-polygon spatial
// resolution. Loaded lazily by fetching the scale's geojson once and reading
// properties. PC4 features don't carry a `name` property — get() returns the
// code itself in that case, which is the canonical human-readable identifier
// at that scale.
//
// The features themselves are kept (non-reactive) so the choropleth click
// handler can resolve a [lng, lat] back to a containing polygon at a
// different scale — used when on PC4 but flows are gem-scale.
import { SvelteMap } from 'svelte/reactivity';
import { geoContains } from 'd3-geo';
import { dataUrl } from '$lib/data/url.js';
import { manifestState } from './manifest.svelte.js';

class GeoNamesState {
	/** @type {SvelteMap<string, Map<string, string>>} */
	byScale = new SvelteMap();
	/** Map<scale, Array<{ code: string, feature: any }>> — kept outside of
	 *  $state so geoContains iteration stays cheap (no proxy overhead). */
	#featuresByScale = new Map();
	/** @type {Map<string, Promise<void>>} */
	#inflight = new Map();

	async ensureLoaded(/** @type {string} */ scale) {
		if (this.byScale.has(scale)) return;
		const existing = this.#inflight.get(scale);
		if (existing) return existing;
		const path = manifestState.data?.geo?.[scale]?.geojson;
		const version = manifestState.data?.version;
		if (!path || !version) return;
		const p = (async () => {
			try {
				const res = await fetch(dataUrl(path, version));
				if (!res.ok) return;
				const gj = await res.json();
				/** @type {Map<string, string>} */
				// eslint-disable-next-line svelte/prefer-svelte-reactivity -- assigned once into the SvelteMap below; not mutated afterwards
				const lookup = new Map();
				/** @type {Array<{code: string, feature: any}>} */
				const features = [];
				for (const f of gj.features ?? []) {
					const code = f.properties?.area_code;
					if (code == null) continue;
					const name = f.properties?.name ?? String(code);
					lookup.set(String(code), String(name));
					features.push({ code: String(code), feature: f });
				}
				this.byScale.set(scale, lookup);
				this.#featuresByScale.set(scale, features);
			} finally {
				this.#inflight.delete(scale);
			}
		})();
		this.#inflight.set(scale, p);
		return p;
	}

	/** Returns the display name for an area_code at a given scale, or the code itself if unknown. */
	get(/** @type {string} */ scale, /** @type {string} */ code) {
		const m = this.byScale.get(scale);
		return m?.get(String(code)) ?? String(code);
	}

	/** Returns the area_code at `scale` whose polygon contains [lng, lat], or null. */
	resolveCoord(/** @type {string} */ scale, /** @type {number} */ lng, /** @type {number} */ lat) {
		const features = this.#featuresByScale.get(scale);
		if (!features) return null;
		const point = [lng, lat];
		for (const { code, feature } of features) {
			if (geoContains(feature, point)) return code;
		}
		return null;
	}
}

export const geoNames = new GeoNamesState();
