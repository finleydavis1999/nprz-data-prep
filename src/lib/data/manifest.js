// Loads and caches `static/data/manifest.json` (served at `${base}/data/manifest.json`).
// Safe to call from SvelteKit load functions (server or client) — pass the
// SvelteKit `fetch` for SSR-safe fetching; otherwise the global `fetch` is used.

import { base } from '$app/paths';

let cached = null;

export async function loadManifest(fetcher = fetch) {
	if (cached) return cached;
	const res = await fetcher(`${base}/data/manifest.json`);
	if (!res.ok) throw new Error(`manifest.json: HTTP ${res.status}`);
	cached = await res.json();
	return cached;
}

// Test-only: clear the in-module cache.
export function _resetManifestCache() {
	cached = null;
}
