// Build a versioned URL for a file under `static/data/`. Appending the manifest
// version as a query string lets us cache `data/geo/*` and `data/parquet/*`
// indefinitely (see `static/_headers`) while still invalidating on rebuilds —
// the manifest itself is fetched fresh every load.
import { base } from '$app/paths';

export function dataUrl(/** @type {string} */ relPath, /** @type {string} */ version) {
	return `${base}/data/${relPath}?v=${encodeURIComponent(version)}`;
}
