/// <reference types="@sveltejs/kit" />
//
// Service worker — exists primarily to cache the ~35 MB DuckDB WASM bundle and
// the multi-MB geojson files across reloads, since GitLab Pages doesn't let us
// set Cache-Control headers and ships everything as effectively no-store.
//
// Strategy: cache-first, no precaching.
//   - Content-hashed Vite output (in `$service-worker.build`) → safe forever.
//   - `/data/geo/*` requests carry `?v=<manifest.version>`, so the full URL is
//     its own cache key — a new build produces new URLs.
//   - Parquets bypass this layer entirely; they're cached in OPFS keyed by
//     manifest version (see `src/lib/data/opfs-cache.js`).
//
// We don't precache on `install` because the page just downloaded those bytes
// on first paint; pre-caching would force GitLab to serve them a second time.
// The cache fills opportunistically on the first reload's `fetch` interception.
import { build, version } from '$service-worker';

const sw = /** @type {ServiceWorkerGlobalScope} */ (/** @type {unknown} */ (self));
const CACHE = `nprz-cache-${version}`;
const IMMUTABLE = new Set(build);

sw.addEventListener('install', () => {
	sw.skipWaiting();
});

sw.addEventListener('activate', (event) => {
	event.waitUntil(
		(async () => {
			for (const key of await caches.keys()) {
				if (key !== CACHE) await caches.delete(key);
			}
			await sw.clients.claim();
		})()
	);
});

sw.addEventListener('fetch', (event) => {
	if (event.request.method !== 'GET') return;
	const url = new URL(event.request.url);
	if (url.origin !== sw.location.origin) return;

	const isImmutable = IMMUTABLE.has(url.pathname);
	const isVersionedGeo = url.pathname.includes('/data/geo/') && url.searchParams.has('v');
	if (!isImmutable && !isVersionedGeo) return;

	event.respondWith(cacheFirst(event.request));
});

async function cacheFirst(/** @type {Request} */ request) {
	const cache = await caches.open(CACHE);
	const hit = await cache.match(request);
	if (hit) return hit;
	const response = await fetch(request);
	if (response.ok) {
		// Don't `await` — the response stream can flow to the page in parallel.
		cache.put(request, response.clone()).catch(() => {});
	}
	return response;
}
