// Singleton manifest state — fetched once on app boot, read by all routes.
import { loadManifest } from '$lib/data/manifest.js';

class ManifestState {
	data = $state(/** @type {any} */ (null));
	error = $state(/** @type {string | null} */ (null));
	loading = $state(false);

	async ensureLoaded() {
		if (this.data || this.loading) return;
		this.loading = true;
		try {
			this.data = await loadManifest();
		} catch (e) {
			this.error = e?.message ?? String(e);
		} finally {
			this.loading = false;
		}
	}
}

export const manifestState = new ManifestState();
