// Saved layers + calc layers. A "filter" layer is a snapshot of selection.*
// (dataset/year/filters at a specific scale); a "calc" layer is a math.js
// expression over other layer slugs. Persisted to localStorage; query results
// recomputed on load.
import { SvelteMap, SvelteSet } from 'svelte/reactivity';
import { runChoropleth } from '$lib/data/query.js';
import { parseExpression, evaluateOverAreas, slugify } from '$lib/data/layer-calc.js';
import { selection } from './selection.svelte.js';
import { queryResult } from './query-result.svelte.js';

const STORAGE_KEY = 'nprz.layers.v1';

function newId() {
	return `l_${Math.random().toString(36).slice(2, 10)}`;
}

class LayersState {
	/** @type {Array<{
	 *   id: string, name: string, slug: string, kind: 'filter' | 'calc',
	 *   scale: string,
	 *   dataset?: string, year?: number, filters?: Record<string, number[]>,
	 *   expression?: string
	 * }>} */
	items = $state([]);
	/** @type {SvelteMap<string, Map<string, number>>} */
	results = new SvelteMap();
	/** @type {SvelteSet<string>} */
	loading = new SvelteSet();
	/** @type {SvelteMap<string, string>} */
	errors = new SvelteMap();
	activeId = $state(/** @type {string | null} */ (null));

	load() {
		if (typeof localStorage === 'undefined') return;
		try {
			const raw = localStorage.getItem(STORAGE_KEY);
			if (!raw) return;
			const parsed = JSON.parse(raw);
			if (Array.isArray(parsed?.items)) this.items = parsed.items;
			if (typeof parsed?.activeId === 'string' || parsed?.activeId === null) {
				this.activeId = parsed.activeId ?? null;
			}
		} catch {
			// ignore — corrupted storage just resets to empty
		}
	}

	persist() {
		if (typeof localStorage === 'undefined') return;
		try {
			localStorage.setItem(
				STORAGE_KEY,
				JSON.stringify({ items: this.items, activeId: this.activeId })
			);
		} catch {
			// quota / private mode — non-fatal
		}
	}

	slugTaken(slug, exceptId = null) {
		return this.items.some((i) => i.slug === slug && i.id !== exceptId);
	}

	/** Snapshot the current `selection` into a filter layer. Returns the new id, or null on conflict. */
	saveCurrent(name) {
		const slug = slugify(name);
		if (!slug || this.slugTaken(slug)) return null;
		const id = newId();
		this.items = [
			...this.items,
			{
				id,
				name: name.trim(),
				slug,
				kind: 'filter',
				scale: selection.scale,
				dataset: selection.dataset,
				year: selection.year,
				filters: structuredClone($state.snapshot(selection.filters))
			}
		];
		this.persist();
		this.refreshFilterLayer(id).then(() => this.recomputeCalcs());
		return id;
	}

	saveCalc(name, expression) {
		const slug = slugify(name);
		if (!slug || this.slugTaken(slug)) return null;
		// Validate parse + that all referenced symbols exist among same-scale layers.
		const { symbols } = parseExpression(expression);
		const known = new Set(this.items.filter((i) => i.scale === selection.scale).map((i) => i.slug));
		const unknown = symbols.filter((s) => !known.has(s));
		if (unknown.length) {
			throw new Error(`Unknown layer(s): ${unknown.join(', ')}`);
		}
		const id = newId();
		this.items = [
			...this.items,
			{ id, name: name.trim(), slug, kind: 'calc', scale: selection.scale, expression }
		];
		this.persist();
		this.computeCalcLayer(id);
		return id;
	}

	remove(id) {
		this.items = this.items.filter((i) => i.id !== id);
		this.results.delete(id);
		this.loading.delete(id);
		this.errors.delete(id);
		if (this.activeId === id) this.activeId = null;
		this.persist();
		this.recomputeCalcs();
	}

	setActive(id) {
		this.activeId = id;
		this.persist();
	}

	recomputeCalcs() {
		for (const l of this.items) {
			if (l.kind === 'calc' && l.scale === selection.scale) this.computeCalcLayer(l.id);
		}
	}

	async refreshFilterLayer(id) {
		const layer = this.items.find((i) => i.id === id);
		if (!layer || layer.kind !== 'filter') return;
		this.loading.add(id);
		this.errors.delete(id);
		try {
			const data = await runChoropleth({
				dataset: layer.dataset,
				scale: layer.scale,
				year: layer.year,
				filters: layer.filters
			});
			this.results.set(id, data);
		} catch (e) {
			this.errors.set(id, /** @type {Error} */ (e)?.message ?? String(e));
		} finally {
			this.loading.delete(id);
		}
	}

	computeCalcLayer(id) {
		const layer = this.items.find((i) => i.id === id);
		if (!layer || layer.kind !== 'calc') return;
		try {
			const { compiled, symbols } = parseExpression(layer.expression ?? '');
			// Resolve symbols → saved layer ids (same scale only).
			const slugToId = new Map(
				this.items
					.filter((i) => i.scale === layer.scale)
					.map((i) => /** @type {[string, string]} */ ([i.slug, i.id]))
			);
			const inputs = new SvelteMap();
			for (const s of symbols) {
				const depId = slugToId.get(s);
				const data = depId ? this.results.get(depId) : null;
				if (!data) {
					this.errors.set(id, `missing input: ${s}`);
					return;
				}
				inputs.set(s, data);
			}
			const out = evaluateOverAreas(compiled, symbols, inputs);
			this.results.set(id, out);
			this.errors.delete(id);
		} catch (e) {
			this.errors.set(id, /** @type {Error} */ (e)?.message ?? String(e));
		}
	}

	/** Re-run all filter layers at the current scale, then evaluate all calc layers.
	 *  If the active layer is on a different scale, clear it (fall back to live preview). */
	async refreshAll() {
		const scale = selection.scale;
		if (this.activeId) {
			const active = this.items.find((i) => i.id === this.activeId);
			if (active && active.scale !== scale) this.activeId = null;
		}
		const matching = this.items.filter((i) => i.scale === scale);
		for (const layer of matching) {
			if (layer.kind === 'filter') {
				await this.refreshFilterLayer(layer.id);
			}
		}
		for (const layer of matching) {
			if (layer.kind === 'calc') this.computeCalcLayer(layer.id);
		}
	}
}

export const layers = new LayersState();

// Read-only facade used by /  and /print to choose between the live preview
// (queryResult, driven by selection) and the active saved-layer result.
const EMPTY = /** @type {Map<string, number>} */ (new SvelteMap());
export const displayed = {
	get data() {
		const id = layers.activeId;
		if (!id) return queryResult.data;
		return layers.results.get(id) ?? EMPTY;
	},
	get loading() {
		const id = layers.activeId;
		if (!id) return queryResult.loading;
		return layers.loading.has(id);
	},
	get error() {
		const id = layers.activeId;
		if (!id) return queryResult.error;
		return layers.errors.get(id) ?? null;
	},
	get activeLayer() {
		const id = layers.activeId;
		if (!id) return null;
		return layers.items.find((i) => i.id === id) ?? null;
	}
};
