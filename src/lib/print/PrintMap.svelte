<script>
	import { feature } from 'topojson-client';
	import { geoPath } from 'd3-geo';
	import { rdProjection } from './projection.js';

	/**
	 * @typedef {Object} Props
	 * @property {string} topojsonUrl
	 * @property {string} objectKey - top-level objects[<key>] in the topojson
	 * @property {Map<string, number>} valueByArea
	 * @property {number[] | null} breaks
	 * @property {string[]} colors
	 * @property {number} [width]
	 * @property {number} [height]
	 * @property {string} [strokeColor]
	 * @property {number} [strokeWidth]
	 * @property {string} [nullColor]
	 * @property {string} [idProp]
	 */
	let {
		topojsonUrl,
		objectKey = null,
		valueByArea,
		breaks,
		colors,
		width = 800,
		height = 1000,
		strokeColor = '#666',
		strokeWidth = 0.2,
		nullColor = '#eee',
		idProp = 'area_code'
	} = $props();

	// Derive objectKey from the URL when not explicitly given.
	// `geo/pc4.topo.json` → `pc4`. The R pipeline writes the same key.
	const resolvedKey = $derived(objectKey ?? topojsonUrl.split('/').pop()?.split('.')[0] ?? null);

	let topo = $state(null);
	let topoError = $state(/** @type {string | null} */ (null));

	$effect(() => {
		topo = null;
		topoError = null;
		fetch(topojsonUrl)
			.then((r) => {
				if (!r.ok) throw new Error(`HTTP ${r.status}`);
				return r.json();
			})
			.then((t) => {
				topo = t;
			})
			.catch((e) => {
				topoError = e.message;
			});
	});

	const features = $derived.by(() => {
		if (!topo || !resolvedKey) return null;
		const obj = topo.objects?.[resolvedKey];
		if (!obj) return null;
		return feature(topo, obj);
	});

	const path = $derived.by(() => {
		if (!features) return null;
		return geoPath(rdProjection([width, height], features));
	});

	function fillFor(value) {
		if (value == null || !breaks || breaks.length < 2) return nullColor;
		// breaks is length n+1; classes are [breaks[i], breaks[i+1])
		for (let i = 1; i < breaks.length - 1; i++) {
			if (value < breaks[i]) return colors[i - 1];
		}
		return colors[colors.length - 1];
	}
</script>

{#if topoError}
	<p class="err">Failed to load {topojsonUrl}: {topoError}</p>
{:else if !features || !path}
	<p class="hint">Loading map…</p>
{:else}
	<svg
		viewBox="0 0 {width} {height}"
		preserveAspectRatio="xMidYMid meet"
		xmlns="http://www.w3.org/2000/svg"
	>
		<g class="features">
			{#each features.features as f, i (f.properties?.[idProp] ?? i)}
				<path
					d={path(f)}
					fill={fillFor(valueByArea.get(f.properties?.[idProp]))}
					stroke={strokeColor}
					stroke-width={strokeWidth}
					stroke-linejoin="round"
				/>
			{/each}
		</g>
	</svg>
{/if}

<style>
	svg {
		width: 100%;
		height: auto;
		display: block;
	}
	.err {
		color: #cf222e;
		font-size: var(--text-sm);
	}
	.hint {
		color: var(--color-hint);
		font-size: var(--text-sm);
	}
</style>
