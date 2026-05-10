<script>
	import { onMount, onDestroy } from 'svelte';
	import { getMapContext } from './context.js';
	import { bezierLine } from '$lib/cartography/curve.js';
	import { stepExpression } from '$lib/cartography/expression.js';

	let {
		sourceId = 'flow',
		flows = [],
		centroids = {},
		breaks,
		colors,
		widthMin = 0.5,
		widthMax = 8,
		opacity = 0.75,
		curvature = 0.2
	} = $props();

	const ctx = getMapContext();
	const casingId = $derived(`${sourceId}-casing`);
	const lineId = $derived(`${sourceId}-line`);
	let installed = false;

	function classIndex(value, bks) {
		const n = bks.length - 1;
		for (let i = 1; i < n; i++) if (value < bks[i]) return i - 1;
		return n - 1;
	}

	const features = $derived.by(() => {
		if (!flows.length || !breaks) return [];
		// Sort descending so heavy flows are added first (drawn underneath) and
		// lighter flows end up on top — keeps small flows from being obscured.
		const sorted = [...flows].sort((a, b) => b.value - a.value);
		const out = [];
		for (const f of sorted) {
			const o = centroids[f.o];
			const d = centroids[f.d];
			if (!o || !d) continue;
			out.push({
				type: 'Feature',
				geometry: { type: 'LineString', coordinates: bezierLine(o, d, { curvature }) },
				properties: { value: f.value, classIdx: classIndex(f.value, breaks) }
			});
		}
		return out;
	});

	const featureCollection = $derived({ type: 'FeatureCollection', features });

	const colorExpr = $derived.by(() => {
		if (!breaks || !colors || colors.length === 0) return '#888';
		return stepExpression({ breaks, colors, input: ['get', 'value'] });
	});

	const widthExpr = $derived.by(() => {
		if (!colors || colors.length === 0) return widthMin;
		const n = colors.length;
		if (n === 1) return widthMax;
		return ['interpolate', ['linear'], ['get', 'classIdx'], 0, widthMin, n - 1, widthMax];
	});

	// Casing: scaled width for a subtle white halo behind the main line.
	const casingWidthExpr = $derived.by(() => {
		if (!colors || colors.length === 0) return widthMin * 2;
		const n = colors.length;
		if (n === 1) return widthMax * 2;
		return ['interpolate', ['linear'], ['get', 'classIdx'], 0, widthMin * 2.5, n - 1, widthMax * 2];
	});

	const casingOpacity = $derived(Math.min(1, opacity * 0.5));

	onMount(() => {
		const map = ctx.map;
		if (!map) return;
		if (!map.getSource(sourceId)) {
			map.addSource(sourceId, { type: 'geojson', data: featureCollection });
		}
		map.addLayer({
			id: casingId,
			type: 'line',
			source: sourceId,
			paint: {
				'line-color': '#ffffff',
				'line-width': casingWidthExpr,
				'line-opacity': casingOpacity,
				'line-blur': 0.5
			},
			layout: { 'line-cap': 'round', 'line-join': 'round' }
		});
		map.addLayer({
			id: lineId,
			type: 'line',
			source: sourceId,
			paint: {
				'line-color': colorExpr,
				'line-width': widthExpr,
				'line-opacity': opacity
			},
			layout: { 'line-cap': 'round', 'line-join': 'round' }
		});
		installed = true;
	});

	// Push new geometry into the source whenever the derived collection changes.
	$effect(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		const src = map.getSource(sourceId);
		if (src) src.setData(featureCollection);
	});

	// Restyle without remounting on paint changes.
	$effect(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		map.setPaintProperty(lineId, 'line-color', colorExpr);
		map.setPaintProperty(lineId, 'line-width', widthExpr);
		map.setPaintProperty(lineId, 'line-opacity', opacity);
		map.setPaintProperty(casingId, 'line-width', casingWidthExpr);
		map.setPaintProperty(casingId, 'line-opacity', casingOpacity);
	});

	onDestroy(() => {
		const map = ctx.map;
		if (!map) return;
		if (map.getLayer(lineId)) map.removeLayer(lineId);
		if (map.getLayer(casingId)) map.removeLayer(casingId);
		if (map.getSource(sourceId)) map.removeSource(sourceId);
	});
</script>
