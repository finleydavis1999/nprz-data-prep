<script>
	import { onMount, onDestroy } from 'svelte';
	import { SvelteSet } from 'svelte/reactivity';
	import { getMapContext } from './context.js';
	import { studyArea } from '$lib/state/study-area.svelte.js';

	let { active = false, fillLayerId } = $props();

	const ctx = getMapContext();
	const SRC_ID = 'lasso-path';
	const LINE_ID = 'lasso-path-line';
	const FILL_ID = 'lasso-path-fill';
	// MapLibre paint expressions can't read CSS custom properties at runtime, so
	// the lasso polyline color is a literal here.
	const LASSO_COLOR = '#1f6feb';

	let installed = false;
	let drawing = false;
	let coords = [];
	let modifier = 'replace';

	function emptyPath() {
		return { type: 'Feature', geometry: { type: 'LineString', coordinates: [] }, properties: {} };
	}
	function pathFeature() {
		return {
			type: 'Feature',
			geometry: { type: 'LineString', coordinates: coords.slice() },
			properties: {}
		};
	}
	function setData(feature) {
		const map = ctx.map;
		const src = map?.getSource(SRC_ID);
		if (src) src.setData(feature ?? emptyPath());
	}
	function bbox(ring) {
		let minX = Infinity,
			minY = Infinity,
			maxX = -Infinity,
			maxY = -Infinity;
		for (const [x, y] of ring) {
			if (x < minX) minX = x;
			if (y < minY) minY = y;
			if (x > maxX) maxX = x;
			if (y > maxY) maxY = y;
		}
		return [
			[minX, minY],
			[maxX, maxY]
		];
	}
	function pointInRing([x, y], ring) {
		let inside = false;
		for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
			const [xi, yi] = ring[i];
			const [xj, yj] = ring[j];
			const intersect = yi > y !== yj > y && x < ((xj - xi) * (y - yi)) / (yj - yi + 0.0) + xi;
			if (intersect) inside = !inside;
		}
		return inside;
	}
	function representativePoint(geom) {
		if (geom.type === 'Polygon') {
			const ring = geom.coordinates[0];
			let sx = 0,
				sy = 0;
			for (const [x, y] of ring) {
				sx += x;
				sy += y;
			}
			return [sx / ring.length, sy / ring.length];
		}
		if (geom.type === 'MultiPolygon') {
			let best = geom.coordinates[0][0];
			for (const poly of geom.coordinates) if (poly[0].length > best.length) best = poly[0];
			let sx = 0,
				sy = 0;
			for (const [x, y] of best) {
				sx += x;
				sy += y;
			}
			return [sx / best.length, sy / best.length];
		}
		return null;
	}
	function reset() {
		drawing = false;
		coords = [];
		setData(null);
	}
	function finalize() {
		const map = ctx.map;
		if (!map || coords.length < 3) {
			reset();
			return;
		}
		const ring = coords.slice();
		ring.push(ring[0]);
		const [sw, ne] = bbox(ring);
		const swPx = map.project(sw);
		const nePx = map.project(ne);
		const pixelBox = [
			[Math.min(swPx.x, nePx.x), Math.min(swPx.y, nePx.y)],
			[Math.max(swPx.x, nePx.x), Math.max(swPx.y, nePx.y)]
		];
		const features = map.queryRenderedFeatures(pixelBox, { layers: [fillLayerId] });
		const hits = [];
		const seen = new SvelteSet();
		for (const f of features) {
			const rep = representativePoint(f.geometry);
			if (!rep) continue;
			if (!pointInRing(rep, ring)) continue;
			const id = String(f.id);
			if (seen.has(id)) continue;
			seen.add(id);
			hits.push(id);
		}
		if (modifier === 'add') studyArea.addMany(hits);
		else if (modifier === 'subtract') studyArea.removeMany(hits);
		else studyArea.replace(hits);
		reset();
	}

	function onMouseDown(e) {
		if (!active) return;
		const map = ctx.map;
		if (!map) return;
		e.preventDefault();
		modifier = e.originalEvent?.shiftKey ? 'add' : e.originalEvent?.altKey ? 'subtract' : 'replace';
		map.dragPan.disable();
		drawing = true;
		coords = [[e.lngLat.lng, e.lngLat.lat]];
		setData(pathFeature());
	}
	function onMouseMove(e) {
		if (!drawing) return;
		coords.push([e.lngLat.lng, e.lngLat.lat]);
		setData(pathFeature());
	}
	function onMouseUp() {
		const map = ctx.map;
		if (!map || !drawing) return;
		map.dragPan.enable();
		finalize();
	}

	$effect(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		const canvas = map.getCanvas();
		if (active) {
			canvas.style.cursor = 'crosshair';
		} else {
			canvas.style.cursor = '';
			if (drawing) {
				map.dragPan.enable();
				reset();
			}
		}
	});

	onMount(() => {
		const map = ctx.map;
		if (!map) return;
		if (!map.getSource(SRC_ID)) {
			map.addSource(SRC_ID, { type: 'geojson', data: emptyPath() });
		}
		map.addLayer({
			id: FILL_ID,
			type: 'fill',
			source: SRC_ID,
			paint: { 'fill-color': LASSO_COLOR, 'fill-opacity': 0.08 },
			filter: ['==', ['geometry-type'], 'Polygon']
		});
		map.addLayer({
			id: LINE_ID,
			type: 'line',
			source: SRC_ID,
			paint: { 'line-color': LASSO_COLOR, 'line-width': 1.5, 'line-dasharray': [2, 2] }
		});
		installed = true;
		map.on('mousedown', onMouseDown);
		map.on('mousemove', onMouseMove);
		map.on('mouseup', onMouseUp);
	});

	onDestroy(() => {
		const map = ctx.map;
		if (!map) return;
		map.off('mousedown', onMouseDown);
		map.off('mousemove', onMouseMove);
		map.off('mouseup', onMouseUp);
		if (drawing) map.dragPan.enable();
		map.getCanvas().style.cursor = '';
		if (map.getLayer(LINE_ID)) map.removeLayer(LINE_ID);
		if (map.getLayer(FILL_ID)) map.removeLayer(FILL_ID);
		if (map.getSource(SRC_ID)) map.removeSource(SRC_ID);
	});
</script>
