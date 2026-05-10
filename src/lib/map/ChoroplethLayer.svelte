<script>
	import { onMount, onDestroy } from 'svelte';
	import { getMapContext } from './context.js';

	let {
		sourceId,
		geoUrl,
		promoteId = 'area_code',
		valueByArea = new Map(),
		// Step 5 will replace this with a classification-driven `step` expression.
		fillColor = [
			'case',
			['==', ['feature-state', 'value'], null],
			'#eee',
			[
				'interpolate',
				['linear'],
				['feature-state', 'value'],
				0,
				'#fee5d9',
				50000,
				'#fb6a4a',
				200000,
				'#a50f15'
			]
		],
		fillOpacity = 0.75,
		lineColor = '#666',
		lineWidth = 0.4
	} = $props();

	const ctx = getMapContext();
	const fillId = $derived(`${sourceId}-fill`);
	const lineId = $derived(`${sourceId}-line`);
	let installed = false;
	let sourceLoaded = false;

	function applyValues() {
		const map = ctx.map;
		if (!map || !installed || !sourceLoaded) return;
		for (const [areaCode, value] of valueByArea) {
			map.setFeatureState({ source: sourceId, id: areaCode }, { value });
		}
	}

	onMount(() => {
		const map = ctx.map;
		if (!map) return;

		if (!map.getSource(sourceId)) {
			map.addSource(sourceId, { type: 'geojson', data: geoUrl, promoteId });
		}
		map.addLayer({
			id: fillId,
			type: 'fill',
			source: sourceId,
			paint: { 'fill-color': fillColor, 'fill-opacity': fillOpacity }
		});
		map.addLayer({
			id: lineId,
			type: 'line',
			source: sourceId,
			paint: { 'line-color': lineColor, 'line-width': lineWidth }
		});
		installed = true;

		const onSourceData = (e) => {
			if (e.sourceId === sourceId && e.isSourceLoaded) {
				sourceLoaded = true;
				applyValues();
			}
		};
		map.on('sourcedata', onSourceData);

		return () => map.off('sourcedata', onSourceData);
	});

	$effect(() => {
		void valueByArea;
		applyValues();
	});

	$effect(() => {
		const map = ctx.map;
		if (!map || !installed) return;
		map.setPaintProperty(fillId, 'fill-color', fillColor);
		map.setPaintProperty(fillId, 'fill-opacity', fillOpacity);
		map.setPaintProperty(lineId, 'line-color', lineColor);
		map.setPaintProperty(lineId, 'line-width', lineWidth);
	});

	onDestroy(() => {
		const map = ctx.map;
		if (!map) return;
		if (map.getLayer(fillId)) map.removeLayer(fillId);
		if (map.getLayer(lineId)) map.removeLayer(lineId);
		if (map.getSource(sourceId)) map.removeSource(sourceId);
	});
</script>
