<script>
	// Wires map mousemove/click against the choropleth fill + flow line layers to
	// the shared `ui` inspect state. Mounted inside <MapView> so it has access to
	// the map context. Layer IDs are passed in by the parent so we can match the
	// current scale's source.
	//
	// `nodeScale` and `flowScale` enable cross-scale flow filtering: when the
	// user clicks a PC4 polygon while flows are gem-scale, we resolve the click
	// point to the containing gemeente (via geoNames spatial lookup) and write
	// that into `ui.selectedFlowNode`, leaving the PC4 as the inspect target.
	import { onMount, onDestroy } from 'svelte';
	import { getMapContext } from './context.js';
	import { ui } from '$lib/state/ui.svelte.js';
	import { geoNames } from '$lib/state/geo-names.svelte.js';

	let {
		nodeFillLayerId,
		flowLineLayerId = null,
		nodeScale = 'gem',
		flowScale = 'gem',
		flowEnabled = false
	} = $props();

	const ctx = getMapContext();
	let canvas;
	let handlers = null;

	function targetLayers() {
		const map = ctx.map;
		if (!map) return [];
		const out = [];
		if (nodeFillLayerId && map.getLayer(nodeFillLayerId)) out.push(nodeFillLayerId);
		if (flowLineLayerId && map.getLayer(flowLineLayerId)) out.push(flowLineLayerId);
		return out;
	}

	function featureToTarget(feat) {
		if (!feat) return null;
		if (feat.layer?.id === flowLineLayerId) {
			const o = feat.properties?.o;
			const d = feat.properties?.d;
			if (o == null || d == null) return null;
			return { kind: /** @type {const} */ ('flow'), o: String(o), d: String(d) };
		}
		// Choropleth: promoted id surfaces as feat.id.
		const id = feat.id;
		if (id == null) return null;
		return { kind: /** @type {const} */ ('node'), id: String(id) };
	}

	onMount(() => {
		const map = ctx.map;
		if (!map) return;
		canvas = map.getCanvas();

		const onMove = (e) => {
			const layers = targetLayers();
			if (layers.length === 0) {
				ui.hovered = null;
				if (canvas) canvas.style.cursor = '';
				return;
			}
			const feats = map.queryRenderedFeatures(e.point, { layers });
			const tgt = featureToTarget(feats[0]);
			ui.hovered = tgt;
			if (canvas) canvas.style.cursor = tgt ? 'pointer' : '';
		};

		const onLeave = () => {
			ui.hovered = null;
			if (canvas) canvas.style.cursor = '';
		};

		const onClick = (e) => {
			const layers = targetLayers();
			if (layers.length === 0) {
				ui.selected = null;
				ui.selectedFlowNode = null;
				return;
			}
			const feats = map.queryRenderedFeatures(e.point, { layers });
			const target = featureToTarget(feats[0]);

			// Toggle: clicking the already-selected feature clears the selection
			// (and the flow filter), so re-clicking a pinned node resets the view.
			const cur = ui.selected;
			const sameAsCurrent =
				cur &&
				target &&
				cur.kind === target.kind &&
				(cur.kind === 'node'
					? cur.id === target.id
					: cur.o === target.o && cur.d === target.d);
			if (sameAsCurrent) {
				ui.selected = null;
				ui.selectedFlowNode = null;
				return;
			}

			ui.selected = target;

			// Compute the flow-filter node. For a node click at the same scale as
			// flows: use the clicked id directly. For a node click on a different
			// scale: resolve the click coordinate to the containing flow-scale
			// polygon. Flow-feature clicks don't drive the flow filter.
			if (!flowEnabled || !target || target.kind === 'flow') {
				ui.selectedFlowNode = null;
			} else if (nodeScale === flowScale) {
				ui.selectedFlowNode = target.id;
			} else {
				const resolved = geoNames.resolveCoord(flowScale, e.lngLat.lng, e.lngLat.lat);
				ui.selectedFlowNode = resolved;
			}
		};

		const onKey = (ev) => {
			if (ev.key === 'Escape') {
				ui.selected = null;
				ui.selectedFlowNode = null;
			}
		};

		map.on('mousemove', onMove);
		map.on('mouseout', onLeave);
		map.on('click', onClick);
		if (typeof window !== 'undefined') window.addEventListener('keydown', onKey);

		handlers = { onMove, onLeave, onClick, onKey };
	});

	onDestroy(() => {
		const map = ctx.map;
		if (!map || !handlers) return;
		map.off('mousemove', handlers.onMove);
		map.off('mouseout', handlers.onLeave);
		map.off('click', handlers.onClick);
		if (typeof window !== 'undefined') window.removeEventListener('keydown', handlers.onKey);
		if (canvas) canvas.style.cursor = '';
	});
</script>
