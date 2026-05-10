<script>
	import { onMount } from 'svelte';
	import { PUBLIC_PROTOMAPS_API_KEY } from '$env/static/public';
	import MapView from '$lib/map/Map.svelte';
	import ChoroplethLayer from '$lib/map/ChoroplethLayer.svelte';
	import BoundaryLayer from '$lib/map/BoundaryLayer.svelte';
	import LassoTool from '$lib/map/LassoTool.svelte';
	import FlowLayer from '$lib/map/FlowLayer.svelte';
	import Panel from '$lib/ui/Panel.svelte';
	import Field from '$lib/ui/Field.svelte';
	import ScaleToggle from '$lib/ui/ScaleToggle.svelte';
	import DatasetPicker from '$lib/ui/DatasetPicker.svelte';
	import YearPicker from '$lib/ui/YearPicker.svelte';
	import CategoryFilters from '$lib/ui/CategoryFilters.svelte';
	import SaveLayerInput from '$lib/ui/SaveLayerInput.svelte';
	import OverlayControls from '$lib/ui/OverlayControls.svelte';
	import StudyAreaControls from '$lib/ui/StudyAreaControls.svelte';
	import ClassificationControls from '$lib/ui/ClassificationControls.svelte';
	import LayerCalculator from '$lib/ui/LayerCalculator.svelte';
	import Legend from '$lib/cartography/Legend.svelte';
	import Histogram from '$lib/cartography/Histogram.svelte';
	import { runChoropleth } from '$lib/data/query.js';
	import { runFlows } from '$lib/data/flowQuery.js';
	import { loadManifest } from '$lib/data/manifest.js';
	import { classify } from '$lib/cartography/classify.js';
	import { paletteColors } from '$lib/cartography/palettes.js';
	import { stepExpression } from '$lib/cartography/expression.js';
	import { selection } from '$lib/state/selection.svelte.js';
	import { cartography } from '$lib/state/cartography.svelte.js';
	import { overlay } from '$lib/state/overlay.svelte.js';
	import { manifestState } from '$lib/state/manifest.svelte.js';
	import { queryResult } from '$lib/state/query-result.svelte.js';
	import { studyArea } from '$lib/state/study-area.svelte.js';
	import { layers, displayed } from '$lib/state/layers.svelte.js';
	import { flow, flowCartography } from '$lib/state/flow.svelte.js';

	let { data } = $props();
	let lassoActive = $state(false);

	onMount(() => {
		studyArea.init();
	});

	let manifest = $state(null);
	let valueByArea = $state(new Map());
	let querying = $state(false);
	let lastQueryMs = $state(null);
	let error = $state(/** @type {string | null} */ (null));

	let centroids = $state(/** @type {Record<string, [number,number]> | null} */ (null));
	let flowResult = $state(/** @type {{flows:{o:string,d:string,value:number}[], min:number, max:number} | null} */ (null));
	let flowQuerying = $state(false);
	let flowError = $state(/** @type {string | null} */ (null));
	// Auto-set minWeight to the ~70th percentile on the first non-empty flow
	// query so the user lands on the top ~30% of flows. Subsequent queries keep
	// the user's slider position (clamped to the new max).
	let flowMinWeightInitialized = false;
	const FLOW_DEFAULT_TOP_FRACTION = 0.3;

	// Load manifest once.
	$effect(() => {
		loadManifest()
			.then((m) => {
				manifest = m;
			})
			.catch((e) => {
				error = `manifest: ${e.message}`;
			});
	});

	// Load gemeente centroids once (used by FlowLayer to draw OD curves).
	$effect(() => {
		const path = manifest?.geo?.gem?.centroids;
		if (!path || centroids) return;
		fetch(`/data/${path}`)
			.then((r) => {
				if (!r.ok) throw new Error(`HTTP ${r.status}`);
				return r.json();
			})
			.then((json) => {
				centroids = json;
			})
			.catch((e) => {
				flowError = `centroids: ${e.message}`;
			});
	});

	// Re-run choropleth query whenever any node selection field changes.
	$effect(() => {
		studyArea.bindToScale(selection.scale);
	});

	const manifest = $derived(manifestState.data);
	// Re-run flow query whenever flow selection changes (only while enabled).
	// Note: flow.minWeight is a client-side filter (see filteredFlows below).
	$effect(() => {
		if (!manifest || !flow.enabled) {
			flowResult = null;
			return;
		}
		const args = {
			dataset: flow.dataset,
			scale: flow.scale,
			yearMin: flow.yearMin,
			yearMax: flow.yearMax,
			filters: flow.filters,
			includeSelfLoops: flow.includeSelfLoops
		};
		flowQuerying = true;
		flowError = null;
		runFlows(args)
			.then((res) => {
				flowResult = res;
				if (!flowMinWeightInitialized && res.flows.length > 0) {
					const sorted = res.flows.map((f) => f.value).sort((a, b) => a - b);
					const idx = Math.floor(sorted.length * (1 - FLOW_DEFAULT_TOP_FRACTION));
					flow.minWeight = sorted[idx] ?? 0;
					flowMinWeightInitialized = true;
				} else if (flow.minWeight > res.max) {
					// New result doesn't reach the user's threshold — drop to 0.
					flow.minWeight = 0;
				}
			})
			.catch((e) => {
				flowError = `flow query: ${e.message}`;
				flowResult = null;
			})
			.finally(() => {
				flowQuerying = false;
			});
	});

	const status = $derived.by(() => {
		if (displayed.error) return displayed.error;
		if (manifestState.error) return manifestState.error;
		if (displayed.loading || manifestState.loading) return 'querying…';
		const unit = selection.scale === 'pc4' ? 'PC4s' : 'gemeenten';
		const active = displayed.activeLayer;
		const prefix = active ? `${active.name}: ` : '';
		return `${prefix}${displayed.data.size.toLocaleString()} ${unit}`;
	});

	const filteredFlows = $derived(
		flowResult ? flowResult.flows.filter((f) => f.value >= flow.minWeight) : []
	);

	const flowStatus = $derived.by(() => {
		if (flowError) return flowError;
		if (flowQuerying) return 'querying…';
		if (!flowResult) return null;
		const total = flowResult.flows.length;
		const shown = filteredFlows.length;
		return shown === total
			? `${total.toLocaleString()} flows`
			: `${shown.toLocaleString()} / ${total.toLocaleString()} flows`;
	});

	const sortedValues = $derived(
		[...displayed.data.values()].filter((v) => Number.isFinite(v) && v > 0)
	);

	const breaks = $derived.by(() => {
		if (sortedValues.length === 0) return null;
		return classify(sortedValues, { method: cartography.method, n: cartography.n });
	});

	const colors = $derived(breaks ? paletteColors(cartography.palette, cartography.n) : []);
	const fillColor = $derived(breaks ? stepExpression({ breaks, colors }) : '#eee');

	const flowValues = $derived(
		filteredFlows.map((f) => f.value).filter((v) => Number.isFinite(v) && v > 0)
	);

	const flowBreaks = $derived.by(() => {
		if (flowValues.length === 0) return null;
		return classify(flowValues, { method: flowCartography.method, n: flowCartography.n });
	});

	const flowColors = $derived(
		flowBreaks ? paletteColors(flowCartography.palette, flowCartography.n) : []
	);

	// Min-weight slider bounds — driven by the current full result so the slider
	// doesn't jump as the user drags it.
	const flowMaxValue = $derived(flowResult?.max ?? 0);
	const flowSliderStep = $derived(
		flowMaxValue > 0 ? Math.max(flowMaxValue / 200, 0.01) : 1
	);

	// Geo selectors driven by current scale.
	const geoMain = $derived(manifest?.geo?.[selection.scale]);
	const geoOverlay = $derived(overlay.scale ? manifest?.geo?.[overlay.scale] : null);
</script>

<div style="position: fixed; inset: 0;">
	<MapView center={[5.3, 52.1]} zoom={7} apiKey={PUBLIC_PROTOMAPS_API_KEY} theme="white">
		{#if manifest && geoMain}
			{#key selection.scale}
				<ChoroplethLayer
					sourceId="choropleth-{selection.scale}"
					geoUrl="/data/{geoMain.geojson}"
					promoteId={geoMain.idProp}
					valueByArea={displayed.data}
					selectedIds={studyArea.ids}
					{fillColor}
					fillOpacity={cartography.fillOpacity}
					lineColor={cartography.lineColor}
					lineWidth={cartography.lineWidth}
				/>
				<LassoTool
					active={lassoActive}
					fillLayerId="choropleth-{selection.scale}-fill"
				/>
			{/key}
			{#if geoOverlay}
				{#key overlay.scale}
					<BoundaryLayer
						sourceId="overlay-{overlay.scale}"
						geoUrl="/data/{geoOverlay.geojson}"
						promoteId={geoOverlay.idProp}
						lineColor={overlay.color}
						lineWidth={overlay.width}
						lineOpacity={overlay.opacity}
					/>
				{/key}
			{/if}
			{#if flow.enabled && filteredFlows.length && flowBreaks && centroids}
				<FlowLayer
					sourceId="flow-{flow.dataset}-{flow.scale}"
					flows={filteredFlows}
					{centroids}
					breaks={flowBreaks}
					colors={flowColors}
					widthMin={flowCartography.widthMin}
					widthMax={flowCartography.widthMax}
					opacity={flowCartography.opacity}
					curvature={flowCartography.curvature}
				/>
			{/if}
		{/if}
	</MapView>
</div>

<div class="sidebar">
	<div class="header">
		<div class="brand-row">
			<div class="brand">NPRZ <span class="brand-sub">analytics</span></div>
			<div class="actions">
				<a class="action" href="/print" title="Print preview">⎙</a>
				{#if data.user}
					<form method="POST" action="?/logout" class="logout-form">
						<button type="submit" class="action" title="Sign out — {data.user.email}">↪</button>
					</form>
				{/if}
			</div>
		</div>
		<div
			class="status"
			class:busy={displayed.loading}
			class:err={displayed.error || manifestState.error}
		>
			{status}
		</div>
		<div class="status" class:busy={querying} class:err={error}>{status}</div>
		{#if flowStatus}
			<div class="status" class:busy={flowQuerying} class:err={flowError}>flow: {flowStatus}</div>
		{/if}
	</div>

	<Panel title="Scale">
		<ScaleToggle />
	</Panel>

	<Panel title="Node data">
		{#if manifest}
			<div class="stack">
				<DatasetPicker {manifest} />
				<YearPicker {manifest} />
				<CategoryFilters {manifest} />
				<div class="save-divider"></div>
				<SaveLayerInput {manifest} />
			</div>
		{:else}
			<p class="hint">Loading manifest…</p>
		{/if}
	</Panel>

	<Panel title="Layers" open={layers.items.length > 0}>
		<LayerCalculator {manifest} />
	</Panel>

	<Panel title="Cartography">
	<Panel title="Flow data" open={false}>
		{#if manifest}
			<div class="stack">
				<Field label="Show flows">
					<input type="checkbox" bind:checked={flow.enabled} />
				</Field>
				<DatasetPicker {manifest} state={flow} section="flows" />
				<YearPicker {manifest} state={flow} section="flows" />
				<CategoryFilters {manifest} state={flow} section="flows" />
				<Field label="Min weight" value={flow.minWeight.toFixed(flowMaxValue < 100 ? 1 : 0)}>
					<input
						type="range"
						min="0"
						max={flowMaxValue || 1}
						step={flowSliderStep}
						bind:value={flow.minWeight}
						disabled={!flowResult || flowMaxValue === 0}
					/>
				</Field>
				<Field label="Self-loops">
					<input type="checkbox" bind:checked={flow.includeSelfLoops} />
				</Field>
			</div>
		{:else}
			<p class="hint">Loading manifest…</p>
		{/if}
	</Panel>

	<Panel title="Node cartography">
		<div class="stack">
			<ClassificationControls />
			{#if breaks && sortedValues.length}
				<Histogram values={sortedValues} {breaks} {colors} />
			{/if}
			{#if breaks}
				<Legend {breaks} {colors} />
			{/if}
		</div>
	</Panel>

	<Panel title="Study area" open={false}>
		<StudyAreaControls bind:lassoActive />
	<Panel title="Flow cartography" open={false}>
		<div class="stack">
			<ClassificationControls target={flowCartography} />
			{#if flowBreaks && flowValues.length}
				<Histogram values={flowValues} breaks={flowBreaks} colors={flowColors} />
			{/if}
			{#if flowBreaks}
				<Legend breaks={flowBreaks} colors={flowColors} />
			{/if}
		</div>
	</Panel>

	<Panel title="Boundary overlay" open={false}>
		<OverlayControls />
	</Panel>
</div>

{#if queryResult.lastMs !== null}
	<div class="debug" title="Last query duration">{queryResult.lastMs} ms</div>
{/if}

<style>
	.sidebar {
		position: fixed;
		top: var(--spacing-4);
		left: var(--spacing-4);
		z-index: 1;
		display: flex;
		flex-direction: column;
		gap: var(--spacing-2);
		width: 300px;
		max-height: calc(100vh - 2 * var(--spacing-4));
		overflow-y: auto;
	}
	.header {
		padding: var(--spacing-2) var(--spacing-3);
		background: var(--color-bg-panel);
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
	}
	.brand {
		font-size: var(--text-base);
		font-weight: 600;
		color: var(--color-text);
		letter-spacing: 0.02em;
	}
	.brand-sub {
		color: var(--color-muted);
		font-weight: 400;
	}
	.brand-row {
		display: flex;
		align-items: baseline;
		justify-content: space-between;
		gap: var(--spacing-2);
	}
	.actions {
		display: flex;
		gap: 4px;
		align-items: center;
	}
	.logout-form {
		margin: 0;
	}
	.action {
		background: transparent;
		border: none;
		color: var(--color-hint);
		cursor: pointer;
		font-size: var(--text-sm);
		padding: 0 4px;
		text-decoration: none;
	}
	.action:hover {
		color: var(--color-text);
	}
	.status {
		font-size: var(--text-xs);
		color: var(--color-muted);
		font-variant-numeric: tabular-nums;
		margin-top: 2px;
	}
	.status.busy {
		color: var(--color-hint);
	}
	.status.err {
		color: #cf222e;
	}
	.stack {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-3);
	}
	.hint {
		color: var(--color-hint);
		font-size: var(--text-sm);
		margin: 0;
	}
	.save-divider {
		border-top: 1px solid var(--color-line);
	}
	.debug {
		position: fixed;
		bottom: var(--spacing-2);
		left: var(--spacing-2);
		z-index: 1;
		padding: 2px var(--spacing-2);
		background: rgba(255, 255, 255, 0.85);
		color: var(--color-hint);
		font-size: var(--text-xs);
		font-variant-numeric: tabular-nums;
		border-radius: var(--radius);
		pointer-events: none;
	}
</style>
