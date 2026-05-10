<script>
	import { onMount } from 'svelte';
	import { PUBLIC_PROTOMAPS_API_KEY } from '$env/static/public';
	import MapView from '$lib/map/Map.svelte';
	import ChoroplethLayer from '$lib/map/ChoroplethLayer.svelte';
	import BoundaryLayer from '$lib/map/BoundaryLayer.svelte';
	import LassoTool from '$lib/map/LassoTool.svelte';
	import Panel from '$lib/ui/Panel.svelte';
	import ScaleToggle from '$lib/ui/ScaleToggle.svelte';
	import DatasetPicker from '$lib/ui/DatasetPicker.svelte';
	import YearPicker from '$lib/ui/YearPicker.svelte';
	import CategoryFilters from '$lib/ui/CategoryFilters.svelte';
	import OverlayControls from '$lib/ui/OverlayControls.svelte';
	import StudyAreaControls from '$lib/ui/StudyAreaControls.svelte';
	import ClassificationControls from '$lib/ui/ClassificationControls.svelte';
	import Legend from '$lib/cartography/Legend.svelte';
	import Histogram from '$lib/cartography/Histogram.svelte';
	import { classify } from '$lib/cartography/classify.js';
	import { paletteColors } from '$lib/cartography/palettes.js';
	import { stepExpression } from '$lib/cartography/expression.js';
	import { selection } from '$lib/state/selection.svelte.js';
	import { cartography } from '$lib/state/cartography.svelte.js';
	import { overlay } from '$lib/state/overlay.svelte.js';
	import { manifestState } from '$lib/state/manifest.svelte.js';
	import { queryResult } from '$lib/state/query-result.svelte.js';
	import { studyArea } from '$lib/state/study-area.svelte.js';

	let { data } = $props();
	let lassoActive = $state(false);

	onMount(() => {
		studyArea.init();
	});

	$effect(() => {
		studyArea.bindToScale(selection.scale);
	});

	const manifest = $derived(manifestState.data);

	const status = $derived.by(() => {
		if (queryResult.error) return queryResult.error;
		if (manifestState.error) return manifestState.error;
		if (queryResult.loading || manifestState.loading) return 'querying…';
		const unit = selection.scale === 'pc4' ? 'PC4s' : 'gemeenten';
		return `${queryResult.data.size.toLocaleString()} ${unit}`;
	});

	const sortedValues = $derived(
		[...queryResult.data.values()].filter((v) => Number.isFinite(v) && v > 0)
	);

	const breaks = $derived.by(() => {
		if (sortedValues.length === 0) return null;
		return classify(sortedValues, { method: cartography.method, n: cartography.n });
	});

	const colors = $derived(breaks ? paletteColors(cartography.palette, cartography.n) : []);
	const fillColor = $derived(breaks ? stepExpression({ breaks, colors }) : '#eee');

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
					valueByArea={queryResult.data}
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
			class:busy={queryResult.loading}
			class:err={queryResult.error || manifestState.error}
		>
			{status}
		</div>
	</div>

	<Panel title="Scale">
		<ScaleToggle />
	</Panel>

	<Panel title="Data">
		{#if manifest}
			<div class="stack">
				<DatasetPicker {manifest} />
				<YearPicker {manifest} />
				<CategoryFilters {manifest} />
			</div>
		{:else}
			<p class="hint">Loading manifest…</p>
		{/if}
	</Panel>

	<Panel title="Cartography">
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
