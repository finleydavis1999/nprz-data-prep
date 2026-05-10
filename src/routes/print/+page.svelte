<script>
	import PrintMap from '$lib/print/PrintMap.svelte';
	import Legend from '$lib/cartography/Legend.svelte';
	import { downloadSvg } from '$lib/print/export.js';
	import { classify } from '$lib/cartography/classify.js';
	import { paletteColors } from '$lib/cartography/palettes.js';
	import { selection } from '$lib/state/selection.svelte.js';
	import { cartography } from '$lib/state/cartography.svelte.js';
	import { manifestState } from '$lib/state/manifest.svelte.js';
	import { queryResult } from '$lib/state/query-result.svelte.js';

	let title = $state('');
	let mapWrap;

	const manifest = $derived(manifestState.data);
	const dataset = $derived(manifest?.datasets?.[selection.dataset]);
	const geo = $derived(manifest?.geo?.[selection.scale]);
	const yearLabel = $derived(
		dataset?.fields?.year?.values?.find((y) => y.id === selection.year)?.label ?? selection.year
	);

	const sortedValues = $derived(
		[...queryResult.data.values()].filter((v) => Number.isFinite(v) && v > 0)
	);
	const breaks = $derived.by(() => {
		if (sortedValues.length === 0) return null;
		return classify(sortedValues, { method: cartography.method, n: cartography.n });
	});
	const colors = $derived(breaks ? paletteColors(cartography.palette, cartography.n) : []);

	const defaultTitle = $derived(
		dataset && geo
			? `${dataset.name} — ${selection.scale === 'pc4' ? 'PC4' : 'Gemeente'} — ${yearLabel}`
			: ''
	);
	const effectiveTitle = $derived(title.trim() || defaultTitle);

	function onDownload() {
		const svgEl = mapWrap?.querySelector('svg');
		if (!svgEl) return;
		const safe = effectiveTitle.replace(/[^\w-]+/g, '-').replace(/^-+|-+$/g, '') || 'map';
		downloadSvg(svgEl, `${safe}.svg`);
	}

	function onPrint() {
		window.print();
	}
</script>

<div class="page">
	<header class="toolbar">
		<a class="back" href="/">← Back to map</a>
		<input class="title-input" type="text" placeholder={defaultTitle} bind:value={title} />
		<div class="grow"></div>
		<button type="button" onclick={onDownload}>Download SVG</button>
		<button type="button" onclick={onPrint}>Print / PDF</button>
	</header>

	<article class="sheet">
		<h1 class="title">{effectiveTitle}</h1>
		<div class="map" bind:this={mapWrap}>
			{#if geo && breaks}
				<PrintMap
					topojsonUrl="/data/{geo.topojson}"
					valueByArea={queryResult.data}
					{breaks}
					{colors}
					idProp={geo.idProp}
				/>
			{:else if !manifest || queryResult.loading}
				<p class="hint">Loading…</p>
			{:else}
				<p class="hint">No data.</p>
			{/if}
		</div>
		{#if breaks}
			<div class="legend-wrap">
				<Legend {breaks} {colors} />
			</div>
		{/if}
		<footer class="footnote">
			<span>Source: CBS microdata. Cells &lt; 10 suppressed for privacy.</span>
			<span>Projection: EPSG:28992 (RD New).</span>
		</footer>
	</article>
</div>

<style>
	.page {
		min-height: 100vh;
		background: #f5f5f5;
		padding: var(--spacing-4);
		display: flex;
		flex-direction: column;
		gap: var(--spacing-4);
		align-items: center;
	}
	.toolbar {
		width: 100%;
		max-width: 800px;
		display: flex;
		gap: var(--spacing-2);
		align-items: center;
	}
	.back {
		color: var(--color-muted);
		text-decoration: none;
		font-size: var(--text-sm);
	}
	.back:hover {
		color: var(--color-text);
	}
	.title-input {
		flex: 1;
		max-width: 360px;
		padding: 4px 8px;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		font-size: var(--text-sm);
	}
	.grow {
		flex: 1;
	}
	button {
		padding: 4px 10px;
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border: none;
		border-radius: var(--radius);
		font-size: var(--text-sm);
		cursor: pointer;
	}
	.sheet {
		width: 100%;
		max-width: 800px;
		background: #fff;
		padding: var(--spacing-4);
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		display: flex;
		flex-direction: column;
		gap: var(--spacing-3);
	}
	.title {
		font-size: 18px;
		font-weight: 600;
		margin: 0;
		color: var(--color-text);
	}
	.map {
		width: 100%;
	}
	.legend-wrap {
		display: flex;
		justify-content: flex-start;
	}
	.footnote {
		display: flex;
		justify-content: space-between;
		gap: var(--spacing-3);
		font-size: var(--text-xs);
		color: var(--color-hint);
		border-top: 1px solid var(--color-line);
		padding-top: var(--spacing-2);
	}
	.hint {
		color: var(--color-hint);
		font-size: var(--text-sm);
		text-align: center;
		padding: var(--spacing-4);
	}

	@media print {
		.page {
			background: #fff;
			padding: 0;
		}
		.toolbar {
			display: none;
		}
		.sheet {
			border: none;
			max-width: none;
			padding: 12mm;
		}
	}
</style>
