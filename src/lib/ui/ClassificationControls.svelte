<script>
	import { cartography } from '$lib/state/cartography.svelte.js';
	import { paletteNames } from '$lib/cartography/palettes.js';
	import Field from './Field.svelte';

	// `target` defaults to the node-cartography singleton; pass a flow-cartography
	// state to drive the flow layer instead. Width/curvature sliders are shown
	// only when those keys exist on the target (flow side); fill-opacity/line
	// controls only when those keys exist (node side).
	let { target = cartography } = $props();

	const methods = [
		{ id: 'jenks', label: 'Jenks (natural breaks)' },
		{ id: 'quantile', label: 'Quantile' },
		{ id: 'equal', label: 'Equal interval' }
	];
	const palettes = paletteNames('sequential');

	const hasFill = $derived('fillOpacity' in target);
	const hasLine = $derived('lineColor' in target);
	const hasWidth = $derived('widthMin' in target && 'widthMax' in target);
	const hasOpacity = $derived('opacity' in target);
	const hasCurvature = $derived('curvature' in target);
</script>

<div class="stack">
	<Field label="Method">
		<select bind:value={target.method}>
			{#each methods as m (m.id)}
				<option value={m.id}>{m.label}</option>
			{/each}
		</select>
	</Field>

	<Field label="Classes">
		<input type="number" min="3" max="9" bind:value={target.n} />
	</Field>

	<Field label="Palette">
		<select bind:value={target.palette}>
			{#each palettes as name (name)}
				<option value={name}>{name}</option>
			{/each}
		</select>
	</Field>

	{#if hasFill}
		<Field label="Fill opacity" value={target.fillOpacity.toFixed(2)}>
			<input type="range" min="0" max="1" step="0.05" bind:value={target.fillOpacity} />
		</Field>
	{/if}

	{#if hasLine}
		<Field label="Line width" value="{target.lineWidth.toFixed(1)}px">
			<input type="range" min="0" max="2" step="0.1" bind:value={target.lineWidth} />
		</Field>

		<Field label="Line color">
			<input type="color" bind:value={target.lineColor} />
		</Field>
	{/if}

	{#if hasWidth}
		<Field label="Width min" value="{target.widthMin.toFixed(1)}px">
			<input type="range" min="0" max="6" step="0.1" bind:value={target.widthMin} />
		</Field>

		<Field label="Width max" value="{target.widthMax.toFixed(1)}px">
			<input type="range" min="1" max="20" step="0.5" bind:value={target.widthMax} />
		</Field>
	{/if}

	{#if hasOpacity}
		<Field label="Opacity" value={target.opacity.toFixed(2)}>
			<input type="range" min="0" max="1" step="0.05" bind:value={target.opacity} />
		</Field>
	{/if}

	{#if hasCurvature}
		<Field label="Curvature" value={target.curvature.toFixed(2)}>
			<input type="range" min="0" max="0.6" step="0.02" bind:value={target.curvature} />
		</Field>
	{/if}
</div>

<style>
	.stack {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-2);
	}
</style>
