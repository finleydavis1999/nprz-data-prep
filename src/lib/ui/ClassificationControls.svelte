<script>
	import { cartography } from '$lib/state/cartography.svelte.js';
	import { paletteNames } from '$lib/cartography/palettes.js';
	import Field from './Field.svelte';

	const methods = [
		{ id: 'jenks', label: 'Jenks (natural breaks)' },
		{ id: 'quantile', label: 'Quantile' },
		{ id: 'equal', label: 'Equal interval' }
	];
	const palettes = paletteNames('sequential');
</script>

<div class="stack">
	<Field label="Method">
		<select bind:value={cartography.method}>
			{#each methods as m (m.id)}
				<option value={m.id}>{m.label}</option>
			{/each}
		</select>
	</Field>

	<Field label="Classes">
		<input type="number" min="3" max="9" bind:value={cartography.n} />
	</Field>

	<Field label="Palette">
		<select bind:value={cartography.palette}>
			{#each palettes as name (name)}
				<option value={name}>{name}</option>
			{/each}
		</select>
	</Field>

	<Field label="Fill opacity" value={cartography.fillOpacity.toFixed(2)}>
		<input type="range" min="0" max="1" step="0.05" bind:value={cartography.fillOpacity} />
	</Field>

	<Field label="Line width" value="{cartography.lineWidth.toFixed(1)}px">
		<input type="range" min="0" max="2" step="0.1" bind:value={cartography.lineWidth} />
	</Field>

	<Field label="Line color">
		<input type="color" bind:value={cartography.lineColor} />
	</Field>
</div>

<style>
	.stack {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-2);
	}
</style>
