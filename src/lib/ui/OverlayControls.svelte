<script>
	import { overlay } from '$lib/state/overlay.svelte.js';
	import Field from './Field.svelte';

	const scales = [
		{ id: 'gem', label: 'Gemeente boundaries' },
		{ id: 'pc4', label: 'PC4 boundaries' }
	];

	let enabled = $state(overlay.scale !== null);
	let pickedScale = $state(overlay.scale ?? 'gem');

	$effect(() => {
		overlay.scale = enabled ? pickedScale : null;
	});
</script>

<div class="stack">
	<label class="check">
		<input type="checkbox" bind:checked={enabled} />
		<span>Show boundary overlay</span>
	</label>

	{#if enabled}
		<Field label="Scale">
			<select bind:value={pickedScale}>
				{#each scales as s (s.id)}
					<option value={s.id}>{s.label}</option>
				{/each}
			</select>
		</Field>
		<Field label="Color">
			<input type="color" bind:value={overlay.color} />
		</Field>
		<Field label="Width" value="{overlay.width.toFixed(1)}px">
			<input type="range" min="0.2" max="4" step="0.1" bind:value={overlay.width} />
		</Field>
		<Field label="Opacity" value={overlay.opacity.toFixed(2)}>
			<input type="range" min="0" max="1" step="0.05" bind:value={overlay.opacity} />
		</Field>
	{/if}
</div>

<style>
	.stack {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-2);
		font-size: var(--text-sm);
	}
	.check {
		display: flex;
		align-items: center;
		gap: var(--spacing-2);
		color: var(--color-text);
	}
	.check input {
		width: auto;
	}
</style>
