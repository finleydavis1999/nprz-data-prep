<script>
	import { selection } from '$lib/state/selection.svelte.js';
	import Field from './Field.svelte';

	let { manifest } = $props();

	const years = $derived(
		manifest?.datasets?.[selection.dataset]?.fields?.year?.values ?? []
	);
</script>

<Field label="Year">
	<select bind:value={selection.year}>
		{#each years as y (y.id)}
			<option value={y.id}>{y.label}</option>
		{/each}
	</select>
</Field>
