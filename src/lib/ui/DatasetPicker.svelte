<script>
	import { selection } from '$lib/state/selection.svelte.js';
	import Field from './Field.svelte';

	let { manifest } = $props();

	const options = $derived(
		Object.entries(manifest?.datasets ?? {}).map(([id, ds]) => ({
			id,
			label: ds.name ?? id
		}))
	);
</script>

<Field label="Dataset">
	<select bind:value={selection.dataset}>
		{#each options as o (o.id)}
			<option value={o.id}>{o.label}</option>
		{/each}
	</select>
</Field>
