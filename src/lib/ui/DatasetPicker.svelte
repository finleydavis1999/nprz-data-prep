<script>
	import { selection } from '$lib/state/selection.svelte.js';
	import Field from './Field.svelte';

	let { manifest, state = selection, section = 'datasets', label = 'Dataset' } = $props();

	const options = $derived(
		Object.entries(manifest?.[section] ?? {}).map(([id, ds]) => ({
			id,
			label: ds.name ?? id
		}))
	);

	// Switching datasets: clear filters (fields differ between datasets) and
	// clamp the year to the new dataset's range so the choropleth query stays valid.
	function onChange(e) {
		const id = e.currentTarget.value;
		const ds = manifest?.datasets?.[id];
		const yearField = ds?.fields?.year;
		const years = yearField?.values?.map((v) => v.id) ?? [];
		selection.dataset = id;
		selection.filters = {};
		if (years.length && !years.includes(selection.year)) {
			selection.year = yearField.default ?? years[years.length - 1];
		}
	}
</script>

<Field {label}>
	<select bind:value={state.dataset}>
		{#each options as o (o.id)}
			<option value={o.id}>{o.label}</option>
		{/each}
	</select>
</Field>
