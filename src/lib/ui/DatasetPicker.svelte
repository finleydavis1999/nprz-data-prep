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
	// clamp single-year selections to the new dataset's value list so the
	// query stays valid. Range-typed years are clamped by YearPicker's $effect.
	function onChange(e) {
		const id = e.currentTarget.value;
		state.dataset = id;
		state.filters = {};
		const yearField = manifest?.[section]?.[id]?.fields?.year;
		if (yearField?.type === 'single') {
			const years = yearField.values?.map((v) => v.id) ?? [];
			if (years.length && !years.includes(state.year)) {
				state.year = yearField.default ?? years[years.length - 1];
			}
		}
	}
</script>

<Field {label}>
	<select value={state.dataset} onchange={onChange}>
		{#each options as o (o.id)}
			<option value={o.id}>{o.label}</option>
		{/each}
	</select>
</Field>
