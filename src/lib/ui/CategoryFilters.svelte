<script>
	import { selection } from '$lib/state/selection.svelte.js';

	let { manifest } = $props();

	const fields = $derived.by(() => {
		const ds = manifest?.datasets?.[selection.dataset];
		if (!ds) return [];
		return Object.entries(ds.fields)
			.filter(([, f]) => f.type === 'multi')
			.map(([id, f]) => ({ id, label: f.label ?? id, values: f.values }));
	});

	const totalActive = $derived(
		Object.values(selection.filters).reduce((n, v) => n + (v?.length ?? 0), 0)
	);

	function toggle(fieldId, valueId) {
		const cur = selection.filters[fieldId] ?? [];
		const has = cur.includes(valueId);
		const next = has ? cur.filter((v) => v !== valueId) : [...cur, valueId];
		selection.filters = { ...selection.filters, [fieldId]: next };
	}

	function clearField(fieldId) {
		const next = { ...selection.filters };
		delete next[fieldId];
		selection.filters = next;
	}

	function clearAll() {
		selection.filters = {};
	}

	function isOn(fieldId, valueId) {
		return (selection.filters[fieldId] ?? []).includes(valueId);
	}

	function activeCount(fieldId) {
		return (selection.filters[fieldId] ?? []).length;
	}
</script>

<div class="filters">
	<div class="filters-head">
		<span class="filters-title">Filters</span>
		<button
			class="link"
			type="button"
			disabled={totalActive === 0}
			onclick={clearAll}
		>
			{totalActive > 0 ? `Reset (${totalActive})` : 'Reset'}
		</button>
	</div>

	{#each fields as f (f.id)}
		<div class="field">
			<div class="field-head">
				<span class="field-label">{f.label}</span>
				<button
					class="link xs"
					type="button"
					disabled={activeCount(f.id) === 0}
					onclick={() => clearField(f.id)}
					title="Clear filter"
				>
					{activeCount(f.id) > 0 ? `×${activeCount(f.id)}` : 'all'}
				</button>
			</div>
			<div class="chips">
				{#each f.values as v (v.id)}
					<button
						type="button"
						class="chip"
						class:on={isOn(f.id, v.id)}
						onclick={() => toggle(f.id, v.id)}
					>
						{v.label}
					</button>
				{/each}
			</div>
		</div>
	{/each}
	{#if fields.length === 0}
		<p class="hint">No filterable fields for this dataset.</p>
	{/if}
</div>

<style>
	.filters {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-2);
	}
	.filters-head {
		display: flex;
		align-items: center;
		justify-content: space-between;
	}
	.filters-title {
		font-size: var(--text-sm);
		font-weight: 600;
		color: var(--color-text);
	}
	.field {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-1);
	}
	.field-head {
		display: flex;
		align-items: center;
		justify-content: space-between;
		font-size: var(--text-sm);
	}
	.field-label {
		color: var(--color-muted);
	}
	.link {
		font-size: var(--text-sm);
		color: var(--color-muted);
		background: transparent;
		border: none;
		cursor: pointer;
		padding: 0 var(--spacing-1);
		font-family: inherit;
	}
	.link.xs {
		font-size: var(--text-xs);
		color: var(--color-hint);
	}
	.link:disabled {
		cursor: default;
		color: var(--color-line);
	}
	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: var(--spacing-1);
	}
	.chip {
		font-size: var(--text-xs);
		padding: 2px var(--spacing-2);
		border: 1px solid var(--color-line);
		background: #fff;
		color: var(--color-muted);
		border-radius: var(--radius-pill);
		cursor: pointer;
		font-family: inherit;
	}
	.chip.on {
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border-color: var(--color-accent);
	}
	.hint {
		color: var(--color-hint);
		font-size: var(--text-sm);
		margin: 0;
	}
</style>
