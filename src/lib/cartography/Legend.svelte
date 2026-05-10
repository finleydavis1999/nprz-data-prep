<script>
	let { breaks, colors, format = (n) => Math.round(n).toLocaleString(), title = '' } = $props();
</script>

<div class="legend">
	{#if title}<div class="title">{title}</div>{/if}
	{#each colors as color, i (i)}
		<div class="row">
			<span class="swatch" style="background: {color}"></span>
			<span class="label">{format(breaks[i])} – {format(breaks[i + 1])}</span>
		</div>
	{/each}
</div>

<style>
	.legend {
		font-size: var(--text-xs);
		color: var(--color-text);
	}
	.title {
		font-weight: 600;
		margin-bottom: var(--spacing-1);
	}
	.row {
		display: flex;
		align-items: center;
		gap: var(--spacing-2);
		line-height: 1.5;
	}
	.swatch {
		width: 18px;
		height: 12px;
		display: inline-block;
		border: 1px solid var(--color-line);
		flex-shrink: 0;
	}
	.label {
		font-variant-numeric: tabular-nums;
		color: var(--color-muted);
	}
</style>
