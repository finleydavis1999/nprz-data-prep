<script>
	import { Plot, RectY, AxisX, AxisY } from 'svelteplot';

	let { values = [], breaks = null, colors = [], nBins = 30, height = 130 } = $props();

	// For each bin, fill = color of the class its midpoint falls into.
	function classIndex(v) {
		if (!breaks) return 0;
		// breaks is length n+1; classes are [breaks[i], breaks[i+1])
		for (let i = 1; i < breaks.length - 1; i++) {
			if (v < breaks[i]) return i - 1;
		}
		return colors.length - 1;
	}

	const bins = $derived.by(() => {
		if (!values.length || !breaks) return [];
		const min = breaks[0];
		const max = breaks[breaks.length - 1];
		if (!Number.isFinite(min) || !Number.isFinite(max) || max <= min) return [];
		const step = (max - min) / nBins;
		const counts = new Array(nBins).fill(0);
		for (const v of values) {
			if (!Number.isFinite(v)) continue;
			let i = Math.floor((v - min) / step);
			if (i < 0) i = 0;
			if (i >= nBins) i = nBins - 1;
			counts[i]++;
		}
		return counts.map((count, i) => {
			const x0 = min + i * step;
			const x1 = x0 + step;
			const mid = x0 + step / 2;
			return { x0, x1, count, color: colors[classIndex(mid)] ?? '#999' };
		});
	});
</script>

<div class="hist">
	{#if bins.length > 0}
		<Plot {height} marginTop={6} marginBottom={22} marginLeft={36} marginRight={6}>
			<RectY data={bins} x1="x0" x2="x1" y="count" fill="color" />
			<AxisX tickCount={3} />
			<AxisY tickCount={3} />
		</Plot>
	{:else}
		<div class="empty">No data</div>
	{/if}
</div>

<style>
	.hist {
		font-size: var(--text-xs);
		color: var(--color-muted);
	}
	.empty {
		color: var(--color-hint);
		padding: var(--spacing-3) 0;
		text-align: center;
	}
</style>
