<script>
	let { title, open = true, children } = $props();
</script>

<details class="panel" {open}>
	<summary>{title}</summary>
	<div class="body">
		{@render children?.()}
	</div>
</details>

<style>
	.panel {
		background: var(--color-bg-panel);
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		font-size: var(--text-sm);
	}
	summary {
		padding: var(--spacing-2) var(--spacing-3);
		font-weight: 600;
		color: var(--color-text);
		cursor: pointer;
		user-select: none;
		list-style: none;
		display: flex;
		align-items: center;
		gap: var(--spacing-2);
	}
	summary::-webkit-details-marker {
		display: none;
	}
	summary::before {
		content: '▸';
		font-size: var(--text-xs);
		color: var(--color-hint);
		transition: transform 0.12s;
	}
	.panel[open] summary::before {
		transform: rotate(90deg);
	}
	.body {
		padding: var(--spacing-2) var(--spacing-3) var(--spacing-3);
		border-top: 1px solid var(--color-line);
	}
</style>
