<script>
	// A draggable floating panel. Position is owned by the parent so it can be
	// persisted to localStorage. Header drag moves the dock; close button calls
	// onClose. Clamps to the viewport so a dock can't be dragged out of reach.
	let {
		title,
		open = false,
		x = 360,
		y = 24,
		width = 320,
		onClose,
		onMove,
		children
	} = $props();

	let dragging = $state(false);
	let dragOffsetX = 0;
	let dragOffsetY = 0;

	function clamp(nx, ny) {
		if (typeof window === 'undefined') return { x: nx, y: ny };
		const maxX = Math.max(0, window.innerWidth - 80);
		const maxY = Math.max(0, window.innerHeight - 40);
		return {
			x: Math.min(Math.max(0, nx), maxX),
			y: Math.min(Math.max(0, ny), maxY)
		};
	}

	function onHeaderPointerDown(e) {
		// Don't start a drag from inside the close button.
		if (e.target instanceof Element && e.target.closest('.dock-close')) return;
		dragging = true;
		dragOffsetX = e.clientX - x;
		dragOffsetY = e.clientY - y;
		e.currentTarget.setPointerCapture?.(e.pointerId);
	}

	function onHeaderPointerMove(e) {
		if (!dragging) return;
		const next = clamp(e.clientX - dragOffsetX, e.clientY - dragOffsetY);
		onMove?.(next);
	}

	function onHeaderPointerUp(e) {
		if (!dragging) return;
		dragging = false;
		e.currentTarget.releasePointerCapture?.(e.pointerId);
	}
</script>

{#if open}
	<section class="dock" style="left: {x}px; top: {y}px; width: {width}px;" aria-label={title}>
		<div
			class="dock-head"
			class:dragging
			role="presentation"
			onpointerdown={onHeaderPointerDown}
			onpointermove={onHeaderPointerMove}
			onpointerup={onHeaderPointerUp}
			onpointercancel={onHeaderPointerUp}
		>
			<span class="dock-title">{title}</span>
			<button type="button" class="dock-close" onclick={onClose} title="Close">×</button>
		</div>
		<div class="dock-body">
			{@render children?.()}
		</div>
	</section>
{/if}

<style>
	.dock {
		position: fixed;
		z-index: 5;
		max-height: calc(100vh - var(--spacing-4) * 2);
		display: flex;
		flex-direction: column;
		background: var(--color-bg-panel);
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		box-shadow: 0 8px 24px rgba(0, 0, 0, 0.12);
		overflow: hidden;
	}
	.dock-head {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: var(--spacing-2);
		padding: var(--spacing-2) var(--spacing-3);
		background: var(--color-bg-panel);
		border-bottom: 1px solid var(--color-line);
		cursor: grab;
		user-select: none;
	}
	.dock-head.dragging {
		cursor: grabbing;
	}
	.dock-title {
		font-weight: 600;
		font-size: var(--text-sm);
		color: var(--color-text);
	}
	.dock-close {
		background: transparent;
		border: none;
		color: var(--color-hint);
		cursor: pointer;
		font-size: var(--text-base);
		line-height: 1;
		padding: 0 var(--spacing-1);
	}
	.dock-close:hover {
		color: var(--color-text);
	}
	.dock-body {
		flex: 1 1 auto;
		min-height: 0;
		overflow-y: auto;
		padding: var(--spacing-2) var(--spacing-3) var(--spacing-3);
	}
</style>
