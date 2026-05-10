<script>
	import Field from './Field.svelte';
	import { studyArea } from '$lib/state/study-area.svelte.js';
	import { selection } from '$lib/state/selection.svelte.js';

	let { lassoActive = $bindable(false) } = $props();

	let pasteOpen = $state(false);
	let pasteText = $state('');
	let copyMsg = $state('');
	let saveName = $state('');
	let warning = $state('');

	const savedNames = $derived(Object.keys(studyArea.saved).sort());
	const count = $derived(studyArea.ids.size);

	function toggleLasso() {
		lassoActive = !lassoActive;
	}

	function clearAll() {
		studyArea.clear();
		warning = '';
	}

	async function copyIds() {
		const text = [...studyArea.ids].join('\n');
		try {
			await navigator.clipboard.writeText(text);
			copyMsg = `Copied ${studyArea.ids.size}`;
		} catch {
			copyMsg = 'Copy failed';
		}
		setTimeout(() => (copyMsg = ''), 1500);
	}

	function applyPaste() {
		const ids = pasteText
			.split(/\s+/)
			.map((s) => s.trim())
			.filter(Boolean);
		studyArea.replace(ids);
		pasteText = '';
		pasteOpen = false;
		warning = '';
	}

	function save() {
		const name = saveName.trim();
		if (!name) return;
		if (studyArea.saveAs(name)) {
			saveName = '';
		}
	}

	function loadSet(name) {
		const result = studyArea.load(name);
		if (result.ok) {
			warning = '';
		} else if (result.reason === 'scale-mismatch') {
			warning = `"${name}" is for ${result.expected.toUpperCase()} — switch scale first`;
		}
	}

	function deleteSet(name) {
		if (confirm(`Delete saved set "${name}"?`)) studyArea.delete(name);
	}
</script>

<div class="stack">
	<Field label="Lasso">
		<button type="button" class="toggle" class:on={lassoActive} onclick={toggleLasso}>
			{lassoActive ? 'Drawing… (click to stop)' : 'Draw'}
		</button>
	</Field>
	<div class="hint">shift = add · alt = subtract · drag = replace</div>

	<div class="row">
		<span class="count">{count} {count === 1 ? 'area' : 'areas'} selected</span>
		<span class="scale">
			{#if studyArea.scale}{studyArea.scale.toUpperCase()}{/if}
		</span>
	</div>

	<div class="actions">
		<button type="button" onclick={clearAll} disabled={count === 0}>Clear</button>
		<button type="button" onclick={copyIds} disabled={count === 0}>Copy</button>
		<button type="button" onclick={() => (pasteOpen = !pasteOpen)}>
			{pasteOpen ? 'Cancel' : 'Paste'}
		</button>
		{#if copyMsg}<span class="msg">{copyMsg}</span>{/if}
	</div>

	{#if pasteOpen}
		<textarea
			class="paste"
			rows="4"
			placeholder="One area_code per line"
			bind:value={pasteText}
		></textarea>
		<div class="actions">
			<button type="button" onclick={applyPaste} disabled={!pasteText.trim()}>Apply</button>
		</div>
	{/if}

	{#if warning}
		<div class="warn">{warning}</div>
	{/if}

	<div class="divider"></div>

	<Field label="Save as">
		<input
			type="text"
			placeholder="name"
			bind:value={saveName}
			onkeydown={(e) => e.key === 'Enter' && save()}
		/>
	</Field>
	<div class="actions">
		<button type="button" onclick={save} disabled={!saveName.trim() || count === 0}>Save</button>
	</div>

	{#if savedNames.length > 0}
		<ul class="saved">
			{#each savedNames as name (name)}
				{@const entry = studyArea.saved[name]}
				<li>
					<button
						type="button"
						class="load"
						onclick={() => loadSet(name)}
						title="Load {name}"
					>
						<span class="name">{name}</span>
						<span class="meta">{entry.ids.length} · {entry.scale.toUpperCase()}</span>
					</button>
					<button
						type="button"
						class="del"
						onclick={() => deleteSet(name)}
						title="Delete {name}"
						aria-label="Delete {name}">✕</button
					>
				</li>
			{/each}
		</ul>
	{:else}
		<div class="hint">No saved sets yet.</div>
	{/if}

	<div class="hint scope">
		Selections are scoped to the current scale ({selection.scale.toUpperCase()}).
	</div>
</div>

<style>
	.stack {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-2);
		font-size: var(--text-sm);
	}
	.toggle {
		width: 100%;
		padding: 4px 8px;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		background: white;
		color: var(--color-text);
		cursor: pointer;
		text-align: center;
	}
	.toggle.on {
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border-color: var(--color-accent);
	}
	.hint {
		font-size: var(--text-xs);
		color: var(--color-hint);
	}
	.row {
		display: flex;
		justify-content: space-between;
		align-items: baseline;
	}
	.count {
		color: var(--color-text);
		font-variant-numeric: tabular-nums;
	}
	.scale {
		font-size: var(--text-xs);
		color: var(--color-muted);
	}
	.actions {
		display: flex;
		gap: var(--spacing-2);
		align-items: center;
		flex-wrap: wrap;
	}
	.actions button {
		padding: 2px 8px;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		background: white;
		color: var(--color-text);
		cursor: pointer;
	}
	.actions button:disabled {
		color: var(--color-hint);
		cursor: not-allowed;
	}
	.msg {
		font-size: var(--text-xs);
		color: var(--color-muted);
	}
	.paste {
		width: 100%;
		padding: 6px;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		font-family: var(--font-sans);
		font-size: var(--text-sm);
		resize: vertical;
		box-sizing: border-box;
	}
	.warn {
		padding: var(--spacing-2);
		background: #fff5b1;
		border: 1px solid #d4a72c;
		border-radius: var(--radius);
		color: #6e4400;
		font-size: var(--text-xs);
	}
	.divider {
		height: 1px;
		background: var(--color-line);
	}
	.saved {
		list-style: none;
		margin: 0;
		padding: 0;
		display: flex;
		flex-direction: column;
		gap: 2px;
	}
	.saved li {
		display: flex;
		gap: var(--spacing-1);
		align-items: stretch;
	}
	.load {
		flex: 1;
		display: flex;
		justify-content: space-between;
		align-items: baseline;
		padding: 4px 6px;
		background: transparent;
		border: 1px solid transparent;
		border-radius: var(--radius);
		cursor: pointer;
		color: var(--color-text);
		text-align: left;
	}
	.load:hover {
		border-color: var(--color-line);
		background: rgba(0, 0, 0, 0.02);
	}
	.name {
		color: var(--color-text);
	}
	.meta {
		font-size: var(--text-xs);
		color: var(--color-muted);
		font-variant-numeric: tabular-nums;
	}
	.del {
		padding: 0 6px;
		background: transparent;
		border: none;
		color: var(--color-hint);
		cursor: pointer;
		font-size: var(--text-xs);
	}
	.del:hover {
		color: var(--color-text);
	}
	.scope {
		margin-top: var(--spacing-1);
	}
</style>
