<script>
	import Field from './Field.svelte';
	import { selection } from '$lib/state/selection.svelte.js';
	import { layers } from '$lib/state/layers.svelte.js';
	import { slugify } from '$lib/data/layer-calc.js';

	let { manifest } = $props();

	let calcName = $state('');
	let calcExpr = $state('');
	let calcError = $state(/** @type {string | null} */ (null));
	let expandedId = $state(/** @type {string | null} */ (null));
	let exprEditor = $state(/** @type {HTMLDivElement | null} */ (null));
	let dragSlug = $state(/** @type {string | null} */ (null));

	const sameScale = $derived(layers.items.filter((l) => l.scale === selection.scale));
	const slugKind = $derived(new Map(sameScale.map((l) => [l.slug, l.kind])));

	function onSaveCalc(e) {
		e.preventDefault();
		calcError = null;
		const slug = slugify(calcName);
		if (!slug) {
			calcError = 'Name required';
			return;
		}
		if (layers.slugTaken(slug)) {
			calcError = 'Name already in use';
			return;
		}
		try {
			layers.saveCalc(calcName, calcExpr);
			calcName = '';
			calcExpr = '';
			// eslint-disable-next-line svelte/no-dom-manipulating -- editor is a controlled contenteditable, not part of Svelte's tree
			if (exprEditor) exprEditor.replaceChildren();
		} catch (err) {
			calcError = /** @type {Error} */ (err)?.message ?? String(err);
		}
	}

	function fieldLabel(fieldId) {
		return manifest?.datasets?.[selection.dataset]?.fields?.[fieldId]?.label ?? fieldId;
	}

	function valueLabel(layer, fieldId, valueId) {
		const ds = manifest?.datasets?.[layer.dataset];
		const values = ds?.fields?.[fieldId]?.values;
		return values?.find((v) => v.id === valueId)?.label ?? String(valueId);
	}

	function datasetLabel(layer) {
		return manifest?.datasets?.[layer.dataset]?.name ?? layer.dataset;
	}

	function toggleExpanded(id) {
		expandedId = expandedId === id ? null : id;
	}

	function setActive(id) {
		layers.setActive(layers.activeId === id ? null : id);
	}

	// The expression editor is a contenteditable div so saved-layer slugs can
	// render as inline chip elements (atomic, contenteditable=false). Other
	// content (operators, numbers, parens, whitespace) is plain text. The
	// canonical string for math.js is built by serialising children — slugs
	// from data-slug, everything else from textContent.
	function serializeEditor() {
		if (!exprEditor) return '';
		const parts = [];
		for (const node of exprEditor.childNodes) {
			if (node.nodeType === Node.ELEMENT_NODE) {
				const slug = /** @type {HTMLElement} */ (node).dataset?.slug;
				if (slug) {
					parts.push(` ${slug} `);
					continue;
				}
				// Unexpected element (e.g. <br> from Enter): use its text.
				parts.push(/** @type {HTMLElement} */ (node).textContent ?? '');
			} else {
				parts.push(node.textContent ?? '');
			}
		}
		return parts.join('').replace(/\s+/g, ' ').trim();
	}

	function syncFromDom() {
		calcExpr = serializeEditor();
		// Browsers leave stray <br>s and empty text nodes after delete-all;
		// normalise so the placeholder pseudo-element renders cleanly.
		if (calcExpr === '' && exprEditor) {
			const onlyArtifacts = [...exprEditor.childNodes].every(
				(n) =>
					(n.nodeType === Node.TEXT_NODE && !n.textContent?.trim()) ||
					(n.nodeType === Node.ELEMENT_NODE && /** @type {HTMLElement} */ (n).tagName === 'BR')
			);
			// eslint-disable-next-line svelte/no-dom-manipulating -- controlled contenteditable
			if (onlyArtifacts && exprEditor.childNodes.length > 0) exprEditor.replaceChildren();
		}
	}

	function makeChipNode(slug) {
		const span = document.createElement('span');
		const kind = slugKind.get(slug);
		span.className = kind === 'calc' ? 'in-chip in-chip-calc' : 'in-chip';
		span.contentEditable = 'false';
		span.dataset.slug = slug;
		span.textContent = slug;
		return span;
	}

	// Insert a chip span at the current selection inside the editor (or at the
	// end if focus is elsewhere). Wraps with single spaces so the surrounding
	// text doesn't fuse with the chip.
	function insertChipAtCaret(slug) {
		if (!exprEditor) return;
		exprEditor.focus();
		const sel = window.getSelection();
		let range = sel && sel.rangeCount > 0 ? sel.getRangeAt(0) : null;
		if (!range || !exprEditor.contains(range.commonAncestorContainer)) {
			range = document.createRange();
			range.selectNodeContents(exprEditor);
			range.collapse(false);
		}
		range.deleteContents();
		const lead = document.createTextNode(' ');
		const chip = makeChipNode(slug);
		const tail = document.createTextNode(' ');
		range.insertNode(tail);
		range.insertNode(chip);
		range.insertNode(lead);
		// Move caret after the trailing space.
		const after = document.createRange();
		after.setStartAfter(tail);
		after.collapse(true);
		sel?.removeAllRanges();
		sel?.addRange(after);
		syncFromDom();
	}

	function onChipDragStart(e, slug) {
		dragSlug = slug;
		e.dataTransfer?.setData('text/plain', slug);
		if (e.dataTransfer) e.dataTransfer.effectAllowed = 'copy';
	}

	function onChipDragEnd() {
		dragSlug = null;
	}

	function onInputDragOver(e) {
		if (!dragSlug && !e.dataTransfer?.types.includes('text/plain')) return;
		e.preventDefault();
		if (e.dataTransfer) e.dataTransfer.dropEffect = 'copy';
	}

	function onInputDrop(e) {
		const slug = e.dataTransfer?.getData('text/plain') ?? dragSlug;
		if (!slug) return;
		e.preventDefault();
		// Move the caret to the drop point before inserting.
		const sel = window.getSelection();
		// @ts-ignore — both APIs are platform-specific.
		const pos = document.caretPositionFromPoint?.(e.clientX, e.clientY);
		// @ts-ignore
		const fallbackRange = document.caretRangeFromPoint?.(e.clientX, e.clientY);
		let range = null;
		if (pos) {
			range = document.createRange();
			range.setStart(pos.offsetNode, pos.offset);
			range.collapse(true);
		} else if (fallbackRange) {
			range = fallbackRange;
		}
		if (range && exprEditor?.contains(range.startContainer)) {
			sel?.removeAllRanges();
			sel?.addRange(range);
		}
		insertChipAtCaret(slug);
		dragSlug = null;
	}

	// Strip rich content from paste — keep just the text.
	function onEditorPaste(e) {
		e.preventDefault();
		const text = e.clipboardData?.getData('text/plain') ?? '';
		if (!text) return;
		document.execCommand('insertText', false, text);
	}
</script>

<div class="stack">
	{#if layers.items.length === 0}
		<p class="hint">No saved layers yet — save one from the Data panel above.</p>
	{:else}
		<ul class="layers">
			<li class="layer live" class:active={layers.activeId === null}>
				<button
					type="button"
					class="radio"
					aria-pressed={layers.activeId === null}
					onclick={() => layers.setActive(null)}
					title="Show live selection"
				>
					{layers.activeId === null ? '●' : '○'}
				</button>
				<span class="kind" title="Live selection">·</span>
				<span class="name muted">live selection</span>
			</li>
			{#each layers.items as layer (layer.id)}
				{@const isOff = layer.scale !== selection.scale}
				{@const isActive = layers.activeId === layer.id}
				<li class="layer" class:active={isActive} class:off={isOff}>
					<button
						type="button"
						class="radio"
						aria-pressed={isActive}
						disabled={isOff}
						onclick={() => setActive(layer.id)}
						title={isOff ? `Different scale (${layer.scale})` : 'Set active'}
					>
						{isActive ? '●' : '○'}
					</button>
					<span class="kind" title={layer.kind}>{layer.kind === 'calc' ? 'ƒ' : '◆'}</span>
					<button
						type="button"
						class="name-btn"
						onclick={() => toggleExpanded(layer.id)}
						title="Show parameters"
					>
						<span class="name">{layer.name}</span>
						{#if layer.slug !== layer.name}
							<span class="slug">({layer.slug})</span>
						{/if}
					</button>
					{#if layers.loading.has(layer.id)}
						<span class="meta">…</span>
					{:else if layers.errors.get(layer.id)}
						<span class="meta err" title={layers.errors.get(layer.id)}>!</span>
					{:else if layers.results.get(layer.id)}
						<span class="meta">{layers.results.get(layer.id).size}</span>
					{/if}
					<button
						type="button"
						class="del"
						onclick={() => layers.remove(layer.id)}
						title="Delete layer"
					>
						×
					</button>
					{#if expandedId === layer.id}
						<div class="details">
							{#if layer.kind === 'filter'}
								<div class="line">
									<span class="k">Dataset</span><span>{datasetLabel(layer)}</span>
								</div>
								<div class="line"><span class="k">Scale</span><span>{layer.scale}</span></div>
								<div class="line"><span class="k">Year</span><span>{layer.year}</span></div>
								{#if layer.filters && Object.keys(layer.filters).length > 0}
									{#each Object.entries(layer.filters) as [fieldId, vals] (fieldId)}
										{#if vals && vals.length}
											<div class="line">
												<span class="k">{fieldLabel(fieldId)}</span>
												<span class="chips">
													{#each vals as v (v)}
														<span class="chip">{valueLabel(layer, fieldId, v)}</span>
													{/each}
												</span>
											</div>
										{/if}
									{/each}
								{:else}
									<div class="line">
										<span class="k">Filters</span><span class="muted">none</span>
									</div>
								{/if}
							{:else}
								<div class="line"><span class="k">Scale</span><span>{layer.scale}</span></div>
								<div class="line">
									<span class="k">Expression</span><code>{layer.expression}</code>
								</div>
							{/if}
						</div>
					{/if}
				</li>
			{/each}
		</ul>
	{/if}

	<form class="calc" onsubmit={onSaveCalc}>
		<div class="calc-head">Add calculation</div>
		<Field label="Name">
			<input type="text" placeholder="e.g. youthShare" bind:value={calcName} autocomplete="off" />
		</Field>
		<Field label="Expression">
			<div
				class="expr-editor"
				class:empty={!calcExpr}
				role="textbox"
				tabindex="0"
				aria-label="Expression"
				data-placeholder="click a layer below, then type operators"
				contenteditable="true"
				bind:this={exprEditor}
				oninput={syncFromDom}
				ondragover={onInputDragOver}
				ondrop={onInputDrop}
				onpaste={onEditorPaste}
				spellcheck="false"
			></div>
		</Field>
		{#if sameScale.length > 0}
			<div class="palette" aria-label="Available layers — click or drag to insert">
				{#each sameScale as l (l.id)}
					<button
						type="button"
						class="layer-chip"
						class:calc={l.kind === 'calc'}
						draggable="true"
						ondragstart={(e) => onChipDragStart(e, l.slug)}
						ondragend={onChipDragEnd}
						onclick={() => insertChipAtCaret(l.slug)}
						title="Click to insert · drag into expression"
					>
						<span class="chip-kind">{l.kind === 'calc' ? 'ƒ' : '◆'}</span>
						<span class="chip-slug">{l.slug}</span>
					</button>
				{/each}
			</div>
		{:else}
			<p class="hint">Save at least one layer from the Data panel first.</p>
		{/if}
		{#if calcError}
			<p class="err-msg">{calcError}</p>
		{/if}
		<button type="submit" class="primary" disabled={!calcName || !calcExpr}>Add layer</button>
	</form>
</div>

<style>
	.stack {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-3);
	}
	.primary {
		align-self: flex-end;
		padding: 2px var(--spacing-2);
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border: none;
		border-radius: var(--radius);
		font-size: var(--text-sm);
		cursor: pointer;
	}
	.primary:disabled {
		background: var(--color-line);
		cursor: default;
	}
	.layers {
		list-style: none;
		margin: 0;
		padding: 0;
		display: flex;
		flex-direction: column;
		gap: var(--spacing-1);
	}
	.layer {
		display: grid;
		grid-template-columns: auto auto 1fr auto auto;
		align-items: center;
		gap: var(--spacing-1);
		font-size: var(--text-sm);
		padding: 2px var(--spacing-1);
		border-radius: var(--radius);
	}
	.layer.active {
		background: rgba(31, 35, 40, 0.06);
	}
	.layer.off {
		opacity: 0.5;
	}
	.layer.live {
		font-style: italic;
	}
	.radio {
		background: transparent;
		border: none;
		cursor: pointer;
		font-size: var(--text-sm);
		color: var(--color-muted);
		padding: 0 2px;
	}
	.radio:disabled {
		cursor: default;
	}
	.kind {
		color: var(--color-hint);
		font-size: var(--text-xs);
		width: 1em;
		text-align: center;
	}
	.name-btn {
		background: transparent;
		border: none;
		cursor: pointer;
		text-align: left;
		padding: 0;
		font: inherit;
		color: var(--color-text);
		display: flex;
		gap: 4px;
		align-items: baseline;
		min-width: 0;
	}
	.name {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
	}
	.slug {
		color: var(--color-hint);
		font-size: var(--text-xs);
	}
	.meta {
		color: var(--color-hint);
		font-size: var(--text-xs);
		font-variant-numeric: tabular-nums;
	}
	.meta.err {
		color: #cf222e;
	}
	.del {
		background: transparent;
		border: none;
		color: var(--color-hint);
		cursor: pointer;
		font-size: var(--text-sm);
		padding: 0 2px;
	}
	.del:hover {
		color: var(--color-text);
	}
	.details {
		grid-column: 1 / -1;
		margin-top: var(--spacing-1);
		padding: var(--spacing-1) var(--spacing-2);
		background: rgba(0, 0, 0, 0.03);
		border-radius: var(--radius);
		display: flex;
		flex-direction: column;
		gap: 2px;
		font-size: var(--text-xs);
	}
	.line {
		display: grid;
		grid-template-columns: 70px 1fr;
		gap: var(--spacing-2);
	}
	.k {
		color: var(--color-muted);
	}
	.chips {
		display: flex;
		flex-wrap: wrap;
		gap: 2px;
	}
	.chip {
		padding: 0 var(--spacing-1);
		border: 1px solid var(--color-line);
		border-radius: var(--radius-pill);
		background: #fff;
		color: var(--color-muted);
	}
	.calc {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-1);
		padding-top: var(--spacing-2);
		border-top: 1px solid var(--color-line);
	}
	.calc-head {
		font-weight: 600;
		color: var(--color-text);
		font-size: var(--text-sm);
	}
	.palette {
		display: flex;
		flex-wrap: wrap;
		gap: var(--spacing-1);
		padding-left: calc(var(--label-col) + var(--spacing-2));
	}
	.layer-chip {
		display: inline-flex;
		align-items: center;
		gap: 3px;
		padding: 1px var(--spacing-2);
		border: 1px solid var(--color-accent);
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border-radius: var(--radius-pill);
		font-size: var(--text-xs);
		font-family: ui-monospace, monospace;
		cursor: grab;
	}
	.layer-chip:hover {
		filter: brightness(1.4);
	}
	.layer-chip:active {
		cursor: grabbing;
	}
	.layer-chip.calc {
		background: #fff;
		color: var(--color-accent);
	}
	.chip-kind {
		font-size: 9px;
		opacity: 0.8;
	}
	.expr-editor {
		width: 100%;
		min-height: calc(var(--text-sm) + 8px);
		padding: 2px 6px;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
		background: #fff;
		font-family: ui-monospace, monospace;
		font-size: var(--text-sm);
		line-height: 1.6;
		outline: none;
		white-space: pre-wrap;
		word-break: break-word;
		cursor: text;
	}
	.expr-editor:focus-within {
		border-color: var(--color-accent);
	}
	.expr-editor.empty::before {
		content: attr(data-placeholder);
		color: var(--color-hint);
		pointer-events: none;
	}
	.expr-editor :global(.in-chip) {
		display: inline-block;
		padding: 0 var(--spacing-2);
		margin: 0 1px;
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border: 1px solid var(--color-accent);
		border-radius: var(--radius-pill);
		font-size: var(--text-xs);
		line-height: 1.4;
		vertical-align: baseline;
		user-select: none;
	}
	.expr-editor :global(.in-chip-calc) {
		background: #fff;
		color: var(--color-accent);
	}
	.err-msg {
		font-size: var(--text-xs);
		color: #cf222e;
		margin: 0;
	}
	.hint {
		font-size: var(--text-xs);
		color: var(--color-hint);
		margin: 0;
	}
	.muted {
		color: var(--color-muted);
	}
	code {
		font-family: ui-monospace, monospace;
		font-size: var(--text-xs);
		color: var(--color-text);
	}
</style>
