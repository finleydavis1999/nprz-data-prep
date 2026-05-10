// Calc-layer expression parsing + evaluation. Uses math.js for safe AST-based
// evaluation (no Function/eval). Symbols in the expression must resolve to
// saved-layer slugs.
import { parse } from 'mathjs';

// Sensible default display name for a snapshot of `selection`. The user can
// always edit before saving. Joins active filter value labels; falls back to
// dataset + year when no filters are set. Expects the manifest object so it
// can resolve field/value labels.
export function defaultLayerName(manifest, sel) {
	const ds = manifest?.datasets?.[sel.dataset];
	const filters = sel.filters ?? {};
	/** @type {string[]} */
	const parts = [];
	for (const [fieldId, vals] of Object.entries(filters)) {
		if (!vals || !vals.length) continue;
		const field = ds?.fields?.[fieldId];
		const labels = vals.map((v) => field?.values?.find((x) => x.id === v)?.label ?? String(v));
		parts.push(labels.join('+'));
	}
	if (parts.length === 0) {
		const dsName = ds?.name?.split(/\s+/)[0] ?? sel.dataset;
		return `${dsName} ${sel.year}`;
	}
	return parts.join(' / ');
}

// Sanitise a free-text layer name into an identifier usable inside math.js
// expressions: leading letter/underscore, then alphanumerics/underscores.
// Display names keep their original spaces/punctuation; slugs are what the
// user types in expressions.
export function slugify(name) {
	const cleaned = String(name)
		.trim()
		.replace(/[^A-Za-z0-9_]+/g, '_')
		.replace(/^_+|_+$/g, '');
	if (!cleaned) return '';
	return /^[A-Za-z_]/.test(cleaned) ? cleaned : `_${cleaned}`;
}

// Parse an expression and return { compiled, symbols } or throw.
export function parseExpression(expr) {
	const node = parse(expr);
	const symbols = new Set();
	node.traverse((n) => {
		if (n.isSymbolNode) symbols.add(n.name);
	});
	return { compiled: node.compile(), symbols: [...symbols] };
}

// Evaluate over a set of per-area inputs.
// inputs: Map<slug, Map<area_code, number>>
// Returns Map<area_code, number> — areas missing in any referenced input or
// producing a non-finite result are skipped.
export function evaluateOverAreas(compiled, symbols, inputs) {
	const out = new Map();
	if (symbols.length === 0) return out;
	const keys = new Set();
	for (const s of symbols) {
		const m = inputs.get(s);
		if (!m) return out; // missing dependency
		for (const k of m.keys()) keys.add(k);
	}
	for (const k of keys) {
		const scope = {};
		let ok = true;
		for (const s of symbols) {
			const v = inputs.get(s).get(k);
			if (v == null || !Number.isFinite(v)) {
				ok = false;
				break;
			}
			scope[s] = v;
		}
		if (!ok) continue;
		try {
			const v = compiled.evaluate(scope);
			if (Number.isFinite(v)) out.set(k, v);
		} catch {
			// e.g. division by zero produces Infinity (skipped above) or other math.js errors
		}
	}
	return out;
}
