// Shared parquet-registration helpers used by both choropleth (`query.js`) and
// flow (`flowQuery.js`) queries. Resolves a manifest entry → fetches via OPFS
// cache → registers a stable filename inside DuckDB-WASM so SQL can do
// `read_parquet('<name>')`.
import { loadManifest } from './manifest.js';
import { dataUrl } from './url.js';

const registered = new Map();

export async function ensureRegistered({ section, dataset, scale }) {
	const key = `${section}-${dataset}-${scale}`;
	if (!registered.has(key)) registered.set(key, registerParquet({ section, dataset, scale }));
	return registered.get(key);
}

async function registerParquet({ section, dataset, scale }) {
	const manifest = await loadManifest();
	const entry = manifest[section]?.[dataset];
	if (!entry) throw new Error(`unknown ${section} entry: ${dataset}`);
	const relPath = entry.scales[scale];
	if (!relPath) throw new Error(`no scale '${scale}' for ${section}.${dataset}`);

	const versionRoot = await initCache(manifest.version);
	const handle = await getOrFetch(versionRoot, relPath, dataUrl(relPath, manifest.version));

	const db = await getDb();
	const duckdb = await duckdbNamespace();
	const name = `${section}-${dataset}-${scale}.parquet`;
	await db.registerFileHandle(name, handle, duckdb.DuckDBDataProtocol.BROWSER_FSACCESS, true);
	return { name, entry };
}

export function num(v) {
	const n = Number(v);
	if (!Number.isFinite(n)) throw new Error(`non-numeric filter value: ${v}`);
	return n;
}

export function valueExpr({ entry, yearMin, yearMax, alias = 'value' }) {
	const years = num(yearMax) - num(yearMin) + 1;
	if (years < 1) throw new Error(`invalid year range: ${yearMin}..${yearMax}`);
	const mode = entry.yearAggregation ?? 'sum';
	const col = entry.countCol ?? 'count';
	const isRawCount = col === 'count';
	let divisor;
	switch (mode) {
		case 'mean':   divisor = years;       break;
		case 'daily':  divisor = years * 365; break;
		case 'sum':    divisor = 1;           break;
		default: throw new Error(`unknown yearAggregation: ${mode}`);
	}
	let expr;
	if (isRawCount) {
		expr = divisor === 1 ? `SUM(count)::DOUBLE` : `(SUM(count)::DOUBLE / ${divisor})`;
	} else {
		expr = `"${col}"::DOUBLE`;
	}
	return alias ? `${expr} AS ${alias}` : expr;
}