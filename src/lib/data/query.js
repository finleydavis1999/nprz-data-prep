// Choropleth query layer. Builds parameterized DuckDB queries against
// OPFS-cached parquet files registered through `registerFileHandle`.
import { getDb, duckdbNamespace } from './duckdb.js';
import { initCache, getOrFetch } from './opfs-cache.js';
import { loadManifest } from './manifest.js';

const registered = new Map();

async function ensureRegistered(dataset, scale) {
	const key = `${dataset}-${scale}`;
	if (!registered.has(key)) registered.set(key, registerParquet(dataset, scale));
	return registered.get(key);
}

async function registerParquet(dataset, scale) {
	const manifest = await loadManifest();
	const ds = manifest.datasets[dataset];
	if (!ds) throw new Error(`unknown dataset: ${dataset}`);
	const relPath = ds.scales[scale];
	if (!relPath) throw new Error(`no scale '${scale}' for dataset '${dataset}'`);

	const versionRoot = await initCache(manifest.version);
	const handle = await getOrFetch(versionRoot, relPath, `/data/${relPath}`);

	const db = await getDb();
	const duckdb = await duckdbNamespace();
	const name = `${dataset}-${scale}.parquet`;
	await db.registerFileHandle(name, handle, duckdb.DuckDBDataProtocol.BROWSER_FSACCESS, true);
	return name;
}

// Quote a numeric IN-list value safely (defense-in-depth — manifest is trusted
// but filter values originate from UI state).
function num(v) {
	const n = Number(v);
	if (!Number.isFinite(n)) throw new Error(`non-numeric filter value: ${v}`);
	return n;
}

function buildSql(parquetName, { year, filters = {} }) {
	const wheres = [`year = ${num(year)}`];
	for (const [field, values] of Object.entries(filters)) {
		if (!values || values.length === 0) continue;
		// Field names come from the trusted manifest, not user input.
		wheres.push(`${field} IN (${values.map(num).join(',')})`);
	}
	return `
		SELECT area_code, SUM(count)::DOUBLE AS value
		FROM read_parquet('${parquetName}')
		WHERE ${wheres.join(' AND ')}
		GROUP BY area_code
	`;
}

// Run a choropleth query and return Map<areaCode, value>.
export async function runChoropleth({ dataset, scale, year, filters }) {
	const parquetName = await ensureRegistered(dataset, scale);
	const db = await getDb();
	const conn = await db.connect();
	try {
		const sql = buildSql(parquetName, { year, filters });
		const result = await conn.query(sql);
		const map = new Map();
		for (const row of result) {
			map.set(row.area_code, row.value);
		}
		return map;
	} finally {
		await conn.close();
	}
}
