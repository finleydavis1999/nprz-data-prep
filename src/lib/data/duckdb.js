// Singleton DuckDB-WASM database, browser-only.
//
// Bundles MVP and EH builds via Vite `?url` imports — `selectBundle` picks the
// best one supported by the runtime (EH is faster; MVP is the safe fallback).
// The WASM and worker scripts are served from the SvelteKit asset pipeline,
// not JSDelivr — keeps the app self-contained.
import { browser } from '$app/environment';

let dbPromise = null;

export async function getDb() {
	if (!browser) throw new Error('getDb() is browser-only');
	if (!dbPromise) dbPromise = init();
	return dbPromise;
}

async function init() {
	const duckdb = await import('@duckdb/duckdb-wasm');
	const [{ default: mvpWasm }, { default: mvpWorker }, { default: ehWasm }, { default: ehWorker }] =
		await Promise.all([
			import('@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url'),
			import('@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url'),
			import('@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url'),
			import('@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url')
		]);

	const bundles = {
		mvp: { mainModule: mvpWasm, mainWorker: mvpWorker },
		eh: { mainModule: ehWasm, mainWorker: ehWorker }
	};
	const bundle = await duckdb.selectBundle(bundles);

	const worker = new Worker(bundle.mainWorker);
	const logger = new duckdb.ConsoleLogger();
	const db = new duckdb.AsyncDuckDB(logger, worker);
	await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
	return db;
}

// Convenience: get the duckdb namespace (for protocol enums etc.) without
// re-importing it at every call site.
export async function duckdbNamespace() {
	if (!browser) throw new Error('duckdbNamespace() is browser-only');
	return import('@duckdb/duckdb-wasm');
}
