// =============================================================================
// db.ts
//
// DuckDB-WASM initialisation and all data-fetching logic.
//
// Architecture notes:
//   - A single DuckDB connection (conn) is shared for the lifetime of the page.
//   - All queries are serialised through queuedQuery() to prevent DataCloneError
//     from concurrent queries on the same connection.
//   - fetchRows() is the universal entry point for variable data — it routes to
//     the correct parquet file(s) based on the variable's `source` field and
//     the requested scale key.
//   - All returned values are cleaned: CBS sentinel values (≤ -99990) become NaN.
// =============================================================================

import * as duckdb from '@duckdb/duckdb-wasm';
import { findScale, colForScale, VARIABLES } from './config';
import type { Normalisation } from './types';

// ── Module-level DuckDB handles ───────────────────────────────────────────────
// Exported so that consumers (page.svelte, model.ts, edges.ts) can call
// queuedQuery() and check dbReady without re-initialising.

let db:   duckdb.AsyncDuckDB | null = null;
let conn: any = null;

// Serial query queue — DuckDB-WASM throws DataCloneError when two queries run
// concurrently on the same connection. All conn.query calls go through this
// promise chain so they execute one at a time.
let queryQueue: Promise<any> = Promise.resolve();

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Initialise DuckDB-WASM. Should be called once on page load (inside $effect).
 * Returns the connection object; also stores it in the module-level `conn`.
 */
export async function initDuckDB(): Promise<{ db: duckdb.AsyncDuckDB; conn: any }> {
  const bundles   = duckdb.getJsDelivrBundles();
  const bundle    = await duckdb.selectBundle(bundles);
  const workerUrl = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  );
  const worker = new Worker(workerUrl);
  db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(workerUrl);
  conn = await db.connect();
  return { db, conn };
}

/**
 * Enqueue a SQL query to run on the shared DuckDB connection.
 * Callers await the returned promise; concurrent calls are serialised.
 *
 * @param sql  SQL string (may use read_parquet() with HTTP URLs)
 * @returns    DuckDB result table (call .toArray() to iterate rows)
 */
export function queuedQuery(sql: string): Promise<any> {
  const p = queryQueue.then(() => conn.query(sql));
  queryQueue = p.catch(() => {}); // keep queue alive even if a query fails
  return p;
}

/**
 * Resolve a static data filename to a full HTTP URL.
 * DuckDB reads parquets via HTTP, so all read_parquet() calls need absolute URLs.
 */
export function dataURL(filename: string): string {
  return new URL(`/data/${filename}`, window.location.origin).href;
}

// ── Normalisation SQL ─────────────────────────────────────────────────────────

/**
 * Wrap a column reference with the appropriate normalisation expression.
 * TRY_CAST guards against missing area columns at some scales.
 *
 * @param col   Raw column name (will be double-quoted)
 * @param norm  Normalisation type
 * @returns     SQL expression string
 */
export function normSQL(col: string, norm: string): string {
  if (norm === 'per_km2') {
    return `CASE WHEN TRY_CAST(oppervlakte_land_in_ha AS DOUBLE) > 0
            THEN "${col}"::DOUBLE / (oppervlakte_land_in_ha::DOUBLE / 100.0)
            ELSE NULL END`;
  }
  if (norm === 'per_1000') {
    return `CASE WHEN TRY_CAST(aantal_inwoners AS DOUBLE) > 0
            THEN ("${col}"::DOUBLE / aantal_inwoners::DOUBLE) * 1000.0
            ELSE NULL END`;
  }
  return `"${col}"::DOUBLE`;
}

// ── Universal data fetcher ────────────────────────────────────────────────────

/**
 * Fetch rows for a variable at a given scale with optional normalisation.
 *
 * Routes to the correct parquet based on the variable's `source` field:
 *   - nodes_emp  → nodes_summary_pc4 / nodes_summary_gem
 *   - nodes_ink  → nodes_demo_inkomen_pc4 / nodes_demo_inkomen_gem
 *   - nodes_opl  → nodes_demo_opleiding_pc4 / nodes_demo_opleiding_gem
 *   - flows      → edges_woonwerk_pc4 / edges_woonwerk_gem (aggregated)
 *   - (default)  → CBS stats parquet for the scale (e.g. buurt_2024_stats.parquet)
 *
 * All sentinel values (CBS suppressed: -99997, -99995, etc.) are nulled out to NaN.
 *
 * @returns  Array of { id, value } pairs; id type matches GeoJSON feature ID type
 */
export async function fetchRows(
  varKey: string,
  scaleKey: string,
  norm: Normalisation
): Promise<{ id: string | number; value: number }[]> {
  const scale = findScale(scaleKey);
  if (!scale) return [];

  const v   = VARIABLES.find(x => x.key === varKey);
  const src = v?.source as string | undefined;

  // ── Employment node summaries ─────────────────────────────────────────────
  if (src === 'nodes_emp') {
    // Strip 'nodes_' prefix → actual column name in the summary parquet
    const col = varKey.replace(/^nodes_/, '');
    if (scaleKey === 'pc4') {
      const res = await queuedQuery(
        `SELECT postcode AS id, "${col}"::DOUBLE AS value
         FROM read_parquet('${dataURL('nodes_summary_pc4.parquet')}')
         WHERE jaar = 2017`
      );
      return res.toArray().map((r: any) => r.toJSON());
    } else {
      const res = await queuedQuery(
        `SELECT gemeentecode AS id, "${col}"::DOUBLE AS value
         FROM read_parquet('${dataURL('nodes_summary_gem.parquet')}')
         WHERE jaar = 2017`
      );
      return res.toArray().map((r: any) => r.toJSON());
    }
  }

  // ── Income breakdown (nodes_demo_inkomen) ─────────────────────────────────
  if (src === 'nodes_ink') {
    const inkCat = varKey.split('_').pop()!;
    const inkLabels: Record<string, string> = {
      '1': '< 20%', '2': '20-40%', '3': '40-60%', '4': '60-80%', '5': '80-100%',
    };
    const inkLabel = inkLabels[inkCat];
    if (scaleKey === 'pc4') {
      const res = await queuedQuery(`
        SELECT postcode AS id, SUM(n)::DOUBLE AS value
        FROM read_parquet('${dataURL('nodes_demo_inkomen_pc4.parquet')}')
        WHERE jaar = 2017 AND ink_label = '${inkLabel}'
        GROUP BY postcode
      `);
      return res.toArray().map((r: any) => r.toJSON());
    } else {
      const res = await queuedQuery(`
        SELECT gemeentecode AS id, SUM(n)::DOUBLE AS value
        FROM read_parquet('${dataURL('nodes_demo_inkomen_gem.parquet')}')
        WHERE jaar = 2017 AND ink_label = '${inkLabel}'
        GROUP BY gemeentecode
      `);
      return res.toArray().map((r: any) => r.toJSON());
    }
  }

  // ── Education breakdown (nodes_demo_opleiding) ────────────────────────────
  if (src === 'nodes_opl') {
    const oplCat = varKey.split('_').pop()!;
    const oplLabels: Record<string, string> = { '1': 'Laag', '2': 'Midden', '3': 'Hoog' };
    const oplLabel = oplLabels[oplCat];
    if (scaleKey === 'pc4') {
      const res = await queuedQuery(`
        SELECT postcode AS id, SUM(n)::DOUBLE AS value
        FROM read_parquet('${dataURL('nodes_demo_opleiding_pc4.parquet')}')
        WHERE jaar = 2017 AND opl_label = '${oplLabel}'
        GROUP BY postcode
      `);
      return res.toArray().map((r: any) => r.toJSON());
    } else {
      const res = await queuedQuery(`
        SELECT gemeentecode AS id, SUM(n)::DOUBLE AS value
        FROM read_parquet('${dataURL('nodes_demo_opleiding_gem.parquet')}')
        WHERE jaar = 2017 AND opl_label = '${oplLabel}'
        GROUP BY gemeentecode
      `);
      return res.toArray().map((r: any) => r.toJSON());
    }
  }

  // ── Flow-derived variables ────────────────────────────────────────────────
  // Computed on the fly from the woonwerk edge parquet.
  // Period is fixed at 2012–2017 for nodal display; the Edges tab has its own period picker.
  if (src === 'flows') {
    const period    = '20122017';
    const isPC4     = scaleKey === 'pc4';
    const flowUrl   = dataURL(isPC4 ? 'edges_woonwerk_pc4.parquet' : 'edges_woonwerk_gem.parquet');
    const originCol = 'origin_id';
    const destCol   = 'destination_id';

    let sql = '';
    switch (varKey) {
      case 'flow_outflow':
        sql = `SELECT ${originCol} AS id, SUM(flow_value)::DOUBLE AS value
               FROM read_parquet('${flowUrl}')
               WHERE periode = '${period}' AND ${originCol} != ${destCol}
               GROUP BY ${originCol}`;
        break;
      case 'flow_inflow':
        sql = `SELECT ${destCol} AS id, SUM(flow_value)::DOUBLE AS value
               FROM read_parquet('${flowUrl}')
               WHERE periode = '${period}' AND ${originCol} != ${destCol}
               GROUP BY ${destCol}`;
        break;
      case 'flow_internal':
        sql = `SELECT ${originCol} AS id, SUM(flow_value)::DOUBLE AS value
               FROM read_parquet('${flowUrl}')
               WHERE periode = '${period}' AND ${originCol} = ${destCol}
               GROUP BY ${originCol}`;
        break;
      case 'flow_net':
        // Inflow − outflow per area (signed, can be negative)
        sql = `SELECT id, SUM(flow)::DOUBLE AS value FROM (
                 SELECT ${destCol}  AS id,  flow_value AS flow FROM read_parquet('${flowUrl}')
                 WHERE periode = '${period}' AND ${originCol} != ${destCol}
                 UNION ALL
                 SELECT ${originCol} AS id, -flow_value AS flow FROM read_parquet('${flowUrl}')
                 WHERE periode = '${period}' AND ${originCol} != ${destCol}
               ) GROUP BY id`;
        break;
      case 'flow_n_destinations':
        sql = `SELECT ${originCol} AS id, COUNT(DISTINCT ${destCol})::DOUBLE AS value
               FROM read_parquet('${flowUrl}')
               WHERE periode = '${period}' AND ${originCol} != ${destCol}
               GROUP BY ${originCol}`;
        break;
      case 'flow_n_origins':
        sql = `SELECT ${destCol} AS id, COUNT(DISTINCT ${originCol})::DOUBLE AS value
               FROM read_parquet('${flowUrl}')
               WHERE periode = '${period}' AND ${originCol} != ${destCol}
               GROUP BY ${destCol}`;
        break;
      case 'flow_self_containment':
        // internal / total outflow (including internal)
        sql = `SELECT o.origin_id AS id,
                 COALESCE(i.internal, 0)::DOUBLE / NULLIF(o.outflow, 0) AS value
               FROM (
                 SELECT ${originCol} AS origin_id, SUM(flow_value) AS outflow
                 FROM read_parquet('${flowUrl}')
                 WHERE periode = '${period}' GROUP BY ${originCol}
               ) o
               LEFT JOIN (
                 SELECT ${originCol} AS origin_id, SUM(flow_value) AS internal
                 FROM read_parquet('${flowUrl}')
                 WHERE periode = '${period}' AND ${originCol} = ${destCol}
                 GROUP BY ${originCol}
               ) i ON o.origin_id = i.origin_id`;
        break;
    }
    if (!sql) return [];
    const res = await queuedQuery(sql);
    return res.toArray().map((r: any) => r.toJSON());
  }

  // ── Legacy flow_summary source ────────────────────────────────────────────
  if (src === 'flow_summary') {
    const res = await queuedQuery(
      `SELECT origin_id AS id, "${varKey}"::DOUBLE AS value
       FROM read_parquet('${dataURL('flow_summary.parquet')}')`
    );
    return res.toArray().map((r: any) => r.toJSON());
  }

  // ── Default: CBS stats parquet ────────────────────────────────────────────
  // Resolve the actual column name for this scale (CBS uses different names
  // for the same concept at different spatial levels — see config.ts columnAt).
  const col = colForScale(varKey, scaleKey);
  const url = dataURL(scale.stats);

  // Verify the column exists before querying — avoids cryptic DuckDB errors.
  const desc = await queuedQuery(
    `DESCRIBE SELECT * FROM read_parquet('${url}') LIMIT 0`
  );
  const cols = desc.toArray().map((r: any) => r.toJSON().column_name as string);
  if (!cols.includes(col)) {
    console.warn(
      `[fetchRows] Column "${col}" (varKey="${varKey}") not found in ${scale.stats}.\n` +
      `Available columns (sample): ${cols.filter((c: string) =>
        c.includes('pct') || c.includes('percentage') || c.includes('aantal')
      ).slice(0, 20).join(', ')}`
    );
    return [];
  }

  console.log(`[fetchRows] OK: varKey="${varKey}" → col="${col}" at scale="${scaleKey}"`);

  const expr = normSQL(col, norm);
  const res  = await queuedQuery(
    `SELECT "${scale.id}" AS id, (${expr}) AS value FROM read_parquet('${url}')`
  );
  const rows = res.toArray().map((r: any) => r.toJSON());

  if (rows.length > 0) {
    console.log(`[fetchRows] First row sample:`, rows[0], `id type: ${typeof rows[0].id}`);
  }

  // Null out CBS suppressed sentinel values (-99997, -99995, -99994, etc.)
  return rows.map((r: any) => ({
    id:    r.id,
    value: (r.value !== null && r.value > -99990) ? r.value : NaN,
  }));
}

/**
 * Convenience wrapper: fetch rows and return as a Map<id, value>.
 * Used by the calculator to build per-feature expression values.
 */
export async function fetchMap(
  varKey: string,
  scaleKey: string
): Promise<Map<string | number, number>> {
  const rows = await fetchRows(varKey, scaleKey, 'none');
  return new Map(rows.map(r => [r.id, Number(r.value)]));
}