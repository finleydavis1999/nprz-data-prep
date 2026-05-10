// Flow (OD) query layer. Aggregates edge parquet by (o_code, d_code) for the
// current year range + filters. Returns the full aggregated set so downstream
// calculations have everything; the UI applies a client-side `minWeight`
// cutoff to keep the rendered set tractable.
import { getDb } from './duckdb.js';
import { ensureRegistered, num, valueExpr } from './parquet-register.js';

function buildSql(parquetName, valueSql, { yearMin, yearMax, filters = {}, includeSelfLoops }) {
	const wheres = [`year BETWEEN ${num(yearMin)} AND ${num(yearMax)}`];
	for (const [field, values] of Object.entries(filters)) {
		if (!values || values.length === 0) continue;
		wheres.push(`${field} IN (${values.map(num).join(',')})`);
	}
	if (!includeSelfLoops) wheres.push('o_code <> d_code');
	return `
		SELECT o_code AS o, d_code AS d, ${valueSql}
		FROM read_parquet('${parquetName}')
		WHERE ${wheres.join(' AND ')}
		GROUP BY o_code, d_code
		HAVING SUM(count) > 0
		ORDER BY value DESC
	`;
}

// Run a flow query and return { flows: [{o,d,value}], min, max }.
export async function runFlows({
	dataset,
	scale = 'gem',
	yearMin,
	yearMax,
	filters = {},
	includeSelfLoops = false
}) {
	const { name, entry } = await ensureRegistered({ section: 'flows', dataset, scale });
	const valueSql = valueExpr({ entry, yearMin, yearMax });
	const db = await getDb();
	const conn = await db.connect();
	try {
		const sql = buildSql(name, valueSql, { yearMin, yearMax, filters, includeSelfLoops });
		const result = await conn.query(sql);
		const flows = [];
		let min = Infinity;
		let max = -Infinity;
		for (const row of result) {
			const v = Number(row.value);
			if (!Number.isFinite(v)) continue;
			flows.push({ o: row.o, d: row.d, value: v });
			if (v < min) min = v;
			if (v > max) max = v;
		}
		return { flows, min: flows.length ? min : 0, max: flows.length ? max : 0 };
	} finally {
		await conn.close();
	}
}
