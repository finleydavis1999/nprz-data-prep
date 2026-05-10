// Choropleth query layer. Builds parameterized DuckDB queries against
// OPFS-cached parquet files registered through `parquet-register`.
//
// Node datasets are single-year. The valueExpr divisor is driven by the
// dataset's manifest `yearAggregation` and is 1 for `sum` (the default for
// node datasets), so the SQL is effectively `SUM(count)` for the chosen year.
import { getDb } from './duckdb.js';
import { ensureRegistered, num, valueExpr } from './parquet-register.js';

function buildSql(parquetName, valueSql, { year, filters = {} }) {
	const wheres = [`year = ${num(year)}`];
	for (const [field, values] of Object.entries(filters)) {
		if (!values || values.length === 0) continue;
		// Field names come from the trusted manifest, not user input.
		wheres.push(`${field} IN (${values.map(num).join(',')})`);
	}
	return `
		SELECT area_code, ${valueSql}
		FROM read_parquet('${parquetName}')
		WHERE ${wheres.join(' AND ')}
		GROUP BY area_code
	`;
}

// Run a choropleth query and return Map<areaCode, value>.
export async function runChoropleth({ dataset, scale, year, filters }) {
	const { name, entry } = await ensureRegistered({ section: 'datasets', dataset, scale });
	const valueSql = valueExpr({ entry, yearMin: year, yearMax: year });
	const db = await getDb();
	const conn = await db.connect();
	try {
		const sql = buildSql(name, valueSql, { year, filters });
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
