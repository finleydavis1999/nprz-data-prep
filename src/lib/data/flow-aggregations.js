// Flow → node aggregators used by the layer calculator. Each takes a flow-
// domain `Map<"${o}|${d}", number>` and produces a node-domain
// `Map<area_code, number>`.
//
// Convention: `net` is *net inflow* — arriving minus leaving. So a node that
// is a sink (more inbound than outbound) has positive net.

const SEP = '|';

function splitKey(key) {
	const i = key.indexOf(SEP);
	return [key.slice(0, i), key.slice(i + 1)];
}

export function aggregateFlow(flowMap, aggName) {
	const out = new Map();
	if (!flowMap) return out;
	for (const [key, value] of flowMap) {
		if (!Number.isFinite(value)) continue;
		const [o, d] = splitKey(key);
		if (aggName === 'inflow') {
			out.set(d, (out.get(d) ?? 0) + value);
		} else if (aggName === 'outflow') {
			out.set(o, (out.get(o) ?? 0) + value);
		} else if (aggName === 'net') {
			out.set(d, (out.get(d) ?? 0) + value);
			out.set(o, (out.get(o) ?? 0) - value);
		}
	}
	return out;
}

export const AGGREGATOR_NAMES = /** @type {const} */ (['inflow', 'outflow', 'net']);
