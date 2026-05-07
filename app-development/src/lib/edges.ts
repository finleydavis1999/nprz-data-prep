// =============================================================================
// edges.ts
//
// OD flow dataset loading and rendering on the MapLibre map.
//
// Each edge dataset (woonwerk, werkwerk, migration at gemeente + PC4 levels)
// is loaded as a GeoJSON LineString FeatureCollection and added as a
// MapLibre source + line layer.  Arrow symbols are added on a separate symbol
// layer to show flow direction.
//
// Filtering:
//   - If income (ink) or education (opl) filters are active AND the dataset
//     supports breakdown, we query the breakdown parquet instead of the marginal.
//   - Flows below a minimum count (≥ 10 commuters) are excluded.
//   - Top 2000 flows by value are drawn to keep rendering performant.
// =============================================================================

import type { Feature, FeatureCollection } from 'geojson';
import { map } from './map';
import { queuedQuery, dataURL } from './db';
import { EDGE_DATASETS } from './config';

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Load (or refresh) a single edge dataset layer on the map.
 *
 * If the source already exists, its data is updated in place.
 * If it doesn't exist yet, a new source + line layer + arrow layer are created.
 *
 * @param datasetKey  Key from EDGE_DATASETS (e.g. 'woonwerk', 'migration_pc4')
 * @param period      Period string (e.g. '20122017', 'p15-18')
 * @param inkFilter   Income categories to include ([] = all)
 * @param oplFilter   Education categories to include ([] = all)
 * @returns           { min, max } flow values for legend display
 */
export async function loadEdgeLayer(
  datasetKey: string,
  period:     string,
  inkFilter:  string[] = [],
  oplFilter:  string[] = [],
): Promise<{ min: number; max: number }> {
  if (!map) return { min: 0, max: 1 };

  const ds = EDGE_DATASETS.find(d => d.key === datasetKey);
  if (!ds) return { min: 0, max: 1 };

  // Load centroids — keyed as String(id) so lookup works regardless of whether
  // the parquet returns integer or string IDs.
  const centroidsFile = ds.scaleKey === 'pc4'
    ? 'pc4_centroids.json'
    : 'gemeente_centroids.json';
  const centroidsRaw = await fetch(dataURL(centroidsFile)).then(r => r.json());
  const centroids = new Map<string, [number, number]>(
    centroidsRaw.map((c: any) => [String(c.id), [Number(c.lng), Number(c.lat)] as [number, number]])
  );

  // Select parquet: breakdown if filters active and dataset supports it
  const hasFilter   = inkFilter.length > 0 || oplFilter.length > 0;
  let flowUrl       = dataURL(ds.flows);
  let filterWhere   = '';

  if (hasFilter && ds.hasBreakdown) {
    // Breakdown parquet contains income (inks) and education (opl) columns
    flowUrl = dataURL(
      ds.scaleKey === 'pc4'
        ? 'edges_woonwerk_ink_opl_pc4.parquet'
        : 'edges_woonwerk_ink_opl_gem.parquet'
    );
    if (inkFilter.length > 0) filterWhere += ` AND inks IN (${inkFilter.join(',')})`;
    if (oplFilter.length > 0) filterWhere += ` AND opl IN (${oplFilter.join(',')})`;
  }

  // Query: top 2000 flows for the selected period
  const res = await queuedQuery(`
    SELECT "${ds.idCols.origin}"      AS o,
           "${ds.idCols.destination}" AS d,
           SUM(flow_value)            AS flow_value
    FROM read_parquet('${flowUrl}')
    WHERE "${ds.idCols.period}" = '${period}'
    AND flow_value >= 10
    ${filterWhere}
    GROUP BY "${ds.idCols.origin}", "${ds.idCols.destination}"
    ORDER BY flow_value DESC LIMIT 2000
  `);
  const flows = res.toArray().map((r: any) => r.toJSON());

  // Compute flow range for legend
  const flowValues = flows.map((f: any) => Number(f.flow_value)).filter((v: number) => v > 0);
  const maxFlow    = Math.max(...flowValues, 1);
  const minFlow    = flowValues.length > 0 ? Math.min(...flowValues) : 0;

  // Build GeoJSON FeatureCollection
  const features: Feature[] = flows.flatMap((flow: any) => {
    const origin = centroids.get(String(flow.o));
    const dest   = centroids.get(String(flow.d));
    if (!origin || !dest) return [];
    return [{
      type:       'Feature' as const,
      properties: {
        flow_value: flow.flow_value,
        flow_norm:  Number(flow.flow_value) / maxFlow,
        origin_id:  flow.o,
        dest_id:    flow.d,
      },
      geometry: { type: 'LineString' as const, coordinates: [origin, dest] },
    }];
  });

  const geojson: FeatureCollection = { type: 'FeatureCollection', features };
  const sid = `flows-${datasetKey}-source`;
  const lid = `flows-${datasetKey}-layer`;
  const aid = `flows-${datasetKey}-arrows`;

  // Update or create source
  if (map.getSource(sid)) {
    (map.getSource(sid) as maplibregl.GeoJSONSource).setData(geojson);
  } else {
    map.addSource(sid, { type: 'geojson', data: geojson });

    // Flow line layer
    map.addLayer({
      id:     lid,
      type:   'line',
      source: sid,
      layout: { 'line-join': 'round', 'line-cap': 'round' },
      paint: {
        'line-color':   ds.colour,
        'line-opacity': ['interpolate', ['linear'], ['get', 'flow_norm'], 0, 0.15, 1, 0.7],
        'line-width':   ['interpolate', ['linear'], ['get', 'flow_value'],
          50, 0.4, 500, 1, 2000, 2.5, 10000, 5],
      },
    });

    // Arrow direction markers — placed at midpoint along each line
    if (!map.getLayer(aid)) {
      map.addLayer({
        id:     aid,
        type:   'symbol',
        source: sid,
        layout: {
          'symbol-placement':   'line',
          'symbol-spacing':     200,
          'icon-image':         'arrow-icon',
          'icon-size':          0.6,
          'icon-allow-overlap': true,
          'icon-rotate':        90, // point arrow along line direction
        },
        paint: {
          'icon-color':   ds.colour,
          'icon-opacity': ['interpolate', ['linear'], ['get', 'flow_norm'], 0, 0.2, 1, 0.8],
        },
      });
    }
  }

  return { min: minFlow, max: maxFlow };
}