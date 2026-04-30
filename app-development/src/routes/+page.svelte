<script lang="ts">
  import maplibregl from 'maplibre-gl';
  import 'maplibre-gl/dist/maplibre-gl.css';
  import { browser } from '$app/environment';
  import * as duckdb from '@duckdb/duckdb-wasm';

  import {
    INNER_SCALES, OUTER_SCALES, ALL_SCALES,
    VARIABLES, EDGE_DATASETS, NORMALISATIONS, CALC_OPERATORS,
    COLOURS_BLUE, COLOURS_GREEN, NO_DATA,
    findScale, colForScale, varsForScale, sharedScales, scaleChips, isInnerScale,
  } from '$lib/config';
  import type { SpatialExtent, Normalisation, ActiveEdgeLayer } from '$lib/types';
  import type { FeatureCollection, Feature } from 'geojson';

  // ── UI state ──────────────────────────────────────────────
  type Tab = 'nodal' | 'edges' | 'calc';
  let activeTab     = $state<Tab>('nodal');
  let panelOpen     = $state(true);

  // ── Nodal state ───────────────────────────────────────────
  let nodalEnabled  = $state(true);
  let nodalVarKey   = $state('total_population');
  let nodalExtent   = $state<SpatialExtent>('inner');  // inner | outer | both
  let innerScaleKey = $state('buurt');
  let outerScaleKey = $state('gemeente');
  let normalisation = $state<Normalisation>('none');

  // Chips for selected variable
  const chips = $derived(scaleChips(nodalVarKey));

  // When variable changes: keep scale selections if still valid, else reset to first available
  $effect(() => {
    const { inner, outer } = scaleChips(nodalVarKey);
    if (!inner.some(c => c.key === innerScaleKey)) innerScaleKey = inner[0]?.key ?? '';
    if (!outer.some(c => c.key === outerScaleKey)) outerScaleKey = outer[0]?.key ?? '';
    // If chosen extent no longer has chips, fall back
    if (nodalExtent === 'inner' && inner.length === 0) nodalExtent = 'outer';
    if (nodalExtent === 'outer' && outer.length === 0) nodalExtent = 'inner';
    if (nodalExtent === 'both' && (inner.length === 0 || outer.length === 0))
      nodalExtent = inner.length > 0 ? 'inner' : 'outer';
  });

  const nodalVar = $derived(VARIABLES.find(v => v.key === nodalVarKey)!);

  // Extent tabs: only show extents that the variable supports
  const extentOptions = $derived(() => {
    const { inner, outer } = chips;
    const opts: { key: SpatialExtent; label: string }[] = [];
    if (inner.length > 0) opts.push({ key: 'inner', label: 'Study area' });
    if (outer.length > 0) opts.push({ key: 'outer', label: 'Wider region' });
    // "Both" only makes sense when variable exists at scales on both sides
    if (inner.length > 0 && outer.length > 0) opts.push({ key: 'both', label: 'Both' });
    return opts;
  });

  // ── Edge state ────────────────────────────────────────────
  let edgeLayers = $state<ActiveEdgeLayer[]>(
    EDGE_DATASETS.map(d => ({
      datasetKey: d.key,
      period:     d.defaultPeriod,
      visible:    false,
      inkFilter:  null,
      oplFilter:  null,
    }))
  );

  function toggleEdge(key: string) {
    edgeLayers = edgeLayers.map(l =>
      l.datasetKey === key ? { ...l, visible: !l.visible } : l
    );
  }
  function setEdgePeriod(key: string, period: string) {
    edgeLayers = edgeLayers.map(l =>
      l.datasetKey === key ? { ...l, period } : l
    );
  }
  function setEdgeInk(key: string, val: string | null) {
    edgeLayers = edgeLayers.map(l =>
      l.datasetKey === key ? { ...l, inkFilter: val } : l
    );
  }
  function setEdgeOpl(key: string, val: string | null) {
    edgeLayers = edgeLayers.map(l =>
      l.datasetKey === key ? { ...l, oplFilter: val } : l
    );
  }

  const INK_OPTIONS = [
    { value: '1', label: '< 20%' },
    { value: '2', label: '20–40%' },
    { value: '3', label: '40–60%' },
    { value: '4', label: '60–80%' },
    { value: '5', label: '80–100%' },
  ];
  const OPL_OPTIONS = [
    { value: '1', label: 'Laag' },
    { value: '2', label: 'Midden' },
    { value: '3', label: 'Hoog' },
  ];

  // ── Calculator state ──────────────────────────────────────
  let calcEnabled  = $state(false);
  let calcVarA     = $state('total_banen_werk');
  let calcOperator = $state<'+' | '-' | '*' | '/'>('/');
  let calcVarB     = $state('total_inwoners');
  let calcScaleKey = $state('gemeente');

  const calcShared = $derived(sharedScales(calcVarA, calcVarB));
  const calcLabel  = $derived(() => {
    const a = VARIABLES.find(v => v.key === calcVarA)?.label ?? calcVarA;
    const b = VARIABLES.find(v => v.key === calcVarB)?.label ?? calcVarB;
    const ops: Record<string, string> = { '+': '+', '-': '−', '*': '×', '/': '÷' };
    return `${a} ${ops[calcOperator]} ${b}`;
  });

  $effect(() => {
    if (!calcShared.includes(calcScaleKey) && calcShared.length > 0)
      calcScaleKey = calcShared[0];
  });

  // ── MapLibre + DuckDB ─────────────────────────────────────
  let map: maplibregl.Map | null = null;
  let mapReady  = $state(false);
  let db: duckdb.AsyncDuckDB | null = null;
  let conn: any = null;
  let dbReady   = $state(false);
  // Non-reactive store for flow ranges (avoids triggering $effect loop)
  const flowRanges = new Map<string, { min: number; max: number }>();

  // ── Legend state ──────────────────────────────────────────
  let nodalBreaks: number[] = $state([]);
  let calcBreaks:  number[] = $state([]);
  let loading   = $state(false);
  let error     = $state<string | null>(null);

  // ── Quantile helpers ──────────────────────────────────────
  function quantileBreaks(values: number[], n = 4): number[] {
    const sorted = [...values].filter(v => isFinite(v) && v > -99990).sort((a, b) => a - b);
    if (sorted.length < n) return [];
    return Array.from({ length: n - 1 }, (_, i) => {
      const idx = Math.floor(((i + 1) / n) * sorted.length);
      return sorted[Math.min(idx, sorted.length - 1)];
    });
  }

  function classify(value: number, breaks: number[]): number {
    if (value == null || value <= -99990 || !isFinite(value)) return -1;
    for (let i = 0; i < breaks.length; i++) if (value <= breaks[i]) return i;
    return breaks.length;
  }

  // ── Normalisation SQL ─────────────────────────────────────
  function normSQL(col: string, norm: string): string {
    if (norm === 'per_km2')
      // try_cast guards against missing oppervlakte_land_in_ha at gemeente scale
      return `CASE WHEN TRY_CAST(oppervlakte_land_in_ha AS DOUBLE) > 0 
              THEN "${col}"::DOUBLE / (oppervlakte_land_in_ha::DOUBLE / 100.0) 
              ELSE NULL END`;
    if (norm === 'per_1000')
      return `CASE WHEN TRY_CAST(aantal_inwoners AS DOUBLE) > 0 
              THEN ("${col}"::DOUBLE / aantal_inwoners::DOUBLE) * 1000.0 
              ELSE NULL END`;
    return `"${col}"::DOUBLE`;
  }

  function dataURL(filename: string): string {
    return new URL(`/data/${filename}`, window.location.origin).href;
  }

  // ── Fetch rows for one scale + variable ───────────────────
  async function fetchRows(
    varKey: string, scaleKey: string, norm: Normalisation
  ): Promise<{ id: string | number; value: number }[]> {
    const scale = findScale(scaleKey);
    if (!scale) return [];
    const v = VARIABLES.find(x => x.key === varKey);
    const src = v?.source as string | undefined;

    if (src === 'nodes') {
      const url = dataURL('nodes_summary_gem.parquet');
      const res = await conn.query(
        `SELECT gemeentecode AS id, "${varKey}"::DOUBLE AS value FROM read_parquet('${url}') WHERE jaar = 2017`
      );
      return res.toArray().map((r: any) => r.toJSON());
    }

    if (src === 'nodes_pc4') {
      const col = varKey.replace(/^pc4_/, '');
      const url = dataURL('nodes_summary_pc4.parquet');
      const res = await conn.query(
        `SELECT postcode AS id, "${col}"::DOUBLE AS value FROM read_parquet('${url}') WHERE jaar = 2017`
      );
      return res.toArray().map((r: any) => r.toJSON());
    }

    // Income breakdown: routes by scale (gemeente vs pc4)
    if (src === 'nodes_ink') {
      const inkCat = varKey.split('_').pop()!;
      const inkLabels: Record<string, string> = {
        '1': '< 20%', '2': '20-40%', '3': '40-60%', '4': '60-80%', '5': '80-100%'
      };
      const inkLabel = inkLabels[inkCat];
      if (scaleKey === 'pc4') {
        const url = dataURL('nodes_demo_inkomen_pc4.parquet');
        const res = await conn.query(`
          SELECT postcode AS id, SUM(n)::DOUBLE AS value
          FROM read_parquet('${url}')
          WHERE jaar = 2017 AND ink_label = '${inkLabel}'
          GROUP BY postcode
        `);
        return res.toArray().map((r: any) => r.toJSON());
      } else {
        const url = dataURL('nodes_demo_inkomen_gem.parquet');
        const res = await conn.query(`
          SELECT gemeentecode AS id, SUM(n)::DOUBLE AS value
          FROM read_parquet('${url}')
          WHERE jaar = 2017 AND ink_label = '${inkLabel}'
          GROUP BY gemeentecode
        `);
        return res.toArray().map((r: any) => r.toJSON());
      }
    }

    // Education breakdown: routes by scale
    if (src === 'nodes_opl') {
      const oplCat = varKey.split('_').pop()!;
      const oplLabels: Record<string, string> = { '1': 'Laag', '2': 'Midden', '3': 'Hoog' };
      const oplLabel = oplLabels[oplCat];
      if (scaleKey === 'pc4') {
        const url = dataURL('nodes_demo_opleiding_pc4.parquet');
        const res = await conn.query(`
          SELECT postcode AS id, SUM(n)::DOUBLE AS value
          FROM read_parquet('${url}')
          WHERE jaar = 2017 AND opl_label = '${oplLabel}'
          GROUP BY postcode
        `);
        return res.toArray().map((r: any) => r.toJSON());
      } else {
        const url = dataURL('nodes_demo_opleiding_gem.parquet');
        const res = await conn.query(`
          SELECT gemeentecode AS id, SUM(n)::DOUBLE AS value
          FROM read_parquet('${url}')
          WHERE jaar = 2017 AND opl_label = '${oplLabel}'
          GROUP BY gemeentecode
        `);
        return res.toArray().map((r: any) => r.toJSON());
      }
    }

    if (src === 'flow_summary') {
      const url = dataURL('flow_summary.parquet');
      const res = await conn.query(
        `SELECT origin_id AS id, "${varKey}"::DOUBLE AS value FROM read_parquet('${url}')`
      );
      return res.toArray().map((r: any) => r.toJSON());
    }

    // CBS stats parquet — resolve actual column name for this scale
    const col    = colForScale(varKey, scaleKey);
    const url    = dataURL(scale.stats);

    // Check column exists before querying
    const desc = await conn.query(`DESCRIBE SELECT * FROM read_parquet('${url}') LIMIT 0`);
    const cols  = desc.toArray().map((r: any) => r.toJSON().column_name as string);
    if (!cols.includes(col)) {
      console.warn(`[fetchRows] Column "${col}" (varKey="${varKey}") not found in ${scale.stats}`);
      console.warn(`[fetchRows] Available columns:`, cols.filter((c: string) => c.includes('pct') || c.includes('percentage') || c.includes('aantal')).slice(0, 20));
      return [];
    }

    console.log(`[fetchRows] OK: varKey="${varKey}" → col="${col}" at scale="${scaleKey}"`);

    const expr = normSQL(col, norm);
    const res  = await conn.query(
      `SELECT "${scale.id}" AS id, (${expr}) AS value FROM read_parquet('${url}')`
    );
    const rows = res.toArray().map((r: any) => r.toJSON());
    if (rows.length > 0) console.log(`[fetchRows] First row sample:`, rows[0], `id type: ${typeof rows[0].id}`);
    // Null out CBS suppressed sentinel values (-99997, -99995, -99994 etc.)
    return rows.map((r: any) => ({
      id: r.id,
      value: (r.value !== null && r.value > -99990) ? r.value : NaN,
    }));
  }

  // ── Apply feature state for one scale ────────────────────
  async function applyScale(
    varKey: string, scaleKey: string, norm: Normalisation
  ): Promise<number[]> {
    const sourceId = `${scaleKey}-source`;
    try { map!.removeFeatureState({ source: sourceId }); } catch (_) {}

    const rows   = await fetchRows(varKey, scaleKey, norm);
    const values = rows.map(r => Number(r.value)).filter(v => isFinite(v) && v > -99990);
    const breaks = quantileBreaks(values);

    if (rows.length === 0) {
      console.warn(`[applyScale] No rows for varKey="${varKey}" scale="${scaleKey}"`);
      return [];
    }

    for (const row of rows) {
      // promoteId preserves GeoJSON property type. String codes (BU/GM/WK) stay strings;
      // PC4 numeric codes stay numbers. Pass raw id — don't coerce.
      map!.setFeatureState(
        { source: sourceId, id: row.id },
        { cls: classify(Number(row.value), breaks) }
      );
    }
    return breaks;
  }

  // ── Calculator: fetch column as id→value map ──────────────
  async function fetchMap(varKey: string, scaleKey: string): Promise<Map<string | number, number>> {
    const rows = await fetchRows(varKey, scaleKey, 'none');
    return new Map(rows.map(r => [r.id, Number(r.value)]));
  }

  async function applyCalc(): Promise<number[]> {
    const scale = findScale(calcScaleKey);
    if (!scale) return [];
    const sourceId = `${calcScaleKey}-source`;
    try { map!.removeFeatureState({ source: sourceId }); } catch (_) {}

    const [mapA, mapB] = await Promise.all([
      fetchMap(calcVarA, calcScaleKey),
      fetchMap(calcVarB, calcScaleKey),
    ]);
    const ids = [...new Set([...mapA.keys(), ...mapB.keys()])];
    const rows = ids.map(id => {
      const a = mapA.get(id) ?? NaN;
      const b = mapB.get(id) ?? NaN;
      let value: number;
      if (!isFinite(a) || !isFinite(b)) value = NaN;
      else if (calcOperator === '/' && b === 0) value = NaN;
      else if (calcOperator === '+') value = a + b;
      else if (calcOperator === '-') value = a - b;
      else if (calcOperator === '*') value = a * b;
      else value = a / b;
      return { id, value };
    });

    const values = rows.map(r => r.value).filter(v => isFinite(v));
    const breaks = quantileBreaks(values);
    for (const row of rows) {
      map!.setFeatureState(
        { source: sourceId, id: row.id },
        { cls: classify(row.value, breaks) }
      );
    }
    return breaks;
  }

  // ── Load one edge dataset ─────────────────────────────────
  async function loadEdgeLayer(
    datasetKey: string, period: string,
    inkFilter: string | null = null, oplFilter: string | null = null
  ) {
    if (!map) return;
    const ds = EDGE_DATASETS.find(d => d.key === datasetKey);
    if (!ds) return;

    const centroidsFile = ds.scaleKey === 'pc4'
      ? 'pc4_centroids.json'
      : 'gemeente_centroids.json';
    const centroidsRaw = await fetch(dataURL(centroidsFile)).then(r => r.json());
    const centroids = new Map<string, [number, number]>(
      centroidsRaw.map((c: any) => [c.id, [c.lng, c.lat] as [number, number]])
    );

    // Edge income/education filtering requires breakdown parquets (future R pipeline work).
    // Currently edge parquets are aggregated marginal totals — no inks/opl columns.
    // TODO: export edges_woonwerk_ink_gem.parquet etc. from 03_process_example_data.R
    const flowUrl = dataURL(ds.flows);

    const res = await conn.query(`
      SELECT "${ds.idCols.origin}" AS o, "${ds.idCols.destination}" AS d,
             flow_value
      FROM read_parquet('${flowUrl}')
      WHERE "${ds.idCols.period}" = '${period}'
      ORDER BY flow_value DESC LIMIT 600
    `);
    const flows = res.toArray().map((r: any) => r.toJSON());
    const flowValues = flows.map((f: any) => Number(f.flow_value)).filter((v: number) => v > 0);
    const maxFlow = Math.max(...flowValues, 1);
    const minFlow = flowValues.length > 0 ? Math.min(...flowValues) : 0;
    // Store in non-reactive map to avoid triggering $effect loop
    flowRanges.set(datasetKey, { min: minFlow, max: maxFlow });

    const features: Feature[] = flows.flatMap((flow: any) => {
      const origin = centroids.get(flow.o);
      const dest   = centroids.get(flow.d);
      if (!origin || !dest) return [];
      return [{
        type: 'Feature' as const,
        properties: {
          flow_value: flow.flow_value,
          flow_norm:  Number(flow.flow_value) / maxFlow,
        },
        geometry: { type: 'LineString' as const, coordinates: [origin, dest] },
      }];
    });

    const geojson: FeatureCollection = { type: 'FeatureCollection', features };
    const sid = `flows-${datasetKey}-source`;
    const lid = `flows-${datasetKey}-layer`;

    if (map.getSource(sid)) {
      (map.getSource(sid) as maplibregl.GeoJSONSource).setData(geojson);
    } else {
      map.addSource(sid, { type: 'geojson', data: geojson });
      map.addLayer({
        id: lid, type: 'line', source: sid,
        layout: { 'line-join': 'round', 'line-cap': 'round' },
        paint: {
          'line-color':   ds.colour,
          'line-opacity': ['interpolate', ['linear'], ['get', 'flow_norm'], 0, 0.15, 1, 0.7],
          'line-width':   ['interpolate', ['linear'], ['get', 'flow_value'],
            50, 0.4, 500, 1, 2000, 2.5, 10000, 5],
        },
      });
    }
  }

  // ── Colour expression helper ──────────────────────────────
  // Cast to any: MapLibre's TS types are very strict about expression arrays
  // but the runtime accepts this correctly.
  function colourExpr(colours: string[]): any {
    return [
      'case',
      ['==', ['feature-state', 'cls'], -1], NO_DATA,
      ['==', ['feature-state', 'cls'], 0],  colours[0],
      ['==', ['feature-state', 'cls'], 1],  colours[1],
      ['==', ['feature-state', 'cls'], 2],  colours[2],
      ['==', ['feature-state', 'cls'], 3],  colours[3],
      NO_DATA,
    ];
  }

  // ── Master update ─────────────────────────────────────────
  let updateTimer: ReturnType<typeof setTimeout> | null = null;
  let updateRunning = false;

  function scheduleUpdate() {
    if (updateTimer) clearTimeout(updateTimer);
    updateTimer = setTimeout(async () => {
      if (updateRunning) return;
      updateRunning = true;
      try { await runUpdate(); }
      finally { updateRunning = false; }
    }, 120);
  }

  async function runUpdate() {
    if (!mapReady || !dbReady) return;
    loading = true; error = null;

    try {
      // Hide all choropleth layers
      for (const s of ALL_SCALES) {
        setLayerVis(`${s.key}-fill`, 'none');
        setLayerVis(`${s.key}-outline`, 'none');
      }
      setLayerVis('inner-mask', 'none');

      if (nodalEnabled) {
        const showInner = nodalExtent === 'inner' || nodalExtent === 'both';
        const showOuter = nodalExtent === 'outer' || nodalExtent === 'both';

        if (showInner && showOuter && innerScaleKey && outerScaleKey) {
          // BOTH mode: fetch both scales in parallel, compute unified breaks,
          // apply ALL feature states before making anything visible (prevents grey flicker).
          const [innerRows, outerRows] = await Promise.all([
            fetchRows(nodalVarKey, innerScaleKey, normalisation),
            fetchRows(nodalVarKey, outerScaleKey, normalisation),
          ]);
          const allValues = [...innerRows, ...outerRows]
            .map(r => Number(r.value))
            .filter(v => isFinite(v) && v > -99990);
          const unifiedBreaks = quantileBreaks(allValues);
          nodalBreaks = unifiedBreaks;

          // Step 1: clear all stale feature states, yield one frame, then apply new ones.
          // The yield prevents a render between clear and re-apply (which causes grey flicker).
          try { map!.removeFeatureState({ source: `${innerScaleKey}-source` }); } catch (_) {}
          try { map!.removeFeatureState({ source: `${outerScaleKey}-source` }); } catch (_) {}
          await new Promise(r => requestAnimationFrame(r));

          for (const row of innerRows) {
            map!.setFeatureState(
              { source: `${innerScaleKey}-source`, id: row.id },
              { cls: classify(Number(row.value), unifiedBreaks) }
            );
          }
          for (const row of outerRows) {
            map!.setFeatureState(
              { source: `${outerScaleKey}-source`, id: row.id },
              { cls: classify(Number(row.value), unifiedBreaks) }
            );
          }

          // Step 2: set colours and make visible — inner fully opaque above mask
          const innerScaleObj = findScale(innerScaleKey);
          const innerOpacity = innerScaleObj?.type === 'point' ? 0.85 : 0.85;
          setColours(innerScaleKey, COLOURS_BLUE, innerOpacity);
          setColours(outerScaleKey, COLOURS_BLUE, 0.6);

          setLayerVis(`${outerScaleKey}-fill`,    'visible');
          setLayerVis(`${outerScaleKey}-outline`, 'visible');
          setLayerVis('inner-mask',               'visible');
          setLayerVis(`${innerScaleKey}-fill`,    'visible');
          setLayerVis(`${innerScaleKey}-outline`, 'visible');

        } else {
          // Single extent — inner or outer only
          if (showInner && innerScaleKey) {
            // Fetch and set states before making visible
            const rows = await fetchRows(nodalVarKey, innerScaleKey, normalisation);
            const values = rows.map(r => Number(r.value)).filter(v => isFinite(v) && v > -99990);
            const breaks = quantileBreaks(values);
            nodalBreaks = breaks;
            try { map!.removeFeatureState({ source: `${innerScaleKey}-source` }); } catch (_) {}
            for (const row of rows) {
              map!.setFeatureState(
                { source: `${innerScaleKey}-source`, id: row.id },
                { cls: classify(Number(row.value), breaks) }
              );
            }
            const innerScaleObj = findScale(innerScaleKey);
            setColours(innerScaleKey, COLOURS_BLUE, innerScaleObj?.type === 'point' ? 0.85 : 0.85);
            setLayerVis(`${innerScaleKey}-fill`,    'visible');
            setLayerVis(`${innerScaleKey}-outline`, 'visible');
          }
          if (showOuter && outerScaleKey) {
            const rows = await fetchRows(nodalVarKey, outerScaleKey, normalisation);
            const values = rows.map(r => Number(r.value)).filter(v => isFinite(v) && v > -99990);
            const breaks = quantileBreaks(values);
            nodalBreaks = breaks;
            try { map!.removeFeatureState({ source: `${outerScaleKey}-source` }); } catch (_) {}
            for (const row of rows) {
              map!.setFeatureState(
                { source: `${outerScaleKey}-source`, id: row.id },
                { cls: classify(Number(row.value), breaks) }
              );
            }
            setColours(outerScaleKey, COLOURS_BLUE, 0.7);
            setLayerVis(`${outerScaleKey}-fill`,    'visible');
            setLayerVis(`${outerScaleKey}-outline`, 'visible');
          }
        }
      }

      if (calcEnabled && calcShared.length > 0) {
        setColours(calcScaleKey, COLOURS_GREEN, isInnerScale(calcScaleKey) ? 0.85 : 0.6);
        setLayerVis(`${calcScaleKey}-fill`,    'visible');
        setLayerVis(`${calcScaleKey}-outline`, 'visible');
        calcBreaks = await applyCalc();
      }

      // Edge layers
      for (const layer of edgeLayers) {
        const lid = `flows-${layer.datasetKey}-layer`;
        if (layer.visible) {
          await loadEdgeLayer(layer.datasetKey, layer.period,
                              layer.inkFilter, layer.oplFilter);
          setLayerVis(lid, 'visible');
        } else if (map?.getLayer(lid)) {
          setLayerVis(lid, 'none');
        }
      }

    } catch (e) {
      error = `${e}`;
      console.error(e);
    }
    loading = false;
  }

  function setLayerVis(id: string, vis: 'visible' | 'none') {
    if (map?.getLayer(id)) map.setLayoutProperty(id, 'visibility', vis);
  }

  function setColours(scaleKey: string, colours: string[], opacity: number) {
    const fillId = `${scaleKey}-fill`;
    const layer = map?.getLayer(fillId);
    if (!layer) return;
    if (layer.type === 'symbol') {
      // Grid layers: SDF square icon — colour via icon-color
      map!.setPaintProperty(fillId, 'icon-color', colourExpr(colours));
      map!.setPaintProperty(fillId, 'icon-opacity', opacity);
    } else if (layer.type === 'circle') {
      // Fallback if circle layers still exist
      map!.setPaintProperty(fillId, 'circle-color', colourExpr(colours));
      map!.setPaintProperty(fillId, 'circle-opacity', opacity);
    } else {
      map!.setPaintProperty(fillId, 'fill-color', colourExpr(colours));
      map!.setPaintProperty(fillId, 'fill-opacity', opacity);
    }
  }

  $effect(() => {
    const _ = [
      nodalEnabled, nodalVarKey, nodalExtent, innerScaleKey, outerScaleKey, normalisation,
      calcEnabled, calcVarA, calcOperator, calcVarB, calcScaleKey,
      JSON.stringify(edgeLayers),
    ];
    if (mapReady && dbReady) scheduleUpdate();
  });

  // ── MapLibre init ─────────────────────────────────────────
  function initMap(container: HTMLDivElement) {
    map = new maplibregl.Map({
      container,
      style:           'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json',
      center:          [4.48, 51.92],
      zoom:            10,
      scrollZoom:      true,
      dragPan:         true,
      dragRotate:      false,
      touchZoomRotate: true,
    });

    map.addControl(new maplibregl.ScaleControl({ unit: 'metric' }), 'bottom-right');
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-right');

    map.on('load', () => {
      // Sources
      for (const scale of ALL_SCALES) {
        map!.addSource(`${scale.key}-source`, {
          type: 'geojson',
          data: new URL(`/data/${scale.geojson}`, window.location.origin).href,
          promoteId: scale.id,
        });
      }
      map!.addSource('boundary-source', {
        type: 'geojson',
        data: new URL('/data/rotterdam_boundary.geojson', window.location.origin).href,
      });

      // Outer fill layers
      for (const scale of OUTER_SCALES) {
        map!.addLayer({
          id: `${scale.key}-fill`, type: 'fill', source: `${scale.key}-source`,
          layout: { visibility: 'none' },
          paint: { 'fill-color': colourExpr(COLOURS_BLUE), 'fill-opacity': 0.6 },
        });
        map!.addLayer({
          id: `${scale.key}-outline`, type: 'line', source: `${scale.key}-source`,
          layout: { visibility: 'none' },
          paint: { 'line-color': '#ffffff', 'line-width': 0.5 },
        });
      }

      map!.addLayer({
        id: 'inner-mask', type: 'fill', source: 'boundary-source',
        layout: { visibility: 'none' },
        paint: { 'fill-color': '#ffffff', 'fill-opacity': 1.0 },
      });

      // Register SDF square icon for grid rendering.
      // SDF (signed distance field): white square mask that MapLibre recolours via icon-color.
      // 62x62 fill inside 64x64 canvas leaves a 1px gap between adjacent cells at exact tiling.
      const sqSize = 64;
      const sqCanvas = document.createElement('canvas');
      sqCanvas.width = sqSize; sqCanvas.height = sqSize;
      const sqCtx = sqCanvas.getContext('2d')!;
      sqCtx.clearRect(0, 0, sqSize, sqSize);   // transparent background → basemap shows through gaps
      sqCtx.fillStyle = '#ffffff';
      sqCtx.fillRect(1, 1, sqSize - 2, sqSize - 2);  // 1px inset = grid-line gap at exact tiling
      const sqData = sqCtx.getImageData(0, 0, sqSize, sqSize);
      map!.addImage('grid-square', sqData, { sdf: true });

      // icon-size = target_screen_px / 64.
      // Values computed from metres-per-pixel at lat 51.9° (Rotterdam), 88% fill to avoid overlap.
      // 100m: z10=0.029, z12=0.117, z14=0.467, z16=1.866
      // 500m: z10=0.146, z12=0.583, z14=2.332, z16=9.329
      // Inner layers
      for (const scale of INNER_SCALES) {
        if (scale.type === 'point') {
          map!.addLayer({
            id: `${scale.key}-fill`, type: 'symbol', source: `${scale.key}-source`,
            layout: {
              visibility: 'none',
              'icon-image': 'grid-square',
              'icon-allow-overlap': true,
              'icon-ignore-placement': true,
              'icon-pitch-alignment': 'map',
              'icon-rotation-alignment': 'map',
              'icon-size': scale.key === '100m'
                ? ['interpolate', ['exponential', 2], ['zoom'],
                    10, 0.029, 11, 0.058, 12, 0.117, 13, 0.233, 14, 0.467, 15, 0.933, 16, 1.866]
                : ['interpolate', ['exponential', 2], ['zoom'],
                    9,  0.073, 10, 0.146, 11, 0.292, 12, 0.583, 13, 1.166, 14, 2.332, 15, 4.665],
            },
            paint: {
              'icon-color': colourExpr(COLOURS_BLUE),
              'icon-opacity': 0.85,
            },
          });
        } else {
          map!.addLayer({
            id: `${scale.key}-fill`, type: 'fill', source: `${scale.key}-source`,
            layout: { visibility: 'none' },
            paint: { 'fill-color': colourExpr(COLOURS_BLUE), 'fill-opacity': 0.85 },
          });
          map!.addLayer({
            id: `${scale.key}-outline`, type: 'line', source: `${scale.key}-source`,
            layout: { visibility: 'none' },
            paint: { 'line-color': '#ffffff', 'line-width': 0.4 },
          });
        }
      }

      // Boundary line — always on top
      map!.addLayer({
        id: 'boundary-line', type: 'line', source: 'boundary-source',
        paint: { 'line-color': '#e63946', 'line-width': 2, 'line-dasharray': [4, 2] },
      });

      mapReady = true;
      if (dbReady) runUpdate();
    });
  }

  // ── DuckDB init ───────────────────────────────────────────
  async function initDuckDB() {
    const bundles   = duckdb.getJsDelivrBundles();
    const bundle    = await duckdb.selectBundle(bundles);
    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
    );
    const worker = new Worker(workerUrl);
    db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);
    conn    = await db.connect();
    dbReady = true;
    if (mapReady) runUpdate();
  }

  $effect(() => {
    if (!browser) return;
    initDuckDB().catch(e => { error = `DuckDB failed: ${e}`; });
  });

  // ── Formatting ────────────────────────────────────────────
  function fmt(v: number): string {
    if (!isFinite(v)) return '–';
    if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
    if (Math.abs(v) >= 1e3) return `${(v / 1e3).toFixed(1)}k`;
    return Math.abs(v) < 10 ? v.toFixed(2) : v.toFixed(0);
  }

  // Grouped variables for selects
  function groupedVars(): Map<string, typeof VARIABLES> {
    const grouped = new Map<string, typeof VARIABLES>();
    for (const v of VARIABLES) {
      if (!grouped.has(v.group)) grouped.set(v.group, []);
      grouped.get(v.group)!.push(v);
    }
    return grouped;
  }
</script>

<!-- ── Map ───────────────────────────────────────────────── -->
<div use:initMap class="map"></div>

<!-- ── Top header ────────────────────────────────────────── -->
<div class="header">
  <button class="header-toggle" onclick={() => panelOpen = !panelOpen}
          title={panelOpen ? 'Collapse panel' : 'Expand panel'}>
    {panelOpen ? '◀' : '▶'}
  </button>
  <span class="app-title">NPRZ Spatial Explorer</span>

  <div class="tab-bar">
    {#each ([['nodal','Nodal','#045a8d'],['edges','Edges','#e63946'],['calc','Calculate','#238b45']] as const) as [key, lbl, col]}
      <button class="tab" class:tab-active={activeTab === key}
              onclick={() => { activeTab = key as Tab; panelOpen = true; }}>
        <span class="dot" style="background:{col}"></span>{lbl}
      </button>
    {/each}
  </div>

  {#if loading}
    <span class="status loading">Loading…</span>
  {:else if error}
    <span class="status err" title={error}>Error</span>
  {/if}
</div>

<!-- ── Side panel ────────────────────────────────────────── -->
{#if panelOpen}
<div class="panel">

  <!-- ── NODAL TAB ── -->
  {#if activeTab === 'nodal'}
    <label class="row toggle-row">
      <input type="checkbox" bind:checked={nodalEnabled} />
      <span class="toggle-lbl">Show choropleth layer</span>
    </label>

    {#if nodalEnabled}
      <div class="field">
        <span class="flbl">Variable</span>
        <select bind:value={nodalVarKey}>
          {#each [...groupedVars()] as [group, vars]}
            <optgroup label={group}>
              {#each vars as v}<option value={v.key}>{v.label}</option>{/each}
            </optgroup>
          {/each}
        </select>
      </div>

      <!-- Spatial extent -->
      <div class="field">
        <span class="flbl">Spatial extent</span>
        <div class="chips">
          {#each extentOptions() as opt}
            <button class="chip" class:chip-on={nodalExtent === opt.key}
                    onclick={() => nodalExtent = opt.key}>
              {opt.label}
            </button>
          {/each}
        </div>
      </div>

      <!-- Inner scale (shown when inner or both) -->
      {#if (nodalExtent === 'inner' || nodalExtent === 'both') && chips.inner.length > 0}
        <div class="field">
          <span class="flbl">{nodalExtent === 'both' ? 'Inner scale' : 'Scale'}</span>
          <div class="chips">
            {#each chips.inner as c}
              <button class="chip" class:chip-on={innerScaleKey === c.key}
                      onclick={() => innerScaleKey = c.key}>{c.label}</button>
            {/each}
          </div>
        </div>
      {/if}

      <!-- Outer scale (shown when outer or both) -->
      {#if (nodalExtent === 'outer' || nodalExtent === 'both') && chips.outer.length > 0}
        <div class="field">
          <span class="flbl">{nodalExtent === 'both' ? 'Outer scale' : 'Scale'}</span>
          <div class="chips">
            {#each chips.outer as c}
              <button class="chip" class:chip-on={outerScaleKey === c.key}
                      onclick={() => outerScaleKey = c.key}>{c.label}</button>
            {/each}
          </div>
        </div>
      {/if}

      <!-- Normalisation -->
      <div class="field">
        <span class="flbl">Normalise</span>
        <div class="chips">
          {#each NORMALISATIONS as n}
            {@const disabled = n.key !== 'none' && !nodalVar?.canNormalise}
            <button class="chip" class:chip-on={normalisation === n.key}
                    class:chip-off={disabled}
                    onclick={() => { if (!disabled) normalisation = n.key as Normalisation; }}>
              {n.label}
            </button>
          {/each}
        </div>
      </div>
    {/if}
  {/if}

  <!-- ── EDGES TAB ── -->
  {#if activeTab === 'edges'}
    <p class="hint">Toggle flow datasets below. All use gemeente-level spatial IDs.</p>

    {#each EDGE_DATASETS as ds, i}
      {@const layer = edgeLayers[i]}
      <div class="edge-card" class:edge-on={layer.visible}>
        <label class="row toggle-row">
          <input type="checkbox" checked={layer.visible} onchange={() => toggleEdge(ds.key)} />
          <span class="edge-dot" style="background:{ds.colour}"></span>
          <strong>{ds.label}</strong>
        </label>
        <p class="hint">{ds.description}</p>
        {#if layer.visible}
          <div class="field">
            <span class="flbl">Period</span>
            <div class="chips">
              {#each ds.periods as p}
                <button class="chip" class:chip-on={layer.period === p}
                        onclick={() => setEdgePeriod(ds.key, p)}>
                  {ds.periodLabels[p]}
                </button>
              {/each}
            </div>
          </div>

          {#if ds.hasBreakdown}
            <div class="field">
              <span class="flbl">Income group</span>
              <div class="chips">
                <button class="chip" class:chip-on={layer.inkFilter === null}
                        onclick={() => setEdgeInk(ds.key, null)}>All</button>
                {#each INK_OPTIONS as opt}
                  <button class="chip" class:chip-on={layer.inkFilter === opt.value}
                          onclick={() => setEdgeInk(ds.key, opt.value)}>
                    {opt.label}
                  </button>
                {/each}
              </div>
            </div>

            <div class="field">
              <span class="flbl">Education level</span>
              <div class="chips">
                <button class="chip" class:chip-on={layer.oplFilter === null}
                        onclick={() => setEdgeOpl(ds.key, null)}>All</button>
                {#each OPL_OPTIONS as opt}
                  <button class="chip" class:chip-on={layer.oplFilter === opt.value}
                          onclick={() => setEdgeOpl(ds.key, opt.value)}>
                    {opt.label}
                  </button>
                {/each}
              </div>
            </div>
          {/if}
        {/if}
      </div>
    {/each}
  {/if}

  <!-- ── CALC TAB ── -->
  {#if activeTab === 'calc'}
    <label class="row toggle-row">
      <input type="checkbox" bind:checked={calcEnabled} />
      <span class="toggle-lbl">Show computed layer</span>
    </label>

    {#if calcEnabled}
      <div class="field">
        <span class="flbl">Variable A</span>
        <select bind:value={calcVarA}>
          {#each [...groupedVars()] as [group, vars]}
            <optgroup label={group}>
              {#each vars as v}<option value={v.key}>{v.label}</option>{/each}
            </optgroup>
          {/each}
        </select>
      </div>

      <div class="field">
        <span class="flbl">Operator</span>
        <div class="chips">
          {#each CALC_OPERATORS as op}
            <button class="chip" class:chip-on={calcOperator === op.key}
                    onclick={() => calcOperator = op.key as typeof calcOperator}>
              {op.label}
            </button>
          {/each}
        </div>
      </div>

      <div class="field">
        <span class="flbl">Variable B</span>
        <select bind:value={calcVarB}>
          {#each [...groupedVars()] as [group, vars]}
            <optgroup label={group}>
              {#each vars as v}<option value={v.key}>{v.label}</option>{/each}
            </optgroup>
          {/each}
        </select>
      </div>

      {#if calcShared.length > 0}
        <div class="field">
          <span class="flbl">Scale</span>
          <div class="chips">
            {#each ALL_SCALES.filter(s => calcShared.includes(s.key)) as s}
              <button class="chip" class:chip-on={calcScaleKey === s.key}
                      onclick={() => calcScaleKey = s.key}>{s.label}</button>
            {/each}
          </div>
        </div>
        <p class="calc-preview">{calcLabel()}</p>
      {:else}
        <p class="calc-warn">No shared scale — pick different variables.</p>
      {/if}
    {/if}
  {/if}

</div>
{/if}

<!-- ── Legend ─────────────────────────────────────────────── -->
{#if (nodalEnabled && nodalBreaks.length) || (calcEnabled && calcBreaks.length) || edgeLayers.some(l => l.visible)}
<div class="legend">

  {#if nodalEnabled && nodalBreaks.length}
    <div class="legend-title">{nodalVar?.label ?? ''}</div>
    {#if normalisation !== 'none'}
      <div class="legend-sub">{NORMALISATIONS.find(n => n.key === normalisation)?.label}</div>
    {/if}
    <div class="legend-row"><span class="sw" style="background:{NO_DATA}"></span>No data</div>
    {#each COLOURS_BLUE as col, i}
      <div class="legend-row">
        <span class="sw" style="background:{col}"></span>
        <span class="legend-cls">
          {i + 1}.&nbsp;
          {#if i === 0}
            ≤ {fmt(nodalBreaks[0] ?? 0)}
          {:else if i === COLOURS_BLUE.length - 1}
            > {fmt(nodalBreaks[nodalBreaks.length - 1] ?? 0)}
          {:else}
            {fmt(nodalBreaks[i - 1] ?? 0)} – {fmt(nodalBreaks[i] ?? 0)}
          {/if}
        </span>
      </div>
    {/each}
  {/if}

  {#if calcEnabled && calcBreaks.length}
    {#if nodalEnabled && nodalBreaks.length}<div class="divider"></div>{/if}
    <div class="legend-title" style="color:#238b45">{calcLabel()}</div>
    <div class="legend-row"><span class="sw" style="background:{NO_DATA}"></span>No data</div>
    {#each COLOURS_GREEN as col, i}
      <div class="legend-row">
        <span class="sw" style="background:{col}"></span>
        <span class="legend-cls">
          {i + 1}.&nbsp;
          {#if i === 0}
            ≤ {fmt(calcBreaks[0] ?? 0)}
          {:else if i === COLOURS_GREEN.length - 1}
            > {fmt(calcBreaks[calcBreaks.length - 1] ?? 0)}
          {:else}
            {fmt(calcBreaks[i - 1] ?? 0)} – {fmt(calcBreaks[i] ?? 0)}
          {/if}
        </span>
      </div>
    {/each}
  {/if}

  {#each edgeLayers.filter(l => l.visible) as layer}
    {@const ds = EDGE_DATASETS.find(d => d.key === layer.datasetKey)!}
    <div class="divider"></div>
    <div class="legend-row">
      <span class="sw" style="background:{ds.colour}; border-radius:50%"></span>
      <span class="legend-edge-lbl">{ds.label}</span>
    </div>
    <div class="legend-sub" style="padding-left:20px">{ds.periodLabels[layer.period]}</div>
    <!-- Flow weight guide: actual flow value range -->
    {@const range = flowRanges.get(layer.datasetKey)}
    {#if range}
      <div class="flow-guide">
        <span style="height:1px; width:8px; background:{ds.colour}; opacity:.4; display:inline-block; margin-right:3px"></span>
        <span>{fmt(range.min)}</span>
        <span style="flex:1; min-width:8px"></span>
        <span style="height:5px; width:16px; background:{ds.colour}; display:inline-block; margin-right:3px"></span>
        <span>{fmt(range.max)}</span>
      </div>
    {/if}
  {/each}

</div>
{/if}

<style>
  .map {
    position: fixed; inset: 0;
    width: 100vw; height: 100vh;
  }

  /* ── Header ─────────────────────────────────────────────── */
  .header {
    position: fixed; top: 0; left: 0; right: 0;
    z-index: 1000; height: 46px;
    background: #fff;
    border-bottom: 1px solid #e4e2de;
    box-shadow: 0 1px 4px rgba(0,0,0,0.07);
    display: flex; align-items: center;
    padding: 0 0.75rem; gap: 1rem;
    font-family: sans-serif;
  }

  .header-toggle {
    background: none; border: none; cursor: pointer;
    font-size: 0.7rem; color: #888; padding: 0.2rem 0.4rem;
    border-radius: 4px; line-height: 1;
  }
  .header-toggle:hover { background: #f0ede8; }

  .app-title {
    font-weight: 700; font-size: 0.88rem;
    color: #1a1a2e; white-space: nowrap;
  }

  .tab-bar { display: flex; gap: 0.2rem; }

  .tab {
    display: flex; align-items: center; gap: 0.3rem;
    padding: 0.25rem 0.65rem;
    border: 1px solid transparent; border-radius: 5px;
    background: none; cursor: pointer;
    font-size: 0.8rem; font-weight: 500; color: #666;
    transition: background 0.1s, border-color 0.1s;
  }
  .tab:hover { background: #f4f1ec; }
  .tab.tab-active { background: #f4f1ec; border-color: #d4d0c8; color: #1a1a2e; }

  .dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }

  .status {
    font-size: 0.7rem; padding: 0.15rem 0.45rem;
    border-radius: 8px; font-weight: 500;
  }
  .status.loading { background: #e8f0fd; color: #1a56c4; }
  .status.err     { background: #fdecea; color: #c62828; }

  /* ── Panel ──────────────────────────────────────────────── */
  .panel {
    position: fixed; top: 54px; left: 0.75rem;
    z-index: 999; width: 288px;
    max-height: calc(100vh - 70px);
    overflow-y: auto;
    background: #fff;
    border-radius: 10px;
    box-shadow: 0 2px 14px rgba(0,0,0,0.12);
    padding: 0.9rem;
    font-family: sans-serif; font-size: 0.82rem;
    display: flex; flex-direction: column; gap: 0.85rem;
  }

  /* ── Fields ─────────────────────────────────────────────── */
  .field { display: flex; flex-direction: column; gap: 0.3rem; }

  .flbl {
    font-size: 0.68rem; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.06em; color: #999;
  }

  select {
    width: 100%; padding: 0.3rem 0.45rem;
    border: 1px solid #ddd; border-radius: 6px;
    font-size: 0.8rem; background: #fff; color: #222;
  }

  /* ── Chips ──────────────────────────────────────────────── */
  .chips { display: flex; flex-wrap: wrap; gap: 0.28rem; }

  .chip {
    padding: 0.18rem 0.55rem;
    border: 1px solid #ccc; border-radius: 20px;
    background: #fff; font-size: 0.75rem; color: #555;
    cursor: pointer;
    transition: background 0.1s, border-color 0.1s, color 0.1s;
  }
  .chip:hover:not(.chip-off) { background: #f0ede8; border-color: #aaa; }
  .chip-on  { background: #1a1a2e; border-color: #1a1a2e; color: #fff; }
  .chip-off { opacity: 0.28; cursor: not-allowed; }

  /* ── Toggle ─────────────────────────────────────────────── */
  .row { display: flex; align-items: center; }
  .toggle-row { gap: 0.5rem; cursor: pointer; }
  .toggle-lbl { font-size: 0.82rem; }

  /* ── Edge cards ─────────────────────────────────────────── */
  .edge-card {
    border: 1px solid #eee; border-radius: 8px;
    padding: 0.6rem 0.7rem;
    display: flex; flex-direction: column; gap: 0.45rem;
  }
  .edge-card.edge-on { border-color: #ccc; }
  .edge-dot { width: 9px; height: 9px; border-radius: 50%; margin: 0 2px; }
  .hint { margin: 0; font-size: 0.72rem; color: #999; line-height: 1.35; }

  /* ── Calculator ─────────────────────────────────────────── */
  .calc-preview {
    font-size: 0.78rem; color: #238b45; font-style: italic; margin: 0;
  }
  .calc-warn { font-size: 0.78rem; color: #c62828; margin: 0; }

  /* ── Legend ─────────────────────────────────────────────── */
  .legend {
    position: fixed; bottom: 1.5rem; left: 0.75rem;
    z-index: 1000;
    background: #fff; border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.13);
    padding: 0.65rem 0.85rem;
    font-family: sans-serif; font-size: 0.76rem;
    min-width: 130px; max-width: 210px;
  }

  .legend-title {
    font-weight: 700; font-size: 0.78rem;
    color: #1a1a2e; margin-bottom: 0.3rem; line-height: 1.3;
  }
  .legend-sub { font-size: 0.68rem; color: #999; margin-bottom: 0.25rem; }
  .legend-edge-lbl { font-size: 0.74rem; }

  .legend-row {
    display: flex; align-items: center;
    gap: 0.38rem; margin: 0.12rem 0;
  }

  .sw {
    width: 11px; height: 11px; border-radius: 2px;
    border: 1px solid rgba(0,0,0,0.08); flex-shrink: 0;
  }

  .divider { border-top: 1px solid #eee; margin: 0.4rem 0; }

  .flow-guide {
    display: flex; align-items: center; gap: 2px;
    font-size: 0.68rem; color: #888; margin-top: 0.2rem;
    padding-left: 2px;
  }

  .legend-cls {
    font-size: 0.74rem; color: #333; line-height: 1.3;
  }
</style>