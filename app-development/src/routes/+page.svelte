<script lang="ts">
  import maplibregl from 'maplibre-gl';
  import 'maplibre-gl/dist/maplibre-gl.css';
  import { browser } from '$app/environment';
  import * as duckdb from '@duckdb/duckdb-wasm';

  import {
    INNER_SCALES, OUTER_SCALES, ALL_SCALES,
    VARIABLES, EDGE_DATASETS, NORMALISATIONS, CALC_OPERATORS,
    COLOURS_BLUE, COLOURS_GREEN, NO_DATA,
    INNER_GEMEENTE_CODES, INNER_GM_NUMS, INNER_PC4_CODES,
    findScale, colForScale, varsForScale, sharedScales, scaleChips, isInnerScale,
  } from '$lib/config';
  import type { SpatialExtent, Normalisation, ActiveEdgeLayer } from '$lib/types';
  import type { FeatureCollection, Feature } from 'geojson';

  // ── UI state ──────────────────────────────────────────────
  type Tab = 'nodal' | 'edges' | 'calc' | 'model';
  let activeTab     = $state<Tab>('nodal');
  let panelOpen     = $state(true);

  // ── Nodal state ───────────────────────────────────────────
  let nodalEnabled  = $state(true);
  let nodalVarKey   = $state('total_population');
  let nodalExtent   = $state<SpatialExtent>('inner');  // inner | outer | both
  let innerScaleKey = $state('buurt');
  let outerScaleKey = $state('gemeente');
  let normalisation    = $state<Normalisation>('none');
  // Chips for selected variable
  const chips = $derived(scaleChips(nodalVarKey));

  // When variable changes: keep scale selections if still valid, else reset to first available
  $effect(() => {
    const { inner, outer } = scaleChips(nodalVarKey);
    if (!inner.some(c => c.key === innerScaleKey)) innerScaleKey = inner[0]?.key ?? '';
    if (!outer.some(c => c.key === outerScaleKey)) outerScaleKey = outer[0]?.key ?? '';
    // Extent fallback: inner picker has all scales so it's rarely empty
    if (nodalExtent === 'inner' && inner.length === 0) nodalExtent = 'outer';
    if (nodalExtent === 'outer' && outer.length === 0) nodalExtent = 'inner';
    if (nodalExtent === 'both' && (inner.length === 0 || outer.length === 0))
      nodalExtent = inner.length > 0 ? 'inner' : 'outer';
    // Both requires at least one outer scale option
    if (nodalExtent === 'both' && outer.length === 0) nodalExtent = 'inner';
  });

  const nodalVar = $derived(VARIABLES.find(v => v.key === nodalVarKey)!);

  // Extent tabs: only show extents that the variable supports
  const extentOptions = $derived(() => {
    const { inner, outer } = chips;
    const opts: { key: SpatialExtent; label: string }[] = [];
    // Study area always available if variable has any scales
    if (inner.length > 0) opts.push({ key: 'inner', label: 'Study area' });
    // Wider region only when outer scales exist
    if (outer.length > 0) opts.push({ key: 'outer', label: 'Wider region' });
    // Both = study area scale + wider region scale simultaneously
    if (inner.length > 0 && outer.length > 0) opts.push({ key: 'both', label: 'Both' });
    return opts;
  });

  // ── Edge / Model panel collapse state ────────────────────
  let edgePanelOpen  = $state(true);
  let modelPanelOpen = $state(true);

  // ── Edge state ────────────────────────────────────────────
  let edgeLayers = $state<ActiveEdgeLayer[]>(
    EDGE_DATASETS.map(d => ({
      datasetKey: d.key,
      period:     d.defaultPeriod,
      visible:    false,
      inkFilter:  [] as string[],   // empty = all income groups
      oplFilter:  [] as string[],   // empty = all education levels
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
  function toggleEdgeInk(key: string, val: string) {
    edgeLayers = edgeLayers.map(l => {
      if (l.datasetKey !== key) return l;
      const cur = l.inkFilter as string[];
      return { ...l, inkFilter: cur.includes(val)
        ? cur.filter(v => v !== val)
        : [...cur, val] };
    });
  }
  function clearEdgeInk(key: string) {
    edgeLayers = edgeLayers.map(l =>
      l.datasetKey === key ? { ...l, inkFilter: [] } : l
    );
  }
  function toggleEdgeOpl(key: string, val: string) {
    edgeLayers = edgeLayers.map(l => {
      if (l.datasetKey !== key) return l;
      const cur = l.oplFilter as string[];
      return { ...l, oplFilter: cur.includes(val)
        ? cur.filter(v => v !== val)
        : [...cur, val] };
    });
  }
  function clearEdgeOpl(key: string) {
    edgeLayers = edgeLayers.map(l =>
      l.datasetKey === key ? { ...l, oplFilter: [] } : l
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
  // Expression is a list of terms: each term is a variable key OR a numeric constant,
  // joined by operators. Supports up to 4 terms.
  // term: { type: 'var', key: string } | { type: 'const', value: string }

  type CalcTerm = { type: 'var'; key: string } | { type: 'const'; value: string };
  type CalcOp   = '+' | '-' | '*' | '/';

  let calcEnabled   = $state(false);
  let calcTerms     = $state<CalcTerm[]>([
    { type: 'var', key: 'nodes_banen_werk' },
    { type: 'var', key: 'nodes_inwoners'   },
  ]);
  let calcOps       = $state<CalcOp[]>([ '/' ]);  // length = calcTerms.length - 1
  let calcScaleKey  = $state('pc4');

  function addCalcTerm() {
    if (calcTerms.length >= 4) return;
    calcTerms = [...calcTerms, { type: 'var', key: 'nodes_banen_werk' }];
    calcOps   = [...calcOps, '+'];
  }
  function removeCalcTerm(i: number) {
    if (calcTerms.length <= 2) return;
    calcTerms = calcTerms.filter((_, idx) => idx !== i);
    calcOps   = calcOps.filter((_, idx) => idx !== (i === 0 ? 0 : i - 1));
  }
  function setCalcTermType(i: number, type: 'var' | 'const') {
    calcTerms = calcTerms.map((t, idx) =>
      idx === i ? (type === 'var'
        ? { type: 'var', key: 'nodes_banen_werk' }
        : { type: 'const', value: '1000' }) : t
    );
  }
  function setCalcTermKey(i: number, key: string) {
    calcTerms = calcTerms.map((t, idx) =>
      idx === i ? { type: 'var', key } : t
    );
  }
  function setCalcTermConst(i: number, value: string) {
    calcTerms = calcTerms.map((t, idx) =>
      idx === i ? { type: 'const', value } : t
    );
  }
  function setCalcOp(i: number, op: CalcOp) {
    calcOps = calcOps.map((o, idx) => idx === i ? op : o);
  }

  // Shared scales: intersection of all variable terms' availableAt
  const calcShared = $derived(() => {
    const varTerms = calcTerms.filter(t => t.type === 'var') as { type: 'var'; key: string }[];
    if (varTerms.length === 0) return ALL_SCALES.map(s => s.key);
    let shared = VARIABLES.find(v => v.key === varTerms[0].key)?.availableAt ?? [];
    for (const t of varTerms.slice(1)) {
      const avail = VARIABLES.find(v => v.key === t.key)?.availableAt ?? [];
      shared = shared.filter(s => avail.includes(s));
    }
    return shared;
  });

  const calcLabel = $derived(() => {
    const ops: Record<string, string> = { '+': '+', '-': '−', '*': '×', '/': '÷' };
    return calcTerms.map((t, i) => {
      const termStr = t.type === 'const'
        ? t.value
        : (VARIABLES.find(v => v.key === t.key)?.label ?? t.key);
      return i < calcOps.length ? `${termStr} ${ops[calcOps[i]]}` : termStr;
    }).join(' ');
  });

  $effect(() => {
    const shared = calcShared();
    if (!shared.includes(calcScaleKey) && shared.length > 0)
      calcScaleKey = shared[0];
  });

  // ── Model state ──────────────────────────────────────────
  // PC4-level gravity model: log(T_ij) = a + Σ b_k·log(X_k) + c·log(d_ij)
  // Variables available at PC4 scale only (from nodes_summary_pc4 + CBS stats)

  interface ModelVar {
    key:   string;
    label: string;
    role:  'origin' | 'destination' | 'covariate';
    useLog:     boolean;
    useDensity: boolean;  // divide by land area
  }

  interface ModelResults {
    coefficients: { name: string; coef: number; se: number; t: number }[];
    r2:           number;
    r2adj:        number;
    n:            number;
    vif:          { name: string; vif: number }[];
    residuals:    { origin_id: number; dest_id: number;
                    observed: number; predicted: number; residual: number }[];
  }

  // PC4 variables available for modelling
  // Model variables — node summary variables always available
  // CBS variables populated dynamically from parquet schema when model tab opens
  const MODEL_NODE_VARS = [
    { key: 'nodes_banen_werk', label: 'Jobs (work location)' },
    { key: 'nodes_banen_woon', label: 'Employed residents' },
    { key: 'nodes_inwoners',   label: 'Population (CBS microdata)' },
  ];

  // CBS PC4 covariate variables — loaded dynamically from parquet
  let cbsCovariateVars = $state<{ key: string; label: string }[]>([]);

  const MODEL_VARS_PC4 = $derived([
    ...MODEL_NODE_VARS,
    ...cbsCovariateVars,
  ]);

  // Load CBS variable list when model tab becomes active
  $effect(() => {
    if (activeTab === 'model' && dbReady && cbsCovariateVars.length === 0) {
      queuedQuery(`DESCRIBE SELECT * FROM read_parquet('${dataURL('pc4_zh_2024_stats.parquet')}') LIMIT 0`)
        .then((res: any) => {
          const skip = new Set(['postcode', 'jaar', 'statcode', 'statnaam',
            'gemeentenaam', 'indelingswijziging_wijken_en_buurten',
            'water', 'meest_voorkomende_postcode']);
          const skipPatterns = ['_aantal_binnen_', '_afstand_in_km',
            'dichtstbijzijnde_', 'hotel_', 'bioscoop_', 'cafe_', 'restaurant_',
            'attractie_', 'museum_', 'theater_', 'podium_', 'warenhuis_'];
          cbsCovariateVars = res.toArray()
            .map((r: any) => r.toJSON().column_name as string)
            .filter((k: string) => !skip.has(k) && !k.startsWith('id')
              && !skipPatterns.some((p: string) => k.includes(p)))
            .map((k: string) => ({ key: k, label: k }));
        })
        .catch(() => {});
    }
  });

  let modelEnabled        = $state(false);
  let modelType           = $state<'gravity' | 'nodal'>('gravity');

  // ── Nodal model state ─────────────────────────────────────────────────────
  let nodalModelOutcome   = $state('flow_outflow');    // Y variable
  let nodalModelPredictors = $state<string[]>([]);     // X variables (CBS columns)
  let nodalModelResults   = $state<{
    r2: number; r2adj: number; n: number;
    coefficients: { name: string; coef: number; se: number; t: number }[];
    residuals: { id: number | string; observed: number; predicted: number; residual: number }[];
  } | null>(null);
  let nodalModelRunning   = $state(false);
  let nodalModelError     = $state<string | null>(null);

  const NODAL_OUTCOMES = [
    { key: 'flow_outflow',          label: 'Commuter outflow' },
    { key: 'flow_inflow',           label: 'Commuter inflow' },
    { key: 'flow_internal',         label: 'Internal commuters' },
    { key: 'flow_self_containment', label: 'Self-containment ratio' },
    { key: 'nodes_banen_werk',      label: 'Jobs at work location' },
    { key: 'nodes_banen_woon',      label: 'Employed residents' },
    { key: 'nodes_ratio_banen_inwoners', label: 'Jobs/residents ratio' },
  ];

  async function runNodalModel() {
    if (!dbReady || !conn) return;
    nodalModelRunning = true; nodalModelError = null; nodalModelResults = null;

    try {
      // Load outcome variable
      const outcomeRows = await fetchRows(nodalModelOutcome, 'pc4', 'none');
      if (outcomeRows.length < 10) {
        nodalModelError = 'Too few areas for regression.';
        nodalModelRunning = false; return;
      }

      // Load CBS covariates
      const cbsRes = await queuedQuery(`
        SELECT * FROM read_parquet('${dataURL('pc4_zh_2024_stats.parquet')}')
        UNION ALL
        SELECT * FROM read_parquet('${dataURL('pc4_supplementary_stats.parquet')}')
      `);
      const cbsMap = new Map<number, any>(
        cbsRes.toArray().map((r: any) => { const j = r.toJSON(); return [Number(j.postcode), j]; })
      );

      // Build design matrix
      const predictorKeys = nodalModelPredictors.slice(0, 5); // max 5 predictors
      const Xrows: number[][] = [];
      const Yrows: number[]   = [];
      const ids: (number | string)[] = [];

      for (const row of outcomeRows) {
        const y = Number(row.value);
        if (!isFinite(y) || y <= -99990) continue;
        const cbs = cbsMap.get(Number(row.id));
        if (!cbs) continue;

        const xs = predictorKeys.map(k => {
          const v = Number(cbs[k]);
          return (isFinite(v) && v > -99990) ? Math.log(Math.max(v, 0.01)) : NaN;
        });
        if (xs.some(x => !isFinite(x))) continue;

        Xrows.push([1, ...xs]);
        Yrows.push(Math.log(Math.max(y, 0.01)));
        ids.push(row.id);
      }

      if (Xrows.length < predictorKeys.length + 3) {
        nodalModelError = `Too few valid areas (${Xrows.length}).`;
        nodalModelRunning = false; return;
      }

      const fit = olsFit(Xrows, Yrows);
      if (!fit) { nodalModelError = 'Singular matrix — check for collinear predictors.'; nodalModelRunning = false; return; }

      const varNames = ['intercept', ...predictorKeys];
      const residuals = ids.map((id, i) => ({
        id,
        observed:  Math.exp(Yrows[i]),
        predicted: Math.exp(fit.fitted[i]),
        residual:  Yrows[i] - fit.fitted[i],
      }));

      nodalModelResults = {
        r2: fit.r2, r2adj: fit.r2adj, n: Xrows.length,
        coefficients: varNames.map((name, i) => ({
          name, coef: fit.coef[i], se: fit.se[i], t: fit.coef[i] / fit.se[i],
        })),
        residuals,
      };

      // Draw nodal residuals as choropleth
      const scale = findScale('pc4');
      if (!scale || !map) return;
      const maxAbs = Math.max(...residuals.map(r => Math.abs(r.residual)), 0.01);
      try { map.removeFeatureState({ source: 'pc4-source' }); } catch (_) {}
      for (const r of residuals) {
        // Use feature state 'resid' for colour, cls for standard choropleth
        map.setFeatureState(
          { source: 'pc4-source', id: r.id },
          { cls: Math.round(2 + (r.residual / maxAbs) * 2) }  // 0-4 mapped to colour
        );
      }
      setColours('pc4', ['#e63946','#f4a261','#dddddd','#74c476','#2a9d8f'], 0.72);
      setLayerVis('pc4-fill',    'visible');
      setLayerVis('pc4-outline', 'visible');

    } catch(e) {
      nodalModelError = `Model error: ${e}`;
      console.error(e);
    }
    nodalModelRunning = false;
  }
  let modelPeriod         = $state<string>('20122017');
  let modelOriginKey      = $state('nodes_banen_woon');
  let modelDestKey        = $state('nodes_banen_werk');
  let modelCovariates     = $state<ModelVar[]>([]);
  let modelLogOrigin      = $state(true);
  let modelLogDest        = $state(true);
  let modelLogDistance    = $state(true);
  let modelDecayExp       = $state(2.0);  // distance decay exponent
  let modelInternals      = $state<'exclude' | 'include' | 'only'>('exclude');
  let modelMinFlow        = $state(10);
  let modelFlowDir        = $state<'out' | 'in' | 'both'>('both');  // outflow, inflow, or all
  let modelResidualView   = $state<'lines' | 'choropleth'>('lines');
  let modelRunning        = $state(false);
  let modelResults        = $state<ModelResults | null>(null);
  let modelError          = $state<string | null>(null);

  // Income/education filter for model flows
  let modelInkFilter      = $state<string[]>([]);  // empty = all
  let modelOplFilter      = $state<string[]>([]);  // empty = all

  function toggleModelInk(val: string) {
    modelInkFilter = modelInkFilter.includes(val)
      ? modelInkFilter.filter(v => v !== val)
      : [...modelInkFilter, val];
  }
  function toggleModelOpl(val: string) {
    modelOplFilter = modelOplFilter.includes(val)
      ? modelOplFilter.filter(v => v !== val)
      : [...modelOplFilter, val];
  }

  function addModelCovariate() {
    if (modelCovariates.length >= 3) return;
    modelCovariates = [...modelCovariates, {
      key: 'nodes_inwoners', label: 'Population (CBS microdata)',
      role: 'covariate', useLog: true, useDensity: false,
    }];
  }
  function removeModelCovariate(i: number) {
    modelCovariates = modelCovariates.filter((_, idx) => idx !== i);
  }
  function updateModelCovariate(i: number, patch: Partial<ModelVar>) {
    modelCovariates = modelCovariates.map((c, idx) =>
      idx === i ? { ...c, ...patch } : c
    );
  }

  // ── OLS solver (normal equations: β = (X'X)^-1 X'y) ─────────────────────────
  // Simple implementation for small matrices (< 10 variables)
  function matMul(A: number[][], B: number[][]): number[][] {
    const m = A.length, n = B[0].length, k = B.length;
    return Array.from({length: m}, (_, i) =>
      Array.from({length: n}, (_, j) =>
        A[i].reduce((s, _, l) => s + A[i][l] * B[l][j], 0)
      )
    );
  }

  function matTranspose(A: number[][]): number[][] {
    return A[0].map((_, j) => A.map(row => row[j]));
  }

  // Gauss-Jordan matrix inverse
  function matInverse(A: number[][]): number[][] | null {
    const n = A.length;
    const M = A.map((row, i) => [...row, ...Array.from({length: n}, (_, j) => i === j ? 1 : 0)]);
    for (let col = 0; col < n; col++) {
      // Find pivot
      let maxRow = col;
      for (let row = col+1; row < n; row++)
        if (Math.abs(M[row][col]) > Math.abs(M[maxRow][col])) maxRow = row;
      [M[col], M[maxRow]] = [M[maxRow], M[col]];
      if (Math.abs(M[col][col]) < 1e-12) return null;  // singular
      const pivot = M[col][col];
      for (let j = 0; j < 2*n; j++) M[col][j] /= pivot;
      for (let row = 0; row < n; row++) {
        if (row === col) continue;
        const factor = M[row][col];
        for (let j = 0; j < 2*n; j++) M[row][j] -= factor * M[col][j];
      }
    }
    return M.map(row => row.slice(n));
  }

  function olsFit(X: number[][], y: number[]): {
    coef: number[]; se: number[]; r2: number; r2adj: number; fitted: number[];
  } | null {
    const n = y.length, p = X[0].length;
    const Xt   = matTranspose(X);
    const XtX  = matMul(Xt, X);
    const XtXi = matInverse(XtX);
    if (!XtXi) return null;
    const Xty  = matMul(Xt, y.map(v => [v]));
    const coef = matMul(XtXi, Xty).map(r => r[0]);

    const fitted = X.map(row => row.reduce((s, v, i) => s + v * coef[i], 0));
    const yMean  = y.reduce((a, b) => a + b, 0) / n;
    const ssTot  = y.reduce((s, v) => s + (v - yMean) ** 2, 0);
    const ssRes  = y.reduce((s, v, i) => s + (v - fitted[i]) ** 2, 0);
    const r2     = 1 - ssRes / ssTot;
    const r2adj  = 1 - (1 - r2) * (n - 1) / (n - p);
    const mse    = ssRes / (n - p);
    const se     = XtXi.map((row, i) => Math.sqrt(Math.max(0, mse * row[i])));

    return { coef, se, r2, r2adj, fitted };
  }

  function computeVIF(X: number[][]): number[] {
    // VIF for each predictor (excluding intercept at col 0)
    const p = X[0].length;
    return Array.from({length: p - 1}, (_, k) => {
      const j = k + 1;  // predictor column index (skip intercept)
      const y = X.map(row => row[j]);
      const Xr = X.map(row => row.filter((_, i) => i !== j));
      const res = olsFit(Xr, y);
      if (!res || res.r2 >= 1) return 99;
      return 1 / (1 - res.r2);
    });
  }

  // ── Run gravity model ─────────────────────────────────────────────────────────
  async function runGravityModel() {
    if (!dbReady || !conn) return;
    modelRunning = true; modelError = null; modelResults = null;

    try {
      // Step 1: Load centroids
      const centroidsRaw = await fetch(dataURL('pc4_centroids.json')).then(r => r.json());
      const centMap = new Map<number, {lng: number; lat: number}>(
        centroidsRaw.map((c: any) => [Number(c.id), {lng: c.lng, lat: c.lat}])
      );

      // Step 2: Load flows for selected period with optional income/education filter
      const inkWhere = modelInkFilter.length > 0
        ? `AND inks IN (${modelInkFilter.join(',')})` : '';
      const oplWhere = modelOplFilter.length > 0
        ? `AND opl IN (${modelOplFilter.join(',')})` : '';

      // Use breakdown parquet if filters active, else marginal
      const flowFile = (modelInkFilter.length > 0 || modelOplFilter.length > 0)
        ? 'edges_woonwerk_ink_opl_pc4.parquet'  // future breakdown parquet
        : 'edges_woonwerk_pc4.parquet';

      const internalWhere = modelInternals === 'exclude'
        ? 'AND origin_id != destination_id'
        : modelInternals === 'only'
          ? 'AND origin_id = destination_id'
          : '';

      const flowRes = await queuedQuery(`
        SELECT origin_id, destination_id, SUM(flow_value) AS flow_value
        FROM read_parquet('${dataURL(flowFile)}')
        WHERE periode = '${modelPeriod}'
        AND flow_value >= ${modelMinFlow}
        ${internalWhere}
        ${inkWhere} ${oplWhere}
        GROUP BY origin_id, destination_id
      `);
      const flows = flowRes.toArray().map((r: any) => r.toJSON());

      if (flows.length < 10) {
        modelError = 'Too few flows to fit model. Check filters or minimum count.';
        modelRunning = false; return;
      }

      // Step 3: Load PC4 node variables (inner area) and CBS stats (middle boundary)
      // Flows involving PC4s outside the middle boundary (~7.6% of flows, max value 35)
      // are skipped — these are low-value long-distance commutes not meaningful for model
      const nodeRes = await queuedQuery(`
        SELECT postcode, total_banen_werk, total_banen_woon, total_inwoners,
               ratio_banen_inwoners, ratio_werkenden_inwoners
        FROM read_parquet('${dataURL('nodes_summary_pc4.parquet')}')
        WHERE jaar = 2017
      `);
      const nodes = new Map<number, any>(
        nodeRes.toArray().map((r: any) => { const j = r.toJSON(); return [j.postcode, j]; })
      );

      // Step 4: Load CBS PC4 stats — middle boundary + supplementary for outer PCs
      // pc4_zh_2024_stats covers the middle boundary (~600 PC4s for map display)
      // pc4_supplementary_stats covers the remaining ~961 PCs that appear in edge data
      const cbsRes = await queuedQuery(`
        SELECT * FROM read_parquet('${dataURL('pc4_zh_2024_stats.parquet')}')
        UNION ALL
        SELECT * FROM read_parquet('${dataURL('pc4_supplementary_stats.parquet')}')
      `);
      const cbsNodes = new Map<number, any>(
        cbsRes.toArray().map((r: any) => { const j = r.toJSON(); return [j.postcode, j]; })
      );
      // Populate cbsVarKeys for dynamic covariate selector
      const cbsKeys = cbsRes.schema.fields.map((f: any) => f.name as string)
        .filter((k: string) => !['postcode','jaar'].includes(k));

      // Step 5: Build design matrix
      // getVarValue: looks up node summary first, then CBS stats column directly.
      // This allows ANY CBS column to be used as a covariate without pre-mapping.
      const getVarValue = (pc4: number, varKey: string): number => {
        const n = nodes.get(pc4);
        const c = cbsNodes.get(pc4);
        // Named node summary variables
        const named: Record<string, number> = {
          nodes_banen_werk: n?.total_banen_werk ?? NaN,
          nodes_banen_woon: n?.total_banen_woon ?? NaN,
          nodes_inwoners:   n?.total_inwoners   ?? NaN,
        };
        if (varKey in named) return named[varKey];
        // CBS column lookup — covers middle boundary PC4s
        // PC4s outside middle boundary (~963 codes, max flow 35) return NaN and are skipped
        const v = c?.[varKey];
        return (v != null && Number(v) > -99990) ? Number(v) : NaN;
      };

      const haversine = (o: number, d: number): number => {
        const oc = centMap.get(o), dc = centMap.get(d);
        if (!oc || !dc) return NaN;
        const R = 6371, dLat = (dc.lat - oc.lat) * Math.PI/180;
        const dLng = (dc.lng - oc.lng) * Math.PI/180;
        const a = Math.sin(dLat/2)**2 +
                  Math.cos(oc.lat * Math.PI/180) * Math.cos(dc.lat * Math.PI/180) *
                  Math.sin(dLng/2)**2;
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
      };

      // Build rows: [y, 1, x_origin, x_dest, x_dist, ...covariates]
      const varNames = ['intercept', 
        `log(${modelOriginKey})`, `log(${modelDestKey})`, 
        `log(dist^${modelDecayExp})`,
        ...modelCovariates.map(c => `${c.useLog ? 'log' : ''}(${c.label}${c.useDensity ? '/ha' : ''})`),
      ];

      const Xrows: number[][] = [];
      const Yrows: number[] = [];
      const rawFlows: typeof flows = [];

      for (const flow of flows) {
        const oi = Number(flow.origin_id), di = Number(flow.destination_id);
        const oVal = getVarValue(oi, modelOriginKey);
        const dVal = getVarValue(di, modelDestKey);
        const dist = haversine(oi, di);

        if (!isFinite(oVal) || !isFinite(dVal) || !isFinite(dist) || dist <= 0) continue;
        if (oVal <= 0 || dVal <= 0) continue;

        const y = Math.log(Math.max(flow.flow_value, 0.5));

        const xOrigin = modelLogOrigin ? Math.log(oVal) : oVal;
        const xDest   = modelLogDest   ? Math.log(dVal) : dVal;
        const xDist   = modelLogDistance
          ? modelDecayExp * Math.log(dist)
          : Math.pow(dist, modelDecayExp);

        const covVals: number[] = [];
        let skip = false;
        for (const cov of modelCovariates) {
          let v = getVarValue(oi, cov.key);  // covariates from origin by default
          if (cov.useDensity) {
            const area = getVarValue(oi, 'land_area_ha');
            v = area > 0 ? v / area : NaN;
          }
          if (cov.useLog) v = v > 0 ? Math.log(v) : NaN;
          if (!isFinite(v)) { skip = true; break; }
          covVals.push(v);
        }
        if (skip) continue;

        Xrows.push([1, xOrigin, xDest, xDist, ...covVals]);
        Yrows.push(y);
        rawFlows.push(flow);
      }

      if (Xrows.length < varNames.length + 5) {
        modelError = `Too few valid rows (${Xrows.length}) for ${varNames.length} variables.`;
        modelRunning = false; return;
      }

      // Step 6: OLS
      const fit = olsFit(Xrows, Yrows);
      if (!fit) { modelError = 'Matrix singular — check for collinear variables.'; modelRunning = false; return; }

      // Step 7: VIF
      const vifVals = computeVIF(Xrows);

      // Step 8: Residuals in LOG space — balanced +/- around zero.
      // residual = log(observed) - fitted (log space ensures ~50% positive, ~50% negative)
      // positive = more commuting than model expects
      // negative = less commuting than model expects
      const residuals = rawFlows.map((flow: any, i: number) => ({
        origin_id:  Number(flow.origin_id),
        dest_id:    Number(flow.destination_id),
        observed:   Number(flow.flow_value),
        predicted:  Math.exp(fit.fitted[i]),
        residual:   Yrows[i] - fit.fitted[i],
      }));

      modelResults = {
        coefficients: varNames.map((name, i) => ({
          name,
          coef: fit.coef[i],
          se:   fit.se[i],
          t:    fit.coef[i] / fit.se[i],
        })),
        r2:    fit.r2,
        r2adj: fit.r2adj,
        n:     Xrows.length,
        vif:   varNames.slice(1).map((name, i) => ({ name, vif: vifVals[i] ?? NaN })),
        residuals,
      };

      // Step 9: Draw residual flow lines
      await drawResiduals(residuals);

    } catch(e) {
      modelError = `Model error: ${e}`;
      console.error(e);
    }
    modelRunning = false;
  }

  // ── Draw residual flow lines on map ───────────────────────────────────────────
  async function drawResiduals(
    residuals: ModelResults['residuals']
  ) {
    if (!map) return;
    const centroidsRaw = await fetch(dataURL('pc4_centroids.json')).then(r => r.json());
    const centMap = new Map<number, [number, number]>(
      centroidsRaw.map((c: any) => [Number(c.id), [c.lng, c.lat] as [number, number]])
    );

    // For choropleth: aggregate residuals by origin or destination
    const sorted = [...residuals].sort((a, b) =>
      Math.abs(b.residual) - Math.abs(a.residual)
    ).slice(0, 1500);  // top 1500 by magnitude

    const maxAbs = Math.max(...sorted.map(r => Math.abs(r.residual)), 1);

    const features: Feature[] = sorted.flatMap(r => {
      // 'both': draw from origin to dest regardless of direction filter
      // 'out': origin is the focus area (outflow perspective)
      // 'in': destination is the focus area (inflow perspective)
      const o = centMap.get(r.origin_id);
      const d = centMap.get(r.dest_id);
      if (!o || !d) return [];
      return [{
        type: 'Feature' as const,
        properties: {
          residual:  r.residual,
          norm:      r.residual / maxAbs,
          observed:  r.observed,
          predicted: r.predicted,
          origin_id: r.origin_id,
          dest_id:   r.dest_id,
        },
        geometry: { type: 'LineString' as const, coordinates: [o, d] },
      }];
    });

    const geojson: FeatureCollection = { type: 'FeatureCollection', features };
    const sid = 'model-residuals-source';
    const lid = 'model-residuals-layer';

    if (map.getSource(sid)) {
      (map.getSource(sid) as maplibregl.GeoJSONSource).setData(geojson);
    } else {
      map.addSource(sid, { type: 'geojson', data: geojson });
      map.addLayer({
        id: lid, type: 'line', source: sid,
        layout: { 'line-join': 'round', 'line-cap': 'round' },
        paint: {
          // Teal (+) = more commuting than model expects (high-performing / underserved)
          // Red  (-) = less commuting than model expects (overperforming in model)
          // Grey (0) = well-fit by model
          'line-color': [
            'interpolate', ['linear'], ['get', 'norm'],
            -1,   '#e63946',  // strong negative: red
            -0.15,'#f4a261',  // weak negative: orange
             0,   '#dddddd',  // neutral: light grey
             0.15,'#74c476',  // weak positive: light green
             1,   '#2a9d8f',  // strong positive: teal
          ],
          'line-opacity': [
            'interpolate', ['linear'],
            ['max', ['get', 'norm'], ['-', 0, ['get', 'norm']]],
            0, 0.1, 0.2, 0.5, 1, 0.85,
          ],
          'line-width': [
            'interpolate', ['linear'],
            ['max', ['get', 'norm'], ['-', 0, ['get', 'norm']]],
            0, 0.3, 0.5, 1.5, 1, 4,
          ],
        },
      });
    }

    if (map.getLayer(lid)) map.setLayoutProperty(lid, 'visibility', 'visible');
  }

  function clearResiduals() {
    const sid = 'model-residuals-source';
    const lid = 'model-residuals-layer';
    if (map?.getLayer(lid))  map.setLayoutProperty(lid, 'visibility', 'none');
  }

  // ── MapLibre + DuckDB ─────────────────────────────────────
  let map: maplibregl.Map | null = null;
  let mapReady  = $state(false);
  // Popup state
  interface PopupInfo {
    x: number; y: number;  // screen pixels
    title: string;
    rows: { label: string; value: string }[];
  }
  let popup = $state<PopupInfo | null>(null);
  let db: duckdb.AsyncDuckDB | null = null;
  let conn: any = null;
  let dbReady   = $state(false);
  // Non-reactive store for flow ranges (avoids triggering $effect loop)
  const flowRanges = new Map<string, { min: number; max: number }>();

  // ── Serial query queue ────────────────────────────────────────────────────────
  // DuckDB-WASM throws DataCloneError when two queries run concurrently on the
  // same connection. All conn.query calls go through this queue to serialise them.
  let queryQueue: Promise<any> = Promise.resolve();

  function queuedQuery(sql: string): Promise<any> {
    const p = queryQueue.then(() => conn.query(sql));
    queryQueue = p.catch(() => {});  // keep queue alive even if query fails
    return p;  // return the actual promise so callers get the result
  }

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
    // -1 = no data (shown as grey). Only for null, NaN, or CBS sentinel values.
    // 0 and small positives are valid and should display in class 0 (lightest colour).
    if (value == null || !isFinite(value) || value <= -99990) return -1;
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

    // Employment nodes — routes by scale to correct summary parquet
    if (src === 'nodes_emp') {
      // Strip 'nodes_' prefix to get actual column name
      const col = varKey.replace(/^nodes_/, '');
      if (scaleKey === 'pc4') {
        const url = dataURL('nodes_summary_pc4.parquet');
        const res = await queuedQuery(
          `SELECT postcode AS id, "${col}"::DOUBLE AS value FROM read_parquet('${url}') WHERE jaar = 2017`
        );
        return res.toArray().map((r: any) => r.toJSON());
      } else {
        const url = dataURL('nodes_summary_gem.parquet');
        const res = await queuedQuery(
          `SELECT gemeentecode AS id, "${col}"::DOUBLE AS value FROM read_parquet('${url}') WHERE jaar = 2017`
        );
        return res.toArray().map((r: any) => r.toJSON());
      }
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
        const res = await queuedQuery(`
          SELECT postcode AS id, SUM(n)::DOUBLE AS value
          FROM read_parquet('${url}')
          WHERE jaar = 2017 AND ink_label = '${inkLabel}'
          GROUP BY postcode
        `);
        return res.toArray().map((r: any) => r.toJSON());
      } else {
        const url = dataURL('nodes_demo_inkomen_gem.parquet');
        const res = await queuedQuery(`
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
        const res = await queuedQuery(`
          SELECT postcode AS id, SUM(n)::DOUBLE AS value
          FROM read_parquet('${url}')
          WHERE jaar = 2017 AND opl_label = '${oplLabel}'
          GROUP BY postcode
        `);
        return res.toArray().map((r: any) => r.toJSON());
      } else {
        const url = dataURL('nodes_demo_opleiding_gem.parquet');
        const res = await queuedQuery(`
          SELECT gemeentecode AS id, SUM(n)::DOUBLE AS value
          FROM read_parquet('${url}')
          WHERE jaar = 2017 AND opl_label = '${oplLabel}'
          GROUP BY gemeentecode
        `);
        return res.toArray().map((r: any) => r.toJSON());
      }
    }

    // Flow-derived variables — routes to pc4 or gemeente parquet by scaleKey
    if (src === 'flows') {
      const period = '20122017';
      const isPC4  = scaleKey === 'pc4';
      const flowUrl = dataURL(isPC4 ? 'edges_woonwerk_pc4.parquet' : 'edges_woonwerk_gem.parquet');
      const idCol   = isPC4 ? 'postcode' : 'gemeentecode';
      // Map unified key to pc4/gem specific SQL
      const k = varKey; // e.g. 'flow_outflow'
      const originCol = isPC4 ? 'origin_id' : 'origin_id';
      const destCol   = isPC4 ? 'destination_id' : 'destination_id';
      const idExpr    = (col: string) => isPC4 ? col : col;

      let sql = '';
      if (k === 'flow_outflow') {
        sql = `SELECT ${originCol} AS id, SUM(flow_value)::DOUBLE AS value
               FROM read_parquet('${flowUrl}')
               WHERE periode = '${period}' AND ${originCol} != ${destCol}
               GROUP BY ${originCol}`;
      } else if (k === 'flow_inflow') {
        sql = `SELECT ${destCol} AS id, SUM(flow_value)::DOUBLE AS value
               FROM read_parquet('${flowUrl}')
               WHERE periode = '${period}' AND ${originCol} != ${destCol}
               GROUP BY ${destCol}`;
      } else if (k === 'flow_internal') {
        sql = `SELECT ${originCol} AS id, SUM(flow_value)::DOUBLE AS value
               FROM read_parquet('${flowUrl}')
               WHERE periode = '${period}' AND ${originCol} = ${destCol}
               GROUP BY ${originCol}`;
      } else if (k === 'flow_net') {
        sql = `SELECT id, SUM(flow)::DOUBLE AS value FROM (
                 SELECT ${destCol} AS id,  flow_value AS flow FROM read_parquet('${flowUrl}')
                 WHERE periode = '${period}' AND ${originCol} != ${destCol}
                 UNION ALL
                 SELECT ${originCol} AS id, -flow_value AS flow FROM read_parquet('${flowUrl}')
                 WHERE periode = '${period}' AND ${originCol} != ${destCol}
               ) GROUP BY id`;
      } else if (k === 'flow_n_destinations') {
        sql = `SELECT ${originCol} AS id, COUNT(DISTINCT ${destCol})::DOUBLE AS value
               FROM read_parquet('${flowUrl}')
               WHERE periode = '${period}' AND ${originCol} != ${destCol}
               GROUP BY ${originCol}`;
      } else if (k === 'flow_n_origins') {
        sql = `SELECT ${destCol} AS id, COUNT(DISTINCT ${originCol})::DOUBLE AS value
               FROM read_parquet('${flowUrl}')
               WHERE periode = '${period}' AND ${originCol} != ${destCol}
               GROUP BY ${destCol}`;
      } else if (k === 'flow_self_containment') {
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
      }
      if (!sql) return [];
      const res = await queuedQuery(sql);
      return res.toArray().map((r: any) => r.toJSON());
    }

        if (src === 'flow_summary') {
      const url = dataURL('flow_summary.parquet');
      const res = await queuedQuery(
        `SELECT origin_id AS id, "${varKey}"::DOUBLE AS value FROM read_parquet('${url}')`
      );
      return res.toArray().map((r: any) => r.toJSON());
    }

    // CBS stats parquet — resolve actual column name for this scale
    const col    = colForScale(varKey, scaleKey);
    const url    = dataURL(scale.stats);

    // Check column exists before querying
    const desc = await queuedQuery(`DESCRIBE SELECT * FROM read_parquet('${url}') LIMIT 0`);
    const cols  = desc.toArray().map((r: any) => r.toJSON().column_name as string);
    if (!cols.includes(col)) {
      console.warn(`[fetchRows] Column "${col}" (varKey="${varKey}") not found in ${scale.stats}`);
      console.warn(`[fetchRows] Available columns:`, cols.filter((c: string) => c.includes('pct') || c.includes('percentage') || c.includes('aantal')).slice(0, 20));
      return [];
    }

    console.log(`[fetchRows] OK: varKey="${varKey}" → col="${col}" at scale="${scaleKey}"`);

    const expr = normSQL(col, norm);
    const res  = await queuedQuery(
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
    // No removeFeatureState — overwrite directly to avoid grey flash

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
    // No removeFeatureState — overwrite directly

    // Fetch all variable terms in parallel; constants are just numbers
    const termMaps = await Promise.all(
      calcTerms.map(t =>
        t.type === 'const'
          ? Promise.resolve(null)  // null = constant, evaluated per-feature
          : fetchMap(t.key, calcScaleKey)
      )
    );

    // Collect all feature IDs from variable terms
    const allIds = new Set<string | number>();
    for (const m of termMaps) {
      if (m) for (const k of m.keys()) allIds.add(k);
    }

    const rows = [...allIds].map(id => {
      // Evaluate each term
      const termValues = calcTerms.map((t, i) => {
        if (t.type === 'const') return parseFloat(t.value);
        return termMaps[i]?.get(id) ?? NaN;
      });

      // Apply operators left to right
      let value = termValues[0];
      for (let i = 0; i < calcOps.length; i++) {
        const next = termValues[i + 1];
        if (!isFinite(value) || !isFinite(next)) { value = NaN; break; }
        const op = calcOps[i];
        if (op === '/' && next === 0) { value = NaN; break; }
        if (op === '+') value = value + next;
        else if (op === '-') value = value - next;
        else if (op === '*') value = value * next;
        else value = value / next;
      }
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
    inkFilter: string[] = [], oplFilter: string[] = []
  ) {
    if (!map) return;
    const ds = EDGE_DATASETS.find(d => d.key === datasetKey);
    if (!ds) return;

    // Load centroids — key as STRING for gemeente (GM0599), NUMBER for pc4 (3011)
    // Use a universal string key to avoid type mismatch with flow IDs from parquet
    const centroidsFile = ds.scaleKey === 'pc4'
      ? 'pc4_centroids.json'
      : 'gemeente_centroids.json';
    const centroidsRaw = await fetch(dataURL(centroidsFile)).then(r => r.json());
    // Key by String(id) so lookup works regardless of whether parquet returns int or string
    const centroids = new Map<string, [number, number]>(
      centroidsRaw.map((c: any) => [String(c.id), [Number(c.lng), Number(c.lat)] as [number, number]])
    );

    // Determine which parquet to query — breakdown if filters active, marginal otherwise
    const hasFilter = inkFilter.length > 0 || oplFilter.length > 0;
    let flowUrl   = dataURL(ds.flows);
    let filterWhere = '';

    if (hasFilter && ds.hasBreakdown) {
      // Use breakdown parquet — no existence check needed (causes DataCloneError)
      // If file missing, DuckDB will throw and we catch below in runUpdate
      flowUrl = dataURL(
        ds.scaleKey === 'pc4'
          ? 'edges_woonwerk_ink_opl_pc4.parquet'
          : 'edges_woonwerk_ink_opl_gem.parquet'
      );
      if (inkFilter.length > 0) filterWhere += ` AND inks IN (${inkFilter.join(',')})`;
      if (oplFilter.length > 0) filterWhere += ` AND opl IN (${oplFilter.join(',')})`;
    }

    const res = await queuedQuery(`
      SELECT "${ds.idCols.origin}" AS o, "${ds.idCols.destination}" AS d,
             SUM(flow_value) AS flow_value
      FROM read_parquet('${flowUrl}')
      WHERE "${ds.idCols.period}" = '${period}'
      AND flow_value >= 10
      ${filterWhere}
      GROUP BY "${ds.idCols.origin}", "${ds.idCols.destination}"
      ORDER BY flow_value DESC LIMIT 2000
    `);
    const flows = res.toArray().map((r: any) => r.toJSON());
    const flowValues = flows.map((f: any) => Number(f.flow_value)).filter((v: number) => v > 0);
    const maxFlow = Math.max(...flowValues, 1);
    const minFlow = flowValues.length > 0 ? Math.min(...flowValues) : 0;
    flowRanges.set(datasetKey, { min: minFlow, max: maxFlow });

    const features: Feature[] = flows.flatMap((flow: any) => {
      const origin = centroids.get(String(flow.o));
      const dest   = centroids.get(String(flow.d));
      if (!origin || !dest) return [];
      return [{
        type: 'Feature' as const,
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
      // Arrow at midpoint showing direction (destination end)
      const aid = `flows-${datasetKey}-arrows`;
      if (!map.getLayer(aid)) {
        map.addLayer({
          id: aid, type: 'symbol', source: sid,
          layout: {
            'symbol-placement': 'line',
            'symbol-spacing': 200,
            'icon-image': 'arrow-icon',
            'icon-size': 0.6,
            'icon-allow-overlap': true,
            'icon-rotate': 90,  // point arrow along line direction
          },
          paint: {
            'icon-color': ds.colour,
            'icon-opacity': ['interpolate', ['linear'], ['get', 'flow_norm'], 0, 0.2, 1, 0.8],
          },
        });
      }
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

  function scheduleUpdate(delay = 120) {
    if (updateTimer) clearTimeout(updateTimer);
    updateTimer = setTimeout(async () => {
      if (updateRunning) return;
      updateRunning = true;
      try { await runUpdate(); }
      finally { updateRunning = false; }
    }, delay);
  }

  // Slow update for filter changes — waits 400ms so rapid clicks don't spam queries
  function scheduleFilterUpdate() { scheduleUpdate(400); }

  // ── Clip outer-type scale to inner boundary when used as study area ────────────
  // Called when innerScaleKey is an outer scale (gemeente/wijk/pc4).
  // Applies a MapLibre filter so only inner-area features show.
  function clipToInnerBoundary(scaleKey: string) {
    const fillId    = `${scaleKey}-fill`;
    const outlineId = `${scaleKey}-outline`;
    if (!map?.getLayer(fillId)) return;

    if (scaleKey === 'gemeente') {
      const f = ['in', ['get', 'gemeentecode'], ['literal', INNER_GEMEENTE_CODES]];
      map.setFilter(fillId,    f as any);
      map.setFilter(outlineId, f as any);
    } else if (scaleKey === 'wijk') {
      const f: any = ['any', ...INNER_GM_NUMS.map((gm: string) =>
        ['==', ['slice', ['get', 'wijkcode'], 2, 6], gm]
      )];
      map.setFilter(fillId,    f);
      map.setFilter(outlineId, f);
    } else if (scaleKey === 'pc4') {
      // PC4: use the study-area postcode list from config
      map.setFilter(fillId,    ['in', ['get', 'postcode'], ['literal', INNER_PC4_CODES]] as any);
      map.setFilter(outlineId, ['in', ['get', 'postcode'], ['literal', INNER_PC4_CODES]] as any);
    }
  }

  function clearBoundaryClip(scaleKey: string) {
    const fillId    = `${scaleKey}-fill`;
    const outlineId = `${scaleKey}-outline`;
    if (map?.getLayer(fillId))    map.setFilter(fillId,    null);
    if (map?.getLayer(outlineId)) map.setFilter(outlineId, null);
  }

  // Clip outer layer to EXCLUDE the inner boundary area (opposite of clipToInnerBoundary)
  // Used in both mode so outer data doesn't bleed into the study area
  function clipOuterToExcludeInner(scaleKey: string) {
    const fillId    = `${scaleKey}-fill`;
    const outlineId = `${scaleKey}-outline`;
    if (!map?.getLayer(fillId)) return;
    if (scaleKey === 'gemeente') {
      const f = ['!', ['in', ['get', 'gemeentecode'], ['literal', INNER_GEMEENTE_CODES]]];
      map.setFilter(fillId,    f as any);
      map.setFilter(outlineId, f as any);
    } else if (scaleKey === 'wijk') {
      const f: any = ['!', ['any', ...INNER_GM_NUMS.map((gm: string) =>
        ['==', ['slice', ['get', 'wijkcode'], 2, 6], gm]
      )]];
      map.setFilter(fillId,    f);
      map.setFilter(outlineId, f);
    } else if (scaleKey === 'pc4') {
      const f = ['!', ['in', ['get', 'postcode'], ['literal', INNER_PC4_CODES]]];
      map.setFilter(fillId,    f as any);
      map.setFilter(outlineId, f as any);
    }
  }

  // Track which scales were active in the previous render
  // so we can hide only scales that are no longer needed
  let prevInnerKey = '';
  let prevOuterKey = '';

  async function runUpdate() {
    if (!mapReady || !dbReady) return;
    loading = true; error = null;

    try {
      const showInner = nodalEnabled && (nodalExtent === 'inner' || nodalExtent === 'both');
      const showOuter = nodalEnabled && (nodalExtent === 'outer' || nodalExtent === 'both');

      // Hide scales that are no longer active (but NOT the ones about to be shown)
      // This avoids hiding-then-showing the same layer causing a flash
      const activeKeys = new Set<string>();
      if (showInner && innerScaleKey) activeKeys.add(innerScaleKey);
      if (showOuter && outerScaleKey) activeKeys.add(outerScaleKey);
      if (calcEnabled) activeKeys.add(calcScaleKey);

      for (const s of ALL_SCALES) {
        if (!activeKeys.has(s.key)) {
          setLayerVis(`${s.key}-fill`,    'none');
          setLayerVis(`${s.key}-outline`, 'none');
        }
      }
      // Hide mask — will re-enable if both mode
      setLayerVis('inner-mask', 'none');

      if (nodalEnabled) {
        const innerIsOuter = !isInnerScale(innerScaleKey);
        const effectiveOuterKey = (showInner && showOuter && innerScaleKey === outerScaleKey)
          ? '' : outerScaleKey;

        // Clear all stale clips from previous render
        for (const s of ALL_SCALES) clearBoundaryClip(s.key);

        if (showInner && showOuter && innerScaleKey && effectiveOuterKey) {
          // BOTH mode: unified breaks across both scales
          const [innerRows, outerRows] = await Promise.all([
            fetchRows(nodalVarKey, innerScaleKey, normalisation),
            fetchRows(nodalVarKey, effectiveOuterKey, normalisation),
          ]);
          const allValues = [...innerRows, ...outerRows]
            .map(r => Number(r.value)).filter(v => isFinite(v) && v > -99990);
          const unifiedBreaks = quantileBreaks(allValues);
          nodalBreaks = unifiedBreaks;

          for (const row of innerRows) {
            map!.setFeatureState(
              { source: `${innerScaleKey}-source`, id: row.id },
              { cls: classify(Number(row.value), unifiedBreaks) }
            );
          }
          for (const row of outerRows) {
            map!.setFeatureState(
              { source: `${effectiveOuterKey}-source`, id: row.id },
              { cls: classify(Number(row.value), unifiedBreaks) }
            );
          }

          // Same opacity for both — consistent colours for comparison
          setColours(innerScaleKey,     COLOURS_BLUE, 0.72);
          setColours(effectiveOuterKey, COLOURS_BLUE, 0.72);

          // Outer: clip to EXCLUDE inner area so it doesn't bleed in
          clipOuterToExcludeInner(effectiveOuterKey);
          setLayerVis(`${effectiveOuterKey}-fill`,    'visible');
          setLayerVis(`${effectiveOuterKey}-outline`, 'visible');

          // Inner: clip to inner boundary if outer-type scale, otherwise show all
          if (innerIsOuter) clipToInnerBoundary(innerScaleKey);
          setLayerVis(`${innerScaleKey}-fill`,    'visible');
          setLayerVis(`${innerScaleKey}-outline`, 'visible');

          // No mask — clips handle visual separation
          setLayerVis('inner-mask', 'none');

        } else {
          // Single extent
          if (showInner && innerScaleKey) {
            const rows = await fetchRows(nodalVarKey, innerScaleKey, normalisation);
            const values = rows.map(r => Number(r.value)).filter(v => isFinite(v) && v > -99990);
            const breaks = quantileBreaks(values);
            nodalBreaks = breaks;
            for (const row of rows) {
              map!.setFeatureState(
                { source: `${innerScaleKey}-source`, id: row.id },
                { cls: classify(Number(row.value), breaks) }
              );
            }
            setColours(innerScaleKey, COLOURS_BLUE, 0.72);
            setLayerVis(`${innerScaleKey}-fill`,    'visible');
            setLayerVis(`${innerScaleKey}-outline`, 'visible');
            if (innerIsOuter) clipToInnerBoundary(innerScaleKey);
          }
          if (showOuter && outerScaleKey) {
            const rows = await fetchRows(nodalVarKey, outerScaleKey, normalisation);
            const values = rows.map(r => Number(r.value)).filter(v => isFinite(v) && v > -99990);
            const breaks = quantileBreaks(values);
            nodalBreaks = breaks;
            for (const row of rows) {
              map!.setFeatureState(
                { source: `${outerScaleKey}-source`, id: row.id },
                { cls: classify(Number(row.value), breaks) }
              );
            }
            setColours(outerScaleKey, COLOURS_BLUE, 0.72);
            setLayerVis(`${outerScaleKey}-fill`,    'visible');
            setLayerVis(`${outerScaleKey}-outline`, 'visible');
          }
        }
      }
      if (calcEnabled && calcShared().length > 0) {
        setColours(calcScaleKey, COLOURS_GREEN, 0.72);
        setLayerVis(`${calcScaleKey}-fill`,    'visible');
        setLayerVis(`${calcScaleKey}-outline`, 'visible');
        calcBreaks = await applyCalc();
      }

      // Edge layers
      for (const layer of edgeLayers) {
        const lid = `flows-${layer.datasetKey}-layer`;
        const aid = `flows-${layer.datasetKey}-arrows`;
        if (layer.visible) {
          await loadEdgeLayer(layer.datasetKey, layer.period,
                              layer.inkFilter as string[], layer.oplFilter as string[]);
          setLayerVis(lid, 'visible');
          setLayerVis(aid, 'visible');
        } else if (map?.getLayer(lid)) {
          setLayerVis(lid, 'none');
          setLayerVis(aid, 'none');
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
      calcEnabled, JSON.stringify(calcTerms), JSON.stringify(calcOps), calcScaleKey,
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

      // Donut polygon: middle boundary minus inner boundary.
      // Used to clip outer fill layers so they render ONLY outside the inner area.
      // This replaces the white mask approach — no opacity interaction with inner layers.
      map!.addSource('outer-donut-source', {
        type: 'geojson',
        data: new URL('/data/outer_donut.geojson', window.location.origin).href,
      });

      // Outer fill layers — clipped to donut polygon in "both" mode
      // In single "outer" mode they render across the full outer boundary.
      // The donut clip layer below handles masking in "both" mode.
      for (const scale of OUTER_SCALES) {
        map!.addLayer({
          id: `${scale.key}-fill`, type: 'fill', source: `${scale.key}-source`,
          layout: { visibility: 'none' },
          paint: { 'fill-color': colourExpr(COLOURS_BLUE), 'fill-opacity': 0.72 },
        });
        map!.addLayer({
          id: `${scale.key}-outline`, type: 'line', source: `${scale.key}-source`,
          layout: { visibility: 'none' },
          paint: { 'line-color': '#ffffff', 'line-width': 0.5 },
        });
      }

      // Donut clip fill: white fill covering the inner area, rendered above outer layers.
      // Replaces the old inner-mask. Only shown in "both" mode.
      // Inner layers render above this at full opacity — no blending against white
      // because inner layers are fully opaque polygons with no transparency.
      map!.addLayer({
        id: 'inner-mask', type: 'fill', source: 'boundary-source',
        layout: { visibility: 'none' },
        paint: { 'fill-color': '#ffffff', 'fill-opacity': 0 },  // disabled — clip approach used instead
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

      // Arrow icon for flow direction — simple triangle pointing right
      const arrowSize = 16;
      const arrowCanvas = document.createElement('canvas');
      arrowCanvas.width = arrowSize; arrowCanvas.height = arrowSize;
      const arrowCtx = arrowCanvas.getContext('2d')!;
      arrowCtx.fillStyle = '#ffffff';
      arrowCtx.beginPath();
      arrowCtx.moveTo(arrowSize, arrowSize / 2);   // tip (right)
      arrowCtx.lineTo(0, 0);                         // top-left
      arrowCtx.lineTo(0, arrowSize);                 // bottom-left
      arrowCtx.closePath();
      arrowCtx.fill();
      const arrowData = arrowCtx.getImageData(0, 0, arrowSize, arrowSize);
      map!.addImage('arrow-icon', arrowData, { sdf: true });

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
              'icon-opacity': 0.72,
            },
          });
        } else {
          map!.addLayer({
            id: `${scale.key}-fill`, type: 'fill', source: `${scale.key}-source`,
            layout: { visibility: 'none' },
            paint: { 'fill-color': colourExpr(COLOURS_BLUE), 'fill-opacity': 0.72 },
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

      // ── Click handlers ────────────────────────────────────────────────────
      // Choropleth polygon click — show area info
      const choroplethLayers = ALL_SCALES.map(s => `${s.key}-fill`);
      map!.on('click', (e) => {
        const layers = choroplethLayers.filter(id => map!.getLayer(id) &&
          map!.getLayoutProperty(id, 'visibility') === 'visible');
        if (layers.length === 0) return;
        const features = map!.queryRenderedFeatures(e.point, { layers });
        if (!features.length) { popup = null; return; }
        const f = features[0];
        const scale = ALL_SCALES.find(s => `${s.key}-fill` === f.layer.id);
        const cls   = f.state?.cls;
        popup = {
          x: e.point.x, y: e.point.y,
          title: scale?.label ?? 'Area',
          rows: [
            { label: 'ID',    value: String(f.id) },
            { label: 'Class', value: cls != null && cls >= 0 ? `Class ${cls + 1}` : 'No data' },
          ],
        };
      });

      // Flow line click — show OD info
      map!.on('click', (e) => {
        const flowLayers = EDGE_DATASETS
          .map(d => `flows-${d.key}-layer`)
          .filter(id => map!.getLayer(id) &&
            map!.getLayoutProperty(id, 'visibility') === 'visible');
        if (!flowLayers.length) return;
        const features = map!.queryRenderedFeatures(e.point, { layers: flowLayers });
        if (!features.length) return;
        const f   = features[0];
        const p   = f.properties;
        const res = p?.residual != null ? Number(p.residual).toFixed(3) : null;
        popup = {
          x: e.point.x, y: e.point.y,
          title: 'Flow',
          rows: [
            { label: 'From',      value: String(p?.origin_id ?? '—') },
            { label: 'To',        value: String(p?.dest_id   ?? '—') },
            { label: 'Observed',  value: p?.observed  != null ? Math.round(Number(p.observed)).toLocaleString()  : String(p?.flow_value ?? '—') },
            ...(p?.predicted != null ? [{ label: 'Predicted', value: Math.round(Number(p.predicted)).toLocaleString() }] : []),
            ...(res != null ? [{ label: 'Residual', value: res }] : []),
          ],
        };
      });

      // Change cursor on hover
      map!.on('mousemove', (e) => {
        const allClickable = [
          ...choroplethLayers,
          ...EDGE_DATASETS.map(d => `flows-${d.key}-layer`),
          ...EDGE_DATASETS.map(d => `flows-${d.key}-arrows`),
        ].filter(id => map!.getLayer(id));
        const hit = map!.queryRenderedFeatures(e.point, { layers: allClickable });
        map!.getCanvas().style.cursor = hit.length ? 'pointer' : '';
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
    if (Math.abs(v) >= 100) return v.toFixed(0);
    if (Math.abs(v) >= 10)  return v.toFixed(1);
    if (Math.abs(v) >= 1)   return v.toFixed(2);
    return v.toFixed(3);  // small percentages like 0.02% get 3dp
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
    {#each ([['nodal','Nodal','#045a8d'],['edges','Edges','#e63946'],['calc','Calculate','#238b45'],['model','Model','#7b2d8b']] as const) as [key, lbl, col]}
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
          <span class="flbl">{nodalExtent === 'both' ? 'Study area scale' : 'Scale'}</span>
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
    <div class="tab-section-header">
      <p class="hint" style="margin:0">Toggle flow datasets. Gemeente: all NL. PC4: study area flows.</p>
      <button class="collapse-btn" onclick={() => edgePanelOpen = !edgePanelOpen}>
        {edgePanelOpen ? '▲' : '▼'}
      </button>
    </div>
    {#if edgePanelOpen}

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
            {@const inks = layer.inkFilter as string[]}
            {@const opls = layer.oplFilter as string[]}
            <div class="field">
              <span class="flbl">Income <span style="color:#aaa">multi-select</span></span>
              <div class="chips">
                <button class="chip" class:chip-on={inks.length === 0}
                        onclick={() => clearEdgeInk(ds.key)}>All</button>
                {#each INK_OPTIONS as opt}
                  <button class="chip" class:chip-on={inks.includes(opt.value)}
                          onclick={() => toggleEdgeInk(ds.key, opt.value)}>
                    {opt.label}
                  </button>
                {/each}
              </div>
            </div>
            <div class="field">
              <span class="flbl">Education <span style="color:#aaa">multi-select</span></span>
              <div class="chips">
                <button class="chip" class:chip-on={opls.length === 0}
                        onclick={() => clearEdgeOpl(ds.key)}>All</button>
                {#each OPL_OPTIONS as opt}
                  <button class="chip" class:chip-on={opls.includes(opt.value)}
                          onclick={() => toggleEdgeOpl(ds.key, opt.value)}>
                    {opt.label}
                  </button>
                {/each}
              </div>
            </div>
            {#if inks.length > 0 || opls.length > 0}
              <p class="hint" style="color:#e07b39; margin:0">
                ⚠ {[inks.length > 0 ? `Income: ${inks.join('+')}` : '',
                    opls.length > 0 ? `Edu: ${opls.join('+')}` : ''].filter(Boolean).join(' · ')}
              </p>
            {/if}
          {/if}
        {/if}
      </div>
    {/each}
    {/if}
  {/if}

  <!-- ── MODEL TAB ── -->
  {#if activeTab === 'model'}
    <div class="tab-section-header">
      <span style="font-size:0.72rem; color:#7b2d8b; font-weight:600">
        {modelType === 'gravity' ? 'PC4 Gravity Model' : 'PC4 Nodal Model'}
      </span>
      <button class="collapse-btn" onclick={() => modelPanelOpen = !modelPanelOpen}>
        {modelPanelOpen ? '▲' : '▼'}
      </button>
    </div>

    <!-- Model type selector -->
    <div class="chips">
      <button class="chip" class:chip-on={modelType === 'gravity'}
              onclick={() => modelType = 'gravity'}>Gravity (OD flows)</button>
      <button class="chip" class:chip-on={modelType === 'nodal'}
              onclick={() => modelType = 'nodal'}>Nodal (area level)</button>
    </div>

    {#if modelPanelOpen && modelType === 'nodal'}
      <div class="field">
        <span class="flbl">Outcome variable (Y)</span>
        <select bind:value={nodalModelOutcome}>
          {#each NODAL_OUTCOMES as v}
            <option value={v.key}>{v.label}</option>
          {/each}
          {#each (cbsCovariateVars ?? []) as v}
            <option value={v.key}>{v.label}</option>
          {/each}
        </select>
      </div>

      <div class="field">
        <span class="flbl">Predictors (CBS variables, max 5)</span>
        {#each nodalModelPredictors as pred, i}
          <div class="calc-term" style="margin-bottom:0.3rem">
            <div class="calc-term-type">
              <select value={pred}
                      onchange={(e) => {
                        nodalModelPredictors = nodalModelPredictors.map((p, idx) =>
                          idx === i ? (e.target as HTMLSelectElement).value : p);
                      }}>
                {#each (cbsCovariateVars ?? []) as v}
                  <option value={v.key}>{v.label}</option>
                {/each}
              </select>
              <button class="calc-remove"
                      onclick={() => nodalModelPredictors = nodalModelPredictors.filter((_, idx) => idx !== i)}>
                ✕
              </button>
            </div>
          </div>
        {/each}
        {#if nodalModelPredictors.length < 5}
          <button class="calc-add"
                  onclick={() => nodalModelPredictors = [...nodalModelPredictors,
                    cbsCovariateVars[0]?.key ?? '']}>
            + Add predictor
          </button>
        {/if}
      </div>

      <button class="model-run-btn" onclick={runNodalModel}
              disabled={nodalModelRunning || nodalModelPredictors.length === 0}
              style="background:#2a9d8f">
        {nodalModelRunning ? 'Running…' : '▶ Run nodal model'}
      </button>

      {#if nodalModelError}
        <p class="calc-warn">{nodalModelError}</p>
      {/if}

      {#if nodalModelResults}
        <div class="model-results">
          <div class="model-stat-row">
            <span class="model-stat">R² <strong>{nodalModelResults.r2.toFixed(3)}</strong></span>
            <span class="model-stat">Adj R² <strong>{nodalModelResults.r2adj.toFixed(3)}</strong></span>
            <span class="model-stat">n <strong>{nodalModelResults.n}</strong></span>
          </div>
          <table class="model-table">
            <thead><tr><th>Variable</th><th>β</th><th>SE</th><th>t</th></tr></thead>
            <tbody>
              {#each nodalModelResults.coefficients as row}
                <tr>
                  <td>{row.name}</td>
                  <td>{row.coef.toFixed(3)}</td>
                  <td>{row.se.toFixed(3)}</td>
                  <td class:sig={Math.abs(row.t) > 2}>{row.t.toFixed(2)}</td>
                </tr>
              {/each}
            </tbody>
          </table>
          <p class="hint" style="margin-top:0.3rem">
            Residuals shown as choropleth on PC4 map (teal = above predicted, red = below)
          </p>
        </div>
      {/if}
    {/if}

    {#if modelPanelOpen && modelType === 'gravity'}
    <div class="field">
      <span class="flbl">Flow dataset</span>
      <div class="chips">
        {#each ['20072012','20122017'] as p}
          <button class="chip" class:chip-on={modelPeriod === p}
                  onclick={() => modelPeriod = p}>
            {p === '20072012' ? '2007–2012' : '2012–2017'}
          </button>
        {/each}
      </div>
    </div>

    <div class="field">
      <span class="flbl">Income group <span style="color:#aaa">multi-select · filters flows</span></span>
      <div class="chips">
        <button class="chip" class:chip-on={modelInkFilter.length === 0}
                onclick={() => modelInkFilter = []}>All</button>
        {#each INK_OPTIONS as opt}
          <button class="chip" class:chip-on={modelInkFilter.includes(opt.value)}
                  onclick={() => toggleModelInk(opt.value)}>{opt.label}</button>
        {/each}
      </div>
    </div>

    <div class="field">
      <span class="flbl">Education <span style="color:#aaa">multi-select · filters flows</span></span>
      <div class="chips">
        <button class="chip" class:chip-on={modelOplFilter.length === 0}
                onclick={() => modelOplFilter = []}>All</button>
        {#each OPL_OPTIONS as opt}
          <button class="chip" class:chip-on={modelOplFilter.includes(opt.value)}
                  onclick={() => toggleModelOpl(opt.value)}>{opt.label}</button>
        {/each}
      </div>
    </div>

    <div class="field">
      <span class="flbl">Origin variable (push)</span>
      <select bind:value={modelOriginKey}>
        {#each MODEL_VARS_PC4 as v}
          <option value={v.key}>{v.label}</option>
        {/each}
      </select>
      <label class="row toggle-row" style="margin-top:0.2rem">
        <input type="checkbox" bind:checked={modelLogOrigin} />
        <span class="toggle-lbl" style="font-size:0.72rem">Log transform</span>
      </label>
    </div>

    <div class="field">
      <span class="flbl">Destination variable (pull)</span>
      <select bind:value={modelDestKey}>
        {#each MODEL_VARS_PC4 as v}
          <option value={v.key}>{v.label}</option>
        {/each}
      </select>
      <label class="row toggle-row" style="margin-top:0.2rem">
        <input type="checkbox" bind:checked={modelLogDest} />
        <span class="toggle-lbl" style="font-size:0.72rem">Log transform</span>
      </label>
    </div>

    <div class="field">
      <span class="flbl">Distance decay</span>
      <div class="chips">
        <button class="chip" class:chip-on={modelLogDistance}
                onclick={() => modelLogDistance = true}>Log dist</button>
        <button class="chip" class:chip-on={!modelLogDistance}
                onclick={() => modelLogDistance = false}>Power decay</button>
      </div>
      <div class="row" style="gap:0.4rem; margin-top:0.25rem; align-items:center">
        <span style="font-size:0.72rem; color:#888">Exponent:</span>
        <input type="number" min="0.5" max="4" step="0.25"
               bind:value={modelDecayExp}
               style="width:60px; padding:0.2rem 0.3rem; border:1px solid #ddd; border-radius:4px; font-size:0.78rem" />
      </div>
    </div>

    <!-- Covariates -->
    <div class="field">
      <span class="flbl">Additional covariates (max 3)</span>
      {#each modelCovariates as cov, i}
        <div class="calc-term" style="margin-bottom:0.3rem">
          <div class="calc-term-type">
            <select value={cov.key}
                    onchange={(e) => updateModelCovariate(i, {
                      key: (e.target as HTMLSelectElement).value,
                      label: MODEL_VARS_PC4.find(v => v.key === (e.target as HTMLSelectElement).value)?.label ?? ''
                    })}>
              {#each MODEL_VARS_PC4 as v}
                <option value={v.key}>{v.label}</option>
              {/each}
            </select>
            <button class="calc-remove" onclick={() => removeModelCovariate(i)}>✕</button>
          </div>
          <div class="row" style="gap:0.6rem">
            <label class="row toggle-row">
              <input type="checkbox" checked={cov.useLog}
                     onchange={() => updateModelCovariate(i, {useLog: !cov.useLog})} />
              <span style="font-size:0.7rem">Log</span>
            </label>

          </div>
        </div>
      {/each}
      {#if modelCovariates.length < 3}
        <button class="calc-add" onclick={addModelCovariate}>+ Add covariate</button>
      {/if}
    </div>

    <!-- Flow options -->
    <div class="field">
      <span class="flbl">Internal flows (same PC4)</span>
      <div class="chips">
        {#each [['exclude','Exclude'],['include','Include'],['only','Only']] as [v,l]}
          <button class="chip" class:chip-on={modelInternals === v}
                  onclick={() => modelInternals = v as typeof modelInternals}>{l}</button>
        {/each}
      </div>
    </div>

    <div class="field">
      <span class="flbl">Min flow count</span>
      <input type="number" min="1" max="500" bind:value={modelMinFlow}
             style="width:80px; padding:0.25rem 0.35rem; border:1px solid #ddd; border-radius:4px; font-size:0.8rem" />
    </div>

    <!-- income/education filter moved to top -->

    <!-- Residual display -->
    <!-- Residual view: always all edges, direction shown by colour not filter -->

    <!-- Run button -->
    <button class="model-run-btn" onclick={runGravityModel} disabled={modelRunning}>
      {modelRunning ? 'Running model…' : '▶ Run gravity model'}
    </button>
    {/if}

    {#if modelError}
      <p class="calc-warn">{modelError}</p>
    {/if}

    <!-- Results always visible once run -->
    {#if modelResults}
      <div class="model-results">
        <div class="model-stat-row">
          <span class="model-stat">R² <strong>{modelResults.r2.toFixed(3)}</strong></span>
          <span class="model-stat">Adj R² <strong>{modelResults.r2adj.toFixed(3)}</strong></span>
          <span class="model-stat">n <strong>{modelResults.n}</strong></span>
        </div>

        <table class="model-table">
          <thead>
            <tr><th>Variable</th><th>β</th><th>SE</th><th>t</th><th>VIF</th></tr>
          </thead>
          <tbody>
            {#each modelResults.coefficients as row, i}
              {@const vif = modelResults.vif[i-1]}
              <tr class:high-vif={vif && vif.vif > 5}>
                <td>{row.name}</td>
                <td>{row.coef.toFixed(3)}</td>
                <td>{row.se.toFixed(3)}</td>
                <td class:sig={Math.abs(row.t) > 2}>{row.t.toFixed(2)}</td>
                <td>{vif ? (vif.vif > 10 ? '⚠️ ' : '') + vif.vif.toFixed(1) : '—'}</td>
              </tr>
            {/each}
          </tbody>
        </table>

        <button class="calc-add" onclick={clearResiduals}
                style="margin-top:0.5rem">Hide residuals</button>
      </div>
    {/if}
  {/if}

  <!-- ── CALC TAB ── -->
  {#if activeTab === 'calc'}
    <label class="row toggle-row">
      <input type="checkbox" bind:checked={calcEnabled} />
      <span class="toggle-lbl">Show computed layer</span>
    </label>

    {#if calcEnabled}
      <p class="hint">Build an expression with up to 4 terms. Mix variables and constants.</p>

      {#each calcTerms as term, i}
        <!-- Operator between terms -->
        {#if i > 0}
          <div class="calc-op-row">
            {#each CALC_OPERATORS as op}
              <button class="chip" class:chip-on={calcOps[i-1] === op.key}
                      onclick={() => setCalcOp(i-1, op.key as CalcOp)}>
                {op.label.split(' ')[2]}
              </button>
            {/each}
          </div>
        {/if}

        <!-- Term -->
        <div class="calc-term">
          <div class="calc-term-type">
            <button class="chip chip-sm" class:chip-on={term.type === 'var'}
                    onclick={() => setCalcTermType(i, 'var')}>Variable</button>
            <button class="chip chip-sm" class:chip-on={term.type === 'const'}
                    onclick={() => setCalcTermType(i, 'const')}>Constant</button>
            {#if calcTerms.length > 2}
              <button class="calc-remove" onclick={() => removeCalcTerm(i)}>✕</button>
            {/if}
          </div>

          {#if term.type === 'var'}
            <select value={term.key}
                    onchange={(e) => setCalcTermKey(i, (e.target as HTMLSelectElement).value)}>
              {#each [...groupedVars()] as [group, vars]}
                <optgroup label={group}>
                  {#each vars as v}<option value={v.key}>{v.label}</option>{/each}
                </optgroup>
              {/each}
            </select>
          {:else}
            <input type="number" value={term.value} class="calc-const-input"
                   oninput={(e) => setCalcTermConst(i, (e.target as HTMLInputElement).value)}
                   placeholder="e.g. 1000" />
          {/if}
        </div>
      {/each}

      {#if calcTerms.length < 4}
        <button class="calc-add" onclick={addCalcTerm}>+ Add term</button>
      {/if}

      {@const shared = calcShared()}
      {#if shared.length > 0}
        <div class="field">
          <span class="flbl">Scale</span>
          <div class="chips">
            {#each ALL_SCALES.filter(s => shared.includes(s.key)) as s}
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

<!-- ── Popup ──────────────────────────────────────────────── -->
{#if popup}
  <div class="map-popup" style="left:{popup.x}px; top:{popup.y}px"
       onclick={() => popup = null}>
    <div class="popup-title">{popup.title} <span class="popup-close">✕</span></div>
    {#each popup.rows as row}
      <div class="popup-row">
        <span class="popup-label">{row.label}</span>
        <span class="popup-value">{row.value}</span>
      </div>
    {/each}
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

    {#if modelResults}
    <div class="divider"></div>
    <div class="legend-title" style="color:#7b2d8b">Residuals</div>
    <div class="legend-sub">observed − predicted</div>
    <div class="legend-row">
      <span class="sw" style="background:#2a9d8f"></span>
      <span class="legend-cls">+ Strong (more than expected)</span>
    </div>
    <div class="legend-row">
      <span class="sw" style="background:#74c476"></span>
      <span class="legend-cls">+ Weak</span>
    </div>
    <div class="legend-row">
      <span class="sw" style="background:#dddddd"></span>
      <span class="legend-cls">Neutral (well-fit)</span>
    </div>
    <div class="legend-row">
      <span class="sw" style="background:#f4a261"></span>
      <span class="legend-cls">− Weak</span>
    </div>
    <div class="legend-row">
      <span class="sw" style="background:#e63946"></span>
      <span class="legend-cls">− Strong (less than expected)</span>
    </div>
    <div class="legend-sub" style="padding-left:20px">
      R² {modelResults.r2.toFixed(3)} · n {modelResults.n}
    </div>
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
    word-break: break-word;
  }
  .calc-warn { font-size: 0.78rem; color: #c62828; margin: 0; }

  .calc-term {
    display: flex; flex-direction: column; gap: 0.3rem;
    background: #f9f8f6; border-radius: 6px;
    padding: 0.45rem 0.5rem;
    border: 1px solid #eee;
  }
  .calc-term-type {
    display: flex; gap: 0.25rem; align-items: center;
  }
  .chip-sm {
    padding: 0.1rem 0.4rem; font-size: 0.7rem;
  }
  .calc-remove {
    margin-left: auto; background: none; border: none;
    color: #c00; cursor: pointer; font-size: 0.75rem;
    padding: 0 0.2rem;
  }
  .calc-op-row {
    display: flex; gap: 0.25rem; justify-content: center;
    padding: 0.1rem 0;
  }
  .calc-add {
    background: none; border: 1px dashed #bbb; border-radius: 6px;
    color: #666; cursor: pointer; font-size: 0.76rem;
    padding: 0.3rem; width: 100%;
    transition: background 0.1s;
  }
  .calc-add:hover { background: #f4f1ec; }
  .calc-const-input {
    width: 100%; padding: 0.28rem 0.45rem;
    border: 1px solid #ddd; border-radius: 6px;
    font-size: 0.8rem; background: #fff;
  }

  /* ── Legend ─────────────────────────────────────────────── */
  .legend {
    position: fixed; bottom: 1.5rem; left: 0.75rem;
    z-index: 998;  /* below panel (999) so panel renders on top */
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

  .tab-section-header {
    display: flex; align-items: center; justify-content: space-between;
    gap: 0.5rem;
  }
  .collapse-btn {
    background: none; border: none; cursor: pointer;
    font-size: 0.65rem; color: #aaa; padding: 0.1rem 0.3rem;
    border-radius: 3px; flex-shrink: 0;
  }
  .collapse-btn:hover { background: #f0ede8; color: #666; }

  /* ── Popup ──────────────────────────────────────────────── */
  .map-popup {
    position: fixed;
    z-index: 1100;
    background: #fff;
    border-radius: 8px;
    box-shadow: 0 3px 14px rgba(0,0,0,0.2);
    padding: 0.6rem 0.8rem;
    font-family: sans-serif;
    font-size: 0.78rem;
    min-width: 160px;
    max-width: 240px;
    pointer-events: auto;
    transform: translate(-50%, -110%);
  }
  .popup-title {
    font-weight: 700; font-size: 0.8rem;
    color: #1a1a2e; margin-bottom: 0.35rem;
    display: flex; justify-content: space-between;
  }
  .popup-close { color: #aaa; cursor: pointer; font-size: 0.7rem; }
  .popup-row {
    display: flex; justify-content: space-between;
    gap: 0.5rem; padding: 0.1rem 0;
    border-bottom: 1px solid #f5f5f5;
  }
  .popup-label { color: #888; }
  .popup-value { font-weight: 500; color: #1a1a2e; }

  /* ── Model tab ───────────────────────────────────────────── */
  .model-run-btn {
    background: #7b2d8b; color: white; border: none;
    border-radius: 6px; padding: 0.5rem 1rem;
    font-size: 0.82rem; font-weight: 600; cursor: pointer;
    width: 100%; transition: background 0.1s;
  }
  .model-run-btn:hover:not(:disabled) { background: #6a2478; }
  .model-run-btn:disabled { opacity: 0.5; cursor: not-allowed; }

  .model-results {
    display: flex; flex-direction: column; gap: 0.5rem;
    border-top: 1px solid #eee; padding-top: 0.5rem;
  }
  .model-stat-row {
    display: flex; gap: 0.75rem; font-size: 0.78rem;
  }
  .model-stat { color: #666; }
  .model-stat strong { color: #1a1a2e; }

  .model-table {
    width: 100%; font-size: 0.72rem; border-collapse: collapse;
  }
  .model-table th {
    text-align: left; font-weight: 600; color: #888;
    border-bottom: 1px solid #eee; padding: 0.2rem 0.3rem;
    font-size: 0.68rem; text-transform: uppercase;
  }
  .model-table td {
    padding: 0.18rem 0.3rem; border-bottom: 1px solid #f5f5f5;
  }
  .model-table tr.high-vif { background: #fff8f0; }
  .model-table td.sig { color: #1a6e2e; font-weight: 600; }
</style>