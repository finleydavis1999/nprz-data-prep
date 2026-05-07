<script lang="ts">
  // ===========================================================================
  // +page.svelte
  //
  // The single Svelte route component for the NPRZ Spatial Explorer.
  //
  // This file is intentionally kept thin: it owns all reactive state ($state,
  // $derived, $effect) and the HTML template, but delegates ALL business logic
  // to the modules in src/lib/.
  //
  // State lives here because Svelte 5 runes ($state/$derived/$effect) cannot
  // be used in plain .ts files — they must live inside a .svelte component.
  //
  // Tab structure:
  //   Nodal    → choropleth variable display at one or two spatial scales
  //   Edges    → OD flow lines (commuting, job moves, migration)
  //   Calculate → multi-term expression builder, rendered as green choropleth
  //   Model    → gravity model (OD flows) or nodal model (area-level OLS)
  // ===========================================================================

  import { browser } from '$app/environment';
  import 'maplibre-gl/dist/maplibre-gl.css';

  // ── Lib imports ─────────────────────────────────────────────────────────────
  import {
    // Config
    ALL_SCALES, INNER_SCALES, OUTER_SCALES,
    VARIABLES, EDGE_DATASETS, NORMALISATIONS, CALC_OPERATORS,
    COLOURS_BLUE, COLOURS_GREEN, NO_DATA,
    findScale, scaleChips, isInnerScale, varsForScale,
    // Classification
    quantileBreaks,
    // Formatting
    fmt, groupedVars,
    // DuckDB
    initDuckDB, queuedQuery, dataURL,
    // Map
    initMap as _initMap, setLayerVis, setColours, clearBoundaryClip,
    // Tab logic
    renderNodal,
    applyCalc, calcSharedScales, calcLabel as _calcLabel,
    loadEdgeLayer,
    runGravityModel, runNodalModel, drawResiduals, clearResiduals,
    // Popup
    buildAreaPopup, buildFlowPopup,
  } from '$lib';

  import type {
    SpatialExtent, Normalisation, ActiveEdgeLayer,
    CalcTerm, CalcOp,
    ModelCovariate, ModelResults, NodalModelResults,
    PopupInfo,
  } from '$lib';

  // ── UI state ─────────────────────────────────────────────────────────────────
  type Tab = 'nodal' | 'edges' | 'calc' | 'model';
  let activeTab  = $state<Tab>('nodal');
  let panelOpen  = $state(true);

  // ── Infrastructure readiness ─────────────────────────────────────────────────
  let mapReady = $state(false);
  let dbReady  = $state(false);
  let loading  = $state(false);
  let error    = $state<string | null>(null);

  // ── Nodal tab state ──────────────────────────────────────────────────────────
  let nodalEnabled   = $state(true);
  let nodalVarKey    = $state('total_population');
  let nodalExtent    = $state<SpatialExtent>('inner');
  let innerScaleKey  = $state('buurt');
  let outerScaleKey  = $state('gemeente');
  let normalisation  = $state<Normalisation>('none');
  let nodalBreaks    = $state<number[]>([]);

  const chips    = $derived(scaleChips(nodalVarKey));
  const nodalVar = $derived(VARIABLES.find(v => v.key === nodalVarKey)!);

  // Keep scale selections valid when the variable changes
  $effect(() => {
    const { inner, outer } = scaleChips(nodalVarKey);
    if (!inner.some(c => c.key === innerScaleKey)) innerScaleKey = inner[0]?.key ?? '';
    if (!outer.some(c => c.key === outerScaleKey)) outerScaleKey = outer[0]?.key ?? '';
    if (nodalExtent === 'inner' && inner.length === 0) nodalExtent = 'outer';
    if (nodalExtent === 'outer' && outer.length === 0) nodalExtent = 'inner';
    if (nodalExtent === 'both' && (inner.length === 0 || outer.length === 0))
      nodalExtent = inner.length > 0 ? 'inner' : 'outer';
  });

  // Extent options available for the currently selected variable
  const extentOptions = $derived(() => {
    const { inner, outer } = chips;
    const opts: { key: SpatialExtent; label: string }[] = [];
    if (inner.length > 0) opts.push({ key: 'inner', label: 'Study area' });
    if (outer.length > 0) opts.push({ key: 'outer', label: 'Wider region' });
    if (inner.length > 0 && outer.length > 0) opts.push({ key: 'both', label: 'Both' });
    return opts;
  });

  // ── Edges tab state ──────────────────────────────────────────────────────────
  let edgePanelOpen = $state(true);

  // One entry per dataset — starts hidden, no filters active
  let edgeLayers = $state<ActiveEdgeLayer[]>(
    EDGE_DATASETS.map(d => ({
      datasetKey: d.key,
      period:     d.defaultPeriod,
      visible:    false,
      inkFilter:  [] as string[],
      oplFilter:  [] as string[],
    }))
  );

  // Per-dataset flow value ranges, used by the legend (min/max line width guide)
  const flowRanges = new Map<string, { min: number; max: number }>();

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
      return { ...l, inkFilter: cur.includes(val) ? cur.filter(v => v !== val) : [...cur, val] };
    });
  }
  function clearEdgeInk(key: string) {
    edgeLayers = edgeLayers.map(l => l.datasetKey === key ? { ...l, inkFilter: [] } : l);
  }
  function toggleEdgeOpl(key: string, val: string) {
    edgeLayers = edgeLayers.map(l => {
      if (l.datasetKey !== key) return l;
      const cur = l.oplFilter as string[];
      return { ...l, oplFilter: cur.includes(val) ? cur.filter(v => v !== val) : [...cur, val] };
    });
  }
  function clearEdgeOpl(key: string) {
    edgeLayers = edgeLayers.map(l => l.datasetKey === key ? { ...l, oplFilter: [] } : l);
  }

  const INK_OPTIONS = [
    { value: '1', label: '< 20%'   },
    { value: '2', label: '20–40%'  },
    { value: '3', label: '40–60%'  },
    { value: '4', label: '60–80%'  },
    { value: '5', label: '80–100%' },
  ];
  const OPL_OPTIONS = [
    { value: '1', label: 'Laag'   },
    { value: '2', label: 'Midden' },
    { value: '3', label: 'Hoog'   },
  ];

  // ── Calculator tab state ──────────────────────────────────────────────────────
  let calcEnabled  = $state(false);
  let calcTerms    = $state<CalcTerm[]>([
    { type: 'var', key: 'nodes_banen_werk' },
    { type: 'var', key: 'nodes_inwoners'   },
  ]);
  let calcOps      = $state<CalcOp[]>(['/']);
  let calcScaleKey = $state('pc4');
  let calcBreaks   = $state<number[]>([]);

  const calcShared = $derived(() => calcSharedScales(calcTerms));
  const calcLabel  = $derived(() => _calcLabel(calcTerms, calcOps));

  // Reset scale selection if it's no longer valid for the current terms
  $effect(() => {
    const shared = calcSharedScales(calcTerms);
    if (!shared.includes(calcScaleKey) && shared.length > 0)
      calcScaleKey = shared[0];
  });

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
        ? { type: 'var',   key:   'nodes_banen_werk' }
        : { type: 'const', value: '1000' }) : t
    );
  }
  function setCalcTermKey(i: number, key: string) {
    calcTerms = calcTerms.map((t, idx) => idx === i ? { type: 'var', key } : t);
  }
  function setCalcTermConst(i: number, value: string) {
    calcTerms = calcTerms.map((t, idx) => idx === i ? { type: 'const', value } : t);
  }
  function setCalcOp(i: number, op: CalcOp) {
    calcOps = calcOps.map((o, idx) => idx === i ? op : o);
  }

  // ── Model tab state ───────────────────────────────────────────────────────────
  let modelPanelOpen  = $state(true);
  let modelType       = $state<'gravity' | 'nodal'>('gravity');

  // Gravity model parameters
  let modelPeriod      = $state('20122017');
  let modelOriginKey   = $state('nodes_banen_woon');
  let modelDestKey     = $state('nodes_banen_werk');
  let modelCovariates  = $state<ModelCovariate[]>([]);
  let modelLogOrigin   = $state(true);
  let modelLogDest     = $state(true);
  let modelLogDistance = $state(true);
  let modelDecayExp    = $state(2.0);
  let modelInternals   = $state<'exclude' | 'include' | 'only'>('exclude');
  let modelMinFlow     = $state(10);
  let modelInkFilter   = $state<string[]>([]);
  let modelOplFilter   = $state<string[]>([]);
  let modelRunning     = $state(false);
  let modelResults     = $state<ModelResults | null>(null);
  let modelError       = $state<string | null>(null);

  // Nodal model parameters
  let nodalModelOutcome    = $state('flow_outflow');
  let nodalModelPredictors = $state<string[]>([]);
  let nodalModelRunning    = $state(false);
  let nodalModelResults    = $state<NodalModelResults | null>(null);
  let nodalModelError      = $state<string | null>(null);

  // CBS covariate list — loaded dynamically when the model tab first opens
  let cbsCovariateVars = $state<{ key: string; label: string }[]>([]);

  $effect(() => {
    if (activeTab === 'model' && dbReady && cbsCovariateVars.length === 0) {
      queuedQuery(
        `DESCRIBE SELECT * FROM read_parquet('${dataURL('pc4_zh_2024_stats.parquet')}') LIMIT 0`
      ).then((res: any) => {
        const skip = new Set([
          'postcode', 'jaar', 'statcode', 'statnaam', 'gemeentenaam',
          'indelingswijziging_wijken_en_buurten', 'water', 'meest_voorkomende_postcode',
        ]);
        const skipPatterns = [
          '_aantal_binnen_', '_afstand_in_km', 'dichtstbijzijnde_',
          'hotel_', 'bioscoop_', 'cafe_', 'restaurant_',
          'attractie_', 'museum_', 'theater_', 'podium_', 'warenhuis_',
        ];
        cbsCovariateVars = res.toArray()
          .map((r: any) => r.toJSON().column_name as string)
          .filter((k: string) =>
            !skip.has(k) &&
            !k.startsWith('id') &&
            !skipPatterns.some((p: string) => k.includes(p))
          )
          .map((k: string) => ({ key: k, label: k }));
      }).catch(() => {});
    }
  });

  const MODEL_NODE_VARS = [
    { key: 'nodes_banen_werk', label: 'Jobs (work location)'     },
    { key: 'nodes_banen_woon', label: 'Employed residents'       },
    { key: 'nodes_inwoners',   label: 'Population (CBS microdata)' },
  ];
  const MODEL_VARS_PC4 = $derived([...MODEL_NODE_VARS, ...cbsCovariateVars]);

  const NODAL_OUTCOMES = [
    { key: 'flow_outflow',               label: 'Commuter outflow'       },
    { key: 'flow_inflow',                label: 'Commuter inflow'        },
    { key: 'flow_internal',              label: 'Internal commuters'     },
    { key: 'flow_self_containment',      label: 'Self-containment ratio' },
    { key: 'nodes_banen_werk',           label: 'Jobs at work location'  },
    { key: 'nodes_banen_woon',           label: 'Employed residents'     },
    { key: 'nodes_ratio_banen_inwoners', label: 'Jobs/residents ratio'   },
  ];

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
  function updateModelCovariate(i: number, patch: Partial<ModelCovariate>) {
    modelCovariates = modelCovariates.map((c, idx) => idx === i ? { ...c, ...patch } : c);
  }

  async function handleRunGravity() {
    modelRunning = true; modelError = null; modelResults = null;
    try {
      modelResults = await runGravityModel({
        period:      modelPeriod,
        originKey:   modelOriginKey,
        destKey:     modelDestKey,
        logOrigin:   modelLogOrigin,
        logDest:     modelLogDest,
        logDistance: modelLogDistance,
        decayExp:    modelDecayExp,
        covariates:  modelCovariates,
        internals:   modelInternals,
        minFlow:     modelMinFlow,
        inkFilter:   modelInkFilter,
        oplFilter:   modelOplFilter,
      });
    } catch (e) {
      modelError = `${e}`;
    }
    modelRunning = false;
  }

  async function handleRunNodal() {
    nodalModelRunning = true; nodalModelError = null; nodalModelResults = null;
    try {
      nodalModelResults = await runNodalModel({
        outcomeKey:    nodalModelOutcome,
        predictorKeys: nodalModelPredictors,
      });
    } catch (e) {
      nodalModelError = `${e}`;
    }
    nodalModelRunning = false;
  }

  // ── Popup state ───────────────────────────────────────────────────────────────
  let popup = $state<PopupInfo | null>(null);

  // ── Map initialisation ────────────────────────────────────────────────────────
  function setupMap(container: HTMLDivElement) {
    _initMap(
      container,
      // onReady
      () => {
        mapReady = true;
        if (dbReady) scheduleUpdate();
      },
      // onAreaClick
      (info) => {
        if (!info.title) { popup = null; return; }
        // TODO: extend to show actual variable value (known bug — see README)
        popup = buildAreaPopup(0, 0, info.title, info.id, info.cls);
      },
      // onFlowClick
      (props) => {
        popup = buildFlowPopup(0, 0, props);
      },
    );
  }

  // ── DuckDB initialisation ─────────────────────────────────────────────────────
  $effect(() => {
    if (!browser) return;
    initDuckDB()
      .then(() => {
        dbReady = true;
        if (mapReady) scheduleUpdate();
      })
      .catch(e => { error = `DuckDB failed: ${e}`; });
  });

  // ── Master update ─────────────────────────────────────────────────────────────
  // Debounced to prevent rapid state changes (e.g. clicking through chips) from
  // firing multiple concurrent queries. Filter changes use a longer delay.

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

  function scheduleFilterUpdate() { scheduleUpdate(400); }

  // Re-run whenever any relevant state changes
  $effect(() => {
    const _ = [
      nodalEnabled, nodalVarKey, nodalExtent, innerScaleKey, outerScaleKey, normalisation,
      calcEnabled, JSON.stringify(calcTerms), JSON.stringify(calcOps), calcScaleKey,
      JSON.stringify(edgeLayers),
    ];
    if (mapReady && dbReady) scheduleUpdate();
  });

  async function runUpdate() {
    if (!mapReady || !dbReady) return;
    loading = true; error = null;

    try {
      const showInner = nodalEnabled && (nodalExtent === 'inner' || nodalExtent === 'both');
      const showOuter = nodalEnabled && (nodalExtent === 'outer' || nodalExtent === 'both');

      // Hide all layers not needed in this render pass
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
      setLayerVis('inner-mask', 'none');

      // ── Nodal choropleth ───────────────────────────────────────────────────
      if (nodalEnabled) {
        const { innerBreaks, outerBreaks } = await renderNodal({
          varKey:        nodalVarKey,
          extent:        nodalExtent,
          innerScaleKey,
          outerScaleKey,
          norm:          normalisation,
        });
        // Legend uses whichever breaks are non-empty
        nodalBreaks = innerBreaks.length ? innerBreaks : outerBreaks;
      }

      // ── Calculator choropleth ─────────────────────────────────────────────
      if (calcEnabled && calcShared().length > 0) {
        calcBreaks = await applyCalc(calcTerms, calcOps, calcScaleKey);
      }

      // ── Edge flow lines ───────────────────────────────────────────────────
      for (const layer of edgeLayers) {
        const lid = `flows-${layer.datasetKey}-layer`;
        const aid = `flows-${layer.datasetKey}-arrows`;
        if (layer.visible) {
          const range = await loadEdgeLayer(
            layer.datasetKey, layer.period,
            layer.inkFilter as string[], layer.oplFilter as string[],
          );
          flowRanges.set(layer.datasetKey, range);
          setLayerVis(lid, 'visible');
          setLayerVis(aid, 'visible');
        } else {
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
</script>

<!-- ── Map ──────────────────────────────────────────────────────────────────── -->
<div use:setupMap class="map"></div>

<!-- ── Header ───────────────────────────────────────────────────────────────── -->
<div class="header">
  <button class="header-toggle" onclick={() => panelOpen = !panelOpen}
          title={panelOpen ? 'Collapse panel' : 'Expand panel'}>
    {panelOpen ? '◀' : '▶'}
  </button>
  <span class="app-title">NPRZ Spatial Explorer</span>

  <div class="tab-bar">
    {#each ([
      ['nodal', 'Nodal',     '#045a8d'],
      ['edges', 'Edges',     '#e63946'],
      ['calc',  'Calculate', '#238b45'],
      ['model', 'Model',     '#7b2d8b'],
    ] as const) as [key, lbl, col]}
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

<!-- ── Side panel ───────────────────────────────────────────────────────────── -->
{#if panelOpen}
<div class="panel">

  <!-- ══ NODAL TAB ══ -->
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

      <div class="field">
        <span class="flbl">Spatial extent</span>
        <div class="chips">
          {#each extentOptions() as opt}
            <button class="chip" class:chip-on={nodalExtent === opt.key}
                    onclick={() => nodalExtent = opt.key}>{opt.label}</button>
          {/each}
        </div>
      </div>

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

  <!-- ══ EDGES TAB ══ -->
  {#if activeTab === 'edges'}
    <div class="tab-section-header">
      <p class="hint" style="margin:0">Toggle flow datasets. Gemeente: all NL. PC4: study area.</p>
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
                            onclick={() => toggleEdgeInk(ds.key, opt.value)}>{opt.label}</button>
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
                            onclick={() => toggleEdgeOpl(ds.key, opt.value)}>{opt.label}</button>
                  {/each}
                </div>
              </div>
            {/if}
          {/if}
        </div>
      {/each}
    {/if}
  {/if}

  <!-- ══ CALCULATE TAB ══ -->
  {#if activeTab === 'calc'}
    <label class="row toggle-row">
      <input type="checkbox" bind:checked={calcEnabled} />
      <span class="toggle-lbl">Show computed layer</span>
    </label>

    {#if calcEnabled}
      <p class="hint">Build an expression with up to 4 terms. Mix variables and constants.</p>

      {#each calcTerms as term, i}
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

  <!-- ══ MODEL TAB ══ -->
  {#if activeTab === 'model'}
    <div class="tab-section-header">
      <span style="font-size:0.72rem; color:#7b2d8b; font-weight:600">
        {modelType === 'gravity' ? 'PC4 Gravity Model' : 'PC4 Nodal Model'}
      </span>
      <button class="collapse-btn" onclick={() => modelPanelOpen = !modelPanelOpen}>
        {modelPanelOpen ? '▲' : '▼'}
      </button>
    </div>

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
          {#each cbsCovariateVars as v}
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
                {#each cbsCovariateVars as v}
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
                  onclick={() => nodalModelPredictors = [
                    ...nodalModelPredictors, cbsCovariateVars[0]?.key ?? ''
                  ]}>
            + Add predictor
          </button>
        {/if}
      </div>

      <button class="model-run-btn" onclick={handleRunNodal}
              disabled={nodalModelRunning || nodalModelPredictors.length === 0}
              style="background:#2a9d8f">
        {nodalModelRunning ? 'Running…' : '▶ Run nodal model'}
      </button>
      {#if nodalModelError}<p class="calc-warn">{nodalModelError}</p>{/if}

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
        <span class="flbl">Income <span style="color:#aaa">multi-select</span></span>
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
        <span class="flbl">Education <span style="color:#aaa">multi-select</span></span>
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
          {#each MODEL_VARS_PC4 as v}<option value={v.key}>{v.label}</option>{/each}
        </select>
        <label class="row toggle-row" style="margin-top:0.2rem">
          <input type="checkbox" bind:checked={modelLogOrigin} />
          <span class="toggle-lbl" style="font-size:0.72rem">Log transform</span>
        </label>
      </div>

      <div class="field">
        <span class="flbl">Destination variable (pull)</span>
        <select bind:value={modelDestKey}>
          {#each MODEL_VARS_PC4 as v}<option value={v.key}>{v.label}</option>{/each}
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
          <input type="number" min="0.5" max="4" step="0.25" bind:value={modelDecayExp}
                 style="width:60px; padding:0.2rem 0.3rem; border:1px solid #ddd;
                        border-radius:4px; font-size:0.78rem" />
        </div>
      </div>

      <div class="field">
        <span class="flbl">Additional covariates (max 3)</span>
        {#each modelCovariates as cov, i}
          <div class="calc-term" style="margin-bottom:0.3rem">
            <div class="calc-term-type">
              <select value={cov.key}
                      onchange={(e) => updateModelCovariate(i, {
                        key:   (e.target as HTMLSelectElement).value,
                        label: MODEL_VARS_PC4.find(v => v.key === (e.target as HTMLSelectElement).value)?.label ?? '',
                      })}>
                {#each MODEL_VARS_PC4 as v}<option value={v.key}>{v.label}</option>{/each}
              </select>
              <button class="calc-remove" onclick={() => removeModelCovariate(i)}>✕</button>
            </div>
            <label class="row toggle-row">
              <input type="checkbox" checked={cov.useLog}
                     onchange={() => updateModelCovariate(i, { useLog: !cov.useLog })} />
              <span style="font-size:0.7rem">Log</span>
            </label>
          </div>
        {/each}
        {#if modelCovariates.length < 3}
          <button class="calc-add" onclick={addModelCovariate}>+ Add covariate</button>
        {/if}
      </div>

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
               style="width:80px; padding:0.25rem 0.35rem; border:1px solid #ddd;
                      border-radius:4px; font-size:0.8rem" />
      </div>

      <button class="model-run-btn" onclick={handleRunGravity} disabled={modelRunning}>
        {modelRunning ? 'Running model…' : '▶ Run gravity model'}
      </button>
    {/if}

    {#if modelError}<p class="calc-warn">{modelError}</p>{/if}

    {#if modelResults}
      <div class="model-results">
        <div class="model-stat-row">
          <span class="model-stat">R² <strong>{modelResults.r2.toFixed(3)}</strong></span>
          <span class="model-stat">Adj R² <strong>{modelResults.r2adj.toFixed(3)}</strong></span>
          <span class="model-stat">n <strong>{modelResults.n}</strong></span>
        </div>
        <table class="model-table">
          <thead><tr><th>Variable</th><th>β</th><th>SE</th><th>t</th><th>VIF</th></tr></thead>
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
        <button class="calc-add" onclick={clearResiduals} style="margin-top:0.5rem">
          Hide residuals
        </button>
      </div>
    {/if}
  {/if}

</div>
{/if}

<!-- ── Popup ─────────────────────────────────────────────────────────────────── -->
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

<!-- ── Legend ────────────────────────────────────────────────────────────────── -->
{#if (nodalEnabled && nodalBreaks.length) || (calcEnabled && calcBreaks.length) || edgeLayers.some(l => l.visible) || modelResults}
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
          {#if i === 0}≤ {fmt(nodalBreaks[0] ?? 0)}
          {:else if i === COLOURS_BLUE.length - 1}> {fmt(nodalBreaks[nodalBreaks.length - 1] ?? 0)}
          {:else}{fmt(nodalBreaks[i - 1] ?? 0)} – {fmt(nodalBreaks[i] ?? 0)}
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
          {#if i === 0}≤ {fmt(calcBreaks[0] ?? 0)}
          {:else if i === COLOURS_GREEN.length - 1}> {fmt(calcBreaks[calcBreaks.length - 1] ?? 0)}
          {:else}{fmt(calcBreaks[i - 1] ?? 0)} – {fmt(calcBreaks[i] ?? 0)}
          {/if}
        </span>
      </div>
    {/each}
  {/if}

  {#if modelResults}
    <div class="divider"></div>
    <div class="legend-title" style="color:#7b2d8b">Residuals</div>
    <div class="legend-sub">observed − predicted (log space)</div>
    {#each [
      ['#2a9d8f', '+ Strong (more than expected)'],
      ['#74c476', '+ Weak'],
      ['#dddddd', 'Neutral (well-fit)'],
      ['#f4a261', '− Weak'],
      ['#e63946', '− Strong (less than expected)'],
    ] as [col, lbl]}
      <div class="legend-row">
        <span class="sw" style="background:{col}"></span>
        <span class="legend-cls">{lbl}</span>
      </div>
    {/each}
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
    {@const range = flowRanges.get(layer.datasetKey)}
    {#if range}
      <div class="flow-guide">
        <span style="height:1px; width:8px; background:{ds.colour}; opacity:.4;
                     display:inline-block; margin-right:3px"></span>
        <span>{fmt(range.min)}</span>
        <span style="flex:1; min-width:8px"></span>
        <span style="height:5px; width:16px; background:{ds.colour};
                     display:inline-block; margin-right:3px"></span>
        <span>{fmt(range.max)}</span>
      </div>
    {/if}
  {/each}

</div>
{/if}

<style>
  /* ── Map ──────────────────────────────────────────────────────────────────── */
  .map { position: fixed; inset: 0; width: 100vw; height: 100vh; }

  /* ── Header ───────────────────────────────────────────────────────────────── */
  .header {
    position: fixed; top: 0; left: 0; right: 0; z-index: 1000; height: 46px;
    background: #fff; border-bottom: 1px solid #e4e2de;
    box-shadow: 0 1px 4px rgba(0,0,0,0.07);
    display: flex; align-items: center; padding: 0 0.75rem; gap: 1rem;
    font-family: sans-serif;
  }
  .header-toggle {
    background: none; border: none; cursor: pointer;
    font-size: 0.7rem; color: #888; padding: 0.2rem 0.4rem;
    border-radius: 4px; line-height: 1;
  }
  .header-toggle:hover { background: #f0ede8; }
  .app-title { font-weight: 700; font-size: 0.88rem; color: #1a1a2e; white-space: nowrap; }
  .tab-bar   { display: flex; gap: 0.2rem; }
  .tab {
    display: flex; align-items: center; gap: 0.3rem;
    padding: 0.25rem 0.65rem; border: 1px solid transparent; border-radius: 5px;
    background: none; cursor: pointer; font-size: 0.8rem; font-weight: 500; color: #666;
    transition: background 0.1s, border-color 0.1s;
  }
  .tab:hover        { background: #f4f1ec; }
  .tab.tab-active   { background: #f4f1ec; border-color: #d4d0c8; color: #1a1a2e; }
  .dot              { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
  .status           { font-size: 0.7rem; padding: 0.15rem 0.45rem; border-radius: 8px; font-weight: 500; }
  .status.loading   { background: #e8f0fd; color: #1a56c4; }
  .status.err       { background: #fdecea; color: #c62828; }

  /* ── Panel ────────────────────────────────────────────────────────────────── */
  .panel {
    position: fixed; top: 54px; left: 0.75rem; z-index: 999; width: 288px;
    max-height: calc(100vh - 70px); overflow-y: auto;
    background: #fff; border-radius: 10px;
    box-shadow: 0 2px 14px rgba(0,0,0,0.12);
    padding: 0.9rem; font-family: sans-serif; font-size: 0.82rem;
    display: flex; flex-direction: column; gap: 0.85rem;
  }

  /* ── Fields & chips ───────────────────────────────────────────────────────── */
  .field   { display: flex; flex-direction: column; gap: 0.3rem; }
  .flbl    { font-size: 0.68rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em; color: #999; }
  select   { width: 100%; padding: 0.3rem 0.45rem; border: 1px solid #ddd; border-radius: 6px; font-size: 0.8rem; background: #fff; color: #222; }
  .chips   { display: flex; flex-wrap: wrap; gap: 0.28rem; }
  .chip    { padding: 0.18rem 0.55rem; border: 1px solid #ccc; border-radius: 20px; background: #fff; font-size: 0.75rem; color: #555; cursor: pointer; transition: background 0.1s, border-color 0.1s, color 0.1s; }
  .chip:hover:not(.chip-off) { background: #f0ede8; border-color: #aaa; }
  .chip-on  { background: #1a1a2e; border-color: #1a1a2e; color: #fff; }
  .chip-off { opacity: 0.28; cursor: not-allowed; }
  .row          { display: flex; align-items: center; }
  .toggle-row   { gap: 0.5rem; cursor: pointer; }
  .toggle-lbl   { font-size: 0.82rem; }

  /* ── Edge cards ───────────────────────────────────────────────────────────── */
  .edge-card    { border: 1px solid #eee; border-radius: 8px; padding: 0.6rem 0.7rem; display: flex; flex-direction: column; gap: 0.45rem; }
  .edge-card.edge-on { border-color: #ccc; }
  .edge-dot     { width: 9px; height: 9px; border-radius: 50%; margin: 0 2px; }
  .hint         { margin: 0; font-size: 0.72rem; color: #999; line-height: 1.35; }

  /* ── Calculator ───────────────────────────────────────────────────────────── */
  .calc-preview  { font-size: 0.78rem; color: #238b45; font-style: italic; margin: 0; word-break: break-word; }
  .calc-warn     { font-size: 0.78rem; color: #c62828; margin: 0; }
  .calc-term     { display: flex; flex-direction: column; gap: 0.3rem; background: #f9f8f6; border-radius: 6px; padding: 0.45rem 0.5rem; border: 1px solid #eee; }
  .calc-term-type { display: flex; gap: 0.25rem; align-items: center; }
  .chip-sm       { padding: 0.1rem 0.4rem; font-size: 0.7rem; }
  .calc-remove   { margin-left: auto; background: none; border: none; color: #c00; cursor: pointer; font-size: 0.75rem; padding: 0 0.2rem; }
  .calc-op-row   { display: flex; gap: 0.25rem; justify-content: center; padding: 0.1rem 0; }
  .calc-add      { background: none; border: 1px dashed #bbb; border-radius: 6px; color: #666; cursor: pointer; font-size: 0.76rem; padding: 0.3rem; width: 100%; transition: background 0.1s; }
  .calc-add:hover { background: #f4f1ec; }
  .calc-const-input { width: 100%; padding: 0.28rem 0.45rem; border: 1px solid #ddd; border-radius: 6px; font-size: 0.8rem; background: #fff; }

  /* ── Legend ───────────────────────────────────────────────────────────────── */
  .legend       { position: fixed; bottom: 1.5rem; left: 0.75rem; z-index: 998; background: #fff; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.13); padding: 0.65rem 0.85rem; font-family: sans-serif; font-size: 0.76rem; min-width: 130px; max-width: 210px; }
  .legend-title { font-weight: 700; font-size: 0.78rem; color: #1a1a2e; margin-bottom: 0.3rem; line-height: 1.3; }
  .legend-sub   { font-size: 0.68rem; color: #999; margin-bottom: 0.25rem; }
  .legend-edge-lbl { font-size: 0.74rem; }
  .legend-row   { display: flex; align-items: center; gap: 0.38rem; margin: 0.12rem 0; }
  .sw           { width: 11px; height: 11px; border-radius: 2px; border: 1px solid rgba(0,0,0,0.08); flex-shrink: 0; }
  .divider      { border-top: 1px solid #eee; margin: 0.4rem 0; }
  .flow-guide   { display: flex; align-items: center; gap: 2px; font-size: 0.68rem; color: #888; margin-top: 0.2rem; padding-left: 2px; }
  .legend-cls   { font-size: 0.74rem; color: #333; line-height: 1.3; }

  /* ── Popup ────────────────────────────────────────────────────────────────── */
  .map-popup    { position: fixed; z-index: 1100; background: #fff; border-radius: 8px; box-shadow: 0 3px 14px rgba(0,0,0,0.2); padding: 0.6rem 0.8rem; font-family: sans-serif; font-size: 0.78rem; min-width: 160px; max-width: 240px; pointer-events: auto; transform: translate(-50%, -110%); }
  .popup-title  { font-weight: 700; font-size: 0.8rem; color: #1a1a2e; margin-bottom: 0.35rem; display: flex; justify-content: space-between; }
  .popup-close  { color: #aaa; cursor: pointer; font-size: 0.7rem; }
  .popup-row    { display: flex; justify-content: space-between; gap: 0.5rem; padding: 0.1rem 0; border-bottom: 1px solid #f5f5f5; }
  .popup-label  { color: #888; }
  .popup-value  { font-weight: 500; color: #1a1a2e; }

  /* ── Model ────────────────────────────────────────────────────────────────── */
  .tab-section-header { display: flex; align-items: center; justify-content: space-between; gap: 0.5rem; }
  .collapse-btn { background: none; border: none; cursor: pointer; font-size: 0.65rem; color: #aaa; padding: 0.1rem 0.3rem; border-radius: 3px; flex-shrink: 0; }
  .collapse-btn:hover { background: #f0ede8; color: #666; }
  .model-run-btn { background: #7b2d8b; color: white; border: none; border-radius: 6px; padding: 0.5rem 1rem; font-size: 0.82rem; font-weight: 600; cursor: pointer; width: 100%; transition: background 0.1s; }
  .model-run-btn:hover:not(:disabled) { background: #6a2478; }
  .model-run-btn:disabled { opacity: 0.5; cursor: not-allowed; }
  .model-results { display: flex; flex-direction: column; gap: 0.5rem; border-top: 1px solid #eee; padding-top: 0.5rem; }
  .model-stat-row { display: flex; gap: 0.75rem; font-size: 0.78rem; }
  .model-stat { color: #666; }
  .model-stat strong { color: #1a1a2e; }
  .model-table { width: 100%; font-size: 0.72rem; border-collapse: collapse; }
  .model-table th { text-align: left; font-weight: 600; color: #888; border-bottom: 1px solid #eee; padding: 0.2rem 0.3rem; font-size: 0.68rem; text-transform: uppercase; }
  .model-table td { padding: 0.18rem 0.3rem; border-bottom: 1px solid #f5f5f5; }
  .model-table tr.high-vif { background: #fff8f0; }
  .model-table td.sig { color: #1a6e2e; font-weight: 600; }
</style>