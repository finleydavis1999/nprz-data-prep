// =============================================================================
// map.ts
//
// MapLibre GL JS initialisation and all layer management helpers.
//
// Responsibilities:
//   - Create the map instance and register sources + layers on 'load'
//   - Provide helpers for setting layer visibility, colours, and feature states
//   - Implement inner/outer boundary clip expressions (used in "both" extent mode)
//   - Register SDF icons (grid square, flow arrow)
//
// The Map instance is created once and stored in a module-level variable so that
// all helpers can reference it without it being passed as a parameter on every call.
// Call initMap(container) once from the Svelte use: directive.
// =============================================================================

import maplibregl from 'maplibre-gl';
import type { FeatureCollection } from 'geojson';

import {
  ALL_SCALES, INNER_SCALES, OUTER_SCALES, EDGE_DATASETS,
  INNER_GEMEENTE_CODES, INNER_GM_NUMS, INNER_PC4_CODES,
} from './config';
import { COLOURS_BLUE, NO_DATA } from './config';

// ── Module-level map handle ───────────────────────────────────────────────────
// Exported so that page.svelte and other modules can reference the map instance
// without threading it through every function call.
export let map: maplibregl.Map | null = null;

// ── Colour expression ─────────────────────────────────────────────────────────

/**
 * Build a MapLibre paint expression that maps feature state `cls` (0–3) to colours.
 * cls = -1 → NO_DATA grey.  cls = 0..3 → colour ramp entries.
 * Cast to `any` because MapLibre's TS types are very strict about expression arrays
 * but the runtime handles this correctly.
 */
export function colourExpr(colours: string[]): any {
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

// ── Layer visibility / paint helpers ─────────────────────────────────────────

/** Show or hide a MapLibre layer by ID. Safe to call even if layer doesn't exist. */
export function setLayerVis(id: string, vis: 'visible' | 'none'): void {
  if (map?.getLayer(id)) map.setLayoutProperty(id, 'visibility', vis);
}

/**
 * Update fill/icon/circle colour and opacity for a scale's fill layer.
 * Handles the three possible layer types:
 *   - 'symbol'  → grid layers (SDF square icon), uses icon-color / icon-opacity
 *   - 'circle'  → legacy fallback
 *   - 'fill'    → standard polygon choropleth
 */
export function setColours(scaleKey: string, colours: string[], opacity: number): void {
  const fillId = `${scaleKey}-fill`;
  const layer  = map?.getLayer(fillId);
  if (!layer) return;

  if (layer.type === 'symbol') {
    map!.setPaintProperty(fillId, 'icon-color',   colourExpr(colours));
    map!.setPaintProperty(fillId, 'icon-opacity', opacity);
  } else if (layer.type === 'circle') {
    map!.setPaintProperty(fillId, 'circle-color',   colourExpr(colours));
    map!.setPaintProperty(fillId, 'circle-opacity', opacity);
  } else {
    map!.setPaintProperty(fillId, 'fill-color',   colourExpr(colours));
    map!.setPaintProperty(fillId, 'fill-opacity', opacity);
  }
}

// ── Boundary clip expressions ─────────────────────────────────────────────────
// Used in "both" extent mode to prevent inner and outer layers from overlapping.

/**
 * Clip a scale's fill + outline layers so that only features WITHIN the
 * inner boundary are visible. Used when an outer-type scale (PC4, Wijk, Gemeente)
 * is selected as the study-area scale in "both" mode.
 */
export function clipToInnerBoundary(scaleKey: string): void {
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
    const f = ['in', ['get', 'postcode'], ['literal', INNER_PC4_CODES]];
    map.setFilter(fillId,    f as any);
    map.setFilter(outlineId, f as any);
  }
}

/**
 * Remove any MapLibre feature filter from a scale's layers.
 * Called when switching away from "both" mode or changing scales.
 */
export function clearBoundaryClip(scaleKey: string): void {
  const fillId    = `${scaleKey}-fill`;
  const outlineId = `${scaleKey}-outline`;
  if (map?.getLayer(fillId))    map.setFilter(fillId,    null);
  if (map?.getLayer(outlineId)) map.setFilter(outlineId, null);
}

/**
 * Clip a scale's fill + outline layers to EXCLUDE the inner boundary area.
 * Used for the outer scale in "both" mode so outer data doesn't bleed into
 * the study area — the inverse of clipToInnerBoundary().
 */
export function clipOuterToExcludeInner(scaleKey: string): void {
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

// ── Map initialisation ────────────────────────────────────────────────────────

/**
 * Initialise the MapLibre map instance.
 * Call this from a Svelte `use:` directive: `<div use:initMap>`.
 *
 * Registers all sources, layers, icons, and click/hover handlers.
 * Calls `onReady()` once the map 'load' event fires.
 *
 * @param container  The DOM element to render the map into
 * @param onReady    Callback invoked when the map is fully loaded
 * @param onAreaClick   Callback for choropleth polygon clicks
 * @param onFlowClick   Callback for flow line clicks
 */
export function initMap(
  container: HTMLDivElement,
  onReady: () => void,
  onAreaClick: (info: { title: string; id: string; cls: number }) => void,
  onFlowClick: (props: Record<string, any>) => void,
): void {
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
    _registerSources();
    _registerIcons();
    _registerLayers();
    _registerInteractions(onAreaClick, onFlowClick);
    onReady();
  });
}

// ── Private helpers (called only from initMap) ────────────────────────────────

/** Register a GeoJSON source for every scale + the boundary and donut layers. */
function _registerSources(): void {
  for (const scale of ALL_SCALES) {
    map!.addSource(`${scale.key}-source`, {
      type: 'geojson',
      data: new URL(`/data/${scale.geojson}`, window.location.origin).href,
      promoteId: scale.id, // preserves feature ID type (string for BU/GM, number for PC4)
    });
  }

  map!.addSource('boundary-source', {
    type: 'geojson',
    data: new URL('/data/rotterdam_boundary.geojson', window.location.origin).href,
  });

  // Donut polygon = middle boundary minus inner boundary.
  // Intended for future use when clipping outer fill layers in "both" mode.
  map!.addSource('outer-donut-source', {
    type: 'geojson',
    data: new URL('/data/outer_donut.geojson', window.location.origin).href,
  });
}

/**
 * Register SDF icons used by grid and flow arrow layers.
 *
 * SDF (signed distance field) icons are white masks that MapLibre recolours
 * via icon-color — this is how the grid choropleth works.
 *
 * Grid square: 62×62 fill inside 64×64 canvas → 1px gap at exact tiling
 * Arrow:       simple right-pointing triangle for flow direction markers
 */
function _registerIcons(): void {
  // Grid square
  const sqSize   = 64;
  const sqCanvas = document.createElement('canvas');
  sqCanvas.width = sqSize; sqCanvas.height = sqSize;
  const sqCtx    = sqCanvas.getContext('2d')!;
  sqCtx.clearRect(0, 0, sqSize, sqSize);
  sqCtx.fillStyle = '#ffffff';
  sqCtx.fillRect(1, 1, sqSize - 2, sqSize - 2); // 1px inset = grid-line gap
  map!.addImage('grid-square', sqCtx.getImageData(0, 0, sqSize, sqSize), { sdf: true });

  // Flow direction arrow
  const arrowSize   = 16;
  const arrowCanvas = document.createElement('canvas');
  arrowCanvas.width = arrowSize; arrowCanvas.height = arrowSize;
  const arrowCtx    = arrowCanvas.getContext('2d')!;
  arrowCtx.fillStyle = '#ffffff';
  arrowCtx.beginPath();
  arrowCtx.moveTo(arrowSize, arrowSize / 2); // tip (right)
  arrowCtx.lineTo(0, 0);                      // top-left
  arrowCtx.lineTo(0, arrowSize);              // bottom-left
  arrowCtx.closePath();
  arrowCtx.fill();
  map!.addImage('arrow-icon', arrowCtx.getImageData(0, 0, arrowSize, arrowSize), { sdf: true });
}

/**
 * Register all choropleth fill/outline layers (initially hidden).
 *
 * Grid layers use 'symbol' type with the SDF square icon so that their colour
 * can be driven by feature state via icon-color (fill layers don't support
 * feature-state-driven colour on point sources).
 *
 * icon-size values computed from metres-per-pixel at lat 51.9° (Rotterdam):
 *   100m: z10=0.029, z12=0.117, z14=0.467, z16=1.866
 *   500m: z10=0.146, z12=0.583, z14=2.332
 */
function _registerLayers(): void {
  // Outer polygon scales (PC4, Wijk, Gemeente)
  for (const scale of OUTER_SCALES) {
    map!.addLayer({
      id:     `${scale.key}-fill`,
      type:   'fill',
      source: `${scale.key}-source`,
      layout: { visibility: 'none' },
      paint:  { 'fill-color': colourExpr(COLOURS_BLUE), 'fill-opacity': 0.72 },
    });
    map!.addLayer({
      id:     `${scale.key}-outline`,
      type:   'line',
      source: `${scale.key}-source`,
      layout: { visibility: 'none' },
      paint:  { 'line-color': '#ffffff', 'line-width': 0.5 },
    });
  }

  // Inner mask placeholder (currently disabled — clip approach used instead)
  map!.addLayer({
    id:     'inner-mask',
    type:   'fill',
    source: 'boundary-source',
    layout: { visibility: 'none' },
    paint:  { 'fill-color': '#ffffff', 'fill-opacity': 0 },
  });

  // Inner scales (100m grid, 500m grid, Buurt)
  for (const scale of INNER_SCALES) {
    if (scale.type === 'point') {
      // Grid layer: symbol layer with SDF square icon
      map!.addLayer({
        id:     `${scale.key}-fill`,
        type:   'symbol',
        source: `${scale.key}-source`,
        layout: {
          visibility:                'none',
          'icon-image':              'grid-square',
          'icon-allow-overlap':      true,
          'icon-ignore-placement':   true,
          'icon-pitch-alignment':    'map',
          'icon-rotation-alignment': 'map',
          'icon-size': scale.key === '100m'
            ? ['interpolate', ['exponential', 2], ['zoom'],
                10, 0.029, 11, 0.058, 12, 0.117, 13, 0.233, 14, 0.467, 15, 0.933, 16, 1.866]
            : ['interpolate', ['exponential', 2], ['zoom'],
                9,  0.073, 10, 0.146, 11, 0.292, 12, 0.583, 13, 1.166, 14, 2.332, 15, 4.665],
        },
        paint: {
          'icon-color':   colourExpr(COLOURS_BLUE),
          'icon-opacity': 0.72,
        },
      });
    } else {
      // Polygon layer (Buurt)
      map!.addLayer({
        id:     `${scale.key}-fill`,
        type:   'fill',
        source: `${scale.key}-source`,
        layout: { visibility: 'none' },
        paint:  { 'fill-color': colourExpr(COLOURS_BLUE), 'fill-opacity': 0.72 },
      });
      map!.addLayer({
        id:     `${scale.key}-outline`,
        type:   'line',
        source: `${scale.key}-source`,
        layout: { visibility: 'none' },
        paint:  { 'line-color': '#ffffff', 'line-width': 0.4 },
      });
    }
  }

  // Study area boundary — always visible, rendered on top
  map!.addLayer({
    id:     'boundary-line',
    type:   'line',
    source: 'boundary-source',
    paint:  { 'line-color': '#e63946', 'line-width': 2, 'line-dasharray': [4, 2] },
  });
}

/**
 * Register click and hover handlers.
 * Choropleth clicks emit area info; flow line clicks emit OD info.
 * Cursor changes to pointer on any clickable feature.
 */
function _registerInteractions(
  onAreaClick: (info: { title: string; id: string; cls: number }) => void,
  onFlowClick: (props: Record<string, any>) => void,
): void {
  const choroplethLayers = ALL_SCALES.map(s => `${s.key}-fill`);
  const flowLineLayers   = EDGE_DATASETS.map(d => `flows-${d.key}-layer`);
  const allClickable     = [...choroplethLayers, ...flowLineLayers,
                             ...EDGE_DATASETS.map(d => `flows-${d.key}-arrows`)];

  // Choropleth click
  map!.on('click', (e) => {
    const visibleChoropleth = choroplethLayers.filter(id =>
      map!.getLayer(id) && map!.getLayoutProperty(id, 'visibility') === 'visible'
    );
    if (!visibleChoropleth.length) return;
    const features = map!.queryRenderedFeatures(e.point, { layers: visibleChoropleth });
    if (!features.length) { onAreaClick({ title: '', id: '', cls: -2 }); return; }
    const f     = features[0];
    const scale = ALL_SCALES.find(s => `${s.key}-fill` === f.layer.id);
    onAreaClick({
      title: scale?.label ?? 'Area',
      id:    String(f.id),
      cls:   f.state?.cls ?? -1,
    });
  });

  // Flow line click
  map!.on('click', (e) => {
    const visibleFlows = flowLineLayers.filter(id =>
      map!.getLayer(id) && map!.getLayoutProperty(id, 'visibility') === 'visible'
    );
    if (!visibleFlows.length) return;
    const features = map!.queryRenderedFeatures(e.point, { layers: visibleFlows });
    if (!features.length) return;
    onFlowClick(features[0].properties ?? {});
  });

  // Hover cursor
  map!.on('mousemove', (e) => {
    const existing = allClickable.filter(id => map!.getLayer(id));
    const hit = map!.queryRenderedFeatures(e.point, { layers: existing });
    map!.getCanvas().style.cursor = hit.length ? 'pointer' : '';
  });
}