// =============================================================================
// config.ts
// Central registry of scales, variables, normalisations, colours, edge datasets
//
// Column names verified against actual parquet files 2024-04-20:
//   grid/pc4        : aantal_mannen, aantal_inwoners_X_tot_Y_jaar, aantal_part_huishoudens
//   buurt/wijk/gem  : mannen, percentage_personen_X, aantal_huishoudens
//
// To add a new scale:
//   1. Run R pipeline → {scale}.geojson + {scale}_stats.parquet in static/data/
//   2. Add entry to INNER_SCALES or OUTER_SCALES
//   3. Add variables with correct column names and scale key in availableAt
// =============================================================================


import type { Scale, Variable, EdgeDataset } from './types';

// ── Spatial scales ────────────────────────────────────────────────────────────

export const INNER_SCALES: Scale[] = [
  {
    key: '100m', label: '100m grid',
    geojson: 'grid_100m_rijnmond.geojson', stats: 'grid_100m_rijnmond_stats.parquet',
    id: 'crs28992res100m', type: 'point', pointSize: 60,
  },
  {
    key: '500m', label: '500m grid',
    geojson: 'grid_500m_rijnmond.geojson', stats: 'grid_500m_rijnmond_stats.parquet',
    id: 'crs28992res500m', type: 'point', pointSize: 300,
  },
  {
    key: 'buurt', label: 'Buurt',
    geojson: 'buurt_2024.geojson', stats: 'buurt_2024_stats.parquet',
    id: 'buurtcode', type: 'polygon',
  },
];

export const OUTER_SCALES: Scale[] = [
  {
    key: 'pc4', label: 'PC4',
    geojson: 'pc4_zh_2024.geojson', stats: 'pc4_zh_2024_stats.parquet',
    id: 'postcode', type: 'polygon',
  },
  {
    key: 'wijk', label: 'Wijk',
    geojson: 'wijk_2024.geojson', stats: 'wijk_2024_stats.parquet',
    id: 'wijkcode', type: 'polygon',
  },
  {
    key: 'gemeente', label: 'Gemeente',
    geojson: 'gemeente_2024.geojson', stats: 'gemeente_2024_stats.parquet',
    id: 'gemeentecode', type: 'polygon',
  },
];

export const ALL_SCALES: Scale[] = [...INNER_SCALES, ...OUTER_SCALES];

// ── Variables ─────────────────────────────────────────────────────────────────

export const VARIABLES: Variable[] = [

  // ── Population ─────────────────────────────────────────────
  { key: 'aantal_inwoners', label: 'Total population', group: 'Population',
    canNormalise: true, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },

  { key: 'aantal_mannen', label: 'Men', group: 'Population',
    canNormalise: true, availableAt: ['100m','500m','pc4'] },
  { key: 'mannen', label: 'Men', group: 'Population',
    canNormalise: true, availableAt: ['buurt','wijk','gemeente'] },
  { key: 'aantal_vrouwen', label: 'Women', group: 'Population',
    canNormalise: true, availableAt: ['100m','500m','pc4'] },
  { key: 'vrouwen', label: 'Women', group: 'Population',
    canNormalise: true, availableAt: ['buurt','wijk','gemeente'] },

  { key: 'aantal_inwoners_0_tot_15_jaar',    label: 'Age 0-15',  group: 'Population',
    canNormalise: true, availableAt: ['100m','500m','pc4'] },
  { key: 'aantal_inwoners_15_tot_25_jaar',   label: 'Age 15-25', group: 'Population',
    canNormalise: true, availableAt: ['100m','500m','pc4'] },
  { key: 'aantal_inwoners_25_tot_45_jaar',   label: 'Age 25-45', group: 'Population',
    canNormalise: true, availableAt: ['100m','500m','pc4'] },
  { key: 'aantal_inwoners_45_tot_65_jaar',   label: 'Age 45-65', group: 'Population',
    canNormalise: true, availableAt: ['100m','500m','pc4'] },
  { key: 'aantal_inwoners_65_jaar_en_ouder', label: 'Age 65+',   group: 'Population',
    canNormalise: true, availableAt: ['100m','500m','pc4'] },

  { key: 'percentage_personen_0_tot_15_jaar',    label: 'Age 0-15 (%)',  group: 'Population',
    canNormalise: false, availableAt: ['buurt','wijk','gemeente'] },
  { key: 'percentage_personen_15_tot_25_jaar',   label: 'Age 15-25 (%)', group: 'Population',
    canNormalise: false, availableAt: ['buurt','wijk','gemeente'] },
  { key: 'percentage_personen_25_tot_45_jaar',   label: 'Age 25-45 (%)', group: 'Population',
    canNormalise: false, availableAt: ['buurt','wijk','gemeente'] },
  { key: 'percentage_personen_45_tot_65_jaar',   label: 'Age 45-65 (%)', group: 'Population',
    canNormalise: false, availableAt: ['buurt','wijk','gemeente'] },
  { key: 'percentage_personen_65_jaar_en_ouder', label: 'Age 65+ (%)',   group: 'Population',
    canNormalise: false, availableAt: ['buurt','wijk','gemeente'] },

  // ── Origin ─────────────────────────────────────────────────
  { key: 'percentage_geb_nederland_herkomst_nederland',
    label: '% Dutch origin', group: 'Origin',
    canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
  { key: 'percentage_geb_nederland_herkomst_overig_europa',
    label: '% European (NL-born)', group: 'Origin',
    canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
  { key: 'percentage_geb_nederland_herkomst_buiten_europa',
    label: '% Non-European (NL-born)', group: 'Origin',
    canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
  { key: 'percentage_geb_buiten_nederland_herkomst_europa',
    label: '% European (foreign-born)', group: 'Origin',
    canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
  { key: 'percentage_geb_buiten_nederland_herkmst_buiten_europa',
    label: '% Non-European (foreign-born)', group: 'Origin',
    canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },

  // ── Households ─────────────────────────────────────────────
  { key: 'aantal_part_huishoudens', label: 'Total households', group: 'Households',
    canNormalise: true, availableAt: ['100m','500m','pc4'] },
  { key: 'aantal_huishoudens', label: 'Total households', group: 'Households',
    canNormalise: true, availableAt: ['buurt','wijk','gemeente'] },

  { key: 'aantal_eenpersoonshuishoudens',     label: 'Single-person households',     group: 'Households',
    canNormalise: true,  availableAt: ['100m','500m','pc4'] },
  { key: 'percentage_eenpersoonshuishoudens', label: 'Single-person households (%)', group: 'Households',
    canNormalise: false, availableAt: ['buurt','wijk','gemeente'] },

  { key: 'gemiddelde_huishoudensgrootte', label: 'Avg household size', group: 'Households',
    canNormalise: false, availableAt: ['100m','500m','pc4'] },
  { key: 'gemiddelde_huishoudsgrootte',   label: 'Avg household size', group: 'Households',
    canNormalise: false, availableAt: ['buurt','wijk','gemeente'] },

  // ── Housing ────────────────────────────────────────────────
  { key: 'aantal_woningen', label: 'Total dwellings', group: 'Housing',
    canNormalise: true, availableAt: ['100m','500m','pc4'] },
  { key: 'woningvoorraad',  label: 'Total dwellings', group: 'Housing',
    canNormalise: true, availableAt: ['buurt','wijk','gemeente'] },

  { key: 'percentage_koopwoningen', label: '% Owner-occupied', group: 'Housing',
    canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },
  { key: 'percentage_huurwoningen', label: '% Rental', group: 'Housing',
    canNormalise: false, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },

  { key: 'aantal_huurwoningen_in_bezit_woningcorporaties', label: 'Social housing units', group: 'Housing',
    canNormalise: true,  availableAt: ['100m','500m','pc4'] },
  { key: 'perc_huurwoningen_in_bezit_woningcorporaties',   label: 'Social housing (%)', group: 'Housing',
    canNormalise: false, availableAt: ['buurt','wijk','gemeente'] },

  { key: 'gemiddelde_woz_waarde_woning', label: 'Avg property value (WOZ)', group: 'Housing',
    canNormalise: false, availableAt: ['pc4'] },
  { key: 'gemiddelde_woningwaarde',      label: 'Avg property value (WOZ)', group: 'Housing',
    canNormalise: false, availableAt: ['buurt','wijk','gemeente'] },

  // ── Income ─────────────────────────────────────────────────
  { key: 'aantal_personen_met_uitkering_onder_aowlft', label: 'Persons on benefits', group: 'Income',
    canNormalise: true, availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'] },

  // ── Area ───────────────────────────────────────────────────
  { key: 'oppervlakte_land_in_ha', label: 'Land area (ha)', group: 'Area',
    canNormalise: false, availableAt: ['buurt','wijk','gemeente','pc4'] },

  // ── Commuting (from flow_summary.parquet) ──────────────────
  { key: 'total_outflow',  label: 'Total commuters (outflow)', group: 'Commuting',
    canNormalise: true,  availableAt: ['gemeente'] },
  { key: 'n_destinations', label: 'No. of destinations', group: 'Commuting',
    canNormalise: false, availableAt: ['gemeente'] },

  // ── Banen (from nodes_summary_gem.parquet) ─────────────────
  { key: 'total_banen_werk',         label: 'Jobs at work location',       group: 'Banen',
    canNormalise: true,  availableAt: ['gemeente'] },
  { key: 'total_banen_woon',         label: 'Employed residents',           group: 'Banen',
    canNormalise: true,  availableAt: ['gemeente'] },
  { key: 'total_inwoners',           label: 'Total population (CBS nodes)', group: 'Banen',
    canNormalise: false, availableAt: ['gemeente'] },
  { key: 'ratio_banen_inwoners',     label: 'Jobs / residents ratio',       group: 'Banen',
    canNormalise: false, availableAt: ['gemeente'] },
  { key: 'ratio_werkenden_inwoners', label: 'Employment rate (proxy)',       group: 'Banen',
    canNormalise: false, availableAt: ['gemeente'] },
];

// ── Edge datasets ─────────────────────────────────────────────────────────────

export const EDGE_DATASETS: EdgeDataset[] = [
  {
    key:         'commuters',
    label:       'Commuting flows (NL, 2022)',
    flows:       'flows.parquet',
    flowSummary: 'flow_summary.parquet',
    scaleKey:    'gemeente',
  },
  {
    key:         'woonwerk',
    label:       'Woon-werk flows (2007-2017)',
    flows:       'edges_woonwerk_gem.parquet',
    flowSummary: 'edges_woonwerk_summary_gem.parquet',
    scaleKey:    'gemeente',
  },
];

// ── Display options ───────────────────────────────────────────────────────────

export const NORMALISATIONS = [
  { key: 'none',     label: 'Raw value' },
  { key: 'per_km2',  label: 'Per km2'   },
  { key: 'per_1000', label: 'Per 1,000 residents' },
] as const;

export const DISPLAY_MODES = [
  { key: 'both',  label: 'Inner + Outer' },
  { key: 'inner', label: 'Inner only'    },
  { key: 'outer', label: 'Outer only'    },
] as const;

export const COLOURS = ['#f1eef6', '#bdc9e1', '#74a9cf', '#045a8d'];
export const NO_DATA = '#d0d0d0';

// ── Helpers ───────────────────────────────────────────────────────────────────

export function varsForScale(scaleKey: string): Variable[] {
  return VARIABLES.filter(v => v.availableAt.includes(scaleKey));
}

export function findScale(key: string): Scale | undefined {
  return ALL_SCALES.find(s => s.key === key);
}