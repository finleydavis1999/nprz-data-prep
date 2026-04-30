// =============================================================================
// config.ts — verified against actual parquet column names April 2026
//
// Column name sources confirmed by glimpse() on exported parquets:
//   grid/pc4    : lowercase CBS grid names (aantal_mannen, etc.)
//   buurt       : lowercase CBS names (mannen, percentage_personen_X, etc.)
//   wijk/gem    : lowercase CBS names PLUS CamelCase_N duplicates (stripped in R)
//                 Origin columns differ: perc_geb_in_nl_... / perc_geb_buiten_nl_...
//   PC4 postcode: numeric (dbl) — promoteId gives MapLibre a number
//   Grid IDs    : string (chr) — "E0975N4520"
//   Buurt IDs   : string (chr) — "BU04890101"
//   Wijk IDs    : string (chr) — "WK048901"
//   Gemeente IDs: string (chr) — stored as WijkenEnBuurten in kwb_raw, exported as gemeentecode
// =============================================================================

import type { Scale, Variable, EdgeDataset } from './types';

// ── Scales ────────────────────────────────────────────────────────────────────

export const INNER_SCALES: Scale[] = [
  {
    key: '100m', label: '100m grid',
    geojson: 'grid_100m_rijnmond.geojson',
    stats:   'grid_100m_rijnmond_stats.parquet',
    id: 'crs28992res100m', type: 'point', pointSize: 60,
  },
  {
    key: '500m', label: '500m grid',
    geojson: 'grid_500m_rijnmond.geojson',
    stats:   'grid_500m_rijnmond_stats.parquet',
    id: 'crs28992res500m', type: 'point', pointSize: 300,
  },
  {
    key: 'buurt', label: 'Buurt',
    geojson: 'buurt_2024.geojson',
    stats:   'buurt_2024_stats.parquet',
    id: 'buurtcode', type: 'polygon',
  },
];

export const OUTER_SCALES: Scale[] = [
  {
    key: 'pc4', label: 'PC4',
    geojson: 'pc4_zh_2024.geojson',
    stats:   'pc4_zh_2024_stats.parquet',
    id: 'postcode', type: 'polygon',
  },
  {
    key: 'wijk', label: 'Wijk',
    geojson: 'wijk_2024.geojson',
    stats:   'wijk_2024_stats.parquet',
    id: 'wijkcode', type: 'polygon',
  },
  {
    key: 'gemeente', label: 'Gemeente',
    geojson: 'gemeente_2024.geojson',
    stats:   'gemeente_2024_stats.parquet',
    id: 'gemeentecode', type: 'polygon',
  },
];

export const ALL_SCALES: Scale[] = [...INNER_SCALES, ...OUTER_SCALES];

// ── Colour ramps ──────────────────────────────────────────────────────────────
export const COLOURS_BLUE   = ['#f1eef6', '#bdc9e1', '#74a9cf', '#045a8d'];
export const COLOURS_ORANGE = ['#fef0d9', '#fdcc8a', '#fc8d59', '#b30000']; // reserved
export const COLOURS_GREEN  = ['#edf8e9', '#bae4b3', '#74c476', '#238b45'];
export const NO_DATA        = '#d0ccc5';

// ── Variables ─────────────────────────────────────────────────────────────────
// Each entry covers all scales it's available at.
// columnAt maps scale key → actual parquet column name.
// Falls back to variable key if scale absent (should not happen for CBS vars).
//
// Shorthand helpers for repeated patterns:
const gridPc4  = (col: string) => Object.fromEntries(['100m','500m','pc4'].map(s => [s, col]));
const adminAll = (col: string) => Object.fromEntries(['buurt','wijk','gemeente'].map(s => [s, col]));
const allSix   = (col: string) => Object.fromEntries(['100m','500m','buurt','wijk','gemeente','pc4'].map(s => [s, col]));

export const VARIABLES: Variable[] = [

  // ── Population ─────────────────────────────────────────────
  // aantal_inwoners is the same column name at ALL scales
  {
    key: 'total_population', label: 'Total population', group: 'Population',
    canNormalise: true,
    availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'],
    columnAt: allSix('aantal_inwoners'),
  },
  {
    key: 'men', label: 'Men', group: 'Population',
    canNormalise: true,
    availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'],
    columnAt: { ...gridPc4('aantal_mannen'), ...adminAll('mannen') },
  },
  {
    key: 'women', label: 'Women', group: 'Population',
    canNormalise: true,
    availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'],
    columnAt: { ...gridPc4('aantal_vrouwen'), ...adminAll('vrouwen') },
  },

  // Age bands — grid/pc4: absolute counts | buurt/wijk/gemeente: percentages
  {
    key: 'age_0_15', label: 'Age 0–15 (n)', group: 'Population — age',
    canNormalise: true,
    availableAt: ['100m','500m','pc4'],
    columnAt: gridPc4('aantal_inwoners_0_tot_15_jaar'),
  },
  {
    key: 'age_0_15_pct', label: 'Age 0–15 (%)', group: 'Population — age',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente'],
    columnAt: adminAll('percentage_personen_0_tot_15_jaar'),
  },
  {
    key: 'age_15_25', label: 'Age 15–25 (n)', group: 'Population — age',
    canNormalise: true,
    availableAt: ['100m','500m','pc4'],
    columnAt: gridPc4('aantal_inwoners_15_tot_25_jaar'),
  },
  {
    key: 'age_15_25_pct', label: 'Age 15–25 (%)', group: 'Population — age',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente'],
    columnAt: adminAll('percentage_personen_15_tot_25_jaar'),
  },
  {
    key: 'age_25_45', label: 'Age 25–45 (n)', group: 'Population — age',
    canNormalise: true,
    availableAt: ['100m','500m','pc4'],
    columnAt: gridPc4('aantal_inwoners_25_tot_45_jaar'),
  },
  {
    key: 'age_25_45_pct', label: 'Age 25–45 (%)', group: 'Population — age',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente'],
    columnAt: adminAll('percentage_personen_25_tot_45_jaar'),
  },
  {
    key: 'age_45_65', label: 'Age 45–65 (n)', group: 'Population — age',
    canNormalise: true,
    availableAt: ['100m','500m','pc4'],
    columnAt: gridPc4('aantal_inwoners_45_tot_65_jaar'),
  },
  {
    key: 'age_45_65_pct', label: 'Age 45–65 (%)', group: 'Population — age',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente'],
    columnAt: adminAll('percentage_personen_45_tot_65_jaar'),
  },
  {
    key: 'age_65plus', label: 'Age 65+ (n)', group: 'Population — age',
    canNormalise: true,
    availableAt: ['100m','500m','pc4'],
    columnAt: gridPc4('aantal_inwoners_65_jaar_en_ouder'),
  },
  {
    key: 'age_65plus_pct', label: 'Age 65+ (%)', group: 'Population — age',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente'],
    columnAt: adminAll('percentage_personen_65_jaar_en_ouder'),
  },

  // ── Origin ─────────────────────────────────────────────────
  // IMPORTANT: column names differ significantly between grid/pc4 and admin scales.
  // Grid/pc4 use: percentage_geb_nederland_herkomst_nederland
  // Buurt/wijk/gemeente use: percentage_met_herkomstland_nederland
  {
    key: 'pct_dutch_origin', label: '% Dutch origin', group: 'Origin',
    canNormalise: false,
    availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'],
    columnAt: {
      ...gridPc4('percentage_geb_nederland_herkomst_nederland'),
      ...adminAll('percentage_met_herkomstland_nederland'),
    },
  },
  {
    key: 'pct_eu_nl_born', label: '% European (NL-born)', group: 'Origin',
    canNormalise: false,
    availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'],
    columnAt: {
      ...gridPc4('percentage_geb_nederland_herkomst_overig_europa'),
      ...adminAll('perc_geb_in_nl_met_herkomstland_in_europa_ex_nl'),
    },
  },
  {
    key: 'pct_non_eu_nl_born', label: '% Non-European (NL-born)', group: 'Origin',
    canNormalise: false,
    availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'],
    columnAt: {
      ...gridPc4('percentage_geb_nederland_herkomst_buiten_europa'),
      ...adminAll('perc_geb_in_nl_met_herkomstland_buiten_europa'),
    },
  },
  {
    key: 'pct_eu_foreign_born', label: '% European (foreign-born)', group: 'Origin',
    canNormalise: false,
    availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'],
    columnAt: {
      ...gridPc4('percentage_geb_buiten_nederland_herkomst_europa'),
      ...adminAll('perc_geb_buiten_nl_met_herkomstlnd_in_europa_ex_nl'),
    },
  },
  {
    key: 'pct_non_eu_foreign_born', label: '% Non-European (foreign-born)', group: 'Origin',
    canNormalise: false,
    availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'],
    columnAt: {
      ...gridPc4('percentage_geb_buiten_nederland_herkmst_buiten_europa'),
      ...adminAll('perc_geb_buiten_nl_met_herkomstlnd_buiten_europa'),
    },
  },

  // ── Households ─────────────────────────────────────────────
  {
    key: 'households', label: 'Total households', group: 'Households',
    canNormalise: true,
    availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'],
    columnAt: {
      ...gridPc4('aantal_part_huishoudens'),
      ...adminAll('aantal_huishoudens'),
    },
  },
  {
    key: 'single_hh_n', label: 'Single-person households (n)', group: 'Households',
    canNormalise: true,
    availableAt: ['100m','500m','pc4'],
    columnAt: gridPc4('aantal_eenpersoonshuishoudens'),
  },
  {
    key: 'single_hh_pct', label: 'Single-person households (%)', group: 'Households',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente'],
    columnAt: adminAll('percentage_eenpersoonshuishoudens'),
  },
  {
    key: 'avg_hh_size', label: 'Avg household size', group: 'Households',
    canNormalise: false,
    availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'],
    columnAt: {
      ...gridPc4('gemiddelde_huishoudensgrootte'),
      ...adminAll('gemiddelde_huishoudsgrootte'),
    },
  },

  // ── Housing ────────────────────────────────────────────────
  {
    key: 'dwellings', label: 'Total dwellings', group: 'Housing',
    canNormalise: true,
    availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'],
    columnAt: {
      ...gridPc4('aantal_woningen'),
      ...adminAll('woningvoorraad'),
    },
  },
  {
    key: 'pct_owner_occupied', label: '% Owner-occupied', group: 'Housing',
    canNormalise: false,
    availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'],
    columnAt: allSix('percentage_koopwoningen'),
  },
  {
    key: 'pct_rental', label: '% Rental', group: 'Housing',
    canNormalise: false,
    availableAt: ['100m','500m','buurt','wijk','gemeente','pc4'],
    columnAt: allSix('percentage_huurwoningen'),
  },
  {
    key: 'social_housing_n', label: 'Social housing (n)', group: 'Housing',
    canNormalise: true,
    availableAt: ['100m','500m','pc4'],
    columnAt: gridPc4('aantal_huurwoningen_in_bezit_woningcorporaties'),
  },
  {
    key: 'social_housing_pct', label: 'Social housing (%)', group: 'Housing',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente'],
    columnAt: adminAll('perc_huurwoningen_in_bezit_woningcorporaties'),
  },
  {
    key: 'avg_woz', label: 'Avg property value (WOZ, €k)', group: 'Housing',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente','pc4'],
    columnAt: {
      'pc4': 'gemiddelde_woz_waarde_woning',
      'buurt': 'gemiddelde_woningwaarde',
      'wijk': 'gemiddelde_woningwaarde',
      'gemeente': 'gemiddelde_woningwaarde',
    },
  },
  {
    key: 'pct_eengezins', label: '% Single-family homes', group: 'Housing',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente'],
    columnAt: adminAll('percentage_eengezinswoning'),
  },

  // ── Income & benefits ──────────────────────────────────────
  // CBS "uitkering onder AOW-leeftijd" — exists at grid/pc4 only
  {
    key: 'persons_on_benefits', label: 'Persons on benefits (grid/PC4)', group: 'Income',
    canNormalise: true,
    availableAt: ['100m','500m','pc4'],
    columnAt: gridPc4('aantal_personen_met_uitkering_onder_aowlft'),
  },
  // Buurt has the same concept but sourced differently — check actual column
  // For now expose the per-type columns at buurt/wijk/gemeente via the benefit entries below
  // Wijk/gemeente only (CamelCase columns, lowercase versions confirmed in wijk glimpse)



  {
    key: 'benefits_bijstand', label: 'Social assistance (bijstand)', group: 'Income',
    canNormalise: true,
    availableAt: ['wijk','gemeente'],
    columnAt: adminAll('aantal_personen_met_een_alg_bijstandsuitkering_tot'),
  },
  {
    key: 'benefits_ao', label: 'Disability benefit (AO)', group: 'Income',
    canNormalise: true,
    availableAt: ['wijk','gemeente'],
    columnAt: adminAll('aantal_personen_met_een_ao_uitkering_totaal'),
  },
  {
    key: 'benefits_ww', label: 'Unemployment benefit (WW)', group: 'Income',
    canNormalise: true,
    availableAt: ['wijk','gemeente'],
    columnAt: adminAll('aantal_personen_met_een_ww_uitkering_totaal'),
  },

  // ── Employment (wijk/gemeente CBS kerncijfers) ─────────────





  // ── Business ───────────────────────────────────────────────
  {
    key: 'bedrijfsvestigingen', label: 'Business establishments', group: 'Business',
    canNormalise: true,
    availableAt: ['buurt','wijk','gemeente'],
    columnAt: adminAll('aantal_bedrijfsvestigingen'),
  },

  // ── Accessibility ──────────────────────────────────────────
  {
    key: 'dist_gp', label: 'Distance to GP (km)', group: 'Accessibility',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente'],
    columnAt: adminAll('huisartsenpraktijk_gemiddelde_afstand_in_km'),
  },
  {
    key: 'dist_supermarkt', label: 'Distance to supermarket (km)', group: 'Accessibility',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente'],
    columnAt: adminAll('grote_supermarkt_gemiddelde_afstand_in_km'),
  },
  {
    key: 'dist_treinstation', label: 'Distance to train station (km)', group: 'Accessibility',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente'],
    columnAt: adminAll('treinstation_gemiddelde_afstand_in_km'),
  },
  {
    key: 'dist_basisonderwijs', label: 'Distance to primary school (km)', group: 'Accessibility',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente'],
    columnAt: adminAll('basisonderwijs_gemiddelde_afstand_in_km'),
  },

  // ── Area ───────────────────────────────────────────────────
  {
    key: 'land_area_ha', label: 'Land area (ha)', group: 'Area',
    canNormalise: false,
    availableAt: ['buurt','wijk','gemeente','pc4'],
    columnAt: {
      'buurt': 'oppervlakte_land_in_ha',
      'wijk':  'oppervlakte_land_in_ha',
      'gemeente': 'oppervlakte_land_in_ha',
      'pc4':   'oppervlakte_land_in_ha',
    },
  },

  // ── Employment nodes PC4 (nodes_summary_pc4.parquet) ─────────
  // Parallel to gemeente nodes but at PC4 scale
  {
    key: 'pc4_total_banen_werk', label: 'Jobs at work location (PC4)', group: 'Employment (nodes)',
    canNormalise: true, availableAt: ['pc4'], source: 'nodes_pc4',
  },
  {
    key: 'pc4_total_banen_woon', label: 'Employed residents (PC4)', group: 'Employment (nodes)',
    canNormalise: true, availableAt: ['pc4'], source: 'nodes_pc4',
  },
  {
    key: 'pc4_total_inwoners', label: 'Population (CBS microdata, PC4)', group: 'Employment (nodes)',
    canNormalise: false, availableAt: ['pc4'], source: 'nodes_pc4',
  },
  {
    key: 'pc4_ratio_banen_inwoners', label: 'Jobs / residents ratio (PC4)', group: 'Employment (nodes)',
    canNormalise: false, availableAt: ['pc4'], source: 'nodes_pc4',
  },
  {
    key: 'pc4_ratio_werkenden_inwoners', label: 'Employment rate proxy (PC4)', group: 'Employment (nodes)',
    canNormalise: false, availableAt: ['pc4'], source: 'nodes_pc4',
  },

  // ── Income breakdown — both gemeente and PC4 ─────────────────────────────────
  // Single variable entry per category; scale chips show gemeente + pc4 options.
  // fetchRows routes to the correct parquet based on selected scale.
  {
    key: 'demo_ink_1', label: 'Income < 20%', group: 'Income (nodes)',
    canNormalise: true, availableAt: ['gemeente', 'pc4'], source: 'nodes_ink',
  },
  {
    key: 'demo_ink_2', label: 'Income 20–40%', group: 'Income (nodes)',
    canNormalise: true, availableAt: ['gemeente', 'pc4'], source: 'nodes_ink',
  },
  {
    key: 'demo_ink_3', label: 'Income 40–60%', group: 'Income (nodes)',
    canNormalise: true, availableAt: ['gemeente', 'pc4'], source: 'nodes_ink',
  },
  {
    key: 'demo_ink_4', label: 'Income 60–80%', group: 'Income (nodes)',
    canNormalise: true, availableAt: ['gemeente', 'pc4'], source: 'nodes_ink',
  },
  {
    key: 'demo_ink_5', label: 'Income 80–100%', group: 'Income (nodes)',
    canNormalise: true, availableAt: ['gemeente', 'pc4'], source: 'nodes_ink',
  },

  // ── Education breakdown — both gemeente and PC4 ───────────────────────────────
  {
    key: 'demo_opl_1', label: 'Education — low', group: 'Education (nodes)',
    canNormalise: true, availableAt: ['gemeente', 'pc4'], source: 'nodes_opl',
  },
  {
    key: 'demo_opl_2', label: 'Education — mid', group: 'Education (nodes)',
    canNormalise: true, availableAt: ['gemeente', 'pc4'], source: 'nodes_opl',
  },
  {
    key: 'demo_opl_3', label: 'Education — high', group: 'Education (nodes)',
    canNormalise: true, availableAt: ['gemeente', 'pc4'], source: 'nodes_opl',
  },

  // ── Employment nodes (nodes_summary_gem.parquet, jaar=2017) ──
  {
    key: 'total_banen_werk', label: 'Jobs at work location', group: 'Employment (nodes)',
    canNormalise: true, availableAt: ['gemeente'], source: 'nodes',
  },
  {
    key: 'total_banen_woon', label: 'Employed residents', group: 'Employment (nodes)',
    canNormalise: true, availableAt: ['gemeente'], source: 'nodes',
  },
  {
    key: 'total_inwoners', label: 'Population (CBS microdata)', group: 'Employment (nodes)',
    canNormalise: false, availableAt: ['gemeente'], source: 'nodes',
  },
  {
    key: 'ratio_banen_inwoners', label: 'Jobs / residents ratio', group: 'Employment (nodes)',
    canNormalise: false, availableAt: ['gemeente'], source: 'nodes',
  },
  {
    key: 'ratio_werkenden_inwoners', label: 'Employment rate (proxy)', group: 'Employment (nodes)',
    canNormalise: false, availableAt: ['gemeente'], source: 'nodes',
  },
];

// ── Edge datasets ─────────────────────────────────────────────────────────────

export const EDGE_DATASETS: EdgeDataset[] = [
  {
    key:          'woonwerk',
    label:        'Home → work commuting',
    description:  'Annual avg flows 2007–2017; divided by 6 per collection period',
    flows:        'edges_woonwerk_gem.parquet',
    flowSummary:  'edges_woonwerk_summary_gem.parquet',
    scaleKey:     'gemeente',
    colour:       '#e63946',
    periods:      ['20072012', '20122017'],
    periodLabels: { '20072012': '2007–2012', '20122017': '2012–2017' },
    defaultPeriod: '20122017',
    idCols:       { origin: 'origin_id', destination: 'destination_id', period: 'periode' },
    hasBreakdown: true,  // supports income/education filtering
  },
  {
    key:          'werkwerk',
    label:        'Job-to-job moves',
    description:  'Workers changing employer municipality 2007–2017',
    flows:        'edges_werkwerk_gem.parquet',
    flowSummary:  'edges_werkwerk_summary_gem.parquet',
    scaleKey:     'gemeente',
    colour:       '#f4a261',
    periods:      ['07-12', '12-17'],
    periodLabels: { '07-12': '2007–2012', '12-17': '2012–2017' },
    defaultPeriod: '12-17',
    idCols: { origin: 'origin_id', destination: 'destination_id', period: 'periode' },
    hasBreakdown: true,
  },
  {
    key:          'migration',
    label:        'Residential moves',
    description:  'Municipality-to-municipality moves; event totals, not averaged',
    flows:        'edges_migration_gem.parquet',
    flowSummary:  'edges_migration_summary_gem.parquet',
    scaleKey:     'gemeente',
    colour:       '#2a9d8f',
    periods:      ['p07-10', 'p11-14', 'p15-18'],
    periodLabels: { 'p07-10': '2007–2010', 'p11-14': '2011–2014', 'p15-18': '2015–2018' },
    defaultPeriod: 'p15-18',
    idCols: { origin: 'origin_id', destination: 'destination_id', period: 'periode' },
  },
  // ── PC4-level edge datasets ────────────────────────────────────────────────
  {
    key:          'woonwerk_pc4',
    label:        'Home → work commuting (PC4)',
    description:  'Annual avg flows 2007–2017 at postcode level, study area filtered',
    flows:        'edges_woonwerk_pc4.parquet',
    flowSummary:  'edges_woonwerk_summary_pc4.parquet',
    scaleKey:     'pc4',
    colour:       '#c1121f',
    periods:      ['20072012', '20122017'],
    periodLabels: { '20072012': '2007–2012', '20122017': '2012–2017' },
    defaultPeriod: '20122017',
    idCols: { origin: 'origin_id', destination: 'destination_id', period: 'periode' },
    hasBreakdown: true,
  },
  {
    key:          'werkwerk_pc4',
    label:        'Job-to-job moves (PC4)',
    description:  'Workers changing employer postcode 2007–2017',
    flows:        'edges_werkwerk_pc4.parquet',
    flowSummary:  'edges_werkwerk_summary_pc4.parquet',
    scaleKey:     'pc4',
    colour:       '#e07b39',
    periods:      ['07-12', '12-17'],
    periodLabels: { '07-12': '2007–2012', '12-17': '2012–2017' },
    defaultPeriod: '12-17',
    idCols: { origin: 'origin_id', destination: 'destination_id', period: 'periode' },
  },
  {
    key:          'migration_pc4',
    label:        'Residential moves (PC4)',
    description:  'Postcode-to-postcode moves, study area filtered',
    flows:        'edges_migration_pc4.parquet',
    flowSummary:  'edges_migration_summary_pc4.parquet',
    scaleKey:     'pc4',
    colour:       '#1a7a6e',
    periods:      ['p07-10', 'p11-14', 'p15-18'],
    periodLabels: { 'p07-10': '2007–2010', 'p11-14': '2011–2014', 'p15-18': '2015–2018' },
    defaultPeriod: 'p15-18',
    idCols: { origin: 'origin_id', destination: 'destination_id', period: 'periode' },
  },
  // ODiN datasets — uncomment when edges-ovin-2022.sqlite is processed
  // {
  //   key: 'odin_all_gem', label: 'All trips — ODiN (gemeente)',
  //   flows: 'edges_odin_all_gem.parquet',
  //   flowSummary: 'edges_odin_all_summary_gem.parquet',
  //   scaleKey: 'gemeente', colour: '#7b2d8b',
  //   periods: ['20072012','20122017'], periodLabels: {...}, defaultPeriod: '20122017',
  //   idCols: { origin: 'origin_id', destination: 'destination_id', period: 'periode' },
  // },
];

// ── Calculator operators ──────────────────────────────────────────────────────

export const CALC_OPERATORS = [
  { key: '+', label: 'A + B' },
  { key: '-', label: 'A − B' },
  { key: '*', label: 'A × B' },
  { key: '/', label: 'A ÷ B' },
] as const;

// ── Normalisation options ─────────────────────────────────────────────────────

export const NORMALISATIONS = [
  { key: 'none',     label: 'Raw' },
  { key: 'per_km2',  label: 'Per km²' },
  { key: 'per_1000', label: 'Per 1k pop' },
] as const;

// ── Helpers ───────────────────────────────────────────────────────────────────

export function findScale(key: string): Scale | undefined {
  return ALL_SCALES.find(s => s.key === key);
}

/** Actual parquet column for a variable at a given scale */
export function colForScale(varKey: string, scaleKey: string): string {
  const v = VARIABLES.find(x => x.key === varKey);
  if (!v) return varKey;
  return v.columnAt?.[scaleKey] ?? v.key;
}

export function varsForScale(scaleKey: string): Variable[] {
  return VARIABLES.filter(v => v.availableAt.includes(scaleKey));
}

export function sharedScales(varKeyA: string, varKeyB: string): string[] {
  const a = VARIABLES.find(v => v.key === varKeyA);
  const b = VARIABLES.find(v => v.key === varKeyB);
  if (!a || !b) return [];
  return a.availableAt.filter(s => b.availableAt.includes(s));
}

export function scaleChips(varKey: string): {
  inner: { key: string; label: string }[];
  outer: { key: string; label: string }[];
} {
  const v = VARIABLES.find(x => x.key === varKey);
  if (!v) return { inner: [], outer: [] };
  return {
    inner: INNER_SCALES.filter(s => v.availableAt.includes(s.key)).map(s => ({ key: s.key, label: s.label })),
    outer: OUTER_SCALES.filter(s => v.availableAt.includes(s.key)).map(s => ({ key: s.key, label: s.label })),
  };
}

export function isInnerScale(key: string): boolean {
  return INNER_SCALES.some(s => s.key === key);
}