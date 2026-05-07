// =============================================================================
// popup.ts
//
// Popup content construction for map click events.
//
// Keeps popup business logic out of page.svelte. The popup itself is rendered
// in the Svelte template using a $state<PopupInfo | null> variable, but the
// content of each popup variant is assembled here.
// =============================================================================

// ── Types ────────────────────────────────────────────────────────────────────

export interface PopupInfo {
  x:     number; // screen x (pixels)
  y:     number; // screen y (pixels)
  title: string;
  rows:  { label: string; value: string }[];
}

// ── Popup builders ────────────────────────────────────────────────────────────

/**
 * Build popup content for a choropleth area click.
 *
 * @param x      Screen x from MapLibre click event
 * @param y      Screen y from MapLibre click event
 * @param title  Scale label (e.g. 'Buurt', 'PC4')
 * @param id     Feature ID (the GeoJSON promoteId value)
 * @param cls    Feature state class (-1 = no data, 0-3 = class index)
 */
export function buildAreaPopup(
  x:     number,
  y:     number,
  title: string,
  id:    string,
  cls:   number,
): PopupInfo {
  return {
    x, y, title,
    rows: [
      { label: 'ID',    value: id },
      { label: 'Class', value: cls >= 0 ? `Class ${cls + 1}` : 'No data' },
    ],
  };
}

/**
 * Build popup content for a flow line click (both edge datasets and model residuals).
 *
 * @param x      Screen x from MapLibre click event
 * @param y      Screen y from MapLibre click event
 * @param props  Feature properties from the GeoJSON (flow_value, origin_id, etc.)
 */
export function buildFlowPopup(
  x:     number,
  y:     number,
  props: Record<string, any>,
): PopupInfo {
  const isResidual = props.residual != null;
  const rows: { label: string; value: string }[] = [
    { label: 'From', value: String(props.origin_id ?? '—') },
    { label: 'To',   value: String(props.dest_id   ?? '—') },
    {
      label: 'Observed',
      value: props.observed != null
        ? Math.round(Number(props.observed)).toLocaleString()
        : String(props.flow_value ?? '—'),
    },
  ];

  if (isResidual) {
    rows.push(
      { label: 'Predicted', value: Math.round(Number(props.predicted)).toLocaleString() },
      { label: 'Residual',  value: Number(props.residual).toFixed(3) },
    );
  }

  return { x, y, title: isResidual ? 'Model residual' : 'Flow', rows };
}