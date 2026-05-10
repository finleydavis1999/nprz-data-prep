// Curved OD geometry — quadratic Bézier in lon/lat space (good enough at NL
// extents). The control point sits perpendicular to the OD vector at its
// midpoint, offset by `curvature * length`.
//
// `curvature` 0 → straight line; 0.2 → gentle arc; >0.5 looks loopy.
// All flows curve the same direction (CCW) so parallel OD pairs separate.
export function bezierLine([lon1, lat1], [lon2, lat2], { curvature = 0.2, segments = 24 } = {}) {
	if (curvature === 0 || segments <= 1) return [[lon1, lat1], [lon2, lat2]];
	const dx = lon2 - lon1;
	const dy = lat2 - lat1;
	const mx = (lon1 + lon2) / 2;
	const my = (lat1 + lat2) / 2;
	// Perpendicular (rotate 90° CCW): (-dy, dx)
	const cx = mx - dy * curvature;
	const cy = my + dx * curvature;
	const out = new Array(segments + 1);
	for (let i = 0; i <= segments; i++) {
		const t = i / segments;
		const u = 1 - t;
		const x = u * u * lon1 + 2 * u * t * cx + t * t * lon2;
		const y = u * u * lat1 + 2 * u * t * cy + t * t * lat2;
		out[i] = [x, y];
	}
	return out;
}
