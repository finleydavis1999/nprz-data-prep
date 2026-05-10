// Projection factory for the print map.
//
// Our TopoJSON is already in EPSG:28992 (RD New) coordinates, so we use
// d3-geo's `geoIdentity()` (no re-projection) with reflectY(true) to flip
// the y-axis (RD is y-up, SVG is y-down). `fitSize` then scales+translates
// the geometry into the SVG viewport.
//
// Note: the original plan called for `geoConicConformal` parameterized as
// RD. That would be the right choice if our topojson were in WGS84 lon/lat.
// Since we baked the projection into the topojson at R-pipeline time,
// `geoIdentity` matches the data exactly with no re-projection cost.
import { geoIdentity } from 'd3-geo';

/**
 * @param {[number, number]} size [width, height] in SVG units
 * @param {GeoJSON.GeoJsonObject} features
 */
export function rdProjection(size, features) {
	return geoIdentity().reflectY(true).fitSize(size, features);
}
