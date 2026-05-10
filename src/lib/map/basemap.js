// Basemap helpers: PMTiles protocol registration + protomaps style builders.
//
// Three sources, all using the same protomaps-themes-base layer set:
//   - protomapsApiStyle({apiKey, theme}): hosted MVT tiles via Protomaps API
//   - pmtilesStyle({url, theme}):         self-hosted PMTiles file (range-served)
//   - emptyStyle({background}):           plain background fallback
import maplibregl from 'maplibre-gl';
import * as pmtilesPkg from 'pmtiles';
import { layers as protomapsLayers, namedTheme } from 'protomaps-themes-base';

const ATTRIBUTION =
	'<a href="https://protomaps.com">Protomaps</a> | © <a href="https://openstreetmap.org">OpenStreetMap</a>';
const GLYPHS = 'https://fonts.protomaps.com/{fontstack}/{range}.pbf';

let protocolRegistered = false;
export function registerPmtilesProtocol() {
	if (protocolRegistered) return;
	const protocol = new pmtilesPkg.Protocol();
	maplibregl.addProtocol('pmtiles', protocol.tile);
	protocolRegistered = true;
}

export function protomapsApiStyle({ apiKey, theme = 'white' }) {
	return {
		version: 8,
		glyphs: GLYPHS,
		sources: {
			protomaps: {
				type: 'vector',
				tiles: [`https://api.protomaps.com/tiles/v4/{z}/{x}/{y}.mvt?key=${apiKey}`],
				minzoom: 0,
				maxzoom: 15,
				attribution: ATTRIBUTION
			}
		},
		layers: protomapsLayers('protomaps', namedTheme(theme))
	};
}

export function pmtilesStyle({ url, theme = 'white' }) {
	return {
		version: 8,
		glyphs: GLYPHS,
		sources: {
			protomaps: {
				type: 'vector',
				url: `pmtiles://${url}`,
				attribution: ATTRIBUTION
			}
		},
		layers: protomapsLayers('protomaps', namedTheme(theme))
	};
}

export function emptyStyle({ background = '#f3f0e8' } = {}) {
	return {
		version: 8,
		sources: {},
		layers: [{ id: 'background', type: 'background', paint: { 'background-color': background } }]
	};
}
