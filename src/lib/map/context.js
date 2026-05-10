// Typed map context — Symbol key avoids string-collision risk and gives
// child layer components a single import to read the parent map.
import { getContext, setContext } from 'svelte';

const MAP_CTX = Symbol('map-context');

/**
 * @typedef {{ map: import('maplibre-gl').Map | null, ready: boolean }} MapContext
 */

/** @param {MapContext} ctx */
export function setMapContext(ctx) {
	setContext(MAP_CTX, ctx);
}

/** @returns {MapContext} */
export function getMapContext() {
	return getContext(MAP_CTX);
}
