// Serialize an inline <svg> element to a standalone .svg file and trigger a
// browser download. The output opens in Illustrator / Inkscape with all paths
// editable; vector text remains text.
export function downloadSvg(svgEl, filename = 'map.svg') {
	if (!svgEl) return;
	const clone = /** @type {SVGSVGElement} */ (svgEl.cloneNode(true));
	clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
	clone.setAttribute('xmlns:xlink', 'http://www.w3.org/1999/xlink');
	const xml = new XMLSerializer().serializeToString(clone);
	const blob = new Blob(['<?xml version="1.0" standalone="no"?>\n', xml], {
		type: 'image/svg+xml;charset=utf-8'
	});
	const url = URL.createObjectURL(blob);
	const a = document.createElement('a');
	a.href = url;
	a.download = filename;
	document.body.appendChild(a);
	a.click();
	a.remove();
	URL.revokeObjectURL(url);
}
