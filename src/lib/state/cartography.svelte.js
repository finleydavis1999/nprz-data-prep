// Shared cartography state — singleton imported by ClassificationControls
// (writes) and the map page (reads to derive fill expression).
class CartographyState {
	method = $state('jenks');
	n = $state(5);
	palette = $state('YlOrRd');
	fillOpacity = $state(0.75);
	lineColor = $state('#666');
	lineWidth = $state(0.4);
}

export const cartography = new CartographyState();
