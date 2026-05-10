// Singleton boundary-overlay state.
// `scale === null` means the overlay is hidden.
class OverlayState {
	scale = $state(/** @type {'pc4' | 'gem' | null} */ (null));
	color = $state('#222');
	width = $state(1.0);
	opacity = $state(0.8);
}

export const overlay = new OverlayState();
