import { redirect } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';

const PUBLIC_ROUTES = new Set(['/login']);

/** @type {import('./$types').LayoutServerLoad} */
export const load = async ({ locals, url }) => {
	// Dev convenience: AUTH_DISABLED=true skips the redirect entirely.
	// Production must NOT set this — it leaves the app fully open.
	if (env.AUTH_DISABLED === 'true') {
		return { user: locals.user ?? null, authDisabled: true };
	}
	if (!locals.session && !PUBLIC_ROUTES.has(url.pathname)) {
		redirect(303, `/login?next=${encodeURIComponent(url.pathname)}`);
	}
	return { user: locals.user ?? null, authDisabled: false };
};
