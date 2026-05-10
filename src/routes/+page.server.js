import { redirect } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';
import { auth } from '$lib/server/auth';

// Static-adapter deploys (AUTH_DISABLED=true) can't run actions and also block
// prerendering when any are exported. Drop the action in that mode.
/** @type {import('./$types').Actions | undefined} */
export const actions =
	env.AUTH_DISABLED === 'true'
		? undefined
		: {
				logout: async (event) => {
					await auth.api.signOut({ headers: event.request.headers });
					redirect(303, '/login');
				}
			};
