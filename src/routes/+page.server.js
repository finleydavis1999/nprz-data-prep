import { redirect } from '@sveltejs/kit';
import { auth } from '$lib/server/auth';

/** @type {import('./$types').Actions} */
export const actions = {
	logout: async (event) => {
		await auth.api.signOut({ headers: event.request.headers });
		redirect(303, '/login');
	}
};
