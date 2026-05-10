import { fail, redirect } from '@sveltejs/kit';
import { auth } from '$lib/server/auth';

/** @type {import('./$types').Actions} */
export const actions = {
	default: async (event) => {
		const data = await event.request.formData();
		const email = String(data.get('email') ?? '').trim();
		const password = String(data.get('password') ?? '');
		if (!email || !password) {
			return fail(400, { email, error: 'Email and password are required.' });
		}
		try {
			await auth.api.signInEmail({
				body: { email, password },
				headers: event.request.headers
			});
		} catch (e) {
			return fail(400, { email, error: e?.message ?? 'Invalid credentials.' });
		}
		const next = event.url.searchParams.get('next') || '/';
		redirect(303, next);
	}
};
