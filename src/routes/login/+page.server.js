import { fail, redirect } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';
import { auth } from '$lib/server/auth';

// See note in src/routes/+page.server.js — actions block prerendering.
/** @type {import('./$types').Actions | undefined} */
export const actions =
	env.AUTH_DISABLED === 'true'
		? undefined
		: {
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
