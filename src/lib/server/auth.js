import { betterAuth } from 'better-auth';
import { sveltekitCookies } from 'better-auth/svelte-kit';
import { env } from '$env/dynamic/private';
import { getRequestEvent } from '$app/server';
import Database from 'better-sqlite3';

const db = new Database(env.DATABASE_URL ?? 'local.db');

export const auth = betterAuth({
	baseURL: env.ORIGIN,
	secret: env.BETTER_AUTH_SECRET,
	database: db,
	emailAndPassword: { enabled: true },
	plugins: [sveltekitCookies(getRequestEvent)]
});
