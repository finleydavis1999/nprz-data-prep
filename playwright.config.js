import { defineConfig } from '@playwright/test';

const PORT = 47356;

export default defineConfig({
	testDir: 'tests/e2e',
	testMatch: '**/*.e2e.{ts,js}',
	fullyParallel: false, // single SQLite + DuckDB-WASM cache shared across tests
	use: {
		baseURL: `http://localhost:${PORT}`,
		trace: 'retain-on-failure'
	},
	webServer: {
		command: `vite dev --port ${PORT} --strictPort`,
		url: `http://localhost:${PORT}`,
		reuseExistingServer: !process.env.CI,
		timeout: 60_000,
		env: {
			ORIGIN: `http://localhost:${PORT}`,
			BETTER_AUTH_SECRET: 'dev-secret-32-chars-padding-okok',
			DATABASE_URL: 'local.db',
			AUTH_DISABLED: 'true', // e2e bypasses login
			PUBLIC_PROTOMAPS_API_KEY: process.env.PUBLIC_PROTOMAPS_API_KEY ?? 'b20f1204b39252e6'
		}
	}
});
