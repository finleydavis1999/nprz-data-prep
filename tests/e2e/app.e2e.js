import { test, expect } from '@playwright/test';

// Tests run with AUTH_DISABLED=true (set in playwright.config.js webServer.env),
// so no login dance is needed.

test.describe.serial('app', () => {
	test.beforeEach(async ({ page }) => {
		await page.goto('/');
		// Choropleth ready when the legend renders.
		await expect(page.locator('.legend')).toBeVisible({ timeout: 30_000 });
		await expect(page.locator('.status')).not.toContainText('querying');
	});

	test('renders header, status, sidebar panels, no console errors', async ({ page }) => {
		const errors = [];
		page.on('pageerror', (e) => errors.push(e.message));
		page.on('console', (m) => {
			if (m.type() === 'error') errors.push(m.text());
		});

		await expect(page.locator('.brand')).toContainText('NPRZ');
		await expect(page.locator('.status').first()).toContainText(/PC4s|gemeenten/);
		const titles = await page.locator('details.panel summary').allTextContents();
		expect(titles).toEqual(['Scale', 'Data', 'Cartography', 'Study area', 'Boundary overlay']);
		expect(titles).toEqual([
			'Scale',
			'Node data',
			'Flow data',
			'Node cartography',
			'Flow cartography',
			'Boundary overlay'
		]);

		await page.waitForTimeout(500);
		expect(errors.filter((e) => !/sourcemap/i.test(e))).toEqual([]);
	});

	test('flow toggle adds curved flow lines to the map', async ({ page }) => {
		// Open Flow data panel and tick the enable checkbox.
		const flowPanel = page.locator('details.panel', { hasText: 'Flow data' });
		await flowPanel.locator('summary').click();
		const enable = flowPanel.locator('label.field', { hasText: 'Show flows' }).locator('input[type="checkbox"]');
		await enable.check();

		// Status row for flows appears with a positive feature count.
		await expect(page.locator('.status').nth(1)).toContainText(/flow:.*flows/i, { timeout: 15_000 });
		await expect(page.locator('.status').nth(1)).not.toContainText('querying', { timeout: 15_000 });

		// Wait for source data to settle, then assert layer + features exist.
		await page.waitForFunction(
			() => {
				const m = window.__map;
				return (
					!!m?.getSource?.('flow-ovin-gem') &&
					!!m?.getLayer?.('flow-ovin-gem-line') &&
					m.querySourceFeatures('flow-ovin-gem').length > 0
				);
			},
			{ timeout: 15_000 }
		);
	});

	test('scale toggle switches PC4 → gemeente', async ({ page }) => {
		await expect(page.locator('.status')).toContainText('PC4s');
		await page.locator('.seg label', { hasText: 'Gemeente' }).click();
		await expect(page.locator('.status')).toContainText('gemeenten', { timeout: 15_000 });
	});

	test('filter chip toggles affect status reset state', async ({ page }) => {
		const nodeStatus = page.locator('.status').first();
		const baselineStatus = await nodeStatus.textContent();
		await page.locator('.chip', { hasText: '12-18' }).first().click();
		await expect(page.locator('button.link', { hasText: /Reset \(/ }).first()).toBeVisible();
		await expect(nodeStatus).not.toContainText('querying', { timeout: 10_000 });
		await page.locator('button.link', { hasText: /Reset/ }).first().click();
		await expect(nodeStatus).toContainText(/PC4s|gemeenten/);
		expect(await nodeStatus.textContent()).toBe(baselineStatus);
	});

	test('classification method swap changes legend break values', async ({ page }) => {
		const cartoPanel = page.locator('details.panel').filter({
			has: page.locator('summary', { hasText: /^Node cartography$/ })
		});
		const before = await cartoPanel.locator('.legend .label').allTextContents();
		const methodSelect = cartoPanel.locator('label.field', { hasText: 'Method' }).locator('select');
		await methodSelect.selectOption({ label: 'Quantile' });
		await page.waitForTimeout(400);
		const after = await cartoPanel.locator('.legend .label').allTextContents();
		expect(after).not.toEqual(before);
		expect(after.length).toBe(before.length);
	});

	test('print route renders SVG with one path per feature, shares classification', async ({
		page
	}) => {
		// Capture screen-side breaks first.
		const screenBreaks = await page.locator('.legend .label').allTextContents();
		await page.click('a.action[href="/print"]');
		await page.waitForURL('/print');
		await expect(page.locator('.sheet svg')).toBeVisible({ timeout: 15_000 });
		// Should render lots of features (4056 PC4s).
		const pathCount = await page.locator('.sheet svg path').count();
		expect(pathCount).toBeGreaterThan(3000);
		// Title + footer present.
		await expect(page.locator('.title')).toContainText('Persoonsgegevens');
		await expect(page.locator('.footnote')).toContainText('EPSG:28992');
		// Same classification breaks as the screen view.
		const printBreaks = await page.locator('.legend .label').allTextContents();
		expect(printBreaks).toEqual(screenBreaks);
	});
});
