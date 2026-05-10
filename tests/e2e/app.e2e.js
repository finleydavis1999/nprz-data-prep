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
		await expect(page.locator('.status')).toContainText(/PC4s|gemeenten/);
		const titles = await page.locator('details.panel summary').allTextContents();
		expect(titles).toEqual(['Scale', 'Data', 'Cartography', 'Study area', 'Boundary overlay']);

		await page.waitForTimeout(500);
		expect(errors.filter((e) => !/sourcemap/i.test(e))).toEqual([]);
	});

	test('scale toggle switches PC4 → gemeente', async ({ page }) => {
		await expect(page.locator('.status')).toContainText('PC4s');
		await page.locator('.seg label', { hasText: 'Gemeente' }).click();
		await expect(page.locator('.status')).toContainText('gemeenten', { timeout: 15_000 });
	});

	test('filter chip toggles affect status reset state', async ({ page }) => {
		const baselineStatus = await page.locator('.status').textContent();
		await page.locator('.chip', { hasText: '12-18' }).first().click();
		await expect(page.locator('button.link', { hasText: /Reset \(/ })).toBeVisible();
		await expect(page.locator('.status')).not.toContainText('querying', { timeout: 10_000 });
		await page.locator('button.link', { hasText: /Reset/ }).first().click();
		await expect(page.locator('.status')).toContainText(/PC4s|gemeenten/);
		expect(await page.locator('.status').textContent()).toBe(baselineStatus);
	});

	test('classification method swap changes legend break values', async ({ page }) => {
		const before = await page.locator('.legend .label').allTextContents();
		const methodSelect = page.locator('label.field', { hasText: 'Method' }).locator('select');
		await methodSelect.selectOption({ label: 'Quantile' });
		await page.waitForTimeout(400);
		const after = await page.locator('.legend .label').allTextContents();
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
