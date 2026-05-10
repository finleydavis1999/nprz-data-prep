<script>
	let { form } = $props();
</script>

<div class="auth">
	<h1>Sign in</h1>
	<form method="POST">
		<label>
			<span>Email</span>
			<input
				type="email"
				name="email"
				autocomplete="email"
				required
				value={form?.email ?? ''}
			/>
		</label>
		<label>
			<span>Password</span>
			<input type="password" name="password" autocomplete="current-password" required />
		</label>
		<button type="submit">Sign in</button>
		{#if form?.error}<p class="error">{form.error}</p>{/if}
	</form>
</div>

<style>
	.auth {
		max-width: 320px;
		margin: 10vh auto;
		padding: var(--spacing-4);
		background: var(--color-bg-panel);
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
	}
	h1 {
		margin: 0 0 var(--spacing-3);
		font-size: 18px;
		font-weight: 600;
	}
	form {
		display: flex;
		flex-direction: column;
		gap: var(--spacing-3);
	}
	label {
		display: flex;
		flex-direction: column;
		gap: 4px;
		font-size: var(--text-sm);
		color: var(--color-muted);
	}
	input {
		padding: 6px 8px;
		border: 1px solid var(--color-line);
		border-radius: var(--radius);
	}
	button {
		padding: 8px 12px;
		background: var(--color-accent);
		color: var(--color-accent-fg);
		border: none;
		border-radius: var(--radius);
		cursor: pointer;
		font-weight: 600;
	}
	.error {
		color: #cf222e;
		font-size: var(--text-sm);
		margin: 0;
	}
</style>
