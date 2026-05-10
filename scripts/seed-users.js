// One-shot seeder: ensures the users in seed-users.json exist in local.db.
// Idempotent — `signUpEmail` will reject existing users; we treat that as OK.
//
//   npm run users:seed
import { readFileSync } from 'node:fs';
import { auth } from '../src/lib/server/auth.js';

const file = process.argv[2] ?? 'seed-users.json';
let users;
try {
	users = JSON.parse(readFileSync(file, 'utf8'));
} catch (e) {
	console.error(`Could not read ${file}: ${e.message}`);
	console.error('Copy seed-users.json.example → seed-users.json and edit it.');
	process.exit(1);
}

let created = 0;
let existed = 0;
for (const u of users) {
	try {
		await auth.api.signUpEmail({
			body: { name: u.name, email: u.email, password: u.password },
			headers: new Headers()
		});
		console.log(`+ ${u.email}`);
		created++;
	} catch (e) {
		// better-auth throws on duplicate / invalid input. Treat duplicate as OK.
		const msg = e?.message ?? String(e);
		if (/already exists|already registered|user.*exists/i.test(msg)) {
			console.log(`= ${u.email} (already exists)`);
			existed++;
		} else {
			console.error(`! ${u.email}: ${msg}`);
		}
	}
}
console.log(`\n${created} created, ${existed} already existed`);
