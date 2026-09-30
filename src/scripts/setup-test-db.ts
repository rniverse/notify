// Prepares the test database for `bun run test` (via the `pretest` script):
// creates it if missing, resets its schema, then applies src/db/migrations.
// The migrations folder is git-ignored and regenerated from src/db/schema.ts,
// so a clean re-apply each run keeps the test DB deterministic.
// Run with NODE_ENV=test so Bun loads .env.test's DATABASE_URL (a dev script,
// not app code — it reads the env directly).

import { drizzle } from 'drizzle-orm/postgres-js';
import { migrate } from 'drizzle-orm/postgres-js/migrator';
import postgres from 'postgres';

const url = process.env.DATABASE_URL;
if (process.env.NODE_ENV !== 'test' || !url) {
	throw new Error('run with NODE_ENV=test and DATABASE_URL set (.env.test)');
}
const dbName = new URL(url).pathname.slice(1);
// This drops the schema — refuse anything that doesn't look like a test DB.
if (!dbName.endsWith('_test')) {
	throw new Error(`refusing to reset "${dbName}": not a *_test database`);
}

// 1. Ensure the database exists — connect to the server's default `postgres` db.
const adminUrl = new URL(url);
adminUrl.pathname = '/postgres';
const admin = postgres(adminUrl.toString(), { max: 1, onnotice: () => {} });
try {
	const [row] = await admin`
		SELECT 1 AS ok FROM pg_database WHERE datname = ${dbName}
	`;
	if (!row) {
		await admin.unsafe(`CREATE DATABASE "${dbName}"`);
		console.log(`created database "${dbName}"`);
	}
} finally {
	await admin.end();
}

// 2. Wipe the schema (incl. drizzle's migration bookkeeping) and re-apply.
const sql = postgres(url, { max: 1, onnotice: () => {} });
try {
	await sql.unsafe(`
		DROP SCHEMA IF EXISTS drizzle CASCADE;
		DROP SCHEMA public CASCADE;
		CREATE SCHEMA public;
	`);
	await migrate(drizzle(sql), { migrationsFolder: './src/db/migrations' });
	console.log(`test db "${dbName}" reset + migrated`);
} finally {
	await sql.end();
}
