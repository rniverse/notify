// Preloaded by bunfig.toml for every test file. Opens connections once and
// clears the database before each test.
import { afterAll, beforeEach } from 'bun:test';
import { connections } from '@connections';
import { getApp, resetDb } from './utils';

beforeEach(async () => {
	await getApp();
	await resetDb();
});

afterAll(async () => {
	await connections().close();
});
