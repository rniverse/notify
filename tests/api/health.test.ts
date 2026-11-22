import { describe, expect, it } from 'bun:test';
import { call, getApp, kafkaReady } from '../utils';

// The routes come from `@rniverse/shared/elysia` (`plugin.health`), which has
// its own tests. These only check notify's wiring.
describe('GET /api/health/*', () => {
	it('live is 200', async () => {
		const app = await getApp();
		const { status, body } = await call(app, 'GET', '/api/health/live');
		expect(status).toBe(200);
		expect(body).toEqual({ status: 'pass' });
	});

	it('ready checks Postgres (required) and is 200', async () => {
		const app = await getApp();
		const { status, body } = await call(app, 'GET', '/api/health/ready');
		expect(status).toBe(200);
		expect(body).toEqual({ status: 'pass' });
	});

	it('details reports Postgres and Kafka healthy once Kafka is up', async () => {
		const app = await getApp();
		await kafkaReady();
		// A Kafka result cached from boot (still connecting) is served once more
		// while it refreshes — poll until the refresh lands.
		let body: any;
		for (let i = 0; i < 50; i++) {
			body = (await call(app, 'GET', '/api/health/details')).body;
			if (body.checks.kafka?.status === 'pass') break;
			await new Promise((resolve) => setTimeout(resolve, 100));
		}
		expect(body.status).toBe('pass');
		expect(body.checks.postgres).toMatchObject({
			required: true,
			status: 'pass',
		});
		expect(body.checks.kafka).toMatchObject({
			required: false,
			status: 'pass',
		});
	});
});
