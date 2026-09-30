import { describe, expect, it } from 'bun:test';
import { call, getApp, kafkaReady } from '../utils';

describe('GET /api/health', () => {
	it('reports Postgres and Kafka healthy once Kafka is up', async () => {
		const app = await getApp();
		await kafkaReady();
		const { status, body } = await call(app, 'GET', '/api/health');
		expect(status).toBe(200);
		expect(body.ok).toBe(true);
		expect(body.isInWorkingState).toBe(true);
		expect(body.services.postgres).toEqual({ ok: true });
		expect(body.services.kafka.ok).toBe(true);
	});
});
