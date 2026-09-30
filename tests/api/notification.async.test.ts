import { describe, expect, it, spyOn } from 'bun:test';
import { config } from '@config';
import { kafka } from '@connections';
import { enum$error } from '@enums/errors.enum';
import { call, email, getApp, kafkaReady, rows, until } from '../utils';

const ASYNC = '/api/notification/send/async';
const producer = () =>
	kafka.producers.get(config.kafka.producers.notifier.name);

describe('POST /api/notification/send/async', () => {
	it('publishes to Kafka; the consumer sends it — nothing written before that', async () => {
		const app = await getApp();
		await kafkaReady();
		const { status, body } = await call(app, 'POST', ASYNC, {
			body: email('async-ok'),
		});
		expect(status).toBe(200);
		expect(body.data.status).toBe('accepted');
		const { id } = body.data;

		await until(async () => (await rows.notification(id))?.status === 'sent', {
			ms: 30_000,
		});
		const row = await rows.notification(id);
		expect(row?.current_attempt).toBe(1);
		expect(await rows.failures(id)).toHaveLength(0);
	}, 60_000);

	it('Kafka down → accepted anyway, written as a pending row right away', async () => {
		const app = await getApp();
		await kafkaReady();
		// Take the connector out of service: its producer follows it to `failed`.
		kafka.breaker.open({ ms: 60_000 });
		try {
			expect(producer().state).toBe('failed');
			const started = Date.now();
			const { status, body } = await call(app, 'POST', ASYNC, {
				body: email('async-down'),
			});
			expect(status).toBe(200);
			expect(body.data.status).toBe('accepted');
			expect(Date.now() - started).toBeLessThan(1_000); // never waits on Kafka

			const row = await rows.notification(body.data.id);
			expect(row?.status).toBe('pending');
			expect(row?.current_attempt).toBe(0);
		} finally {
			kafka.breaker.reset();
			await kafka.health();
		}
		expect(producer().state).toBe('ready');
	}, 60_000);

	it('a failed publish falls back to a pending row', async () => {
		const app = await getApp();
		await kafkaReady();
		const send = spyOn(producer().getInstance(), 'send').mockRejectedValueOnce(
			new Error('broker said no'),
		);
		try {
			const { body } = await call(app, 'POST', ASYNC, {
				body: email('async-publish-fail'),
			});
			expect(send).toHaveBeenCalledTimes(1);
			const row = await rows.notification(body.data.id);
			expect(row?.status).toBe('pending');
		} finally {
			send.mockRestore();
		}
	}, 60_000);

	it('rejects an invalid body → 422', async () => {
		const app = await getApp();
		const { status, body } = await call(app, 'POST', ASYNC, {
			body: { channel: 'email', payload: { to: [] } },
		});
		expect(status).toBe(422);
		expect(body.error.code).toBe(enum$error.codes.VALIDATION_FAILED);
	});
});
