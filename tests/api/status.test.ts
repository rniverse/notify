import { describe, expect, it } from 'bun:test';
import { enum$error } from '@enums/errors.enum';
import { ulid } from '@rniverse/utils';
import { call, email, getApp, UNVERIFIED_FROM } from '../utils';

describe('GET /api/notification/status/:id', () => {
	it('unknown id → 404', async () => {
		const app = await getApp();
		const { status, body } = await call(
			app,
			'GET',
			`/api/notification/status/${ulid.generate()}`,
		);
		expect(status).toBe(404);
		expect(body.error.code).toBe(enum$error.codes.NOT_FOUND);
	});

	it('a sent notification comes from notifications', async () => {
		const app = await getApp();
		const sent = await call(app, 'POST', '/api/notification/send/sync', {
			body: email('status-sent'),
		});
		const { status, body } = await call(
			app,
			'GET',
			`/api/notification/status/${sent.body.data.id}`,
		);
		expect(status).toBe(200);
		expect(body.data.id).toBe(sent.body.data.id);
		expect(body.data.status).toBe('sent');
		expect(body.data.moved_at).toBeUndefined();
	});

	it('a failed notification comes from failed_notifications', async () => {
		const app = await getApp();
		const { service$notification } = await import('@services');
		const id = ulid.generate();
		await service$notification.send({
			id,
			...email('status-failed', { from: UNVERIFIED_FROM }),
		});
		const { status, body } = await call(
			app,
			'GET',
			`/api/notification/status/${id}`,
		);
		expect(status).toBe(200);
		expect(body.data.status).toBe('failed');
		expect(body.data.moved_at).toBeTruthy();
	});
});
