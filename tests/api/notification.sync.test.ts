import { describe, expect, it } from 'bun:test';
import { enum$error } from '@enums/errors.enum';
import { call, email, getApp, rows, UNVERIFIED_FROM } from '../utils';

const SYNC = '/api/notification/send/sync';

describe('POST /api/notification/send/sync', () => {
	it('sends through Resend → 200 sent, row recorded as sent', async () => {
		const app = await getApp();
		const { status, body } = await call(app, 'POST', SYNC, {
			body: email('sync-ok'),
		});
		expect(status).toBe(200);
		expect(body.ok).toBe(true);
		expect(body.data.status).toBe('sent');

		const row = await rows.notification(body.data.id);
		expect(row?.status).toBe('sent');
		expect(row?.current_attempt).toBe(1);
		expect(await rows.failures(body.data.id)).toHaveLength(0);
	});

	it('a provider failure → 502 NOTIFICATION_SEND_FAILED, moved to failed_notifications with a sync failure row', async () => {
		const app = await getApp();
		const before = Date.now();
		const { status, body } = await call(app, 'POST', SYNC, {
			body: email('sync-fail', { from: UNVERIFIED_FROM }),
		});
		expect(status).toBe(502);
		expect(body.error.code).toBe(enum$error.codes.NOTIFICATION_SEND_FAILED);

		// The id isn't in the error body — find the one row this call made.
		const { pg } = await import('@connections');
		const { failed_notifications } = await import('@db/schema');
		const [failed] = await pg().select().from(failed_notifications);
		expect(failed?.status).toBe('failed');
		expect(failed?.current_attempt).toBe(1);
		expect(failed?.created_at.getTime()).toBeGreaterThanOrEqual(before - 1_000);
		const audit = await rows.failures(failed!.id);
		expect(audit).toHaveLength(1);
		expect(audit[0]?.source).toBe('sync');
		expect(audit[0]?.reason.length).toBeGreaterThan(0);
	});

	it.each([
		[
			'an invalid recipient',
			{ channel: 'email', payload: { to: ['nope'], subject: 's', html: 'h' } },
		],
		[
			'an unknown channel',
			{ channel: 'sms', payload: { to: ['a@b.co'], subject: 's', html: 'h' } },
		],
		['a missing payload', { channel: 'email' }],
	])('rejects %s → 422', async (_label, body) => {
		const app = await getApp();
		const { status, body: res } = await call(app, 'POST', SYNC, { body });
		expect(status).toBe(422);
		expect(res.error.code).toBe(enum$error.codes.VALIDATION_FAILED);
	});
});
