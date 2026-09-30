import { describe, expect, it } from 'bun:test';
import { ulid } from '@rniverse/utils';
import { service$notification } from '@services';
import { email, getApp, resend, rows, UNVERIFIED_FROM } from '../utils';

const failing = (label: string) => email(label, { from: UNVERIFIED_FROM });

describe('service$notification.send', () => {
	it('an already-sent id is skipped without calling the provider', async () => {
		await getApp();
		const id = ulid.generate();
		expect(
			(await service$notification.send({ id, ...email('dup') })).status,
		).toBe('sent');
		const calls = resend.mock.calls.length;
		const again = await service$notification.send({ id, ...email('dup') });
		expect(again).toEqual({ status: 'sent', id });
		expect(resend.mock.calls.length).toBe(calls);
	});

	it('a failure with attempts left → pending, attempt counted, audit row with the source', async () => {
		const id = ulid.generate();
		const outcome = await service$notification.send(
			{ id, ...failing('retry-left'), maxAttempts: 3 },
			{ source: 'cron' },
		);
		expect(outcome.status).toBe('failed');
		const row = await rows.notification(id);
		expect(row?.status).toBe('pending');
		expect(row?.current_attempt).toBe(1);
		expect(row?.max_attempts).toBe(3);
		const audit = await rows.failures(id);
		expect(audit.map((f) => [f.attempt, f.source])).toEqual([[1, 'cron']]);
	});

	it('a pending row that later succeeds → sent, attempt carried on', async () => {
		const id = ulid.generate();
		await service$notification.send({
			id,
			...failing('then-ok'),
			maxAttempts: 3,
		});
		const outcome = await service$notification.send({
			id,
			...email('then-ok'),
		});
		expect(outcome.status).toBe('sent');
		const row = await rows.notification(id);
		expect(row?.status).toBe('sent');
		expect(row?.current_attempt).toBe(2);
	});

	it('the last attempt failing moves the row to failed_notifications', async () => {
		const id = ulid.generate();
		await service$notification.send({
			id,
			...failing('exhaust'),
			maxAttempts: 2,
		});
		await service$notification.send({ id, ...failing('exhaust') });
		expect(await rows.notification(id)).toBeUndefined();
		const failed = await rows.failed(id);
		expect(failed?.current_attempt).toBe(2);
		expect(failed?.max_attempts).toBe(2);
		expect(await rows.failures(id)).toHaveLength(2);
	});

	it("an existing row's max_attempts wins over a later call's", async () => {
		const id = ulid.generate();
		await service$notification.send({
			id,
			...failing('terms'),
			maxAttempts: 2,
		});
		await service$notification.send({
			id,
			...failing('terms'),
			maxAttempts: 10,
		});
		expect(await rows.failed(id)).toBeDefined();
	});

	it('an expired notification fails for good on its first failure, attempts left or not', async () => {
		const id = ulid.generate();
		await service$notification.send({
			id,
			...failing('expired'),
			maxAttempts: 5,
			expiresAt: new Date(Date.now() - 60_000),
		});
		const failed = await rows.failed(id);
		expect(failed?.current_attempt).toBe(1);
		expect(failed?.expires_at).toBeTruthy();
	});

	it('source defaults to manual', async () => {
		const id = ulid.generate();
		await service$notification.send({ id, ...failing('manual') });
		const [audit] = await rows.failures(id);
		expect(audit?.source).toBe('manual');
	});
});
