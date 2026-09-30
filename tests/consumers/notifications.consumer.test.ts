import { describe, expect, it } from 'bun:test';
import { onEachMessage } from '@consumers/notifications.consumer';
import { ulid } from '@rniverse/utils';
import type { EachMessagePayload } from 'kafkajs';
import { email, getApp, resend, rows } from '../utils';

/** A kafkajs message as the consumer receives it — called directly, no broker. */
function message(value: string | null): EachMessagePayload {
	return {
		topic: 'notify-test',
		partition: 0,
		message: {
			value: value === null ? null : Buffer.from(value),
			offset: '7',
		},
	} as unknown as EachMessagePayload;
}

/** The one failed_notifications row a rejection wrote. */
async function rejected() {
	const { pg } = await import('@connections');
	const { failed_notifications } = await import('@db/schema');
	const all = await pg().select().from(failed_notifications);
	expect(all).toHaveLength(1);
	return all[0]!;
}

describe('notifications consumer — onEachMessage', () => {
	it('a valid message is sent, tagged source async', async () => {
		await getApp();
		const id = ulid.generate();
		await onEachMessage(
			message(JSON.stringify({ id, ...email('consumer-ok') })),
		);
		expect((await rows.notification(id))?.status).toBe('sent');
	});

	it('an empty message is skipped', async () => {
		const calls = resend.mock.calls.length;
		await onEachMessage(message(null));
		expect(resend.mock.calls.length).toBe(calls);
	});

	it('invalid JSON is rejected into failed_notifications — never retried', async () => {
		await onEachMessage(message('{ not json'));
		const row = await rejected();
		expect(row.channel).toBe('unknown');
		expect(row.max_attempts).toBe(0);
		expect(row.payload).toEqual({ raw: '{ not json' });
		const [audit] = await rows.failures(row.id);
		expect(audit?.reason).toStartWith('unparseable JSON');
		expect(audit?.source).toBe('async');
	});

	it('a wrong shape is rejected, keeping the channel it could read', async () => {
		await onEachMessage(
			message(JSON.stringify({ channel: 'email', payload: { to: ['x'] } })),
		);
		const row = await rejected();
		expect(row.channel).toBe('email');
		const [audit] = await rows.failures(row.id);
		expect(audit?.reason).toStartWith('schema validation failed');
	});

	it('a non-object payload is wrapped as { raw }', async () => {
		await onEachMessage(message('[1,2]'));
		const row = await rejected();
		expect(row.payload).toEqual({ raw: [1, 2] });
		expect(row.channel).toBe('unknown');
	});

	it('a rejection never calls the provider', async () => {
		const calls = resend.mock.calls.length;
		await onEachMessage(message('nope'));
		expect(resend.mock.calls.length).toBe(calls);
	});
});
