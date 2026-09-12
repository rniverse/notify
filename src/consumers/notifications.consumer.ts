import { pg } from '@connections';
import { failed_notifications, failures } from '@db/schema';
import { reasonOf } from '@rniverse/shared/error';
import { log, t, ulid } from '@rniverse/utils';
import { schema$notification } from '@schema/api/notification.schema';
import { send } from '@services/notification/orchestrator';
import type { EachMessagePayload } from 'kafkajs';

/**
 * A message that never made it into `notifications` — invalid JSON, or valid
 * JSON not matching the notification shape (spec §8 poison-message guard).
 * Recorded into `failed_notifications` + `failures` so it's auditable.
 * `max_attempts: 0` marks "rejected before any attempt", distinct from a row
 * that tried and ran out of attempts.
 */
async function reject(
	context: { topic: string; partition: number; offset: string },
	raw: unknown,
	reason: string,
): Promise<void> {
	const id = ulid.generate();
	const channel =
		raw &&
		typeof raw === 'object' &&
		typeof (raw as { channel?: unknown }).channel === 'string'
			? (raw as { channel: string }).channel
			: 'unknown';
	const payload: Record<string, unknown> =
		raw && typeof raw === 'object' && !Array.isArray(raw)
			? (raw as Record<string, unknown>)
			: { raw };

	const client = pg();
	await client.insert(failed_notifications).values({
		id,
		channel,
		payload,
		current_attempt: 0,
		max_attempts: 0,
		created_at: new Date(),
		updated_at: new Date(),
	});
	await client.insert(failures).values({
		id: ulid.generate(),
		notification_id: id,
		reason,
		attempt: 0,
		source: 'async',
	});
	log.error(
		{ ...context, id, reason },
		'notifications.consumer: message rejected — recorded in failed_notifications',
	);
}

/**
 * Poison-message guard + dispatch for one Kafka message, tagged
 * `source: 'async'`. A genuine infra error (not a provider send failure —
 * `send()` already handles that) must propagate so kafkajs doesn't commit
 * the offset and Kafka redelivers (spec §3, §8).
 */
export async function onEachMessage({
	topic,
	partition,
	message,
}: EachMessagePayload): Promise<void> {
	if (!message.value) return;

	// A malformed/unrelated message on this topic can never become valid on
	// redelivery — unlike a genuine infra error (below), retrying it forever
	// just crash-loops the consumer. Log and move past it instead of throwing.
	const context = { topic, partition, offset: message.offset };

	let raw: unknown;
	try {
		raw = JSON.parse(message.value.toString());
	} catch (err) {
		await reject(
			context,
			message.value.toString(),
			`unparseable JSON: ${reasonOf(err)}`,
		);
		return;
	}

	const parsed = t.safeParse(schema$notification.message, raw);
	if (!parsed.success) {
		const reason = parsed.issues.map((issue) => issue.message).join('; ');
		await reject(context, raw, `schema validation failed: ${reason}`);
		return;
	}

	const input = parsed.output;
	log.info(
		{ id: input.id, topic, partition, offset: message.offset },
		'notifications.consumer: message received',
	);
	// A genuine infra error (DB down, etc.) from here on is allowed to
	// propagate — that's what makes kafkajs *not* commit the offset, so it
	// redelivers once the infra recovers (spec §3, §8).
	await send(input, { source: 'async' });
}
