import { pg } from '@connections';
import { failed_notifications, failures, notifications } from '@db/schema';
import { reasonOf } from '@rniverse/shared/error';
import { log, ulid } from '@rniverse/utils';
import type { NotificationInput, SendOpts, SendOutcome } from '@schema/types';
import { eq } from 'drizzle-orm';
import { dispatch } from './dispatch';

function isExpired(expiresAt: Date | null): boolean {
	return expiresAt !== null && expiresAt < new Date();
}

// Orchestration layer (spec §7) — the only thing consumer, retry cron, and
// the sync API call. Dispatches by channel, wires `id` through as the
// provider idempotency key, owns pending/sent/failed_notifications bookkeeping.
// Metrics cron bypasses this and calls service$email.send() directly.
export async function send(
	notification: NotificationInput,
	opts: SendOpts = {},
): Promise<SendOutcome> {
	const source = opts.source ?? 'manual';
	const client = pg();
	const { id, channel, payload } = notification;
	log.info({ id, channel, source }, 'notification.send: start');

	const [existing] = await client
		.select()
		.from(notifications)
		.where(eq(notifications.id, id))
		.limit(1);

	// Cheap skip, not the correctness mechanism — that's the provider
	// idempotency key below (spec §3).
	if (existing?.status === 'sent') {
		log.info({ id }, 'notification.send: already sent, skipping');
		return { status: 'sent', id };
	}

	const current_attempt = (existing?.current_attempt ?? 0) + 1;
	const max_attempts = existing?.max_attempts ?? notification.maxAttempts ?? 1;
	const expires_at = existing?.expires_at ?? notification.expiresAt ?? null;

	const dispatchStartedAt = Date.now();
	const result = await dispatch(channel, payload);
	const dispatchMs = Date.now() - dispatchStartedAt;

	if (result.ok) {
		await client
			.insert(notifications)
			.values({
				id,
				channel,
				payload,
				status: 'sent',
				current_attempt,
				max_attempts,
				expires_at,
			})
			.onConflictDoUpdate({
				target: notifications.id,
				set: { status: 'sent', current_attempt, updated_at: new Date() },
			});
		log.info(
			{ id, channel, attempt: current_attempt, dispatchMs },
			'notification.send: sent',
		);
		return { status: 'sent', id };
	}

	const reason = reasonOf(result.error);
	await client.insert(failures).values({
		id: ulid.generate(),
		notification_id: id,
		reason,
		attempt: current_attempt,
		source,
	});

	const exhausted = current_attempt >= max_attempts || isExpired(expires_at);

	if (exhausted) {
		await client.transaction(async (tx) => {
			await tx.insert(failed_notifications).values({
				id,
				channel,
				payload,
				status: 'failed',
				current_attempt,
				max_attempts,
				expires_at,
				created_at: existing?.created_at ?? new Date(),
				updated_at: new Date(),
			});
			await tx.delete(notifications).where(eq(notifications.id, id));
		});
		log.error(
			{ id, channel, attempt: current_attempt, dispatchMs, reason },
			'notification.send: failed, exhausted — moved to failed_notifications',
		);
		return { status: 'failed', id };
	}

	await client
		.insert(notifications)
		.values({
			id,
			channel,
			payload,
			status: 'pending',
			current_attempt,
			max_attempts,
			expires_at,
		})
		.onConflictDoUpdate({
			target: notifications.id,
			set: { status: 'pending', current_attempt, updated_at: new Date() },
		});
	log.warn(
		{ id, channel, attempt: current_attempt, max_attempts, dispatchMs, reason },
		'notification.send: failed, will retry',
	);
	return { status: 'failed', id };
}
