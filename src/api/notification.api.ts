import { pg } from '@connections';
import { failed_notifications, notifications } from '@db/schema';
import { enum$error } from '@enums/errors.enum';
import { ulid } from '@rniverse/utils';
import { schema$notification } from '@schema/api/notification.schema';
import { AppError, service$notification } from '@services';
import { ok } from '@utils';
import { eq } from 'drizzle-orm';
import Elysia from 'elysia';

export const notificationAPI = new Elysia({ prefix: '/notification' })
	.post(
		'/send/sync',
		async ({ body }) => {
			const id = ulid.generate();
			const result = await service$notification.send(
				{ id, channel: body.channel, payload: body.payload },
				{ source: 'sync' },
			);
			// Sync callers want a definite yes/no, not a status string to
			// inspect — a failed send here has already exhausted its
			// attempts (spec's retry semantics are for async only).
			if (result.status === 'failed') {
				throw new AppError(enum$error.key.NOTIFICATION_SEND_FAILED);
			}
			return ok(result);
		},
		{ body: schema$notification.send },
	)
	.post(
		'/send/async',
		async ({ body }) => {
			const id = ulid.generate();
			await service$notification.submit({
				id,
				channel: body.channel,
				payload: body.payload,
			});
			return ok({ status: 'accepted', id });
		},
		{ body: schema$notification.send },
	)
	.get('/status/:id', async ({ params }) => {
		const client = pg();

		// Sequential, not parallel — a hit against `notifications` (the hot
		// table) is the common case, so this avoids a wasted second query on
		// the happy path (spec §13 leaves the choice open).
		const [current] = await client
			.select()
			.from(notifications)
			.where(eq(notifications.id, params.id))
			.limit(1);
		if (current) return ok(current);

		const [failure] = await client
			.select()
			.from(failed_notifications)
			.where(eq(failed_notifications.id, params.id))
			.limit(1);
		if (failure) return ok(failure);

		// Plain 404 for an id not (yet) visible in either table — confirmed
		// fine, async callers are expected to wait/poll (spec §13).
		throw new AppError(enum$error.key.NOT_FOUND);
	});
