import { t } from '@rniverse/utils';
import type { InferOutput } from 'valibot';

const emailPayload = t.object({
	to: t.array(t.pipe(t.string(), t.email())),
	subject: t.string(),
	html: t.string(),
	from: t.optional(t.string()), // omit to use Resend's account default
});

// Discriminated union on `channel` (spec §6) — one arm per channel. `sms`,
// `whatsapp`, etc. each get a new arm + a new provider wrapper later; nothing
// here changes shape for that.
const notification = t.variant('channel', [
	t.object({ channel: t.literal('email'), payload: emailPayload }),
]);

// Same shape plus `id` — the Kafka wire format (spec §8). Used by the
// consumer to reject a malformed/unrelated message on the topic outright
// rather than let it crash-loop the consumer (a shape that's invalid can
// never become valid on redelivery).
const notificationMessage = t.variant('channel', [
	t.object({
		id: t.string(),
		channel: t.literal('email'),
		payload: emailPayload,
	}),
]);

export type EmailPayload = InferOutput<typeof emailPayload>;
export type NotificationInput = InferOutput<typeof notification>;
export type NotificationMessage = InferOutput<typeof notificationMessage>;

export const schema$notification = {
	send: notification,
	message: notificationMessage,
};
