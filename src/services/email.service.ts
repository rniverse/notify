import { config } from '@config';
import { mail } from '@connections';
import { err, ok, type Result } from '@rniverse/utils/result';
import type { EmailPayload } from '@schema/api/notification.schema';

// Deliberately returns a Result rather than throwing, unlike aham's
// service$email — a provider failure here is an expected, recoverable
// outcome that notification.send() (§7) needs to inspect to decide
// pending vs. failed_notifications, not something that should bubble as an
// HTTP 500.
async function send(payload: EmailPayload): Promise<Result<{ id: string }>> {
	const client = mail();
	const { data, error } = await client.send({
		from: payload.from ?? config.resend.from,
		to: payload.to,
		subject: payload.subject,
		html: payload.html,
	});
	if (error) return err(error);
	return ok(data as { id: string });
}

export const service$email = { send };
