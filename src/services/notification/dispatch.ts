import type { EmailPayload } from '@schema/api/notification.schema';
import type { NotificationChannel } from '@schema/types';
import { service$email } from '../email.service';

// Channel → provider routing (spec §7). A new channel is a new arm here
// plus a new provider wrapper — nothing else changes.
export function dispatch(channel: NotificationChannel, payload: EmailPayload) {
	switch (channel) {
		case 'email':
			return service$email.send(payload);
		default:
			throw new Error(`dispatch: unknown channel ${channel}`);
	}
}
