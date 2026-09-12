import type { EmailPayload } from './api/notification.schema';

export type NotificationChannel = 'email';

export type NotificationInput = {
	id: string;
	channel: NotificationChannel;
	payload: EmailPayload;
	// Only apply on the first attempt for `id` — an existing row's own
	// max_attempts/expires_at win, so a retry can't change the terms.
	maxAttempts?: number;
	expiresAt?: Date;
};

export type SendSource = 'async' | 'sync' | 'cron' | 'manual';
export type SendOpts = { source?: SendSource };
export type SendOutcome = { status: 'sent' | 'failed'; id: string };
