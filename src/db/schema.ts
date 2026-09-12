import {
	index,
	integer,
	jsonb,
	pgTable,
	text,
	timestamp,
	uniqueIndex,
} from 'drizzle-orm/pg-core';

// Shared shape between `notifications` and `failed_notifications` (spec §5)
// — the latter is a terminal copy, not a status value on the former.
const notificationColumns = {
	id: text('id').primaryKey(), // ULID — the notification_id (spec §3)
	channel: text('channel').notNull(), // 'email' for now (spec §6)
	payload: jsonb('payload').notNull(), // shape depends on `channel`
	current_attempt: integer('current_attempt').notNull().default(0),
	max_attempts: integer('max_attempts').notNull().default(1),
	expires_at: timestamp('expires_at'), // null = never expires on time
};

export const notifications = pgTable('notifications', {
	...notificationColumns,
	status: text('status').notNull().default('pending'), // 'pending' | 'sent'
	created_at: timestamp('created_at').defaultNow().notNull(),
	updated_at: timestamp('updated_at').defaultNow().notNull(),
});

export const failed_notifications = pgTable('failed_notifications', {
	...notificationColumns,
	status: text('status').notNull().default('failed'),
	created_at: timestamp('created_at').notNull(), // carried over, not defaultNow
	updated_at: timestamp('updated_at').notNull(),
	moved_at: timestamp('moved_at').defaultNow().notNull(),
});

export const failures = pgTable(
	'failures',
	{
		id: text('id').primaryKey(),
		// Simple index, not a FK — the parent row can currently live in either
		// `notifications` or `failed_notifications` depending on state (spec §5).
		notification_id: text('notification_id').notNull(),
		reason: text('reason').notNull(),
		attempt: integer('attempt').notNull(),
		// Which caller triggered this attempt — audit metadata only, doesn't
		// change attempt/expiry logic (spec §7).
		source: text('source').notNull(), // 'async' | 'sync' | 'cron' | 'manual'
		created_at: timestamp('created_at').defaultNow().notNull(),
	},
	(t) => [index('failures_notification_id_idx').on(t.notification_id)],
);

export const config = pgTable(
	'config',
	{
		id: text('id').primaryKey(),
		key: text('key').notNull(),
		value: jsonb('value').notNull(),
		context: jsonb('context'), // present, meaning deferred (spec §5/§18)
		created_at: timestamp('created_at').defaultNow().notNull(),
		updated_at: timestamp('updated_at').defaultNow().notNull(),
	},
	// Unique INDEX, not a unique CONSTRAINT — deliberate (spec §5).
	(t) => [uniqueIndex('config_key_idx').on(t.key)],
);
