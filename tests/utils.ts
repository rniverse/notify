import { spyOn } from 'bun:test';
import { init } from '@app';
import { config } from '@config';
import { kafka, pg } from '@connections';
import { failed_notifications, failures, notifications } from '@db/schema';
import { sleep } from '@rniverse/utils/generic';
import { service$email } from '@services';
import { eq, sql } from 'drizzle-orm';

export type App = Awaited<ReturnType<typeof init>>;

// Build the app + open connections exactly once, shared across every test file
// (bun runs them in a single process).
let appPromise: Promise<App> | undefined;
export const getApp = (): Promise<App> => {
	appPromise ??= init();
	return appPromise;
};

export async function resetDb(): Promise<void> {
	await pg().execute(
		sql.raw('TRUNCATE notifications, failed_notifications, failures, config'),
	);
}

// --- HTTP ------------------------------------------------------------------

export type CallResult<T = any> = { res: Response; status: number; body: T };

export async function call<T = any>(
	app: App,
	method: string,
	path: string,
	opts: { body?: unknown } = {},
): Promise<CallResult<T>> {
	const headers: Record<string, string> = {};
	if (opts.body !== undefined) headers['content-type'] = 'application/json';
	const res = await app.handle(
		new Request(`http://localhost${path}`, {
			method,
			headers,
			body: opts.body === undefined ? undefined : JSON.stringify(opts.body),
		}),
	);
	const text = await res.text();
	let body: unknown;
	try {
		body = text ? JSON.parse(text) : undefined;
	} catch {
		body = text;
	}
	return { res, status: res.status, body: body as T };
}

// --- email -----------------------------------------------------------------

// Resend's test inboxes (resend.com/docs/dashboard/emails/send-test-emails):
// real API calls, no real mail. `+label` tags a scenario in the dashboard.
export const delivered = (label: string) => `delivered+${label}@resend.dev`;

// A sender on a domain the Resend account hasn't verified — Resend rejects
// the send with a real API error, the provider-failure path without mocking.
export const UNVERIFIED_FROM = 'Notify Test <notify-test@example.com>';

// Every send still goes to the real Resend API — spaced out, because Resend
// allows ~2 requests/s and a 429 would read as a provider failure. The spy
// also counts sends (`resend.mock.calls`).
const GAP_MS = 600;
let last = 0;
const send = service$email.send;
export const resend = spyOn(service$email, 'send').mockImplementation(
	async (payload) => {
		const wait = last + GAP_MS - Date.now();
		if (wait > 0) await sleep(wait);
		last = Date.now();
		return send(payload);
	},
);

export const email = (label: string, extra: { from?: string } = {}) => ({
	channel: 'email' as const,
	payload: {
		to: [delivered(label)],
		subject: `notify test: ${label}`,
		html: `<p>${label}</p>`,
		...extra,
	},
});

// --- waiting ---------------------------------------------------------------

/** Wait until `check` holds, or fail after `ms`. */
export async function until(
	check: () => boolean | Promise<boolean>,
	options: { ms?: number } = {},
): Promise<void> {
	const ms = options.ms ?? 15_000;
	const started = Date.now();
	while (!(await check())) {
		if (Date.now() - started > ms) throw new Error('until: timed out');
		await sleep(100);
	}
}

/**
 * Kafka fully up: connector, producer and consumer `ready`, and the consumer
 * group `Stable` with a member — only then does a publish get consumed (a new
 * group starts at the latest offset when it joins).
 */
export async function kafkaReady(): Promise<void> {
	const { producers, consumers } = config.kafka;
	const producer = kafka.producers.get(producers.notifier.name);
	const consumer = kafka.consumers.get(consumers.notifications.name);
	await until(
		() =>
			kafka.state === 'ready' &&
			producer.state === 'ready' &&
			consumer.state === 'ready',
		{ ms: 30_000 },
	);
	await until(
		async () => {
			const { groups } = await kafka
				.admin()
				.describeGroups([consumers.notifications.groupId]);
			const group = groups[0];
			return group?.state === 'Stable' && group.members.length > 0;
		},
		{ ms: 60_000 },
	);
}

// --- rows ------------------------------------------------------------------

export const rows = {
	async notification(id: string) {
		const [row] = await pg()
			.select()
			.from(notifications)
			.where(eq(notifications.id, id));
		return row;
	},
	async failed(id: string) {
		const [row] = await pg()
			.select()
			.from(failed_notifications)
			.where(eq(failed_notifications.id, id));
		return row;
	},
	failures(id: string) {
		return pg().select().from(failures).where(eq(failures.notification_id, id));
	},
};
