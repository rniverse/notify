import { config } from '@config';
import { log } from '@rniverse/utils';
import { duration } from '@rniverse/utils/duration';
import type { EachMessagePayload } from 'kafkajs';
import { consumers, kafka, producers } from '../kafka.connection';

type ConsumerKey = keyof typeof config.kafka.consumers;
export type Subscribers = Record<
	ConsumerKey,
	(payload: EachMessagePayload) => Promise<void>
>;

// A consumer is only `ready` once it has joined its group, and `connecting`
// from `run()` until then — only these states mean nothing is running it.
const STOPPED = new Set(['idle', 'failed', 'closed']);

/** connect → subscribe → run: a (re)connected consumer has no subscription. */
async function run(key: ConsumerKey, subscribers: Subscribers): Promise<void> {
	const link = consumers[key];
	try {
		await link.connect();
		const consumer = link.getInstance();
		await consumer.subscribe({ topic: config.kafka.consumers[key].topic });
		await consumer.run({ eachMessage: subscribers[key] });
	} catch (err) {
		log.warn(err, `Kafka consumer '${link.name}' failed to start`);
		// Close so the next pass opens a fresh consumer instead of finding
		// this one half-started.
		await link.close();
	}
}

/**
 * Kafka is optional (spec §8) and never blocks boot. One pass: health-check
 * the cluster (which reconnects it when down), then connect any producer
 * that isn't ready and start any consumer that isn't running. Never throws.
 */
async function recover(subscribers: Subscribers): Promise<void> {
	const health = await kafka.health();
	if (!health.ok) return;
	const keys = Object.keys(subscribers) as ConsumerKey[];
	await Promise.all([
		...Object.values(producers)
			.filter((producer) => producer.state !== 'ready')
			.map((producer) =>
				producer.connect().catch((err) => {
					log.warn(err, `Kafka producer '${producer.name}' failed to connect`);
				}),
			),
		...keys
			.filter((key) => STOPPED.has(consumers[key].state))
			.map((key) => run(key, subscribers)),
	]);
}

let timer: ReturnType<typeof setInterval> | undefined;
let running = false;

async function tick(subscribers: Subscribers): Promise<void> {
	// A pass against a down broker can outlast the interval — skip, don't pile up.
	if (running) return;
	running = true;
	try {
		await recover(subscribers);
	} finally {
		running = false;
	}
}

/**
 * First pass now (not awaited), then every `config.kafka.recoverEvery`.
 * `subscribers` are the message handlers, one per configured consumer.
 */
function start(options: { subscribers: Subscribers }): void {
	if (timer) return;
	const pass = () => tick(options.subscribers);
	void pass();
	timer = setInterval(pass, duration.toMs(config.kafka.recoverEvery));
	timer.unref?.();
}

/** Stop recovering — call before closing connections, or a pass reopens them. */
function stop(): void {
	clearInterval(timer);
	timer = undefined;
}

export const setup$kafka = { start, stop };
