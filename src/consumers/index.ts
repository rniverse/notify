import { config } from '@config';
import { kafka } from '@connections';
import { log } from '@rniverse/utils';
import { retry } from '@rniverse/utils/resilience';
import type { EachMessagePayload } from 'kafkajs';
import { onEachMessage as notifications } from './notifications.consumer';

type ConsumerKey = keyof typeof config.kafka.consumers;
type EachMessageHandler = (payload: EachMessagePayload) => Promise<void>;

// Keyed the same as config.kafka.consumers — a consumer declared in config
// without a handler here (or vice versa) is a compile error, not a silent
// runtime skip.
const subscribers: Record<ConsumerKey, EachMessageHandler> = { notifications };

/**
 * Subscribe + run each consumer whenever a new kafkajs consumer connects —
 * the first time, and after a fresh reconnect. A kafkajs crash-restart keeps
 * its subscription, so this doesn't run again for it.
 */
let attached = false;

function attach(): void {
	if (attached) return; // init() may run more than once (tests)
	attached = true;
	for (const key of Object.keys(subscribers) as ConsumerKey[]) {
		const entry = config.kafka.consumers[key];
		const link = kafka.consumers.get(entry.name);
		link.on('connect', {
			name: 'subscribe',
			handler: async () => {
				const consumer = link.getInstance();
				// A topic can briefly be unknown to a broker that just started.
				await retry(() => consumer.subscribe({ topic: entry.topic }), {
					attempts: 5,
					backoff: { strategy: 'exponential', min: 500, max: 5_000 },
					on: {
						retry: ({ attempt }) =>
							log.warn(
								`Kafka consumer '${link.name}': subscribe retry ${attempt}`,
							),
					},
				});
				await consumer.run({ eachMessage: subscribers[key] });
			},
		});
	}
}

export const consumers = { subscribers, attach };
