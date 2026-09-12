import { config } from '@config';
import { kafka } from '@connections';
import type { EachMessagePayload } from 'kafkajs';
import { onEachMessage as notifications } from './notifications.consumer';

type EachMessageHandler = (payload: EachMessagePayload) => Promise<void>;

// Keyed the same as config.kafka.consumers — a consumer declared in config
// without a handler here (or vice versa) is a compile error, not a silent
// runtime skip.
const subscribers: Record<
	keyof typeof config.kafka.consumers,
	EachMessageHandler
> = { notifications };

async function start(): Promise<void> {
	for (const [key, entry] of Object.entries(config.kafka.consumers)) {
		const onEachMessage = subscribers[key as keyof typeof subscribers];
		await kafka()
			?.consumers.get(entry.name)
			?.run({ eachMessage: onEachMessage });
	}
}

export const consumers = { subscribers, start };
