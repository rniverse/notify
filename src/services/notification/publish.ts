import { config } from '@config';
import { kafka, pg } from '@connections';
import { notifications } from '@db/schema';
import { log } from '@rniverse/utils';
import type { NotificationChannel } from '@schema/types';

/**
 * Async submission entry point (spec §3, §8). Only place that writes to
 * Postgres *before* Kafka — and only on a publish failure. Happy path:
 * nothing written here, the consumer owns the first row.
 */
export async function submit(input: {
	id: string;
	channel: NotificationChannel;
	payload: unknown;
}): Promise<{ published: boolean }> {
	const { id, channel, payload } = input;
	const PRODUCER = config.kafka.producers.notifier;
	// Not `ready` while Kafka is down or still recovering — it follows the
	// connector's state — so don't wait on it, fall back to the pending row.
	const producer = kafka.producers.get(PRODUCER.name);

	if (producer.state !== 'ready') {
		log.warn(
			{ id },
			'Kafka producer not available — falling back to pending row',
		);
		await pg().insert(notifications).values({ id, channel, payload });
		return { published: false };
	}

	const startedAt = Date.now();
	try {
		await producer.getInstance().send({
			topic: PRODUCER.topic,
			messages: [{ value: JSON.stringify({ id, channel, payload }) }], // unkeyed — spec §10
		});
		log.info(
			{ id, publishMs: Date.now() - startedAt },
			'Kafka publish completed',
		);
		return { published: true };
	} catch (err) {
		log.error(
			{ err, publishMs: Date.now() - startedAt },
			'Kafka publish failed',
		);
		await pg().insert(notifications).values({ id, channel, payload });
		return { published: false };
	}
}
