// One-off connectivity check. Run directly:
//   bun run src/scripts/check-kafka.ts
// Confirms (a) the broker accepts the configured auth, (b) every topic
// declared under config.kafka.producers/consumers actually exists.
import { redpanda } from '@connections/redpanda.connection';
import { config } from '@config';
import { log } from '@rniverse/utils';

try {
	const admin = await redpanda.connect();
	log.info('Kafka connected');

	const topics = [
		...Object.values(config.kafka.producers).map((p) => p.topic),
		...Object.values(config.kafka.consumers).map((c) => c.topic),
	];
	const { topics: metadata } = await admin.fetchTopicMetadata({ topics });
	log.info({ metadata }, `Topic metadata for: ${topics.join(', ')}`);
} catch (err) {
	log.error(err, 'Kafka connection check failed');
	process.exitCode = 1;
} finally {
	await redpanda.close();
}
