// One-off connectivity check. Run directly:
//   bun run src/scripts/check-kafka.ts
// Confirms (a) the broker accepts the configured auth, (b) every topic
// declared under config.kafka.producers/consumers actually exists.
import { config } from '@config';
import { kafka } from '@connections/kafka.connection';
import { log } from '@rniverse/utils';

try {
	await kafka.connect();
	log.info('Kafka connected');

	const topics = [
		...Object.values(config.kafka.producers).map((p) => p.topic),
		...Object.values(config.kafka.consumers).map((c) => c.topic),
	];
	const { topics: metadata } = await kafka
		.admin()
		.fetchTopicMetadata({ topics });
	log.info({ metadata }, `Topic metadata for: ${topics.join(', ')}`);
} catch (err) {
	log.error(err, 'Kafka connection check failed');
	process.exitCode = 1;
} finally {
	await kafka.close();
}
