import { config } from '@config';
import { KafkaConnector } from '@rniverse/connectors/kafka';
import { duration } from '@rniverse/utils/duration';

// The producer and consumer are declared here; the connector connects them once
// it's ready, and re-checks itself every KAFKA_RECOVER_EVERY (spec §8).
// consumers/index.ts subscribes + runs each consumer on its `connect`.
export const kafka = new KafkaConnector({
	name: 'kafka',
	appName: config.appName,
	brokers: config.kafka.brokers,
	ssl: config.kafka.ssl,
	sasl: config.kafka.sasl,
	recover: { every: duration.toMs(config.kafka.recoverEvery) },
	producers: [{ name: config.kafka.producers.notifier.name }],
	consumers: [
		{
			name: config.kafka.consumers.notifications.name,
			groupId: config.kafka.consumers.notifications.groupId,
		},
	],
});
