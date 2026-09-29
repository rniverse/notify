import { config } from '@config';
import { KafkaConnector } from '@rniverse/connectors/kafka';

export const kafka = new KafkaConnector({
	name: 'kafka',
	appName: config.appName,
	brokers: config.kafka.brokers,
	ssl: config.kafka.ssl,
	sasl: config.kafka.sasl,
});

// Links only — nothing connects here. setup/kafka.setup.ts connects them after boot
// and brings them back when they fail (spec §8).
export const producers = {
	notifier: kafka.producer({ name: config.kafka.producers.notifier.name }),
};

export const consumers = {
	notifications: kafka.consumer({
		name: config.kafka.consumers.notifications.name,
		groupId: config.kafka.consumers.notifications.groupId,
	}),
};
