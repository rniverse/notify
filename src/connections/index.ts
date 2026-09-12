import { config } from '@config';
import { createRegistry } from '@rniverse/shared/registry';
import { email } from './email.connection';
import { postgres } from './postgres.connection';
import { redpanda } from './redpanda.connection';

const registry = createRegistry({
	connections: [
		{ name: 'postgres', connector: postgres, required: true },
		// redpanda is `required: false` — only the async send route and the
		// consumer depend on it (spec §8); sync/status keep working without it.
		{ name: 'redpanda', connector: redpanda, required: false },
	],
	kafka: {
		connector: redpanda,
		producers: config.kafka.producers,
		consumers: config.kafka.consumers,
	},
});

export const connections = registry.connections;
export const kafka = registry.kafka;

export const pg = () => postgres.getInstance();
export const mail = () => email.getInstance();
