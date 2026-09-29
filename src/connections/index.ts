import { createRegistry } from '@rniverse/shared/registry';
import { email } from './email.connection';
import { kafka } from './kafka.connection';
import { postgres } from './postgres.connection';

const registry = createRegistry({
	connections: [
		{ name: 'postgres', connector: postgres, required: true },
		// Kafka is `required: false` — only the async send route and the
		// consumer depend on it (spec §8); sync/status keep working without it.
		// The registry connects an optional connection in the background, so a
		// slow or unreachable broker never holds up boot.
		{ name: 'kafka', connector: kafka, required: false },
	],
});

export const connections = registry.connections;

export { kafka } from './kafka.connection';

export const pg = () => postgres.getInstance();
export const mail = () => email.getInstance();
