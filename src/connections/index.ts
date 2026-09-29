import { createRegistry } from '@rniverse/shared/registry';
import type { Result } from '@rniverse/utils/result';
import { email } from './email.connection';
import { kafka } from './kafka.connection';
import { postgres } from './postgres.connection';

const registry = createRegistry({
	connections: [
		{ name: 'postgres', connector: postgres, required: true },
		// Kafka is `required: false` — only the async send route and the
		// consumer depend on it (spec §8); sync/status keep working without it.
		// Its connect is a no-op here so a slow or unreachable broker never
		// holds up boot: setup/kafka.setup.ts connects and recovers it. The registry
		// still reports its health and closes it — except while still `idle`
		// (init's own health report, before setup$kafka starts), where a
		// real check would be a connect attempt that boot waits on.
		{
			name: 'kafka',
			required: false,
			connector: {
				connect: async () => {},
				close: () => kafka.close(),
				health: async (): Promise<Result<unknown>> =>
					kafka.state === 'idle'
						? { ok: false, error: new Error('kafka: not connected yet') }
						: kafka.health(),
			},
		},
	],
});

export const connections = registry.connections;

export { consumers, kafka, producers } from './kafka.connection';
export { setup$kafka } from './setup/kafka.setup';

export const pg = () => postgres.getInstance();
export const mail = () => email.getInstance();
