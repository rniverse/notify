import { createAPI } from '@api';
import { config } from '@config';
import { connections, setup$kafka } from '@connections';
import { consumers } from '@consumers';
import { BOOTSTRAP_ERRORS } from '@enums/errors.enum';
import { boot, createApp, listen } from '@rniverse/shared/bootstrap';

export const init = async () => {
	await connections().init();

	// Kafka connects in the background and keeps recovering — a broker
	// that's down only takes out async delivery, never boot or sync/status.
	setup$kafka.start({ subscribers: consumers.subscribers });

	const api = createAPI();
	return createApp({ api, errors: BOOTSTRAP_ERRORS });
};

export { listen };

// Only bootstrap a real server when run directly (`bun run src/index.ts`).
// When imported (e.g. from tests) this block is skipped — callers use
// `init()` to get the app and drive it with `app.handle()`.
if (import.meta.main) {
	boot(init, config.server, async () => {
		setup$kafka.stop();
		await connections().close();
	});
}
