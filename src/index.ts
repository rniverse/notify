import { createAPI } from '@api';
import { config } from '@config';
import { connections } from '@connections';
import { consumers } from '@consumers';
import { BOOTSTRAP_ERRORS } from '@enums/errors.enum';
import { boot, createApp, listen } from '@rniverse/shared/bootstrap';
import { log } from '@rniverse/utils';

export const init = async () => {
	await connections().init();

	// Best-effort, same as the producer (connections/index.ts) — a consumer
	// that fails to start doesn't take down sync/status, only async delivery.
	consumers.start().catch((err) => {
		log.error(err, 'Kafka consumer failed to start');
	});

	const api = createAPI();
	return createApp({ api, errors: BOOTSTRAP_ERRORS });
};

export { listen };

// Only bootstrap a real server when run directly (`bun run src/index.ts`).
// When imported (e.g. from tests) this block is skipped — callers use
// `init()` to get the app and drive it with `app.handle()`.
if (import.meta.main) {
	boot(init, config.server, () => connections().close());
}
