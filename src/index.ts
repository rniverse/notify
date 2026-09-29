import { createAPI } from '@api';
import { config } from '@config';
import { connections } from '@connections';
import { consumers } from '@consumers';
import { BOOTSTRAP_ERRORS } from '@enums/errors.enum';
import { boot, createApp, listen } from '@rniverse/shared/bootstrap';

export const init = async () => {
	// Before init() so the consumer's `connect` listener is there when it
	// connects (a late one would still run once — this just keeps it obvious).
	consumers.attach();
	await connections().init();

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
