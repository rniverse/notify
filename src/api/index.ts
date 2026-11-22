import { health } from '@connections';
import { Elysia, plugin } from '@rniverse/shared/elysia';
import { notificationAPI } from './notification.api';

// Everything under `/api`. No CORS, no auth guards —
// internal-only (spec §14).
export const createAPI = () =>
	new Elysia({ prefix: '/api' })
		.use(plugin.health(health))
		.use(notificationAPI);
