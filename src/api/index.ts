import Elysia from 'elysia';
import { healthAPI } from './health.api';
import { notificationAPI } from './notification.api';

// Everything under `/api`. No CORS, no auth guards —
// internal-only (spec §14).
export const createAPI = () =>
	new Elysia({ prefix: '/api' }).use(healthAPI).use(notificationAPI);
