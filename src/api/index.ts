import Elysia from 'elysia';
import { notificationAPI } from './notification.api';

// No `/api` prefix, no CORS, no auth guards — internal-only (spec §14).
export const createAPI = () => new Elysia().use(notificationAPI);
