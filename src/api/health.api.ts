import { connections } from '@connections';
import Elysia from 'elysia';

export const healthAPI = new Elysia().get('/health', () =>
	connections().health(),
);
