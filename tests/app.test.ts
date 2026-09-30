import { describe, expect, it } from 'bun:test';
import { enum$error } from '@enums/errors.enum';
import { call, getApp } from './utils';

describe('error envelope', () => {
	it('unknown route → 404 in the standard envelope', async () => {
		const app = await getApp();
		const { status, body } = await call(app, 'GET', '/api/does/not/exist');
		expect(status).toBe(404);
		expect(body.ok).toBe(false);
		expect(body.error.code).toBe(enum$error.codes.NOT_FOUND);
		expect(Object.keys(body).sort()).toEqual(['error', 'ok']);
		expect(Object.keys(body.error).sort()).toEqual(['code', 'message']);
	});

	it('validation failure → 422 VALIDATION_FAILED envelope', async () => {
		const app = await getApp();
		const { status, body } = await call(
			app,
			'POST',
			'/api/notification/send/sync',
			{ body: {} },
		);
		expect(status).toBe(422);
		expect(body.error.code).toBe(enum$error.codes.VALIDATION_FAILED);
	});

	it('every response carries x-request-id', async () => {
		const app = await getApp();
		const { res } = await call(app, 'GET', '/api/does/not/exist');
		expect(res.headers.get('x-request-id')).toBeTruthy();
	});
});
