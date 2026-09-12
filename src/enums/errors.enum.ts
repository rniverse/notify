import { createErrorEnum } from '@rniverse/shared/error';

// Pruned to what notify's API surface actually uses today (§10, §13 of
// spec.md) — grow this list as real error cases show up, not speculatively.
const list = [
	['NOT_FOUND', 'Resource not found', 404],
	['VALIDATION_FAILED', 'Validation failed', 422],
	['INTERNAL_ERROR', 'Internal server error', 500],
	['NOTIFICATION_SEND_FAILED', 'Failed to send notification', 502],
] as const;

const { key, messages, codes, status, spec, AppError } = createErrorEnum(list);

export type ErrorKey = keyof typeof key;
export const enum$error = { key, messages, codes, status };
export { AppError };

// What createApp()'s `errors` option needs, ready-built — see src/index.ts.
export const BOOTSTRAP_ERRORS = {
	AppError,
	NOT_FOUND: spec('NOT_FOUND'),
	VALIDATION_FAILED: spec('VALIDATION_FAILED'),
	INTERNAL_ERROR: spec('INTERNAL_ERROR'),
};
