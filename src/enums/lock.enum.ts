import { sync$seq } from '@rniverse/utils';

// Postgres advisory-lock keys for cron jobs that must run at most once
// across all instances (spec.md §7). Sequential, not hardcoded — same
// discipline as errors.enum.ts's codes.
const list = [
	['RETRY_CRON', 'Sweeps pending notifications for retry'],
	['METRICS_CRON', 'Sends the nightly failure report'],
] as const;

export type LockKey = (typeof list)[number][0];

const next_key = sync$seq.next.seq();
const keys = Object.fromEntries(list.map(([k]) => [k, next_key()])) as Record<
	LockKey,
	bigint
>;

export const enum$lock = { keys };
