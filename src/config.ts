import { readFileSync } from 'node:fs';
import type {
	RedpandaSASLConfig,
	RedpandaTLSConfig,
} from '@rniverse/connectors/redpanda';
import { boundedParseInt, environment as env } from '@rniverse/utils';
import { duration } from '@rniverse/utils/duration';

// Standard Kafka security protocols — only SASL_SSL used today, others
// supported since they're cheap to keep correct (spec.md §15).
type KafkaProtocol = 'PLAINTEXT' | 'SSL' | 'SASL_PLAINTEXT' | 'SASL_SSL';
const KAFKA_PROTOCOLS: readonly KafkaProtocol[] = [
	'PLAINTEXT',
	'SSL',
	'SASL_PLAINTEXT',
	'SASL_SSL',
];
function kafkaProtocol(): KafkaProtocol {
	const raw = env.get('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT');
	const protocol = KAFKA_PROTOCOLS.find((p) => p === raw);
	if (!protocol) {
		throw new Error(`Unsupported KAFKA_SECURITY_PROTOCOL: ${raw}`);
	}
	return protocol;
}

export const config = Object.freeze({
	get environment() {
		return env.get('NODE_ENV', 'development');
	},

	server: {
		get port() {
			return boundedParseInt(process.env.PORT, {
				min: 1,
				max: 65535,
				fallback: 3001,
			});
		},
		get host() {
			return env.get('HOST', '0.0.0.0');
		},
	},

	database: {
		get url() {
			return env.required('DATABASE_URL');
		},
	},

	resend: {
		get key() {
			return env.required('RESEND_API_KEY');
		},
		get from() {
			return env.get(
				'RESEND_FROM_EMAIL',
				'no-reply@notifications.rniverse.com',
			);
		},
	},

	// One entry per scheduled job (spec §7 lock keys: RETRY_CRON,
	// METRICS_CRON). `every`: duration string for a simple interval.
	// `cron`: cron expression for a fixed time of day.
	jobs: {
		retry: {
			get every() {
				const raw = env.get('NOTIFICATION_RETRY_EVERY', '5m');
				duration.toMs(raw); // throws on a malformed value
				return raw;
			},
		},
		metrics: {
			get cron() {
				const hour = boundedParseInt(
					process.env.NOTIFICATION_METRICS_CRON_HOUR,
					{
						min: 0,
						max: 23,
						fallback: 0,
					},
				);
				return `0 ${hour} * * *`;
			},
		},
	},

	// Translates KAFKA_* env vars into RedpandaConnectorConfig shape, not a
	// 1:1 pass-through (spec.md §15): protocol splits into ssl/sasl, mechanism
	// lowercased, CA cert read from disk rather than forwarded as a path.
	kafka: {
		get url() {
			return env.required('KAFKA_BOOTSTRAP_SERVERS');
		},
		get ssl(): RedpandaTLSConfig | undefined {
			const protocol = kafkaProtocol();
			if (protocol !== 'SSL' && protocol !== 'SASL_SSL') return undefined;
			const caPath = env.get('KAFKA_CA_CERTIFICATE');
			if (!caPath) return true;
			return { ca: [readFileSync(caPath, 'utf-8')] };
		},
		get sasl(): RedpandaSASLConfig | undefined {
			const protocol = kafkaProtocol();
			if (protocol !== 'SASL_PLAINTEXT' && protocol !== 'SASL_SSL')
				return undefined;
			const mechanism = env.required('KAFKA_SASL_MECHANISM').toLowerCase();
			if (
				mechanism !== 'plain' &&
				mechanism !== 'scram-sha-256' &&
				mechanism !== 'scram-sha-512'
			) {
				throw new Error(`Unsupported KAFKA_SASL_MECHANISM: ${mechanism}`);
			}
			return {
				mechanism,
				username: env.required('KAFKA_SASL_USERNAME'),
				password: env.required('KAFKA_SASL_PASSWORD'),
			};
		},
		// Keyed by a stable TS property (`producers.notifier`) so callers get
		// a typo-checked reference; `name` inside is the actual runtime/registry
		// key, env-driven so it can change per deployment without a code change.
		get producers() {
			return {
				notifier: {
					name: env.get('KAFKA_NOTIFIER_NAME', 'notifier'),
					topic: env.required('KAFKA_NOTIFIER_TOPIC_NAME'),
				},
			};
		},
		get consumers() {
			return {
				notifications: {
					name: env.get('KAFKA_NOTIFICATIONS_NAME', 'notifications'),
					groupId: env.get('KAFKA_NOTIFICATIONS_GROUP_ID', 'notify-consumer'),
					topic: env.required('KAFKA_NOTIFICATIONS_TOPIC_NAME'),
				},
			};
		},
	},
});
