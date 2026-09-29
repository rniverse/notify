import type { Subscribers } from '@connections/setup/kafka.setup';
import { onEachMessage as notifications } from './notifications.consumer';

// Keyed the same as config.kafka.consumers — a consumer declared in config
// without a handler here (or vice versa) is a compile error, not a silent
// runtime skip. connections/setup/kafka.setup.ts runs each one.
const subscribers: Subscribers = { notifications };

export const consumers = { subscribers };
