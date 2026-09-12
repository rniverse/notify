import { send } from './orchestrator';
import { submit } from './publish';

export const service$notification = { send, submit };

export type {
	NotificationChannel,
	NotificationInput,
	SendOpts,
	SendOutcome,
	SendSource,
} from '@schema/types';
