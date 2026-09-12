import { config } from '@config';
import { Resend } from 'resend';

const resend = new Resend(config.resend.key);

export const email = {
	getInstance: () => resend.emails,
};
