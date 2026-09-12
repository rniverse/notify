import { config } from '@config';
import { RedpandaConnector } from '@rniverse/connectors/redpanda';

export const redpanda = new RedpandaConnector({
	url: config.kafka.url,
	ssl: config.kafka.ssl,
	sasl: config.kafka.sasl,
});
