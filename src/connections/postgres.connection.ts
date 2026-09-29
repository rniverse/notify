import { config } from '@config';
import { PostgresConnector } from '@rniverse/connectors/postgres';

export const postgres = new PostgresConnector({
	name: 'postgres',
	appName: config.appName,
	url: config.database.url,
});
