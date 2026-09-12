import { config } from '@config';
import { SQLConnector } from '@rniverse/connectors/sql';

export const postgres = new SQLConnector({ url: config.database.url });
