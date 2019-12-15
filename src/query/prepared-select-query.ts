
import { DatabasePool } from '../pool';
import { formatDuration } from '../format-duration';
import { getConnectionRole, onRelease } from '../mysql';
import { Query, QueryCompiler, ParamCompiler, QueryIsRetryableCallback, onError, PreparedQueryConfig } from './query';
import { FieldInfo, PoolConnection, MysqlError } from 'mysql2';
import { SelectQueryResult } from './results';
import { Logger } from '@viva-eng/logger';

export class PreparedSelectQuery<Params, Record> implements Query<Params, SelectQueryResult<Record>> {
	public readonly description: string;
	public readonly maxRetries: number;
	public readonly isRetryable: QueryIsRetryableCallback;
	public readonly prepared: string;
	public readonly prepareParams: ParamCompiler<Params>;

	constructor(config: PreparedQueryConfig<Params>) {
		this.description = config.description;
		this.maxRetries = config.maxRetries || 0;
		this.isRetryable = config.isRetryable;
		this.prepared = config.prepared;
		this.prepareParams = config.prepareParams;
	}

	public async execute(params: Params, connection: PoolConnection, logger: Logger, retries?: number) : Promise<SelectQueryResult<Record>> {
		const startTime = process.hrtime();
		const preparedParams = await this.prepareParams(params);

		const threadId = connection.threadId;
		const dbRole = getConnectionRole(connection);

		return new Promise((resolve, reject) => {
			logger.debug('Starting MySQL Query', { threadId, dbRole, query: this.description });
			logger.silly('Full MySQL Query', { threadId, dbRole, query: this.prepared });

			connection.execute(this.prepared, preparedParams, (error, results, fields) => {
				if (error) {
					return onError(error, startTime, this, params, logger, connection, retries);
				}

				logger.verbose('Completed MySQL Query', {
					threadId,
					dbRole,
					query: this.description,
					duration: formatDuration(process.hrtime(startTime))
				});

				resolve({ results, fields });
			});
		});
	}
}
