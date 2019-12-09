
import { DatabasePool } from './pool';
import { formatDuration } from './format-duration';
import { getConnectionRole, onRelease } from './mysql';
import { Query, QueryCompiler, QueryIsRetryableCallback, onError, QueryConfig } from './query';
import { FieldInfo, PoolConnection, MysqlError } from 'mysql';
import { Logger } from '@viva-eng/logger';

export interface SelectQueryResult<Record> {
	results: Record[];
	fields: FieldInfo[];
}

export class SelectQuery<Params, Record> implements Query<Params, SelectQueryResult<Record>> {
	public readonly description: string;
	public readonly maxRetries: number;
	public readonly compile: QueryCompiler<Params>;
	public readonly isRetryable: QueryIsRetryableCallback;

	constructor(config: QueryConfig<Params>) {
		this.description = config.description;
		this.maxRetries = config.maxRetries || 0;
		this.compile = config.compile;
		this.isRetryable = config.isRetryable;
	}

	public async execute(params: Params, connection: PoolConnection, logger: Logger, retries?: number) : Promise<SelectQueryResult<Record>> {
		const startTime = process.hrtime();
		const compiled = await this.compile(params);

		const threadId = connection.threadId;
		const dbRole = getConnectionRole(connection);

		return new Promise((resolve, reject) => {
			logger.debug('Starting MySQL Query', { threadId, dbRole, query: this.description });
			logger.silly('Full MySQL Query', { threadId, dbRole, query: compiled });

			connection.query(compiled, (error, results, fields) => {
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
