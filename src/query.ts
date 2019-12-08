
import { DatabasePool } from './index';
import { formatDuration } from './format-duration';
import { getConnectionRole, onRelease } from './mysql';
import { PoolConnection, MysqlError } from 'mysql';
import { Logger } from '@viva-eng/logger';

export interface Query<Params, Result> {
	description: string;
	maxRetries: number;
	compile: QueryCompiler<Params>;
	isRetryable: QueryIsRetryableCallback;
	execute(params: Params, connection: PoolConnection, logger: Logger, retries?: number) : Promise<Result>;
}

export interface QueryConfig<Params> {
	description: string;
	compile: QueryCompiler<Params>;
	maxRetries?: number;
	isRetryable?: QueryIsRetryableCallback;
}

export interface QueryCompiler<P> {
	(params: P) : string;
}

export interface QueryIsRetryableCallback {
	(error: MysqlError) : boolean;
}

export type QueryParams<Q extends Query<any, any>> = Q extends Query<infer P, any> ? P : never;
export type QueryResult<Q extends Query<any, any>> = Q extends Query<any, infer R> ? R : never;

export const onError = <P, R>(error: MysqlError, startTime: [ number, number ], query: Query<P, R>, params: P, logger: Logger, connection: PoolConnection, retries?: number) : Promise<R> => {
	return new Promise((resolve, reject) => {
		const threadId = connection.threadId;
		const dbRole = getConnectionRole(connection);

		if (error.fatal) {
			logger.warn('MySQL Query Error', {
				threadId,
				dbRole,
				query: query.description,
				duration: formatDuration(process.hrtime(startTime)),
				code: error.code,
				fatal: error.fatal,
				error: error.sqlMessage
			});

			// On fatal errors, make sure the connection is properly destroyed
			onRelease(logger)(connection);
			connection.destroy();

			return reject(error);
		}

		if (query.isRetryable(error)) {
			const remainingRetries = retries == null ? query.maxRetries : retries;

			logger.warn('MySQL Query Error', {
				threadId,
				dbRole,
				query: query.description,
				duration: formatDuration(process.hrtime(startTime)),
				retryable: true,
				remainingRetries,
				code: error.code,
				fatal: error.fatal,
				error: error.sqlMessage
			});

			if (remainingRetries) {
				const backoff = (query.maxRetries - retries) ** 2 * 500;

				setTimeout(() => {
					query.execute(params, connection, logger, retries).then(resolve, reject);
				}, backoff);
			}

			return reject(error);
		}

		logger.warn('MySQL Query Error', {
			threadId,
			dbRole,
			query: query.description,
			duration: formatDuration(process.hrtime(startTime)),
			retryable: true,
			code: error.code,
			fatal: error.fatal,
			error: error.sqlMessage
		});

		return reject(error);
	});
};
