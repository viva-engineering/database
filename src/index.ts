
import { Logger } from '@viva-eng/logger';
import { createPool, PoolConfig, PoolConnection, Pool, MysqlError } from 'mysql';
import { SelectQuery, WriteQuery, SelectQueryResult, WriteQueryResult, Query, QueryResult } from './query';

export { SelectQuery, WriteQuery, SelectQueryResult, WriteQueryResult, Query, QueryResult } from './query';

export interface ClusterConfig {
	master: PoolConfig,
	replica: PoolConfig,
	logger: Logger
}

export interface StreamingQueryCallback<T> {
	(record: T)
}

const holdTimers: WeakMap<PoolConnection, NodeJS.Timeout> = new WeakMap();

export class DatabasePool {
	protected readonly master: Pool;
	protected readonly replica: Pool;
	protected readonly logger: Logger;

	constructor(protected readonly config: ClusterConfig) {
		this.logger = config.logger;
		this.master = makePool(config.master, config.logger);
		this.replica = makePool(config.replica, config.logger);
	}

	getReadConnection() : Promise<PoolConnection> {
		return new Promise((resolve, reject) => {
			this.replica.getConnection((error, connection) => {
				if (error) {
					return reject(error);
				}

				resolve(connection);
			});
		});
	}

	getWriteConnection() : Promise<PoolConnection> {
		return new Promise((resolve, reject) => {
			this.master.getConnection((error, connection) => {
				if (error) {
					return reject(error);
				}

				resolve(connection);
			});
		});
	}

	async query<Q extends WriteQuery>(query: Q, params?: any) : Promise<WriteQueryResult>;
	async query<Q extends SelectQuery<R>, R extends object>(query: Q, params?: any) : Promise<SelectQueryResult<R>>;
	async query(query: Query, params?: any) {
		const isSelect = query instanceof SelectQuery;
		const connection = isSelect
			? await this.getReadConnection()
			: await this.getWriteConnection();

		const result = await this.runQuery(connection, query, params) as QueryResult;

		connection.release();

		return result;
	}

	runQuery<Q extends WriteQuery>(connection: PoolConnection, query: Q, params?: any) : Promise<WriteQueryResult>;
	runQuery<Q extends SelectQuery<R>, R extends object>(connection: PoolConnection, query: Q, params?: any) : Promise<SelectQueryResult<R>>;
	runQuery(connection: PoolConnection, query: Query, params?: any, retries?: number) {
		const isSelect = query instanceof SelectQuery;

		return new Promise(async (resolve, reject) => {
			const compiledQuery = query.compile(params);

			const retry = (retries: number) => {
				const backoff = (4 - retries) ** 2 * 500;

				setTimeout(() => {
					// @ts-ignore The fourth `retries` param is intentionally hidden, ignore the warning
					this.runQuery(connection, query, params, retries - 1).then(resolve, reject);
				}, backoff);
			};

			const onError = (error: MysqlError) => {
				if (error.fatal) {
					this.logger.warn('MySQL Query Error', {
						thread: connection.threadId,
						code: error.code,
						fatal: error.fatal,
						error: error.sqlMessage
					});

					onRelease(connection);
					connection.destroy();

					return reject(error);
				}

				const remainingRetries = retries == null ? 3 : retries;

				if (query.isRetryable(error)) {
					this.logger.warn('MySQL Query Error', {
						thread: connection.threadId,
						code: error.code,
						fatal: error.fatal,
						error: error.sqlMessage,
						retryable: true,
						retriesRemaining: remainingRetries
					});

					if (remainingRetries) {
						return retry(remainingRetries);
					}

					return reject(error);
				}

				this.logger.warn('MySQL Query Error', {
					thread: connection.threadId,
					code: error.code,
					fatal: error.fatal,
					error: error.sqlMessage,
					retryable: false
				});

				reject(error);
			};

			connection.query(compiledQuery, (error, results, fields) => {
				if (error) {
					return onError(error);
				}

				if (isSelect) {
					const result = {
						results,
						fields
					};

					query;

					return resolve(result);
				}

				return resolve(results);
			});
		});
	}
}

const makePool = (config: PoolConfig, logger: Logger) : Pool => {
	const pool = createPool(config);

	pool.on('connection', onConnection(logger));
	pool.on('acquire', onAcquire(logger));
	pool.on('release', onRelease);
	pool.on('enqueue', () => {
		logger.warn('No remaining connections available in the database pool, queue up query');
	});

	return pool;
};

const onConnection = (logger: Logger) => (connection: PoolConnection) => {
	connection.on('error', (error) => {
		logger.error('Unhandled MySQL Error', {
			thread: connection.threadId,
			code: error.code,
			fatal: error.fatal,
			error: error.sqlMessage
		});

		if (error.fatal) {
			onRelease(connection);
			connection.destroy();
		}
	});

	connection.on('enqueue', (query) => {
		if (query.sql) {
			const startTime = process.hrtime();
			// const queryFormat = 
		}
	});
};

const onAcquire = (logger: Logger) => (connection: PoolConnection) => {
	const onHeldTooLong = () => {
		logger.warn('MySQL connection held for over a minute', { threadId: connection.threadId });
	};

	holdTimers.set(connection, setTimeout(onHeldTooLong, 60000));
};

const onRelease = (connection: PoolConnection) => {
	const timer = holdTimers.get(connection);

	if (timer) {
		clearTimeout(timer);
		holdTimers.delete(connection);
	}
};
