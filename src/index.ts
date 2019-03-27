
import { Logger } from '@viva-eng/logger';
import { createPool, PoolConfig, PoolConnection, Pool, MysqlError } from 'mysql';
import { SelectQuery, WriteQuery, SelectQueryResult, WriteQueryResult, Query, QueryResult } from './query';
import { formatDuration } from './format-duration';

export { SelectQuery, WriteQuery, SelectQueryResult, WriteQueryResult, Query, QueryResult } from './query';

type Role = 'master' | 'replica';

export interface ClusterConfig {
	master: PoolConfig,
	replica: PoolConfig,
	logger: Logger
}

export interface HealthcheckResult {
	available: boolean,
	url: string,
	timeToConnection?: string,
	duration?: string,
	warning?: string,
	info?: string
}

export interface HealthcheckResults {
	master: HealthcheckResult,
	replica: HealthcheckResult
}

const holdTimers: WeakMap<PoolConnection, NodeJS.Timeout> = new WeakMap();
const connectionRoles: WeakMap<PoolConnection, Role> = new WeakMap();

export class DatabasePool {
	protected readonly master: Pool;
	protected readonly replica: Pool;
	protected readonly logger: Logger;
	public readonly masterUrl: string;
	public readonly replicaUrl: string;

	constructor(protected readonly config: ClusterConfig) {
		this.logger = config.logger;
		this.master = makePool('master', config.master, config.logger);
		this.replica = makePool('replica', config.replica, config.logger);
		this.masterUrl = `mysql://${config.master.host}:${config.master.port}/${config.master.database}`;
		this.replicaUrl = `mysql://${config.master.host}:${config.master.port}/${config.master.database}`;
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
		const startTime = process.hrtime();
		const isSelect = query instanceof SelectQuery;
		const role = connectionRoles.get(connection);

		this.logger.verbose('Starting MySQL Query', {
			threadId: connection.threadId,
			dbRole: role,
			query: query.toString()
		});

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
				const duration = formatDuration(process.hrtime(startTime));

				if (error.fatal) {
					this.logger.warn('MySQL Query Error', {
						threadId: connection.threadId,
						dbRole: role,
						code: error.code,
						fatal: error.fatal,
						error: error.sqlMessage,
						query: query.toString(),
						duration
					});

					onRelease(this.logger)(connection);
					connection.destroy();

					return reject(error);
				}

				const remainingRetries = retries == null ? 3 : retries;

				if (query.isRetryable(error)) {
					this.logger.warn('MySQL Query Error', {
						threadId: connection.threadId,
						dbRole: role,
						code: error.code,
						fatal: error.fatal,
						error: error.sqlMessage,
						query: query.toString(),
						duration,
						retryable: true,
						retriesRemaining: remainingRetries
					});

					if (remainingRetries) {
						return retry(remainingRetries);
					}

					return reject(error);
				}

				this.logger.warn('MySQL Query Error', {
					threadId: connection.threadId,
					dbRole: role,
					code: error.code,
					fatal: error.fatal,
					error: error.sqlMessage,
					query: query.toString(),
					duration,
					retryable: false
				});

				reject(error);
			};

			connection.query(compiledQuery, (error, results, fields) => {
				if (error) {
					return onError(error);
				}

				const duration = formatDuration(process.hrtime(startTime));

				this.logger.verbose('MySQL Query Complete', {
					threadId: connection.threadId,
					dbRole: role,
					duration,
					query: query.toString()
				});

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

	healthcheck() : Promise<HealthcheckResults> {
		return new Promise(async (resolve, reject) => {
			resolve({
				master: await healthcheck(this.logger, this.masterUrl, this.master),
				replica: await healthcheck(this.logger, this.replicaUrl, this.replica)
			});
		});
	}

	destroy() : Promise<void[]> {
		return Promise.all([
			closePool(this.master),
			closePool(this.replica)
		]);
	}
}

const makePool = (role: Role, config: PoolConfig, logger: Logger) : Pool => {
	const pool = createPool(config);

	pool.on('connection', onConnection(role, logger));
	pool.on('acquire', onAcquire(logger));
	pool.on('release', onRelease(logger));
	pool.on('enqueue', () => {
		logger.warn('No remaining connections available in the database pool, queue up query');
	});

	return pool;
};

const closePool = (pool: Pool) : Promise<void> => {
	return new Promise((resolve, reject) => {
		pool.end((error) => {
			if (error) {
				return reject(error);
			}

			resolve();
		});
	});
};

const onConnection = (role: Role, logger: Logger) => (connection: PoolConnection) => {
	connectionRoles.set(connection, role);

	logger.silly('New MySQL connection established', { threadId: connection.threadId, dbRole: role });

	connection.on('error', (error) => {
		logger.error('Unhandled MySQL Error', {
			threadId: connection.threadId,
			dbRole: role,
			code: error.code,
			fatal: error.fatal,
			error: error.sqlMessage
		});

		if (error.fatal) {
			onRelease(logger)(connection);
			connection.destroy();
		}
	});
};

const onAcquire = (logger: Logger) => (connection: PoolConnection) => {
	const role = connectionRoles.get(connection);
	const onHeldTooLong = () => {
		logger.warn('MySQL connection held for over a minute', { threadId: connection.threadId, dbRole: role });
	};

	holdTimers.set(connection, setTimeout(onHeldTooLong, 60000));
};

const onRelease = (logger: Logger) => (connection: PoolConnection) => {
	const role = connectionRoles.get(connection);

	logger.silly('New MySQL connection released', { threadId: connection.threadId, dbRole: role });

	const timer = holdTimers.get(connection);

	if (timer) {
		clearTimeout(timer);
		holdTimers.delete(connection);
	}
};

const healthcheck = async (logger: Logger, url: string, pool: Pool) : Promise<HealthcheckResult> => {

	try {
		const result = await testPool(logger, url, pool);
		const status: HealthcheckResult = {
			url,
			available: true,
			timeToConnection: formatDuration(result.timeToConnection),
			duration: formatDuration(result.duration)
		};

		if (result.duration[0] > 0 || result.duration[1] / 10e5 > 50) {
			status.warning = 'Connection slower than 50ms';
		}

		return status;
	}

	catch (error) {
		return {
			url,
			available: false,
			info: error.code
		};
	}
};

interface TestResult {
	timeToConnection: [ number, number ],
	duration: [ number, number ]
}

const testPool = (logger:Logger, url: string, pool: Pool) : Promise<TestResult> => {
	return new Promise((resolve, reject) => {
		const startTime = process.hrtime();

		pool.getConnection((error, connection) => {
			const timeToConnection = process.hrtime(startTime);

			if (error) {
				return reject(error);
			}
			
			const role = connectionRoles.get(connection);

			connection.query('select version() as version', (error) => {
				const duration = process.hrtime(startTime);

				logger.verbose('MySQL Healthcheck Complete', {
					threadId: connection.threadId,
					dbRole: role,
					duration: formatDuration(duration)
				});

				connection.release();

				if (error) {
					return reject(error);
				}

				resolve({
					timeToConnection: timeToConnection,
					duration: duration
				});
			});
		});
	});
};
