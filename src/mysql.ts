
import { DatabasePool } from './pool';
import { formatDuration } from './format-duration';
import { createPool, format, PoolConfig, PoolConnection, Pool, MysqlError } from 'mysql';
import { Logger } from '@viva-eng/logger';

export type Role = 'master' | 'replica';

export const enum TransactionType {
	ReadOnly = 'read only',
	ReadWrite = 'read write'
}

export interface TestResult {
	timeToConnection: [ number, number ],
	duration: [ number, number ]
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
const connectionPools: WeakMap<PoolConnection, DatabasePool> = new WeakMap();
const transactions: WeakMap<PoolConnection, TransactionType> = new WeakMap();

export const getConnectionRole = (connection: PoolConnection) => {
	return connectionRoles.get(connection);
};

export const setTransactionType = (connection: PoolConnection, type: TransactionType) => {
	transactions.set(connection, type);
};

export const getTransactionType = (connection: PoolConnection) : TransactionType => {
	return transactions.get(connection);
}

export const clearTransactionType = (connection: PoolConnection) => {
	transactions.delete(connection);
};

export const makePool = (role: Role, config: PoolConfig, logger: Logger, dbPool: DatabasePool) : Pool => {
	const pool = createPool(config);

	pool.on('connection', onConnection(role, logger, dbPool));
	pool.on('acquire', onAcquire(logger));
	pool.on('release', onRelease(logger));
	pool.on('enqueue', () => {
		logger.warn('No remaining connections available in the database pool, queue up query');
	});

	return pool;
};

export const closePool = (pool: Pool) : Promise<void> => {
	return new Promise((resolve, reject) => {
		pool.end((error) => {
			if (error) {
				return reject(error);
			}

			resolve();
		});
	});
};

export const onConnection = (role: Role, logger: Logger, dbPool: DatabasePool) => (connection: PoolConnection) => {
	connectionRoles.set(connection, role);
	connectionPools.set(connection, dbPool);

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

export const onAcquire = (logger: Logger) => (connection: PoolConnection) => {
	const role = connectionRoles.get(connection);
	const onHeldTooLong = () => {
		logger.warn('MySQL connection held for over a minute', { threadId: connection.threadId, dbRole: role });
	};

	holdTimers.set(connection, setTimeout(onHeldTooLong, 60000));
};

export const onRelease = (logger: Logger) => (connection: PoolConnection) => {
	const role = connectionRoles.get(connection);
	const pool = connectionPools.get(connection);
	const transactionType = transactions.get(connection);

	if (transactionType != null) {
		logger.error('A connection was released that still had an open transaction; Forcing rollback', {
			threadId: connection.threadId,
			dbRole: role,
			transactionType
		});

		pool.rollbackTransaction(connection);
	}

	logger.silly('MySQL connection released', { threadId: connection.threadId, dbRole: role });

	const timer = holdTimers.get(connection);

	if (timer) {
		clearTimeout(timer);
		holdTimers.delete(connection);
	}
};

export const healthcheck = async (logger: Logger, url: string, pool: Pool) : Promise<HealthcheckResult> => {
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

export const testPool = (logger:Logger, url: string, pool: Pool) : Promise<TestResult> => {
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
