
import { Logger } from '@viva-eng/logger';
import { formatDuration } from './format-duration';
import { createPool, format, PoolConfig, PoolConnection, Pool, MysqlError } from 'mysql';
import { Query } from './query';
import { SelectQuery } from './select-query';
import {
	makePool,
	getConnectionRole,
	onRelease,
	healthcheck,
	HealthcheckResults,
	closePool,
	TransactionType,
	getTransactionType,
	setTransactionType,
	clearTransactionType
} from './mysql';

export interface ClusterConfig {
	master: PoolConfig,
	replica: PoolConfig,
	logger: Logger
}

export class DatabasePool {
	protected readonly master: Pool;
	protected readonly replica: Pool;

	public readonly logger: Logger;
	public readonly masterUrl: string;
	public readonly replicaUrl: string;

	constructor(protected readonly config: ClusterConfig) {
		this.logger = config.logger;
		this.master = makePool('master', config.master, config.logger, this);
		this.replica = makePool('replica', config.replica, config.logger, this);
		this.masterUrl = `mysql://${config.master.host}:${config.master.port}/${config.master.database}`;
		this.replicaUrl = `mysql://${config.master.host}:${config.master.port}/${config.master.database}`;
	}

	/**
	 * Obtains a read-only connection to the database
	 */
	public getReadConnection() : Promise<PoolConnection> {
		return new Promise((resolve, reject) => {
			this.replica.getConnection((error, connection) => {
				if (error) {
					return reject(error);
				}

				resolve(connection);
			});
		});
	}

	/**
	 * Obtains a read-write connection to the database
	 */
	public getWriteConnection() : Promise<PoolConnection> {
		return new Promise((resolve, reject) => {
			this.master.getConnection((error, connection) => {
				if (error) {
					return reject(error);
				}

				resolve(connection);
			});
		});
	}

	/**
	 * Execute a single query against the database
	 *
	 * @param query The query to run
	 * @param params Any params to interpolate into the query
	 */
	public async query<P, R>(query: Query<P, R>, params?: P) : Promise<R> {
		const connection = query instanceof SelectQuery
			? await this.getReadConnection()
			: await this.getWriteConnection();

		try {
			const result = await this.runQuery<P, R>(connection, query, params);

			connection.release();

			return result;
		}

		catch (error) {
			connection.release();

			throw error;
		}
	}

	/**
	 * Execute a single query against the database using the provided connection. The
	 * connection will be left open after execution.
	 *
	 * @param connection The DB connection to execute the connection on
	 * @param query The query to run
	 * @param params Any params to interpolate into the query
	 */
	public runQuery<P, R>(connection: PoolConnection, query: Query<P, R>, params?: P) : Promise<R> {
		return query.execute(params, connection, this.logger);
	}

	/**
	 * Executes a healthcheck ping against the database(s)
	 */
	public healthcheck() : Promise<HealthcheckResults> {
		return new Promise(async (resolve, reject) => {
			resolve({
				master: await healthcheck(this.logger, this.masterUrl, this.master),
				replica: await healthcheck(this.logger, this.replicaUrl, this.replica)
			});
		});
	}

	/**
	 * Closes the database pool(s) and destroys all connections
	 */
	public destroy() : Promise<void[]> {
		return Promise.all([
			closePool(this.master),
			closePool(this.replica)
		]);
	}

	/**
	 * Obtains a new database connection and starts a transaction
	 *
	 * @param transactionType The type of transaction to open (ie. readonly or read-write)
	 */
	public async startTransaction(transactionType: TransactionType = TransactionType.ReadOnly) : Promise<PoolConnection> {
		const connection = transactionType === TransactionType.ReadOnly
			? await this.getReadConnection()
			: await this.getWriteConnection();

		const query = transactionType === TransactionType.ReadOnly
			? 'start transaction read only'
			: 'start transaction read write';

		const dbRole = getConnectionRole(connection);
		const threadId = connection.threadId;

		setTransactionType(connection, transactionType);

		this.logger.debug('Starting new MySQL transaction', { threadId, dbRole, transactionType });

		return new Promise((resolve, reject) => {
			connection.query(query, (error) => {
				if (error) {
					this.logger.error('Failed to start MySQL transaction', { threadId, dbRole, transactionType, error });

					return reject(error);
				}

				resolve(connection);
			});
		});
	}

	/**
	 * Commits the transaction on the given connection
	 *
	 * @param connection The DB connection to commit on
	 */
	public commitTransaction(connection: PoolConnection) : Promise<void> {
		const dbRole = getConnectionRole(connection);
		const transactionType = getTransactionType(connection);
		const threadId = connection.threadId;

		return new Promise((resolve, reject) => {
			if (transactionType == null) {
				this.logger.error('Attempted to commit a transaction when none was running', { threadId, dbRole });

				return reject(new Error('Cannot commit transaction; none is running'));
			}

			this.logger.debug('Commiting MySQL transaction', { threadId, dbRole, transactionType });

			connection.query('commit', (error) => {
				if (error) {
					this.logger.warn('Failed to commit transaction', { threadId, dbRole, error });

					return reject(error);
				}

				this.logger.debug('Commit successful', { threadId, dbRole, transactionType });

				clearTransactionType(connection);
				resolve();
			});
		});
	}

	/**
	 * Rolls back the transaction on the given connection
	 *
	 * @param connection The DB connection to rollback on
	 */
	public rollbackTransaction(connection: PoolConnection) : Promise<void> {
		const dbRole = getConnectionRole(connection);
		const transactionType = getTransactionType(connection);
		const threadId = connection.threadId;

		return new Promise((resolve, reject) => {
			if (transactionType == null) {
				this.logger.error('Attempted to rollback a transaction when none was running', { threadId, dbRole });

				return reject(new Error('Cannot rollback transaction; none is running'));
			}

			this.logger.debug('Rolling back MySQL transaction', { threadId, dbRole, transactionType });

			connection.query('rollback', (error) => {
				if (error) {
					this.logger.warn('Failed to rollback transaction', { threadId, dbRole, error });

					return reject(error);
				}

				this.logger.debug('Rollback successful', { threadId, dbRole, transactionType });

				clearTransactionType(connection);
				resolve();
			});
		});
	}
}

