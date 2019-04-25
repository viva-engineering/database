
import { MysqlError, FieldInfo } from 'mysql';

export interface SelectQueryResult<R> {
	results: R[];
	fields: FieldInfo[];
}

export interface WriteQueryResult {
	insertId: number | string;
	affectedRows: number;
	changedRows: number;
}

export type QueryResult = SelectQueryResult<any> | WriteQueryResult;

export interface Query<P, R extends QueryResult> {
	template: string;
	compile(params: P): string;
	isRetryable(error: MysqlError): boolean;
	toString(): string;
}

export interface StreamingSelectCallback<T> {
	(record: T): void;
}

export abstract class SelectQuery<P, R> implements Query<P, SelectQueryResult<R>> {
	/**
	 * A short, abstract representation of the query, included in log messages about the query
	 *
	 * For example, the value might look something like `select ... from some_table where something = ?`
	 */
	public abstract readonly template: string;

	/**
	 * Compiles the query with the given parameters to provide a finished, executable query
	 *
	 * @param params The parameters used to build the query
	 */
	public abstract compile(params: P) : string;

	/**
	 * When provided with a MySQL error, should determine if the query is retryable.
	 *
	 * For example, if the query failed due to a table lock, its probably retyable, so this method would return true.
	 * If the query failed because of a constraint violation on the table, it's probably not retryable without some
	 * kind of corrective action first, so the method would return false.
	 *
	 * @param error The error that occurred
	 */
	public abstract isRetryable(error: MysqlError) : boolean;

	/**
	 * Should return a short, abstract representation of the query so it can be identified in the case that the object
	 * is included in a log somewhere. Simply returning the `template` property is usually probably good enough.
	 */
	public toString() : string {
		return this.template;
	}
}

export abstract class WriteQuery<P> implements Query<P, WriteQueryResult> {
	/**
	 * A short, abstract representation of the query, included in log messages about the query
	 *
	 * For example, the value might look something like `insert into some_table (...) values (...)`
	 */
	public abstract readonly template: string;
	
	/**
	 * Compiles the query with the given parameters to provide a finished, executable query
	 *
	 * @param params The parameters used to build the query
	 */
	public abstract compile(params: P) : string;

	/**
	 * When provided with a MySQL error, should determine if the query is retryable.
	 *
	 * For example, if the query failed due to a table lock, its probably retyable, so this method would return true.
	 * If the query failed because of a constraint violation on the table, it's probably not retryable without some
	 * kind of corrective action first, so the method would return false.
	 *
	 * @param error The error that occurred
	 */
	public abstract isRetryable(error: MysqlError) : boolean;

	/**
	 * Should return a short, abstract representation of the query so it can be identified in the case that the object
	 * is included in a log somewhere. Simply returning the `template` property is usually probably good enough.
	 */
	public toString() : string {
		return this.template;
	}
}

export interface RawQueryFragment<T extends string> {
	toSqlString(): string;
}

/**
 * Represent a select sub-query that can be included into another query
 */
export abstract class SelectSubQuery<P> {
	public abstract readonly columns;

	protected raw(sql: string) : RawQueryFragment<any> {
		return {
			toSqlString() {
				return sql;
			}
		};
	}

	/**
	 * Compiles the sub-query with the given parameters to provide a finished, executable query
	 *
	 * @param params The parameters used to build the sub-query
	 */
	public abstract compile(params: P) : RawQueryFragment<any>;
}
