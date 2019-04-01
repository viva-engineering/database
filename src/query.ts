
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
	compile(params: P): string;
	isRetryable(error: MysqlError): boolean;
	toString(): string;
}

export interface StreamingSelectCallback<T> {
	(record: T): void;
}

export abstract class SelectQuery<P, R> implements Query<P, SelectQueryResult<R>> {
	abstract compile(params: P) : string;
	abstract isRetryable(error: MysqlError) : boolean;
	abstract toString() : string;
}

export abstract class WriteQuery<P> implements Query<P, WriteQueryResult> {
	abstract compile(params: P) : string;
	abstract isRetryable(error: MysqlError) : boolean;
	abstract toString() : string;
}
