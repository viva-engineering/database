
import { FieldInfo, MysqlError } from 'mysql';

export interface SelectQueryResult<T> {
	results: T[],
	fields: FieldInfo[]
}

export interface StreamingSelectCallback<T> {
	(record: T): void;
}

export abstract class SelectQuery<T extends object, P> {
	abstract compile(params: P) : string;
	abstract isRetryable(error: MysqlError) : boolean;
	abstract toString() : string;
}
