
import { FieldInfo, MysqlError } from 'mysql';

export interface SelectQueryResult<T> {
	results: T[],
	fields: FieldInfo[]
}

export interface StreamingSelectCallback<T> {
	(record: T): void;
}

export abstract class SelectQuery<T extends object> {
	abstract compile(...params: any[]) : string;
	abstract isRetryable(error: MysqlError) : boolean;
}
