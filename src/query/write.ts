
import { MysqlError } from 'mysql';

export interface WriteQueryResult {
	insertId: number | string,
	affectedRows: number,
	changedRows: number
}

export abstract class WriteQuery<P> {
	abstract compile(params: P) : string;
	abstract isRetryable(error: MysqlError) : boolean;
	abstract toString() : string;
}
