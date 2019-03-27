
import { MysqlError } from 'mysql';

export interface WriteQueryResult {
	insertId: number | string,
	affectedRows: number,
	changedRows: number
}

export abstract class WriteQuery {
	abstract compile(...params: any[]) : string;
	abstract isRetryable(error: MysqlError) : boolean;
}
