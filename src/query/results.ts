
import { FieldInfo } from 'mysql2';

export interface SelectQueryResult<Record> {
	results: Record[];
	fields: FieldInfo[];
}

export interface WriteQueryResult {
	insertId: number | string;
	affectedRows: number;
	changedRows: number;
}
