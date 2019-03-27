
import { SelectQuery, SelectQueryResult } from './select';
import { WriteQuery, WriteQueryResult } from './write';

export { SelectQuery, SelectQueryResult } from './select';
export { WriteQuery, WriteQueryResult } from './write';

export type Query = SelectQuery<any> | WriteQuery;
export type QueryResult = SelectQueryResult<any> | WriteQueryResult;
