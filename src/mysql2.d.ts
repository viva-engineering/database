
// 
// Typings for mysql2 are not in DT, and I have zero interest whatsoever in using some
// special, one-off backwards way of importing type definitions so I can use one extra
// function. There are more differences than this in the types, but this will work for
// my use cases of enabling the use for prepared statements.
// 

declare module 'mysql2' {
	import * as mysql from 'mysql';

	export * from 'mysql';

	export interface Connection extends mysql.Connection {
	    execute: mysql.QueryFunction;
	}

	export interface PoolConnection extends mysql.PoolConnection {
	    execute: mysql.QueryFunction;
	}
}
