
An extremely light-weight logger for node.js

### Install

```bash
$ npm install --save @viva-eng/logger
```

### Basic Usage

```typescript
import { Logger, ReadableFormat, StdoutOutput } from '@viva-eng/logger';

const logger = new Logger({
	format: ReadableFormat,
	output: new StdoutOutput(),
	colors: true,
	level: 'info'
});

logger.info('Hi there');
```

There are 6 logging methods: `error`, `warn`, `info`, `verbose`, `debug`, and `silly`.

### Outputs

Any normal [writable stream](https://nodejs.org/api/stream.html#stream_writable_streams) can be used as an output. It will receive messages to be logged. That's about all there is to say about that.

There are 2 outputs built in to the library. The `StdoutOutput` is just a thin wrapper around `process.stdout`. The other is the `ClusterOutput` which is designed to enable logging from workers in a clustered application. It works by passing the messages to be logged up to the master process using IPC, and the master process then logs the message to ensure that messages don't interlace with each other in unusual ways.


```typescript
import { isMaster } from 'cluster';
import { Logger, ReadableFormat, StdoutOutput, ClusterOutput, ClusterOutputReceiver } from '@viva-eng/logger';

export let logger;

if (isMaster) {
	logger = new Logger({
		format: ReadableFormat,
		// Use whatever output you want here, this is where your logs will
		// end up for all workers and the master
		output: new StdoutOutput()
	});

	// Pass the logger into the receiver so it knows what to do with the messages
	const clusterOutputReceiver = new ClusterOutputReceiver(logger);
}

else {
	logger = new Logger({
		format: ReadableFormat,
		// This will forward all log messages up to the master
		output: new ClusterOutput()
	});
}

logger.info('Hi there! I work in all processes!');
```

### Formats

A format is any object that implements the `Format` interface, which looks like this:

```typescript
interface Format {
	format(level: string, message: string, meta?: object): string;
}
```

It takes in the message to be logged and formats it in whatever way you want, so long as the end result is a string.

There are two formats built in, the `ReadableFormat` which is a human-readable pretty print format (optionally with colors!) and the `JsonFormat` which outputs all your logs in JSON.
