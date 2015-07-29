var asyncplify = require('asyncplify');
var fork = require('./fork');
var d = Date.now();

asyncplify
	.range(8)
	.map(function (x) { return String(x); })
	.pipe(fork({ file: './worker.js', maxConcurrency: 1 }))
	.subscribe({
		end: function () {
			console.log("%dms", Date.now() - d);
		}
	});