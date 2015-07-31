var asyncplify = require('asyncplify');
var lib = require('../index');
var tests = require('asyncplify-tests');

describe('fork', function () {
	asyncplify
		.range(10)
		.pipe(lib(__dirname + '/file.js'))
		.toArray()
		.map(function (array) { return array.sort(); })	// sort because the result order is not guaranteed
		.pipe(tests.itShouldEmitValues([[10, 11, 12, 13, 14, 15, 16, 17, 18, 19]]));

	asyncplify
		.range(10)
		.pipe(lib({ file: __dirname + '/file.js', maxConcurrency: 1 }))
		.pipe(tests.itShouldEmitValues({
			title: 'should emit values in correct order when maxConcurrency = 1',
			values: [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
		}));
});