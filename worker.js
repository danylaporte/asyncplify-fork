var asyncplify = require('asyncplify');

require('./forkChild')(function (item) {
	for (var i = 0; i < 1000000000; i++) {
	}
	return asyncplify.value(item + 1); 
});