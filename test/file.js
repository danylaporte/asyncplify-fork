require('../index').process(function (source) {
	return source.map(function (item) { return item + 10; });
});