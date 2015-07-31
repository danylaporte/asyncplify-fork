var Asyncplify = require('asyncplify');

module.exports = function (mapper) {
	var parent = { sink: null };
	var source = mapper(new Asyncplify(Source, parent));
	new Sink(parent, source);
};

function Source(parent, sink) {
	var self = this;

	this.parent = parent;
	this.sink = sink;
	this.sink.source = this;

	process.on('message', function (msg) { self.handleMessage(msg); });
}

Source.prototype = {
	handleMessage: function (msg) {
		switch (msg.type) {
			case 'emit':
				this.sink.emit(msg.data);
				break;

			case 'end':
				this.sink.end(msg.data);
				break;

			case 'setState':
				if (this.parent.sink) this.parent.sink.setState(msg.data);
				break;
		}
	}
};

function Sink(parent, source) {
	this.source = null;
	parent.sink = this;
	source._subscribe(this);
}

Sink.prototype = {
	emit: function (data) {
		process.send({ type: 'emit', data: data });
	},
	end: function (err) {
		this.source = null;
		process.send({ type: 'end', data: err });
	},
	setState: function (state) {
		if (this.source) this.source.setState(state);
	}
};