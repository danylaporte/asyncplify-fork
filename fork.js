/* global ; */
var Asyncplify = require('asyncplify');
var childProcess = require('child_process');
var debug = require('debug')('asyncplify-fork');
var os = require('os');

function Fork(options, sink, source) {
	var load = 0;
	var cpuCount = os.cpus().length;

	this.affinity = options && options.affinity || function () {
		return load++;
	};

	this.file = options && options.file || options;
	this.maxConcurrency = Math.max(1, Math.min(options && options.maxConcurrency || cpuCount, cpuCount));
	this.pool = {};
	this.sink = sink;
	this.sink.source = this;
	this.source = null;

	debug('subscribe, concurrency: %d/%d', this.maxConcurrency, cpuCount);
	source._subscribe(this);
}

Fork.prototype = {
	areAllClosed: function () {
		for (var k in this.pool)
			if (this.pool[k].state !== Asyncplify.states.CLOSED) return false;
		return true;
	},
	emit: function (value) {
		var id = this.affinity(value) % this.maxConcurrency;
		var forkItem = this.pool[id];

		if (!forkItem) {
			debug('create child process %d', id);
			forkItem = this.pool[id] = new ForkItem(this, id);
		}

		forkItem.emit(value);
	},
	end: function (err) {
		debug('source end', err);

		if (!err)
			for (var k in this.pool)
				this.pool[k].end();

		this.source = this.affinity = null;

		if (err || this.areAllClosed()) {
			debug('end', err);
			this.setState(Asyncplify.states.CLOSED);
			this.sink.end(err);
		}
	},
	setState: function (state) {
		var count = 0;

		for (var k in this.pool) {
			count++;
			this.pool[k].setState(state);
		}

		if (!count && this.source) this.source.setState(state);
	}
};

function ForkItem(parent, id) {
	var self = this;

	this.id = id;
	this.parent = parent;
	this.state = Asyncplify.states.RUNNING;
	this.process = childProcess.fork(parent.file, parent.args, parent.options);
	this.process.on('message', function (msg) { self.handleMessage(msg); });
}

ForkItem.prototype = {
	emit: function (data) {
		debug('emit item to child process %d', this.id, data);
		this.process.send({ type: 'emit', data: data });
	},
	end: function () {
		debug('send end to child process %d', this.id);
		this.process.send({ type: 'end', data: null });
	},
	handleMessage: function (msg) {
		switch (msg.type) {
			case 'emit':
				debug('child process %d emit', this.id, msg.data);
				this.parent.sink.emit(msg.data);
				break;

			case 'end':
				debug('child process %d end', this.id, msg.data);
				debug('child process %d kill', this.id);
				this.state = Asyncplify.states.CLOSED;
				this.process.kill();
				this.process = null;

				if (msg.data || (!this.parent.source && this.parent.areAllClosed())) {
					this.parent.setState(Asyncplify.states.CLOSED);
					debug('end', msg.data);
					this.parent.sink.end(msg.data);
				}
				break;

			case 'state':
				if (this.state === msg.data) return;
				this.state = msg.data;

				switch (this.state) {
					case Asyncplify.states.CLOSED:
					case Asyncplify.states.RUNNING:

						for (var k in this.parent.items)
							if (this.parent.items[k].state !== this.state)
								return;
						break;

					case Asyncplify.states.PAUSED:
						if (this.parent.source) this.parent.source.setState(Asyncplify.states.PAUSED);
						break;
				}
				break;
		}
	},
	setState: function (state) {
		switch (state) {
			case Asyncplify.states.CLOSED:
				if (this.process) {
					debug('kill child process %d', this.id);
					this.process.kill();
					this.process = null;
				}
				break;

			case Asyncplify.states.RUNNING:
			case Asyncplify.states.PAUSED:
				if (this.process) this.process.send({ type: 'state', data: state });
				break;
		}
	}
};

module.exports = function (options) {
	return function (source) {
		return new Asyncplify(Fork, options, source);
	};
}