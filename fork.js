var Asyncplify = require('asyncplify');
var childProcess = require('child_process');
var debug = require('debug')('asyncplify-fork');
var os = require('os');

function Fork(options, sink, source) {
	var load = 0;
	var cpuCount = os.cpus().length;
	var self = this;

	this.affinity = options && options.affinity || function () {
		return load++;
	};

	this.file = options && options.file || options;
	this.maxConcurrency = Math.max(1, Math.min(options && options.maxConcurrency || cpuCount, cpuCount));
	this.pool = {};
	this.sink = sink;
	this.sink.source = this;
	this.source = null;
	this.working = 0;

	debug('subscribe, concurrency: %d/%d', this.maxConcurrency, cpuCount);
	source._subscribe(this);
}

Fork.prototype = {
	emit: function (value) {
		var affinity = this.affinity(value) % this.maxConcurrency;
		var forkItem = this.pool[affinity];

		if (!forkItem) {
			debug('create fork %d', affinity);
			forkItem = this.pool[affinity] = new ForkItem(this, affinity);
		}

		debug('queue to fork %d', affinity, value);
		forkItem.emit(value);
	},
	end: function (err) {
		debug('source end', err);
		this.source = this.affinity = null;

		if (err || !this.working) {
			this.setState(Asyncplify.states.CLOSED);
			debug('end', this.affinity, err);
			this.sink.end(err);
		}
	},
	setState: function (state) {
		switch (state) {
			case Asyncplify.states.CLOSED:
				if (this.source) this.source.setState(state);

				for (var k in this.pool)
					this.pool[k].process.kill();

				this.pool = {};
				this.working = 0;
				this.source = null;
				break;

			default:
				if (this.source) this.source.setState(state);
				for (var k in this.pool)
					this.pool.process.setState(state);
				break;
		}
	}
};

function ForkItem(parent, affinity) {
	var self = this;

	this.affinity = affinity;
	this.parent = parent;
	this.process = childProcess.fork(parent.file, parent.args, parent.options);
	this.process.on('message', function (msg) { self.handleMessage(msg); });
	this.process.send({ type: 'init', data: { affinity: affinity } });
	this.working = 0;
}

ForkItem.prototype = {
	emit: function (data) {
		this.working++;
		if (this.working === 1) this.parent.working++;
		this.process.send({ type: 'emit', data: data });
	},
	handleMessage: function (msg) {
		switch (msg.type) {
			case 'emit':
				debug('receive from fork %d', this.affinity, msg.data);
				this.parent.sink.emit(msg.data);
				break;
			case 'end':
				this.working--;
				debug('received end from fork %d', this.affinity, msg.data);
				if (!this.working) this.parent.working--;
				if (msg.data || (!this.parent.working && !this.parent.source)) {
					this.parent.setState(Asyncplify.states.CLOSED);
					debug('end %j', this.affinity, msg.data);
					this.parent.sink.end(msg.data);
				}
				break;
		}
	},
	setState: function (state) {
		switch (state) {
			case Asyncplify.states.CLOSED:
				if (this.process) this.process.kill();
				this.process = null;
				break;

			case Asyncplify.states.RUNNING:
			case Asyncplify.states.PAUSED:
				if (this.process && this.working) this.process.setState(state);
				break;
		}
	}
};

module.exports = function (options) {
	return function (source) {
		return new Asyncplify(Fork, options, source);
	};
};