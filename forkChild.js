var Asyncplify = require('asyncplify');

module.exports = function (mapper) {
	new ForkChild(mapper);
};

function ForkChild(mapper) {
	var self = this;

	this.affinity = 0;
	this.items = [];
	this.mapper = mapper;
	process.on('message', function handleMessage(msg) { self.handleMessage(msg); });
}

ForkChild.prototype = {
	emit: function (data) {
		try {
			var source = this.mapper(data);
			if (source) new ForkChildItem(this, source);
		} catch (ex) {
			this.setState(Asyncplify.states.CLOSED);
			process.send({ type: 'end', data: ex });
		}
	},
	handleMessage: function (msg) {
		switch (msg.type) {
			case 'emit':
				this.emit(msg.data);
				break;
			case 'init':
				this.affinity = msg.data.affinity;
				break;
			case 'state':
				this.setState(msg.data);
				break;
		}
	},
	setState: function (state) {
		for (var i = 0; i < this.items.length; i++)
			this.items[i].setState(state);
	}
};

function ForkChildItem(parent, source) {
	this.parent = parent;
	this.source = null;

	parent.items.push(this);
	source._subscribe(this);
}

ForkChildItem.prototype = {
	emit: function (data) {
		process.send({ type: 'emit', data: data });
	},
	end: function (err) {
		this.source = null;

		for (var i = 0; i < this.parent.items.length; i++) {
			if (this.parent.items[i] === this) {
				this.parent.items.splice(i, 1);
				break;
			}
		}

		if (err) this.parent.setState(Asyncplify.states.CLOSED);
		process.send({ type: 'end', data: err });
	},
	setState: function (state) {
		if (this.source) this.source.setState(state);
	}
};