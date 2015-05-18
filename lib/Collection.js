/*
 * start
 * end
 * pick
 * list
 */

module.exports = function() {
	var _init = function() {
		this.start = 0;
		this.end = 0;
		this.list = [];
		return this;
	}
	, add = function(data) {
		if(!data) { return false; }
		this.list.push(data);
		if(data['_id'] > this.start) { this.start =  data['_id'];}
		if(this.end == 0 || data['_id'] < this.end) { this.end =  data['_id'];}
	}
	, toJSON = function(detail) {
		var rs;
		if(!!detail) {
			rs = {
				start: this.start,
				pick: this.list.length,
				end: this.end,
				list: this.list
			};
		}
		else {
			rs = this.list;
		}

		return rs;
	}

	, that = {
		_init: _init,
		add: add,
		toJSON: toJSON
	};

	return that._init();
}