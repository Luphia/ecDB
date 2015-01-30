var Schema = function(data) {
	this.init(data);
};

var dataType = function(type) {
	// default type: String;
	var rtType = "String";
	if(typeof type == "object") { return 'JSON'; }
	else if(typeof type != 'string') { return rtType; }

	var typeList = ['String', 'Number', 'Date', 'Boolean', 'JSON', 'Binary'];
	var searchList = [];
	for(var key in typeList) { searchList[key] = typeList[key].toLowerCase(); }
	si = searchList.indexOf(type.toLowerCase());

	if(si > -1) {
		rtType = typeList[si];
	}

	return rtType;
};

var clone = function(target) {
	if(typeof(target) == 'object') {
		var rs = Array.isArray(target)? []: {};
		for(var key in target) {
			rs[key] = clone(target[key]);
		}
		return rs;
	}
	else {
		return target;
	}
};

Schema.prototype.init = function(data) {
	this.data = {};
	this.data.columns = {};

	var columns = data;
	if(typeof(data.columns) == 'object') {
		this.data.name = data.name;
		this.data.strick = !!data.strick;

		columns = data.columns;
	}

	for(var key in columns) {
		if(key.indexOf('_', 0) == 0) { continue; }
		this.data.columns[key] = dataType(columns[key]);
	}
};

Schema.prototype.setName = function(name) {
	this.data.name = name.toString();
};

Schema.prototype.setStrick = function(strick) {
	this.data.strick = !!strick;
};

Schema.prototype.setMaxSerialNum = function(num) {
	this.data.max_serial_num = parseInt(num) || 0;
};

Schema.prototype.setTableLength = function(num) {
	this.data.table_length = parseInt(num) || 0;
};

Schema.prototype.toConfig = function() {
	var data = clone(this.data);
	delete data.table_length;

	return data;
};

Schema.prototype.toJSON = function() {
	var data = clone(this.data);
	return data;
};

module.exports = Schema;