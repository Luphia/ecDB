/*
	"dependencies": {
		"mongodb": ">=1.4.9",
		"deasync": ">=0.0.7",
	}

	option: {
		url: "mongodb://127.0.0.1:27017/",
		user: null,
		pass: null
	}
 */

var Collection = require('../lib/Collection.js')
,	Schema = require('../lib/Schema.js')
,	Mongo = require('mongodb')
,	url = require('url')
,	Client = Mongo.MongoClient
,	dbURL
,	defaultDB = "mongodb://127.0.0.1:27017/";


var parseCondition = function(ast) {
	var rs = {};

	ast.WHERE && (ast = ast.WHERE);

	!ast && (ast = {});
	if(ast.operator) {
		rs = {};
		switch(ast.operator.toLowerCase()) {
			case "=":
				rs[ast.left] = ast.right;
				break;
			case "like":
				var arr = ast.right.split('*'),
					reg = '';

				if(arr.length == 1) {
					reg = arr[0];
				}
				else {
					if(arr[0].length > 0) { reg = '^' + arr[0] + ".*"; }
					arr.splice(0, 1);

					reg += arr.join('.*');
					if(arr[arr.length - 1].length > 0) { reg += '$'; }
				}

				rs[ast.left] = {"$regex": new RegExp(reg)};
				break;
			case "!=":
				rs[ast.left] = {"$ne": ast.right};
				break;
			case ">":
				rs[ast.left] = {"$gt": ast.right};
				break;
			case ">=":
				rs[ast.left] = {"$gte": ast.right};
				break;
			case "<":
				rs[ast.left] = {"$lt": ast.right};
				break;
			case "<=":
				rs[ast.left] = {"$lte": ast.right};
				break;
		}
	}
	else if(ast.logic) {
		var cond = [];
		for(var key in ast.terms) {
			cond.push(parseCondition(ast.terms[key]));
		}

		ast.logic = ast.logic.toLowerCase();
		switch(ast.logic) {
			case "and":
				rs = {"$and": cond};
				break;
			case "or":
				rs = {"$or": cond};
				break;
		}
	}

	return rs;
};

var EasyMongo = function(logger) {
	if(logger) {
		this.logger = logger;
	}
	else {
		this.logger = console.log;
	}
};

EasyMongo.prototype.connect = function(option, callback) {
	var rs;
	var that = this;
	!option && (option = {});
	!option.url && (option.url = defaultDB);
	dbURL = url.parse(option.url);
	option.protocol = 'mongodb:';
	option.slashes = true;
	option.host = dbURL.host;
	option.port = dbURL.port;
	option.pathname = dbURL.pathname;

	Client.connect(url.format(option), function(err, _db) {
		if(err) { callback(err); }
		else {
			callback(err, _db);
			that.DB = _db;
		}
	});
	return true;
};
EasyMongo.prototype.disconnect = function() {
	return this.DB.close();
};
EasyMongo.prototype.tableExist = function(table, callback) {
	this.DB.collection('_tables').find({'name': table}).toArray(function(err, data) {
		if(err) { callback(err); }
		else { callback(err, (data.length > 0)); }
	});
};
EasyMongo.prototype.tableCount = function(table, callback) {
	this.DB.collection(table).count(function(err, count) {
		if(err) { callback(err); }
		else { callback(err, count); }
	});
};
EasyMongo.prototype.getSchema = function(table, callback) {
	this.DB.collection('_tables').find({'name': table}).toArray(function(err, data) {
		if(err) { callback(err); }
		else {
			var rs;
			if(data.length > 0) {
				var schema = new Schema(data[0]);
				rs = schema.toJSON();
			}
			else {
				rs = false;
			}
			callback(err, rs);
		}
	});
};
EasyMongo.prototype.newSchema = function(table, schema, callback) {
	var label;

	if(typeof(table) == 'object') {
		label = table.label;
		table = table.name;
	}

	var rs
	,	condition = { "name": table }
	,	tableSchema = new Schema(schema)
	;

	tableSchema.setName(table);
	tableSchema.setMaxSerialNum(0);
	if(label) { tableSchema.setLabel(label); }

	this.DB.collection('_tables').update(condition, tableSchema.toConfig(), {w:1, upsert: true}, function(err, data) {
		if(err) { callback(err); }
		else { callback(err, true); }
	});
};
EasyMongo.prototype.setSchema = function(table, schema, callback) {
	var label;

	if(typeof(table) == 'object') {
		label = table.label;
		table = table.name;
	}

	var rs
	,	condition = { "name": table }
	,	tableSchema = new Schema(schema);

	tableSchema.setLabel(label);

	this.DB.collection('_tables').update(condition, {"$set": tableSchema.toConfig()}, {w:1, upsert: true}, function(err, data) {
		if(err) { callback(err); }
		else { callback(err, true); }
	});
};
EasyMongo.prototype.getID = function(table, callback) {
	this.DB.collection('_tables').findAndModify(
		{'name': table}, 
		['max_serial_num'],
		{$inc: {"max_serial_num": 1}},
		{},
		function(err, data) {
			if(err) { callback(err); }
			else if(!data) { callback(err, 1); }
			else { callback(err, data.max_serial_num + 1); }
		}
	);
};
EasyMongo.prototype.getIDs = function(table, n, callback) {
	this.DB.collection('_tables').findAndModify(
		{'name': table}, 
		['max_serial_num'],
		{$inc: {"max_serial_num": n}},
		{},
		function(err, data) {
			if(err) { callback(err); }
			else if(!data) { callback(err, 1); }
			else { callback(err, data.max_serial_num + 1); }
		}
	);
};
EasyMongo.prototype.checkID = function(table, id, callback) {
	id = parseInt(id);
	var that = this;

	if(!id) {
		callback(false, false);
		return false;
	}
	else {
		this.DB.collection('_tables').find({'name': table}).toArray(function(err, data) {
			if(err) { callback(err); }
			else if(!(data.length > 0) || data[0].max_serial_num < id) {
				that.DB.collection('_tables').findAndModify(
					{'name': table},
					['max_serial_num'],
					{$set: {"max_serial_num": id}},
					{},
					function(_err, _data) {
						if(_err) { callback(_err); }
						else { callback(_err, id); }
					}
				);
			}
			else {
				callback(err, data[0].max_serial_num);
			}
		});
	}
};
EasyMongo.prototype.listTable = function(callback) {
	this.DB.collection('_tables').find().toArray(function(err, data) {
		if(err) { callback(err); }
		else {
			var list = [];
			for(var k in data) {
				if(data[k].name.replace(/^([^.]*)./, "").indexOf('system.', 0) == 0) {
					// do not show system table
				}
				else if(data[k].name.replace(/^([^.]*)./, "").indexOf('_', 0) == 0) {
					// do not show protected table
				}
				else {
					var table = {
						"name": data[k].name,
						"label": (data[k].label || data[k].name)
					};
					list.push(table);
				}
			}

			callback(err, list);
		}
	});
};
EasyMongo.prototype.getTable = function(table, callback) {
	var that = this;
	this.getSchema(table, function(err, schema) {
		if(err) { callback(err); }
		else if(schema) {
			that.tableCount(table, function(_err, count) {
				if(_err) { callback(_err); }
				else {
					schema.table_length = count;
					callback(_err, schema);
				}
			});
		}
		else { callback(err, false); }
	});
};
EasyMongo.prototype.postTable = function(table, schema, callback) {
	var that = this;
	this.tableExist(table, function(err, exist) {
		if(err) { callback(err); }
		else {
			if(exist) {
				callback(err, false);
			}
			else {
				that.newSchema(table, schema, callback);
			}
		}
	});
};
EasyMongo.prototype.putTable = function(table, schema, callback) {
	this.setSchema(table, schema, callback);
};
EasyMongo.prototype.deleteTable = function(table, callback) {
	var todo = 2;
	var done = function(err) {
		todo--;
		if(todo < 0) { return false; }

		if(err) { callback(err); todo = 0; }
		else if(todo == 0) { callback(err, true); }
	};
	this.DB.collection('_tables').findAndModify(
		{name: table}, [], {}, {remove: true}, done
	);
	this.DB.collection(table).remove(done);
};
EasyMongo.prototype.listData = function(table, query, callback) {
	var condition = parseCondition(query);
	var limit;
	var find = this.DB.collection(table).find(condition);
	var sort = {}, orderBy;

	if(orderBy = query['ORDER BY']) {
		for(var k in orderBy) {
			var column = orderBy[k]['column'];
			sort[column] = orderBy[k]['order'].toUpperCase() == 'ASC'? 1: -1;
		}

		find = find.sort(sort);
	}

	if(limit = query.LIMIT) {
		if(limit.nb > 0) {
			find = find.limit(limit.nb);
		}
		if(limit.from > 0) {
			find = find.skip(limit.from);
		}
	}

	find.toArray(function(err, data) {
		if(err) { callback(err); }
		else { callback(err, data); }
	});
};
EasyMongo.prototype.flowData = function(table, query, callback) {
	var condition = parseCondition(query);
	var limit;
	var sort = {}, orderBy;
	var find = this.DB.collection(table).find(condition);

	if(orderBy = query['ORDER BY']) {
		for(var k in orderBy) {
			var column = orderBy[k]['column'];
			sort[column] = orderBy[k]['order'].toUpperCase() == 'ASC'? 1: -1;
		}

		find = find.sort(sort);
	}
	else {
		find = find.sort({'_id': -1});
	}

	if(limit = query.LIMIT) {
		if(limit.from > 0) {
			condition['_id'] = {"$lte": limit.from};
		}

		find = find.limit(limit.nb);
	}

	find.toArray(function(err, data) {
		if(err) { callback(err); }
		else { callback(err, data); }
	});
};
EasyMongo.prototype.pageData = function(table, query, callback) {
	var limit = query.LIMIT;
	var sort = {}, orderBy;
	var find = this.DB.collection(table).find();

	if(orderBy = query['ORDER BY']) {
		for(var k in orderBy) {
			var column = orderBy[k]['column'];
			sort[column] = orderBy[k]['order'].toUpperCase() == 'ASC'? 1: -1;
		}

		find = find.sort(sort);
	}
	else {
		find = find.sort({'_id': -1});
	}

	pick = limit.nb;
	skip = parseInt(limit.from) || 0;

	find = find.skip(skip).limit(pick);

	find.toArray(function(err, data) {
		if(err) { callback(err); }
		else { callback(err, data); }
	});
};
EasyMongo.prototype.getData = function(table, query, callback) {
	var condition = parseCondition(query);
	this.DB.collection(table).find(condition).toArray(function(err, data) {
		if(err) { callback(err); }
		else if(data.length > 0) { callback(err, data[0]); }
		else { callback(err, false); }
	});
};
EasyMongo.prototype.find = function(table, data, callback) {
	this.DB.collection(table).find(data).toArray(function(err, _data) {
		if(err) { callback(err); }
		else if(_data.length > 0) { callback(err, _data); }
		else { callback(err, false); }
	});
};
EasyMongo.prototype.postData = function(table, data, callback) {
	this.DB.collection(table).insert(data, function(err, _data) {
		if(err) { callback(err); }
		else { callback(err, true); }
	});
};
EasyMongo.prototype.updateData = function(table, query, data, callback) {
	var condition = parseCondition(query);
	this.DB.collection(table).update(condition, data, {multi: true}, function(err) {
		if(err) { callback(err); }
		else { callback(err, true); }
	});
};
EasyMongo.prototype.putData = function(table, query, data, callback) {
	var condition = parseCondition(query);
	this.DB.collection(table).update(condition, data, {w:1, upsert: true}, function(err) {
		if(err) { callback(err); }
		else { callback(err, true); }
	});
};
EasyMongo.prototype.replaceData = function(table, query, data, callback) {
	var condition = parseCondition(query);
	this.DB.collection(table).update(condition, data, {w:1, upsert: true}, function(err) {
		if(err) { callback(err); }
		else { callback(err, true); }
	});
};
EasyMongo.prototype.deleteData = function(table, query, callback) {
	var condition = parseCondition(query);

	this.DB.collection(table).remove(condition, {}, function(err, data) {
		if(err) { callback(err); }
		else { callback(err, true); }
	});
};

module.exports = EasyMongo;