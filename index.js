var driverPath = './DBDriver/'
,	defaultDriver = 'EasyTingo'
,	util = require('util')
,	Collection = require('./lib/Collection.js')
,	Parser = require('./lib/Parser.js')
,	defaultLimit = 30;

/*
	this.DB
	this.params = {
		driverPath,
		driver
	}

	var test = {a: 1, b: { "b1": 2 }}
 */

 /*

var edb = require('ecdb'); // var edb = require('./index.js');
var db = new edb();
db.connect();
db.listData('users', 'where authtime > "2012-10-10"', function(e, d) {console.log(d);});
db.listTable(function(e, d) {console.log(d);});
db.putData('tmp', 5, {name: 'String', birth: 'Date', age: 1}, function(e, d) {console.log(d);});
db.postTable('testuser', {name: 'String', birth: 'Date', age: 1}, function(e, d) {console.log(d);});
db.postData('user', {name: 'A', birth: '1982-04-01', age: 2}, function(e, d) {console.log(d);});
db.postData('testuser', {name: 'B', birth: '1988-09-18', age: 3}, function(e, d) {console.log(d);});
db.postData('user', {name: 'B', birth: '1995-08-23', age: 4}, function(e, d) {console.log(d);});
db.listData('user', "birth < '1990-01-01' and birth > '1984-01-01'", function(e, d) {console.log(d);});
db.postData('user', [{name: 'D', birth: '1982-05-01'}], function(e, d) {console.log(d);});
db.postData('user', [{name: 'D', birth: '1982-05-01'}, {name: 'E', birth: '1982-06-01'}, {name: 'F', birth: '1982-07-01'}], function(e, d) {console.log(d);});
db.sql("select * from user where birth > '1982-05-01'", function(e, d) {console.log(d);});
db.sql("select * from user inner join info on user.x = info.x", function(e, d) {console.log(d);});

db.dataFind(
[{name: 'A', birth: '1982-04-01', age: 10},{name: 'B', birth: '1983-04-01', age: 5},{name: 'C', birth: '1984-04-01', age: 20}],
"select * from table where birth > '1983-01-01'",
function(e, d) {console.log(d);}
);

db.putData('tmp2', 5, {name: 'String', birth: 'Date', age: 1}, function(e, d) {console.log(d);});
db.sql("update Yo set a = 1, b = 'b', c = '1999-01-01'");
db.sql("delete from user where _id > 3");

 */

var Schema = function(table) {
		return { "name": table, "max_serial_num": 0, "columns": {} };
	}
,	dataSize = function(data) {
		var size = 0;
		for(var key in data) { if(key.indexOf('_') != 0) size++; }
		return size;
	}
,	checkTable = function(table, toObj) {
		!table && (table = '');
		var result = table;
		var tablename = table.name || table;

		tablename = tablename.trim();
		if(tablename.length == 0) { return false; }

		while(tablename.substr(0, 1) == '_') { tablename = tablename.substr(1); }
		result.name = tablename;

		return toObj? result: tablename;
	}
,	checkSQL = function(sql) {
	if(!sql || typeof sql != 'string') { return ''; }

	var tmp = sql.toLowerCase();

	if(
		tmp.indexOf("select") != 0 &&
		tmp.indexOf("update") != 0 &&
		tmp.indexOf("insert") != 0 &&
		tmp.indexOf("delete") != 0 &&
		tmp.indexOf("limit") != 0 &&
		tmp.indexOf("where") != 0
	) {
		sql = "WHERE " + sql;
	}

	return sql;
}
,	dataType = function(type) {
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
}
,	valueType = function(value) {
	var rs;
	if(typeof value == "object") { rs = "JSON"; }
	else if( /^-?\d+(?:\.\d*)?(?:e[+\-]?\d+)?$/i.test(value) ) { rs = dataType("Number"); }
	else if( !isNaN(Date.parse(value)) ) { rs = dataType("Date"); }
	else { rs = dataType(typeof value); }
	return rs;
}
,	getValueSchema = function(data) {
	var schema = {};
	if(!data || typeof data != 'object') { data = {}; }
	else if(util.isArray(data)) { return getValueSchema(data[0]); }

	for(var key in data) {
		schema[key] = valueType(data[key]);
	}

	return schema;
}
,	dataTransfer = function(value, type) {
	if(typeof type != "string") { return checkValue(value); }
	var rs = value;
	if(value === null) { return null; }

	switch(type) {
		case "String":
			if(!value) {
				rs = "";
			}
			else {
				rs = value.toString();
			}
			break;

		case "Number":
			rs = isNaN(parseFloat(value))? null: parseFloat(value);
			break;

		case "Date":
			rs = isNaN(new Date(value))? null: new Date(value);
			break;

		case "Boolean":
			if(typeof value == 'string') { value = value.toLowerCase(); }
			switch(value) {
				case true:
				case "true":
				case "t":
				case 1:
				case "1":
					rs = true;
					break;

				case false:
				case "false":
				case "f":
				case 0:
				case "0":
					rs = false;
					break;

				default:
					rs = null;
					break;
			}

			break;

		case "JSON":
			rs = typeof value == 'object'? value: {};
			break;

		case "Binary":
			break;
	}

	return rs;
}
,	parseData = function(data) {
	var type = typeof(data),
		rs = {};

	if(type != 'object') {
		key = '_' + type;
		rs[key] = data;
	}
	else {
		rs = data;
	}

	return rs;
}
,	checkValue = function(value) {
	var rs;

	if( /^-?\d+(?:\.\d*)?(?:e[+\-]?\d+)?$/i.test(value) ) { rs = parseFloat(value); }
	else if( !isNaN(Date.parse(value)) ) { rs = new Date(value); }
	else { rs = value; }

	return rs;
}
,	parseValue = function(value, type) {
	var rs;

	value = value.trim();
	if(value.indexOf("'") == 0 || value.indexOf('"') == 0) {
		rs = value.substr(1, value.length - 2);
		if(type) { rs = dataTransfer(rs, type); }
	}
	else {
		if(type) {
			rs = dataTransfer(value, type);
		}
		if(typeof(value) == 'object') {
			rs = value;
		}
		else if(value == 'true') {
			rs = true;
		}
		else if(value == 'false') {
			rs = false;
		}
		else {
			rs = parseFloat(value);
		}
	}

	return rs;
}
var preCondiction = function(ast, schema) {
	!ast && (ast = {});
	!schema && (schema = { "columns": {} });

	if(ast.operator) {
		ast.right = parseValue(ast.right, schema.columns[ast.left]);
	}
	else if(ast.logic) {
		for(var key in ast.terms) {
			ast.terms[key] = preCondiction(ast.terms[key], schema);
		}
	}

	return ast;
}
,	parseSet = function(set) {
	var rs = {};
	for(var key in set) {
		var tmp = set[key].expression,
			pos = tmp.indexOf("=");

		if(pos == -1) { continue; }

		var column = tmp.slice(0, pos).trim(),
			value = parseValue(tmp.slice(pos + 1));

		rs[column] = value;
	}

	return rs;
}
,	compareSchema = function(data, schema) {
	var rs = {};
	!schema && (schema = { "columns": {} });
	if(typeof schema != 'object' || schema.strick === false) {
		for(var key in data) {
			rs[key] = dataTransfer(data[key]);
		}
	}
	else {
		for(var key in data) {
			if(schema.columns[key]) {
				rs[key] = dataTransfer(data[key], schema.columns[key]);
			}
		}
	}

	return rs;
};

var ecDB = function(conf, logger) {
	this.init(conf, logger);
};

ecDB.prototype.init = function(conf, logger) {
	!conf && (conf = {});
	this.config(conf);
	this.setDriverPath(conf.driverPath);
	this.setDriver(conf.driver);
	this.Parser = Parser;
	return this;
};
ecDB.prototype.setDriver = function(driver) {
	!!driver && (this.params.driver = driver);
	var Driver = require(this.params.driverPath + this.params.driver);
	this.DB = new Driver();
};
ecDB.prototype.setDriverPath = function(path) {
	!!path && (this.driverPath = path);
};
ecDB.prototype.config = function(config) {
	this.params = {
		driverPath: config.driverPath? config.driverPath: driverPath,
		driver: config.driver? config.driver: defaultDriver
	};
};
ecDB.prototype.connect = function(option, callback) {
	this.DB.connect(option, function(err) {
		rs = !err;
		if(typeof(callback) == 'function') {
			callback(err, true);
		}
		else {
			console.log('DB connect');
		}
	});

	return true;
};
ecDB.prototype.disconnect = function() {
	this.DB.disconnect();
};
ecDB.prototype.sql = function(sql, callback) {
	var self = this;
	var query = Parser.sql2ast(sql)
	,	operate;

	query.hasOwnProperty("SELECT") && (operate = "SELECT");
	query.hasOwnProperty("UPDATE") && (operate = "UPDATE");
	query.hasOwnProperty("INSERT INTO") && (operate = "INSERT");
	query.hasOwnProperty("DELETE FROM") && (operate = "DELETE");

	switch(operate) {
		case "SELECT":
			var table = query.FROM[0].table;
			this.listData(table, sql, callback);
			break;
		case "UPDATE":
			var table = query.UPDATE[0].table;
			this.getSchema(table, function(err, schema) {
				var cond = preCondiction(query.WHERE, schema),
					rowData = compareSchema( parseSet(query.SET), schema );

				self.updateData(table, query, rowData, function(err, data) {
					if(typeof(callback) == 'function') {
						callback(err, data);
					}
					else {
						console.log('execute SQL update');
						console.log(err || data);
					}
				});
			});
			break;
		case "INSERT":
			break;
		case "DELETE":
			var table = query['DELETE FROM'][0].table,
				cond = preCondiction(query.WHERE),
				limit = query.LIMIT;

			this.deleteData(table, query, function(err, data) {
				if(typeof(callback) == 'function') {
					callback(err, data);
				}
				else {
					console.log('execute SQL delete');
					console.log(err || data);
				}

			});
			break;
	}
};

ecDB.prototype.getID = function(table, n, callback) {
	var self = this;
	var rs, check;
	table = checkTable(table);

	if(!table) { return false; }

	this.DB.tableExist(table, function(err, data) {
		check = err? false: data;

		if(!check) {
			self.postTable(table, {}, function() {
				self.execGetID(table, n, callback);
			});
		}
		else {
			self.execGetID(table, n, callback);
		}
	});
};

ecDB.prototype.execGetID = function(table, n, callback) {
	if(n > 0) {
		this.DB.getIDs(table, n, function(err, data) {
			if(typeof(callback) == 'function') {
				callback(err, data);
			}
			else {
				console.log('get ID');
				console.log(err || data);
			}
		});
	}
	else {
		this.DB.getID(table, function(err, data) {
			if(typeof(callback) == 'function') {
				callback(err, data);
			}
			else {
				console.log('get ID');
				console.log(err || data);
			}
		});
	}
};

ecDB.prototype.getSchema = function(table, callback) {
	var rs;
	table = checkTable(table);

	if(!table) { return false; }

	this.DB.getSchema(table, function(err, data) {
		rs = err? false: data;
		if(rs) { rs.columns._id = 'Number'; }

		if(typeof(callback) == 'function') {
			callback(err, rs);
		}
		else {
			console.log('get Schema');
			console.log(err || rs);
		}
	});
};
ecDB.prototype.setSchema = function(table, schema, callback) {
	var self = this;
	table = checkTable(table, true);
	if(!table) { return false; }

	var rs;

	for(var key in schema) {
		if(key.indexOf('_', 0) == 0) { continue; }
		schema[key] = dataType(schema[key]);
	}

	this.getSchema(table, function(err, _schema) {
		if(_schema) {
			self.DB.setSchema(table, schema, function(_err, _data) {
				if(typeof(callback) == 'function') {
					callback(_err, true);
				}
				else {
					console.log('set Schema');
					console.log(_err || true);
				}
			});
		}
		else {
			self.DB.newSchema(table, schema, function(_err, _data) {
				if(typeof(callback) == 'function') {
					callback(_err, true);
				}
				else {
					console.log('set Schema');
					console.log(_err || true);
				}
			});
		}
	});
};
ecDB.prototype.setSchemaByValue = function(table, value, callback) {
	table = checkTable(table, true);
	if(!table) { return false; }

	var rs
	,	tableSchema = new Schema(table);

	for(var key in value) {
		if(key.indexOf('_', 0) == 0) { continue; }
		tableSchema.columns[key] = valueType(value[key]);
	}

	this.DB.setSchema(table, tableSchema, function(err) {
		rs = !err;
		if(typeof(callback) == 'function') {
			callback(err, rs);
		}
		else {
			console.log('set Schema by value');
			console.log(err || rs);
		}
	});
};
ecDB.prototype.listTable = function(callback) {
	var rs;

	this.DB.listTable(function(err, data) {
		rs = err? false: data;

		if(typeof(callback) == 'function') {
			callback(err, rs);
		}
		else {
			console.log('list Table');
			console.log(err || rs);
		}
	});
};
ecDB.prototype.getTable = function(table, callback) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs;

	this.DB.getTable(table, function(err, data) {
		rs = err? false: data;

		if(typeof(callback) == 'function') {
			callback(err, rs);
		}
		else {
			console.log('get Table');
			console.log(err || rs);
		}
	});
};
ecDB.prototype.postTable = function(table, schema, callback) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs;

	this.DB.postTable(table, schema, function(err, data) {
		rs = err? false: true;

		if(typeof(callback) == 'function') {
			callback(err, rs);
		}
		else {
			console.log('post Table');
			console.log(err || rs);
		}
	});
};
ecDB.prototype.putTable = function(table, schema, callback) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs;

	this.DB.putTable(table, schema, function(err, data) {
		rs = err? false: data;

		if(typeof(callback) == 'function') {
			callback(err, rs);
		}
		else {
			console.log('put Table');
			console.log(err || rs);
		}
	});
};
ecDB.prototype.cleanTable = function(table, callback) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs;

	this.DB.deleteData(table, {}, function(err, data) {
		rs = err? false: true;

		if(typeof(callback) == 'function') {
			callback(err, rs);
		}
		else {
			console.log('clean Table');
			console.log(err || rs);
		}
	});
};
ecDB.prototype.deleteTable = function(table, callback) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs;

	this.DB.deleteTable(table, function(err, data) {
		rs = err? false: true;

		if(typeof(callback) == 'function') {
			callback(err, rs);
		}
		else {
			console.log('delete Table');
			console.log(err || rs);
		}
	});
};
ecDB.prototype.listData = function(table, query, callback) {
	var self = this;
	table = checkTable(table);
	if(!table) { return false; }

	if(!query) { query = '' }
	else {
		query = checkSQL(query);
	}

	var rs
	,	cond = Parser.sql2ast(query);

	this.getSchema(table, function(err, schema) {
		cond.WHERE = preCondiction( cond.WHERE, schema );

		self.DB.listData(table, cond, function(_err, data) {
			rs = _err? false: data;
			if(_err) { rs = false; }
			else {
				var collection = new Collection();
				for(var key in data) {
					collection.add( compareSchema(data[key], schema) );
				}
				rs = collection.toJSON();

				if(typeof(callback) == 'function') {
					callback(_err, rs);
				}
				else {
					console.log('list Data');
					console.log(_err || rs);
				}
			}
		});
	});
};
ecDB.prototype.flowData = function(table, query, callback) {
	var self = this;
	table = checkTable(table);
	if(!table) { return false; }

	if(!query) { query = '' }
	else {
		query = checkSQL(query);
	}

	var rs
	,	cond = Parser.sql2ast(query);

	this.getSchema(table, function(err, schema) {
		cond.WHERE = preCondiction( cond.WHERE, schema );

		if(!cond.LIMIT) {
			cond.LIMIT = {
				"nb": defaultLimit
			};
		}
		else if(!cond.LIMIT.nb) {
			cond.LIMIT.nb = defaultLimit
		}

		self.DB.flowData(table, cond, function(_err, data) {
			rs = _err? false: data;
			if(_err) { rs = false; }
			else {
				var collection = new Collection();
				for(var key in data) {
					collection.add( compareSchema(data[key], schema) );
				}
				rs = collection.toJSON();

				if(typeof(callback) == 'function') {
					callback(_err, rs);
				}
				else {
					console.log('flow Data');
					console.log(_err || rs);
				}
			}
		});
	});
};
ecDB.prototype.pageData = function(table, query, callback) {
	var self = this;
	table = checkTable(table);

	if(!table) { return false; }

	if(!query) { query = '' }
	else {
		query = checkSQL(query);
	}

	var rs
	,	cond = Parser.sql2ast(query);


	this.getSchema(table, function(err, schema) {
		cond.WHERE = preCondiction( cond.WHERE, schema );

		if(!cond.LIMIT) {
			cond.LIMIT = {"nb": defaultLimit};
		}
		else if(!cond.LIMIT.nb) {
			cond.LIMIT.nb = defaultLimit;
		}

		self.DB.pageData(table, cond, function(_err, data) {
			rs = _err? false: data;
			if(_err) { callback(_err); }
			else {
				var collection = new Collection();
				for(var key in data) {
					collection.add( compareSchema(data[key], schema) );
				}
				rs = collection.toJSON();

				if(typeof(callback) == 'function') {
					callback(_err, rs);
				}
				else {
					console.log('page Data');
					console.log(_err || data);
				}
			}
		});
	});
};
ecDB.prototype.getData = function(table, id, callback) {
	var self = this;
	table = checkTable(table);
	if(!table) { return false; }

	var rs
	,	cond = Parser.sql2ast("WHERE _id = " + id);

	this.getSchema(table, function(err, schema) {
		cond.WHERE = preCondiction(cond.WHERE, schema);

		self.DB.getData(table, cond, function(_err, data) {
			rs = err? false: compareSchema(data, schema);

			if(typeof(callback) == 'function') {
				callback(_err, rs);
			}
			else {
				console.log('get Data');
				console.log(_err || rs);
			}
		});
	});
};
ecDB.prototype.find = function(table, query, callback) {
	var self = this;
	table = checkTable(table);
	if(!table) {
		if(typeof(callback) == 'function') {
			callback(err, false);
		}
		else {
			callback(true);
		}

		return false;
	}

	var rs;
	this.getSchema(table, function(err, schema) {
		self.DB.find(table, query, function(_err, _data) {
			if(err) { rs = false; }
			else {
				rs = [];
				for(var key in _data) {
					rs.push(compareSchema(_data[key], schema));
				}
			}

			if(typeof(callback) == 'function') {
				callback(_err, rs);
			}
			else {
				console.log('find');
				console.log(_err || rs);
			}
		});
	});
};

ecDB.prototype.importData = function(data, callback) {
	var rs;
	var table = "_" + Math.random().toString(36).substring(7);
	var jobs = 0;

	this.postData(table, data, function(err, _data) {
		callback(err, table);
	});
};

ecDB.prototype.dataFind = function(data, sql, callback) {
	var self = this;
	this.importData(data, function(err, table) {
		self.listData(table, sql, function(_err, _data) {
			self.deleteTable(table, function() {});

			if(typeof(callback) == 'function') {
				callback(_err, _data);
			}
			else {
				console.log('data find');
				console.log(_err || _data);
			}
		});
	});
};
ecDB.prototype.postData = function(table, data, callback) {
	var self = this;
	var check, rs, schema, id = [];
	var label = !!table? table.label: '';
	table = checkTable(table);
	if(!table) { return false; }

	this.getSchema(table, function(err, schema) {
		if(dataSize(schema.columns) == 0) {
			var valueSchema = getValueSchema(data);
			if(label) {
				var tableOBJ = {"name": table, "label": label};
				self.setSchema(tableOBJ, valueSchema, function() {});
			}
			else {
				self.setSchema(table, valueSchema, function() {});
			}

			schema = {columns: valueSchema};
		}
		schema.strict = !!schema.strict;

		var dl = util.isArray(data)? data.length: 1;
		self.getID(table, dl, function(_err, ID) {
			if(util.isArray(data)) {
				for(var key in data) {
					data[key] = compareSchema(data[key], schema);
					data[key]._id = ID + parseInt(key);
					id.push(data[key]._id);
				}
			}
			else {
				data = compareSchema(data, schema);
				data._id = ID;
				id.push(data._id);
			}

			self.DB.postData(table, data, function(__err, _data) {
				rs = err? false: id.join(', ');

				if(typeof(callback) == 'function') {
					callback(__err, rs);
				}
				else {
					console.log('post Data');
					console.log(_err || _data);
				}
			});
		});
	});
};
ecDB.prototype.updateData = function(table, q, data, callback) {
	var self = this;
	var rs;

	this.getSchema(table, function(err, schema) {
		var	query = Parser.sql2ast( checkSQL(q) ),
			cond = preCondiction(query.WHERE, schema),
			rowData = compareSchema( data, schema );

		self.DB.updateData(table, cond, {$set: rowData}, function(_err, _data) {
			rs = _err? false: _data;

			if(typeof(callback) == 'function') {
				callback(_err, rs);
			}
			else {
				console.log('update Data');
				console.log(_err || _data);
			}
		});
	});
};
ecDB.prototype.replaceData = function(table, id, data, callback) {
	var self = this;
	table = checkTable(table);
	if(!table) { callback(false, false); return false; }

	var rs, check

	this.getSchema(table, function(err, schema) {
		var	query = Parser.sql2ast("WHERE _id = " + id);
		query.WHERE = preCondiction( query.WHERE, schema );
		data = compareSchema(data, schema);

		self.DB.checkID(table, id, function(_err) {
			data._id = id;
			self.DB.replaceData(table, query, data, function(__err) {
				rs = __err? false: true;

				if(typeof(callback) == 'function') {
					callback(__err, rs);
				}
				else {
					console.log('replace Data');
					console.log(__err || rs);
				}
			});
		});
	});
};
ecDB.prototype.putData = function(table, id, data, callback) {
	var self = this;
	var label = table.label;
	table = checkTable(table);
	if(!table) { callback(false, false); return false; }
	var rs, check, query = Parser.sql2ast("WHERE _id = " + id);

	this.getSchema(table, function(err, schema) {
		if(dataSize(schema.columns) == 0) {
			var valueSchema = getValueSchema(data);
			if(label) {
				var tableOBJ = {"name": table, "label": label};
				self.setSchema(tableOBJ, valueSchema, function() {});
			}
			else {
				self.setSchema(table, valueSchema, function() {});
			}

			schema = {columns: valueSchema};
		}
		schema.strict = !!schema.strict;

		query.WHERE = preCondiction( query.WHERE, schema );
		data = compareSchema(data, schema);

		self.DB.checkID(table, id, function(_err, _data) {
			check = _err? false: _data;
		});

		newData = {$set: data};
		self.DB.putData(table, query, newData, function(_err, _data) {
			rs = _err? false: true;

			if(typeof(callback) == 'function') {
				callback(_err, rs);
			}
			else {
				console.log('put Data');
				console.log(_err || rs);
			}
		});
	});
};
ecDB.prototype.deleteData = function(table, query, callback) {
	var self = this;
	table = checkTable(table);
	if(!table) { return false; }

	var rs,	cond;
	this.getSchema(table, function(err, schema) {
		if(!!query.WHERE) {
			cond = query;
		}
		else if(new RegExp('^[0-9]*$').test(query)) {
			cond = Parser.sql2ast("WHERE _id = " + query);
			cond.WHERE = preCondiction( cond.WHERE, schema );
		}
		else {
			query = checkSQL(query);

			cond = Parser.sql2ast(query);console.log(query);
			cond.WHERE = preCondiction( cond.WHERE, schema );
		}

		var x = 0;
		for(var key in cond) {
			x++;
		}
		if(x == 0) { return false; }

		self.DB.deleteData(table, cond, function(_err, data) {
			rs = _err? false: true;

			if(typeof(callback) == 'function') {
				callback(_err, rs);
			}
			else {
				console.log('put Data');
				console.log(_err || rs);
			}
		});
	});
};

module.exports = ecDB;