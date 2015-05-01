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
db.listData('users', 'where authtime > "2012-10-10"')
db.listTable()
db.postTable('user', {name: 'String', birth: 'Date'})
db.postData('user', {name: 'A', birth: '1982-04-01'})
db.postData('user', {name: 'B', birth: '1988-09-18'})
db.postData('user', {name: 'B', birth: '1995-08-23'})
db.listData('user', "birth < '1990-01-01' and birth > '1984-01-01'");
db.postData('user', [{name: 'D', birth: '1982-05-01'}]);
db.postData('user', [{name: 'D', birth: '1982-05-01'}, {name: 'E', birth: '1982-06-01'}, {name: 'F', birth: '1982-07-01'}])

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

		var column = tmp.slice(0, pos),
			value = parseValue(tmp.slice(pos + 1));

		rs[column] = value;
	}

	return rs;
}
,	compareSchema = function(data, schema) {
	var rs = {};
	!schema && (schema = { "columns": {} });
	if(typeof schema != 'object' || !schema.strick) {
		for(var key in data) {
			rs[key] = dataTransfer(data[key]);
		}
	}
	else {
		for(var key in data) {
			rs[key] = dataTransfer(data[key], schema.columns[key]);
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
ecDB.prototype.connect = function(option) {
	var rs;
	this.DB.connect(option, function(err) {
		rs = !err;
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.disconnect = function() {
	this.DB.disconnect();
};
ecDB.prototype.sql = function(sql) {
	var query = Parser.sql2ast(sql)
	,	operate;

	query.hasOwnProperty("SELECT") && (operate = "SELECT");
	query.hasOwnProperty("UPDATE") && (operate = "UPDATE");
	query.hasOwnProperty("INSERT INTO") && (operate = "INSERT");
	query.hasOwnProperty("DELETE FROM") && (operate = "DELETE");

	switch(operate) {
		case "SELECT":
			var table = query.FROM[0].table;
			return this.listData(table, sql);
			break;
		case "UPDATE":
			var table = query.UPDATE[0].table;
				schema = this.getSchema(table),
				cond = preCondiction(query.WHERE, schema),
				rowData = compareSchema( parseSet(query.SET), schema );
			db.collection(table).update(cond, {$set: rowData}, {multi: true, upsert: true}, function(_err, _data) {
				if(_err) {
					this.logger.exception.error(_err);
					return setResult(res.result, next, 0, 'update failed');
				}
				setResult(res.result, next, 1, 'number of affected rows: ' + _data);
			});
			break;
		case "INSERT":
			break;
		case "DELETE":
			var table = query['DELETE FROM'][0].table,
				cond = preCondiction(query.WHERE),
				limit = query.LIMIT;
			db.collection(table).remove(cond, {justOne: limit && (limit.nb == 1)}, function(_err, _data) {
				if(_err) {
					this.logger.exception.error(_err);
					return setResult(res.result, next, 0, 'delete failed');
				}
				setResult(res.result, next, 1, 'number of affected rows: ' + _data);
			});
			break;
	}
};
ecDB.prototype.getID = function(table, n) {
	var rs, check;
	table = checkTable(table);

	if(!table) { return false; }

	this.DB.tableExist(table, function(err, data) {
		check = err? false: data;
	});

	while(check === undefined) {
		require('deasync').runLoopOnce();
	}

	if(!check) {
		this.postTable(table, {});
	}	

	if(n > 0) {
		this.DB.getIDs(table, n, function(err, data) {
			rs = err? false: data;
		});
	}
	else {
		this.DB.getID(table, function(err, data) {
			rs = err? false: data;
		});
	}

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.getSchema = function(table) {
	var rs;
	table = checkTable(table);

	if(!table) { return false; }

	this.DB.getSchema(table, function(err, data) {
		rs = err? false: data;
		if(rs) { rs.columns._id = 'Number'; }
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.setSchema = function(table, schema, replace) {
	table = checkTable(table, true);
	if(!table) { return false; }

	var rs;

	for(var key in schema) {
		if(key.indexOf('_', 0) == 0) { continue; }
		schema[key] = dataType(schema[key]);
	}

	if(this.getSchema(table)) {
		this.DB.setSchema(table, schema, function(err, data) {
			rs = !err;
		});
	}
	else {
		this.DB.newSchema(table, schema, function(err, data) {
			rs = !err;
		});
	}

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.setSchemaByValue = function(table, value) {
	table = checkTable(table, true);
	if(!table) { return false; }

	var rs
	,	tableSchema = new Schema(table);

	for(var key in value) {
		if(key.indexOf('_', 0) == 0) { continue; }
		tableSchema.columns[key] = valueType(value[key]);
	}

	this.DB.setSchema(table, tableSchema, function(err, data) {
		rs = !err;
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.listTable = function() {
	var rs;

	this.DB.listTable(function(err, data) {
		rs = err? false: data;
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.getTable = function(table) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs;

	this.DB.getTable(table, function(err, data) {
		rs = err? false: data;
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.postTable = function(table, schema) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs;

	this.DB.postTable(table, schema, function(err, data) {
		rs = err? false: true;
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.putTable = function(table, schema) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs;

	this.DB.putTable(table, schema, function(err, data) {
		rs = err? false: data;
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.cleanTable = function(table) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs;

	this.DB.deleteData(table, {}, function(err, data) {
		rs = err? false: true;
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.deleteTable = function(table) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs;

	this.DB.deleteTable(table, function(err, data) {
		rs = err? false: true;
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.listData = function(table, query) {
	table = checkTable(table);
	if(!table) { return false; }

	if(!query) { query = '' }
	else {
		query = checkSQL(query);
	}

	var rs
	,	schema = this.getSchema(table)
	,	cond = Parser.sql2ast(query);
	cond.WHERE = preCondiction( cond.WHERE, schema );

	this.DB.listData(table, cond, function(err, data) {
		rs = err? false: data;
		if(err) { rs = false; }
		else {
			var collection = new Collection();
			for(var key in data) {
				collection.add( compareSchema(data[key], schema) );
			}
			rs = collection.toJSON();
		}
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.flowData = function(table, query) {
	table = checkTable(table);
	if(!table) { return false; }

	if(!query) { query = '' }
	else {
		query = checkSQL(query);
	}

	var rs
	,	schema = this.getSchema(table)
	,	cond = Parser.sql2ast(query);
	cond.WHERE = preCondiction( cond.WHERE, schema );
	if(!cond.LIMIT) {
		cond.LIMIT = {
			"nb": defaultLimit
		};
	}
	else if(!cond.LIMIT.nb) {
		cond.LIMIT.nb = defaultLimit
	}

	this.DB.flowData(table, cond, function(err, data) {
		rs = err? false: data;
		if(err) { rs = false; }
		else {
			var collection = new Collection();
			for(var key in data) {
				collection.add( compareSchema(data[key], schema) );
			}
			rs = collection.toJSON();
		}
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.pageData = function(table, query) {
	table = checkTable(table);

	if(!table) { return false; }

	if(!query) { query = '' }
	else {
		query = checkSQL(query);
	}

	var rs
	,	schema = this.getSchema(table)
	,	cond = Parser.sql2ast(query);
	;

	if(!cond.LIMIT) {
		cond.LIMIT = {"nb": defaultLimit};
	}
	else if(!cond.LIMIT.nb) {
		cond.LIMIT.nb = defaultLimit;
	}

	this.DB.pageData(table, cond, function(err, data) {
		rs = err? false: data;
		if(err) { rs = false; }
		else {
			var collection = new Collection();
			for(var key in data) {
				collection.add( compareSchema(data[key], schema) );
			}
			rs = collection.toJSON();
		}
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.getData = function(table, id) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs
	,	schema = this.getSchema(table)
	,	cond = Parser.sql2ast("WHERE _id = " + id);
	cond.WHERE = preCondiction(cond.WHERE, schema);

	this.DB.getData(table, cond, function(err, data) {
		rs = err? false: compareSchema(data, schema);
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.find = function(table, data) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs
	,	schema = this.getSchema(table)
	;

	this.DB.find(table, data, function(err, _data) {
		if(err) { rs = false; }
		else {
			rs = [];
			for(var key in _data) {
				rs.push(compareSchema(_data[key], schema));
			}
		}
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.dataFind = function(data, sql) {
	var rs;
	var table = "_" + Math.random().toString(36).substring(7);
	var jobs = 0;
	if(typeof(sql) != 'string') { sql = ''; }

	var search = Parser.sql2ast(sql);
	search.WHERE = preCondiction(search.WHERE);
	var where = 0;
	for(var k in search.WHERE) {
		where ++;
	}

	if(util.isArray(data)) {
		for(var k in data) {

			jobs++;
			var query = Parser.sql2ast("WHERE _id = " + k);
			query.WHERE = preCondiction(query.WHERE);

			for(var kk in data[k]) {
				data[k][kk] = checkValue(data[k][kk]);
			}

			if(where > 0) {
				this.DB.putData(table, query, {"$set": data[k]}, function(err, _data) { jobs--; });
			}
		}

		if(where == 0) {
			return data;
		}
	}
	else {
		jobs++;
		this.DB.putData(table, {}, data, function(err, _data) {
			jobs--;
		});
	}
	while(jobs > 0) { require('deasync').runLoopOnce(); }

	this.DB.listData(table, search, function(err, _data) { rs = _data;});
	while(rs === undefined) { require('deasync').runLoopOnce(); }

	this.DB.deleteTable(table, function() {});
	return rs;
};
ecDB.prototype.postData = function(table, data) {
	var check, rs, schema, id = [];
	var label = table.label;
	table = checkTable(table);
	if(!table) { return false; }

	schema = this.getSchema(table);
	if(dataSize(schema.columns) == 0) {
		if(label) {
			var tableOBJ = {"name": table, "label": label};
			this.setSchema(tableOBJ, getValueSchema(data));
		}
		else {
			this.setSchema(table, getValueSchema(data));
		}
	}

	if(util.isArray(data)) {
		var ID = this.getID(table, data.length);
		for(var key in data) {
			data[key] = compareSchema(data[key], schema);
			data[key]._id = ID + parseInt(key);
			id.push(data[key]._id);
		}
	}
	else {
		data = compareSchema(data, schema);
		data._id = this.getID(table);
		id.push(data._id);
	}

	this.DB.postData(table, data, function(err, _data) {
		rs = err? false: id.join(', ');
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.updateData = function(table, q, data) {
	var rs,
		schema = this.getSchema(table),
		query = Parser.sql2ast( checkSQL(q) ),
		cond = preCondiction(query.WHERE, schema),
		rowData = compareSchema( data, schema );
		this.DB.updateData(table, cond, {$set: rowData}, function(_err, _data) {
			rs = _err? false: _data;
		});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.replaceData = function(table, id, data) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs, check
	,	schema = this.getSchema(table)
	,	query = Parser.sql2ast("WHERE _id = " + id);
	query.WHERE = preCondiction( query.WHERE, schema );
	data = compareSchema(data, schema);

	this.DB.checkID(table, id, function(err, _data) {
		check = err? false: _data;
	});

	while(check === undefined) {
		require('deasync').runLoopOnce();
	}

	data._id = id;
	this.DB.replaceData(table, query, data, function(err, _data) {
		rs = err? false: true;
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.putData = function(table, id, data) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs, check
	,	schema = this.getSchema(table)
	,	query = Parser.sql2ast("WHERE _id = " + id);
	query.WHERE = preCondiction( query.WHERE, schema );
	data = compareSchema(data, schema);

	this.DB.checkID(table, id, function(err, _data) {
		check = err? false: _data;
	});

	while(check === undefined) {
		require('deasync').runLoopOnce();
	}

	newData = {$set: data};
	this.DB.putData(table, query, newData, function(err, _data) {
		rs = err? false: true;
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};
ecDB.prototype.deleteData = function(table, query) {
	table = checkTable(table);
	if(!table) { return false; }

	var rs,	cond
	,	schema = this.getSchema(table);

	if(new RegExp('^[0-9]*$').test(query)) {
		cond = Parser.sql2ast("WHERE _id = " + query);
		cond.WHERE = preCondiction( cond.WHERE, schema );
	}
	else {
		query = checkSQL(query);

		cond = Parser.sql2ast(query);
		cond.WHERE = preCondiction( cond.WHERE, schema );
	}

	var x = 0;
	for(var key in cond) {
		x++;
	}
	if(x == 0) { return false; }

	this.DB.deleteData(table, cond, function(err, _data) {
		rs = err? false: true;
	});

	while(rs === undefined) {
		require('deasync').runLoopOnce();
	}

	return rs;
};

module.exports = ecDB;