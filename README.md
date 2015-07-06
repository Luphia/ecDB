# ecDB
[![Build Status](https://travis-ci.org/Luphia/ecDB.png?branch=master)](https://travis-ci.org/Luphia/ecDB)
[![Deps Status](https://david-dm.org/Luphia/ecDB.png)](https://david-dm.org/Luphia/ecDB)

Make DB operation Easy

## Install
https://www.npmjs.com/package/ecdb
```shell
npm install ecdb
```

## Use
```node
var ECDB = require('ecdb');
var ecDB = new ECDB();
ecDB.connect();

ecDB.listTable();
ecDB.listData('TableName');
```

## Use MongoDB
```node
var ECDB = require('ecdb');
var ecDB = new ECDB({"driver": "EasyMongo"});
ecDB.connect({"url": "mongodb://127.0.0.1"});

ecDB.listTable();
ecDB.listData('TableName');
```