# ecDB
Make DB operation Easy

## Install
https://www.npmjs.com/package/ecdb
```shell
npm install ecdb
```

## Use
```shell
var ECDB = require('ecdb');
var ecDB = new ECDB();
ecDB.connect({"url": "mongodb://127.0.0.1"});

ecDB.listTable();
ecDB.listData('TableName');
```
