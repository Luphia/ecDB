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
```

## Use MongoDB
```node
var ECDB = require('ecdb');
var ecDB = new ECDB({"driver": "EasyMongo"});
ecDB.connect({"url": "mongodb://127.0.0.1"});
```

## Insert Data
ecDB would auto create the table and schema with your data
* insert single row
```node
ecDB.postData(
  'users',
  {name: 'WEI', birth: '1982-04-01', age: 33},
  function(error, result) {console.log(result);}
);
```
*  insert multiple rows
```node
ecDB.postData(
  'users',
  [
    {name: 'WEI', birth: '1982-04-01', age: 33},
    {name: 'Becca', birth: '1985-07-18', age: 30},
    {name: 'Gary', birth: '1989-12-11', age: 26}
  ],
  function(error, result) {console.log(result);}
);
```

## List All Tables
```node
ecDB.listTable();
```

## Get Table Schema
```node
ecDB.getTable('users');
```

## List Data in Table
* List all data
```node
ecDB.listData('TableName');
```
* You can also use search query
```node
ecDB.listData(
  'users',
  'where birth > "1988-01-01"'
);
```

## Serach JSON data
```node
ecDB.dataFind(
  [
    {path: '/aaa/bbb/ccc/'},
    {path:'/aaa/bbb'},
    {path:'/aaa/bbb/qqq/'}
  ],
  'where path like "*/bbb/*"',
  function(error, result) {
    console.log(result);
  }
);
```
