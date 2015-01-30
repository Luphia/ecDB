var ECDB = require('../index.js');
var ecDB = new ECDB();
var url = "mongodb://127.0.0.1";
if(ecDB.connect({"url": url})) {
	console.log("It works !!");

	ecDB.disconnect();
}
else {
	console.log("Oops, it dosen't work with %s !!", url);
}
