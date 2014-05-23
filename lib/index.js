var opt = require('minimist');
var csv = require('csv');
var fs = require('fs');
var util = require('util');
var MongoClient = require('mongodb').MongoClient;
var Server = require('mongodb').Server;

var argv = opt(process.argv.slice(2));
var files = argv._; // arguments that don't have options before them

var DB_COLLECTION = 'csv';

var handle = function(file, csvDb, mongoClient) {

  var input = fs.createReadStream('./' + file);
  var parser = csv.parse({columns: true});
  var transformer = csv.transform(function(data){
    // because I set {columns:true} in parser, the data will be an object.
    // Otherwise, it'll be an array.
    // parser options: https://github.com/wdavidw/node-csv-parse#parser-options
    return data;
  });

  input.on('readable', function(){
    while(data = input.read()) {
      parser.write(data);
    }
  });

  parser.on('readable', function(){
    while(data = parser.read()){
      transformer.write(data);
    }
  });

  transformer.on('readable', function(){
    while(data = transformer.read()){
      // after transformation. save to mongo
      csvDb.collection(DB_COLLECTION).insert(
        data, {safe: true}, function(err, records){
          if(err){
            console.log('error occurred while processing `%s`, error `%s` ',
              data, err);
          }
        }
      );

      console.log('processed '+ JSON.stringify(data));
    }
  });
};

var mongoClient = new MongoClient(new Server('localhost', 27017));

mongoClient.open(function(err, mongoClient) {

  var csvDb = mongoClient.db("csvDb");

  files.forEach(function(f) {
    handle(f, csvDb, mongoClient);
  });

});
