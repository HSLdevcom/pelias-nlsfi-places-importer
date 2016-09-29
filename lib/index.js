var processor = require('./readStream');
var fs = require('fs');
var argv = require('minimist')(process.argv.slice(2));
var logger = require('pelias-logger').get('nlsfi-places-importer');

processor.processData(argv.d, function() {
  console.log("all done!");
});
