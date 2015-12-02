var processor = require ('./readStream');
var fs        = require('fs');
var argv = require('minimist')(process.argv.slice(2));

processor.processData(argv['d'], function() {
  console.log("all done!");
});
