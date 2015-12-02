var fs = require('fs'),
  path = require('path'),
  XmlStream = require('xml-stream'),
  _ = require('lodash');
var through = require('through');
var components = require('./readStreamComponents');
var sink = require('through2-sink');

function processData(dir, callback) {


  var kunta_schema = fs.createReadStream(path.join(dir, "kunta.xsd"));
  var maakunta_schema = fs.createReadStream(path.join(dir, "maakunta.xsd"));

  require('./schema-parser')(kunta_schema, function(kunnat) {
    require('./schema-parser')(maakunta_schema, function(maakunnat) {

      //    console.log("kunnat done", kunnat)

      // Create a file stream and pass it to XmlStream
      var stream = fs.createReadStream(path.join(dir, "paikka.xml"));

      var xml = new XmlStream(stream);

      var r = through(function write(data) {
          this.queue(data)
        },
        function end() { //optional
          this.queue(null)
        });

      xml.on('endElement: pnr:Paikka', function(item) {
//        console.log("writing item", item);
        r.write(item);
      });

      xml.collect("pnr:nimi")

      xml.on('end', function() {
        //      console.log("stream end");
        r.end();
      });

      r.pipe(components.createPaikkatyyppiAlaryhmaFilter())
        .pipe(components.createFieldMapper())
        .pipe(components.createNameConverter())
        .pipe(components.createCoordinateConverter())
        .pipe(components.createTypeMapper())
        .pipe(components.createDocumentGenerator(kunnat, maakunnat))
        .pipe(require('./elasticsearchPipeline')())
        .on('finish', callback);
    });
  });
}

module.exports = {
  processData: processData
}
