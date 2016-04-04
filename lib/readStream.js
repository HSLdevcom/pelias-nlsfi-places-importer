var fs = require('fs'),
  path = require('path'),
  XmlStream = require('xml-stream'),
  through = require('through'),
  components = require('./readStreamComponents'),
  peliasDbClient = require( 'pelias-dbclient' ),
  model = require( 'pelias-model' ),
  logger = require('pelias-logger').get('nlsfi-places-importer');

function processData(dir, callback) {

  var kunta_schema = fs.createReadStream(path.join(dir, "kunta.xsd"));
  var maakunta_schema = fs.createReadStream(path.join(dir, "maakunta.xsd"));

  require('./schema-parser')(kunta_schema, function(kunnat) {
    require('./schema-parser')(maakunta_schema, function(maakunnat) {

      var count = 0;
      // console.log("kunnat done", kunnat)

      // Create a file stream and pass it to XmlStream
      var stream = fs.createReadStream(path.join(dir, "paikka.xml"));

      var xml = new XmlStream(stream);

      var intervalId = setInterval(function() {
        logger.info("Number of records parsed: " + count);
      }, 10000);

      var r = through(function write(data) {
          this.queue(data)
        },
        function end() { //optional
          this.queue(null)
          clearInterval(intervalId);
        });

      xml.on('endElement: pnr:Paikka', function(item) {
        // console.log("writing item", item);
        count++;
        r.write(item);
      });

      xml.collect("pnr:nimi")

      xml.on('end', function() {
        // console.log("stream end");
        r.end();
      });

      r.pipe(components.createPaikkatyyppiAlaryhmaFilter())
        .pipe(components.createNameConverter())
        .pipe(components.createCoordinateConverter())
        .pipe(components.createTypeMapper())
        .pipe(components.createDocumentGenerator(kunnat, maakunnat))
        .pipe(model.createDocumentMapperStream())
        .pipe(peliasDbClient())
        .on('finish', callback);
    });
  });
}

module.exports = {
  processData: processData
}
