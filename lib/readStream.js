var fs = require('fs'),
  path = require('path'),
  XmlStream = require('xml-stream'),
  through = require('through'),
  components = require('./readStreamComponents'),
  peliasDbClient = require( 'pelias-dbclient' ),
  deduper = require( './deduper' ),
  blacklist = require( './blacklist' ),
  model = require( 'pelias-model' ),
  logger = require('pelias-logger').get('nlsfi-places-importer'),
  adminLookupStream = require('pelias-wof-admin-lookup');

function processData(dir, existingHashes, callback) {

  var kunta_schema = fs.createReadStream(path.join(dir, 'municipality.xsd'));
  var maakunta_schema = fs.createReadStream(path.join(dir, 'region.xsd'));
  var paikkatyyppi_schema = fs.createReadStream(path.join(dir, 'placetype.xsd'));

  var kunnat, maakunnat, paikkatyypit;

  function parsePaikat() {

    components.defineAdmin(kunnat, maakunnat, paikkatyypit);

    var count = 0; // parsed
    var prevcount = 0;
    var inside = 0; // count of items entered our pipeline
    var paused = false;

    // Create a file stream and pass it to XmlStream
    var stream = fs.createReadStream(path.join(dir, 'places.xml'));

    var xml = new XmlStream(stream);

    var intervalId = setInterval(function() {
      if (count>prevcount) {
        logger.info('Number of records parsed: ' + count);
        prevcount=count;
      }
    }, 10000);

    var r = through(function write(data) {
      this.queue(data);
    }, function end() { //optional
      this.queue(null);
      clearInterval(intervalId);
    });

    /* Import pipeline is by default badly unbalanced: for some reason XML parser gets most CPU time
       and eats all memory by continuously pushing new records into the rest of the pipeline which does
       not get much processing time. One reason to this is that xml-parser is not connected to import
       using the usual pipe method but writes explicitly from a parsing callback. This apparently breaks
       stream's pause/resume mechanism.

       Fortunately, xml-stream has a pausing control. So we simply count parsed records and records, which
       have entered the pipeline. If the difference grows too big, we halt the parsing until the pipeline
       has consumed the buffered records.
    */
    var sync = through(function write(data) {
      this.queue(data);
      inside++;
      if(paused && count-inside<1000) { // pipeline is empty enough, resume parsing
        paused=false;
        xml.resume();
      }
    });

    xml.on('endElement: Place', function(item) {
      // console.log('writing item', item);
      count++;
      r.write(item);
      if(!paused && count-inside>10000) { // parser is getting too far ahead, slow down
        xml.pause();
        paused=true;
      }
    });

    xml.collect('name');

    xml.on('end', function() {
      // console.log('stream end');
      r.end();
    });

    r.pipe(sync)
      .pipe(components.createPaikkatyyppiAlaryhmaFilter())
      .pipe(components.createNameConverter())
      .pipe(components.createCoordinateConverter())
      .pipe(components.createTypeMapper())
      .pipe(components.createDocumentGenerator())
      .pipe(blacklist())
      .pipe(adminLookupStream.create())
      .pipe(deduper(existingHashes))
      .pipe(model.createDocumentMapperStream())
      .pipe(peliasDbClient())
      .on('finish', callback);
  }

  function maakunnatDone(mk) {
    maakunnat = mk;
    parsePaikat();
  }

  function kunnatDone(k) {
    kunnat = k;
    require('./schema-parser')(maakunta_schema, maakunnatDone);
  }


  function paikkatyypitDone(pt) {
    paikkatyypit = pt;
    require('./schema-parser')(kunta_schema, kunnatDone);
  }

  require('./schema-parser')(paikkatyyppi_schema, paikkatyypitDone);
}

module.exports = {
  processData: processData
};
