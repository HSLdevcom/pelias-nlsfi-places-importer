var map_stream = require('through2-map');
var filter_stream = require('through2-filter');
var _ = require('lodash');
var proj4 = require('proj4');
var Document = require('pelias-model').Document;
var schemaParser = require('./schema-parser');

function createFieldMapper() {

  var preserveNames = [
    "pnr:paikkaID",
    "pnr:nimi",
    "pnr:rinnakkaisnimi",
    "pnr:paikkaSijainti",
    "pnr:paikkatyyppiKoodi",
    "pnr:paikkatyyppialaryhmaKoodi",
    "pnr:kuntaKoodi",
    "pnr:maakuntaKoodi"
  ];

  return map_stream.obj(function(record) {
    return _.pick(record, preserveNames);
  });
};

/**
 * Map paikkatyyppi to type
 */
function createTypeMapper() {

  var types = {
    "540": "locality",
    "550": "locality",
    "560": "neighborhood"
  }

  return map_stream.obj(function(record) {
    record.type = types[record["pnr:paikkatyyppiKoodi"]] || 'geoname';
    return record;
  });
}

function createPaikkatyyppiAlaryhmaFilter() {
  var allow = ['11', '21', '22'];
  return filter_stream.obj(function(record) {
    return allow.indexOf(record["pnr:paikkatyyppialaryhmaKoodi"]) != -1;
  });
}

function createCoordinateConverter() {
  proj4.defs("EPSG:3067", "+proj=utm +zone=35 +ellps=GRS80 +units=m +no_defs");

  return map_stream.obj(function(record) {
    var location = record["pnr:paikkaSijainti"]["gml:Point"]["gml:pos"];
    delete record.location;
    var srcCoords = location.split(" ").map(function(n) {
      return Number(n)
    });
    var dstCoords = proj4("EPSG:3067", "WGS84", srcCoords);
    record.lat = dstCoords[1];
    record.lon = dstCoords[0];
    return record;
  });
}

function createNameConverter() {

  langMapping = {'swe':'sv','fin':'default'}

  return map_stream.obj(function(record) {
    if (record["pnr:nimi"]) {
      record["pnr:nimi"] = record["pnr:nimi"].map(function(nimi) {
        var nameTxt = nimi["pnr:PaikanNimi"]["pnr:kirjoitusasu"];
        var lang = langMapping[nimi["pnr:PaikanNimi"]["pnr:kieliKoodi"]] || 'default';
        return {
          name: nameTxt,
          lang: lang
        };
      });
    }
    return record;
  });

}

var createDocumentGenerator = function(kunnat, maakunnat) {
  return map_stream.obj(function(record) {


    var mmlDoc = new Document(record.type, "mml-" + record["pnr:paikkaID"]);


    if (record["pnr:nimi"].length>0) {
      record["pnr:nimi"].forEach(function (name){
        mmlDoc.setName(name.lang, name.name);
      });
    } else {
      mmlDoc.setName("default", record["pnr:nimi"].name);
    }

    mmlDoc.setMeta("author", "mml");
    mmlDoc.setMeta("mmlType", record["pnr:paikkatyyppiKoodi"]);

    mmlDoc.setAdmin('locality', kunnat[record["pnr:kuntaKoodi"]][0].text);
    mmlDoc.setAdmin('admin1', maakunnat[record["pnr:maakuntaKoodi"]][0].text);

    mmlDoc.setAdmin('admin0', 'Finland');

    mmlDoc.setPopularity(1000000);

    mmlDoc.setCentroid({
      lat: record.lat,
      lon: record.lon
    });

    console.log(mmlDoc);
    return mmlDoc;
  });
};

module.exports = {
  createFieldMapper: createFieldMapper,
  createPaikkatyyppiAlaryhmaFilter: createPaikkatyyppiAlaryhmaFilter,
  createCoordinateConverter: createCoordinateConverter,
  createDocumentGenerator: createDocumentGenerator,
  createTypeMapper: createTypeMapper,
  createNameConverter: createNameConverter
};
