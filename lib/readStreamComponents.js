
var map_stream = require('through2-map');
var filter_stream = require('through2-filter');
var _ = require('lodash');
var proj4 = require('proj4');
var Document = require('pelias-model').Document;
var schemaParser = require('./schema-parser');

/**
 * Map paikkatyyppi to type
 */
function createTypeMapper() {

  var types = {
    "540": "locality",
    "550": "locality",
    "560": "neighborhood",
    "120": "station"
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

  var langMapping = {
    'fin': 'fi',
    'swe': 'sv',
    'sme': 'se',
    'smn': 'smn',
    'sms': 'sms'
  };
  var langList = ["fi", "sv", "se", "smn", "sms"];

  return map_stream.obj(function(record) {
    if (record["pnr:nimi"]) {
      record["pnr:nimi"] = record["pnr:nimi"].map(function(nimi) {
        var nameTxt = nimi["pnr:PaikanNimi"]["pnr:kirjoitusasu"];
        var lang = langMapping[nimi["pnr:PaikanNimi"]["pnr:kieliKoodi"]];
        return {
          name: nameTxt,
          lang: lang
        };
      });
    }

    //find "default" lang. use langList as priority list
    var defaultText = record["pnr:nimi"].reduce(function(previous, current) {
      var currentIndex = langList.indexOf(current.lang);
      if (currentIndex != -1 && currentIndex < langList.indexOf(previous.lang)) {
        return current;
      }
      return previous;
    }, record["pnr:nimi"][0]);

    record["pnr:nimi"].push({
      lang: "default",
      name: defaultText.name
    });

    return record;
  });
}

var createDocumentGenerator = function(kunnat, maakunnat) {
  return map_stream.obj(function(record) {

    var mmlDoc = new Document(record.type, "mml-" + record["pnr:paikkaID"]);

    var phrases = {};
    record["pnr:nimi"].forEach(function(name) {
      mmlDoc.setName(name.lang, name.name);
      phrases[name.lang] = name.name;
    });

//    mmlDoc.phrase = phrases;

//    mmlDoc.setAdmin('locality', kunnat[record["pnr:kuntaKoodi"]][0].text);
//    mmlDoc.setAdmin('admin1', maakunnat[record["pnr:maakuntaKoodi"]][0].text);

    mmlDoc.addParent('locality', kunnat[record["pnr:kuntaKoodi"]][0].text);
    mmlDoc.addParent('country', 'Finland');
//    mmlDoc.setAdmin('admin0', 'Suomi');

    mmlDoc.setCentroid({
      lat: record.lat,
      lon: record.lon
    });

//    console.log(mmlDoc);
    return mmlDoc;
  });
};

module.exports = {
  createPaikkatyyppiAlaryhmaFilter: createPaikkatyyppiAlaryhmaFilter,
  createCoordinateConverter: createCoordinateConverter,
  createDocumentGenerator: createDocumentGenerator,
  createTypeMapper: createTypeMapper,
  createNameConverter: createNameConverter
};
