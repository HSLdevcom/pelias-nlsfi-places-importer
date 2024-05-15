var map_stream = require('through2-map');
var filter_stream = require('through2-filter');
var proj4 = require('proj4');
var Document = require('pelias-model').Document;
var logger = require('pelias-logger').get('nlsfi-places-importer');

var kunnat, maakunnat, paikkatyypit;

// must be called before - use - of stream components below
function defineAdmin(_kunnat, _maakunnat, _paikkatyypit) {
  kunnat = _kunnat;
  maakunnat = _maakunnat;
  paikkatyypit = _paikkatyypit;
}

/**
 * Map paikkatyyppi to type
 */
function createTypeMapper() {

  var types = {
    '3010110': 'neighbourhood', // Village or neighbourhood
    '4010120': 'region',
    '4010125': 'localadmin', // Municipality
    '6010105': 'station', // Bus station
    '6010205': 'station', // Railway station or stop
  };

  return map_stream.obj(function(record) {
    var koodi = record['placeType'];
    if(types[koodi]) {
      record.type = types[koodi];
    } else {
      var tyyppi = paikkatyypit[koodi];
      record.category=tyyppi[2].text; // english version
    }
    return record;
  });
}

function createPaikkatyyppiAlaryhmaFilter() {
  /* Filter out some data that we don't want
    by placeTypeSubgroup:
    0 - Määrittelemättömät kohteet
    10104 - Vedenalaiset pinnanmuodot
    10203 - Pellot
    10204 - Matalan kasvillisuuden alueet
    10205 - Louhikot ja kivikot
    19999 - Muut maastopaikat
    29999 - Muut vesipaikat
    30301 - Talot
    39999 - Muut asutuskohteet
    40102 - Hallintorajat
    60101 - Tieliikennepaikat
    60102 - Raideliikennepaikat
    60103 - Vesiliikennepaikat
    60104 - Ilmaliikennepaikat
    60105 - Tietoliikennepaikat
    60199 - Muut liikennepaikat
    70101 - Maatalouden yksiköt
    70102 - Riista- ja kalatalouden yksiköt
    70103 - Maa-aineksenottoalueet

    by placeType:
    2010215 - Järven osa
  */
  var denyPlaceType = ['2010215'];
  var denyPlaceTypeSubgroup = ['0', '10104', '10203', '10204', '10205', '19999', '29999', '30301', '39999', '40102', '60101', '60102', '60103', '60104', '60105', '60199', '70101', '70102', '70103'];
  return filter_stream.obj(function(record) {
    return denyPlaceTypeSubgroup.indexOf(record['placeTypeSubgroup']) == -1 || denyPlaceType.indexOf(record['placeType']) == -1;
  });
}

function createCoordinateConverter() {
  proj4.defs('EPSG:3067', '+proj=utm +zone=35 +ellps=GRS80 +units=m +no_defs');

  return map_stream.obj(function(record) {
    var location = record['placeLocation']['gml:Point']['gml:pos'];
    var srcCoords = location.split(' ').map(function(n) {
      return Number(n);
    });
    var dstCoords = proj4('EPSG:3067', 'WGS84', srcCoords);
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
    'sms': 'sms',
    'eng': 'en'
  };
  var langList = ['fi', 'sv', 'se', 'smn', 'sms', 'en'];

  return map_stream.obj(function(record) {
    if (record['name']) {
      record['name'] = record['name'].map(function(nimi) {
        var nameTxt = nimi['Name']['spelling'];
        var lang = langMapping[nimi['Name']['language']];
        return {
          name: nameTxt,
          lang: lang
        };
      });
    }

    //find 'default' lang. use langList as priority list
    var defaultText = record['name'].reduce(function(previous, current) {
      var currentIndex = langList.indexOf(current.lang);
      if (currentIndex != -1 && currentIndex < langList.indexOf(previous.lang)) {
        return current;
      }
      return previous;
    }, record['name'][0]);

    record['name'].push({
      lang: 'default',
      name: defaultText.name
    });

    return record;
  });
}

const lang = ['fin','swe','eng'];

var createDocumentGenerator = function() {
  return map_stream.obj(function(record) {
    var type = record.type || 'venue';
    var mmlDoc = new Document('nlsfi', type, 'mml-' + record['placeId']);

    var defaultName;
    record['name'].forEach(function(name) {
      if (name.lang === 'default') {
	defaultName = name.name;
      }
    });
    mmlDoc.setName('default', defaultName);
    record['name'].forEach(function(name) {
      if (name.name !== defaultName) {
	mmlDoc.setName(name.lang, name.name);
      }
    });

    mmlDoc.setCentroid({
      lat: record.lat,
      lon: record.lon
    });
    var kuntaKoodi = record['municipality'];
    var maakuntaKoodi = record['region'];
    var kunta = kunnat[kuntaKoodi];
    var maakunta = maakunnat[maakuntaKoodi];
    // var maakuntaNimi = maakunta[0].text;
    // var kuntaNimi = kunta[0].text;

    kuntaKoodi = 'mml-' + kuntaKoodi;
    maakuntaKoodi = 'mml-' + maakuntaKoodi;

    if(type !== 'localadmin' && type !== 'region') {
      lang.forEach( l => {
	if(kunta[l]) {
          try { mmlDoc.addParent('localadmin', kunta[l].text, kuntaKoodi); }
          catch (err) { logger.info('invalid localadmin', err); }
	}
      })
    }

    if(type !== 'region') {
      lang.forEach( l => {
	if(maakunta[l]) {
          try { mmlDoc.addParent('region', maakunta[l].text, maakuntaKoodi); }
          catch(err) { logger.info('invalid region', err); }
	}
      })
    }

    switch(type) {
    case 'station': mmlDoc.setPopularity(10000); break;
    case 'region': mmlDoc.setPopularity(20); break;
    case 'localadmin': mmlDoc.setPopularity(15); break;
    case 'neighbourhood': mmlDoc.setPopularity(4); break;
      // Nlsfi pois are genarally not preferred, so set a low priority
      // Pois a re often very minor targets such as 'niitty' or 'talo'.
      // Neighbourhood is a geometric centerpoint and therefore bad for routing
    default: mmlDoc.setPopularity(3); break;
    }

    if(record.category) {
      mmlDoc.addCategory(record.category);
    }

    try { mmlDoc.addParent('country', 'Suomi', '85633143', 'FIN'); }
    catch(err) { logger.info('invalid country', err); }

    //  logger.info('MmlDoc: ', JSON.stringify(mmlDoc));
    return mmlDoc;
  });
};

module.exports = {
  defineAdmin: defineAdmin,
  createPaikkatyyppiAlaryhmaFilter: createPaikkatyyppiAlaryhmaFilter,
  createCoordinateConverter: createCoordinateConverter,
  createDocumentGenerator: createDocumentGenerator,
  createTypeMapper: createTypeMapper,
  createNameConverter: createNameConverter
};

