var map_stream = require('through2-map');
var filter_stream = require('through2-filter');
var _ = require('lodash');
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
    '540': 'localadmin',
    '550': 'localadmin',
    '560': 'neighbourhood',
    '575': 'region',
    '580': 'region',
    '120': 'station'
  };

  return map_stream.obj(function(record) {
    var koodi = record['pnr:paikkatyyppiKoodi'];
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
  var allow = ['11', '21', '22'];
  return filter_stream.obj(function(record) {
    return allow.indexOf(record['pnr:paikkatyyppialaryhmaKoodi']) != -1;
  });
}

function createCoordinateConverter() {
  proj4.defs('EPSG:3067', '+proj=utm +zone=35 +ellps=GRS80 +units=m +no_defs');

  return map_stream.obj(function(record) {
    var location = record['pnr:paikkaSijainti']['gml:Point']['gml:pos'];
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
    if (record['pnr:nimi']) {
      record['pnr:nimi'] = record['pnr:nimi'].map(function(nimi) {
        var nameTxt = nimi['pnr:PaikanNimi']['pnr:kirjoitusasu'];
        var lang = langMapping[nimi['pnr:PaikanNimi']['pnr:kieliKoodi']];
        return {
          name: nameTxt,
          lang: lang
        };
      });
    }

    //find 'default' lang. use langList as priority list
    var defaultText = record['pnr:nimi'].reduce(function(previous, current) {
      var currentIndex = langList.indexOf(current.lang);
      if (currentIndex != -1 && currentIndex < langList.indexOf(previous.lang)) {
        return current;
      }
      return previous;
    }, record['pnr:nimi'][0]);

    record['pnr:nimi'].push({
      lang: 'default',
      name: defaultText.name
    });

    return record;
  });
}

var createDocumentGenerator = function() {
  return map_stream.obj(function(record) {
    var type = record.type || 'venue';
    var mmlDoc = new Document('nlsfi', type, 'mml-' + record['pnr:paikkaID']);
    record['pnr:nimi'].forEach(function(name) {
      mmlDoc.setName(name.lang, name.name);
    });

    mmlDoc.setCentroid({
      lat: record.lat,
      lon: record.lon
    });
    var kuntaKoodi = record['pnr:kuntaKoodi'];
    var maakuntaKoodi = record['pnr:maakuntaKoodi'];
    var kunta = kunnat[kuntaKoodi];
    var maakunta = maakunnat[maakuntaKoodi];
    var maakuntaNimi = maakunta[0].text;
    var kuntaNimi = kunta[0].text;

    kuntaKoodi = 'mml-' + kuntaKoodi;
    maakuntaKoodi = 'mml-' + maakuntaKoodi;

    if(type !== 'localadmin' && type !== 'region') {
      for(var i in kunta) {
        try { mmlDoc.addParent('localadmin', kunta[i].text, kuntaKoodi); }
        catch (err) { logger.info('invalid localadmin', err); }
      }
    }

    if(type !== 'region') {
      for(var j in maakunta) {
        try { mmlDoc.addParent('region', maakunta[j].text, maakuntaKoodi); }
        catch(err) { logger.info('invalid region', err); }
      }
    }

    switch(type) {
    case 'station': mmlDoc.setPopularity(50000); break;
    case 'region': mmlDoc.setPopularity(20000); break;
    case 'localadmin': mmlDoc.setPopularity(30000); break;
      // Nlsfi pois and neighbourhoods are genarally not preferred, so set a low priority
      // Pois a re often very minor targets such as 'niitty' or 'talo'.
      // Neighbourhood is a geometric centerpoint and therefore bad for routing
    default: mmlDoc.setPopularity(3); break;
    }

    if(record.category) {
      mmlDoc.addCategory(record.category);
    }
    try { mmlDoc.addParent('country', 'Finland', "85633143", "FIN"); }
    catch(err) { logger.info('invalid country', err); }

    try { mmlDoc.addParent('country', 'Suomi', "85633143", "FIN"); }
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

