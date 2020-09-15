const through = require('through2');
const logger = require( 'pelias-logger' ).get( 'nlsfi-places-importer' );
let venues = {};

var dedupedVenues = 0;

function dedupe( doc ){
  const layer = doc.getLayer();
  const postal = doc.parent.postalcode;
  var hash;

  if(!postal) {
    return true;
  }
  const name = doc.getName('default');
  if(!name) {
    return true;
  }
  hash = name + postal + layer;

  if (!venues[hash]) {
    venues[hash] = true;
    return true;
  } else {
    dedupedVenues++;
    return false;
  }
}

module.exports = function(existingVenueHashes) {
  if (existingVenueHashes) {
    venues = existingVenueHashes;
  }
  return through.obj(function( record, enc, next ) {
    if (dedupe(record)) {
      this.push(record);
    }
    next();
  }, function(next) {
    logger.info('Deduped venues: ' + dedupedVenues);
    next();
  });
};
