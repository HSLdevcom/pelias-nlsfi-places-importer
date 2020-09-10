const through = require('through2');
const logger = require( 'pelias-logger' ).get( 'nlsfi-places-importer' );
let venues = {};

var dedupedVenues = 0;

function dedupe( doc ){
  const layer = doc.getLayer();
  const postal = doc.parent.postalcode;
  const pop  = doc.getPopularity();
  var hash;

  if(!postal) {
    return true;
  }
  const name = doc.getName('default');
  if(!name) {
    return true;
  }
  const pos = doc.getCentroid();
  hash = name + postal + layer;

  let newVenue = true;
  if (!venues[hash]) {
    venues[hash] = [];
  } else {
    for (const v of venues[hash]) {
      // fast check for approximately 100 m wide surrounding latlon box
      if (Math.abs(pos.lat - v.lat)<0.001 && Math.abs(pos.lon - v.lon) < 0.001 && pop <= v.popularity) {
	// found existing nearby  item which is as popular, no need to create a duplicate
	newVenue = false;
	break;
      }
    }
  }
  if (newVenue) {
    venues[hash].push({'popularity': pop, 'lat': pos.lat, 'lon': pos.lon});
    return true;
  } else {
    dedupedVenues++;
    return false;
  }

  return true;
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
