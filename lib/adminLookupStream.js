var logger = require( 'pelias-logger' ).get( 'nlsfi-importer' );
var peliasAdminLookup = require( 'pelias-wof-admin-lookup' );

function createAdminLookupStream() {
  logger.info( 'Setting up admin value lookup stream.' );
  var pipResolver = peliasAdminLookup.createLocalWofPipResolver(
    null,
    ["locality", "neighbourhood", "postalcode"]
  );
  return peliasAdminLookup.createLookupStream(pipResolver);
}

module.exports = {
  create: createAdminLookupStream
};
