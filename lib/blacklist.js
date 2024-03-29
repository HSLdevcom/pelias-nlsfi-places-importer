const through = require('through2');
const logger = require( 'pelias-logger' ).get( 'nlsfi-places-importer' );
const config = require('pelias-config').generate();

module.exports = function() {
  const blacklist = {};
  let removed = 0;

  if(config && config.imports && config.imports.blacklist) {
    bl = config.imports.blacklist;
    if (Array.isArray(bl) && bl.length > 0) {
      bl.forEach(id => blacklist[id] = true);
      logger.info('Blacklist size: ' + bl.length);
    }
  }
  return through.obj(function( record, enc, next ) {
    if (!blacklist[record.getId()] && record.getLayer() !== 'station') {
      this.push(record);
    } else {
      removed++;
    }
    next();
  }, function(next) {
    if (removed) {
      logger.info('Blacklisted: ' + removed);
    }
    next();
  });
};
