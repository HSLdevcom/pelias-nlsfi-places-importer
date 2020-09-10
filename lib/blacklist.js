const through = require('through2');
const logger = require( 'pelias-logger' ).get( 'nlsfi-places-importer' );
const config = require('pelias-config').generate();

logger.info(JSON.stringify(config));

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
  else process.exit(1);
  return through.obj(function( record, enc, next ) {
    if (!blacklist[record.getId]) {
      this.push(record);
    } else {
      removed++;
    }
    next();
  }, function(next) {
    if (blacklist) {
      logger.info('Blacklisted: ' + removed);
    }
    next();
  });
};
