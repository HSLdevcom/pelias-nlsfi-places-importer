var processor = require('./readStream');
var fs = require('fs');
var argv = require('minimist')(process.argv.slice(2));
var logger = require('pelias-logger').get('nlsfi-places-importer');
var elasticsearch = require('elasticsearch');

const hashes = {};
var hashCount = 0;

// OSM import is 2nd step after GTFS  stop/station import
// read all existing stations from ES into a hashtable for deduping

function addHash(hit) {
  const doc = hit._source;
  const name = doc.name.default;
  const postal = doc.parent.postalcode;

  if(name && postal) {
    const hash = name + postal + doc.layer;
    hashes[hash] = true;
    hashCount++;
  }
}

const client = new elasticsearch.Client({
  host: 'localhost:9200',
  apiVersion: '7.6',
});

async function readHashes(layer) {
  const responseQueue = [];

  logger.info( 'Reading existing ' + layer + 's for deduping');
  const response = await client.search({
    index: 'pelias',
    scroll: '30s',
    size: 10000,
    body: {
      'query': {
        'term': {
          'layer': {
            'value': layer,
            'boost': 1.0
          }
        }
      }
    }
  });
  responseQueue.push(response);

  while (responseQueue.length) {
    const body = responseQueue.shift();
    body.hits.hits.forEach(addHash);

    // check to see if we have collected all docs
    if (!body.hits.hits.length) {
      logger.info('Extracted ' + hashCount + ' ' + layer + 's');
      break;
    }
    // get the next response if there are more items
    responseQueue.push(
      await client.scroll({
        scrollId: body._scroll_id,
        scroll: '30s'
      })
    );
  }
}

readHashes('station').then(() => {
  readHashes('venue').then(() => {
    logger.info( 'Starting NLSFI import');
    processor.processData(argv.d, hashes, function() {
      console.log('NLSFI import done!');
    })
  });
});

