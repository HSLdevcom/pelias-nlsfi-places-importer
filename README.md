
# pelias-nlsfi-places-importer
National Land Survey places importer for Pelias

## Install dependencies

```bash
npm install
```

## Usage

`node --max-old-space-size=6200 lib/index.js -d /path-to-nls-places-data/`: run the data import using the given data path

The required data is currently available at: http://kartat.kapsi.fi/files/nimisto/paikat/etrs89/gml/paikat_2015_05.zip
