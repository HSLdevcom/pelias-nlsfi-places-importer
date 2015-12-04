var fs = require('fs'),
  XmlStream = require('xml-stream')

function getEnumerations(stream, callback) {

  var enumerations = {};

  var xml = new XmlStream(stream);
  xml.collect('xsd:documentation');

  xml.on('endElement: xsd:enumeration', function(item) {

    var code = item['$']['value'];
    var names = item['xsd:annotation']['xsd:documentation'].map(function(documentation) {
      var lang = documentation["$"]["xml:lang"]
      var text = documentation["$text"]
      return {
        lang: lang,
        text: text
      };
    });
    enumerations[code] = names;
  });

  xml.on('end', function() {
    callback(enumerations);
  });
}

module.exports = getEnumerations
