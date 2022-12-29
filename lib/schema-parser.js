var XmlStream = require('xml-stream');

function getEnumerations(stream, callback) {

  var enumerations = {};

  var xml = new XmlStream(stream);
  xml.collect('xs:documentation');

  xml.on('endElement: xs:enumeration', function(item) {
    var code = item['$']['value'];
    var names = item['xs:annotation']['xs:documentation'].map(function(documentation) {
      var lang = documentation['$']['xml:lang'];
      var text = documentation['$text'];
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

module.exports = getEnumerations;
