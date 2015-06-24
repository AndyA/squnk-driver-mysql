"use strict";

var path = require("path");
var fs = require("fs");
var _ = require("underscore");

module.exports = {
  createDelta: function(dir, context) {

    var templateDir = path.join(__dirname, "..", "templates");

    var spec = {
      index: "index.js",
      deploy: "deploy.sql",
      verify: "verify.sql",
      rollback: "rollback.sql"
    };

    _.each(spec, function(dst, src) {
      var srcFile = path.join(templateDir, src + ".template");
      var dstFile = path.join(dir, dst);
      var srcData = fs.readFileSync(srcFile);
      var dstData = _.template(srcData.toString());
      fs.writeFileSync(dstFile, dstData(context));
    });

  }
};
