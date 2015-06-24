"use strict";

var _ = require("underscore");

function Driver(config) {
  this.config = _.extend({
    logger: console,
    prefix: "_squnk_meta_"
  }, config);

  this.connection = null;
  this.tableInfoCache = {};
  this.metaTable = null;
}

Driver.prototype.log = function() {
  var logger = this.config.logger;
  if (logger !== null) {
    logger.log.apply(logger, _.toArray(arguments));
  }
};

_.extend(Driver.prototype, require("./parsers.js"));
_.extend(Driver.prototype, require("./db.js"));
_.extend(Driver.prototype, require("./state.js"));

module.exports = Driver;
