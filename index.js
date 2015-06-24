"use strict";

module.exports = function(engine) {
  engine.registerDriver("mysql", require("./lib/driver/"));
};
