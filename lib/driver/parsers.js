"use strict";

var url = require("url");

module.exports = {
  parseConnectionURI: function(uri) {

    var spec = url.parse(uri, true);
    if (spec.protocol !== "mysql:") {
      throw new Error(uri + " is not a mysql:// URI");
    }
    var conn = spec.query;
    conn.host = spec.hostname.length ? spec.hostname : "localhost";

    if (spec.pathname !== null && spec.pathname.length > 1) {
      var dbPath = spec.pathname.substr(1)
        .split("/");
      if (dbPath.length > 1) {
        throw new Error(uri +
          "has more than one component in the database path");
      }
      conn.database = dbPath[0];
    }

    if (spec.port !== null) {
      conn.port = spec.port;
    }

    if (spec.auth) {
      var auth = spec.auth.split(":");
      conn.user = auth[0];
      if (auth.length > 1) {
        conn.password = auth[1];
      }
    }

    return conn;
  },

  // This probably moves up to our super
  parseScript: function(script) {
    var out = [];

    var matchBlank = /^\s*$/;
    var matchComment = /^\s*--\s*(.*)/;
    var matchStatement =
      /^\s*(;|(?:(?:"(?:\\.|.)*")|(?:"(?:\\.|.)*")|[^;])+)/g;

    var statement = [];

    script.split("\n")
      .forEach(function(ln) {
        if (matchBlank.test(ln)) {
          return;
        }

        var cm = matchComment.exec(ln);
        if (cm !== null) {
          out.push({
            kind: "comment",
            text: cm[1]
          });
          return;
        }

        matchStatement.lastIndex = 0;
        var sm = matchStatement.exec(ln);
        if (sm !== null) {
          while (sm !== null) {
            if (sm[1] === ";") {
              out.push({
                kind: "statement",
                text: statement.join(" ")
              });
              statement = [];
            } else {
              statement.push(sm[1]);
            }
            // No \G in JS RegExp
            ln = ln.substr(matchStatement.lastIndex);
            matchStatement.lastIndex = 0;
            sm = matchStatement.exec(ln);
          }
          return;
        }
        throw new Error("Syntax error: " + ln);
      });

    if (statement.length) {
      throw new Error("Missing semicolon at end of script");
    }

    return out;
  }
};
