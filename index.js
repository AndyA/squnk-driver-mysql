"use strict";

var mysql = require("mysql");
var url = require("url");
var _ = require("underscore");
var Promise = require("bluebird");

function onError(thing, cb) {
  return function(err, rows, fields) {
    if (err) {
      var msg = thing + " failed: " + err.code + "(" + err.errno + ")";
      throw new Error(msg);
    }
    if (cb) {
      cb(rows, fields);
    }
  };
}

function Driver(config) {
  this.config = _.extend({
    prefix: "_squnk_meta_"
  }, config);

  this.connection = null;
}

Driver.prototype.parseConnectionURI = function(uri) {

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
};

// This probably moves up to our super
Driver.prototype.parseScript = function(script) {
  var out = [];

  var matchBlank = /^\s*$/;
  var matchComment = /^\s*--\s*(.*)/;
  var matchStatement = /^\s*(;|(?:(?:"(?:\\.|.)*")|(?:"(?:\\.|.)*")|[^;])+)/g;

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
};

Driver.prototype.connect = function(uri) {

  this.disconnect(); // one at a time

  var options = this.parseConnectionURI(uri);
  this.connection = mysql.createConnection(options);
  var self = this;
  return new Promise(function(resolve, reject) {
    self.connection.connect(function(err) {
      if (err) {
        reject(err);
      }
      resolve(self.connection);
    });
  });
};

Driver.prototype.disconnect = function() {
  if (this.connection !== null) {
    this.connection.end();
    this.connection = null;
  }
};

Driver.prototype.getConnection = function() {
  if (this.connection === null) {
    throw new Error("Not connected");
  }
  return this.connection;
};

Driver.prototype.query = function(sql) {
  var conn = this.getConnection();
  return new Promise(function(resolve, reject) {
      conn.query(sql, function(err, rows, fields) {
        if (err) {
          reject(err);
        }
        console.log("result: " + JSON.stringify({
          rows: rows,
          fields: fields
        }, null, 2));
        resolve([rows, fields]);
      });
    })
    .bind(this);
};

Driver.prototype.runScript = function(script) {
  var conn = this.getConnection();
  var p = Promise.bind(this);

  this.parseScript(script)
    .forEach(function(itm) {
      switch (itm.kind) {
        case "comment":
          if (itm.text.charAt(0) === "*") {
            p = p.then(function() {
              console.log(itm.text);
            });
          }
          break;
        case "statement":
          p = p.then(function() {
            return this.query(itm.text);
          });
          break;
        default:
          throw new Error("Unpexeced " + itm.kind + " in script");
      }
    }, this);

  return p;
};

// Create our tables (private)

Driver.prototype.makeMetaTable = function(table) {
  var conn = this.getConnection();
  var create = conn.format("CREATE TABLE ?? (" +
    "  name VARCHAR(80) NOT NULL" + ")", [table])
  return this.query(create)
    .spread(function(rows, fields) {
      console.log("result: " + JSON.stringify({
        rows: rows,
        fields: fields
      }, null, 2));
      return table;
    });
};

Driver.prototype.getMetaTable = function() {
  var conn = this.getConnection();
  var table = this.config.prefix + "deltas";

  return this.query(conn.format("SHOW TABLES LIKE ?", [table]))
    .spread(function(rows, fields) {
      console.log("result: " + JSON.stringify({
        rows: rows,
        fields: fields
      }, null, 2));
      if (rows.length === 0) {
        return this.makeMetaTable(table);
      }
      return table;
    });
};

// Functions to manipulate deltas

Driver.prototype.saveDelta = function(name, delta) {};
Driver.prototype.loadDelta = function(name) {};
Driver.prototype.loadDeltas = function(name) {};

Driver.prototype.setDeltaState = function(name, state) {
  var delta = this.loadDelta(name);
  if (delta === null) {
    throw new Error("Unknown delta: " + name);
  }
  if (delta.state !== state) {
    delta.state = state;
    this.saveDelta(name, delta);
  }
};

Driver.prototype.getDeltaState = function(name) {
  return this.loadDelta(name)
    .state;
};

module.exports = Driver;
