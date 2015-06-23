"use strict";

var mysql = require("mysql");
var url = require("url");
var _ = require("underscore");
var Promise = require("bluebird");

function Driver(config) {
  this.config = _.extend({
    prefix: "_squnk_meta_"
  }, config);

  this.connection = null;
  this.tableInfoCache = {};
  this.metaTable = null;
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
        console.log("query: " + sql);
        //        console.log("result: " + JSON.stringify({
        //          rows: rows,
        //          fields: fields
        //        }, null, 2));
        resolve([rows, fields]);
      });
    })
    .bind(this);
};

Driver.prototype.runScript = function(script) {
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

Driver.prototype.format = function() {
  var conn = this.getConnection();
  var args = _.toArray(arguments);
  var sql = args.shift();
  var flatArgs = args.map(function(arg) {
    return _.flatten(arg, true);
  });
  flatArgs.unshift(sql);
  return conn.format.apply(conn, flatArgs);
};

Driver.prototype.getMetaTableName = function() {
  return this.config.prefix + "deltas";
};

Driver.prototype.makeMetaTable = function() {
  var table = this.getMetaTableName();

  var script = [
      "-- * Creating ??",
      "DROP TABLE IF EXISTS ??;", [
        "CREATE TABLE ?? (",
        "  `name` varchar(80) NOT NULL COMMENT 'The name of the delta',",
        "  `sequence` int(10) unsigned NOT NULL COMMENT 'The sequence number of the delta',",
        "  `state` varchar(20) NOT NULL DEFAULT 'pending' COMMENT 'The state of the delta',",
        "  `delta` mediumtext COMMENT 'JSONball of the scripts',",
        "  `meta` mediumtext COMMENT 'JSON metadata bundle',",
        "  PRIMARY KEY (`name`),",
        "  UNIQUE KEY `sequence` (`sequence`),",
        "  KEY `state` (`state`)",
        ");"
      ].join(" ")
    ].map(function(ln) {
      return this.format(ln, [table]);
    }, this)
    .join("\n");

  return this.runScript(script);

};

Driver.prototype.getTableInfo = function(table) {
  var cache = this.tableInfoCache;
  if (cache.hasOwnProperty(table)) {
    return cache[table];
  }
  cache[table] = this.query(this.format("DESCRIBE ??", [table]));
  return cache[table];
};

Driver.prototype.getTableColumns = function(table) {
  return this.getTableInfo(table)
    .spread(function(rows) {
      var pk = [];
      var cols = [];
      var all = [];
      rows.forEach(function(row) {
        all.push(row.Field);
        if (row.Key === "PRI") {
          pk.push(row.Field);
        } else {
          cols.push(row.Field);
        }
      });
      return {
        pk: pk,
        cols: cols,
        all: all
      };
    });
};

Driver.prototype.getMetaTable = function() {
  var table = this.getMetaTableName();

  if (this.metaTable === null) {
    this.metaTable = this.query(this.format("SHOW TABLES LIKE ?", [table]))
      .spread(function(rows) {
        if (rows.length === 0) {
          return this.makeMetaTable()
            .return(table);
        }
        return table;
      })
      .then(function(tableName) {
        return [tableName, this.getTableColumns(tableName)];
      });
  }

  return this.metaTable;
};

function placeholder(str, rep) {
  var ph = [];
  for (var i = 0; i < rep; i++) {
    ph.push(str);
  }
  return ph.join(", ");
}

function indexedValues(obj, idx) {
  var donor = _.clone(obj);
  var missing = [];
  var vals = idx.map(function(key) {
    if (!donor.hasOwnProperty(key)) {
      missing.push(key);
      return null;
    }
    var v = donor[key];
    delete donor[key];
    return v;
  }, this);
  var extra = _.keys(donor);
  if (missing.length || extra.length) {
    var and = [];
    if (missing.length) {
      and.push("is missing field(s): " + missing.join(", "));
    }
    if (extra.length) {
      and.push("has these unknown field(s): " + extra.join(", "));
    }
    throw new Error("Data" + and.join(" and "));
  }
  return vals;
}

var codec = (function() {
  var jsonCols = ["delta", "meta"];

  return {
    encode: function(delta) {
      var copy = _.clone(delta);
      jsonCols.forEach(function(col) {
        copy[col] = JSON.stringify(copy[col]);
      });
      return copy;
    },

    decode: function(delta) {
      var copy = _.clone(delta);
      jsonCols.forEach(function(col) {
        copy[col] = JSON.parse(copy[col]);
      });
      return copy;
    }
  };
})();

// Functions to manipulate deltas

Driver.prototype.saveDelta = function(delta) {
  return this.getMetaTable()
    .spread(function(table, columns) {
      var cols = columns.all;
      var bind = [table, cols, indexedValues(codec.encode(delta), cols)];
      var sql = this.format("REPLACE INTO ?? (" + placeholder("??", cols.length) +
        ") VALUES (" + placeholder("?", cols.length) + ")", bind);
      return this.query(sql);
    });
};

Driver.prototype.loadDelta = function(name) {
  return this.getMetaTable()
    .spread(function(table) {
      var bind = [table, "name", name];
      return this.query(this.format("SELECT * FROM ?? WHERE ?? = ?", bind))
        .spread(function(rows) {
          return codec.decode(rows[0]);
        });
    });
};

Driver.prototype.loadDeltas = function() {};

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
