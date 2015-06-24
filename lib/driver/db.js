"use strict";

var mysql = require("mysql");
var _ = require("underscore");
var Promise = require("bluebird");

module.exports = {

  connect: function(uri) {

    if (this.connection !== null) {
      throw new Error("Already connected");
    }

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
  },

  disconnect: function() {
    if (this.connection !== null) {
      this.connection.end();
      this.connection = null;
    }
  },

  getConnection: function() {
    if (this.connection === null) {
      throw new Error("Not connected");
    }
    return this.connection;
  },

  query: function(sql) {
    var conn = this.getConnection();
    return new Promise(function(resolve, reject) {
        conn.query(sql, function(err, rows, fields) {
          if (err) {
            reject(err);
          }
          resolve([rows, fields]);
        });
      })
      .bind(this);
  },

  runScript: function(script) {
    var p = Promise.bind(this);

    this.parseScript(script)
      .forEach(function(itm) {
        switch (itm.kind) {
          case "comment":
            if (itm.text.charAt(0) === "*") {
              p = p.then(function() {
                this.log(itm.text);
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
  },

  // Create our tables (private)

  format: function() {
    var conn = this.getConnection();
    var args = _.toArray(arguments);
    var sql = args.shift();
    var flatArgs = args.map(function(arg) {
      return _.flatten(arg, true);
    });
    flatArgs.unshift(sql);
    return conn.format.apply(conn, flatArgs);
  },

  getMetaTableName: function() {
    return this.config.prefix + "deltas";
  },

  dropMetaTable: function() {
    var table = this.getMetaTableName();

    var script = [
        "-- * Dropping ??",
        "DROP TABLE IF EXISTS ??;"
      ].map(function(ln) {
        return this.format(ln, [table]);
      }, this)
      .join("\n");

    return this.runScript(script);
  },

  makeMetaTable: function() {
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

  },

  getTableInfo: function(table) {
    var cache = this.tableInfoCache;
    if (cache.hasOwnProperty(table)) {
      return cache[table];
    }
    cache[table] = this.query(this.format("DESCRIBE ??", [table]));
    return cache[table];
  },

  getTableColumns: function(table) {
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
  },

  getMetaTable: function() {
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
  }

};
