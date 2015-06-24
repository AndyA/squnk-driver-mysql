"use strict";

var _ = require("underscore");

// State manipulation

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

module.exports = {

  saveDelta: function(delta) {
    return this.getMetaTable()
      .spread(function(table, columns) {
        var cols = columns.all;
        var bind = [table, cols, indexedValues(codec.encode(delta), cols)];
        var sql = this.format("REPLACE INTO ?? (" + placeholder("??",
            cols.length) +
          ") VALUES (" + placeholder("?", cols.length) + ")", bind);
        return this.query(sql);
      });
  },

  loadDelta: function(sequence) {
    return this.getMetaTable()
      .spread(function(table) {
        var bind = [table, "sequence", sequence];
        return this.query(this.format("SELECT * FROM ?? WHERE ?? = ?",
            bind))
          .spread(function(rows) {
            if (rows.length === 0) {
              return null;
            }
            return codec.decode(rows[0]);
          });
      });
  },

  loadDeltas: function() {
    return this.getMetaTable()
      .spread(function(table) {
        var bind = [table, "sequence"];
        return this.query(this.format("SELECT * FROM ?? ORDER BY ??",
            bind))
          .spread(function(rows) {
            return rows.map(codec.decode);
          });
      });
  },

  loadDeltaStates: function() {
    return this.getMetaTable()
      .spread(function(table) {
        var bind = ["name", "sequence", "state", table, "sequence"];
        var sql = this.format("SELECT ??, ??, ?? FROM ?? ORDER BY ??",
          bind);
        return this.query(sql)
          .spread(function(rows) {
            return rows;
          });
      });
  },

  setDeltaState: function(sequence, state) {
    return this.loadDelta(sequence)
      .then(function(delta) {
        if (delta === null) {
          throw new Error("Unknown delta: " + sequence);
        }
        if (delta.state !== state) {
          delta.state = state;
          return this.saveDelta(delta);
        }
      });
  }
};
