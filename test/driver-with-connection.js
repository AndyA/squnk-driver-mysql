"use strict";

var chai = require("chai");
chai.use(require("chai-subset"));
chai.use(require("chai-as-promised"));
var expect = chai.expect;
var _ = require("underscore");

var Driver = require("../");

// Set SQUNK_DB to a db URI to run these tests

if (process.env.SQUNK_DB) {
  describe("squnk-driver-mysql", function() {

    var driver = new Driver({
      logger: null
    });

    before("connect driver", function() {
      return driver.connect(process.env.SQUNK_DB);
    });

    before("connect, clear database", function() {
      return driver.dropMetaTable();
    });

    after("disconnect driver", function() {
      driver.disconnect();
    });

    it("should create the meta table", function() {
      var metaTableName = driver.getMetaTableName();
      return expect(driver.getMetaTable()
          .spread(function(table, columns) {
            return [table, columns];
          }))
        .to.eventually.deep.equal([metaTableName, {
          "all": ["name", "sequence", "state", "delta", "meta"],
          "cols": ["sequence", "state", "delta", "meta"],
          "pk": ["name"]
        }]);
    });

    function makeDeltaWithState(sequence, state) {
      return {
        name: "test-" + sequence,
        sequence: sequence,
        state: state,
        delta: {},
        meta: {
          description: "Test delta " + sequence
        }
      };
    }

    function makeDelta(sequence) {
      return makeDeltaWithState(sequence,
        sequence < 3 ? "deployed" : "pending");
    }

    _.shuffle(_.range(5))
      .forEach(function(sequence) {
        it("should save delta " + sequence, function() {
          return expect(driver.saveDelta(makeDelta(sequence)))
            .to.eventually.be.not.null;
        });
      });

    _.shuffle(_.range(5))
      .forEach(function(sequence) {
        it("should load delta " + sequence, function() {
          return expect(driver.loadDelta(sequence))
            .to.eventually.deep.equal(makeDelta(sequence));
        });
      });

    it("should load all the deltas", function() {
      return expect(driver.loadDeltas())
        .to.eventually.deep.equal(_.range(5)
          .map(makeDelta));
    });

    it("should return null for a missing delta", function() {
      return expect(driver.loadDelta(100))
        .to.eventually.be.null;
    });

    it("should re-set an existing state", function() {
      return expect(driver.setDeltaState(2, "deployed"))
        .to.eventually.be.not.null;
    });

    it("should not have changed the state", function() {
      return expect(driver.loadDelta(2))
        .to.eventually.deep.equal(makeDelta(2));
    });

    it("should set a different state", function() {
      return expect(driver.setDeltaState(3, "deployed"))
        .to.eventually.be.not.null;
    });

    it("should have changed the state", function() {
      return expect(driver.loadDelta(3))
        .to.eventually.deep.equal(makeDeltaWithState(3, "deployed"));
    });

  });
}
