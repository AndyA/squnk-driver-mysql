"use strict";

var chai = require("chai");
chai.use(require("chai-subset"));
var expect = chai.expect;

var Driver = require("../lib/driver/");

describe("squnk-driver-mysql", function() {

  describe("parseConnectionURI", function() {

    var driver = new Driver();

    it("should reject a non-mysql URI", function() {
      expect(function() {
          driver.parseConnectionURI("mssql://");
        })
        .to.throw(Error);
    });

    it("should reject a complex path", function() {
      expect(function() {
          driver.parseConnectionURI("mysql:///foo/bar");
        })
        .to.throw(Error);
    });

    it("should parse a minimal URI", function() {
      expect(driver.parseConnectionURI("mysql://"))
        .to.deep.equal({
          host: "localhost"
        });
    });

    it("should parse a hostname", function() {
      expect(driver.parseConnectionURI("mysql://db"))
        .to.deep.equal({
          host: "db"
        });
    });

    it("should parse a port", function() {
      expect(driver.parseConnectionURI("mysql://db:3307"))
        .to.deep.equal({
          host: "db",
          port: "3307"
        });
    });

    it("should parse a username", function() {
      expect(driver.parseConnectionURI("mysql://root@db"))
        .to.deep.equal({
          host: "db",
          user: "root"
        });
    });

    it("should parse a username & password", function() {
      expect(driver.parseConnectionURI("mysql://root:s3kr1t@db"))
        .to.deep.equal({
          host: "db",
          user: "root",
          password: "s3kr1t"
        });
    });

    it("should parse a database name", function() {
      expect(driver.parseConnectionURI("mysql://db/testdb"))
        .to.deep.equal({
          host: "db",
          database: "testdb"
        });
    });

    it("should parse options", function() {
      expect(driver.parseConnectionURI(
          "mysql://db/testdb?timezone=UTC&connectTimeout=60000"))
        .to.deep.equal({
          host: "db",
          database: "testdb",
          timezone: "UTC",
          connectTimeout: "60000"
        });
    });

  });

  describe("parseScript", function() {

    var driver = new Driver();

    it("should ignore blank lines", function() {
      expect(driver.parseScript("\n\n\n"))
        .to.deep.equal([]);
    });

    it("should parse comments", function() {
      expect(driver.parseScript("-- Hello!\n-- Bye!"))
        .to.deep.equal([{
          kind: "comment",
          text: "Hello!"
        }, {
          kind: "comment",
          text: "Bye!"
        }]);
    });

    it("should parse a one line statement", function() {
      expect(driver.parseScript("TRUNCATE test;"))
        .to.deep.equal([{
          kind: "statement",
          text: "TRUNCATE test"
        }]);
    });

    it("should parse multiple statements on a line", function() {
      expect(driver.parseScript("TRUNCATE test; TRUNCATE users;"))
        .to.deep.equal([{
          kind: "statement",
          text: "TRUNCATE test"
        }, {
          kind: "statement",
          text: "TRUNCATE users"
        }]);
    });

    it("should parse a multi-line statement", function() {
      expect(driver.parseScript(
          "SELECT *\n  FROM `users`\n WHERE `id` < 1000;"))
        .to.deep.equal([{
          kind: "statement",
          text: "SELECT * FROM `users` WHERE `id` < 1000"
        }]);
    });

    it("should handle strings", function() {
      expect(driver.parseScript(
          "INSERT INTO `users` VALUES (1000, \"Jonny\\\" DROP TABLES;\");"
        ))
        .to.deep.equal([{
          kind: "statement",
          text: "INSERT INTO `users` VALUES (1000, \"Jonny\\\" DROP TABLES;\")"
        }]);
    });

    it("should handle comments in statements", function() {
      expect(driver.parseScript(
          "INSERT INTO `users`\n-- Is this OK?\nVALUES (1000, \"Jonny\\\" DROP TABLES;\");"
        ))
        .to.deep.equal([{
          kind: "comment",
          text: "Is this OK?"
        }, {
          kind: "statement",
          text: "INSERT INTO `users` VALUES (1000, \"Jonny\\\" DROP TABLES;\")"
        }]);
    });

  });
});
