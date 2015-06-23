"use strict";

var eslint = require("gulp-eslint");
var gulp = require("gulp");
var mocha = require("gulp-mocha");

gulp.task("lint", function() {
  var src = [];
  src.push("gulpfile.js");
  src.push("index.js");
  src.push("bin/**/*.js");
  src.push("lib/**/*.js");
  src.push("test/**/*.js");
  return gulp.src(src).pipe(eslint()).pipe(eslint.format());
});

gulp.task("test", function() {
  return gulp.src(["test/**/*.js"]).pipe(mocha({
    reporter: "dot"
  }));
});

gulp.task("default", ["lint", "test"], function() {});
