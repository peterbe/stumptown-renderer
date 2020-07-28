const express = require("express");

const { slugToFoldername } = require("content");
const { STATIC_ROOT } = require("./constants");

// Lowercase every request because every possible file we might have
// on disk is always in lowercase.
// This only helps when you're on a filesystem (e.g. Linux) that is case
// sensitive.
const slugRewrite = (req, res, next) => {
  req.url = req.url.toLowerCase();

  if (req.url.includes("/_samples_/")) {
    // We need to convert incoming live-sample URL's like:
    //   /en-us/docs/web/css/:indeterminate/_samples_/progress_bar
    // to:
    //   /en-us/docs/web/css/_colon_indeterminate/_samples_/progress_bar
    // since they should be served directly by the static middleware.
    req.url = slugToFoldername(req.url);
  }
  next();
};

module.exports = {
  staticMiddlewares: [slugRewrite, express.static(STATIC_ROOT)],
};
