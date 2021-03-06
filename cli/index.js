import React from "react";
import fs from "fs";
import url from "url";
import path from "path";

// const crypto = require("crypto");

// This is necessary because the cli.js is in dist/cli.js
// and we need to reach the .env this way.
require("dotenv").config({ path: path.join(__dirname, "../../.env") });

import yaml from "js-yaml";
import minimist from "minimist";
import buildOptions from "minimist-options";
import { StaticRouter as Router, matchPath } from "react-router";
import sourceMapSupport from "source-map-support";

import App from "../client/src/App";
import render from "./render";

const STATIC_ROOT = path.join(__dirname, "../../client/build");

const ROUTES = [
  { path: "", exact: true },
  { path: "/:locale", exact: true },
  { path: "/:locale/docs/:slug*" },
  { path: "/docs/:slug*" },
  { path: "/search", exact: true }
];

sourceMapSupport.install();

/* Return a absolute path that is the correct URI for the website */
function mapToURI({ document }) {
  return url.parse(document.mdn_url).pathname;
}

function buildHtmlAndJson({ filePath, output, buildHtml }) {
  const data = fs.readFileSync(filePath, "utf8");
  // const buildHash = crypto
  //   .createHash("md5")
  //   .update(data)
  //   .digest("hex");

  const jsonData = JSON.parse(data);

  const baseNameSans = path.basename(filePath).replace(/\.json/g, "");
  const options = {
    // XXX this is weird
    document: jsonData.html.elements[baseNameSans]
  };

  const uri = mapToURI({ filePath, document: options.document });

  const destination = path.join(output, uri);
  const outfileHtml = path.join(destination, "index.html");
  const outfileJson = path.join(destination, "index.json");
  // const outfileHash = path.join(destination, "index.hash");

  // let previousHash = "";
  // try {
  //   previousHash = fs.readFileSync(outfileHash, "utf8");
  // } catch (ex) {
  //   // That's fine
  // }
  // console.log("PREVIOUS HASH", [previousHash, buildHash]);

  let rendered = null;
  if (buildHtml) {
    const match = ROUTES.reduce((acc, route) => {
      return matchPath(uri, route) || acc;
    }, null);

    if (!match) {
      throw new Error(`Urecognized URL pattern ${uri}`);
    }
    try {
      rendered = render(
        <Router context={{}} location={uri}>
          <App {...options} />
        </Router>,
        options
      );
    } catch (ex) {
      console.error(`Rendering HTML failed!
      uri=${uri}
      filePath=${filePath}`);
      throw ex;
    }
  }

  fs.mkdirSync(destination, { recursive: true });
  if (rendered) {
    fs.writeFileSync(outfileHtml, rendered);
  }
  fs.writeFileSync(
    outfileJson,
    process.env.NODE_ENV === "development"
      ? JSON.stringify(options, null, 2)
      : JSON.stringify(options)
  );
  // fs.writeFileSync(outfileHash, buildHash);
  if (rendered) {
    console.log(`Wrote ${outfileHtml} and ${outfileJson}`);
  } else {
    console.log(`Wrote ${outfileJson}`);
  }
}

const options = buildOptions({
  help: {
    type: "boolean",
    alias: ["h"],
    default: false
  },

  output: {
    type: "string",
    alias: "o",
    default: STATIC_ROOT
  },

  version: {
    type: "boolean",
    alias: ["v"],
    default: false
  },

  debug: "boolean",

  "build-html": {
    type: "boolean",
    alias: ["b"],
    default: JSON.parse(process.env.CLI_BUILD_HTML || "false")
  },

  // Special option for positional arguments (`_` in minimist)
  arguments: "string"
});

const args = minimist(process.argv.slice(2), options);

if (args["help"]) {
  console.log(`
  Usage:
    yarn run run [options] FILES

  Options:
    -h, --help         print usage information
    -v, --version      show version info and exit
    -d, --debug        with more verbose output (currently not supported!)
    -o, --output       root directory to store built files (default ${STATIC_ROOT})
    -b, --build-html   also generate fully formed index.html files (or env var $CLI_BUILD_HTML)
  `);
  process.exit(0);
}

if (args["version"]) {
  console.log(require("./package.json").version);
  process.exit(0);
}

if (args["debug"]) {
  console.warn("--debug is not yet supported");
  process.exit(1);
}

const paths = args["_"];
if (!paths.length) {
  console.warn("Building for ALL files is currently not supported");
  process.exit(1);
}

paths.forEach(filePath => {
  const output = args.output;
  fs.access(filePath, fs.constants.R_OK, err => {
    if (err) {
      console.error(err.toString());
      process.exit(1);
    }
    buildHtmlAndJson({ filePath, output, buildHtml: args["build-html"] });
  });
});
