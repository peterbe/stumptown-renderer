const fs = require("fs");
const path = require("path");

const chalk = require("chalk");
const express = require("express");
const send = require("send");
const proxy = require("express-http-proxy");
const cookieParser = require("cookie-parser");
const openEditor = require("open-editor");

const {
  buildDocumentFromURL,
  buildDocument,
  buildLiveSamplePageFromURL,
  renderContributorsTxt,
} = require("../build");
const { CONTENT_ROOT, Document, Redirect, Image } = require("../content");
// eslint-disable-next-line node/no-missing-require
const { prepareDoc, renderDocHTML } = require("../ssr/dist/main");

const { STATIC_ROOT, PROXY_HOSTNAME, FAKE_V1_API } = require("./constants");
const documentRouter = require("./document");
const fakeV1APIRouter = require("./fake-v1-api");
const { searchRoute } = require("./document-watch");
const flawsRoute = require("./flaws");
const { staticMiddlewares, originRequestMiddleware } = require("./middlewares");

const app = express();

app.use(express.json());

// Needed because we read cookies in the code that mimics what we do in Lambda@Edge.
app.use(cookieParser());

app.use(originRequestMiddleware);

app.use(staticMiddlewares);

app.use(express.urlencoded({ extended: true }));

app.use(
  "/api/v1",
  // Depending on if FAKE_V1_API is set, we either respond with JSON based
  // on `.json` files on disk or we proxy the requests to Kuma.
  FAKE_V1_API
    ? fakeV1APIRouter
    : proxy(PROXY_HOSTNAME, {
        // More options are available on
        // https://www.npmjs.com/package/express-http-proxy#options
        proxyReqPathResolver: (req) => "/api/v1" + req.url,
      })
);

app.use("/_document", documentRouter);

app.get("/_open", (req, res) => {
  const { line, column, filepath } = req.query;
  if (!filepath) {
    throw new Error("No .filepath in the request query");
  }

  // Sometimes that 'filepath' query string parameter is a full absolute
  // filepath (e.g. /Users/peterbe/yari/content.../index.html), which usually
  // happens when you this is used from the displayed flaws on a preview
  // page.
  // But sometimes, it's a relative path and if so, it's always relative
  // to the main builder source.
  let absoluteFilepath;
  if (fs.existsSync(filepath)) {
    absoluteFilepath = filepath;
  } else {
    absoluteFilepath = path.join(CONTENT_ROOT, filepath);
  }

  // Double-check that the file can be found.
  if (!fs.existsSync(absoluteFilepath)) {
    return res.status(400).send(`${absoluteFilepath} does not exist on disk.`);
  }

  let spec = absoluteFilepath;
  if (line) {
    spec += `:${parseInt(line)}`;
    if (column) {
      spec += `:${parseInt(column)}`;
    }
  }
  openEditor([spec]);
  res.status(200).send(`Tried to open ${spec} in ${process.env.EDITOR}`);
});

app.use("/:locale/search-index.json", searchRoute);

app.get("/_flaws", flawsRoute);

app.get("/*/contributors.txt", async (req, res) => {
  const url = req.url.replace(/\/contributors\.txt$/, "");
  const document = Document.findByURL(url);
  res.setHeader("content-type", "text/plain");
  if (!document) {
    return res.status(404).send(`Document not found by URL (${url})`);
  }
  const [builtDocument] = await buildDocument(document);
  if (document.metadata.contributors || !document.isArchive) {
    res.send(
      renderContributorsTxt(
        document.metadata.contributors,
        !document.isArchive
          ? builtDocument.source.github_url.replace("/blob/", "/commits/")
          : null
      )
    );
  } else {
    res.status(410).send("Contributors not known for this document.\n");
  }
});

app.get("/*", async (req, res) => {
  if (req.url.startsWith("_")) {
    // URLs starting with _ is exclusively for the meta-work and if there
    // isn't already a handler, it's something wrong.
    return res.status(404).send("Page not found");
  }

  // If the catch-all gets one of these something's gone wrong
  if (req.url.startsWith("/static")) {
    return res.status(404).send("Page not found");
  }

  if (req.url.includes("/_samples_/")) {
    try {
      return res.send(await buildLiveSamplePageFromURL(req.url));
    } catch (e) {
      return res.status(404).send(e.toString());
    }
  }

  if (!req.url.includes("/docs/")) {
    // This should really only be expected for "single page apps".
    // All *documents* should be handled by the
    // `if (req.url.includes("/docs/"))` test above.
    res.sendFile(path.join(STATIC_ROOT, "/index.html"));
    return;
  }

  // TODO: Would be nice to have a list of all supported file extensions
  // in a constants file.
  if (/\.(png|webp|gif|jpeg|svg)$/.test(req.url)) {
    // Remember, Image.findByURL() will return the absolute file path
    // iff it exists on disk.
    const filePath = Image.findByURL(req.url);
    if (filePath) {
      // The second parameter to `send()` has to be either a full absolute
      // path or a path that doesn't start with `../` otherwise you'd
      // get a 403 Forbidden.
      // See https://github.com/mdn/yari/issues/1297
      return send(req, path.resolve(filePath)).pipe(res);
    } else {
      return res.status(404).send("File not found on disk");
    }
  }

  let lookupURL = decodeURI(req.url);
  let extraSuffix = "";
  let bcdDataURL = "";
  const bcdDataURLRegex = /\/(bcd-\d+|bcd)\.json$/;

  if (req.url.endsWith("index.json")) {
    // It's a bit special then.
    // The URL like me something like
    // /en-US/docs/HTML/Global_attributes/index.json
    // and that won't be found in getRedirectUrl() since that doesn't
    // index things with the '/index.json' suffix. So we need to
    // temporarily remove it and remember to but it back when we're done.
    extraSuffix = "/index.json";
    lookupURL = lookupURL.replace(extraSuffix, "");
  } else if (bcdDataURLRegex.test(req.url)) {
    bcdDataURL = req.url;
    lookupURL = lookupURL.replace(bcdDataURLRegex, "");
  }

  const isJSONRequest = extraSuffix.endsWith(".json");

  let document;
  let bcdData;
  try {
    console.time(`buildDocumentFromURL(${lookupURL})`);
    const built = await buildDocumentFromURL(lookupURL, {
      // The only times the server builds on the fly is basically when
      // you're in "development mode". And when you're not building
      // to ship you don't want the cache to stand have any hits
      // since it might prevent reading fresh data from disk.
      clearKumascriptRenderCache: true,
    });
    console.timeEnd(`buildDocumentFromURL(${lookupURL})`);
    if (built) {
      document = built.doc;
      bcdData = built.bcdData;
    }
  } catch (error) {
    console.error(`Error in buildDocumentFromURL(${lookupURL})`, error);
    return res.status(500).send(error.toString());
  }

  if (!document) {
    const redirectURL = Redirect.resolve(lookupURL);
    if (redirectURL !== lookupURL) {
      return res.redirect(301, redirectURL + extraSuffix);
    }
    return res
      .status(404)
      .sendFile(path.join(STATIC_ROOT, "en-us", "_spas", "404.html"));
  }

  if (bcdDataURL) {
    return res.json(
      bcdData.find((data) => data.url.toLowerCase() === bcdDataURL).data
    );
  }

  prepareDoc(document);
  if (isJSONRequest) {
    res.json({ doc: document });
  } else {
    res.send(renderDocHTML(document, lookupURL));
  }
});

if (!fs.existsSync(path.resolve(CONTENT_ROOT))) {
  throw new Error(`${path.resolve(CONTENT_ROOT)} does not exist!`);
}

console.log(
  `CONTENT_ROOT: ${chalk.bold(CONTENT_ROOT)}`,
  path.resolve(CONTENT_ROOT) !== CONTENT_ROOT
    ? chalk.grey(`(absolute path: ${path.resolve(CONTENT_ROOT)})`)
    : ""
);

const PORT = parseInt(process.env.SERVER_PORT || "5000");
app.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
  if (process.env.EDITOR) {
    console.log(`Your EDITOR is set to: ${chalk.bold(process.env.EDITOR)}`);
  } else {
    console.warn(
      chalk.yellow(
        "Warning! You have not set an EDITOR environment variable. " +
          'Using the "Open in your editor" button will probably fail.'
      )
    );
  }
});
