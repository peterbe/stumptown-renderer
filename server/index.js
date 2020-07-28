const fs = require("fs");
const path = require("path");

const express = require("express");
const openEditor = require("open-editor");

const { buildDocumentFromURL, buildLiveSamplePageFromURL } = require("build");
const { CONTENT_ROOT, Redirect } = require("content");
const { prepareDoc, renderHTML } = require("ssr");

const { STATIC_ROOT } = require("./constants");
const documentRouter = require("./document");
const { searchRoute } = require("./document-watch");
const flawsRoute = require("./flaws");
const { staticMiddlewares } = require("./middlewares");

const app = express();
app.use(express.json());

app.use(staticMiddlewares);

app.use(express.static(STATIC_ROOT));

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

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

// Return about redirects based on a list of URLs.
// This is used by the "<Flaws/>" component which displays information
// about broken links in a page, as some of those broken links might just
// be redirects.
app.post("/_redirects", (req, res) => {
  if (req.body === undefined) {
    throw new Error("express.json middleware not installed");
  }
  const redirects = {};
  if (!req.body.urls) {
    return res.status(400).send("No .urls array sent in JSON");
  }
  for (const url of req.body.urls) {
    redirects[url] = getRedirectURL(url);
  }
  res.json({ redirects });
});

app.use("/:locale/search-index.json", searchRoute);

app.get("/_flaws", flawsRoute);

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
    return res.send(await buildLiveSamplePageFromURL(req.url));
  }

  if (!req.url.includes("/docs/")) {
    // This should really only be expected for "single page apps".
    // All *documents* should be handled by the
    // `if (req.url.includes("/docs/"))` test above.
    res.sendFile(path.join(STATIC_ROOT, "/index.html"));
    return;
  }

  let lookupURL = req.url;
  let extraSuffix = "";

  if (req.url.endsWith("index.json")) {
    // It's a bit special then.
    // The URL like me something like
    // /en-US/docs/HTML/Global_attributes/index.json
    // and that won't be found in getRedirectUrl() since that doesn't
    // index things with the '/index.json' suffix. So we need to
    // temporarily remove it and remember to but it back when we're done.
    extraSuffix = "/index.json";
    lookupURL = lookupURL.replace(extraSuffix, "");
  }

  const isJSONRequest = extraSuffix.endsWith(".json");

  // TODO: Do something prettier here so you can see, on stdout, what
  // documents get built on-the-fly.
  console.time(`buildDocumentFromURL(${lookupURL})`);
  const document = await buildDocumentFromURL(lookupURL);
  console.timeEnd(`buildDocumentFromURL(${lookupURL})`);
  if (!document) {
    // redirect resolving can take some time, so we only do it when there's no document
    // for the current route
    const redirectURL = Redirect.resolve(lookupURL);
    if (redirectURL !== lookupURL) {
      return res.redirect(301, redirectURL + extraSuffix);
    }
    return res.sendStatus(404);
  }

  prepareDoc(document);
  if (isJSONRequest) {
    res.json({ doc: document });
  } else {
    res.send(renderHTML(document, lookupURL));
  }
});

const PORT = parseInt(process.env.SERVER_PORT || "5000");
app.listen(PORT, () => console.log(`Listening on port ${PORT}`));
