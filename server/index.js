const fs = require("fs");
const path = require("path");

const chalk = require("chalk");
const express = require("express");
const send = require("send");
const { createProxyMiddleware } = require("http-proxy-middleware");
const cookieParser = require("cookie-parser");
const openEditor = require("open-editor");

const {
  buildDocument,
  buildLiveSamplePageFromURL,
  renderContributorsTxt,
} = require("../build");
const { findDocumentTranslations } = require("../content/translations");
const {
  CONTENT_ROOT,
  Document,
  Redirect,
  Image,
  CONTENT_TRANSLATED_ROOT,
} = require("../content");
// eslint-disable-next-line node/no-missing-require
const {
  prepareDoc,
  renderDocHTML,
  renderPDFStream,
} = require("../ssr/dist/main");
const { CSP_VALUE_DEV, DEFAULT_LOCALE } = require("../libs/constants");

const { STATIC_ROOT, PROXY_HOSTNAME, FAKE_V1_API } = require("./constants");
const documentRouter = require("./document");
const fakeV1APIRouter = require("./fake-v1-api");
const { searchIndexRoute } = require("./search-index");
const flawsRoute = require("./flaws");
const { translationsRoute } = require("./translations");
const { staticMiddlewares, originRequestMiddleware } = require("./middlewares");
const { getRoot } = require("../content/utils");

async function buildDocumentFromURL(url) {
  const document = Document.findByURL(url);
  if (!document) {
    return null;
  }
  const documentOptions = {
    // The only times the server builds on the fly is basically when
    // you're in "development mode". And when you're not building
    // to ship you don't want the cache to stand have any hits
    // since it might prevent reading fresh data from disk.
    clearKumascriptRenderCache: true,
  };
  if (CONTENT_TRANSLATED_ROOT) {
    // When you're running the dev server and build documents
    // every time a URL is requested, you won't have had the chance to do
    // the phase that happens when you do a regular `yarn build`.
    document.translations = findDocumentTranslations(document);
  }
  return await buildDocument(document, documentOptions);
}

const app = express();

app.use(express.json());

// Needed because we read cookies in the code that mimics what we do in Lambda@Edge.
app.use(cookieParser());

app.use(originRequestMiddleware);

app.use(staticMiddlewares);

// Depending on if FAKE_V1_API is set, we either respond with JSON based
// on `.json` files on disk or we proxy the requests to Kuma.
const proxy = FAKE_V1_API
  ? fakeV1APIRouter
  : createProxyMiddleware({
      target: `${
        ["developer.mozilla.org", "developer.allizom.org"].includes(
          PROXY_HOSTNAME
        )
          ? "https://"
          : "http://"
      }${PROXY_HOSTNAME}`,
      changeOrigin: true,
      // proxyTimeout: 20000,
      // timeout: 20000,
    });

app.use("/api/v1", proxy);
// This is an exception and it's only ever relevant in development.
app.post("/:locale/users/account/signup", proxy);

// It's important that this line comes *after* the setting up for the proxy
// middleware for `/api/v1` above.
// See https://github.com/chimurai/http-proxy-middleware/issues/40#issuecomment-163398924
app.use(express.urlencoded({ extended: true }));

app.post(
  "/csp-violation-capture",
  express.json({ type: "application/csp-report" }),
  (req, res) => {
    const report = req.body["csp-report"];
    console.warn(
      chalk.yellow(
        "CSP violation for directive",
        report["violated-directive"],
        "which blocked:",
        report["blocked-uri"]
      )
    );
    res.sendStatus(200);
  }
);

app.use("/_document", documentRouter);

app.get("/_open", (req, res) => {
  const { line, column, filepath, url } = req.query;
  // Sometimes that 'filepath' query string parameter is a full absolute
  // filepath (e.g. /Users/peterbe/yari/content.../index.html), which usually
  // happens when you this is used from the displayed flaws on a preview
  // page.
  // But sometimes, it's a relative path and if so, it's always relative
  // to the main builder source.
  let absoluteFilepath;
  if (filepath) {
    if (fs.existsSync(filepath)) {
      absoluteFilepath = filepath;
    } else {
      const [locale] = filepath.split(path.sep);
      const root = getRoot(locale);
      absoluteFilepath = path.join(root, filepath);
    }
  } else if (url) {
    const document = Document.findByURL(url);
    if (!document) {
      res.status(410).send(`No known document by the URL '${url}'\n`);
    }
    absoluteFilepath = document.fileInfo.path;
  } else {
    throw new Error("No .filepath or .url in the request query");
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

app.use("/:locale/search-index.json", searchIndexRoute);

app.get("/_flaws", flawsRoute);

app.get("/_translations", translationsRoute);

app.get("/*/contributors.txt", async (req, res) => {
  const url = req.path.replace(/\/contributors\.txt$/, "");
  const document = Document.findByURL(url);
  res.setHeader("content-type", "text/plain");
  if (!document) {
    return res.status(404).send(`Document not found by URL (${url})`);
  }
  const { doc: builtDocument } = await buildDocument(document);
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
  if (req.url.startsWith("/_")) {
    // URLs starting with _ is exclusively for the meta-work and if there
    // isn't already a handler, it's something wrong.
    return res.status(404).send("Page not found");
  }

  // If the catch-all gets one of these something's gone wrong
  if (req.url.startsWith("/static")) {
    return res.status(404).send("Page not found");
  }

  if (req.url.includes("/_sample_.")) {
    try {
      return res.send(await buildLiveSamplePageFromURL(req.path));
    } catch (e) {
      return res.status(404).send(e.toString());
    }
  }

  if (!req.url.includes("/docs/")) {
    // If it's a known SPA, like `/en-US/search` then that should have been
    // matched to its file and not end up here in the catchall handler.
    // Simulate what we do in the Lambda@Edge.
    return res
      .status(404)
      .sendFile(path.join(STATIC_ROOT, "en-us", "_spas", "404.html"));
  }

  // TODO: Would be nice to have a list of all supported file extensions
  // in a constants file.
  if (/\.(png|webp|gif|jpe?g|svg)$/.test(req.path)) {
    // Remember, Image.findByURL() will return the absolute file path
    // iff it exists on disk.
    const filePath = Image.findByURL(req.path);
    if (filePath) {
      // The second parameter to `send()` has to be either a full absolute
      // path or a path that doesn't start with `../` otherwise you'd
      // get a 403 Forbidden.
      // See https://github.com/mdn/yari/issues/1297
      return send(req, path.resolve(filePath)).pipe(res);
    }
    return res.status(404).send("File not found on disk");
  }

  let lookupURL = decodeURI(req.path);
  let extraSuffix = "";
  let bcdDataURL = "";
  const bcdDataURLRegex = /\/(bcd-\d+|bcd)\.json$/;

  if (req.path.endsWith("index.json")) {
    // It's a bit special then.
    // The URL like me something like
    // /en-US/docs/HTML/Global_attributes/index.json
    // and that won't be found in getRedirectUrl() since that doesn't
    // index things with the '/index.json' suffix. So we need to
    // temporarily remove it and remember to but it back when we're done.
    extraSuffix = "/index.json";
    lookupURL = lookupURL.replace(extraSuffix, "");
  } else if (bcdDataURLRegex.test(req.path)) {
    bcdDataURL = req.path;
    lookupURL = lookupURL.replace(bcdDataURLRegex, "");
  } else if (req.path.endsWith(".pdf")) {
    extraSuffix = ".pdf";
    lookupURL = lookupURL.replace(/\.pdf$/, "");
  }

  const isJSONRequest = extraSuffix.endsWith(".json");
  const isPDFRequest = extraSuffix.endsWith(".pdf");

  let document;
  let bcdData;
  try {
    console.time(`buildDocumentFromURL(${lookupURL})`);
    const built = await buildDocumentFromURL(lookupURL);
    if (built) {
      document = built.doc;
      bcdData = built.bcdData;
    } else if (
      lookupURL.split("/")[1] &&
      lookupURL.split("/")[1].toLowerCase() !== DEFAULT_LOCALE.toLowerCase() &&
      !CONTENT_TRANSLATED_ROOT
    ) {
      // Such a common mistake. You try to view a URL that is not en-US but
      // you forgot to set CONTENT_TRANSLATED_ROOT.
      console.warn(
        `URL is for locale '${
          lookupURL.split("/")[1]
        }' but CONTENT_TRANSLATED_ROOT is not set. URL will 404.`
      );
    }
  } catch (error) {
    console.error(`Error in buildDocumentFromURL(${lookupURL})`, error);
    return res.status(500).send(error.toString());
  } finally {
    console.timeEnd(`buildDocumentFromURL(${lookupURL})`);
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
  } else if (isPDFRequest) {
    // const pdfStream = await ReactPDF.renderToStream(<MyDocument />);
    const pdfStream = await renderPDFStream(lookupURL, { doc: document });
    console.log({ pdfStream });
    res.setHeader("Content-Type", "application/pdf");
    pdfStream.pipe(res);
    // pdfStream.on("end", () => console.log("Done streaming, response sent."));
  } else {
    res.header("Content-Security-Policy", CSP_VALUE_DEV);
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
