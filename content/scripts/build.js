const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const childProcess = require("child_process");
const { performance } = require("perf_hooks");

const fm = require("front-matter");
const chalk = require("chalk");
const sanitizeFilename = require("sanitize-filename");
const chokidar = require("chokidar");
const WebSocket = require("ws");
// XXX does this work on Windows?
const packageJson = require("../../package.json");

require("dotenv").config();

const cheerio = require("./monkeypatched-cheerio");
const ProgressBar = require("./progress-bar");
const { packageBCD } = require("./resolve-bcd");
const { buildHtmlAndJsonFromDoc } = require("ssr");
const Document = require("./document");
const {
  extractDocumentSections,
  extractSidebar,
} = require("./document-extractor");
const {
  ALLOW_STALE_TITLES,
  VALID_LOCALES,
  DEFAULT_POPULARITIES_FILEPATH,
  FLAW_LEVELS,
  VALID_FLAW_CHECKS,
  DEFAULT_FLAW_LEVELS,
  DEFAULT_SITEMAP_BASE_URL,
  DEFAULT_LIVE_SAMPLES_BASE_URL,
  DEFAULT_INTERACTIVE_EXAMPLES_BASE_URL,
} = require("./constants");
const { slugToFoldername } = require("./utils");

const kumascript = require("kumascript");

const ALL_TITLES_JSON_FILEPATH = path.join(
  path.dirname(__dirname),
  "_all-titles.json"
);

function getCurretGitHubBaseURL() {
  return packageJson.repository;
}

// Module level global that gets set once and reused repeatedly
let _currentGitBranch = null;
function getCurrentGitBranch(fallback = "master") {
  if (!_currentGitBranch) {
    // XXX Fixme with what you'd get in the likes of TravisCI!
    if (process.env.CI_CURRENT_BRANCH) {
      _currentGitBranch = process.env.CI_CURRENT_BRANCH;
    } else {
      const spawned = childProcess.spawnSync("git", [
        "branch",
        "--show-current",
      ]);
      if (spawned.error || spawned.status) {
        console.warn(
          "\nUnable to run 'git branch' to find out name of the current branch:\n",
          spawned.error ? spawned.error : spawned.stderr.toString().trim()
        );
        // I don't think it makes sense to keep trying, so let's cache the fallback.
        _currentGitBranch = fallback;
      } else {
        _currentGitBranch = spawned.stdout.toString().trim();
      }
    }
  }
  return _currentGitBranch;
}

// XXX is this the best way??
function isTTY() {
  return !!process.stdout.columns;
}

// Turn a Map instance into a object.
// This is something you might need to do when serializing a Map
// with JSON.stringify().
function mapToObject(map) {
  const obj = Object.create(null);
  for (const [key, value] of map) {
    obj[key] = value;
  }
  return obj;
}
/** Given a array of locales, return it "cleaned up".
 * For example, they should always be lowercase and whitespace stripped.
 * and if they locale (case INsensitively) is not in VALID_LOCALES it
 * should throw an error.
 */
function cleanLocales(locales) {
  const clean = [];
  for (const locale of locales) {
    // The user *might* type locales as a comma separated strings.
    // Explode those split by ','.
    if (locale.includes(",")) {
      clean.push(...locale.split(",").map((l) => l.toLowerCase()));
    } else {
      // As a convenience, we know that every locale folder is always lowercase,
      // but it's very possible that someone specifies it in NOT lowercase.
      // E.g '--locales en-US'. So just lowercase them all.
      clean.push(locale.toLowerCase());
    }
  }
  return clean.filter((x) => {
    if (x) {
      if (!VALID_LOCALES.has(x)) {
        throw new Error(`'${x}' is not a valid locale (see VALID_LOCALES)`);
      }
    }
    return x;
  });
}

/**
 * Return a map of {flaw identifier => level} from a comma separated string.
 *
 * The input is a comma separated string that looks like this:
 *   'broken_links:ignore, macros:error, *:warn'
 *
 * Every flaw identifier and every level is checked against the known
 * values and throws an error on anything unrecognized.
 */
function checkFlawLevels(flawChecks) {
  const checks = flawChecks
    .split(",")
    .map((x) => x.trim())
    .filter((x) => x)
    .map((x) => x.split(":").map((s) => s.trim()));

  // Check that it doesn't contain more than 1 wildcard,
  // because that'd make no sense.
  const wildcards = checks.filter((tuple) => tuple[0] === "*");
  if (wildcards.length > 1) {
    throw new Error(`Can only be 1 wild card (not: ${wildcards})`);
  }

  // Put any wildcards (e.g. '*:warn') first
  checks.sort((a, b) => {
    if (a[0] === "*" && b[0] !== "*") {
      return -1;
    } else if (a[0] !== "*" && b[0] === "*") {
      return 1;
    }
    return 0;
  });

  const checked = new Map();

  // Unless specified, set 'ignore' on all of them first.
  for (const check of VALID_FLAW_CHECKS) {
    checked.set(check, FLAW_LEVELS.IGNORE);
  }

  const levelValues = Object.values(FLAW_LEVELS);

  for (const tuple of checks) {
    if (tuple.length !== 2) {
      throw new Error(`Not a tuple pair of 2 (${tuple})`);
    }
    const [identifier, level] = tuple;
    if (!levelValues.includes(level)) {
      throw new Error(`Invalid level: '${level}' (only ${levelValues})`);
    }
    if (identifier === "*") {
      for (const check of VALID_FLAW_CHECKS) {
        checked.set(check, level);
      }
    } else if (!VALID_FLAW_CHECKS.has(identifier)) {
      throw new Error(
        `Unrecognized flaw identifier: '${identifier}' (only ${[
          ...VALID_FLAW_CHECKS,
        ]})`
      );
    } else {
      checked.set(identifier, level);
    }
  }

  return checked;
}

/** Needs doc string */
function triggerTouch(filepath, document, root) {
  const changedFile = {
    path: filepath,
    name: path.relative(root, filepath),
  };
  const data = {
    documentURL: document.mdn_url,
    changedFile,
    hasEDITOR: Boolean(process.env.EDITOR),
  };
  broadcastWebsocketMessage(JSON.stringify(data));
}

/** Needs doc string */
function buildMDNUrl(locale, slug) {
  if (!locale) throw new Error("locale falsy!");
  if (!slug) throw new Error("slug falsy!");
  return `/${locale}/docs/${slug}`;
}

/** Throw an error if the slug is insane.
 * This helps breaking the build if someone has put in faulty data into
 * the content (metadata file).
 * If all is well, do nothing. Nothing is expected to return.
 */
function validateSlug(slug) {
  if (!slug) {
    throw new Error("slug is empty");
  }
  if (slug.startsWith("/")) {
    throw new Error(`Slug '${slug}' starts with a /`);
  }
  if (slug.endsWith("/")) {
    throw new Error(`Slug '${slug}' ends with a /`);
  }
  if (slug.includes("//")) {
    throw new Error(`Slug '${slug}' contains a double /`);
  }
}

function repairUri(uri) {
  // Returns a lowercase URI with common irregularities repaired.
  uri = uri.trim().toLowerCase();
  if (!uri.startsWith("/")) {
    // Ensure the URI starts with a "/".
    uri = "/" + uri;
  }
  // Remove redundant forward slashes, like "//".
  uri = uri.replace(/\/{2,}/g, "/");
  // Ensure the URI starts with a valid locale.
  const maybeLocale = uri.split("/")[1];
  if (!VALID_LOCALES.has(maybeLocale)) {
    if (maybeLocale === "en") {
      // Converts URI's like "/en/..." to "/en-us/...".
      uri = uri.replace(`/${maybeLocale}`, "/en-us");
    } else {
      // Converts URI's like "/web/..." to "/en-us/web/...", or
      // URI's like "/docs/..." to "/en-us/docs/...".
      uri = "/en-us" + uri;
    }
  }
  // Ensure the locale is followed by "/docs".
  const [locale, maybeDocs] = uri.split("/").slice(1, 3);
  if (maybeDocs !== "docs") {
    // Converts URI's like "/en-us/web/..." to "/en-us/docs/web/...".
    uri = uri.replace(`/${locale}`, `/${locale}/docs`);
  }
  return uri;
}

let webSocketServer = null;
function broadcastWebsocketMessage(msg) {
  if (!webSocketServer) {
    console.warn("No WebSocket server started");
    return;
  }
  // let i = 0;
  webSocketServer.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      // i++;
      // console.log(`SENDING (${i})...`, msg);
      client.send(msg);
    }
  });
  // console.log(`Sent to ${i} open clients`);
}

async function runBuild(sources, options, logger) {
  try {
    const builder = new Builder(sources, options, logger);

    if (options.listLocales) {
      return builder.listLocales();
    } else if (options.ensureTitles) {
      builder.initSelfHash();
      return builder.ensureAllTitles();
    } else {
      builder.initSelfHash();
      builder.ensureAllTitles();
      builder.ensureAllRedirects();
      builder.prepareRoots();
      if (!options.watch) {
        return builder.start();
      }
      if (options.watch || options.buildAndWatch) {
        builder.watch();
        console.log("Starting WebSocket (port 8080) to report on builds.");
        webSocketServer = new WebSocket.Server({ port: 8080 });
      }
    }
  } catch (err) {
    console.error(err);
    throw err;
  }
}

// Enums for return values
const processing = Object.freeze({
  ALREADY: "already",
  PROCESSED: "processed",
  EMPTY: "empty",
  EXCLUDED: "excluded",
});

class Builder {
  constructor(sources, options, logger) {
    this.sources = sources;
    this.destination = options.destination;
    this.options = options;
    this.logger = logger;
    this.selfHash = null;
    this.allTitles = new Map();
    this.allRedirects = new Map();
    this.flawsByType = new Map();

    this.options.locales = cleanLocales(this.options.locales || []);
    this.options.notLocales = cleanLocales(this.options.notLocales || []);
    this.options.popularitiesfile =
      this.options.popularitiesfile || DEFAULT_POPULARITIES_FILEPATH;

    this.options.flawLevels = checkFlawLevels(
      this.options.flawLevels || DEFAULT_FLAW_LEVELS
    );
    this.options.allowStaleTitles =
      this.options.allowStaleTitles || ALLOW_STALE_TITLES;
    this.options.sitemapBaseUrl =
      this.options.sitemapBaseUrl || DEFAULT_SITEMAP_BASE_URL;
    this.options.liveSamplesBaseUrl =
      this.options.liveSamplesBaseUrl || DEFAULT_LIVE_SAMPLES_BASE_URL;
    this.options.interactiveExamplesBaseUrl =
      this.options.interactiveExamplesBaseUrl ||
      DEFAULT_INTERACTIVE_EXAMPLES_BASE_URL;

    this.macroRenderer = new kumascript.Renderer({
      uriTransform: this.cleanUri.bind(this),
      liveSamplesBaseUrl: this.options.liveSamplesBaseUrl,
      interactiveExamplesBaseUrl: this.options.interactiveExamplesBaseUrl,
    });

    this.progressBar = !options.noProgressbar
      ? new ProgressBar({
          includeMemory: true,
        })
      : null;
  }

  initProgressbar(total) {
    this.progressBar && this.progressBar.init(total);
  }

  tickProgressbar(incr) {
    this.progressBar && this.progressBar.update(incr);
  }

  printProcessing(result, fileWritten) {
    !this.progressBar && this.logger.info(`${result}: ${fileWritten}`);
  }

  cleanUri(uri) {
    // Attempts to both repair the incoming URI, as well as transform it
    // into a URI that represents an existing document within allTitles.
    // (i.e., it may not exist because it has been redirected, so in that
    // case let's use its final destination instead). Returns a lowercase
    // URI.
    const repairedUri = repairUri(uri);
    return this.allRedirects.get(repairedUri) || repairedUri;
  }

  // Just print what could be found and exit
  listLocales() {
    for (const source of this.sources.entries()) {
      console.log(`\n${chalk.bold("Source:")} ${chalk.white(source.filepath)}`);
      const counts = this.countLocaleFolders(source);
      const sumCounts = Array.from(counts.values()).reduce((a, b) => a + b, 0);
      Array.from(counts)
        .sort((a, b) => b[1] - a[1])
        .forEach(([locale, count], i) => {
          const percent = sumCounts ? (100 * count) / sumCounts : 0;
          console.log(
            `${chalk.green(locale.padEnd(6))}${count
              .toLocaleString()
              .padStart(10)} ${chalk.grey(
              percent.toFixed(percent > 10 ? 1 : 2) + "%"
            )}`
          );
          if (i === counts.size - 1) {
            // Last one
            console.log(
              `${chalk.green(
                "TOTAL".padEnd(6)
              )}${sumCounts.toLocaleString().padStart(10)} ${chalk.grey(
                " 100%"
              )}`
            );
          }
        });
    }
  }

  *walkSources({ allLocales = false } = {}) {
    for (const source of this.sources.entries()) {
      for (const localeFolder of this.getLocaleRootFolders(source, {
        allLocales,
      })) {
        for (const [folder, files] of walker(localeFolder)) {
          yield { source, localeFolder, folder, files };
        }
      }
    }
  }

  getSource(folder) {
    if (!fs.existsSync(folder)) {
      throw new Error(`${folder} does not exist`);
    }
    if (!fs.statSync(folder).isDirectory()) {
      throw new Error(`${folder} is not a directory`);
    }
    const source = this.sources.entries().find((source) => {
      return folder.startsWith(source.filepath);
    });
    if (!source) {
      throw new Error(`Unable to find the source based on ${folder}`);
    }
    return source;
  }

  /**
   * Recursive method that renders the macros within the document
   * represented by this raw HTML and metadata, but only after first
   * rendering all of this document's prerequisites, taking into
   * account that the prerequisites may have prerequisites and so on.
   */
  async renderMacros(source, rawHtml, metadata, { cacheResult = false } = {}) {
    const prerequisites = kumascript.getPrerequisites(rawHtml);
    const mdn_url = buildMDNUrl(metadata.locale, metadata.slug);
    const uri = mdn_url.toLowerCase();

    // First, render all of the prerequisites of this document.
    let allPrerequisiteFlaws = [];
    for (const preUri of prerequisites) {
      const preCleanUri = this.cleanUri(preUri);
      if (!this.allTitles.has(preCleanUri)) {
        allPrerequisiteFlaws.push(
          new Error(
            `${uri} has the prerequisite ${preCleanUri}, which does not exist`
          )
        );
        continue;
      }
      const titleData = this.allTitles.get(preCleanUri);
      const folder = titleData.file;
      if (titleData.source !== source.filepath) {
        allPrerequisiteFlaws.push(
          new Error(
            `${uri} has the prerequisite ${preCleanUri}, which is from a different source`
          )
        );
        continue;
      }
      const { metadata, rawHtml, fileInfo } = Document.read(
        source.filepath,
        folder,
        false
      );
      // When rendering prerequisites, we're only interested in
      // caching the results for later use. We don't care about
      // the results returned.
      const preResult = await this.renderMacros(source, rawHtml, metadata, {
        cacheResult: true,
      });
      const preFlaws = preResult[1];
      // Flatten the flaws from this other document into the current flaws,
      // and set the filepath for flaws that haven't already been set at
      // a different level of recursion.
      for (const flaw of preFlaws) {
        if (!flaw.filepath) {
          flaw.filepath = fileInfo.path;
        }
        allPrerequisiteFlaws.push(flaw);
      }
    }

    // Now that all of the prerequisites have been rendered, we can render
    // this document and return the result.

    const [renderedHtml, flaws] = await this.macroRenderer.render(
      rawHtml,
      {
        path: mdn_url,
        url: `${this.options.sitemapBaseUrl}${mdn_url}`,
        locale: metadata.locale,
        slug: metadata.slug,
        title: metadata.title,
        tags: metadata.tags || [],
        selective_mode: false,
      },
      cacheResult
    );

    // Add the flaws from rendering the macros within all of the prerequisite
    // documents to the flaws from rendering the macros within this document.
    for (const flaw of allPrerequisiteFlaws) {
      flaws.push(flaw);
    }

    return [renderedHtml, flaws];
  }

  async start({ specificFolders = null } = {}) {
    const self = this;

    // Clear any cached results.
    self.macroRenderer.clearCache();

    if (specificFolders) {
      // Check that they all exist and are folders
      let source;
      let processed;
      const allProcessed = [];

      for (const folder of specificFolders) {
        source = self.getSource(folder);
        try {
          processed = await self.processFolder(source, folder);
        } catch (err) {
          self.logger.error(chalk.red(`Error while processing: ${folder}`));
          console.error(err);
          throw err;
        }
        allProcessed.push(processed);
      }

      return allProcessed;
    }

    self.describeActiveSources();
    self.describeActiveFilters();
    self.describeActiveFlawLevels();

    // To be able to make a progress bar we need to first count what we're
    // going to need to do.
    if (self.progressBar) {
      const countTodo = self.sources
        .entries()
        .map((source) => self.countLocaleFolders(source))
        .map((m) => Array.from(m.values()).reduce((a, b) => a + b, 0))
        .reduce((a, b) => a + b);
      if (!countTodo) {
        this.logger.warn(
          chalk.red("No folders found to process. Did you filter too much?")
        );
        return;
      }
      self.initProgressbar(countTodo);
    }

    let total = 0;
    let processed;

    // Record of counts of all results
    const counts = {};
    Object.values(processing).forEach((key) => {
      counts[key] = 0;
    });

    // Record of flaw counts recorded
    const flawCounts = Object.fromEntries(
      [...VALID_FLAW_CHECKS].map((key) => [key, 0])
    );

    function reportProcessed(processed) {
      const { result, file, doc } = processed;
      self.printProcessing(result, file);
      counts[result]++;
      if (doc && doc.flaws) {
        Object.entries(doc.flaws).forEach(([key, value]) => {
          flawCounts[key] += value.length;
        });
      }
      self.tickProgressbar(++total);
    }

    // Start the real processing
    const t0 = new Date();

    for (const { source, localeFolder, folder, files } of self.walkSources()) {
      if (self.excludeFolder(source, folder, localeFolder, files)) {
        // If the folder was a Stumptown folder, what we're
        // actually excluding is all the .json files in the folder.
        if (source.isStumptown) {
          counts[processing.EXCLUDED] += files.filter((n) =>
            n.endsWith(".json")
          ).length;
        } else {
          counts[processing.EXCLUDED]++;
        }
        continue;
      }

      if (source.isStumptown) {
        // In the case of stumptown, one folder will have multiple
        // files with each representing a document.
        for (const filename of files.filter((n) => n.endsWith(".json"))) {
          const filepath = path.join(folder, filename);
          try {
            processed = self.processStumptownFile(source, filepath);
          } catch (err) {
            self.logger.error(chalk.red(`Error while processing: ${filepath}`));
            self.logger.error(err);
            throw err;
          }
          reportProcessed(processed);
        }
      } else {
        try {
          processed = await self.processFolder(source, folder);
        } catch (err) {
          self.logger.error(chalk.red(`Error while processing: ${folder}`));
          console.error(err);
          throw err;
        }
        reportProcessed(processed);
      }
    }

    const t1 = new Date();
    self.dumpAllURLs();
    self.summarizeResults(counts, flawCounts, t1 - t0);
  }

  ensureAllTitles(forceRegenerate = false) {
    if (!this.selfHash) {
      throw new Error("this.selfHash hasn't been set yet");
    }
    if (
      this.allTitles.size &&
      this.allTitles.get("_hash") === this.selfHash &&
      !this.options.regenerateAllTitles &&
      !forceRegenerate
    ) {
      // No reason to proceed, the titles have already been loaded into memory.
      return;
    }

    // First walk all the content and pick up all the titles.
    // Note, no matter what locales you have picked in the filtering,
    // *always* include 'en-US' because with that, it becomes possible
    // to reference back to the English version for any locale.
    // But first, see if we can use the title from the last build.
    if (
      fs.existsSync(ALL_TITLES_JSON_FILEPATH) &&
      !this.options.regenerateAllTitles &&
      !forceRegenerate
    ) {
      this.allTitles = new Map(
        Object.entries(
          JSON.parse(fs.readFileSync(ALL_TITLES_JSON_FILEPATH, "utf8"))
        ).map(([key, value]) => [key.toLowerCase(), value])
      );
      // We got it from disk, but is it out-of-date?
      const outOfDate = this.allTitles.get("_hash") !== this.selfHash;
      if (outOfDate && !this.options.allowStaleTitles) {
        this.logger.info(
          chalk.yellow(
            `${ALL_TITLES_JSON_FILEPATH} existed but is out-of-date.`
          )
        );
      } else {
        if (outOfDate) {
          this.logger.info(
            chalk.yellow(
              `Warning! ${ALL_TITLES_JSON_FILEPATH} exists but is out-of-date. ` +
                "To reset run `yarn clean`."
            )
          );
        }
        // This means we DON'T need to re-generate all titles, so
        // let's set the context for the Kumascript renderer.
        this.macroRenderer.use(this.allTitles);
        return;
      }
    }

    // If we're going to generate all titles, we need all popularities.
    const allPopularities = this._getAllPopularities();

    // You're here and it means we're going to fill up the this.allTitles map.
    // Just to be absolutely clear that there's nothing in there already
    // let's make sure it's cleared:
    this.allTitles.clear();

    // This helps us exclusively to know about the validitity of the
    // _all-titles.json file which is our disk-based caching strategy.
    // It's very possible that the "self hash" has changed because of some
    // change that has no effect on the map of all titles and their data.
    // But it's better to be safe rather than sorry. After all, the
    // _all-titles.json file is purely for local development when you
    // stop and start the builder.
    this.allTitles.set("_hash", this.selfHash);

    this.logger.info("Building a list of ALL titles and URIs...");
    let t0 = new Date();
    for (const { source, folder, files } of this.walkSources({
      allLocales: true,
    })) {
      if (source.isStumptown) {
        for (const filename of files.filter((n) => n.endsWith(".json"))) {
          const filepath = path.join(folder, filename);
          this.processStumptownFileTitle(source, filepath, allPopularities);
        }
      } else if (files.includes("index.html")) {
        this.processFolderTitle(source, folder, allPopularities);
      }
    }

    // Only after *all* titles have been processed can we iterate over the
    // mapping and figure out all translations.
    // What this does is that it sets `.translations=[{slug, locale}, ...]`
    // on every title that is the "translation_of". Practically, that means
    // that every 'en-US' document that has been translated, will have a list
    // of other locales and slugs.
    let countBrokenTranslationOfDocuments = 0;
    for (const [key, data] of this.allTitles) {
      if (key === "_hash") continue;
      if (!data.translation_of) continue;

      const parentUrlLC = buildMDNUrl(
        "en-US",
        data.translation_of
      ).toLowerCase();
      const parentData = this.allTitles.get(parentUrlLC);

      // TODO: Our dumper is not perfect yet. We still get bad
      // 'translation_of' references.
      // I.e. a localized document's metadata says its
      // 'translation_of' is 'Web/Foo/Bar' but there is actually no en-US
      // document by that slug!
      // We're working on it in the dumper and this problem is known also
      // in the 'ensureAllTitles()' method which at least debug logs the
      // bad ones and warn logs about a total count of bad documents.
      // Once the dumper has matured, we'll remove this defensive style and
      // throw an error here. That'll be a form of validation-by-building
      // which can really help our CI trap content edit PRs that sets
      // or gets these references wrong.

      if (parentData) {
        if (!parentData.hasOwnProperty("translations")) {
          parentData.translations = [];
        }
        parentData.translations.push({
          locale: data.locale,
          slug: data.slug,
        });
      } else {
        countBrokenTranslationOfDocuments++;
        this.logger.debug(
          `${data.locale}/${data.slug} (${data.file}) refers to a ` +
            "en-US translation_of that doesn't exist."
        );
      }
    }
    if (countBrokenTranslationOfDocuments) {
      this.logger.warn(
        chalk.yellow(
          `${countBrokenTranslationOfDocuments.toLocaleString()} documents ` +
            "have a 'translation_of' that can actually not be found."
        )
      );
    }

    // Set the context for the Kumascript renderer.
    this.macroRenderer.use(this.allTitles);

    this.dumpAllTitles();
    let t1 = new Date();
    this.logger.info(
      chalk.green(`Building list of all titles took ${ppMilliseconds(t1 - t0)}`)
    );
  }

  dumpAllTitles() {
    fs.writeFileSync(
      ALL_TITLES_JSON_FILEPATH,
      JSON.stringify(mapToObject(this.allTitles), null, 2)
    );
  }

  ensureAllRedirects() {
    if (this.allRedirects.size) {
      // No reason to proceed, the redirects have already been loaded into memory.
      return;
    }

    this.logger.info("Building a map of ALL redirects...");
    let t0 = new Date();

    // Walk all the locale folders and gather all of the redirects.
    for (const source of this.sources.entries()) {
      for (const localeFolder of this.getLocaleRootFolders(source, {
        allLocales: true,
      })) {
        const filepath = path.join(localeFolder, "_redirects.txt");
        if (fs.existsSync(filepath)) {
          const rawRedirects = fs.readFileSync(filepath, "utf8");
          for (const line of rawRedirects.split(/[\r\n]+/)) {
            const trimmedLineLC = line.trim().toLowerCase();
            if (trimmedLineLC && !trimmedLineLC.startsWith("#")) {
              const [fromUri, toUri] = trimmedLineLC
                .split(/\s+/)
                .map((uri) => repairUri(uri));
              this.allRedirects.set(fromUri, toUri);
            }
          }
        }
      }
    }

    let t1 = new Date();
    this.logger.info(
      chalk.green(
        `Building map of all redirects took ${ppMilliseconds(t1 - t0)}`
      )
    );
  }

  async watch() {
    const onChange = async (filepath, source) => {
      const folder = path.dirname(filepath);
      this.logger.info(`${chalk.bold("change")} in ${folder}`);
      const t0 = performance.now();
      // Clear any cached results.
      this.macroRenderer.clearCache();
      const { result, file, doc } = await this.processFolder(source, folder);
      const t1 = performance.now();

      const tookStr = ppMilliseconds(t1 - t0);
      console.log(
        `${
          result === processing.PROCESSED
            ? chalk.green(result)
            : chalk.yellow(result)
        }: ${chalk.white(file)} ${chalk.grey(tookStr)}`
      );
      if (result === processing.PROCESSED) {
        doc.modified = new Date().toISOString();
        triggerTouch(filepath, doc, source.filepath);

        const titleData = this.allTitles.get(doc.mdn_url.toLowerCase());
        for (const [key, value] of Object.entries(titleData)) {
          if (key in doc) {
            if (key !== "source" && value !== doc[key]) {
              titleData[key] = doc[key];
            }
          }
        }
        this.dumpAllTitles();
      }
    };

    this.sources
      .entries()
      .filter((source) => source.watch)
      .forEach((source) => {
        const watchdir = path.resolve(source.filepath);

        console.log(chalk.yellow(`Setting up file watcher on ${watchdir}...`));
        const watcher = chokidar.watch(path.join(watchdir, "**/*.(html|yaml)"));
        watcher.on("change", (path) => {
          onChange(path, source);
        });
        // watcher.on("add", (path) => {
        //   console.log("NEW PATH ADDED", path);
        // });
        watcher.on("ready", () => {
          const watchedPaths = watcher.getWatched();
          const folders = Object.values(watchedPaths);
          const count = folders
            .map((list) => list.length)
            .reduce((a, b) => a + b, 0);
          console.log(
            chalk.yellow(
              `File watcher set up for ${watchdir}. ` +
                `Watching over ${count.toLocaleString()} files in ${folders.length.toLocaleString()} folders.`
            )
          );
          if (isTTY()) {
            console.log("Hit Ctrl-C to quit the watcher when ready.");
          }
        });
      });
  }

  describeActiveSources() {
    const sourcesHuman = this.sources.entries().map((s, i) => {
      let out = `${i + 1}. ${s.filepath}`;
      if (s.isStumptown) {
        out += `\t(stumptown)`;
      } else if (s.htmlAlreadyRendered) {
        out += `\t(html already rendered)`;
      }
      return out;
    });
    console.log(
      chalk.yellow(`Current sources...\n\t${sourcesHuman.join("\n\t")}\n`)
    );
  }

  describeActiveFilters() {
    const filtersHuman = [];
    if (this.options.locales.length) {
      filtersHuman.push(`Locales: ${JSON.stringify(this.options.locales)}`);
    } else if (this.options.notLocales.length) {
      filtersHuman.push(
        `Not locales: ${JSON.stringify(this.options.notLocales)}`
      );
    }
    if (this.options.foldersearch.length) {
      filtersHuman.push(
        `Folders: ${JSON.stringify(this.options.foldersearch)}`
      );
    }
    if (filtersHuman.length) {
      console.log(
        chalk.yellow(`Current filters...\n\t${filtersHuman.join("\n\t")}\n`)
      );
    }
  }

  describeActiveFlawLevels() {
    const keys = [...this.options.flawLevels.keys()];
    keys.sort();
    const longestKey = Math.max(...keys.map((k) => k.length));
    const levels = keys.map(
      (k) => `${k.padEnd(longestKey + 1)} ${this.options.flawLevels.get(k)}`
    );
    console.log(
      chalk.yellow(`Current flaw levels...\n\t${levels.join("\n\t")}\n`)
    );
  }

  prepareRoots() {
    for (const source of this.sources.entries()) {
      for (const localeFolder of this.getLocaleRootFolders(source)) {
        this.prepareRoot(path.basename(localeFolder));
      }
    }
  }

  initSelfHash() {
    const contentRoot = path.resolve(__dirname, "..");
    const ssrRoot = path.resolve(__dirname, "..", "..", "ssr");
    this.selfHash = makeHash([
      // This'll be different when new packages are changed like
      // `mdn-browser-compat-data` but we *could* be more explicit but
      // this'll be on the safe side.
      path.join(contentRoot, "package.json"),
      // This is broad but let's make it depend on every .js file
      // in this file's folder
      ...simpleGlob(contentRoot, ".js"),
      ...simpleGlob(path.join(contentRoot, "scripts"), ".js"),
      // Also factor in the ssr builder's package.json
      path.join(ssrRoot, "package.json"),
      // and it's .js files
      ...simpleGlob(ssrRoot, ".js"),
    ]);
  }

  prepareRoot(locale) {
    if (!this.options.destination) {
      throw new Error("options.destination not set");
    }
    const folderpath = path.join(
      this.options.destination,
      locale.toLowerCase()
    );
    if (this.options.startClean) {
      // Experimental new feature
      // https://nodejs.org/api/fs.html#fs_fs_rmdirsync_path_options
      fs.rmdirSync(folderpath, { recursive: true });
    }
    fs.mkdirSync(folderpath, { recursive: true });
  }

  _getAllPopularities() {
    const { popularitiesfile } = this.options;
    if (popularitiesfile) {
      const allPopularities = JSON.parse(
        fs.readFileSync(popularitiesfile, "utf8")
      );
      this.logger.info(
        chalk.magenta(
          `Parsed ${Object.keys(
            allPopularities
          ).length.toLocaleString()} popularities.`
        )
      );
      return allPopularities;
    }
    // If the popularitiesfile isn't available you simply get
    // no popularity numbers set on any of the documents.
    return {};
  }

  summarizeResults(counts, flawCounts, took) {
    console.log(chalk.green("\nSummary of build:"));
    const totalProcessed = counts[processing.PROCESSED];
    // const totalDocuments = Object.values(counts).reduce((a, b) => a + b);
    const rate = (1000 * totalProcessed) / took; // per second
    console.log(
      chalk.yellow(
        `Processed ${totalProcessed.toLocaleString()} in ${ppMilliseconds(
          took
        )} (roughly ${rate.toFixed(1)} docs/sec)`
      )
    );
    const longestKey = Math.max(...Object.keys(counts).map((k) => k.length));
    Object.keys(counts)
      .sort()
      .forEach((key) => {
        const count = counts[key];
        console.log(`${key.padEnd(longestKey + 1)} ${count.toLocaleString()}`);
      });

    const flawCountsTotal = Object.values(flawCounts).reduce(
      (a, b) => a + b,
      0
    );
    console.log(
      chalk.yellow(
        `${flawCountsTotal.toLocaleString()} flaws detected ` +
          `(from ${totalProcessed.toLocaleString()} documents processed).`
      )
    );
    const flawKeys = Object.keys(flawCounts);

    const longestFlawKey = Math.max(...flawKeys.map((k) => k.length));
    flawKeys.sort().forEach((key) => {
      const count = flawCounts[key];
      console.log(
        `${key.padEnd(longestFlawKey + 1)} ${count.toLocaleString()}`
      );
    });
  }

  /** `this.allTitles` is a map of mdn_url (lowercase) => object that
   * contains useful data for the SSR work. This function dumps just
   * the necessary data of that, per locale, to the final destination.
   */
  dumpAllURLs() {
    const t0 = new Date();
    // First regroup ALL URLs into buckets per locale.
    const byLocale = {};
    const mostModified = {};
    for (const [key, data] of this.allTitles) {
      // That one special key. Ignore it in this context.
      if (key === "_hash") {
        continue;
      }
      const uri = data.mdn_url;
      const { locale, modified } = data;
      // Lowercase because the 'this.options.locales' and
      // 'this.options.notLocales' are always in lowercase but the locale
      // as part of the allTitles values is not.
      const localeLower = locale.toLowerCase();
      if (
        this.options.notLocales.length &&
        this.options.notLocales.includes(localeLower)
      ) {
        continue;
      }
      if (
        this.options.locales.length &&
        !this.options.locales.includes(localeLower)
      ) {
        continue;
      }

      if (!(locale in byLocale)) {
        byLocale[locale] = {};
        mostModified[locale] = modified;
      }
      byLocale[locale][uri] = data;
      if (data.modified > mostModified[locale]) {
        mostModified[locale] = modified;
      }
    }

    const { sitemapBaseUrl } = this.options;
    const allSitemapsBuilt = [];
    const allTitlesBuilt = [];
    Object.entries(byLocale).forEach(([locale, data]) => {
      if (!this.options.noSitemaps) {
        // For every locale, build a
        // `$DESTINATION/sitemaps/$LOCALE/sitemap_other.xml` and
        // `$DESTINATION/sitemaps/$LOCALE/sitemap.xml`

        const sitemapXml = makeSitemapXML(
          Object.entries(data)
            .filter(([uri, documentData]) => {
              // We're looping over all the keys in this.allTitles but
              // nestled into it is also some custom keys that need
              // to be ignored.
              return !documentData.excludeInSitemaps && uri !== "_hash";
            })
            .map(([uri, documentData]) => {
              if (!documentData.modified) {
                throw new Error("No .modified in documentData");
              }
              return {
                loc: sitemapBaseUrl + uri,
                lastmod: documentData.modified.split("T")[0],
              };
            })
        );
        const sitemapsDir = path.join(
          this.destination,
          "sitemaps",
          locale.toLowerCase()
        );
        fs.mkdirSync(sitemapsDir, { recursive: true });
        const sitemapFilepath = path.join(sitemapsDir, "sitemap.xml");
        fs.writeFileSync(sitemapFilepath, sitemapXml);
        allSitemapsBuilt.push(locale);
        this.logger.debug(`Wrote: ${sitemapFilepath}`);
      }

      // Dump a `titles.json` into each locale folder
      const titles = {};
      Object.entries(data)
        .filter(([uri, documentData]) => {
          return !documentData.excludeInTitlesJson && uri !== "_hash";
        })
        .forEach(([uri, documentData]) => {
          // This is the data that gets put into each 'titles.json` file which
          // gets XHR downloaded by the React autocomplete search widget.
          // So, it's important to only inject the absolutely minimum because
          // network bytes matter.
          titles[uri] = {
            title: documentData.title,
            popularity: documentData.popularity,
          };
        });

      const localeFolder = path.join(this.destination, locale.toLowerCase());
      fs.mkdirSync(localeFolder, { recursive: true });
      const titlesFilepath = path.join(localeFolder, "titles.json");
      fs.writeFileSync(titlesFilepath, JSON.stringify({ titles }, null, 2));
      allTitlesBuilt.push(titlesFilepath);
    });
    this.logger.info(
      chalk.green(`${allTitlesBuilt.length} titles.json files created`)
    );

    // Need to make the generic /sitemap.xml for all sitemaps
    if (!this.options.noSitemaps) {
      const allSitemapXml = makeSitemapXML(
        allSitemapsBuilt.map((locale) => {
          return {
            loc: sitemapBaseUrl + `/sitemaps/${locale}/sitemap.xml`,
            lastmod: mostModified[locale],
          };
        })
      );
      const allSitemapFilepath = path.join(this.destination, "sitemap.xml");
      fs.writeFileSync(allSitemapFilepath, allSitemapXml);
      this.logger.debug(`Wrote ${allSitemapFilepath}`);
    }

    const t1 = new Date();
    console.log(
      chalk.yellow(
        `Dumping all URLs to sitemaps and titles.json took ${ppMilliseconds(
          t1 - t0
        )}`
      )
    );
  }

  /** Return true if for any reason this folder should not be processed */
  excludeFolder(source, folder, localeFolder, files) {
    if (this.options.foldersearch.length) {
      // The slice makes it so that `foldername` never starts with a '/'
      // (or '\' on Windows).
      const foldername = folder.replace(localeFolder, "").slice(1);
      if (
        !this.options.foldersearch.some((search) => {
          // The folder search can contain special characters.
          // For example `^web` means `.startswith('web')`.
          // For example `foo/bar/media$` means `.endswith('foo/bar/media$')`.
          // anything else is plainly looking if the string is in the folder name.
          if (search.startsWith("^")) {
            return foldername.startsWith(search.slice(1));
          }
          return foldername.includes(search);
        })
      ) {
        return true;
      }
    }
    if (source.isStumptown) {
      return !files.some((filepath) => filepath.endsWith(".json"));
    } else {
      return !files.includes("index.html");
    }
  }

  /**
   * Return an array of full paths for the locale folders.
   * The 'allLocales' parameter means it overrides the
   * 'options.locales` or `options.notLocales` values.
   */
  *getLocaleRootFolders(source, { allLocales = false } = {}) {
    if (!source || !source.filepath) {
      throw new Error("Invalid source");
    }
    const { locales, notLocales } = this.options;
    const files = fs.readdirSync(source.filepath);
    for (const name of files) {
      const filepath = path.join(source.filepath, name);
      const isDirectory = fs.statSync(filepath).isDirectory();
      if (
        isDirectory &&
        (allLocales ||
          ((!locales.length || locales.includes(name)) &&
            (!notLocales || !notLocales.includes(name))))
      ) {
        yield filepath;
      }
    }
  }

  // For a given source, return a mapping of locale-> docs to build.
  // E.g. {'en-us': 123, fr: 9}
  countLocaleFolders(source) {
    let locales = new Map();
    for (const localeFolder of this.getLocaleRootFolders(source)) {
      const locale = path.basename(localeFolder);
      for (const [folder, files] of walker(localeFolder)) {
        if (this.excludeFolder(source, folder, localeFolder, files)) {
          continue;
        }
        if (source.isStumptown) {
          // In stumptown, you'll have multiple .json files per folder.
          // For example `en-us/html/reference/elements/abbr.json` and
          // `en-us/html/reference/elements/video.json` etc.
          locales.set(
            locale,
            (locales.get(locale) || 0) +
              files.filter((x) => x.endsWith(".json")).length
          );
        } else {
          locales.set(locale, (locales.get(locale) || 0) + 1);
        }
      }
    }
    return locales;
  }

  excludeSlug(url) {
    // XXX would it be faster to compute a regex in the constructor
    // and use it repeatedly here instead?
    const urlLC = url.toLowerCase();
    if (this.options.slugsearch.length) {
      return !this.options.slugsearch.some((search) => urlLC.includes(search));
    }
    return false;
  }

  /**
   * Recursive method that renders the macros within the document
   * represented by this source, URI, metadata and raw HTML, as
   * well as builds any live samples within this document or within
   * other documents referenced by this document.
   */
  async renderMacrosAndBuildLiveSamples(
    source,
    uri,
    metadata,
    rawHtml,
    destinationDir,
    { cacheResult = false, selectedSampleIDs = null } = {}
  ) {
    // First, render the macros to get the rendered HTML.
    const [renderedHtml, flaws] = await this.renderMacros(
      source,
      rawHtml,
      metadata,
      {
        cacheResult,
      }
    );

    // Next, let's find any samples that we need to build, and note
    // that one or more or even all might be within other documents.
    let ownSampleIds;
    let otherSampleIds = null;
    const liveSamplesDir = path.join(destinationDir, "_samples_");
    if (selectedSampleIDs) {
      ownSampleIds = selectedSampleIDs;
    } else {
      [ownSampleIds, otherSampleIds] = kumascript.getLiveSampleIDs(
        metadata.slug,
        rawHtml
      );
    }

    // Next, let's build the live sample pages from the current document, if any.
    if (ownSampleIds) {
      // First, remove any live samples that no longer exist.
      if (fs.existsSync(liveSamplesDir)) {
        // Build a set of sampleID's from the list of sampleID objects.
        const ownSampleIDSet = new Set(
          ownSampleIds.map((x) => x.sampleID.toLowerCase())
        );
        for (const de of fs.readdirSync(liveSamplesDir, {
          withFileTypes: true,
        })) {
          // All directories under the main live-samples directory for this
          // document are specific live samples. If one exists that is not
          // contained in the current set of live samples, remove it.
          if (de.isDirectory() && !ownSampleIDSet.has(de.name)) {
            fs.rmdirSync(path.join(liveSamplesDir, de.name), {
              recursive: true,
            });
          }
        }
      }
      // Now, we'll either update or create new live sample pages.
      let liveSamplePageHtml;
      for (const sampleIDWithContext of ownSampleIds) {
        try {
          liveSamplePageHtml = kumascript.buildLiveSamplePage(
            uri,
            metadata.title,
            renderedHtml,
            sampleIDWithContext
          );
        } catch (e) {
          if (e instanceof kumascript.LiveSampleError) {
            flaws.push(e);
            continue;
          } else {
            throw e;
          }
        }
        const liveSampleDir = path.join(
          liveSamplesDir,
          sampleIDWithContext.sampleID.toLowerCase()
        );
        const liveSamplePath = path.join(liveSampleDir, "index.html");
        if (!fs.existsSync(liveSampleDir)) {
          fs.mkdirSync(liveSampleDir, { recursive: true });
        }
        fs.writeFileSync(liveSamplePath, liveSamplePageHtml);
      }
    } else if (fs.existsSync(liveSamplesDir)) {
      // The live samples within this document have been removed, so
      // recursively remove the entire contents of the live-samples directory.
      fs.rmdirSync(liveSamplesDir, { recursive: true });
    }

    // Finally, let's build any live sample pages within other documents.
    // This document may rely on live sample pages from other documents,
    // so if that's the case, we need to build the specific live sample
    // pages in each of the other documents that this document references.
    // Consider "/en-US/docs/learn/forms/how_to_build_custom_form_controls",
    // which references live sample pages from each of the following:
    // "/en-us/docs/learn/forms/how_to_build_custom_form_controls/example_1"
    // "/en-us/docs/learn/forms/how_to_build_custom_form_controls/example_2"
    // "/en-us/docs/learn/forms/how_to_build_custom_form_controls/example_3"
    // "/en-us/docs/learn/forms/how_to_build_custom_form_controls/example_4"
    // "/en-us/docs/learn/forms/how_to_build_custom_form_controls/example_5"
    for (const [slug, sampleIDs] of otherSampleIds || []) {
      const otherUri = buildMDNUrl(metadata.locale, slug);
      const otherCleanUri = this.cleanUri(otherUri);
      if (!this.allTitles.has(otherCleanUri)) {
        // I suppose we could use any, but let's use the context of the first
        // usage of the sampleID within the original source file.
        const context = sampleIDs[0].context;
        flaws.push(
          new kumascript.LiveSampleError(
            new Error(
              `${uri} references live sample(s) from ${otherCleanUri}, which does not exist`
            ),
            context.source,
            context.token
          )
        );
        continue;
      }
      const otherTitleData = this.allTitles.get(otherCleanUri);
      const otherFolder = otherTitleData.file;
      if (otherTitleData.source !== source.filepath) {
        // Again let's just use the context of the first usage of sampleID within
        // the original source file.
        const context = sampleIDs[0].context;
        flaws.push(
          new kumascript.LiveSampleError(
            new Error(
              `${uri} references live sample(s) from ${otherCleanUri}, which is from a different source`
            ),
            context.source,
            context.token
          )
        );
        continue;
      }
      const {
        metadata: otherMetadata,
        rawHtml: otherRawHtml,
        fileInfo: { path: otherRawHtmlFilepath },
      } = Document.read(source.filepath, otherFolder, false);
      const otherDestinationDir = path.join(
        this.destination,
        slugToFoldername(otherUri)
      );
      const otherResult = await this.renderMacrosAndBuildLiveSamples(
        source,
        otherCleanUri,
        otherMetadata,
        otherRawHtml,
        otherDestinationDir,
        { cacheResult: true, selectedSampleIDs: sampleIDs }
      );
      const otherFlaws = otherResult[1];
      // Flatten the flaws from this other document into the current flaws,
      // and set the filepath for flaws that haven't already been set at
      // a different level of recursion.
      for (const flaw of otherFlaws) {
        if (!flaw.filepath) {
          flaw.filepath = otherRawHtmlFilepath;
        }
        flaws.push(flaw);
      }
    }

    return [renderedHtml, flaws];
  }

  async processFolder(source, folder, config) {
    const { metadata, rawHtml, fileInfo } = Document.read(
      source.filepath,
      folder,
      false
    );
    const mdn_url = buildMDNUrl(metadata.locale, metadata.slug);
    const mdnUrlLC = mdn_url.toLowerCase();
    if (!this.allTitles.has(mdnUrlLC)) {
      // This means the this.allTitles is out of date
      // This can happen if you've added a new document and made
      // the first ever edit on it.
      // For example, if you edit a document's front-matter to change
      // the slug. Then it's a new mdnUrl since the watcher started.
      this.ensureAllTitles(true);
    }

    if (this.excludeSlug(mdn_url)) {
      return { result: processing.EXCLUDED, file: folder };
    }

    config = config || {};

    // The destination is the same as source but with a different base.
    // If the file *came from* /path/to/files/en-US/foo/bar/
    // the final destination is /path/to/build/en-US/foo/bar/index.json
    const destinationDirRaw = path.join(this.destination, mdnUrlLC);
    const destinationDir = path.join(
      this.destination,
      slugToFoldername(mdn_url)
    );
    const hashDestination = path.join(destinationDir, "_index.hash");

    // XXX should we get some of this stuff from this.allTitles instead?!
    // XXX see https://github.com/mdn/stumptown-renderer/issues/502
    const doc = {};

    doc.flaws = {};

    // XXX What might be interesting is to make KS do less.
    // The idea is we first have the raw HTML, which'll contain strings
    // like `{{Compat('foo.bar')}}`, then we allow KS turn that into
    // something like `<div class="bc-data" data-query="foo.bar">...`.
    // To avoid KS having to do that, just replace the KS macro with a marker
    // like `<--#Compat('foo.bar')--> and then replace it here in the
    // post-processing instead.

    let renderedHtml;
    // When 'source.htmlAlreadyRendered' is true, it simply means that the 'index.html'
    // is already fully rendered HTML.
    if (source.htmlAlreadyRendered) {
      renderedHtml = rawHtml;
    } else {
      let flaws;
      try {
        [renderedHtml, flaws] = await this.renderMacrosAndBuildLiveSamples(
          source,
          mdnUrlLC,
          metadata,
          rawHtml,
          destinationDir
        );
      } catch (err) {
        throw err;
      }
      if (flaws.length) {
        // The flaw objects might have a 'line' attribute, but the
        // original document it came from had front-matter in the file.
        // The KS renderer doesn't know about this, so we adjust it
        // accordingly.
        // Only applicable if the flaw has a 'line'
        flaws.forEach((flaw) => {
          if (flaw.line) {
            // The extra `- 1` is because of the added newline that
            // is only present because of the serialized linebreak.
            flaw.line += fileInfo.frontMatterOffset - 1;
          }
        });

        if (this.options.flawLevels.get("macros") === FLAW_LEVELS.ERROR) {
          // Report and exit immediately on the first document with flaws.
          this.logger.error(
            chalk.red.bold(
              `Flaws (${flaws.length}) within ${mdnUrlLC} while rendering macros:`
            )
          );
          flaws.forEach((flaw, i) => {
            this.logger.error(chalk.bold.red(`${i + 1}: ${flaw.name}`));
            this.logger.error(chalk.red(`${flaw}\n`));
          });
          // XXX This is probably the wrong way to bubble up.
          process.exit(1);
        } else if (this.options.flawLevels.get("macros") === FLAW_LEVELS.WARN) {
          // For each flaw, inject the path of the file that was used.
          // This gets used in the dev UI so that you can get a shortcut
          // link to open that file directly in your $EDITOR.
          flaws.forEach((flaw) => {
            if (!flaw.filepath) {
              flaw.filepath = fileInfo.path;
            }
          });
          doc.flaws.macros = flaws;
        }
      }
    }

    // Now we've read in all the "inputs" needed.
    // Even if there's no hope in hell that we're going to get a cache hit,
    // we have to compute this hash because on a cache miss, we need to
    // write it down after we've done the work.
    const hasher = crypto.createHash("md5");
    hasher.update(JSON.stringify(metadata) + rawHtml);
    // I think we should use the "renderedHtml" instead of the "rawHtml" for
    // the document hash since it takes into account document prerequisites
    // (i.e. one document including parts of another document within itself).
    hasher.update(renderedHtml);
    const docHash = hasher.digest("hex").slice(0, 12);
    const combinedHash = `${this.selfHash}.${docHash}`;
    // If the destination and the hash file already exists AND the content
    // of an existing hash file is the same as this `combinedHash` then we
    // can bail early.
    if (
      !this.options.noCache &&
      fs.existsSync(destinationDir) &&
      fs.existsSync(hashDestination) &&
      fs.readFileSync(hashDestination, "utf8") === combinedHash
    ) {
      // XXX If you delete the 'index.html' and/or the 'index.json' file
      // but leave the folder and the 'index.hash' file, this code here
      // will wrongfully say the it's already built.
      return {
        result: processing.ALREADY,
        file: path.join(destinationDir, "index.html"),
        jsonFile: path.join(destinationDir, "index.json"),
      };
    }

    // TODO: The slug should always match the folder name.
    // If you edit the slug bug don't correctly edit the folder it's in
    // it's going to lead to confusion.
    // We can use the utils.slugToFoldername() function and compare
    // its output with the `folder`.
    validateSlug(metadata.slug);

    const $ = cheerio.load(`<div id="_body">${renderedHtml}</div>`, {
      // decodeEntities: false
    });

    // Remove those '<span class="alllinks"><a href="/en-US/docs/tag/Web">View All...</a></span>' links.
    // If a document has them, they don't make sense in a Yari world anyway.
    $("span.alllinks").remove();

    doc.title = metadata.title;
    doc.summary = metadata.summary;
    doc.mdn_url = mdn_url;
    if (metadata.translation_of) {
      doc.translation_of = metadata.translation_of;
    }

    // Note that 'extractSidebar' will always return a string.
    // And if it finds a sidebar section, it gets removed from '$' too.
    // Also note, these operations mutate the `$`.
    doc.sidebarHTML = extractSidebar($, config);

    // With the sidebar out of the way, go ahead and check the rest
    this.injectFlaws(source, doc, $);

    doc.body = extractDocumentSections($, config);

    const titleData = this.allTitles.get(mdnUrlLC);
    if (titleData === undefined) {
      throw new Error(`${mdnUrlLC} is not present in this.allTitles`);
    }
    doc.popularity = titleData.popularity || 0.0;
    doc.modified = titleData.modified;

    const otherTranslations = this.allTitles.get(mdnUrlLC).translations || [];
    if (!otherTranslations.length && metadata.translation_of) {
      // But perhaps the parent has other translations?!
      const parentUrlLC = buildMDNUrl(
        "en-US",
        metadata.translation_of
      ).toLowerCase();
      const parentData = this.allTitles.get(parentUrlLC);
      // See note in 'ensureAllTitles()' about why we need this if statement.
      if (parentData) {
        const parentOtherTranslations = parentData.translations;
        if (parentOtherTranslations && parentOtherTranslations.length) {
          otherTranslations.push(
            ...parentOtherTranslations.filter(
              (translation) => translation.locale !== metadata.locale
            )
          );
        }
      }
    }
    if (otherTranslations.length) {
      doc.other_translations = otherTranslations;
    }

    this.injectSource(source, doc, folder);

    const { outfileJson, outfileHtml } = buildHtmlAndJsonFromDoc({
      doc,
      destinationDir,
      buildHtml: !this.options.buildJsonOnly,
      allTitles: this.allTitles,
    });

    // We're *assuming* that `slugToFoldername(metadata.mdn_url)`
    // can be a valid folder name on the current filesystem. It if's all
    // non-control characters, it should be fine, but some characters can't be
    // used when storing folders. E.g. `:` in Windows.
    // However, we might want that for the eventual S3 key when it gets
    // uploaded. So make a note about it if necessary.
    if (destinationDir !== destinationDirRaw) {
      // In the cleaned folder that the file was put, put a "hidden
      // file" which'll be used by the deployer when it picks S3 key names.
      fs.writeFileSync(
        path.join(destinationDir, "_preferred-name.txt"),
        destinationDirRaw.replace(this.destination, "")
      );
    }

    fs.writeFileSync(hashDestination, combinedHash);
    return {
      result: processing.PROCESSED,
      file: outfileHtml || outfileJson,
      jsonFile: outfileJson,
      doc,
    };
  }

  /**
   * Validate the parsed HTML, with the sidebar removed.
   *
   * @param {folder} source
   * @param {Object} doc
   * @param {Cheerio document instance} $
   */
  injectFlaws(source, doc, $) {
    // The 'broken_links' flaw check looks for internal links that
    // link to a document that's going to fail with a 404 Not Found.
    if (this.options.flawLevels.get("broken_links") !== FLAW_LEVELS.IGNORE) {
      // This is needed because the same href can occur multiple time.
      // Especially when there's...
      //    <a href="/foo/bar#one">
      //    <a href="/foo/bar#two">
      const checked = new Set();

      $("a[href]").each((i, element) => {
        const a = $(element);
        const href = a.attr("href").split("#")[0];
        if (href.startsWith("/") && !checked.has(href)) {
          checked.add(href);
          if (!this.allTitles.has(href.toLowerCase())) {
            if (!doc.flaws.hasOwnProperty("broken_links")) {
              doc.flaws.broken_links = [];
            }
            doc.flaws.broken_links.push(href);
          }
        }
      });
      if (this.options.flawLevels.get("broken_links") === FLAW_LEVELS.ERROR) {
        throw new Error(`broken_links flaws: ${doc.flaws.broken_links}`);
      }
    }

    if (this.options.flawLevels.get("bad_bcd_queries") !== FLAW_LEVELS.IGNORE) {
      $("div.bc-data").each((i, element) => {
        const dataQuery = $(element).attr("id");
        if (!dataQuery) {
          if (!doc.flaws.hasOwnProperty("bad_bcd_queries")) {
            doc.flaws.bad_bcd_queries = [];
          }
          doc.flaws.bad_bcd_queries.push("BCD table without an ID");
        } else {
          const query = dataQuery.replace(/^bcd:/, "");
          const data = packageBCD(query);
          if (!data) {
            if (!doc.flaws.hasOwnProperty("bad_bcd_queries")) {
              doc.flaws.bad_bcd_queries = [];
            }
            doc.flaws.bad_bcd_queries.push(`No BCD data for query: ${query}`);
          }
        }
      });
      if (this.options.flawLevels.get("broken_links") === FLAW_LEVELS.ERROR) {
        throw new Error(`bad_bcd_queries flaws: ${doc.flaws.bad_bcd_queries}`);
      }
    }
  }

  injectSource(source, doc, folder) {
    doc.source = {
      folder: path.relative(source.filepath, folder),
      github_url: this.getGitHubURL(source, folder),
    };
  }

  processStumptownFile(source, file, config) {
    config = config || {};

    const hasher = crypto.createHash("md5");
    const docRaw = fs.readFileSync(file);
    const doc = JSON.parse(docRaw);
    if (this.excludeSlug(doc.mdn_url)) {
      return { result: processing.EXCLUDED, file };
    }
    hasher.update(docRaw);

    const { mdn_url } = doc;
    const destinationDirRaw = path.join(
      this.destination,
      mdn_url.toLowerCase()
    );
    const destinationDir = destinationDirRaw
      .split(path.sep)
      .map(sanitizeFilename)
      .join(path.sep);

    const hashDestination = path.join(destinationDir, "_index.hash");

    // Now let's see if the inputs have changed since last time
    const docHash = hasher.digest("hex").slice(0, 12);
    const combinedHash = `${this.selfHash}.${docHash}`;
    if (
      !this.options.noCache &&
      fs.existsSync(destinationDir) &&
      fs.existsSync(hashDestination) &&
      fs.readFileSync(hashDestination, "utf8") === combinedHash
    ) {
      return {
        result: processing.ALREADY,
        file: path.join(destinationDir, "index.html"),
      };
    }

    this.injectSource(source, doc, file);

    const { outfileJson, outfileHtml } = buildHtmlAndJsonFromDoc({
      doc,
      destinationDir,
      buildHtml: !this.options.buildJsonOnly,
      allTitles: this.allTitles,
    });

    return {
      result: processing.PROCESSED,
      file: outfileHtml || outfileJson,
      jsonFile: outfileJson,
      doc,
    };
  }

  /** Similar to processFolder() but this time we're only interesting it
   * adding this document's uri and title to this.allTitles
   */
  processFolderTitle(source, folder, allPopularities) {
    const { metadata } = Document.read(source.filepath, folder, true);
    let mdn_url;
    try {
      mdn_url = buildMDNUrl(metadata.locale, metadata.slug);
    } catch (err) {
      console.warn(`The folder that caused the error was: ${folder}`);
      throw err;
    }

    const mdnUrlLC = mdn_url.toLowerCase();

    if (this.allTitles.has(mdnUrlLC)) {
      // Already been added by stumptown probably.
      // But, before we exit early, let's update some of the pieces of
      // information that stumptown might not have, such as "modified"
      // and "translation_of".
      if (!this.allTitles.get(mdnUrlLC).modified) {
        this.allTitles.get(mdnUrlLC).modified = metadata.modified;
      }
      if (!this.allTitles.get(mdnUrlLC).translation_of) {
        this.allTitles.get(mdnUrlLC).translation_of = metadata.translation_of;
      }
      return;
    }

    const doc = {
      mdn_url,
      title: metadata.title,
      popularity: allPopularities[mdn_url] || 0.0,
      locale: metadata.locale,
      summary: metadata.summary,
      slug: metadata.slug,
      // It's important that this is a full absolute path so that it
      // will work more universally across builder, server, and watcher.
      file: path.resolve(folder),
      modified: metadata.modified,
      translation_of: metadata.translation_of,
      // XXX To be lean if either of these are false, perhaps not
      // bother setting it.
      excludeInTitlesJson: source.excludeInTitlesJson,
      excludeInSitemaps: source.excludeInSitemaps,
      source: path.resolve(source.filepath),
    };
    if (metadata.tags) {
      // Unfortunately, some of the Kumascript macros (including some of the
      // sidebar macros) depend on tags for proper operation, so we need to
      // keep them for now.
      doc.tags = metadata.tags;
    }
    this.allTitles.set(mdnUrlLC, doc);
  }

  removeFolderTitle(locale, slug) {
    this.allTitles.delete(buildMDNUrl(locale, slug).toLowerCase());
  }

  processStumptownFileTitle(source, file, allPopularities) {
    const metadata = JSON.parse(fs.readFileSync(file));
    const { mdn_url, title } = metadata;
    const doc = {
      mdn_url,
      title,
      popularity: allPopularities[mdn_url] || 0.0,
      locale: null,
      slug: null,
      modified: null,
      parent: null,
      excludeInTitlesJson: source.excludeInTitlesJson,
      excludeInSitemaps: source.excludeInSitemaps,
      source: source.filepath,
    };

    this.allTitles.set(mdn_url.toLowerCase(), doc);
  }

  /**
   * Return the full URL directly to the file in GitHub based on this folder.
   *
   *
   * @param {String} folder - the current folder we're processing.
   * @paraam {Bool} stumptown - source is from stumptown.
   */
  getGitHubURL(source, folder, stumptown = false) {
    const gitUrl = getCurretGitHubBaseURL();
    const branch = getCurrentGitBranch();
    const relativePath = path.relative(source.filepath, folder);
    return `${gitUrl}/blob/${branch}/content/files/${relativePath}/index.html`;
  }
}

function* walker(root, depth = 0) {
  const files = fs.readdirSync(root);
  if (!depth) {
    yield [
      root,
      files.filter((name) => {
        return !fs.statSync(path.join(root, name)).isDirectory();
      }),
    ];
  }
  for (const name of files) {
    const filepath = path.join(root, name);
    const isDirectory = fs.statSync(filepath).isDirectory();
    if (isDirectory) {
      yield [
        filepath,
        fs.readdirSync(filepath).filter((name) => {
          return !fs.statSync(path.join(filepath, name)).isDirectory();
        }),
      ];
      // Now go deeper
      yield* walker(filepath, depth + 1);
    }
  }
}

function ppMilliseconds(ms) {
  // If the number of millseconds is really large, use seconds. Or minutes
  // even.
  if (ms > 1000 * 60 * 5) {
    const seconds = ms / 1000;
    const minutes = seconds / 60;
    return `${minutes.toFixed(1)} minutes`;
  } else if (ms > 100) {
    const seconds = ms / 1000;
    return `${seconds.toFixed(1)} seconds`;
  } else {
    return `${ms.toFixed(1)} milliseconds`;
  }
}

// Return a md5 hash based on the content of a list of file paths
function makeHash(filepaths, length = 12) {
  const hasher = crypto.createHash("md5");
  filepaths
    .map((fp) => fs.readFileSync(fp, "utf8"))
    .forEach((content) => hasher.update(content));
  return hasher.digest("hex").slice(0, length);
}

function makeSitemapXML(locations) {
  const xmlParts = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
  ];
  xmlParts.push(
    ...locations.map((location) => {
      return (
        `<url><loc>${location.loc}</loc>` +
        `<lastmod>${location.lastmod}</lastmod></url>`
      );
    })
  );
  xmlParts.push("</urlset>");
  xmlParts.push("");
  return xmlParts.join("\n");
}

// Slight extension of fs.readdirSync that returns full paths
function simpleGlob(directory, extension) {
  return fs
    .readdirSync(directory)
    .filter((name) => name.endsWith(extension))
    .map((name) => path.join(directory, name));
}

module.exports = {
  runBuild,
  Builder,
};
