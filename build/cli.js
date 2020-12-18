const fs = require("fs");
const path = require("path");
const zlib = require("zlib");

const cliProgress = require("cli-progress");
const program = require("@caporal/core").default;

const { Document, slugToFolder } = require("../content");
// eslint-disable-next-line node/no-missing-require
const { renderDocHTML, renderHTML } = require("../ssr/dist/main");

const options = require("./build-options");
const { buildDocument, renderContributorsTxt } = require("./index");
const SearchIndex = require("./search-index");
const { BUILD_OUT_ROOT } = require("./constants");
const { makeSitemapXML, makeSitemapIndexXML } = require("./sitemaps");
const { CONTENT_TRANSLATED_ROOT } = require("../content/constants");
const { uniqifyTranslationsOf } = require("./translationsof");
const { humanFileSize } = require("./utils");

async function buildDocuments(files = null) {
  // If a list of files was set, it came from the CLI.
  // Override whatever was in the build options.
  const findAllOptions = files
    ? Object.assign({}, options, { files: new Set(files) })
    : options;

  const documents = Document.findAll(findAllOptions);
  const progressBar = new cliProgress.SingleBar(
    {},
    cliProgress.Presets.shades_grey
  );

  const docPerLocale = {};
  const searchIndex = new SearchIndex();

  if (!documents.count) {
    throw new Error("No documents to build found");
  }

  let peakHeapBytes = 0;

  // For keeping track of the total counts of flaws
  const totalFlaws = new Map();

  function appendTotalFlaws(flaws) {
    for (const [key, actualFlaws] of Object.entries(flaws)) {
      const count = actualFlaws.length;
      if (!totalFlaws.has(key)) {
        totalFlaws.set(key, 0);
      }
      totalFlaws.set(key, totalFlaws.get(key) + count);
    }
  }

  // This builds up a mapping from en-US slugs to their translated slugs.
  const translationsOf = new Map();

  !options.noProgressbar && progressBar.start(documents.count);
  for (const document of documents.iter()) {
    const outPath = path.join(BUILD_OUT_ROOT, slugToFolder(document.url));
    fs.mkdirSync(outPath, { recursive: true });

    const { translation_of } = document.metadata;

    // If it's a non-en-US document, it'll most likely have a `translation_of`.
    // If so, add it to the map so that when we build the en-US one, we can
    // get an index of the *other* translations available.
    if (translation_of) {
      if (!translationsOf.has(translation_of)) {
        translationsOf.set(translation_of, []);
      }
      const translation = {
        url: document.url,
        locale: document.metadata.locale,
        title: document.metadata.title,
      };
      if (document.metadata.translation_of_original) {
        translation.original = document.metadata.translation_of_original;
      }
      translationsOf.get(translation_of).push(translation);
      // This is a shortcoming. If this is a translated document, we don't have a
      // complete mapping of all other translations. So, the best we can do is
      // at least link to the English version.
      // In 2021, when we refactor localization entirely, this will need to change.
      // Perhaps, then, we'll do a complete scan through all content first to build
      // up the map before we process each one.
      document.translations = [];
    } else if (translationsOf.has(document.metadata.slug)) {
      document.translations = uniqifyTranslationsOf(
        translationsOf.get(document.metadata.slug),
        document.url
      );
    }

    const {
      doc: builtDocument,
      liveSamples,
      fileAttachments,
      bcdData,
    } = await buildDocument(document);

    if (builtDocument.flaws) {
      appendTotalFlaws(builtDocument.flaws);
    }

    fs.writeFileSync(
      path.join(outPath, "index.html"),
      renderDocHTML(builtDocument, document.url)
    );
    fs.writeFileSync(
      path.join(outPath, "index.json"),
      // This is exploiting the fact that renderDocHTML has the side-effect of
      // mutating the built document which makes this not great and refactor-worthy.
      JSON.stringify({ doc: builtDocument })
    );
    // There are some archived documents that, due to possible corruption or other
    // unknown reasons, don't have a list of contributors.
    if (document.metadata.contributors || !document.isArchive) {
      fs.writeFileSync(
        path.join(outPath, "contributors.txt"),
        renderContributorsTxt(
          document.metadata.contributors,
          !document.isArchive
            ? builtDocument.source.github_url.replace("/blob/", "/commits/")
            : null
        )
      );
    }
    for (const { url, data } of bcdData) {
      fs.writeFileSync(
        path.join(outPath, path.basename(url)),
        JSON.stringify(data)
      );
    }

    for (const { id, html } of liveSamples) {
      const liveSamplePath = path.join(outPath, "_samples_", id, "index.html");
      fs.mkdirSync(path.dirname(liveSamplePath), { recursive: true });
      fs.writeFileSync(liveSamplePath, html);
    }

    for (const filePath of fileAttachments) {
      // We *could* use symlinks instead. But, there's no point :)
      // Yes, a symlink is less disk I/O but it's nominal.
      fs.copyFileSync(filePath, path.join(outPath, path.basename(filePath)));
    }

    // Decide whether it should be indexed (sitemaps, robots meta tag, search-index)
    document.noIndexing =
      (document.isArchive && !document.isTranslated) ||
      document.metadata.slug === "MDN/Kitchensink";

    // Collect non-archived documents' slugs to be used in sitemap building and
    // search index building.
    if (!document.noIndexing) {
      const { locale, slug } = document.metadata;
      if (!docPerLocale[locale]) {
        docPerLocale[locale] = [];
      }
      docPerLocale[locale].push({
        slug,
        modified: document.metadata.modified,
      });

      searchIndex.add(document);
    }

    if (!options.noProgressbar) {
      progressBar.increment();
    } else {
      console.log(outPath);
    }
    const heapBytes = process.memoryUsage().heapUsed;
    if (heapBytes > peakHeapBytes) {
      peakHeapBytes = heapBytes;
    }
  }

  !options.noProgressbar && progressBar.stop();

  const sitemapsBuilt = [];
  for (const [locale, docs] of Object.entries(docPerLocale)) {
    const sitemapDir = path.join(
      BUILD_OUT_ROOT,
      "sitemaps",
      locale.toLowerCase()
    );
    fs.mkdirSync(sitemapDir, { recursive: true });
    const sitemapFilePath = path.join(sitemapDir, "sitemap.xml.gz");
    fs.writeFileSync(
      sitemapFilePath,
      zlib.gzipSync(makeSitemapXML(locale, docs))
    );
    sitemapsBuilt.push(sitemapFilePath);
  }

  // Only if you've just built all of CONTENT_ROOT and all of CONTENT_TRANSLATED_ROOT
  // do we bother generating the combined sitemaps index file.
  // That means, that if you've done this at least once, consequent runs of
  // *only* CONTENT_ROOT will just keep overwriting the sitemaps/en-us/sitemap.xml.gz.
  if (CONTENT_TRANSLATED_ROOT) {
    const sitemapIndexFilePath = path.join(BUILD_OUT_ROOT, "sitemap.xml");
    fs.writeFileSync(
      sitemapIndexFilePath,
      makeSitemapIndexXML(
        sitemapsBuilt.map((fp) => fp.replace(BUILD_OUT_ROOT, ""))
      )
    );
  }

  searchIndex.sort();
  for (const [locale, items] of Object.entries(searchIndex.getItems())) {
    fs.writeFileSync(
      path.join(BUILD_OUT_ROOT, locale.toLowerCase(), "search-index.json"),
      JSON.stringify(items)
    );
  }
  return { slugPerLocale: docPerLocale, peakHeapBytes, totalFlaws };
}

async function buildOtherSPAs() {
  const outPath = path.join(BUILD_OUT_ROOT, "en-us", "_spas");
  fs.mkdirSync(outPath, { recursive: true });

  // The URL isn't very important as long as it triggers the right route in the <App/>
  const url = "/en-US/404.html";
  const html = renderHTML(url, { pageNotFound: true });
  fs.writeFileSync(path.join(outPath, path.basename(url)), html);
  console.log("Wrote", path.join(outPath, path.basename(url)));

  // XXX Here, build things like the home page, site-search etc.
  // ...
}

function formatTotalFlaws(flawsCountMap, header = "Total_Flaws_Count") {
  if (!flawsCountMap.size) {
    return "";
  }
  const keys = [...flawsCountMap.keys()];
  const longestKey = Math.max(...keys.map((k) => k.length));
  const out = ["\n"];
  out.push(header);
  for (const key of keys.sort()) {
    out.push(
      `${key.padEnd(longestKey + 1)} ${flawsCountMap.get(key).toLocaleString()}`
    );
  }
  out.push("\n");
  return out.join("\n");
}

program
  .name("build")
  .option("--spas", "Build the SPA pages", { default: true }) // PR builds
  .option("--spas-only", "Only build the SPA pages", { default: false })
  .argument("[files...]", "specific files to build")
  .action(async ({ args, options }) => {
    try {
      if (options.spas) {
        console.log("\nBuilding SPAs...");
        await buildOtherSPAs();
      }
      if (options.spasOnly) {
        return;
      }

      console.log("\nBuilding Documents...");
      const { files } = args;
      const t0 = new Date();
      const { slugPerLocale, peakHeapBytes, totalFlaws } = await buildDocuments(
        files
      );
      const t1 = new Date();
      const count = Object.values(slugPerLocale).reduce(
        (a, b) => a + b.length,
        0
      );
      const seconds = (t1 - t0) / 1000;
      const took =
        seconds > 60
          ? `${(seconds / 60).toFixed(1)} minutes`
          : `${seconds.toFixed(1)} seconds`;
      console.log(
        `Built ${count.toLocaleString()} pages in ${took}, at a rate of ${(
          count / seconds
        ).toFixed(1)} documents per second.`
      );
      console.log(`Peak heap memory usage: ${humanFileSize(peakHeapBytes)}`);
      console.log(formatTotalFlaws(totalFlaws));
    } catch (error) {
      // So you get a stacktrace in the CLI output
      console.error(error);
      // So that the CLI ultimately fails
      throw error;
    }
  });

program.run();
