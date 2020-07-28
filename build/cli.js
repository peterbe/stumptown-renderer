const fs = require("fs");
const path = require("path");

const cliProgress = require("cli-progress");

const { CONTENT_ROOT, Document, slugToFoldername } = require("content");
const { renderHTML } = require("ssr");

const options = require("./build-options");
const { buildDocument } = require("./index");
const SearchIndex = require("./search-index");
const { BUILD_OUT_ROOT } = require("./constants");

function makeSitemapXML(locale, slugs) {
  const wikiHistory = JSON.parse(
    fs.readFileSync(
      path.join(CONTENT_ROOT, locale.toLowerCase(), "_wikihistory.json"),
      "utf-8"
    )
  );
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ...slugs
      .filter((slug) => slug in wikiHistory)
      .map((slug) =>
        [
          "<url>",
          `<loc>https://developer.mozilla.org/${locale}/docs/${slug}</loc>`,
          `<lastmod>${wikiHistory[slug].modified}</lastmod>`,
          "</url>",
        ].join("")
      ),
    "</urlset>",
    "",
  ].join("\n");
}

async function buildDocuments() {
  const documents = Document.findAll(options);
  const progressBar = new cliProgress.SingleBar(
    {},
    cliProgress.Presets.shades_grey
  );

  const slugPerLocale = {};
  const searchIndex = new SearchIndex();

  if (!documents.count) {
    console.warn("No documents to build found");
    return;
  }

  !options.noProgressbar && progressBar.start(documents.count);
  for (const document of documents.iter()) {
    const outPath = path.join(BUILD_OUT_ROOT, slugToFoldername(document.url));
    fs.mkdirSync(outPath, { recursive: true });

    const [builtDocument, liveSamples] = await buildDocument(document);

    fs.writeFileSync(
      path.join(outPath, "index.html"),
      renderHTML(builtDocument, document.url)
    );
    fs.writeFileSync(
      path.join(outPath, "index.json"),
      // This is exploiting the fact that renderHTML has the side-effect of mutating builtDocument
      // which makes this not great and refactor-worthy
      JSON.stringify({ doc: builtDocument })
    );

    for (const { id, html } of liveSamples) {
      const liveSamplePath = path.join(outPath, "_samples_", id, "index.html");
      fs.mkdirSync(path.dirname(liveSamplePath), { recursive: true });
      fs.writeFileSync(liveSamplePath, html);
    }

    // Collect non-archived documents' slugs to be used in sitemap building and
    // search index building
    if (!document.isArchive) {
      const { locale, slug } = document.metadata;
      if (!slugPerLocale[locale]) {
        slugPerLocale[locale] = [];
      }
      slugPerLocale[locale].push(slug);

      searchIndex.add(document);
    }

    if (!options.noProgressbar) {
      progressBar.increment();
    } else {
      console.log(outPath);
    }
  }

  !options.noProgressbar && progressBar.stop();

  for (const [locale, slugs] of Object.entries(slugPerLocale)) {
    const sitemapDir = path.join(
      BUILD_OUT_ROOT,
      "sitemaps",
      locale.toLowerCase()
    );
    fs.mkdirSync(sitemapDir, { recursive: true });
    fs.writeFileSync(
      path.join(sitemapDir, "sitemap.xml"),
      makeSitemapXML(locale, slugs)
    );
  }

  for (const [locale, items] of Object.entries(searchIndex.getItems())) {
    fs.writeFileSync(
      path.join(BUILD_OUT_ROOT, locale.toLowerCase(), "search-index.json"),
      JSON.stringify(items)
    );
  }
}

if (require.main === module) {
  buildDocuments().catch((error) => {
    console.error("error while building documents:", error);
  });
}
