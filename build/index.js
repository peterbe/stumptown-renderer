const chalk = require("chalk");

const {
  Document,
  CONTENT_ROOT,
  REPOSITORY_URLS,
  execGit,
} = require("../content");
const kumascript = require("../kumascript");

const { FLAW_LEVELS } = require("./constants");
const {
  extractSections,
  extractSidebar,
  extractSummary,
} = require("./document-extractor");
const SearchIndex = require("./search-index");
const { addBreadcrumbData } = require("./document-utils");
const { fixFixableFlaws, injectFlaws, injectSectionFlaws } = require("./flaws");
const { normalizeBCDURLs, extractBCDData } = require("./bcd-urls");
const { checkImageReferences } = require("./check-images");
const { getPageTitle } = require("./page-title");
const { syntaxHighlight } = require("./syntax-highlight");
const cheerio = require("./monkeypatched-cheerio");
const buildOptions = require("./build-options");
const { gather: gatherGitHistory } = require("./git-history");
const { renderCache: renderKumascriptCache } = require("../kumascript");

const DEFAULT_BRANCH_NAME = "main"; // That's what we use for github.com/mdn/content

// Module-level cache
const rootToGitBranchMap = new Map();

function getCurrentGitBranch(root) {
  if (!rootToGitBranchMap.has(root)) {
    // If this is running in a GitHub Action "PR Build" workflow the current
    // branch name will be set in `GITHUB_REF_NAME_SLUG`.
    let name = DEFAULT_BRANCH_NAME;
    // Only bother getting fancy if the root is CONTENT_ROOT.
    // For other possible roots, just leave it to the default.
    if (root === CONTENT_ROOT) {
      if (process.env.GITHUB_REF) {
        name = process.env.GITHUB_REF.split("/").slice(2).join("/");
      } else {
        // Most probably, you're hacking on the content, using Yari to preview,
        // in a topic branch. Then figure this out using a child-process.
        // Note, if you're in detached head, (e.g. "d6a6c3f17") instead of a named
        // branch, this will fail. But that's why we rely on a default.
        try {
          const output = execGit(["branch", "--show-current"], {
            cwd: root,
          });
          if (output) {
            name = output;
          }
        } catch (e) {
          /* allowed to fail for non git content root */
        }
      }
    }

    rootToGitBranchMap.set(root, name);
  }
  return rootToGitBranchMap.get(root);
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

/**
 * Find all tags that we need to change to tell tools like Google Translate
 * to not translate.
 *
 * @param {Cheerio document instance} $
 */
function injectNoTranslate($) {
  $("pre").addClass("notranslate");
}

/**
 * For every image and iframe, where appropriate add the `loading="lazy"` attribute.
 *
 * @param {Cheerio document instance} $
 */
function injectLoadingLazyAttributes($) {
  $("img:not([loading]), iframe:not([loading])").attr("loading", "lazy");
}

/**
 * Find all `in-page-callout` div elements and rewrite
 * to be just `callout`, no more need to mark them as `webdev`
 * @param {Cheerio document instance} $
 */
function injectInPageCallout($) {
  $("div.in-page-callout")
    .addClass("callout")
    .removeClass("in-page-callout webdev");
}

/**
 * Find all `<div class="warning">` and turn them into `<div class="warning notecard">`
 * and keep in mind that if it was already been manually fixed so, you
 * won't end up with `<div class="warning notecard notecard">`.
 *
 * @param {Cheerio document instance} $
 */
function injectNotecardOnWarnings($) {
  $("div.warning, div.note, div.blockIndicator")
    .addClass("notecard")
    .removeClass("blockIndicator");
}

/**
 * Return the full URL directly to the file in GitHub based on this folder.
 * @param {String} folder - the current folder we're processing.
 */
function getGitHubURL(root, folder) {
  const baseURL = `https://github.com/${REPOSITORY_URLS[root]}`;
  return `${baseURL}/blob/${getCurrentGitBranch(
    root
  )}/files/${folder}/index.html`;
}

function injectSource(doc, document) {
  const folder = document.fileInfo.folder;
  doc.source = {
    folder,
    github_url: getGitHubURL(document.fileInfo.root, folder),
  };
}

/**
 * Return an array of objects like this [{text: ..., id: ...}, ...]
 * from a document's body.
 * This will be used for the "Table of Contents" menu which expects to be able
 * to link to each section with anchor links.
 *
 * @param {Document} doc
 */
function makeTOC(doc) {
  return doc.body
    .map((section) => {
      if (
        (section.type === "prose" ||
          section.type === "browser_compatibility") &&
        section.value.id &&
        section.value.title &&
        !section.value.isH3
      ) {
        return { text: section.value.title, id: section.value.id };
      }
      return null;
    })
    .filter(Boolean);
}

async function buildDocument(document, documentOptions = {}) {
  // Important that the "local" document options comes last.
  // And use Object.assign to create a new object instead of mutating the
  // global one.
  const options = Object.assign({}, buildOptions, documentOptions);
  const { metadata, fileInfo } = document;

  const doc = {
    isArchive: document.isArchive,
    isTranslated: document.isTranslated,
  };

  doc.flaws = {};

  let renderedHtml = "";
  let flaws = [];
  const liveSamples = [];

  if (doc.isArchive) {
    renderedHtml = document.rawHTML;
  } else {
    if (options.clearKumascriptRenderCache) {
      renderKumascriptCache.clear();
    }
    try {
      [renderedHtml, flaws] = await kumascript.render(document.url);
    } catch (error) {
      if (error.name === "MacroInvocationError") {
        // The source HTML couldn't even be parsed! There's no point allowing
        // anything else move on.
        // But considering that this might just be one of many documents you're
        // building, let's at least help by setting a more user-friendly error
        // message.
        error.updateFileInfo(document.fileInfo);
        throw new Error(
          `MacroInvocationError trying to parse ${error.filepath}, line ${error.line} column ${error.column} (${error.error.message})`
        );
      }

      // Any other unexpected error re-thrown.
      throw error;
    }

    const sampleIds = kumascript.getLiveSampleIDs(
      document.metadata.slug,
      document.rawHTML
    );
    for (const sampleIdObject of sampleIds) {
      const liveSamplePage = kumascript.buildLiveSamplePage(
        document.url,
        document.metadata.title,
        renderedHtml,
        sampleIdObject
      );
      if (liveSamplePage.flaw) {
        flaws.push(liveSamplePage.flaw.updateFileInfo(fileInfo));
        continue;
      }
      liveSamples.push({
        id: sampleIdObject.id.toLowerCase(),
        html: liveSamplePage.html,
      });
    }

    if (flaws.length) {
      if (options.flawLevels.get("macros") === FLAW_LEVELS.ERROR) {
        // Report and exit immediately on the first document with flaws.
        console.error(
          chalk.red.bold(
            `Flaws (${flaws.length}) within ${document.metadata.slug} while rendering macros:`
          )
        );
        flaws.forEach((flaw, i) => {
          console.error(chalk.bold.red(`${i + 1}: ${flaw.name}`));
          console.error(chalk.red(`${flaw}\n`));
        });
        // // XXX This is probably the wrong way to bubble up.
        // process.exit(1);
        throw new Error("Flaw error encountered");
      } else if (options.flawLevels.get("macros") === FLAW_LEVELS.WARN) {
        // doc.flaws.macros = flaws;
        // The 'flaws' array don't have everything we need from the
        // kumascript rendering, so we "beef it up" to have convenient
        // attributes needed.
        doc.flaws.macros = flaws.map((flaw, i) => {
          const fixable =
            flaw.name === "MacroRedirectedLinkError" &&
            (!flaw.filepath || flaw.filepath === document.fileInfo.path);
          const suggestion = fixable
            ? flaw.macroSource.replace(
                flaw.redirectInfo.current,
                flaw.redirectInfo.suggested
              )
            : null;
          const id = `macro${i}`;
          const explanation = flaw.error.message;
          return Object.assign({ id, fixable, suggestion, explanation }, flaw);
        });
      }
    }
  }

  // TODO: The slug should always match the folder name.
  // If you edit the slug bug don't correctly edit the folder it's in
  // it's going to lead to confusion.
  // We can use the utils.slugToFolder() function and compare
  // its output with the `folder`.
  validateSlug(metadata.slug);

  const $ = cheerio.load(`<div id="_body">${renderedHtml}</div>`);

  // Remove those '<span class="alllinks"><a href="/en-US/docs/tag/Web">View All...</a></span>' links.
  // If a document has them, they don't make sense in a Yari world anyway.
  $("span.alllinks").remove();

  doc.title = metadata.title;
  doc.mdn_url = document.url;
  doc.locale = metadata.locale;

  // Note that 'extractSidebar' will always return a string.
  // And if it finds a sidebar section, it gets removed from '$' too.
  // Also note, these operations mutate the `$`.
  doc.sidebarHTML = extractSidebar($);

  // Check and scrutinize any local image references
  const fileAttachments = checkImageReferences(doc, $, options, document);

  // With the sidebar out of the way, go ahead and check the rest
  try {
    injectFlaws(doc, $, options, document);
  } catch (error) {
    console.warn(
      `Injecting flaws into ${document.fileInfo.path} (${document.url}) failed.`
    );
    throw error;
  }

  // Some hyperlinks are not easily fixable and we should never include them
  // because they're potentially evil.
  $("a[href]").each((i, a) => {
    // See https://github.com/mdn/kuma/issues/7647
    // Ideally we should manually remove this from all sources (archived or not)
    // but that's not immediately feasible. So at least make sure we never
    // present the link in any rendered HTML.
    if (
      a.attribs.href.startsWith("http") &&
      a.attribs.href.includes("fxsitecompat.com")
    ) {
      $(a).attr("href", "https://github.com/mdn/kuma/issues/7647");
    }
  });

  // If fixFlaws is on and the doc has fixable flaws, this returned
  // raw HTML string will be different.
  try {
    await fixFixableFlaws(doc, options, document);
  } catch (error) {
    console.error(error);
    throw error;
  }

  // Apply syntax highlighting all <pre> tags.
  syntaxHighlight($, doc);

  // Post process HTML so that the right elements gets tagged so they
  // *don't* get translated by tools like Google Translate.
  injectNoTranslate($);

  // Add the `loading=lazy` HTML attribute to the appropriate elements.
  injectLoadingLazyAttributes($);

  // All content that uses `<div class="in-page-callout">` needs to
  // become `<div class="callout">`
  // Some day, we can hopefully do a mass search-and-replace so we never
  // need to do this code here.
  // We might want to delete this injection in 2021 some time when all content's
  // raw HTML has been fixed to always have it in there already.
  injectInPageCallout($);

  // All content that uses `<div class="warning">` needs to become
  // `<div class="warning notecard">` instead.
  // Some day, we can hopefully do a mass search-and-replace so we never
  // need to do this code here.
  // We might want to delete this injection in 2021 some time when all content's
  // raw HTML has been fixed to always have it in there already.
  injectNotecardOnWarnings($);

  // Turn the $ instance into an array of section blocks. Most of the
  // section blocks are of type "prose" and their value is a string blob
  // of HTML.
  const [sections, sectionFlaws] = extractSections($);
  doc.body = sections;
  if (sectionFlaws.length) {
    injectSectionFlaws(doc, sectionFlaws, options);
  }

  // Extract all the <h2> tags as they appear into an array.
  doc.toc = makeTOC(doc);

  // The summary comes from the HTML and potentially the <h2>Summary</h2>
  // section. It's always a plain text string.
  doc.summary = extractSummary(doc.body);

  // Creates new mdn_url's for the browser-compatibility-table to link to
  // pages within this project rather than use the absolute URLs
  normalizeBCDURLs(doc, options);

  const bcdData = extractBCDData(doc);

  // If the document has a `.popularity` make sure don't bother with too
  // many significant figures on it.
  doc.popularity = metadata.popularity
    ? Number(metadata.popularity.toFixed(4))
    : 0.0;

  doc.modified = metadata.modified || null;

  const otherTranslations = document.translations || [];
  if (!otherTranslations.length && metadata.translation_of) {
    // If built just-in-time, we won't have a record of all the other translations
    // available. But if the current document has a translation_of, we can
    // at least use that.
    const translationOf = Document.findByURL(
      `/en-US/docs/${metadata.translation_of}`
    );
    if (translationOf) {
      otherTranslations.push({
        locale: "en-US",
        // slug: translationOf.metadata.slug,
        url: translationOf.url,
        title: translationOf.metadata.title,
      });
    }
  }

  if (otherTranslations.length) {
    doc.other_translations = otherTranslations;
  }

  injectSource(doc, document);

  // The `titles` object should contain every possible URI->Title mapping.
  // We can use that generate the necessary information needed to build
  // a breadcrumb in the React component.
  addBreadcrumbData(document.url, doc);

  doc.pageTitle = getPageTitle(doc);

  return { doc, liveSamples, fileAttachments, bcdData };
}

async function buildDocumentFromURL(url, documentOptions = {}) {
  const document = Document.findByURL(url);
  if (!document) {
    return null;
  }
  return await buildDocument(document, documentOptions);
}

async function buildLiveSamplePageFromURL(url) {
  const [documentURL, sampleID] = url.split("/_samples_/");
  const document = Document.findByURL(documentURL);
  if (!document) {
    throw new Error(`No document found for ${documentURL}`);
  }
  // Convert the lower-case sampleID we extract from the incoming URL into
  // the actual sampleID object with the properly-cased live-sample ID.
  for (const sampleIDObject of kumascript.getLiveSampleIDs(
    document.metadata.slug,
    document.rawHTML
  )) {
    if (sampleIDObject.id.toLowerCase() === sampleID) {
      const liveSamplePage = kumascript.buildLiveSamplePage(
        document.url,
        document.metadata.title,
        (await kumascript.render(document.url))[0],
        sampleIDObject
      );
      if (liveSamplePage.flaw) {
        throw new Error(liveSamplePage.flaw.toString());
      }
      return liveSamplePage.html;
    }
  }
  throw new Error(`No live-sample "${sampleID}" found within ${documentURL}`);
}

// This is used by the builder (yarn build) and by the server (JIT).
// Someday, this function might change if we decide to include the list
// of GitHub usernames that have contributed to it since it moved to GitHub.
function renderContributorsTxt(wikiContributorNames = null, githubURL = null) {
  let txt = "";
  if (githubURL) {
    // Always show this first
    txt += `# Contributors by commit history\n${githubURL}\n\n`;
  }
  if (wikiContributorNames) {
    txt += `# Original Wiki contributors\n${wikiContributorNames.join("\n")}\n`;
  }
  return txt;
}

module.exports = {
  FLAW_LEVELS,

  buildDocument,

  buildDocumentFromURL,
  buildLiveSamplePageFromURL,
  renderContributorsTxt,

  SearchIndex,

  options: buildOptions,
  gatherGitHistory,
};
