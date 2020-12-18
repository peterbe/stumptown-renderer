const fs = require("fs");
const path = require("path");

const chalk = require("chalk");
const got = require("got");
const imagemin = require("imagemin");
const imageminPngquant = require("imagemin-pngquant");
const imageminMozjpeg = require("imagemin-mozjpeg");
const imageminGifsicle = require("imagemin-gifsicle");
const imageminSvgo = require("imagemin-svgo");

const { Document, Redirect } = require("../content");
const { FLAW_LEVELS } = require("./constants");
const { packageBCD } = require("./resolve-bcd");
const {
  findMatchesInText,
  replaceMatchesInText,
} = require("./matches-in-text");
const { humanFileSize } = require("./utils");

function injectFlaws(doc, $, options, { rawContent }) {
  if (doc.isArchive) return;

  injectBrokenLinksFlaws(
    options.flawLevels.get("broken_links"),
    doc,
    $,
    rawContent
  );

  injectBadBCDQueriesFlaws(options.flawLevels.get("bad_bcd_queries"), doc, $);

  injectPreWithHTMLFlaws(
    options.flawLevels.get("pre_with_html"),
    doc,
    $,
    rawContent
  );
}

function injectSectionFlaws(doc, flaws, options) {
  if (doc.isArchive || !flaws.length) {
    return;
  }

  const level = options.flawLevels.get("sectioning");
  if (level === FLAW_LEVELS.ERROR) {
    throw new Error(flaws.join(" "));
  } else if (level === FLAW_LEVELS.WARN) {
    doc.flaws.sectioning = flaws.map((explanation, i) => {
      const id = `sectioning${i + 1}`;
      return { id, explanation };
    });
  }
}

// The 'broken_links' flaw check looks for internal links that
// link to a document that's going to fail with a 404 Not Found.
function injectBrokenLinksFlaws(level, doc, $, rawContent) {
  if (level === FLAW_LEVELS.IGNORE) return;

  // This is needed because the same href can occur multiple time.
  // For example:
  //    <a href="/foo/bar">
  //    <a href="/foo/other">
  //    <a href="/foo/bar">  (again!)
  // In this case, when we call `addBrokenLink()` that third time, we know
  // this refers to the second time it appears. That's important for the
  // sake of finding which match, in the original source (rawContent),
  // it belongs to.
  const checked = new Map();

  // Our cache for looking things up by `href`. This basically protects
  // us from calling `findMatchesInText()` more than once.
  const matches = new Map();

  // A closure function to help making it easier to append flaws
  function addBrokenLink($element, index, href, suggestion = null) {
    if (!matches.has(href)) {
      matches.set(
        href,
        Array.from(
          findMatchesInText(href, rawContent, {
            attribute: "href",
          })
        )
      );
    }
    // findMatchesInText() is a generator function so use `Array.from()`
    // to turn it into an array so we can use `.forEach()` because that
    // gives us an `i` for every loop.
    matches.get(href).forEach((match, i) => {
      if (i !== index) {
        return;
      }
      if (!("broken_links" in doc.flaws)) {
        doc.flaws.broken_links = [];
      }
      const id = `link${doc.flaws.broken_links.length + 1}`;
      const explanation = `Can't resolve ${href}`;
      let fixable = false;
      if (suggestion) {
        $element.attr("href", suggestion);
        fixable = true;
      }
      $element.attr("data-flaw", id);
      doc.flaws.broken_links.push(
        Object.assign({ explanation, id, href, suggestion, fixable }, match)
      );
    });
  }

  $("a[href]").each((i, element) => {
    const a = $(element);
    const href = a.attr("href");

    // This gives us insight into how many times this exact `href`
    // has been encountered in the doc.
    // Then, when we call addBrokenLink() we can include an index so that
    // that function knows which match it's referring to.
    checked.set(href, checked.has(href) ? checked.get(href) + 1 : 0);

    if (href.startsWith("https://developer.mozilla.org/")) {
      // It might be a working 200 OK link but the link just shouldn't
      // have the full absolute URL part in it.
      const absoluteURL = new URL(href);
      addBrokenLink(
        a,
        checked.get(href),
        href,
        absoluteURL.pathname + absoluteURL.search + absoluteURL.hash
      );
    } else if (href.startsWith("/") && !href.startsWith("//")) {
      // Got to fake the domain to sensible extract the .search and .hash
      const absoluteURL = new URL(href, "http://www.example.com");
      // Note, a lot of links are like this:
      //  <a href="/docs/Learn/Front-end_web_developer">
      // which means the author wanted the link to work in any language.
      // When checking it against disk, we'll have to assume a locale.
      let hrefNormalized = href.split("#")[0];
      if (hrefNormalized.startsWith("/docs/")) {
        const thisDocumentLocale = doc.mdn_url.split("/")[1];
        hrefNormalized = `/${thisDocumentLocale}${hrefNormalized}`;
      }
      const found = Document.findByURL(hrefNormalized);
      if (!found) {
        // Before we give up, check if it's a redirect
        const resolved = Redirect.resolve(hrefNormalized);
        if (resolved !== hrefNormalized) {
          addBrokenLink(
            a,
            checked.get(href),
            href,
            resolved + absoluteURL.search + absoluteURL.hash
          );
        } else {
          addBrokenLink(a, checked.get(href), href);
        }
      } else {
        // But does it have the correct case?!
        if (found.url !== href.split("#")[0]) {
          // Inconsistent case.
          addBrokenLink(
            a,
            checked.get(href),
            href,
            found.url + absoluteURL.search + absoluteURL.hash
          );
        }
      }
    }
  });
  if (
    level === FLAW_LEVELS.ERROR &&
    doc.flaws.broken_links &&
    doc.flaws.broken_links.length
  ) {
    throw new Error(
      `broken_links flaws: ${doc.flaws.broken_links.map(JSON.stringify)}`
    );
  }
}

// Bad BCD queries are when the `<div class="bc-data">` tags have an
// ID (or even lack the `id` attribute) that don't match anything in the
// @mdn/browser-compat-data package. E.g. Something like this:
//
//    <div class="bc-data" id="bcd:never.ever.heard.of">
//
function injectBadBCDQueriesFlaws(level, doc, $) {
  if (level === FLAW_LEVELS.IGNORE) return;

  $("div.bc-data").each((i, element) => {
    const dataQuery = $(element).attr("id");
    if (!dataQuery) {
      if (!("bad_bcd_queries" in doc.flaws)) {
        doc.flaws.bad_bcd_queries = [];
      }
      doc.flaws.bad_bcd_queries.push({
        id: `bad_bcd_queries${doc.flaws.bad_bcd_queries.length}`,
        explanation: "BCD table without an ID",
        suggestion: null,
      });
    } else {
      const query = dataQuery.replace(/^bcd:/, "");
      const { data } = packageBCD(query);
      if (!data) {
        if (!("bad_bcd_queries" in doc.flaws)) {
          doc.flaws.bad_bcd_queries = [];
        }
        doc.flaws.bad_bcd_queries.push({
          id: `bad_bcd_queries${doc.flaws.bad_bcd_queries.length}`,
          explanation: `No BCD data for query: ${query}`,
          suggestion: null,
        });
      }
    }
  });
  if (
    level === FLAW_LEVELS.ERROR &&
    doc.flaws.bad_bcd_queries &&
    doc.flaws.bad_bcd_queries.length
  ) {
    throw new Error(
      `bad_bcd_queries flaws: ${doc.flaws.bad_bcd_queries.map(
        (f) => f.explanation
      )}`
    );
  }
}

// Over the years, we've accumulated a lot of Kuma-HTML where the <pre> tags
// are actually full of HTML. Almost exclusively we've observed <pre> tags whose
// content is the HTML produced by Prism in the browser. Instead, in these cases,
// we should just have the code that it will syntax highlight.
// Note! Having HTML in a <pre> tag is nothing weird or wrong. But if you have
//
//  <pre class="language-js">
//    <code class="language-js">
//     <span class="keyword">foo</span>
//   </code>
//  </pre>
//
// It's better just have the text itself inside the <pre> block:
//  <pre class="language-js">
//    foo
//  </pre>
//
// This makes it easier to edit the code in raw form. It also makes it less
// heavy because any HTML will be replaced with Prism HTML anyway.
function injectPreWithHTMLFlaws(level, doc, $, rawContent) {
  if (level === FLAW_LEVELS.IGNORE) return;

  function escapeHTML(s) {
    return s
      .replace(/&/g, "&amp;")
      .replace(/"/g, "&quot;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function addFlaw($pre) {
    if (!("pre_with_html" in doc.flaws)) {
      doc.flaws.pre_with_html = [];
    }

    const id = `pre_with_html${doc.flaws.pre_with_html.length + 1}`;
    const explanation = `<pre><code>CODE can be just <pre>CODE`;
    const suggestion = escapeHTML($pre.text());

    let fixable = false;
    let html = $pre.html();
    if (rawContent.includes(html)) {
      fixable = true;
    } else {
      // Many times, the HTML found in the sources is what Cheerio would
      // serialize as HTML but with the small difference that some entities
      // are unnecessarily HTML encoded as entities. So, we try to ignore
      // that and see if we can match it as serialized.
      const htmlFixed = html.replace(/&apos;/g, "'");
      if (rawContent.includes(htmlFixed)) {
        html = htmlFixed;
        fixable = true;
      }
    }

    const flaw = { explanation, id, fixable, html, suggestion };
    if (fixable) {
      // Only if it's fixable, is the `html` perfectly findable in the raw content.
      for (const { line, column } of findMatchesInText(html, rawContent)) {
        flaw.line = line;
        flaw.column = column;
      }
    }

    // Actually mutate the cheerio instance so we benefit from the
    // flaw detection immediately.
    // Note that `$pre.text()` might contain unescaped HTML, but Cheerio will
    // take care of that.
    $pre.html(suggestion); // strips all other HTML but preserves whitespace
    $pre.attr("data-flaw", id);

    doc.flaws.pre_with_html.push(flaw);
  }

  // Matches all <code> that are preceded by
  // a <pre class="...brush...">
  // Note, when we (in Node) syntax highlight code, the first thing
  // we look for is the `$("pre[class*=brush]")` selector.
  // Note, in jQuery you can do `$("pre + code")` which means it only selects
  // the `<code>` tags that are *immediately preceeding*. This is not supported
  // in Cheerio :( but the `>` operator works. And since we're only looking
  // at a specific classname on the <pre> the chances of picking up the wrong
  // selectors is small.
  $("pre[class*=brush] > code").each((i, element) => {
    const $pre = $(element).parent();
    // Because Cheerio doesn't support selectors like `pre + code` we have to
    // manually (double) check that the parent really is a `<pre>` tag.
    if ($pre.length && $pre.get(0).tagName === "pre") {
      addFlaw($pre);
    }
  });

  if (
    level === FLAW_LEVELS.ERROR &&
    doc.flaws.pre_with_html &&
    doc.flaws.pre_with_html.length
  ) {
    throw new Error(
      `pre_with_html flaws: ${doc.flaws.pre_with_html.map(JSON.stringify)}`
    );
  }
}

async function fixFixableFlaws(doc, options, document) {
  if (!options.fixFlaws || document.isArchive) return;

  let newRawHTML = document.rawHTML;

  const phrasing = options.fixFlawsDryRun ? "Would fix" : "Fixed";

  const loud = options.fixFlawsDryRun || options.fixFlawsVerbose;

  // Any 'macros' of type "MacroRedirectedLinkError"...
  for (const flaw of doc.flaws.macros || []) {
    if (flaw.fixable) {
      // Sanity check that our understanding of flaws, filepaths, and sources
      // work as expected.
      if (!newRawHTML.includes(flaw.macroSource)) {
        throw new Error(
          `rawHTML doesn't contain macroSource (${flaw.macroSource})`
        );
      }
      const newMacroSource = flaw.suggestion;
      // Remember, in JavaScript only the first occurrence will be replaced.
      newRawHTML = newRawHTML.replace(flaw.macroSource, newMacroSource);
      if (loud) {
        console.log(
          chalk.grey(
            `${phrasing} (${flaw.id}) macro ${chalk.white.bold(
              flaw.macroSource
            )} to ${chalk.white.bold(newMacroSource)}`
          )
        );
      }
    }
  }

  // Any 'broken_links' with a suggestion...
  for (const flaw of doc.flaws.broken_links || []) {
    if (!flaw.suggestion) {
      continue;
    }
    // The reason we're not using the parse HTML, as a cheerio object `$`
    // is because the raw HTML we're dealing with isn't actually proper
    // HTML. It's only proper HTML when the kumascript macros have been
    // expanded.
    newRawHTML = replaceMatchesInText(flaw.href, newRawHTML, flaw.suggestion, {
      inAttribute: "href",
    });
    if (loud) {
      console.log(
        chalk.grey(
          `${phrasing} (${flaw.id}) broken_link ${chalk.white.bold(
            flaw.href
          )} to ${chalk.white.bold(flaw.suggestion)}`
        )
      );
    }
  }

  // Any 'pre_with_html' with a suggestion...
  for (const flaw of doc.flaws.pre_with_html || []) {
    if (!flaw.suggestion || !flaw.fixable) {
      continue;
    }

    if (!newRawHTML.includes(flaw.html)) {
      throw new Error(`rawHTML doesn't contain flaw HTML (${flaw.html})`);
    }
    // It's not feasible to pin point exactly which `<pre>` tag this
    // refers to, so do the same query we use when we find the
    // flaw, but this time actually make the mutation.
    newRawHTML = newRawHTML.replace(flaw.html, flaw.suggestion);
    if (loud) {
      console.log(chalk.grey(`${phrasing} (${flaw.id}) pre_with_html`));
    }
  }

  // Any 'images' flaws with a suggestion or external image...
  for (const flaw of doc.flaws.images || []) {
    if (!(flaw.suggestion || flaw.externalImage)) {
      continue;
    }
    // The reason we're not using the parse HTML, as a cheerio object `$`
    // is because the raw HTML we're dealing with isn't actually proper
    // HTML. It's only proper HTML when the kumascript macros have been
    // expanded.
    let newSrc;
    if (flaw.externalImage) {
      // Sanity check that it's an external image
      const url = new URL(flaw.src);
      if (url.protocol !== "https:") {
        throw new Error(`Insecure image URL ${flaw.src}`);
      }
      try {
        const imageBuffer = await got(flaw.src, {
          responseType: "buffer",
          resolveBodyOnly: true,
          timeout: 10000,
          retry: 3,
        });
        const destination = path.join(
          Document.getFolderPath(document.metadata),
          path
            .basename(decodeURI(url.pathname))
            .replace(/\s+/g, "_")
            .toLowerCase()
        );
        // Before writing to disk, run it through the same imagemin
        // compression we do in the filecheck CLI.
        const compressedImageBuffer = await imagemin.buffer(imageBuffer, {
          plugins: [getImageminPlugin(url.pathname)],
        });
        if (compressedImageBuffer.length < imageBuffer.length) {
          console.log(
            `Raw image size: ${humanFileSize(
              imageBuffer.length
            )} Compressed: ${humanFileSize(compressedImageBuffer.length)}`
          );
          fs.writeFileSync(destination, compressedImageBuffer);
        } else {
          console.log(`Raw image size: ${humanFileSize(imageBuffer.length)}`);
          fs.writeFileSync(destination, imageBuffer);
        }
        console.log(`Downloaded ${flaw.src} to ${destination}`);
        newSrc = path.basename(destination);
      } catch (error) {
        const { response } = error;
        if (response && response.statusCode === 404) {
          console.log(chalk.yellow(`Skipping ${flaw.src} (404)`));
          continue;
        } else {
          console.error(error);
          throw error;
        }
      }
    } else {
      newSrc = flaw.suggestion;
    }
    newRawHTML = replaceMatchesInText(flaw.src, newRawHTML, newSrc, {
      inAttribute: "src",
    });
    if (loud) {
      console.log(
        chalk.grey(
          `${phrasing} (${flaw.id}) image ${chalk.white.bold(
            flaw.src
          )} to ${chalk.white.bold(newSrc)}`
        )
      );
    }
  }

  // Finally, summarized what happened...
  if (newRawHTML !== document.rawHTML) {
    // It changed the raw HTML of the source. So deal with this.
    if (options.fixFlawsDryRun && options.fixFlawsVerbose) {
      console.log(
        chalk.yellow(
          `Would modify "${document.fileInfo.path}" from fixable flaws.`
        )
      );
    } else {
      Document.update(document.url, newRawHTML, document.metadata);
      if (options.fixFlawsVerbose) {
        console.log(
          chalk.green(
            `Modified "${chalk.bold(
              document.fileInfo.path
            )}" from fixable flaws.`
          )
        );
      }
    }
  }
}

function getImageminPlugin(fileName) {
  const extension = path.extname(fileName);
  if (extension === ".jpg" || extension === ".jpeg") {
    return imageminMozjpeg();
  }
  if (extension === ".png") {
    return imageminPngquant();
  }
  if (extension === ".gif") {
    return imageminGifsicle();
  }
  if (extension === ".svg") {
    return imageminSvgo();
  }
  throw new Error(`No imagemin plugin for ${extension}`);
}

module.exports = { injectFlaws, injectSectionFlaws, fixFixableFlaws };
