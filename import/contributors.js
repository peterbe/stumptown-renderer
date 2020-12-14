const assert = require("assert").strict;
const fs = require("fs");
const path = require("path");
const stream = require("stream");
const { promisify } = require("util");

const chalk = require("chalk");
const mysql = require("mysql");

const {
  CONTENT_ROOT,
  CONTENT_ARCHIVED_ROOT,
  CONTENT_TRANSLATED_ROOT,
  VALID_LOCALES,
  Document,
  Redirect,
  resolveFundamental,
} = require("../content");

const MAX_OPEN_FILES = 256;

// Contributors, from the revisions, that we deliberately ignore.
const IGNORABLE_CONTRIBUTORS = new Set(["mdnwebdocs-bot"]);

// const OLD_LOCALE_PREFIXES = new Map([
//   ["en", "en-US"],
//   ["cn", "zh-CN"],
//   ["zh_tw", "zh-TW"],
//   ["zh", "zh-TW"],
//   ["pt", "pt-PT"],
// ]);
// // Double check that every value of the old locale mappings
// // point to valid ones.
// assert(
//   [...OLD_LOCALE_PREFIXES.values()].every((x) =>
//     [...VALID_LOCALES.values()].includes(x)
//   )
// );

function makeURL(locale, slug) {
  return `/${locale}/docs/${encodeURI(slug)}`;
}

async function populateRedirectInfo(pool, constraintsSQL, queryArgs) {
  // Populates two data structures: "redirectsToArchive", a set of URI's
  // that ultimately redirect to a page that will be archived, as well as
  // "redirectFinalDestinations", a mapping of the URI's of redirects
  // to the URI of their final destination.

  function extractFromChain(toUri, chainOfRedirects) {
    // Recursive function that builds the set of redirects to
    // archive, as well as the map that provides the final
    // destination of each redirect that we'll keep.
    const isInfiniteLoop = chainOfRedirects.has(toUri);
    if (!isInfiniteLoop) {
      const nextUri = redirects.get(toUri);
      if (nextUri) {
        return extractFromChain(nextUri, chainOfRedirects.add(toUri));
      }
    }
    // Is the final destination meant to be archived?
    if (isInfiniteLoop || startsWithArchivePrefix(toUri)) {
      for (const uri of chainOfRedirects) {
        // All of these URI's ultimately redirect to a page that
        // will be archived or are involved in an inifinite loop.
        // We'll only add to the set of "redirectsToArchive" those
        // that are not already covered by "archiveSlugPrefixes".
        if (!startsWithArchivePrefix(uri)) {
          // console.log(`adding to archive: ${uri}`);
          redirectsToArchive.add(uri);
        }
      }
    }
    // Let's record the final destination of each URI in the chain.
    for (const uri of chainOfRedirects) {
      redirectFinalDestinations.set(uri, toUri);
    }
  }

  const redirectDocs = await queryRedirects(pool, constraintsSQL, queryArgs);

  redirectDocs.on("error", (error) => {
    console.error("Querying redirect documents failed with", error);
    process.exit(1);
  });

  const redirects = new Map();

  for await (const row of redirectDocs) {
    if (row.slug.startsWith("/")) {
      console.warn("Bad redirect (slug starts with /)", [row.locale, row.slug]);
      continue;
    }
    if (row.slug.includes("//")) {
      console.warn("Bad redirect (slug contains '//')", [row.locale, row.slug]);
      continue;
    }
    let redirect = null;
    const fromUri = makeURL(row.locale, row.slug);
    const fundamentalRedirect = resolveFundamental(fromUri).url;
    if (fundamentalRedirect) {
      redirect = fundamentalRedirect;
    } else {
      const processedRedirectUrl = (processRedirect(row, fromUri) || {}).url;
      const fundamentalTargetRedirect =
        processedRedirectUrl && resolveFundamental(processedRedirectUrl).url;
      redirect = fundamentalTargetRedirect || processedRedirectUrl;
    }
    if (redirect) {
      if (fromUri.toLowerCase() === redirect.toLowerCase()) {
        console.log("Bad redirect (from===to)", [fromUri]);
      } else {
        redirects.set(fromUri, redirect);
      }
    }
  }

  for (const [fromUri, toUri] of redirects.entries()) {
    extractFromChain(toUri, new Set([fromUri]));
  }
}

function getSQLConstraints(
  { alias = null, parentAlias = null, includeDeleted = false } = {},
  options
) {
  // Yeah, this is ugly but it bloody works for now.
  const a = alias ? `${alias}.` : "";
  const extra = [];
  const queryArgs = [];
  // Always exclude these. These are straggler documents that don't yet
  // have a revision
  extra.push(`${a}current_revision_id IS NOT NULL`);
  // There aren't many but these get excluded in kuma anyway.
  extra.push(`${a}html <> ''`);

  if (!includeDeleted) {
    extra.push(`${a}deleted = false`);
  }
  const { locales, excludePrefixes } = options;
  if (locales.length) {
    extra.push(`${a}locale in (?)`);
    queryArgs.push(locales);
  }
  if (excludePrefixes.length) {
    extra.push(
      `NOT (${excludePrefixes.map(() => `${a}slug LIKE ?`).join(" OR ")})`
    );
    queryArgs.push(...excludePrefixes.map((s) => `${s}%`));
    if (parentAlias) {
      extra.push(
        `((${parentAlias}.slug IS NULL) OR NOT (${excludePrefixes
          .map(() => `${parentAlias}.slug LIKE ?`)
          .join(" OR ")}))`
      );
      queryArgs.push(...excludePrefixes.map((s) => `${s}%`));
    }
  }

  return {
    constraintsSQL: ` WHERE ${extra.join(" AND ")}`,
    queryArgs,
  };
}

async function queryContributors(query, options) {
  const [contributors, usernames] = await Promise.all([
    (async () => {
      console.log("Going to fetch ALL contributor *mappings*");
      const { constraintsSQL, queryArgs } = getSQLConstraints(
        {
          includeDeleted: true,
          alias: "d",
        },
        options
      );
      const documentCreators = await query(
        `
          SELECT r.document_id, r.creator_id
          FROM wiki_revision r
          INNER JOIN wiki_document d ON r.document_id = d.id
          ${constraintsSQL}
          ORDER BY r.created DESC
        `,
        queryArgs
      );
      const contributors = {};
      for (const { document_id, creator_id } of documentCreators) {
        if (!(document_id in contributors)) {
          contributors[document_id] = []; // Array because order matters
        }
        if (!contributors[document_id].includes(creator_id)) {
          contributors[document_id].push(creator_id);
        }
      }
      return contributors;
    })(),
    (async () => {
      console.log("Going to fetch ALL contributor *usernames*");
      const users = await query("SELECT id, username FROM auth_user");
      const usernames = {};
      for (const user of users) {
        usernames[user.id] = user.username;
      }
      return usernames;
    })(),
  ]);

  return { contributors, usernames };
}

async function queryDocumentCount(query, constraintsSQL, queryArgs) {
  const localesSQL = `
    SELECT w.locale, COUNT(*) AS count
    FROM wiki_document w
    LEFT OUTER JOIN wiki_document p ON w.parent_id = p.id
    ${constraintsSQL}
    GROUP BY w.locale
    ORDER BY count DESC
  `;
  const results = await query(localesSQL, queryArgs);

  let totalCount = 0;
  console.log(`LOCALE\tDOCUMENTS`);
  let countNonEnUs = 0;
  let countEnUs = 0;
  for (const { count, locale } of results) {
    console.log(`${locale}\t${count.toLocaleString()}`);
    totalCount += count;
    if (locale === "en-US") {
      countEnUs += count;
    } else {
      countNonEnUs += count;
    }
  }

  if (countNonEnUs && countEnUs) {
    const nonEnUsPercentage = (100 * countNonEnUs) / (countNonEnUs + countEnUs);
    console.log(
      `(FYI ${countNonEnUs.toLocaleString()} (${nonEnUsPercentage.toFixed(
        1
      )}%) are non-en-US)`
    );
  }

  return totalCount;
}

async function queryRedirects(pool, constraintsSQL, queryArgs) {
  const documentsSQL = `
    SELECT
      w.html,
      w.slug,
      w.locale,
      w.is_redirect
    FROM wiki_document w
    LEFT OUTER JOIN wiki_document p ON w.parent_id = p.id
    ${constraintsSQL} AND w.is_redirect = true
  `;

  return pool
    .query(documentsSQL, queryArgs)
    .stream({ highWaterMark: MAX_OPEN_FILES })
    .pipe(new stream.PassThrough({ objectMode: true }));
}

async function addLocalizedArchiveSlugPrefixes(
  query,
  constraintsSQL,
  queryArgs
) {
  // Adds all of the localized versions of the English archive
  // slug prefixes to "archiveSlugPrefixes".
  const slugsSQL = `
    SELECT
      w.slug
    FROM wiki_document w
    INNER JOIN wiki_document p ON w.parent_id = p.id
    ${constraintsSQL} AND p.slug in (?)
  `;

  queryArgs.push(ARCHIVE_SLUG_ENGLISH_PREFIXES);

  const slugsFromLocales = await query(slugsSQL, queryArgs);

  for (const slug of new Set(slugsFromLocales)) {
    if (!archiveSlugPrefixes.includes(slug)) {
      archiveSlugPrefixes.push(slug);
    }
  }
}

async function queryDocuments(pool, options) {
  const { constraintsSQL, queryArgs } = getSQLConstraints(
    {
      alias: "w",
      parentAlias: "p",
    },
    options
  );

  const query = promisify(pool.query).bind(pool);

  await addLocalizedArchiveSlugPrefixes(query, constraintsSQL, queryArgs);
  await populateRedirectInfo(pool, constraintsSQL, queryArgs);
  const totalCount = await queryDocumentCount(query, constraintsSQL, queryArgs);

  const documentsSQL = `
    SELECT
      w.id,
      w.title,
      w.slug,
      w.locale,
      w.is_redirect,
      w.html,
      w.rendered_html,
      w.modified,
      p.id AS parent_id,
      p.slug AS parent_slug,
      p.locale AS parent_locale,
      p.modified AS parent_modified,
      p.is_redirect AS parent_is_redirect
    FROM wiki_document w
    LEFT OUTER JOIN wiki_document p ON w.parent_id = p.id
    ${constraintsSQL}
  `;

  return {
    totalCount,
    stream: pool
      .query(documentsSQL, queryArgs)
      .stream({ highWaterMark: MAX_OPEN_FILES })
      // node MySQL uses custom streams which are not iterable. Piping it through a native stream fixes that
      .pipe(new stream.PassThrough({ objectMode: true })),
  };
}

async function queryDocumentTags(query, options) {
  const { constraintsSQL, queryArgs } = getSQLConstraints(
    {
      alias: "w",
    },
    options
  );
  const sql = `
    SELECT
      w.id,
      t.name
    FROM wiki_document w
    INNER JOIN wiki_taggeddocument wt ON wt.content_object_id = w.id
    INNER JOIN wiki_documenttag t ON t.id = wt.tag_id
    ${constraintsSQL}
  `;

  console.log("Going to fetch ALL document tags");
  const results = await query(sql, queryArgs);
  const tags = {};
  for (const row of results) {
    if (!(row.id in tags)) {
      tags[row.id] = [];
    }
    tags[row.id].push(row.name);
  }
  return tags;
}

async function withTimer(label, fn) {
  console.time(label);
  const result = await fn();
  console.timeEnd(label);
  return result;
}

function isArchiveDoc(row) {
  return (
    archiveSlugPrefixes.some(
      (prefix) =>
        row.slug.startsWith(prefix) ||
        (row.parent_slug && row.parent_slug.startsWith(prefix))
    ) ||
    (row.is_redirect && isArchiveRedirect(makeURL(row.locale, row.slug))) ||
    (row.parent_slug &&
      row.parent_is_redirect &&
      isArchiveRedirect(makeURL(row.parent_locale, row.parent_slug)))
  );
}

function uriToSlug(uri) {
  if (uri.includes("/docs/")) {
    return uri.split("/docs/")[1];
  }
  return uri;
}

const REDIRECT_HTML = "REDIRECT <a ";

// Return either 'null' or an object that looks like this:
//
//  { url: redirectURL, status: null };
//  or
//  { url: null, status: "mess" }
//  or
//  { url: fixedRedirectURL, status: "improved" }
//
// So basically, if it's an object it has the keys 'url' and 'status'.
function processRedirect(doc, absoluteURL) {
  if (!doc.html.includes(REDIRECT_HTML)) {
    console.log(`${doc.locale}/${doc.slug} is redirect but no REDIRECT_HTML`);
    return null;
  }

  let redirectURL = getRedirectURL(doc.html);
  if (!redirectURL) {
    return null;
  }

  if (redirectURL.includes("://")) {
    if (
      redirectURL.includes("developer.mozilla.org") ||
      redirectURL.includes("/http")
    ) {
      console.warn(
        "WEIRD REDIRECT:",
        redirectURL,
        "  FROM  ",
        `https://developer.mozilla.org${encodeURI(absoluteURL)}`,
        doc.html
      );
    }
    // Generally, leave external redirects untouched
    return { url: redirectURL, status: null };
  }

  return postProcessRedirectURL(redirectURL);
}

function postProcessRedirectURL(redirectURL) {
  if (redirectURL === "/") {
    return { url: "/en-US/", status: "improved" };
  }
  const split = redirectURL.split("/");
  let locale;
  if (split[1] === "docs") {
    // E.g. /docs/en/JavaScript
    locale = split[2];
  } else if (split[2] == "docs") {
    // E.g. /en/docs/HTML
    locale = split[1];
  } else if (!split.includes("docs")) {
    // E.g. /en-us/Addons
    locale = split[1];
  } else {
    // That's some seriously messed up URL!
    locale = null;
  }

  if (locale) {
    const localeLC = locale.toLowerCase();
    if (OLD_LOCALE_PREFIXES.has(localeLC)) {
      locale = OLD_LOCALE_PREFIXES.get(localeLC);
    } else if (VALID_LOCALES.has(localeLC)) {
      locale = VALID_LOCALES.get(localeLC);
    } else {
      // If the URL contains no recognizable locale that can be cleaned up
      // we have to assume 'en-US'. There are so many redirect URLs
      // in MySQL that look like this: '/docs/Web/JavaScript...'
      // And for them we have to assume it's '/en-US/docs/Web/JavaScript...'
      locale = "en-US";
      split.splice(1, 0, locale);
    }
  }

  // No valid locale found. We have to try to fix that manually.
  if (!locale) {
    console.log(split, { redirectURL });
    throw new Error("WHAT THE HELL?");
  }

  // E.g. '/en/' or '/en-uS/' or '/fr'
  if (!split.includes("docs") && split.filter((x) => x).length === 1) {
    return { url: `/${locale}/`, status: null };
  }

  // E.g. '/en/docs/Foo' or '/en-us/docs/Foo' - in other words; perfect
  // but the locale might need to be corrected
  if (split[2] === "docs") {
    if (locale !== split[1]) {
      split[1] = locale;
      return { url: split.join("/"), status: "improved" };
    }
    return { url: split.join("/"), status: null };
  }

  // E.g. '/en-US/Foo/Bar' or '/en/Foo/Bar'
  if (!split.includes("docs")) {
    // The locale is valid but it's just missing the '/docs/' part
    split[1] = locale;
    split.splice(2, 0, "docs");
    return { url: split.join("/"), status: "improved" };
  }

  // E.g. '/docs/en-uS/Foo' or '/docs/cn/Foo'
  if (split[1] === "docs") {
    split.splice(2, 1); // remove the local after '/docs/'
    split.splice(1, 0, locale); // put the (correct) locale in before
    return { url: split.join("/"), status: "improved" };
  }

  return { url: null, status: "mess" };
}

module.exports = async function runContributorsDump(options) {
  options = { locales: [], excludePrefixes: [], ...options };

  const pool = mysql.createPool(options.dbURL);

  console.log(
    `Going to try to connect to ${pool.config.connectionConfig.database} (locales=${options.locales})`
  );

  const query = promisify(pool.query).bind(pool);
  const x = await queryContributors(query, options);
  console.log(Object.keys(x));
  // const [{ usernames, contributors }, tags] = await Promise.all([
  //   withTimer("Time to fetch all contributors", () =>

  //   ),
  //   withTimer("Time to fetch all document tags", () =>
  //     queryDocumentTags(query, options)
  //   ),
  // ]);

  // let startTime = Date.now();

  // const documents = await queryDocuments(pool, options);

  // const progressBar = !options.noProgressbar
  //   ? new ProgressBar({
  //       includeMemory: true,
  //     })
  //   : null;

  // if (!options.noProgressbar) {
  //   progressBar.init(documents.totalCount);
  // }

  // documents.stream.on("error", (error) => {
  //   console.error("Querying documents failed with", error);
  //   process.exit(1);
  // });

  // let processedDocumentsCount = 0;
  // let pendingDocuments = 0;

  // const redirects = {};
  // let improvedRedirects = 0;
  // let messedupRedirects = 0;
  // let discardedRedirects = 0;
  // let archivedRedirects = 0;
  // let fundamentalRedirects = 0;
  // let fastForwardedRedirects = 0;

  // const allWikiHistory = new Map();
  // const archiveWikiHistory = new Map();

  // for await (const row of documents.stream) {
  //   processedDocumentsCount++;

  //   while (pendingDocuments > MAX_OPEN_FILES) {
  //     await new Promise((resolve) => setTimeout(resolve, 500));
  //   }

  //   pendingDocuments++;
  //   (async () => {
  //     const currentDocumentIndex = processedDocumentsCount;
  //     // Only update (and repaint) every 20th time.
  //     // Make it much more than every 1 time or else it'll flicker.
  //     if (progressBar && currentDocumentIndex % 20 == 0) {
  //       progressBar.update(currentDocumentIndex);
  //     }

  //     const absoluteUrl = makeURL(row.locale, row.slug);
  //     const isFundamentalRedirect = resolveFundamental(absoluteUrl).url;
  //     if (isFundamentalRedirect) {
  //       fundamentalRedirects++;
  //       return;
  //     }
  //     const isArchive = isArchiveDoc(row);
  //     if (row.is_redirect) {
  //       if (isArchive) {
  //         // This redirect or its parent is a page that will
  //         // be archived, or eventually arrives at a page that
  //         // will be archived. So just drop it!
  //         archivedRedirects++;
  //         return;
  //       }
  //       const redirect = processRedirect(row, absoluteUrl);
  //       if (!redirect) {
  //         discardedRedirects++;
  //         return;
  //       }
  //       if (redirect.url) {
  //         const finalUri = redirectFinalDestinations.get(absoluteUrl);
  //         if (redirect.url !== finalUri) {
  //           fastForwardedRedirects++;
  //         }
  //         redirects[absoluteUrl] = finalUri;
  //       }
  //       if (redirect.status == "mess") {
  //         messedupRedirects++;
  //       } else if (redirect.status == "improved") {
  //         improvedRedirects++;
  //       }
  //     } else {
  //       assert(row.locale);
  //       if (isArchive) {
  //         if (!archiveWikiHistory.has(row.locale)) {
  //           archiveWikiHistory.set(row.locale, new Map());
  //         }
  //       } else {
  //         if (!allWikiHistory.has(row.locale)) {
  //           allWikiHistory.set(row.locale, new Map());
  //         }
  //       }
  //       await processDocument(
  //         row,
  //         options,
  //         isArchive,
  //         isArchive
  //           ? archiveWikiHistory.get(row.locale)
  //           : allWikiHistory.get(row.locale),
  //         {
  //           usernames,
  //           contributors,
  //           tags,
  //         }
  //       );
  //     }
  //   })()
  //     .catch((err) => {
  //       console.log("An error occured during processing");
  //       console.error(err);
  //       // The slightest unexpected error should stop the importer immediately.
  //       process.exit(1);
  //     })
  //     .then(() => {
  //       pendingDocuments--;
  //     });
  // }

  // if (!options.noProgressbar) {
  //   progressBar.stop();
  // }

  pool.end();
};
