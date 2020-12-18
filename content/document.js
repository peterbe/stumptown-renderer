const fs = require("fs");
const path = require("path");

const fm = require("front-matter");
const glob = require("glob");
const yaml = require("js-yaml");

const {
  CONTENT_ARCHIVED_ROOT,
  CONTENT_TRANSLATED_ROOT,
  CONTENT_ROOT,
  VALID_LOCALES,
  ROOTS,
} = require("./constants");
const { getPopularities } = require("./popularities");
const { getWikiHistories } = require("./wikihistories");
const { getGitHistories } = require("./githistories");

const { buildURL, memoize, slugToFolder, execGit } = require("./utils");
const Redirect = require("./redirect");

function buildPath(localeFolder, slug) {
  return path.join(localeFolder, slugToFolder(slug));
}

const HTML_FILENAME = "index.html";
const getHTMLPath = (folder) => path.join(folder, HTML_FILENAME);

function updateWikiHistory(localeContentRoot, oldSlug, newSlug = null) {
  const all = JSON.parse(
    fs.readFileSync(path.join(localeContentRoot, "_wikihistory.json"))
  );
  if (oldSlug in all) {
    if (newSlug) {
      all[newSlug] = all[oldSlug];
    }
    delete all[oldSlug];
    fs.writeFileSync(
      path.join(localeContentRoot, "_wikihistory.json"),
      JSON.stringify(all, null, 2)
    );
  }
}

function extractLocale(folder) {
  // E.g. 'pr-br'
  const localeFolderName = folder.split(path.sep)[0].toLowerCase();
  // E.g. 'pt-BR'
  const locale = VALID_LOCALES.get(localeFolderName);
  // This checks that the extraction worked *and* that the locale found
  // really is in VALID_LOCALES *and* it ultimately returns the
  // locale as we prefer to spell it (e.g. 'pt-BR' not 'Pt-bR')
  if (!locale) {
    throw new Error(
      `Unable to figure out locale from ${folder} with ${localeFolderName}`
    );
  }
  return locale;
}

function saveHTMLFile(
  filePath,
  rawHTML,
  { slug, title, translation_of, tags, translation_of_original }
) {
  if (slug.includes("#")) {
    throw new Error("newSlug can not contain the '#' character");
  }
  const metadata = {
    title,
    slug,
  };
  if (tags) {
    metadata.tags = tags;
  }
  if (translation_of) {
    metadata.translation_of = translation_of;
  }
  if (translation_of_original) {
    // This will only make sense during the period where we're importing from
    // MySQL to disk. Once we're over that period we can delete this if-statement.
    metadata.translation_of_original = translation_of_original;
  }
  const combined = `---\n${yaml.safeDump(metadata)}---\n${rawHTML.trim()}\n`;
  fs.writeFileSync(filePath, combined);
}

function trimLineEndings(string) {
  return string
    .split("\n")
    .map((s) => s.trimEnd())
    .join("\n");
}

function urlToFolderPath(url) {
  const [, locale, , ...slugParts] = url.split("/");
  return path.join(locale.toLowerCase(), slugToFolder(slugParts.join("/")));
}

function create(html, metadata, root = null) {
  const folderPath = getFolderPath(metadata, root);

  fs.mkdirSync(folderPath, { recursive: true });

  saveHTMLFile(getHTMLPath(folderPath), trimLineEndings(html), metadata);
  return folderPath;
}

function getFolderPath(metadata, root = null) {
  if (!root) {
    root = metadata.locale === "en-US" ? CONTENT_ROOT : CONTENT_TRANSLATED_ROOT;
  }
  return buildPath(
    path.join(root, metadata.locale.toLowerCase()),
    metadata.slug
  );
}

function archive(
  renderedHTML,
  rawHTML,
  metadata,
  isTranslatedContent = false,
  root = null
) {
  if (!root) {
    root = isTranslatedContent
      ? CONTENT_TRANSLATED_ROOT
      : CONTENT_ARCHIVED_ROOT;
  }
  if (!CONTENT_ARCHIVED_ROOT) {
    throw new Error("Can't archive when CONTENT_ARCHIVED_ROOT is not set");
  }
  if (isTranslatedContent && !CONTENT_TRANSLATED_ROOT) {
    throw new Error(
      "Can't archive translated content when CONTENT_TRANSLATED_ROOT is not set"
    );
  }
  const folderPath = buildPath(
    path.join(root, metadata.locale.toLowerCase()),
    metadata.slug
  );

  fs.mkdirSync(folderPath, { recursive: true });

  // The `rawHTML` is only applicable in the importer when it saves
  // archived content. The archived content gets the *rendered* html
  // saved but by storing the raw html too we can potentially resurrect
  // the document if we decide to NOT archive it in the future.
  if (rawHTML) {
    fs.writeFileSync(
      path.join(folderPath, "raw.html"),
      trimLineEndings(rawHTML)
    );
  }

  saveHTMLFile(
    getHTMLPath(folderPath),
    trimLineEndings(renderedHTML),
    metadata
  );
  return folderPath;
}

const read = memoize((folder) => {
  let filePath = null;
  let root = null;
  for (const possibleRoot of ROOTS) {
    const possibleFilePath = path.join(possibleRoot, getHTMLPath(folder));
    if (fs.existsSync(possibleFilePath)) {
      root = possibleRoot;
      filePath = possibleFilePath;
      break;
    }
  }
  if (!filePath) {
    return;
  }
  if (filePath.includes(" ")) {
    throw new Error("Folder contains whitespace which is not allowed.");
  }
  if (filePath.includes("\u200b")) {
    throw new Error(
      `Folder contains zero width whitespace which is not allowed (${filePath})`
    );
  }
  const isTranslated =
    CONTENT_TRANSLATED_ROOT && filePath.startsWith(CONTENT_TRANSLATED_ROOT);
  const isArchive =
    isTranslated ||
    (CONTENT_ARCHIVED_ROOT && filePath.startsWith(CONTENT_ARCHIVED_ROOT));

  const rawContent = fs.readFileSync(filePath, "utf8");
  const {
    attributes: metadata,
    body: rawHTML,
    bodyBegin: frontMatterOffset,
  } = fm(rawContent);

  const locale = extractLocale(folder);
  const url = `/${locale}/docs/${metadata.slug}`;

  // The last-modified is always coming from the git logs. Independent of
  // which root it is.
  const gitHistory = getGitHistories(root, locale).get(
    path.relative(root, filePath)
  );
  let modified = (gitHistory && gitHistory.modified) || null;
  // Use the wiki histories for a list of legacy contributors.
  const wikiHistory = getWikiHistories(root, locale).get(url);
  if (!modified && wikiHistory && wikiHistory.modified) {
    modified = wikiHistory.modified;
  }
  const fullMetadata = {
    metadata: {
      ...metadata,
      locale,
      popularity: getPopularities().get(url) || 0.0,
      modified,
      contributors: wikiHistory ? wikiHistory.contributors : [],
    },
    url,
  };

  return {
    ...fullMetadata,
    ...{ rawHTML, rawContent },
    isArchive,
    isTranslated,
    fileInfo: {
      folder,
      path: filePath,
      frontMatterOffset,
      root,
    },
  };
});

function update(url, rawHTML, metadata) {
  const folder = urlToFolderPath(url);
  const indexPath = path.join(CONTENT_ROOT, getHTMLPath(folder));
  const document = read(folder);
  const oldSlug = document.metadata.slug;
  const newSlug = metadata.slug;
  const isNewSlug = oldSlug !== newSlug;

  if (
    isNewSlug ||
    document.rawHTML !== rawHTML ||
    document.metadata.title !== metadata.title
  ) {
    saveHTMLFile(indexPath, rawHTML, {
      ...document.metadata,
      ...metadata,
    });
    if (isNewSlug) {
      updateWikiHistory(
        path.join(CONTENT_ROOT, metadata.locale.toLowerCase()),
        oldSlug,
        newSlug
      );
    }
  }

  if (isNewSlug) {
    const locale = metadata.locale;
    const redirects = new Map();
    const url = buildURL(locale, oldSlug);
    for (const { metadata, rawHTML, fileInfo } of findChildren(url)) {
      const childLocale = metadata.locale;
      const oldChildSlug = metadata.slug;
      const newChildSlug = oldChildSlug.replace(oldSlug, newSlug);
      metadata.slug = newChildSlug;
      updateWikiHistory(
        path.join(CONTENT_ROOT, metadata.locale.toLowerCase()),
        oldChildSlug,
        newChildSlug
      );
      saveHTMLFile(fileInfo.path, rawHTML, metadata);
      redirects.set(
        buildURL(childLocale, oldChildSlug),
        buildURL(childLocale, newChildSlug)
      );
    }
    redirects.set(buildURL(locale, oldSlug), buildURL(locale, newSlug));
    const newFolderPath = buildPath(
      path.join(CONTENT_ROOT, locale.toLowerCase()),
      newSlug
    );
    const oldFolderPath = buildPath(
      path.join(CONTENT_ROOT, locale.toLowerCase()),
      oldSlug
    );

    execGit(["mv", oldFolderPath, newFolderPath]);
    Redirect.add(locale, [...redirects.entries()]);
  }
}

function findByURL(url, ...args) {
  const [bareURL, hash = ""] = url.split("#", 2);
  const doc = read(urlToFolderPath(bareURL), ...args);
  if (doc && hash) {
    return { ...doc, url: `${doc.url}#${hash}` };
  }
  return doc;
}

function findAll(
  { files, folderSearch } = { files: new Set(), folderSearch: null }
) {
  if (!(files instanceof Set)) throw new TypeError("'files' not a Set");
  if (folderSearch && typeof folderSearch !== "string")
    throw new TypeError("'folderSearch' not a string");

  // TODO: doesn't support archive content yet
  // console.warn("Currently hardcoded to only build 'en-us'");
  const filePaths = [];
  const roots = [];
  if (CONTENT_ARCHIVED_ROOT) {
    // roots.push({ path: CONTENT_ARCHIVED_ROOT, isArchive: true });
    roots.push(CONTENT_ARCHIVED_ROOT);
  }
  if (CONTENT_TRANSLATED_ROOT) {
    roots.push(CONTENT_TRANSLATED_ROOT);
  }
  roots.push(CONTENT_ROOT);
  console.log("Building roots:", roots);
  for (const root of roots) {
    filePaths.push(
      ...glob
        .sync(path.join(root, "**", HTML_FILENAME))
        .filter((filePath) => {
          // The 'files' set is either a list of absolute full paths or a
          // list of endings.
          // Why endings? Because it's highly useful when you use git and the
          // filepath might be relative to the git repo root.
          if (files.size) {
            if (files.has(filePath)) {
              return true;
            }
            for (const fp of files) {
              if (filePath.endsWith(fp)) {
                return true;
              }
            }
            return false;
          }
          if (folderSearch) {
            return filePath
              .replace(CONTENT_ROOT, "")
              .replace(HTML_FILENAME, "")
              .includes(folderSearch);
          }
          return true;
        })
        .map((filePath) => {
          return path.relative(root, path.dirname(filePath));
        })
    );
  }
  return {
    count: filePaths.length,
    iter: function* () {
      for (const filePath of filePaths) {
        yield read(filePath);
      }
    },
  };
}

function findChildren(url) {
  const folder = urlToFolderPath(url);
  const childPaths = glob.sync(
    path.join(CONTENT_ROOT, folder, "*", HTML_FILENAME)
  );
  return childPaths
    .map((childFilePath) =>
      path.relative(CONTENT_ROOT, path.dirname(childFilePath))
    )
    .map((folder) => read(folder));
}

function move(oldSlug, newSlug, locale, { dry = false } = {}) {
  const oldUrl = buildURL(locale, oldSlug);
  const doc = findByURL(oldUrl);
  if (!doc) {
    throw new Error(`document for ${oldSlug} does not exist`);
  }
  const newParentSlug = parentSlug(newSlug);
  // Otherwise we have a top level slug.
  if (newParentSlug) {
    const newParent = findByURL(buildURL(locale, newParentSlug));
    if (!newParent) {
      throw new Error(`Parent document for ${newSlug} does not exist`);
    }
  }

  const realOldSlug = doc.metadata.slug;
  const paris = [doc, ...findChildren(oldUrl)].map(({ metadata }) => [
    metadata.slug,
    metadata.slug.replace(realOldSlug, newSlug),
  ]);
  if (dry) {
    return paris;
  }

  doc.metadata.slug = newSlug;
  update(oldUrl, doc.rawHTML, doc.metadata);

  return paris;
}

function fileForSlug(slug, locale) {
  return getHTMLPath(getFolderPath({ slug, locale }));
}

function exists(slug, locale) {
  return Boolean(read(buildPath(locale.toLowerCase(), slug)));
}

function parentSlug(slug) {
  return slug.split("/").slice(0, -1).join("/");
}

function validate(slug, locale) {
  const errors = [];
  const file = buildPath(locale.toLowerCase(), slug);

  const doc = read(file);

  if (doc.metadata.slug.toLowerCase() !== slug.toLowerCase()) {
    errors.push("slug mismatch");
  }
  // Add more validations here.

  if (errors.length > 0) {
    throw new Error(errors.join(", "));
  }
}

function remove(
  slug,
  locale,
  { recursive = false, dry = false, redirect = "" } = {}
) {
  const url = buildURL(locale, slug);
  const { metadata, fileInfo } = findByURL(url) || {};
  if (!metadata) {
    throw new Error(`document does not exists: ${url}`);
  }

  const children = findChildren(url);
  if (children.length > 0 && (redirect || !recursive)) {
    throw new Error("unable to remove and redirect a document with children");
  }
  const docs = [slug, ...children.map(({ metadata }) => metadata.slug)];

  if (dry) {
    return docs;
  }

  for (const { metadata } of children) {
    const slug = metadata.slug;
    updateWikiHistory(
      path.join(CONTENT_ROOT, metadata.locale.toLowerCase()),
      slug
    );
  }

  if (redirect) {
    Redirect.add(locale, [[url, redirect]]);
  }

  execGit(["rm", "-r", path.dirname(fileInfo.path)]);

  updateWikiHistory(
    path.join(CONTENT_ROOT, metadata.locale.toLowerCase()),
    metadata.slug
  );

  return docs;
}

module.exports = {
  create,
  archive,
  read,
  update,
  exists,
  remove,
  move,
  validate,

  urlToFolderPath,
  getFolderPath,
  fileForSlug,
  parentSlug,

  findByURL,
  findAll,
  findChildren,
};
