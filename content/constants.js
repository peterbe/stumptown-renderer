const fs = require("fs");
const path = require("path");

require("dotenv").config({
  path: path.join(__dirname, "..", process.env.ENV_FILE || ".env"),
});

const CONTENT_ROOT = (() => {
  const root =
    process.env.CONTENT_ROOT || path.join(__dirname, "..", "content", "files");
  // If the CONTENT_ROOT wasn't an absolute (existing) directory, it's
  // assumed to be relative to the project root.
  if (!fs.existsSync(root)) {
    return path.join(__dirname, "..", root);
  }
  return root;
})();

const CONTENT_ARCHIVE_ROOT = process.env.CONTENT_ARCHIVE_ROOT
  ? path.join(__dirname, "..", process.env.CONTENT_ARCHIVE_ROOT)
  : null;

// Make a combined array of all truthy roots. This way, you don't
// need to constantly worry about CONTENT_ARCHIVE_ROOT potentially being
// null.
const ROOTS = [CONTENT_ROOT];
if (CONTENT_ARCHIVE_ROOT) {
  ROOTS.push(CONTENT_ARCHIVE_ROOT);
}

const VALID_LOCALES = new Map(
  [
    "ar",
    "bg",
    "bm",
    "bn",
    "ca",
    "de",
    "el",
    "en-US",
    "es",
    "fa",
    "fi",
    "fr",
    "he",
    "hi-IN",
    "hu",
    "id",
    "it",
    "ja",
    "kab",
    "ko",
    "ms",
    "my",
    "nl",
    "pl",
    "pt-BR",
    "pt-PT",
    "ru",
    "sv-SE",
    "th",
    "tr",
    "uk",
    "vi",
    "zh-CN",
    "zh-TW",
  ].map((x) => [x.toLowerCase(), x])
);

module.exports = {
  CONTENT_ROOT,
  CONTENT_ARCHIVE_ROOT,
  ROOTS,
  VALID_LOCALES,
};
