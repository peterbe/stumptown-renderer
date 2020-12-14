import fs from "fs";
import path from "path";

import jsesc from "jsesc";
import { renderToString } from "react-dom/server";
import cheerio from "./monkeypatched-cheerio";

import {
  GOOGLE_ANALYTICS_ACCOUNT,
  GOOGLE_ANALYTICS_DEBUG,
  SPEEDCURVE_LUX_ID,
  ALWAYS_NO_ROBOTS,
} from "../build/constants";

// When there are multiple options for a given language, this gives the
// preferred locale for that language (language => preferred locale).
const PREFERRED_LOCALE = {
  pt: "pt-PT",
  zh: "zh-CN",
};

function getHrefLang(locale, otherLocales) {
  // In most cases, just return the language code, removing the country
  // code if present (so, for example, 'en-US' becomes 'en').
  let hreflang = locale.split("-")[0];

  // Suppose the locale is one that is ambiguous, we need to fall back on a
  // a preferred one. For example, if the document is available in 'zh-CN' and
  // in 'zh-TW', we need to output something like this:
  //   <link rel=alternate hreflang=zh href=...>
  //   <link rel=alternate hreflang=zh-TW href=...>
  //
  // But other bother if both ambigious locale-to-hreflang are present.
  const preferred = PREFERRED_LOCALE[hreflang];
  if (preferred) {
    // e.g. `preferred===zh-CN` if hreflang was `zh`
    if (locale !== preferred) {
      // e.g. `locale===zh-TW`
      if (otherLocales.includes(preferred)) {
        // If the more preferred one was there, use the locale + region format.
        return locale;
      }
    }
  }
  return hreflang;
}

const lazy = (creator) => {
  let res;
  let processed = false;
  return () => {
    if (processed) return res;
    res = creator.apply(this, arguments);
    processed = true;
    return res;
  };
};

const clientBuildRoot = path.resolve(__dirname, "../../client/build");

const readBuildHTML = lazy(() => {
  const html = fs.readFileSync(
    path.join(clientBuildRoot, "index.html"),
    "utf8"
  );
  if (!html.includes('<div id="root"></div>')) {
    throw new Error(
      'The render depends on being able to inject into <div id="root"></div>'
    );
  }
  return html;
});

const getGoogleAnalyticsJS = lazy(() => {
  // The reason for the `path.join(__dirname, ".."` is because this file you're
  // reading gets compiled by Webpack into ssr/dist/*.js
  const dntHelperCode = fs
    .readFileSync(
      path.join(__dirname, "..", "mozilla.dnthelper.min.js"),
      "utf-8"
    )
    .trim();
  return `
  // Mozilla DNT Helper
  ${dntHelperCode}
  // only load GA if DNT is not enabled
  if (Mozilla && !Mozilla.dntEnabled()) {
      window.ga=window.ga||function(){(ga.q=ga.q||[]).push(arguments)};ga.l=+new Date;
      ga('create', '${GOOGLE_ANALYTICS_ACCOUNT}', 'mozilla.org');
      ga('set', 'anonymizeIp', true);
  }`.trim();
});

const getSpeedcurveJS = lazy(() => {
  return fs
    .readFileSync(
      // The file is called `...js.txt` so that Prettier never touches it.
      path.join(__dirname, "..", "speedcurve-lux-snippet.js.txt"),
      "utf-8"
    )
    .trim();
});

const extractWebFontURLs = lazy(() => {
  const urls = [];
  const manifest = JSON.parse(
    fs.readFileSync(path.join(clientBuildRoot, "asset-manifest.json"), "utf8")
  );
  for (const entrypoint of manifest.entrypoints) {
    if (!entrypoint.endsWith(".css")) continue;
    const css = fs.readFileSync(
      path.join(clientBuildRoot, entrypoint),
      "utf-8"
    );
    const generator = extractCSSURLs(
      css,
      (url) => url.endsWith(".woff2") && /Bold/i.test(url)
    );
    urls.push(...generator);
  }
  return urls;
});

function* extractCSSURLs(css, filterFunction) {
  for (const match of css.matchAll(/url\((.*?)\)/g)) {
    const url = match[1];
    if (filterFunction(url)) {
      yield url;
    }
  }
}

function serializeDocumentData(data) {
  return jsesc(JSON.stringify(data), {
    json: true,
    isScriptContext: true,
  });
}

export default function render(
  renderApp,
  { doc = null, pageNotFound = false } = {}
) {
  const buildHtml = readBuildHTML();
  const webfontURLs = extractWebFontURLs();
  const $ = cheerio.load(buildHtml);

  const rendered = renderToString(renderApp);

  let pageTitle = "MDN Web Docs"; // default
  let canonicalURL = "https://developer.mozilla.org";

  let pageDescription = "";

  if (pageNotFound) {
    pageTitle = `🤷🏽‍♀️ Page not found | ${pageTitle}`;
    const documentDataTag = `<script>window.__pageNotFound__ = true;</script>`;
    $("#root").after(documentDataTag);
  } else if (doc) {
    // Use the doc's title instead
    pageTitle = doc.pageTitle;
    canonicalURL += doc.mdn_url;

    if (doc.summary) {
      pageDescription = doc.summary;
    }

    const documentDataTag = `<script>window.__data__ = JSON.parse(${serializeDocumentData(
      doc
    )});</script>`;
    $("#root").after(documentDataTag);

    if (doc.other_translations) {
      const allOtherLocales = doc.other_translations.map((t) => t.locale);
      // Note, we also always include "self" as a locale. That's why we concat
      // this doc's locale plus doc.other_translations.
      const thisLocale = {
        locale: doc.locale,
        title: doc.title,
        url: doc.mdn_url,
      };
      for (const translation of [...doc.other_translations, thisLocale]) {
        // The locale used in `<link rel="alternate">` needs to be the ISO-639-1
        // code. For example, it's "en", not "en-US". And it's "sv" not "sv-SE".
        // See https://developers.google.com/search/docs/advanced/crawling/localized-versions?hl=en&visit_id=637411409912568511-3980844248&rd=1#language-codes
        $('<link rel="alternate">')
          .attr("title", translation.title)
          .attr("href", `https://developer.mozilla.org${translation.url}`)
          .attr("hreflang", getHrefLang(translation.locale, allOtherLocales))
          .insertAfter("title");
      }
    }
  }

  if (pageDescription) {
    // This overrides the default description. Also assumes there's always
    // one tag there already.
    $('meta[name="description"]').attr("content", pageDescription);
  }

  const robotsContent =
    ALWAYS_NO_ROBOTS || (doc && doc.noIndexing) || pageNotFound
      ? "noindex, nofollow"
      : "index, follow";
  $(`<meta name="robots" content="${robotsContent}">`).insertAfter(
    $("meta").eq(-1)
  );

  if (!pageNotFound) {
    $('link[rel="canonical"]').attr("href", canonicalURL);
  }

  if (SPEEDCURVE_LUX_ID) {
    // The snippet is always the same, if it's present, but the ID varies
    // See LUX settings here https://speedcurve.com/mozilla-add-ons/mdn/settings/lux/
    const speedcurveJS = getSpeedcurveJS();
    $("<script>").text(`\n${speedcurveJS}\n`).appendTo($("head"));
    $(
      `<script src="https://cdn.speedcurve.com/js/lux.js?id=${SPEEDCURVE_LUX_ID}" async defer crossorigin="anonymous"></script>`
    ).appendTo($("head"));
  }

  if (GOOGLE_ANALYTICS_ACCOUNT) {
    const googleAnalyticsJS = getGoogleAnalyticsJS();
    if (googleAnalyticsJS) {
      $("<script>").text(`\n${googleAnalyticsJS}\n`).appendTo($("head"));
      $(
        `<script async src="https://www.google-analytics.com/${
          GOOGLE_ANALYTICS_DEBUG ? "anaytics_debug" : "analytics"
        }.js"></script>`
      ).appendTo($("head"));
    }
  }

  $("title").text(pageTitle);
  const $title = $("title");
  $title.text(pageTitle);

  for (const webfontURL of webfontURLs) {
    $('<link rel="preload" as="font" type="font/woff2" crossorigin>')
      .attr("href", webfontURL)
      .insertAfter($title);
  }

  $("#root").html(rendered);

  // Every script tag that create-react-app inserts, make them defer
  $("body script[src]").attr("defer", "");

  // Move the script tags from the body to the head.
  // That way the browser can notice, and start downloading these files sooner
  // but they'll still be executed after the first render.
  $("body script[src]").appendTo("head");
  $("body script[src]").remove();

  return $.html();
}
