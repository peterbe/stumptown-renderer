const assert = require("assert").strict;
const fs = require("fs");
const path = require("path");
const stream = require("stream");
const { promisify } = require("util");

const chalk = require("chalk");
const mysql = require("mysql");
const cheerio = require("../build/monkeypatched-cheerio");

const {
  // CONTENT_ROOT,
  // CONTENT_ARCHIVED_ROOT,
  // CONTENT_TRANSLATED_ROOT,
  VALID_LOCALES,
  // Document,
  // Redirect,
  resolveFundamental,
} = require("../content");

const MAX_OPEN_FILES = 256;

// Contributors, from the revisions, that we deliberately ignore.
const IGNORABLE_CONTRIBUTORS = new Set(["mdnwebdocs-bot"]);

const OLD_LOCALE_PREFIXES = new Map([
  ["en", "en-US"],
  ["cn", "zh-CN"],
  ["zh_tw", "zh-TW"],
  ["zh", "zh-TW"],
  ["pt", "pt-PT"],
]);
// // Double check that every value of the old locale mappings
// // point to valid ones.
// assert(
//   [...OLD_LOCALE_PREFIXES.values()].every((x) =>
//     [...VALID_LOCALES.values()].includes(x)
//   )
// );

// Any slug that starts with one of these prefixes goes into a different
// folder; namely the archive folder.
// Case matters but 100% of Prod slugs are spelled like this. I.e.
// there's *no* slug that is something like this 'archiVe/Foo/Bar'.
const ARCHIVE_SLUG_ENGLISH_PREFIXES = [
  "Archive",
  "BrowserID",
  "Debugging",
  "Extensions",
  "Firefox_OS",
  "Garbage_MixedContentBlocker",
  "Gecko",
  "Hacking_Firefox",
  "Interfaces",
  "Mercurial",
  // "Mozilla",
  "Multi-Process_Architecture",
  "NSS",
  "nsS",
  "Performance",
  "Persona",
  "Preferences_System",
  "Sandbox",
  "SpiderMonkey",
  "Thunderbird",
  "Trash",
  "XML_Web_Services",
  "XUL",
  "XULREF",
  "Zones",

  // All the 'Mozilla/' prefixes, EXCEPT the ones we want to keep.
  // To see a list of the ones we keep, see
  // https://github.com/mdn/yari/issues/563
  "Mozilla/API",
  "Mozilla/About_omni.ja_(formerly_omni.jar)",
  "Mozilla/Accessibility",
  "Mozilla/Add-ons/AMO",
  "Mozilla/Add-ons/Add-on_Debugger",
  "Mozilla/Add-ons/Add-on_Manager",
  "Mozilla/Add-ons/Add-on_Repository",
  "Mozilla/Add-ons/Add-on_SDK",
  "Mozilla/Add-ons/Add-on_guidelines",
  "Mozilla/Add-ons/Adding_extensions_using_the_Windows_registry",
  "Mozilla/Add-ons/Bootstrapped_extensions",
  "Mozilla/Add-ons/Code_snippets",
  "Mozilla/Add-ons/Comparing_Extension_Toolchains",
  "Mozilla/Add-ons/Contact_us",
  "Mozilla/Add-ons/Creating_Custom_Firefox_Extensions_with_the_Mozilla_Build_System",
  "Mozilla/Add-ons/Creating_OpenSearch_plugins_for_Firefox",
  "Mozilla/Add-ons/Differences_between_desktop_and_Android",
  "Mozilla/Add-ons/Distribution",
  "Mozilla/Add-ons/Extension_Frequently_Asked_Questions",
  "Mozilla/Add-ons/Extension_Packaging",
  "Mozilla/Add-ons/Extension_etiquette",
  "Mozilla/Add-ons/Firefox_for_Android",
  "Mozilla/Add-ons/Hotfix",
  "Mozilla/Add-ons/How_to_convert_an_overlay_extension_to_restartless",
  "Mozilla/Add-ons/Index",
  "Mozilla/Add-ons/Inline_Options",
  "Mozilla/Add-ons/Install_Manifests",
  "Mozilla/Add-ons/Installing_extensions",
  "Mozilla/Add-ons/Interfacing_with_the_Add-on_Repository",
  "Mozilla/Add-ons/Legacy_Firefox_for_Android",
  "Mozilla/Add-ons/Legacy_add_ons",
  "Mozilla/Add-ons/Listing",
  "Mozilla/Add-ons/Overlay_Extensions",
  "Mozilla/Add-ons/Performance_best_practices_in_extensions",
  "Mozilla/Add-ons/Plugins",
  "Mozilla/Add-ons/SDK",
  "Mozilla/Add-ons/SeaMonkey_2",
  "Mozilla/Add-ons/Security_best_practices_in_extensions",
  "Mozilla/Add-ons/Setting_up_extension_development_environment",
  "Mozilla/Add-ons/Source_Code_Submission",
  "Mozilla/Add-ons/Submitting_an_add-on_to_AMO",
  "Mozilla/Add-ons/Techniques",
  "Mozilla/Add-ons/Themes",
  "Mozilla/Add-ons/Third_Party_Library_Usage",
  "Mozilla/Add-ons/Thunderbird",
  "Mozilla/Add-ons/Updates",
  "Mozilla/Add-ons/Webapps.jsm",
  "Mozilla/Add-ons/Why_develop_add-ons_For_Firefox",
  "Mozilla/Add-ons/Working_with_AMO",
  "Mozilla/Add-ons/Working_with_multiprocess_Firefox",
  "Mozilla/Adding_a_new_event",
  "Mozilla/Adding_a_new_style_property",
  "Mozilla/Adding_a_new_word_to_the_en-US_dictionary",
  "Mozilla/Adding_phishing_protection_data_providers",
  "Mozilla/An_introduction_to_hacking_Mozilla",
  "Mozilla/Android-specific_test_suites",
  "Mozilla/Application_cache_implementation_overview",
  "Mozilla/B2G_OS",
  "Mozilla/Benchmarking",
  "Mozilla/Bird_s_Eye_View_of_the_Mozilla_Framework",
  "Mozilla/Boot_to_Gecko",
  "Mozilla/Browser_chrome_tests",
  "Mozilla/Browser_security",
  "Mozilla/Bugzilla",
  "Mozilla/Building_Mozilla",
  "Mozilla/Building_SpiderMonkey_with_UBSan",
  "Mozilla/C++_Portability_Guide",
  "Mozilla/CSS",
  "Mozilla/Calendar",
  "Mozilla/Chat_Core",
  "Mozilla/Choosing_the_right_memory_allocator",
  "Mozilla/ChromeWorkers",
  "Mozilla/Chrome_Registration",
  "Mozilla/Command_Line_Options",
  "Mozilla/Connect",
  "Mozilla/Contact_us",
  "Mozilla/Continuous_integration",
  "Mozilla/Cookies_Preferences",
  "Mozilla/Cookies_in_Mozilla",
  "Mozilla/Cpp_portability_guide",
  "Mozilla/Creating_JavaScript_callbacks_in_components",
  "Mozilla/Creating_Mercurial_User_Repositories",
  "Mozilla/Creating_MozSearch_plugins",
  "Mozilla/Creating_a_Firefox_sidebar",
  "Mozilla/Creating_a_dynamic_status_bar_extension",
  "Mozilla/Creating_a_language_pack",
  "Mozilla/Creating_a_localized_Windows_installer_of_SeaMonkey",
  "Mozilla/Creating_a_login_manager_storage_module",
  "Mozilla/Creating_a_spell_check_dictionary_add-on",
  "Mozilla/Creating_reftest-based_unit_tests",
  "Mozilla/Creating_sandboxed_HTTP_connections",
  "Mozilla/Debugging",
  "Mozilla/Developer_Program",
  "Mozilla/Displaying_Place_information_using_views",
  "Mozilla/Errors",
  "Mozilla/Firefox/Australis_add-on_compat",
  "Mozilla/Firefox/Build_system",
  "Mozilla/Firefox/Building_Firefox_with_Rust_code",
  "Mozilla/Firefox/Developer_Edition",
  "Mozilla/Firefox/Enterprise_deployment",
  "Mozilla/Firefox/Firefox_ESR",
  "Mozilla/Firefox/Headless_mode",
  "Mozilla/Firefox/Index",
  "Mozilla/Firefox/Linux_compatibiility_matrix",
  "Mozilla/Firefox/Linux_compatibility_matrix",
  "Mozilla/Firefox/Multiple_profiles",
  "Mozilla/Firefox/Multiprocess_Firefox",
  "Mozilla/Firefox/Per-test_coverage",
  "Mozilla/Firefox/Performance_best_practices_for_Firefox_fe_engineers",
  "Mozilla/Firefox/Privacy",
  "Mozilla/Firefox/Security_best_practices_for_Firefox_front-end_engi",
  "Mozilla/Firefox/Site_identity_button",
  "Mozilla/Firefox/The_about_protocol",
  "Mozilla/Firefox/UI_considerations",
  "Mozilla/Firefox/Updating_add-ons_for_Firefox_10",
  "Mozilla/Firefox/Updating_add-ons_for_Firefox_5",
  "Mozilla/Firefox/Updating_add-ons_for_Firefox_6",
  "Mozilla/Firefox/Updating_add-ons_for_Firefox_8",
  "Mozilla/Firefox/Updating_add-ons_for_Firefox_9",
  "Mozilla/Firefox/Updating_extensions_for_Firefox_7",
  "Mozilla/Firefox/Versions/14",
  "Mozilla/Firefox/australis-add-on-compat-draft",
  "Mozilla/Firefox/releases/3/CSS_improvements",
  "Mozilla/FirefoxOS",
  "Mozilla/Firefox_1.5_for_Developers",
  "Mozilla/Firefox_25_for_developers",
  "Mozilla/Firefox_28_for_developers",
  "Mozilla/Firefox_Accounts",
  "Mozilla/Firefox_OS",
  "Mozilla/Firefox_Operational_Information_Database:_SQLite",
  "Mozilla/Firefox_addons_developer_guide",
  "Mozilla/Firefox_clone",
  "Mozilla/Firefox_for_Android",
  "Mozilla/Firefox_for_iOS",
  "Mozilla/Gecko",
  "Mozilla/Getting_from_Content_to_Layout",
  "Mozilla/Getting_started_with_IRC",
  "Mozilla/Git",
  "Mozilla/HTTP_cache",
  "Mozilla/Hacking_with_Bonsai",
  "Mozilla/How_Mozilla_determines_MIME_Types",
  "Mozilla/How_test_harnesses_work",
  "Mozilla/How_to_Turn_Off_Form_Autocompletion",
  "Mozilla/How_to_add_a_build-time_test",
  "Mozilla/How_to_get_a_process_dump_with_Windows_Task_Manager",
  "Mozilla/How_to_get_a_stacktrace_for_a_bug_report",
  "Mozilla/How_to_get_a_stacktrace_with_WinDbg",
  "Mozilla/How_to_implement_custom_autocomplete_search_component",
  "Mozilla/How_to_investigate_Disconnect_failures",
  "Mozilla/How_to_report_a_hung_Firefox",
  "Mozilla/IME_handling_guide",
  "Mozilla/IPDL",
  "Mozilla/Implementing_Pontoon_in_a_Mozilla_website",
  "Mozilla/Implementing_QueryInterface",
  "Mozilla/Implementing_download_resuming",
  "Mozilla/Infallible_memory_allocation",
  "Mozilla/Instantbird",
  "Mozilla/Integrated_authentication",
  "Mozilla/Internal_CSS_attributes",
  "Mozilla/Internationalized_domain_names_support_in_Mozilla",
  "Mozilla/Introduction",
  "Mozilla/JS_libraries",
  "Mozilla/JavaScript-DOM_Prototypes_in_Mozilla",
  "Mozilla/JavaScript_Tips",
  "Mozilla/JavaScript_code_modules",
  "Mozilla/Localization",
  "Mozilla/MFBT",
  "Mozilla/Marketplace",
  "Mozilla/MathML_Project",
  "Mozilla/Memory_Sanitizer",
  "Mozilla/Mercurial",
  "Mozilla/Mobile",
  "Mozilla/Mozilla_DOM_Hacking",
  "Mozilla/Mozilla_Framework_Based_on_Templates_(MFBT)",
  "Mozilla/Mozilla_Port_Blocking",
  "Mozilla/Mozilla_SVG_Project",
  "Mozilla/Mozilla_Web_Developer_Community",
  "Mozilla/Mozilla_Web_Developer_FAQ",
  "Mozilla/Mozilla_Web_Services_Security_Model",
  "Mozilla/Mozilla_development_strategies",
  "Mozilla/Mozilla_development_tools",
  "Mozilla/Mozilla_external_string_guide",
  "Mozilla/Mozilla_on_GitHub",
  "Mozilla/Mozilla_project_presentations",
  "Mozilla/Mozilla_quirks_mode_behavior",
  "Mozilla/Mozilla_style_system",
  "Mozilla/Multiple_Firefox_Profiles",
  "Mozilla/NSPR",
  "Mozilla/Namespaces",
  "Mozilla/Participating_in_the_Mozilla_project",
  "Mozilla/Performance",
  "Mozilla/Persona",
  "Mozilla/Phishing",
  "Mozilla/Preferences",
  "Mozilla/Productization_guide",
  "Mozilla/Profile_Manager",
  "Mozilla/Projects",
  "Mozilla/QA",
  "Mozilla/RAII_classes",
  "Mozilla/Redis_Tips",
  "Mozilla/Rust",
  "Mozilla/SeaMonkey",
  "Mozilla/Security",
  "Mozilla/Setting_up_an_update_server",
  "Mozilla/Signing_Mozilla_apps_for_Mac_OS_X",
  "Mozilla/Supported_build_configurations",
  "Mozilla/Task_graph",
  "Mozilla/Tech",
  "Mozilla/Test-Info",
  "Mozilla/Testing",
  "Mozilla/The_Mozilla_platform",
  "Mozilla/Thunderbird",
  "Mozilla/Toolkit_version_format",
  "Mozilla/Using_CXX_in_Mozilla_code",
  "Mozilla/Using_JS_in_Mozilla_code",
  "Mozilla/Using_Mozilla_code_in_other_projects",
  "Mozilla/Using_XML_Data_Islands_in_Mozilla",
  "Mozilla/Using_popup_notifications",
  "Mozilla/Using_tab-modal_prompts",
  "Mozilla/Using_the_Mozilla_source_server",
  "Mozilla/Using_the_Mozilla_symbol_server",
  "Mozilla/WebIDL_bindings",
  "Mozilla/Working_with_windows_in_chrome_code",
  "Mozilla/XMLHttpRequest_changes_for_Gecko_1.8",
  "Mozilla/XPCOM",
  "Mozilla/XPConnect",
  "Mozilla/XPI",
  "Mozilla/XRE",
  "Mozilla/Zombie_compartments",
  "Mozilla/httpd.js",
  "Mozilla/js-ctypes",
  "Mozilla/security-bugs-policy",
];

function makeURL(locale, slug) {
  return `/${locale}/docs/${encodeURI(slug)}`;
}

const redirectsToArchive = new Set();
const redirectFinalDestinations = new Map();
const archiveSlugPrefixes = [...ARCHIVE_SLUG_ENGLISH_PREFIXES];

function startsWithArchivePrefix(uri) {
  return archiveSlugPrefixes.some((prefix) =>
    uriToSlug(uri).startsWith(prefix)
  );
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
      // console.warn("Bad redirect (slug starts with /)", [row.locale, row.slug]);
      continue;
    }
    if (row.slug.includes("//")) {
      // console.warn("Bad redirect (slug contains '//')", [row.locale, row.slug]);
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
        // console.log("Bad redirect (from===to)", [fromUri]);
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
  `;
  const results = await query(localesSQL, queryArgs);

  let totalCount = 0;
  // console.log(`LOCALE\tDOCUMENTS`);
  // let countNonEnUs = 0;
  // let countEnUs = 0;
  for (const { count } of results) {
    // console.log(`${locale}\t${count.toLocaleString()}`);
    totalCount += count;
    // if (locale === "en-US") {
    //   countEnUs += count;
    // } else {
    //   countNonEnUs += count;
    // }
  }

  // if (countNonEnUs && countEnUs) {
  //   const nonEnUsPercentage = (100 * countNonEnUs) / (countNonEnUs + countEnUs);
  //   console.log(
  //     `(FYI ${countNonEnUs.toLocaleString()} (${nonEnUsPercentage.toFixed(
  //       1
  //     )}%) are non-en-US)`
  //   );
  // }

  return totalCount;
}

async function queryRevisionCount(query) {
  const localesSQL = `
    SELECT COUNT(*) AS count
    FROM wiki_revision
  `;
  const results = await query(localesSQL, queryArgs);

  let totalCount = 0;
  for (const { count } of results) {
    totalCount += count;
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

function getRedirectURL(html) {
  /**
   * Sometimes the HTML is like this:
   *   'REDIRECT <a class="redirect" href="/docs/http://wiki.commonjs.org/wiki/C_API">http://wiki.commonjs.org/wiki/C_API</a>'
   * and sometimes it's like this:
   *   'REDIRECT <a class="redirect" href="/en-US/docs/Web/API/WebGL_API">WebGL</a>'
   * and sometimes it's like this:
   *   'REDIRECT <a class="redirect" href="/en-US/docs/https://developer.mozilla.org/en-US/docs/Mozilla">Firefox Marketplace FAQ</a>'
   *
   * So we need the "best of both worlds".
   * */
  const $ = cheerio.load(html);
  for (const a of $("a[href].redirect").toArray()) {
    const hrefHref = $(a).attr("href");
    const hrefText = $(a).text();

    if (hrefHref.includes("http://")) {
      // Life's too short to accept these. Not only is it scary to even
      // consider sending our users to a http:// site but it's not
      // even working in Kuma because in Kuma redirects that
      // start with '/docs/http://..' end up in a string of redirects
      // and eventually fails with a 404.
      return null;
    }

    let href;
    if (hrefHref.startsWith("https://")) {
      href = hrefHref;
    } else if (
      hrefHref.includes("/https://") &&
      hrefText.startsWith("https://")
    ) {
      href = hrefText;
    } else if (hrefHref.includes("/https://")) {
      href = "https://" + hrefHref.split("https://")[1];
    } else {
      href = hrefHref;
    }
    if (href.startsWith("https://developer.mozilla.org")) {
      return new URL(href).pathname;
    } else {
      return href;
    }
  }
  return null;
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

async function queryRevisions(pool, options) {
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
  // const totalCount = await queryDocumentCount(query, constraintsSQL, queryArgs);
  const totalCount = await queryRevisionCount(query);

  const documentsSQL = `
    SELECT
      r.document_id,
      r.creator_id,
      r.created
    FROM wiki_revision r
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

async function processDocument(
  doc,
  { startClean },
  isArchive = false,
  localeWikiHistory,
  { usernames, contributors }
) {
  const { slug, locale, title } = doc;

  const docPath = path.join(locale, slug);
  if (startClean && allBuiltPaths.has(docPath)) {
    throw new Error(`${docPath} already exists!`);
  } else {
    // allBuiltPaths.add(docPath);
  }

  const meta = {
    title,
    slug,
    locale,
  };
  // if (doc.parent_slug) {
  //   assert(doc.parent_locale === "en-US");
  //   if (doc.parent_is_redirect) {
  //     const parentUri = makeURL(doc.parent_locale, doc.parent_slug);
  //     const finalUri = redirectFinalDestinations.get(parentUri);
  //     meta.translation_of = uriToSlug(finalUri);

  //     // What might have happened is the following timeline...
  //     // 1. Some writes an English document at SlugA
  //     // 2. Someone translates that SlugA English document into Japanese
  //     // 3. Someone decides to move that English document to SlugB, and makes
  //     //    SlugA redirect to SlugB.
  //     // 4. Someone translates that SlugB English document into Japanese
  //     // 5. Now you have 2 Japanese translations. One whose parent is
  //     //    SlugA and one whose parent is SlugB. But if you follow the redirects
  //     //    for SlugA you end up on SlugB and, voila! you now have 2 Japanese
  //     //    documents that claim to be a translation of SlugB.
  //     // This code here is why it sets the `.translation_of_original`.
  //     // More context on https://github.com/mdn/yari/issues/2034
  //     if (doc.parent_slug !== meta.translation_of) {
  //       meta.translation_of_original = doc.parent_slug;
  //     }
  //   } else {
  //     meta.translation_of = doc.parent_slug;
  //   }
  // }

  const wikiHistory = {
    modified: doc.modified.toISOString(),
  };

  console.log("CONTRIBUTORS:");
  console.log(contributors);
  console.log("DOC CONTRIBUTORS:");
  console.log(contributors[doc.id]);

  throw new Error("STOP");

  const docContributors = (contributors[doc.id] || [])
    .map((userId) => usernames[userId])
    .filter((username) => !IGNORABLE_CONTRIBUTORS.has(username));
  if (docContributors.length) {
    wikiHistory.contributors = docContributors;
  }

  localeWikiHistory.set(doc.slug, wikiHistory);
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

  const documents = await queryDocuments(pool, options);

  // const progressBar = !options.noProgressbar
  //   ? new ProgressBar({
  //       includeMemory: true,
  //     })
  //   : null;

  // if (!options.noProgressbar) {
  //   progressBar.init(documents.totalCount);
  // }

  documents.stream.on("error", (error) => {
    console.error("Querying documents failed with", error);
    process.exit(1);
  });

  let processedDocumentsCount = 0;
  let pendingDocuments = 0;

  // const redirects = {};
  // let improvedRedirects = 0;
  // let messedupRedirects = 0;
  // let discardedRedirects = 0;
  // let archivedRedirects = 0;
  // let fundamentalRedirects = 0;
  // let fastForwardedRedirects = 0;

  const allWikiHistory = new Map();
  // const archiveWikiHistory = new Map();

  for await (const row of documents.stream) {
    processedDocumentsCount++;

    while (pendingDocuments > MAX_OPEN_FILES) {
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    pendingDocuments++;
    (async () => {
      // const currentDocumentIndex = processedDocumentsCount;

      const absoluteUrl = makeURL(row.locale, row.slug);
      const isFundamentalRedirect = resolveFundamental(absoluteUrl).url;
      if (isFundamentalRedirect) {
        fundamentalRedirects++;
        return;
      }
      const isArchive = isArchiveDoc(row);
      if (row.is_redirect) {
        if (isArchive) {
          // This redirect or its parent is a page that will
          // be archived, or eventually arrives at a page that
          // will be archived. So just drop it!
          archivedRedirects++;
          return;
        }
        const redirect = processRedirect(row, absoluteUrl);
        if (!redirect) {
          discardedRedirects++;
          return;
        }
        if (redirect.url) {
          const finalUri = redirectFinalDestinations.get(absoluteUrl);
          if (redirect.url !== finalUri) {
            fastForwardedRedirects++;
          }
          redirects[absoluteUrl] = finalUri;
        }
        if (redirect.status == "mess") {
          messedupRedirects++;
        } else if (redirect.status == "improved") {
          improvedRedirects++;
        }
      } else {
        assert(row.locale);
        if (isArchive) {
          // if (!archiveWikiHistory.has(row.locale)) {
          //   archiveWikiHistory.set(row.locale, new Map());
          // }
        } else {
          if (!allWikiHistory.has(row.locale)) {
            allWikiHistory.set(row.locale, new Map());
          }
        }
        if (!isArchive) {
          // const { slug, locale, title } = row;
          await processDocument(
            row,
            options,
            false,
            allWikiHistory.get(row.locale),
            {
              usernames,
              contributors,
            }
          );
        }
      }
    })()
      .catch((err) => {
        console.log("An error occured during processing");
        console.error(err);
        // The slightest unexpected error should stop the importer immediately.
        process.exit(1);
      })
      .then(() => {
        pendingDocuments--;
      });
  }

  // if (!options.noProgressbar) {
  //   progressBar.stop();
  // }

  pool.end();

  await saveWikiHistory(allWikiHistory, false);
};

async function saveWikiHistory(allHistory, isArchive) {
  /**
   * The 'allHistory' is an object that looks like this:
   *
   * {'en-us': {
   *   'Games/Foo': {
   *     modified: '2019-01-21T12:13:14',
   *     contributors: ['Gregoor', 'peterbe', 'ryan']
   *   }
   *  }}
   *
   * But, it's a Map!
   *
   * Save these so that there's a _wikihistory.json in every locale folder.
   */

  const rows = [];

  for (const [locale, history] of allHistory) {
    const root = locale === "en-US" ? CONTENT_ROOT : CONTENT_TRANSLATED_ROOT;
    const localeFolder = path.join(root, locale.toLowerCase());
    let extraLocaleFolder = null;
    if (!isArchive && locale !== "en-US") {
      extraLocaleFolder = path.join(
        CONTENT_TRANSLATED_RENDERED_ROOT,
        locale.toLowerCase()
      );
    }
    const filePath = path.join(localeFolder, "_wikihistory.json");
    let extraFilePath = null;
    if (extraLocaleFolder) {
      extraFilePath = path.join(extraLocaleFolder, "_wikihistory.json");
    }
    const obj = Object.create(null);
    const keys = Array.from(history.keys());
    keys.sort();
    for (const key of keys) {
      obj[key] = history.get(key);
    }
    fs.writeFileSync(filePath, JSON.stringify(obj, null, 2));
    if (extraFilePath) {
      fs.writeFileSync(extraFilePath, JSON.stringify(obj, null, 2));
    }
  }
}
