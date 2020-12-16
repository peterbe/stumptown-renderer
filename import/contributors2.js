const assert = require("assert").strict;
const fs = require("fs");
const path = require("path");
const stream = require("stream");

const csv = require("fast-csv");
const mysql = require("mysql");
const cheerio = require("../build/monkeypatched-cheerio");

const { resolveFundamental } = require("../content");

const MAX_OPEN_FILES = 256;

// Any slug that starts with one of these prefixes goes into a different
// folder; namely the archive folder.
// Case matters but 100% of Prod slugs are spelled like this. I.e.
// there's *no* slug that is something like this 'archiVe/Foo/Bar'.
const ARCHIVE_SLUG_ENGLISH_PREFIXES = [
  "Experiment:",
  "Help:",
  "Help_talk:",
  "Project:",
  "Project_talk:",
  "Special:",
  "Talk:",
  "Template:",
  "Template_talk:",
  "User:",
  "User_talk:",
  "Trash",
  "azsdfvg",
  "doc_temp",
  "tempjenzed",
  "Junk",
  "Temp_input",
  "Admin:groovecoder",
  "temp_gamepad",
  "temp",
  "MDN/Doc_status",
  "MDN/Jobs",

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
  // "Mozilla/Projects",
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

const archiveSlugPrefixes = [...ARCHIVE_SLUG_ENGLISH_PREFIXES];

function startsWithArchivePrefix(uri) {
  return archiveSlugPrefixes.some((prefix) =>
    uriToSlug(uri).startsWith(prefix)
  );
}

async function queryRevisions(pool, options) {
  const sql = `
    SELECT
        d.locale,
        d.slug,
        r.created,
        u.username

    FROM wiki_revision r
    INNER JOIN wiki_document d ON r.document_id = d.id
    INNER JOIN auth_user u ON r.creator_id = u.id
    WHERE
      u.username != 'mdnwebdocs-bot'
    ORDER BY d.locale, r.created DESC
  `;

  return {
    // totalCount,
    stream: pool
      .query(sql)
      .stream({ highWaterMark: MAX_OPEN_FILES })
      // node MySQL uses custom streams which are not iterable. Piping it through a native stream fixes that
      .pipe(new stream.PassThrough({ objectMode: true })),
  };
}

function uriToSlug(uri) {
  if (uri.includes("/docs/")) {
    return uri.split("/docs/")[1];
  }
  return uri;
}

module.exports = async function runContributorsDump(options) {
  options = { locales: [], excludePrefixes: [], ...options };

  const pool = mysql.createPool(options.dbURL);

  const revisions = await queryRevisions(pool, options);
  const csvStream = csv.format({ headers: true });
  csvStream.pipe(process.stdout);

  const lastRevisionByDoc = new Map();
  for await (const row of revisions.stream) {
    const { locale, slug, created, username } = row;
    if (startsWithArchivePrefix(slug)) {
      continue;
    }
    // console.log(row);
    // console.log(
    //   `https://developer.mozilla.org/${locale}/docs/${slug}`,
    //   username,
    //   created
    // );
    const url = `https://developer.mozilla.org/${locale}/docs/${slug}`;
    // If the last revision, on this document, was by same exact user
    // then skip this revision.
    if (lastRevisionByDoc.get(url) === username) {
      continue;
    }
    lastRevisionByDoc.set(url, username);
    const writeRow = {
      LOCALE: locale,
      SLUG: slug,
      USERNAME: username,
      CREATED: created.toISOString(),
    };
    csvStream.write(writeRow);
  }
  csvStream.end();
  pool.end();
  //   console.log(allWikiHistory.get("sv-SE"));

  //   await saveWikiHistory(allWikiHistory, false);
};
