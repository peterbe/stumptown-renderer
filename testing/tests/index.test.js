const fs = require("fs");
const path = require("path");

const cheerio = require("cheerio");

const buildRoot = path.join("..", "client", "build");

test("content built foo page", () => {
  expect(fs.existsSync(buildRoot)).toBeTruthy();

  const builtFolder = path.join(buildRoot, "en-us", "docs", "web", "foo");
  expect(fs.existsSync(builtFolder)).toBeTruthy();

  const jsonFile = path.join(builtFolder, "index.json");
  expect(fs.existsSync(jsonFile)).toBeTruthy();

  // We should be able to read it and expect certain values
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  expect(doc.title).toBe("<foo>: A test tag");
  expect(doc.pageTitle).toBe(`${doc.title} | MDN`);
  expect(doc.summary).toBe("This becomes the summary.");
  expect(doc.mdn_url).toBe("/en-US/docs/Web/Foo");
  expect(new Date(doc.modified)).toBeTruthy();
  expect(doc.source).toBeTruthy();

  expect(doc.flaws.macros.length).toBe(6);
  expect(doc.flaws.macros[0].name).toBe("MacroRedirectedLinkError");
  expect(doc.flaws.macros[0].macroSource).toBe("{{CSSxRef('dumber')}}");
  expect(doc.flaws.macros[0].line).toBe(8);
  expect(doc.flaws.macros[0].column).toBe(7);
  expect(doc.flaws.macros[0].sourceContext).toEqual(
    expect.stringContaining("<li>{{CSSxRef('dumber')}}</li>")
  );
  expect(doc.flaws.macros[0].redirectInfo).toBeDefined();
  expect(doc.flaws.macros[0].redirectInfo.current).toBe("dumber");
  expect(doc.flaws.macros[0].redirectInfo.suggested).toBe("number");
  expect(doc.flaws.macros[0].filepath).toMatch(
    /\/en-us\/web\/fixable_flaws\/index\.html$/
  );
  expect(doc.flaws.macros[1].name).toBe("MacroRedirectedLinkError");
  expect(doc.flaws.macros[1].macroSource).toBe(
    '{{htmlattrxref("href", "anchor")}}'
  );
  expect(doc.flaws.macros[1].line).toBe(9);
  expect(doc.flaws.macros[1].column).toBe(7);
  expect(doc.flaws.macros[1].sourceContext).toEqual(
    expect.stringContaining('<li>{{htmlattrxref("href", "anchor")}}</li>')
  );
  expect(doc.flaws.macros[1].redirectInfo).toBeDefined();
  expect(doc.flaws.macros[1].redirectInfo.current).toBe("anchor");
  expect(doc.flaws.macros[1].redirectInfo.suggested).toBe("a");
  expect(doc.flaws.macros[1].filepath).toMatch(
    /\/en-us\/web\/fixable_flaws\/index\.html$/
  );
  expect(doc.flaws.macros[2].name).toBe("MacroBrokenLinkError");
  expect(doc.flaws.macros[2].macroSource).toBe(
    '{{CSSxRef("will-never-be-fixable")}}'
  );
  expect(doc.flaws.macros[2].line).toBe(10);
  expect(doc.flaws.macros[2].column).toBe(7);
  expect(doc.flaws.macros[2].sourceContext).toEqual(
    expect.stringContaining('<li>{{CSSxRef("will-never-be-fixable")}}</li>')
  );
  expect(doc.flaws.macros[2].filepath).toMatch(
    /\/en-us\/web\/fixable_flaws\/index\.html$/
  );
  expect(doc.flaws.macros[3].name).toBe("MacroRedirectedLinkError");
  expect(doc.flaws.macros[3].macroSource).toBe("{{CSSxRef('dumber')}}");
  expect(doc.flaws.macros[3].line).toBe(11);
  expect(doc.flaws.macros[3].column).toBe(7);
  expect(doc.flaws.macros[3].sourceContext).toEqual(
    expect.stringContaining("<li>{{CSSxRef('dumber')}} second time!</li>")
  );
  expect(doc.flaws.macros[3].redirectInfo).toBeDefined();
  expect(doc.flaws.macros[3].redirectInfo.current).toBe("dumber");
  expect(doc.flaws.macros[3].redirectInfo.suggested).toBe("number");
  expect(doc.flaws.macros[3].filepath).toMatch(
    /\/en-us\/web\/fixable_flaws\/index\.html$/
  );
  expect(doc.flaws.macros[4].name).toBe("MacroExecutionError");
  expect(doc.flaws.macros[4].errorStack).toEqual(
    expect.stringContaining(
      '/en-us/docs/web/fubar references /en-us/docs/does-not-exist (derived from "does-not-exist"), which does not exist'
    )
  );
  expect(doc.flaws.macros[4].line).toBe(10);
  expect(doc.flaws.macros[4].column).toBe(6);
  // Check that the line numbers in the source context have been adjusted by the offset.
  expect(doc.flaws.macros[4].sourceContext).toEqual(
    expect.stringContaining('<div>{{page("does-not-exist")}}</div>')
  );
  expect(doc.flaws.macros[4].filepath).toMatch(
    /\/en-us\/web\/fubar\/index\.html$/
  );
  expect(doc.flaws.macros[5].name).toBe("MacroExecutionError");
  expect(doc.flaws.macros[5].errorStack).toEqual(
    expect.stringContaining(
      "/en-us/docs/web/fubar references /en-us/docs/does/not/exist, which does not exist"
    )
  );
  expect(doc.flaws.macros[5].line).toBe(11);
  expect(doc.flaws.macros[5].column).toBe(6);
  // Check that the line numbers in the source context have been adjusted by the offset.
  expect(doc.flaws.macros[5].sourceContext).toEqual(
    expect.stringContaining(
      `<div>{{ EmbedLiveSample('example', '300', '300', "", "does/not/exist") }}</div>`
    )
  );
  expect(doc.flaws.macros[5].filepath).toMatch(
    /\/en-us\/web\/fubar\/index\.html$/
  );

  const htmlFile = path.join(builtFolder, "index.html");
  const html = fs.readFileSync(htmlFile, "utf-8");
  const $ = cheerio.load(html);

  expect($('meta[name="description"]').attr("content")).toBe(
    "This becomes the summary."
  );

  // The 'Foo' page has 1 image. It should have been given the `loading="lazy"`
  // attribute.
  expect($('img[loading="lazy"]').length).toBe(1);

  // Every page, should have a `link[rel=canonical]` whose `href` always
  // starts with 'https://developer.mozilla.org' and ends with doc's URL.
  expect($("link[rel=canonical]").attr("href")).toBe(
    `https://developer.mozilla.org${doc.mdn_url}`
  );

  expect($('meta[name="robots"]').attr("content")).toBe("index, follow");

  // The HTML should contain the Google Analytics snippet.
  // The ID should match what's set in `testing/.env`.
  expect(
    $('script[src="https://www.google-analytics.com/analytics.js"]').length
  ).toBe(1);

  // The HTML should contain the Speedcurve LUX snippet.
  // The ID should match what's set in `testing/.env`.
  expect($('script[src^="https://cdn.speedcurve.com/"]').attr("src")).toContain(
    "012345"
  );

  // Because this en-US page has a French translation
  expect($('link[rel="alternate"]').length).toBe(2);
  expect($('link[rel="alternate"][hreflang="en"]').length).toBe(1);
  expect($('link[rel="alternate"][hreflang="fr"]').length).toBe(1);
});

test("content built French foo page", () => {
  expect(fs.existsSync(buildRoot)).toBeTruthy();

  const builtFolder = path.join(buildRoot, "fr", "docs", "web", "foo");
  expect(fs.existsSync(builtFolder)).toBeTruthy();

  const jsonFile = path.join(builtFolder, "index.json");
  expect(fs.existsSync(jsonFile)).toBeTruthy();

  // We should be able to read it and expect certain values
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  expect(doc.title).toBe("<foo>: Une page de test");
  expect(doc.isTranslated).toBe(true);
  expect(doc.other_translations[0].locale).toBe("en-US");
  expect(doc.other_translations[0].url).toBe("/en-US/docs/Web/Foo");
  expect(doc.other_translations[0].title).toBe("<foo>: A test tag");

  const htmlFile = path.join(builtFolder, "index.html");
  const html = fs.readFileSync(htmlFile, "utf-8");
  const $ = cheerio.load(html);
  expect($('link[rel="alternate"]').length).toBe(2);
  expect($('link[rel="alternate"][hreflang="en"]').length).toBe(1);
  expect($('link[rel="alternate"][hreflang="fr"]').length).toBe(1);
});

test("wrong xref macro errors", () => {
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "web",
    "wrong_xref_macro"
  );
  const jsonFile = path.join(builtFolder, "index.json");
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  // Expect the first flaw to be that we're using the wrong xref macro.
  expect(doc.flaws.macros[0].name).toBe("MacroBrokenLinkError");
  expect(doc.flaws.macros[0].macroSource).toBe('{{DOMxRef("Promise")}}');
  expect(doc.flaws.macros[0].line).toBe(7);
  expect(doc.flaws.macros[0].column).toBe(51);
  expect(doc.flaws.macros[0].sourceContext).toEqual(
    expect.stringContaining('Web API: {{DOMxRef("Promise")}}')
  );
});

test("summary extracted correctly by span class", () => {
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "web",
    "seo_summarized"
  );
  expect(fs.existsSync(builtFolder)).toBeTruthy();

  const jsonFile = path.join(builtFolder, "index.json");
  expect(fs.existsSync(jsonFile)).toBeTruthy();

  // We should be able to read it and expect certain values
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  expect(doc.summary).toBe("This is going to be the summary.");

  const htmlFile = path.join(builtFolder, "index.html");
  const html = fs.readFileSync(htmlFile, "utf-8");
  const $ = cheerio.load(html);

  expect($('meta[name="description"]').attr("content")).toBe(
    "This is going to be the summary."
  );
});

test("pageTitle on deeper docs within 'Web'", () => {
  const { doc: parentDoc } = JSON.parse(
    fs.readFileSync(
      path.join(buildRoot, "en-us", "docs", "web", "api", "index.json")
    )
  );
  const { doc } = JSON.parse(
    fs.readFileSync(
      path.join(
        buildRoot,
        "en-us",
        "docs",
        "web",
        "api",
        "page_visibility_api",
        "index.json"
      )
    )
  );
  expect(doc.pageTitle).toBe(`${doc.title} - ${parentDoc.title} | MDN`);
});

test("content built interactiveexample page", () => {
  expect(fs.existsSync(buildRoot)).toBeTruthy();

  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "web",
    "interactiveexample"
  );
  expect(fs.existsSync(builtFolder)).toBeTruthy();

  const htmlFile = path.join(builtFolder, "index.html");
  const html = fs.readFileSync(htmlFile, "utf-8");
  const $ = cheerio.load(html);
  expect($("p").text()).toMatch(/Below is a sample interactive example/);
  expect($("iframe").length).toEqual(1);
});

test("the 'notranslate' class is correctly inserted", () => {
  const folder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "web",
    "donttranslatethese"
  );
  const htmlFile = path.join(folder, "index.html");
  const html = fs.readFileSync(htmlFile, "utf-8");
  const $ = cheerio.load(html);
  expect($("pre.notranslate").length).toEqual($("pre").length);
});

test("the 'notecard' class is correctly inserted", () => {
  const folder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "web",
    "donttranslatethese"
  );
  const htmlFile = path.join(folder, "index.html");
  const html = fs.readFileSync(htmlFile, "utf-8");
  const $ = cheerio.load(html);
  expect($("div.warning.notecard").length).toEqual($("div.warning").length);
});

test("content with non-ascii characters in the slug", () => {
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "glossary",
    "bézier_curve"
  );
  expect(fs.existsSync(builtFolder)).toBeTruthy();

  const jsonFile = path.join(builtFolder, "index.json");
  expect(fs.existsSync(jsonFile)).toBeTruthy();
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  expect(doc.title).toBe("Bézier curve");
  expect(doc.mdn_url).toBe("/en-US/docs/Glossary/Bézier_curve");
});

test("content built bar page", () => {
  expect(fs.existsSync(buildRoot)).toBeTruthy();

  const builtFolder = path.join(buildRoot, "en-us", "docs", "web", "bar");
  expect(fs.existsSync(builtFolder)).toBeTruthy();

  const jsonFile = path.join(builtFolder, "index.json");
  expect(fs.existsSync(jsonFile)).toBeTruthy();

  // We should be able to read it and expect certain values
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  expect(doc.title).toBe("bar: A collection of xref macro calls");
  expect(doc.summary).toBe("Here is the summary of the document.");
  expect(doc.mdn_url).toBe("/en-US/docs/Web/Bar");
  // expect(doc.popularity).toBe(0.51);
  // expect(doc.modified).toBeTruthy();
  expect(doc.source).toBeTruthy();
  expect(doc.flaws.macros.length).toBe(12);
  expect(doc.flaws.macros[0].name).toBe("MacroBrokenLinkError");
  expect(doc.flaws.macros[0].macroSource).toBe('{{CSSxRef("bigfoot")}}');
  expect(doc.flaws.macros[0].line).toBe(9);
  expect(doc.flaws.macros[0].column).toBe(6);
  expect(doc.flaws.macros[1].name).toBe("MacroRedirectedLinkError");
  expect(doc.flaws.macros[1].macroSource).toBe('{{CSSxRef("dumber")}}');
  expect(doc.flaws.macros[1].line).toBe(10);
  expect(doc.flaws.macros[1].column).toBe(6);
  expect(doc.flaws.macros[1].redirectInfo).toBeDefined();
  expect(doc.flaws.macros[1].redirectInfo.current).toBe("dumber");
  expect(doc.flaws.macros[1].redirectInfo.suggested).toBe("number");
  expect(doc.flaws.macros[2].name).toBe("MacroBrokenLinkError");
  expect(doc.flaws.macros[2].macroSource).toBe('{{DOMxRef("bigfoot")}}');
  expect(doc.flaws.macros[2].line).toBe(12);
  expect(doc.flaws.macros[2].column).toBe(6);
  expect(doc.flaws.macros[3].name).toBe("MacroRedirectedLinkError");
  expect(doc.flaws.macros[3].macroSource).toBe('{{DOMxRef("Bob")}}');
  expect(doc.flaws.macros[3].line).toBe(13);
  expect(doc.flaws.macros[3].column).toBe(6);
  expect(doc.flaws.macros[3].redirectInfo).toBeDefined();
  expect(doc.flaws.macros[3].redirectInfo.current).toBe("Bob");
  expect(doc.flaws.macros[3].redirectInfo.suggested).toBe("Blob");
  expect(doc.flaws.macros[4].name).toBe("MacroBrokenLinkError");
  expect(doc.flaws.macros[4].macroSource).toBe(
    '{{htmlattrxref("href", "bigfoot")}}'
  );
  expect(doc.flaws.macros[4].line).toBe(15);
  expect(doc.flaws.macros[4].column).toBe(6);
  expect(doc.flaws.macros[5].name).toBe("MacroRedirectedLinkError");
  expect(doc.flaws.macros[5].macroSource).toBe(
    '{{htmlattrxref("href", "anchor")}}'
  );
  expect(doc.flaws.macros[5].line).toBe(16);
  expect(doc.flaws.macros[5].column).toBe(6);
  expect(doc.flaws.macros[5].redirectInfo).toBeDefined();
  expect(doc.flaws.macros[5].redirectInfo.current).toBe("anchor");
  expect(doc.flaws.macros[5].redirectInfo.suggested).toBe("a");
  expect(doc.flaws.macros[6].name).toBe("MacroBrokenLinkError");
  expect(doc.flaws.macros[6].macroSource).toBe('{{jsxref("bigfoot")}}');
  expect(doc.flaws.macros[6].line).toBe(18);
  expect(doc.flaws.macros[6].column).toBe(6);
  expect(doc.flaws.macros[7].name).toBe("MacroRedirectedLinkError");
  expect(doc.flaws.macros[7].macroSource).toBe('{{jsxref("Stern_mode")}}');
  expect(doc.flaws.macros[7].line).toBe(19);
  expect(doc.flaws.macros[7].column).toBe(6);
  expect(doc.flaws.macros[7].redirectInfo).toBeDefined();
  expect(doc.flaws.macros[7].redirectInfo.current).toBe("Stern_mode");
  expect(doc.flaws.macros[7].redirectInfo.suggested).toBe("Strict_mode");
  expect(doc.flaws.macros[8].name).toBe("MacroRedirectedLinkError");
  expect(doc.flaws.macros[8].macroSource).toBe('{{jsxref("Flag")}}');
  expect(doc.flaws.macros[8].line).toBe(21);
  expect(doc.flaws.macros[8].column).toBe(6);
  expect(doc.flaws.macros[8].redirectInfo).toBeDefined();
  expect(doc.flaws.macros[8].redirectInfo.current).toBe("Flag");
  expect(doc.flaws.macros[8].redirectInfo.suggested).toBe("Boolean");
  expect(doc.flaws.macros[9].name).toBe("MacroRedirectedLinkError");
  expect(doc.flaws.macros[9].macroSource).toBe("{{ jsxref('Flag') }}");
  expect(doc.flaws.macros[9].line).toBe(22);
  expect(doc.flaws.macros[9].column).toBe(6);
  expect(doc.flaws.macros[9].redirectInfo).toBeDefined();
  expect(doc.flaws.macros[9].redirectInfo.current).toBe("Flag");
  expect(doc.flaws.macros[9].redirectInfo.suggested).toBe("Boolean");
  expect(doc.flaws.macros[10].name).toBe("MacroRedirectedLinkError");
  expect(doc.flaws.macros[10].macroSource).toBe('{{JSXref("Flag")}}');
  expect(doc.flaws.macros[10].line).toBe(23);
  expect(doc.flaws.macros[10].column).toBe(6);
  expect(doc.flaws.macros[10].redirectInfo).toBeDefined();
  expect(doc.flaws.macros[10].redirectInfo.current).toBe("Flag");
  expect(doc.flaws.macros[10].redirectInfo.suggested).toBe("Boolean");
  expect(doc.flaws.macros[11].name).toBe("MacroRedirectedLinkError");
  expect(doc.flaws.macros[11].macroSource).toBe('{{JSXref("Flag")}}');
  expect(doc.flaws.macros[11].line).toBe(24);
  expect(doc.flaws.macros[11].column).toBe(6);
  expect(doc.flaws.macros[11].redirectInfo).toBeDefined();
  expect(doc.flaws.macros[11].redirectInfo.current).toBe("Flag");
  expect(doc.flaws.macros[11].redirectInfo.suggested).toBe("Boolean");

  const htmlFile = path.join(builtFolder, "index.html");
  expect(fs.existsSync(htmlFile)).toBeTruthy();
  const html = fs.readFileSync(htmlFile, "utf-8");
  const $ = cheerio.load(html);
  expect($("a[data-flaw-src]").length).toEqual(12);

  const brokenLinks = $("a.new");
  expect(brokenLinks.length).toEqual(4);
  expect(brokenLinks.eq(0).data("flaw-src")).toBe('{{CSSxRef("bigfoot")}}');
  expect(brokenLinks.eq(0).text()).toBe("bigfoot");
  expect(brokenLinks.eq(1).data("flaw-src")).toBe('{{DOMxRef("bigfoot")}}');
  expect(brokenLinks.eq(1).text()).toBe("bigfoot");
  expect(brokenLinks.eq(2).data("flaw-src")).toBe(
    '{{htmlattrxref("href", "bigfoot")}}'
  );
  expect(brokenLinks.eq(2).text()).toBe("href");
  expect(brokenLinks.eq(3).data("flaw-src")).toBe('{{jsxref("bigfoot")}}');
  expect(brokenLinks.eq(3).text()).toBe("bigfoot");
  brokenLinks.each((index, element) => {
    expect($(element).attr("title")).toMatch(
      /The documentation about this has not yet been written/
    );
  });

  const numberLinks = $('a[href="/en-US/docs/Web/CSS/number"]');
  expect(numberLinks.length).toEqual(2);
  expect(numberLinks.eq(0).text()).toBe("<dumber>");
  expect(numberLinks.eq(0).data("flaw-src")).toBe('{{CSSxRef("dumber")}}');
  expect(numberLinks.eq(1).text()).toBe("<number>");
  expect(numberLinks.eq(1).data("flaw-src")).toBeFalsy();

  const blobLinks = $('a[href="/en-US/docs/Web/API/Blob"]:not([title])');
  expect(blobLinks.length).toEqual(2);
  expect(blobLinks.eq(0).text()).toBe("Bob");
  expect(blobLinks.eq(0).data("flaw-src")).toBe('{{DOMxRef("Bob")}}');
  expect(blobLinks.eq(1).text()).toBe("Blob");
  expect(blobLinks.eq(1).data("flaw-src")).toBeFalsy();

  const hrefLinks = $(
    'a[href="/en-US/docs/Web/HTML/Element/a#attr-href"]:not([title])'
  );
  expect(hrefLinks.length).toEqual(2);
  hrefLinks.each((index, element) => {
    expect($(element).text()).toBe("href");
  });
  expect(hrefLinks.eq(0).data("flaw-src")).toBe(
    '{{htmlattrxref("href", "anchor")}}'
  );
  expect(hrefLinks.eq(1).data("flaw-src")).toBeFalsy();

  const strictModeLinks = $(
    'a[href="/en-US/docs/Web/JavaScript/Reference/Strict_mode"]:not([title])'
  );
  expect(strictModeLinks.length).toEqual(2);
  expect(strictModeLinks.eq(0).text()).toBe("Stern_mode");
  expect(strictModeLinks.eq(0).data("flaw-src")).toBe(
    '{{jsxref("Stern_mode")}}'
  );
  expect(strictModeLinks.eq(1).text()).toBe("Strict_mode");
  expect(strictModeLinks.eq(1).data("flaw-src")).toBeFalsy();

  const booleanLinks = $(
    'a[href="/en-US/docs/Web/JavaScript/Reference/Global_Objects/Boolean"]:not([title])'
  );
  expect(booleanLinks.length).toEqual(6);
  expect(booleanLinks.eq(0).text()).toBe("Flag");
  expect(booleanLinks.eq(0).data("flaw-src")).toBe('{{jsxref("Flag")}}');
  expect(booleanLinks.eq(1).text()).toBe("Flag");
  expect(booleanLinks.eq(1).data("flaw-src")).toBe("{{ jsxref('Flag') }}");
  expect(booleanLinks.eq(2).text()).toBe("Flag");
  expect(booleanLinks.eq(2).data("flaw-src")).toBe('{{JSXref("Flag")}}');
  expect(booleanLinks.eq(3).text()).toBe("Flag");
  expect(booleanLinks.eq(3).data("flaw-src")).toBe('{{JSXref("Flag")}}');
  expect(booleanLinks.eq(4).text()).toBe("Boolean");
  expect(booleanLinks.eq(4).data("flaw-src")).toBeFalsy();
  expect(booleanLinks.eq(5).text()).toBe("bOOleAn");
  expect(booleanLinks.eq(5).data("flaw-src")).toBeFalsy();
});

test("broken links flaws", () => {
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "web",
    "brokenlinks"
  );
  const jsonFile = path.join(builtFolder, "index.json");
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  const { flaws } = doc;
  // You have to be intimately familiar with the fixture to understand
  // why these flaws come out as they do.
  expect(flaws.broken_links.length).toBe(9);
  // Map them by 'href'
  const map = new Map(flaws.broken_links.map((x) => [x.href, x]));
  expect(map.get("/en-US/docs/Hopeless/Case").suggestion).toBeNull();
  expect(map.get("/en-US/docs/Web/CSS/dumber").line).toBe(10);
  expect(map.get("/en-US/docs/Web/CSS/dumber").column).toBe(13);
  expect(
    map.get("https://developer.mozilla.org/en-US/docs/Web/API/Blob").suggestion
  ).toBe("/en-US/docs/Web/API/Blob");
  expect(
    map.get("https://developer.mozilla.org/en-US/docs/Web/API/Blob#Anchor")
      .suggestion
  ).toBe("/en-US/docs/Web/API/Blob#Anchor");
  expect(
    map.get("https://developer.mozilla.org/en-US/docs/Web/API/Blob?a=b")
      .suggestion
  ).toBe("/en-US/docs/Web/API/Blob?a=b");
  expect(map.get("/en-us/DOCS/Web/api/BLOB").suggestion).toBe(
    "/en-US/docs/Web/API/Blob"
  );
  expect(
    map.get("/en-US/docs/Web/HTML/Element/anchor#fragment").suggestion
  ).toBe("/en-US/docs/Web/HTML/Element/a#fragment");
  expect(
    map.get("/en-US/docs/glossary/bézier_curve#identifier").suggestion
  ).toBe("/en-US/docs/Glossary/Bézier_curve#identifier");
});

test("repeated broken links flaws", () => {
  // This fixture has the same broken link, that redirects, 3 times.
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "web",
    "brokenlinks_repeats"
  );
  const jsonFile = path.join(builtFolder, "index.json");
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  const { flaws } = doc;
  // You have to be intimately familiar with the fixture to understand
  // why these flaws come out as they do.
  expect(flaws.broken_links.length).toBe(3);

  // Map them by 'id'
  const map = new Map(flaws.broken_links.map((x) => [x.id, x]));
  expect(map.size).toBe(3);
  expect(map.get("link1").suggestion).toBe("/en-US/docs/Web/CSS/number");
  expect(map.get("link2").suggestion).toBe("/en-US/docs/Web/CSS/number");
  expect(map.get("link3").suggestion).toBe("/en-US/docs/Web/CSS/number");
});

test("without locale prefix broken links flaws", () => {
  // This fixture has the same broken link, that redirects, 3 times.
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "web",
    "brokenlinks",
    "without_locale_prefix"
  );
  const jsonFile = path.join(builtFolder, "index.json");
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  const { flaws } = doc;
  // You have to be intimately familiar with the fixture to understand
  // why these flaws come out as they do.
  expect(flaws.broken_links.length).toBe(3);

  // Map them by 'id'
  const map = new Map(flaws.broken_links.map((x) => [x.id, x]));
  expect(map.size).toBe(3);
  expect(map.get("link1").suggestion).toBe("/en-US/docs/Web/CSS/number");
  expect(map.get("link2").suggestion).toBe("/en-US/docs/Web/CSS/number");
  expect(map.get("link3").suggestion).toBeNull();
});

test("check built flaws for /en-us/learn/css/css_layout/introduction/grid page", () => {
  expect(fs.existsSync(buildRoot)).toBeTruthy();
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "learn",
    "css",
    "css_layout",
    "introduction",
    "grid"
  );
  expect(fs.existsSync(builtFolder)).toBeTruthy();

  const jsonFile = path.join(builtFolder, "index.json");
  expect(fs.existsSync(jsonFile)).toBeTruthy();

  // Let's make sure there are only 2 "macros" flaws.
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  expect(doc.flaws.macros.length).toBe(2);
});

test("check built flaws for /en-us/learn/css/css_layout/introduction/flex page", () => {
  expect(fs.existsSync(buildRoot)).toBeTruthy();
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "learn",
    "css",
    "css_layout",
    "introduction",
    "flex"
  );
  expect(fs.existsSync(builtFolder)).toBeTruthy();

  const htmlFile = path.join(builtFolder, "index.html");
  const html = fs.readFileSync(htmlFile, "utf-8");
  const $ = cheerio.load(html);
  // The css_layout/introduction/flex page has 2 iframes
  expect($('iframe[loading="lazy"]').length).toBe(2);
});

test("detect bad_bcd_queries flaws", () => {
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "web",
    "badbcdqueries"
  );
  expect(fs.existsSync(builtFolder)).toBeTruthy();
  const jsonFile = path.join(builtFolder, "index.json");
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  expect(doc.flaws.bad_bcd_queries.length).toBe(1);
  // If the flaw is there, it's always an array because a document could
  // potentially have multiple bad BCD queries.
  expect(doc.flaws.bad_bcd_queries.length).toBe(1);
  expect(doc.flaws.bad_bcd_queries[0].explanation).toBe(
    "No BCD data for query: api.Does.Not.exist"
  );
  expect(doc.flaws.bad_bcd_queries[0].suggestion).toBeNull();
});

test("detect bad_bcd_links flaws from", () => {
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "web",
    "api",
    "page_visibility_api"
  );
  expect(fs.existsSync(builtFolder)).toBeTruthy();
  const jsonFile = path.join(builtFolder, "index.json");
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  expect(doc.flaws.bad_bcd_links.length).toBe(1);
  // The reasons it's a bad link is because the @mdn/browser-compat-data,
  // for the query `api.Document.visibilityState` refers to a page
  // with mdn_url `/en-US/docs/Web/API/Document/visibilityState` which we
  // don't have. At least not in the testing content :)
  const flaw = doc.flaws.bad_bcd_links[0];
  expect(flaw.slug).toBe("/en-US/docs/Web/API/Document/visibilityState");
  expect(flaw.suggestion).toBeNull();
  expect(flaw.query).toBe("api.Document.visibilityState");
});

test("detect pre_with_html flaws", () => {
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "learn",
    "some_code"
  );
  expect(fs.existsSync(builtFolder)).toBeTruthy();
  const jsonFile = path.join(builtFolder, "index.json");
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  expect(doc.flaws.pre_with_html.length).toBe(1);
  const flaw = doc.flaws.pre_with_html[0];
  expect(flaw.explanation).toBe("<pre><code>CODE can be just <pre>CODE");
  expect(flaw.id).toBeTruthy();
  expect(flaw.fixable).toBe(true);
  expect(flaw.html).toBeTruthy();
  expect(flaw.suggestion).toBeTruthy();
  expect(flaw.line).toBe(29);
  expect(flaw.column).toBe(50);
});

test("image flaws", () => {
  const builtFolder = path.join(buildRoot, "en-us", "docs", "web", "images");
  const jsonFile = path.join(builtFolder, "index.json");
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  const { flaws } = doc;
  // You have to be intimately familiar with the fixture to understand
  // why these flaws come out as they do.
  expect(flaws.images.length).toBe(8);
  const map = new Map(flaws.images.map((x) => [x.src, x]));

  let flaw = map.get(
    "https://www.peterbe.com/static/images/howsmywifi-scr.png"
  );
  expect(flaw.explanation).toBe("External image URL");
  expect(flaw.suggestion).toBeNull();
  expect(flaw.line).toBe(19);
  expect(flaw.column).toBe(13);

  flaw = map.get("idontexist.png");
  expect(flaw.explanation).toBe("File not present on disk");
  expect(flaw.suggestion).toBeNull();
  expect(flaw.line).toBe(34);
  expect(flaw.column).toBe(13);

  flaw = map.get("/en-US/docs/Web/Images/florian.png");
  expect(flaw.explanation).toBe("Pathname should be relative to document");
  expect(flaw.suggestion).toBe("florian.png");
  expect(flaw.line).toBe(39);
  expect(flaw.column).toBe(13);

  flaw = map.get("Florian.PNG");
  expect(flaw.explanation).toBe("Pathname should always be lowercase");
  expect(flaw.suggestion).toBe("florian.png");
  expect(flaw.line).toBe(44);
  expect(flaw.column).toBe(13);

  flaw = map.get("http://www.peterbe.com/static/images/favicon-32.png");
  expect(flaw.explanation).toBe("Insecure URL");
  expect(flaw.suggestion).toBe(
    "https://www.peterbe.com/static/images/favicon-32.png"
  );
  expect(flaw.line).toBe(49);
  expect(flaw.column).toBe(13);

  flaw = map.get(
    "https://developer.mozilla.org/en-US/docs/Web/Images/screenshot.png"
  );
  expect(flaw.explanation).toBe("Unnecessarily absolute URL");
  expect(flaw.suggestion).toBe("screenshot.png");
  expect(flaw.line).toBe(54);
  expect(flaw.column).toBe(13);

  flaw = map.get("/en-US/docs/Web/Foo/screenshot.png");
  expect(flaw.explanation).toBe("Pathname should be relative to document");
  expect(flaw.suggestion).toBe("../Foo/screenshot.png");
  expect(flaw.line).toBe(59);
  expect(flaw.column).toBe(13);

  flaw = map.get("../Foo/nonexistent.png");
  expect(flaw.explanation).toBe("File not present on disk");
  expect(flaw.suggestion).toBeNull();
  expect(flaw.line).toBe(64);
  expect(flaw.column).toBe(13);

  const htmlFile = path.join(builtFolder, "index.html");
  const html = fs.readFileSync(htmlFile, "utf-8");
  const $ = cheerio.load(html);
  $("#content img[src]").each((i, element) => {
    const img = $(element);
    const src = img.attr("src");
    if (src.includes("www.peterbe.com/")) {
      // These are forced to be https
      expect(src.startsWith("https://")).toBeTruthy();
    } else {
      expect(src.startsWith("/en-US/docs/Web/")).toBeTruthy();
    }
  });
});

test("chicken_and_egg page should build with flaws", () => {
  const builtFolder = path.join(buildRoot, "en-us", "docs", "chicken_and_egg");
  expect(fs.existsSync(builtFolder)).toBeTruthy();
  const jsonFile = path.join(builtFolder, "index.json");
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  expect(doc.flaws.macros.length).toBe(1);
  // The filepath will be that of the "egg" or the "childen" page.
  // Let's not try to predict which one exactly, because that'd mean this
  // test would need to use the exact same sort order as the glob used
  // when we ran "yarn build" to set up the build fixtures.
  const flaw = doc.flaws.macros[0];
  expect(flaw.name).toBe("MacroExecutionError");
  expect(
    flaw.errorStack.includes(
      "documents form a circular dependency when rendering"
    )
  ).toBeTruthy();
});

test("404 page", () => {
  const builtFolder = path.join(buildRoot, "en-us", "_spas");
  expect(fs.existsSync(builtFolder)).toBeTruthy();
  const htmlFile = path.join(builtFolder, "404.html");
  const html = fs.readFileSync(htmlFile, "utf-8");
  const $ = cheerio.load(html);
  expect($("title").text()).toContain("Page not found");
  expect($("h1").text()).toContain("Page not found");
  expect($('meta[name="robots"]').attr("content")).toBe("noindex, nofollow");
});

test("bcd table extraction followed by h3", () => {
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "web",
    "bcd_table_extraction"
  );
  expect(fs.existsSync(builtFolder)).toBeTruthy();
  const jsonFile = path.join(builtFolder, "index.json");
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  expect(doc.body[0].type).toBe("prose");
  expect(doc.body[1].type).toBe("prose");
  expect(doc.body[2].type).toBe("browser_compatibility");
  expect(doc.body[2].value.isH3).toBeFalsy();
  expect(doc.body[3].type).toBe("prose");
  expect(doc.body[4].type).toBe("prose");
  expect(doc.body[4].value.isH3).toBeTruthy();
});

test("bcd table extraction when overly nested is a flaw", () => {
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "web",
    "bcd_table_extraction",
    "nested_divs"
  );
  expect(fs.existsSync(builtFolder)).toBeTruthy();
  const jsonFile = path.join(builtFolder, "index.json");
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  expect(doc.flaws.sectioning[0].explanation).toBe(
    "2 'div.bc-data' elements found but deeply nested."
  );
});

test("img tags with an empty 'src' should be a flaw", () => {
  const builtFolder = path.join(
    buildRoot,
    "en-us",
    "docs",
    "web",
    "empty_image"
  );
  expect(fs.existsSync(builtFolder)).toBeTruthy();
  const jsonFile = path.join(builtFolder, "index.json");
  const { doc } = JSON.parse(fs.readFileSync(jsonFile));
  expect(doc.flaws.images.length).toBe(1);
  expect(doc.flaws.images[0].explanation).toBe("Empty img 'src' attribute");
  expect(doc.flaws.images[0].fixable).toBeFalsy();
  expect(doc.flaws.images[0].externalImage).toBeFalsy();
  expect(doc.flaws.images[0].line).toBe(8);
  expect(doc.flaws.images[0].column).toBe(13);
});
