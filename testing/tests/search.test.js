require("expect-puppeteer");

function testURL(pathname = "/") {
  return "http://localhost:5000" + pathname;
}

describe("Site search", () => {
  const SEARCH_SELECTOR = 'form input[type="search"]';

  beforeAll(async () => {
    // Yes, this is a cheeky implementation testing detail but it's a nice
    // sanity check that the server can respond with *something* that's
    // sensible. This "test" also helps make sense of other potentially
    // very confusing errors within the important tests themselves.
    await page.goto(testURL("/en-US/search-index.json"));
    // It's JSON but this asserts that page will be findable
    await expect(page).toMatch("<foo>: A test tag");
  });

  test("find Foo page", async () => {
    await page.goto(testURL("/"));
    await expect(page).toFill(SEARCH_SELECTOR, "foo");
    await expect(page).toMatch("<foo>: A test tag");
    await expect(page).toClick('[aria-selected="true"]');
    await expect(page).toMatchElement("h1", { text: "<foo>: A test tag" });
    // Should have been redirected too...
    // Note! It's important that this happens *after* the `.toMatchElement`
    // on the line above because expect-puppeteer doesn't have a wait to
    // properly wait for the (pushState) URL to have changed.
    expect(page.url()).toBe(testURL("/en-US/docs/Web/Foo"));
  });

  test("input placeholder changes when focused", async () => {
    await expect(page).toMatchElement(SEARCH_SELECTOR, {
      placeholder: /Site search/,
    });
    await expect(page).toClick(SEARCH_SELECTOR);
    await expect(page).toMatchElement(SEARCH_SELECTOR, {
      placeholder: /Go ahead/,
    });
  });

  test("should NOT get search results", async () => {
    await page.goto(testURL("/"));
    await expect(page).toFill(SEARCH_SELECTOR, "div");
    await expect(page).toMatchElement(".nothing-found", {
      text: "nothing found",
    });
  });
});
