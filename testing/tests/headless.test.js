require("expect-puppeteer");

function testURL(pathname = "/") {
  return "http://localhost:5000" + pathname;
}

describe("Basic viewing of functional pages", () => {
  it("open the temporary home page", async () => {
    await page.goto(testURL("/"));
    await expect(page).toMatch("MDN Web Docs");
    await expect(page).toMatchElement("title", { text: /MDN Web Docs/ });
  });

  it("open the /en-US/docs/Web/Foo page", async () => {
    await page.goto(testURL("/en-US/docs/Web/Foo"));
    await expect(page).toMatch("<foo>: A test tag");
  });

  it("open the /en-US/docs/Web/InteractiveExample page", async () => {
    await page.goto(testURL("/en-US/docs/Web/InteractiveExample"), {
      // Be a bit less patient with this particular page because it contains
      // an iframe, on an external URL,  which we're not particularly
      // interested in waiting for.
      waitUntil: "domcontentloaded",
    });
    await expect(page).toMatch("I Have an Interactive Example");
  });

  it("open the /en-US/docs/Learn/CSS/CSS_layout/Introduction page", async () => {
    const uri = "/en-US/docs/Learn/CSS/CSS_layout/Introduction";
    const flexSample1Uri = `${uri}/Flex/_samples_/Flex_1`;
    const flexSample2Uri = `${uri}/Flex/_samples_/Flex_2`;
    const gridSample1Uri = `${uri}/Grid/_samples_/Grid_1`;
    const gridSample2Uri = `${uri}/_samples_/Grid_2`;
    await page.goto(testURL(uri));
    await expect(page).toMatch("A Test Introduction to CSS layout");
    await expect(page).toMatchElement("h1", {
      text: "A Test Introduction to CSS layout",
    });
    await expect(page).toMatchElement("#Flexbox", {
      text: "Flexbox",
    });
    await expect(page).toMatchElement(
      `iframe.live-sample-frame.sample-code-frame[src$="${flexSample1Uri}"]`
    );
    await expect(page).toMatchElement(
      `iframe.live-sample-frame.sample-code-frame[src$="${flexSample2Uri}"]`
    );
    await expect(page).toMatchElement("#Grid_Layout", {
      text: "Grid Layout",
    });
    await expect(page).toMatchElement(
      `iframe.live-sample-frame.sample-code-frame[src$="${gridSample1Uri}"]`
    );
    await expect(page).toMatchElement("#Grid_2 > pre.css.notranslate", {
      text: /\.wrapper\s*\{\s*display\:\s*grid\;/,
    });
    await expect(page).toMatchElement(
      `iframe.live-sample-frame.sample-code-frame[src$="${gridSample2Uri}"]`
    );
    // Ensure that the live-sample pages were built.
    for (const sampleUri of [
      flexSample1Uri,
      flexSample2Uri,
      gridSample1Uri,
      gridSample2Uri,
    ]) {
      await page.goto(testURL(sampleUri));
      await expect(page).toMatchElement("body > div.wrapper > div.box1", {
        text: "One",
      });
      await expect(page).toMatchElement("body > div.wrapper > div.box2", {
        text: "Two",
      });
      await expect(page).toMatchElement("body > div.wrapper > div.box3", {
        text: "Three",
      });
    }
  });

  it("open the /en-US/docs/Learn/CSS/CSS_layout/Introduction/Flex page", async () => {
    const uri = "/en-US/docs/Learn/CSS/CSS_layout/Introduction/Flex";
    const flexSample1Uri = `${uri}/_samples_/Flex_1`;
    const flexSample2Uri = `${uri}/_samples_/Flex_2`;
    await page.goto(testURL(uri));
    await expect(page).toMatch("A Test Introduction to CSS Flexbox Layout");
    await expect(page).toMatchElement("h1", {
      text: "A Test Introduction to CSS Flexbox Layout",
    });
    await expect(page).toMatchElement("#Flexbox", {
      text: "Flexbox",
    });
    await expect(page).toMatchElement("#Flex_1 > pre.css.notranslate", {
      text: /\.wrapper\s*\{\s*display\:\s*flex\;\s*\}/,
    });
    await expect(page).toMatchElement(
      `iframe.live-sample-frame.sample-code-frame[src$="${flexSample1Uri}"]`
    );
    await expect(page).toMatchElement("#Flex_2 > pre.css.notranslate", {
      text: /\.wrapper\s*\{\s*display\:\s*flex\;\s*\}.+flex\:\s*1\;/,
    });
    await expect(page).toMatchElement(
      `iframe.live-sample-frame.sample-code-frame[src$="${flexSample2Uri}"]`
    );
  });

  it("open the /en-US/docs/Learn/CSS/CSS_layout/Introduction/Grid page", async () => {
    const uri = "/en-US/docs/Learn/CSS/CSS_layout/Introduction/Grid";
    const gridSample1Uri = `${uri}/_samples_/Grid_1`;
    const gridSample2Uri = `${uri}/_samples_/Grid_2`;
    await page.goto(testURL(uri));
    await expect(page).toMatch("A Test Introduction to CSS Grid Layout");
    await expect(page).toMatchElement("h1", {
      text: "A Test Introduction to CSS Grid Layout",
    });
    await expect(page).toMatchElement("#Grid_Layout", {
      text: "Grid Layout",
    });
    await expect(page).toMatchElement("#Grid_1 > pre.css.notranslate", {
      text: /\.wrapper\s*\{\s*display\:\s*grid\;/,
    });
    await expect(page).toMatchElement(
      `iframe.live-sample-frame.sample-code-frame[src$="${gridSample1Uri}"]`
    );
    await expect(page).toMatchElement("#Grid_2 > pre.css.notranslate", {
      text: /\.wrapper\s*\{\s*display\:\s*grid\;.+\.box1\s*\{/,
    });
    await expect(page).toMatchElement(
      `iframe.live-sample-frame.sample-code-frame[src$="${gridSample2Uri}"]`
    );
    // Ensure that the live-sample page "gridSample2Uri" was built.
    await page.goto(testURL(gridSample2Uri));
    await expect(page).toMatchElement("body > div.wrapper > div.box1", {
      text: "One",
    });
    await expect(page).toMatchElement("body > div.wrapper > div.box2", {
      text: "Two",
    });
    await expect(page).toMatchElement("body > div.wrapper > div.box3", {
      text: "Three",
    });
  });

  it("should return to previous page on back-button press", async () => {
    await page.goto(testURL("/en-US/docs/Web/Foo"));
    await expect(page).toMatch("<foo>: A test tag");
    await expect(page).toMatchElement("h1", {
      text: "<foo>: A test tag",
    });
    await expect(page).toClick("header h1 a", { text: "MDN Web Docs" });
    await expect(page).toMatchElement("h2", {
      text: "Welcome to MDN",
    });
    expect(page.url()).toBe(testURL("/"));
    await page.goBack();
    await expect(page).toMatchElement("h1", {
      text: "<foo>: A test tag",
    });
  });
});
