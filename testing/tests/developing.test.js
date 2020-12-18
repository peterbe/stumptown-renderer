const got = require("got");
require("expect-puppeteer");
const { setDefaultOptions } = require("expect-puppeteer");

// The default it 500ms. Building and running these pages can be pretty slow
// since the rendering both involves create-react-app bundling the test page,
// and then the server building of the page can be pretty heavy.
setDefaultOptions({ timeout: 5000 });

function devURL(pathname = "/") {
  return "http://localhost:3000" + pathname;
}

function serverURL(pathname = "/") {
  return "http://localhost:5000" + pathname;
}

// This "trick" is to force every test to be skipped if the environment
// variable hasn't been set. This way, when you run `jest ...`, and it finds
// all `**/*.test.js` it doesn't actually run these tests unless explicitly
// prepared to do so.
// The source of this idea comes from https://github.com/facebook/jest/issues/7245
const withDeveloping = JSON.parse(process.env.TESTING_DEVELOPING || "false")
  ? it
  : it.skip;

describe("Testing the kitchensink page", () => {
  withDeveloping("open the page", async () => {
    await page.goto(devURL("/en-US/docs/MDN/Kitchensink"));
    await expect(page).toMatch("The MDN Content Kitchensink");
    // If there are no flaws on that page or if there are, it will still
    // mention the word "flaws" somewhere in the Toolbar
    await expect(page).toMatchElement(".toolbar", { text: "flaws" });
  });

  withDeveloping("server-side render HTML", async () => {
    // You can go to the page directly via the server
    await page.goto(serverURL("/en-US/docs/MDN/Kitchensink"), {
      // This is necessary because the page contains lazy loading iframes
      // to external domains.
      waitUntil: "networkidle0",
    });
    await expect(page).toMatch("The MDN Content Kitchensink");
  });

  withDeveloping("server-side render HTML", async () => {
    // Loading the index.json doesn't require a headless browser
    const { doc } = await got(
      serverURL("/en-US/docs/MDN/Kitchensink/index.json")
    ).json();
    expect(doc.title).toBe("The MDN Content Kitchensink");
  });

  // XXX Do more advanced tasks that test the server and document "CRUD operations"
});

// Note, some of these tests cover some of the core code that we use in
// the Lambda@Edge origin-request handler.
describe("Testing the Express server", () => {
  withDeveloping("redirect without any useful headers", async () => {
    let response = await got(serverURL("/"), { followRedirect: false });
    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe("/en-US/");

    response = await got(serverURL("/docs/Web"), {
      followRedirect: false,
    });
    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe("/en-US/docs/Web");

    // Trailing slashed
    response = await got(serverURL("/docs/Web/"), {
      followRedirect: false,
    });
    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe("/en-US/docs/Web");
  });

  withDeveloping("redirect based on _redirects.txt", async () => {
    // Yes, this is a bit delicate since it depends on non-fixtures, but
    // it's realistic and it's a good end-to-end test.
    // See mdn/content/files/en-us/_redirects.txt

    // First redirect *out* to an external URL.
    let response = await got(
      serverURL(
        "/en-US/docs/Mozilla/Add-ons/WebExtensions/Publishing_your_WebExtension"
      ),
      { followRedirect: false }
    );
    expect(response.statusCode).toBe(301);
    expect(response.headers.location).toBe(
      "https://extensionworkshop.com/documentation/publish/package-your-extension/"
    );

    // Redirect within.
    response = await got(
      serverURL(
        "/en-US/docs/Mozilla/Add-ons/WebExtensions/Extension_API_differences"
      ),
      { followRedirect: false }
    );
    expect(response.statusCode).toBe(301);
    expect(response.headers.location).toBe(
      "/en-US/docs/Mozilla/Add-ons/WebExtensions/Differences_between_API_implementations"
    );
  });

  withDeveloping("redirect by preferred locale cookie", async () => {
    let response = await got(serverURL("/"), {
      followRedirect: false,
      headers: {
        // Note! Case insensitive
        Cookie: "preferredlocale=zH-cN; foo=bar",
      },
    });
    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe("/zh-CN/");

    // Some bogus locale we definitely don't recognized
    response = await got(serverURL("/"), {
      followRedirect: false,
      headers: {
        Cookie: "preferredlocale=xyz; foo=bar",
      },
    });
    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe("/en-US/");
  });

  withDeveloping("redirect by 'Accept-Language' header", async () => {
    let response = await got(serverURL("/"), {
      followRedirect: false,
      headers: {
        // Based on https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Accept-Language
        "Accept-language": "fr-CH, fr;q=0.9, en;q=0.8, de;q=0.7, *;q=0.5",
      },
    });
    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe("/fr/");

    // Some bogus locale we definitely don't recognized
    response = await got(serverURL("/"), {
      followRedirect: false,
      headers: {
        "accept-language": "xyz, X;q=0.9, Y;q=0.8, Z;q=0.7, *;q=0.5",
      },
    });
    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe("/en-US/");
  });

  withDeveloping("redirect by cookie trumps", async () => {
    let response = await got(serverURL("/"), {
      followRedirect: false,
      headers: {
        Cookie: "preferredlocale=SV-se",
        "Accept-language": "fr-CH, fr;q=0.9, en;q=0.8, de;q=0.7, *;q=0.5",
      },
    });
    expect(response.statusCode).toBe(302);
    expect(response.headers.location).toBe("/sv-SE/");
  });
});
