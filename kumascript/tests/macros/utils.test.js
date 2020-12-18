/**
 * @prettier
 */
const { lintHTML } = require("./utils");

const ERROR_TEST_CASES = [
  {
    title: "with an invalid HTML element",
    html: "<junk></junk>",
    error: "<junk> is not a valid element name",
  },
  {
    title: "with an HTML element missing its closing tag",
    html: "<div>closing tag has gone missing",
    error:
      "Missing close-tag, expected '</div>' but document ended before it was found.",
  },
  {
    title: "with an illegal value for a link attribute",
    html: '<a href="https://example.com" dir="xxx">an example</a>',
    error: 'Attribute "dir" has invalid value "xxx"',
  },
];

describe("test lintHTML function", function () {
  for (const test of ERROR_TEST_CASES) {
    it(test.title, function () {
      expect(lintHTML(test.html)).toContain(test.error);
    });
  }
  it("with valid HTML input", function () {
    expect(lintHTML("<div>This is nice</div>")).toBeFalsy();
  });
});
