/**
 * @prettier
 */

const fs = require("fs");
const Templates = require("../src/templates.js");
const { render } = require("../src/render.js");
const {
  MacroInvocationError,
  MacroNotFoundError,
  MacroCompilationError,
  MacroExecutionError,
} = require("../src/errors.js");

describe("render() function", () => {
  function fixture(name) {
    return __dirname + "/fixtures/render/" + name;
  }
  function get(name) {
    return fs.readFileSync(fixture(name), "utf8");
  }
  function renderPrerequisiteFromURL(url) {
    throw new Error(`unexpected prerequisite: ${url}`);
  }

  it("is a function", () => {
    expect(typeof render).toBe("function");
  });

  let cases = ["testcase1", "testcase2", "testcase3", "testcase4"];
  it.each(cases)("handles basic rendering %s", async (casedir) => {
    let input = get(casedir + "/input");
    let expected = get(casedir + "/output");
    let templates = new Templates(fixture(casedir + "/macros"));
    let [result, errors] = await render(input, {}, renderPrerequisiteFromURL, {
      templates,
    });
    expect(result).toEqual(expected);
    expect(errors).toEqual([]);
  });

  it.each(["render", "remove"])("handles selective %s", async (mode) => {
    let input = get("testcase2/input");
    let expected = get(`testcase2/output_selective_${mode}`);
    let templates = new Templates(fixture("testcase2/macros"));
    let pageEnv = {
      selective_mode: [mode, ["Multi:Line:Macro", "頁尾附註", "MacroWithJson"]],
    };
    let [result, errors] = await render(
      input,
      pageEnv,
      renderPrerequisiteFromURL,
      {
        templates,
      }
    );
    expect(result).toEqual(expected);
    expect(errors).toEqual([]);
  });

  it("renders asynchronous macros", async () => {
    jest.useFakeTimers();
    async function after(delay, value) {
      return new Promise((resolve, reject) => {
        setTimeout(() => resolve(value), delay);
      });
    }

    let templates = new Templates(fixture("macros"));
    let promise = render(
      "{{asyncMacro}}",
      { after },
      renderPrerequisiteFromURL,
      {
        templates,
      }
    );
    jest.runAllTimers();
    let [result, errors] = await promise;
    expect(errors.length).toBe(0);
    expect(result).toEqual("yay!");
  });

  it("exposes the per-page env object", async () => {
    let templates = new Templates(fixture("macros"));
    let [result, errors] = await render(
      "{{env}}",
      {
        x: 1,
        y: 2,
      },
      renderPrerequisiteFromURL,
      {
        templates,
      }
    );
    expect(errors.length).toBe(0);
    expect(result).toEqual("3");
  });

  let syntaxCases = ["syntax1", "syntax2", "syntax3", "syntax4"];
  it.each(syntaxCases)("handles syntax errors: %s", async (fn) => {
    let input = get(fn);
    // null templates since we expect errors before we render any
    expect.assertions(4);
    try {
      await render(input, {}, renderPrerequisiteFromURL, {
        templates: null,
      });
    } catch (e) {
      expect(e).toBeInstanceOf(MacroInvocationError);
      expect(e.name).toBe("MacroInvocationError");
      expect(e).toHaveProperty("line");
      expect(e).toHaveProperty("column");
    }
  });

  it("handles undefined templates", async () => {
    let templates = new Templates(fixture("macros"));
    let [result, errors] = await render(
      "foo{{nope}}bar",
      {},
      renderPrerequisiteFromURL,
      {
        templates,
      }
    );
    expect(result).toEqual("foo{{nope}}bar");
    expect(errors.length).toBe(1);
    expect(errors[0]).toBeInstanceOf(MacroNotFoundError);
    expect(errors[0].name).toBe("MacroNotFoundError");
    expect(errors[0]).toHaveProperty("line");
    expect(errors[0]).toHaveProperty("column");
  });

  it("handles compilation errors", async () => {
    let templates = new Templates(fixture("macros"));
    let [result, errors] = await render(
      "foo{{syntax}}bar",
      {},
      renderPrerequisiteFromURL,
      {
        templates,
      }
    );
    expect(result).toEqual("foo{{syntax}}bar");
    expect(errors.length).toBe(1);
    expect(errors[0]).toBeInstanceOf(MacroCompilationError);
    expect(errors[0].name).toBe("MacroCompilationError");
    expect(errors[0]).toHaveProperty("line");
    expect(errors[0]).toHaveProperty("column");
  });

  it("handles execution errors", async () => {
    let templates = new Templates(fixture("macros"));
    let [result, errors] = await render(
      "foo{{throw}}bar",
      {},
      renderPrerequisiteFromURL,
      {
        templates,
      }
    );
    expect(result).toEqual("foo{{throw}}bar");
    expect(errors.length).toBe(1);
    expect(errors[0]).toBeInstanceOf(MacroExecutionError);
    expect(errors[0].name).toBe("MacroExecutionError");
    expect(errors[0]).toHaveProperty("line");
    expect(errors[0]).toHaveProperty("column");
  });

  it("handles undefined variables in macros", async () => {
    let templates = new Templates(fixture("macros"));
    let [result, errors] = await render(
      "foo{{ undefined() }}bar",
      {},
      renderPrerequisiteFromURL,
      {
        templates,
      }
    );
    expect(result).toEqual("foo{{ undefined() }}bar");
    expect(errors.length).toBe(1);
    expect(errors[0]).toBeInstanceOf(MacroExecutionError);
    expect(errors[0].name).toBe("MacroExecutionError");
    expect(errors[0]).toHaveProperty("line");
    expect(errors[0]).toHaveProperty("column");
  });

  it("handles multiple errors in one document", async () => {
    let templates = new Templates(fixture("macros"));
    let [result, errors] = await render(
      "foo{{nope(1)}}bar{{throw(2)}}baz{{syntax(3)}}",
      {},
      renderPrerequisiteFromURL,
      {
        templates,
      }
    );
    expect(result).toEqual("foo{{nope(1)}}bar{{throw(2)}}baz{{syntax(3)}}");
    expect(errors.length).toBe(3);
    expect(errors[0]).toBeInstanceOf(MacroNotFoundError);
    expect(errors[1]).toBeInstanceOf(MacroExecutionError);
    expect(errors[2]).toBeInstanceOf(MacroCompilationError);
  });

  it("handles success plus errors in one document", async () => {
    let templates = new Templates(fixture("macros"));
    let [result, errors] = await render(
      'foo{{echo("!")}} bar{{ throw(1,2) }}baz{{echo("?")}}',
      {},
      renderPrerequisiteFromURL,
      {
        templates,
      }
    );
    expect(result).toEqual("foo! bar{{ throw(1,2) }}baz?");
    expect(errors.length).toBe(1);
    expect(errors[0]).toBeInstanceOf(MacroExecutionError);
  });

  it("macros can include other macros with template()", async () => {
    let input = "foo {{bar}} baz";
    let expected = "foo (included words) baz";
    let templates = new Templates(fixture("macros"));
    let [result, errors] = await render(input, {}, renderPrerequisiteFromURL, {
      templates,
    });
    expect(result).toEqual(expected);
    expect(errors).toEqual([]);
  });

  it("errors in included macros are reported", async () => {
    let templates = new Templates(fixture("macros"));
    let [result, errors] = await render(
      "foo{{includeError}}bar",
      {},
      renderPrerequisiteFromURL,
      {
        templates,
      }
    );
    expect(result).toEqual("foo{{includeError}}bar");
    expect(errors.length).toBe(1);
    expect(errors[0]).toBeInstanceOf(MacroExecutionError);
    expect(errors[0]).toHaveProperty("line");
    expect(errors[0]).toHaveProperty("column");
  });
});
