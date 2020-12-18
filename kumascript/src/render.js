/**
 * This file defines a render() function that takes as input a string
 * of text containing embedded KumaScript macros and asynchronously
 * returns a string in which the embedded macros have been
 * expanded. The render() function itself does not include any
 * asynchronous code, but macros may be asynchronous (they can make
 * HTTP requests, and use `await` for example), so the render() method
 * is declared `async`.
 *
 * Macros are embedded in source documents within pairs of curly
 * braces {{...}}. The Parser object of parser.js is used to extract
 * macro invocations (which can include arguments) and strings of
 * constant text from the source document.
 *
 * A Templates object (which represents a directory of EJS templates) is
 * used to render individual macros.
 *
 * When a macro is rendered, it takes a context object that defines
 * the values available to the macro. These values come from three
 * sources:
 *
 *   1) The macro API defined by the Environment class.
 *
 *   2) A context object passed to render(). This object defines
 *      values such as env.locale and env.title that are specific to
 *      the page being rendered.
 *
 *   3) An object that represents the arguments (if any) for a single
 *      macro invocation within a page. These are values that appear
 *      in the source document as part of the macro, and are bound to
 *      names $0, $1, etc.
 *
 * To render an HTML document that includes macro invocations, call
 * the render() function passing:
 *
 *   - the text of the page to be rendered
 *
 *   - the environment object that defines per-page values such as
 *     locale, title and slug.
 *
 * @prettier
 */
const Parser = require("./parser.js");
const Templates = require("./templates.js");
const Environment = require("./environment.js");
const {
  MacroInvocationError,
  MacroNotFoundError,
  MacroCompilationError,
  MacroExecutionError,
  MacroRedirectedLinkError,
  MacroBrokenLinkError,
  MacroWrongXRefError,
  MacroDeprecatedError,
  MacroPagesError,
} = require("./errors.js");

const defaultTemplates = new Templates();

function normalizeMacroName(name) {
  return name.replace(/:/g, "-").toLowerCase();
}

async function render(
  source,
  pageEnvironment,
  renderPrerequisiteFromURL,
  { templates = null } = {}
) {
  let tokens;
  try {
    tokens = Parser.parse(source);
  } catch (e) {
    // If there are any parsing errors in the input document
    // we can't process any of the macros. Return early with a MacroInvocationError
    // which contains useful information to the caller.
    // Note that rendering errors in the macros are different;
    // we handle these individually below.
    throw new MacroInvocationError(e, source);
  }

  // The default templates are only overridden during testing.
  templates = templates || defaultTemplates;

  // If a mode (either 'render' or 'remove') and a list of macro names
  // was passed-in for the "selective_mode" environment variable, then
  // we only process (according to the mode) those selected macros,
  // ignoring all others.
  let selectMacros;
  let selectiveMode = false;
  if (pageEnvironment.selective_mode) {
    [selectiveMode, selectMacros] = pageEnvironment.selective_mode;
    // Normalize the macro names for the purpose of robust comparison.
    selectMacros = selectMacros.map((name) => normalizeMacroName(name));
  }

  // Loop through the tokens, rendering the macros and collecting
  // the results. We detect duplicate invocations and only render
  // those once, on the assumption that their output will be the
  // same each time.
  let output = "";
  const errors = [];
  const signatureToResult = new Map();
  // This tracks the token for the "recordNonFatalError()" function.
  let currentToken;
  // This tracks the result object for the "recordNonFatalError()" function.
  let currentResult;

  function recordNonFatalError(kind, message, redirectInfo = null) {
    let NonFatalErrorClass;
    let args = [new Error(message), source, currentToken];
    if (kind === "deprecated") {
      NonFatalErrorClass = MacroDeprecatedError;
    } else if (kind === "broken-link") {
      NonFatalErrorClass = MacroBrokenLinkError;
    } else if (kind === "redirected-link") {
      NonFatalErrorClass = MacroRedirectedLinkError;
      args.push(redirectInfo);
    } else if (kind === "bad-pages") {
      NonFatalErrorClass = MacroPagesError;
    } else if (kind === "wrong-xref-macro") {
      NonFatalErrorClass = MacroWrongXRefError;
    } else {
      throw Error(`unsupported kind of non-fatal error requested: "${kind}"`);
    }
    const macroError = new NonFatalErrorClass(...args);
    if (!currentResult.errors.nonFatal) {
      currentResult.errors.nonFatal = [];
    }
    currentResult.errors.nonFatal.push(macroError);
    return macroError;
  }

  // Create the Environment object that we'll use to render all of
  // the macros on the page, and provide a way for macros or the
  // utilities they call to record non-fatal errors.
  let environment = new Environment(
    {
      ...pageEnvironment,
      recordNonFatalError,
    },
    templates,
    renderPrerequisiteFromURL
  );

  // Loop through the tokens
  for (let token of tokens) {
    // We only care about macros; skip anything else
    if (token.type !== "MACRO") {
      // If it isn't a MACRO token, it's a TEXT token.
      output += token.chars;
      continue;
    }

    let macroName = normalizeMacroName(token.name);

    if (selectiveMode) {
      if (selectMacros.includes(macroName)) {
        if (selectiveMode === "remove") {
          continue;
        }
      } else {
        // For un-selected macros, just use the original macro
        // source text for the output.
        output += source.slice(
          token.location.start.offset,
          token.location.end.offset
        );
        continue;
      }
    }

    // Check to see if we're already processing this exact
    // macro invocation. To do that we need a signature for
    // the macro. When the macro has json arguments we want to
    // ignore their order, so we do some tricky stringification
    // here in that case.
    if (token.args.length === 1 && typeof token.args[0] === "object") {
      // the json args case
      let keys = Object.keys(token.args[0]);
      keys.sort();
      token.signature = macroName + JSON.stringify(token.args[0], keys);
    } else {
      // the regular case: args is just an array of strings
      token.signature = macroName + JSON.stringify(token.args);
    }

    currentToken = token;
    currentResult = {
      output: null,
      errors: {
        fatal: null,
        nonFatal: null,
      },
    };

    // If the token signature is already in the map, then we've
    // already run the macro. We're only going to use the prior run
    // if there were no errors. If there were errors in the prior run,
    // let's re-run the macro in order to capture the context in fresh
    // errors.
    const priorResult = signatureToResult.get(token.signature);

    if (
      priorResult &&
      !priorResult.errors.fatal &&
      !priorResult.errors.nonFatal
    ) {
      currentResult.output = priorResult.output;
    } else {
      if (!priorResult) {
        signatureToResult.set(token.signature, currentResult);
      }
      // Now start rendering this macro.
      try {
        currentResult.output = await templates.render(
          macroName,
          environment.getExecutionContext(token.args)
        );
      } catch (e) {
        let macroError;
        if (
          e instanceof ReferenceError &&
          e.message.startsWith("Unknown macro")
        ) {
          // The named macro does not exist
          macroError = new MacroNotFoundError(e, source, token);
        } else if (e.name === "SyntaxError") {
          // There was a syntax error compiling the macro
          macroError = new MacroCompilationError(e, source, token);
        } else {
          // There was a runtime error executing the macro
          macroError = new MacroExecutionError(e, source, token);
        }
        currentResult.errors.fatal = macroError;
        // There was a fatal error while rendering this macro, so
        // just use the original macro source text for the output.
        currentResult.output = source.slice(
          token.location.start.offset,
          token.location.end.offset
        );
      }
    }
    output += currentResult.output;
    if (currentResult.errors.fatal) {
      errors.push(currentResult.errors.fatal);
    } else if (currentResult.errors.nonFatal) {
      for (const error of currentResult.errors.nonFatal) {
        errors.push(error);
      }
    }
  }
  return [output, errors];
}

module.exports = { normalizeMacroName, render };
