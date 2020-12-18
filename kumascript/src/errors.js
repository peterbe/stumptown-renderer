/**
 * Error classes that can be thown when trying to render the macros on a page.
 * @prettier
 */

/**
 * This is the common superclass of the other error classes here.
 * It includes the code for excerpting the portion of the document that the
 * error occurs in and drawing an ASCII art arrow to point at it.
 */
class SourceCodeError {
  constructor(name, error, source, line, column, macroName, fatal = true) {
    this.name = name;
    this.error = error;
    // So it becomes available in JSON.stringfy when doing that on
    // instances of this class. Otherwise we'd need to monkey-patch
    // the `.toJSON` of `Error` which feels fragile.
    this.errorStack = error.stack;
    this.offset = 0;
    this.line = line;
    this.column = column;
    this.macroName = macroName;
    this.sourceContext = this.getSourceContext(source);
    this.fatal = fatal;
  }

  get key() {
    // Generates a unique key for this error.
    return [
      this.name,
      this.error.message,
      this.line,
      this.column,
      this.filepath,
    ].join("\n");
  }

  updateOffset(value) {
    // Update the "offset" property to account for things like front-matter in the
    // source. If the offset changes, this method will update related information.
    // NOTE: We're not using a getter/setter for "offset", which would be a more
    //       robust interface, so that the code that converts this instance to/from
    //       JSON can remain simple.
    if (this.offset !== value) {
      // First, let's calculate the change in offset.
      const offsetDelta = value - this.offset;
      // Now, let's update things. First, the offset itself.
      this.offset += offsetDelta;
      // Next, let's update the line number.
      this.line += offsetDelta;
      // Finally, let's update the line numbers in the source context to reflect the new offset.
      this.sourceContext = this.sourceContext.replace(
        /^\s{0,4}(\d{1,5}) \| /gm,
        (match, p1) => {
          return (parseInt(p1) + offsetDelta).toString().padStart(5) + " | ";
        }
      );
    }
  }

  updateFileInfo(fileInfo) {
    this.filepath = fileInfo.path;
    // The extra `- 1` is because of the added newline that
    // is only present because of the serialized linebreak.
    this.updateOffset(fileInfo.frontMatterOffset - 1);
    return this;
  }

  // TODO(djf): a lot of our HTML documents have really long lines and
  // showing line-oriented errors when the column number is > 100
  // doesn't really make sense. Perhaps we can modify this function to
  // show the relevant context in a more useful way.
  getSourceContext(source) {
    function arrow(column) {
      let arrow = "";
      for (let i = 0; i < column + 7; i++) {
        arrow += "-";
      }
      return arrow + "^";
    }

    function formatLine(i, line) {
      let lnum = ("      " + (i + 1)).substr(-5);
      return lnum + " | " + line;
    }

    let lines = source.split("\n");

    // Work out a range of lines to show for context around the error,
    // 2 before and after.
    let errorLine = this.line - 1;
    let startLine = Math.max(errorLine - 2, 0);
    let endLine = Math.min(errorLine + 3, lines.length);

    // Assemble the lines of error context, inject the column pointer
    // at the appropriate spot after the error line.
    var context = [];
    for (var i = startLine; i < endLine; i++) {
      context.push(formatLine(i, lines[i]));
      if (i == errorLine) {
        context.push(arrow(this.column));
      }
    }
    return context.join("\n");
  }

  toString() {
    return (
      `${this.name} error on ${this.macroName} ` +
      `at line ${this.line}, column ${this.column} in:\n` +
      `${this.sourceContext}\nOriginal error: ${this.error.message}`
    );
  }
}

/**
 * A MacroInvocationError is thrown if we can't parse the HTML document
 * because it uses incorrect syntax for invoking macros. In this case
 * the error object is from the parser class and tells us the location
 * of the error.
 */
class MacroInvocationError extends SourceCodeError {
  constructor(error, source) {
    // If the error is not a SyntaxError, with a location property then
    // just return it instead of creating a wrapper object
    if (error.name !== "SyntaxError" || error.location === undefined) {
      return error;
    }

    super(
      "MacroInvocationError",
      error,
      source,
      error.location.start.line,
      error.location.start.column,
      error.name
    );
  }
}

/**
 * A MacroNotFoundError is thrown when an HTML document uses
 * a macro that does not exist. The error message shows the location of the
 * macro in the HTML document, which it determines from the token argument.
 */
class MacroNotFoundError extends SourceCodeError {
  constructor(error, source, token) {
    super(
      "MacroNotFoundError",
      error,
      source,
      token.location.start.line,
      token.location.start.column,
      token.name
    );
  }
}

/**
 * A MacroCompilationError is thrown when there is an exception during
 * template compilation. The error message shows the location of the
 * macro in the HTML document and also includes the underlying error message.
 */
class MacroCompilationError extends SourceCodeError {
  constructor(error, source, token) {
    super(
      "MacroCompilationError",
      error,
      source,
      token.location.start.line,
      token.location.start.column,
      token.name
    );
  }
}

/**
 * A MacroExecutionError is thrown when there is an exception during
 * template rendering. The error message shows the location of the
 * macro in the HTML document and also includes the error message
 * from the underlying runtime error.
 */
class MacroExecutionError extends SourceCodeError {
  constructor(error, source, token, fatal = true) {
    super(
      "MacroExecutionError",
      error,
      source,
      token.location.start.line,
      token.location.start.column,
      token.name,
      fatal
    );
  }
}

/**
 * A MacroRedirectedLinkError is a special case of MacroExecutionError.
 */
class MacroRedirectedLinkError extends MacroExecutionError {
  constructor(error, source, token, redirectInfo) {
    super(error, source, token, false);
    this.name = "MacroRedirectedLinkError";
    this.macroSource = source.slice(
      token.location.start.offset,
      token.location.end.offset
    );
    this.redirectInfo = { ...redirectInfo };
  }
}

/**
 * A MacroBrokenLinkError is a special case of MacroExecutionError.
 */
class MacroBrokenLinkError extends MacroExecutionError {
  constructor(error, source, token) {
    super(error, source, token, false);
    this.name = "MacroBrokenLinkError";
    this.macroSource = source.slice(
      token.location.start.offset,
      token.location.end.offset
    );
  }
}

class MacroWrongXRefError extends MacroBrokenLinkError {
  constructor(error, source, token) {
    super(error, source, token);
    this.name = "MacroWrongXRefError";
  }
}

/**
 * A MacroDeprecatedError is a special case of MacroExecutionError.
 */
class MacroDeprecatedError extends MacroExecutionError {
  constructor(error, source, token) {
    super(error, source, token, false);
    this.name = "MacroDeprecatedError";
    this.macroSource = source.slice(
      token.location.start.offset,
      token.location.end.offset
    );
  }
}

/**
 * A MacroLiveSampleError is a special case of MacroExecutionError.
 */
class MacroLiveSampleError extends MacroExecutionError {
  constructor(error, source, token) {
    super(error, source, token, true);
    this.name = "MacroLiveSampleError";
    this.macroSource = source.slice(
      token.location.start.offset,
      token.location.end.offset
    );
  }
}

/**
 * A MacroPagesError is when we try to get other sub-pages in sidebars
 * based on optimistically combining names to make URLs.
 */
class MacroPagesError extends MacroExecutionError {
  constructor(error, source, token) {
    super(error, source, token, false);
    this.name = "MacroPagesError";
    this.macroSource = source.slice(
      token.location.start.offset,
      token.location.end.offset
    );
  }
}

module.exports = {
  SourceCodeError,
  MacroInvocationError,
  MacroNotFoundError,
  MacroCompilationError,
  MacroExecutionError,
  MacroRedirectedLinkError,
  MacroBrokenLinkError,
  MacroWrongXRefError,
  MacroDeprecatedError,
  MacroLiveSampleError,
  MacroPagesError,
};
