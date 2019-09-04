import cheerio from "cheerio";
import fs from "fs";
import path from "path";
import { renderToString } from "react-dom/server";

function readBuildHtml() {
  return fs.readFileSync(
    path.resolve(__dirname, "../../client/build/index.html"),
    "utf8"
  );
}

let buildHtml = "";
if (process.env.NODE_ENV !== "development") {
  // read it once
  buildHtml = readBuildHtml();
  if (buildHtml.indexOf('<div id="root"></div>') === -1) {
    throw new Error(
      'The server depends on being able to inject into <div id="root"></div>'
    );
  }
}

export default (renderApp, options) => {
  if (process.env.NODE_ENV === "development") {
    // Reread on every request
    buildHtml = readBuildHtml();
  }
  const $ = cheerio.load(buildHtml);

  const rendered = renderToString(renderApp);

  let pageTitle = "MDN Web Docs"; // default

  const { document } = options;

  if (document) {
    // Use the document's title instead
    pageTitle = document.title;

    // // XXX We *could* just expose some absolute minimal here. Just enough
    // // for the React Document component to know it doesn't need to re-render.
    // const escapeDocumentJson = JSON.stringify(document).replace("</", "<\\/");
    // const documentDataTag = `
    // <script id="documentdata" type="application/json">${escapeDocumentJson}</script>
    // `.trim();
    // $("#root").after(documentDataTag);
  }

  $("title").text(pageTitle);

  $("#root").html(rendered);

  // Every script tag that create-react-app inserts, make them defer
  $("body script[src]").attr("defer", "");

  return $.html();
};
