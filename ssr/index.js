import path from "path";

import React from "react";
import { StaticRouter } from "react-router-dom/server";
import ReactPDF from "@react-pdf/renderer";

import { App } from "../client/src/app";
import { MyDocument } from "../client/src/document/pdf";
import render from "./render";

// This is necessary because the ssr.js is in dist/ssr.js
// and we need to reach the .env this way.
require("dotenv").config({
  path: path.join(__dirname, "..", process.env.ENV_FILE || ".env"),
});

/**
 * Transform the `related_content` object for this document. For each node:
 * - rename `mdn_url` to `uri`
 * - use `short_title` instead of `title`, if it is available
 * - delete `short_description`
 * - set `isActive` for the node whose `uri` matches this document's `mdn_url`
 * - set `open` for nodes which are active or which have an active child
 *
 * This /mutates/ the document data.
 */
function fixRelatedContent(document) {
  function fixBlock(node) {
    if (node.mdn_url) {
      // always expect this to be a relative URL
      if (!node.mdn_url.startsWith("/")) {
        throw new Error(
          `Document's .mdn_url doesn't start with / (${node.mdn_url})`
        );
      }
      // Complicated way to rename an object key.
      node.uri = node.mdn_url;
      delete node.mdn_url;
    }

    // The sidebar only needs a 'title' and doesn't really care if
    // it came from the full title or the 'short_title'.
    node.title = node.short_title || node.title;
    delete node.short_title;
    // At the moment, we never actually use the 'short_description'
    // so no use including it.
    delete node.short_description;

    // isActive means that this node is a link to the current document
    // open means that this node or one of its children is a link to the current document
    if (node.uri === document.mdn_url) {
      node.open = true;
      node.isActive = true;
    }

    if (node.content) {
      for (const child of node.content) {
        fixBlock(child);
        if (child.open) {
          node.open = true;
        }
      }
    }
  }

  if (document.related_content) {
    document.related_content.forEach((block) => {
      fixBlock(block);
    });
  }
}

export function prepareDoc(doc) {
  // always expect this to be a relative URL
  if (!doc.mdn_url.startsWith("/")) {
    throw new Error(
      `Document's .mdn_url doesn't start with / (${doc.mdn_url})`
    );
  }

  // Stumptown produces a `.related_content` for every document. But it
  // contains data that is either not needed or not appropriate for the way
  // we're using it in the renderer. So mutate it for the specific needs
  // of the renderer.
  fixRelatedContent(doc);
}

export function renderDocHTML(doc, url) {
  prepareDoc(doc);
  return renderHTML(url, { doc });
}

export function renderHTML(url, context) {
  return render(
    React.createElement(
      StaticRouter,
      { location: url, context },
      React.createElement(App, context)
    ),
    context
  );
}

export function renderPDFFile(url, context) {
  const app = React.createElement(MyDocument, { url, ...context });
  ReactPDF.render(app, "/tmp/example.pdf");
}
export function renderPDFStream(url, context) {
  const app = React.createElement(MyDocument, { url, ...context });
  return ReactPDF.renderToStream(app);
}
