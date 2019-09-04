import React from "react";
import ReactDOM from "react-dom";
import "./index.scss";
import "typeface-zilla-slab";
import { App } from "./app";
// import * as serviceWorker from './serviceWorker';

const container = document.getElementById("root");

// If the `<div id="root">` was filled with stuff, it means the page was
// rendered on the server. That's a chance to "send a message" to the
// Document component (called from the App component depending on the URL)
// that the page is rendered fine and it doesn't need to re-render
// client-side.
let documentData = container.firstChild ? {} : null;

const app = <App document={documentData} />;
if (container.firstElementChild) {
  console.log("HYDRATE!");
  ReactDOM.hydrate(app, container);
} else {
  console.log("RENDER!");
  ReactDOM.render(app, container);
}

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA
// serviceWorker.unregister();
