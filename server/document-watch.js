const path = require("path");
const { Worker } = require("worker_threads");

const WebSocket = require("ws");

const { SearchIndex } = require("../build");

let isReady = false;
const searchIndex = new SearchIndex();

const webSocketServer = new WebSocket.Server({
  port: parseInt(process.env.SERVER_WEBSOCKET_PORT || 8080),
});

function sendWebSocketMessage(message) {
  for (const client of webSocketServer.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(message));
    }
  }
}

const worker = new Worker(path.join(__dirname, "document-watch.worker.js"));
worker.on("message", (event) => {
  switch (event.type) {
    case "ready":
      isReady = true;
      searchIndex.sort();
      sendWebSocketMessage({ type: "SEARCH_INDEX_READY" });
      break;

    case "added":
    case "updated":
      searchIndex.add(event.document);
      if (isReady) {
        sendWebSocketMessage({
          type: "DOCUMENT_CHANGE",
          hasEditorSet: Boolean(process.env.EDITOR),
          change: event,
        });
      }
      break;

    case "deleted":
      // delete titles[event.filePath];
      break;

    default:
      console.error(new Error(`unknown event type: ${event.type}`));
  }
});

module.exports = {
  searchRoute(req, res) {
    res.json(isReady ? searchIndex.getItems()[req.params.locale] : null);
  },
};
