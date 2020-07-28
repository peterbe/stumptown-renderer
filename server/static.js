/**
 * THIS NEEDS AN EXPLANATION
 */

const express = require("express");

const { staticMiddlewares } = require("./middlewares");

const app = express();
app.use(express.json());

app.use(staticMiddlewares);

// TEMPT
// Used for headless test sanity checking
app.get("/_ping", (req, res) => {
  res.send("pong");
});

const PORT = parseInt(process.env.SERVER_PORT || "5000");
app.listen(PORT, () => console.log(`Listening on port ${PORT}`));
