/**
 * THIS NEEDS AN EXPLANATION
 */

const express = require("express");

const { staticMiddlewares } = require("./middlewares");

const app = express();
app.use(express.json());

// Lowercase every request because every possible file we might have
// on disk is always in lowercase.
// This only helps when you're on a filesystem (e.g. Linux) that is case
// sensitive.
app.use(staticMiddlewares);

// TEMPT
// Used for headless test sanity checking
app.get("/_ping", (req, res) => {
  res.send("pong");
});

const PORT = parseInt(process.env.SERVER_PORT || "5000");
app.listen(PORT, () => console.log(`Listening on port ${PORT}`));
