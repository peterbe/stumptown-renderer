const path = require("path");

const STATIC_ROOT = path.join(__dirname, "../client/build");
const DISABLE_CATCHALL = Boolean(
  JSON.parse(process.env.SERVER_DISABLE_CATCHALL || "false")
);

module.exports = { STATIC_ROOT, DISABLE_CATCHALL };
