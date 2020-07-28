const path = require("path");

const STATIC_ROOT = path.join(__dirname, "../client/build");

// Set this, using env var SERVER_DISABLE_CATCHALL when you want to make sure
// the server does NOT automatically build documents based on the URL.
const DISABLE_CATCHALL = Boolean(
  JSON.parse(process.env.SERVER_DISABLE_CATCHALL || "false")
);

module.exports = { STATIC_ROOT, DISABLE_CATCHALL };
