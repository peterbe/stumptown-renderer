const serverExports = {};
if (JSON.parse(process.env.TESTING_START_SERVER || "false")) {
  serverExports.server = {
    // This is the .env file here inside the 'testing/' directory.
    // This is needed so that the server that gets started get the right
    // environment variables specifically for the functional test suite.
    command: "ENV_FILE=.env node ../server/static.js",
    port: 5000,
    host: "localhost",
    debug: true, // XXX Not sure that the harm is of having this on
  };
}

module.exports = {
  ...serverExports,
  launch: {
    headless: !JSON.parse(process.env.TESTING_OPEN_BROWSER || "false"),
  },
};
