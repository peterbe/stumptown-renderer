const sanitizeFilename = require("sanitize-filename");

const CONTENT_DEVELOPMENT_DOMAIN = ".content.dev.mdn.mozit.cloud";

/*
 * NOTE: This function is derived from the function of the same name within
 *       ../../content/utils.js. It differs only in its final "join", which
 *       uses "/", as required by S3 keys, rather than "path.sep".
 */
function slugToFolder(slug) {
  return slug
    .replace(/\*/g, "_star_")
    .replace(/::/g, "_doublecolon_")
    .replace(/:/g, "_colon_")
    .replace(/\?/g, "_question_")
    .toLowerCase()
    .split("/")
    .map(sanitizeFilename)
    .join("/");
}

exports.handler = async (event, context) => {
  /*
   * Modify the request before it's passed to the S3 origin.
   */
  const request = event.Records[0].cf.request;
  const host = request.headers.host[0].value.toLowerCase();
  // Rewrite the URI to match the keys in S3.
  // NOTE: The incoming URI should remain URI-encoded.
  let newURI = slugToFolder(request.uri);
  if (newURI.includes("/docs/") && !newURI.endsWith("/index.json")) {
    if (!newURI.endsWith("/")) {
      newURI += "/";
    }
    newURI += "index.html";
  }
  request.uri = newURI;
  // Rewrite the HOST header to match the S3 bucket website domain.
  // This is required only because we're using S3 as a website, which
  // we need in order to do redirects from S3. NOTE: The origin is
  // considered a "custom" origin because we're using S3 as a website.
  request.headers.host[0].value = request.origin.custom.domainName;
  // Conditionally rewrite the path (prefix) of the origin.
  if (host.endsWith(CONTENT_DEVELOPMENT_DOMAIN)) {
    // When reviewing PR's, each PR gets its own subdomain, and
    // all of its content is prefixed with that subdomain in S3.
    request.origin.custom.path = `/${host.split(".")[0]}`;
  }
  return request;
};
