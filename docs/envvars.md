# Environment Variables

Most things can be controlled via environment variables. Some things are
specific to the client and some are specific to the builder etc.

This document attempts to describe each environment variable.

## General

### `EDITOR`

#### Default: n/a

This is a general OS, optional, environment variable and it's needed
if you want the ability to go from viewing a rendered document to
opening the source in your preferred editor. It's needed for the
"Edit this page in your editor" button to work.

## Builder

For the builder, a lot of environment variables can be overridden with
CLI arguments.

The general rule is that environment variables specific to the builder are
all prefixed with `CONTENT_`. E.g. `CONTENT_ROOT`

### `CONTENT_ROOT`

**Default: `content/files`**

Where the files are.

### `CONTENT_ARCHIVED_ROOT`

**Default: `null`** (meaning, not set nor included)

If you want to build archive content (no KumaScript rendering) you
can say where the location to the folder is.

**Example: `/tmp/mdn-archive-content`**

### `BUILD_FOLDERSEARCH`

**Default: ``** (meaning, none)

**Example: `web/css,web/html`**

Applicable if you run batches of builds but want to limit it to only the
folders you care about.
When doing a batch build, it can be very time-consuming so just doing
one or two sub-folders will speed things up.

### `BUILD_FILES`

**Default: `[]`**

A comma or newline separated list of file paths. Can be absolute or relative
to the `CONTENT_ROOT`.

### `BUILD_OUT_ROOT`

**Default: `client/build`**

Location into which things should be built.

### `BUILD_FLAW_LEVELS`

**Default: `*:warn`**

To set this you need to know what all the possible flaw identifiers are called.
The complete set can be found in `content/src/constants.js` in
`VALID_FLAW_CHECKS`. And the level values are `error`, `warn`, or `ignore`.

The value of this environment variable is a comma separated string. Any
identifier or level value that is not recognized will throw an error.

You can mix and match like this for example:

```sh
macros:error, broken_links: warn, bad_bcd_queries: ignore
```

Anything _not_ mentioned defaults to `ignore`, so the above example
is equivalent to:

```sh
macros:error, broken_links: warn
```

When a flaw is discovered, if the level is `error` it will, halt the build
and throw an error. It will halt on the first flaw error encountered.
If the level is `warn` it will inject the flaw message into the built
`index.json` file which you can view when rendering the document on
`http://localhost:3000/`.

### `BUILD_FIX_FLAWS`

**Default: `false`**

Whether fixable flaws should be fixed when building documents.

### `BUILD_FIX_FLAWS_DRY_RUN`

**Default: `false`**

When set to true (with the `BUILD_FIX_FLAWS` flag) it will only print out
information about fixable flaws instead of actually fixing it on disk.

### `BUILD_FIX_FLAWS_VERBOSE`

**Default: `false`**

### `BUILD_LIVE_SAMPLES_BASE_URL`

**Default: `https://mdn.mozillademos.org`**

When generating live samples `<iframe>` tags, the `src` attribute gets this
set as a prefix. The ultimate reason why it's meant to be different is
because the security of the `iframe`'s content has not been audited as
carefully as the rest of the site.

When doing local development, it's recommended to set this to
`http://localhost:5000` in your personal `.env`.

### `BUILD_INTERACTIVE_EXAMPLES_BASE_URL`

**Default: `https://interactive-examples.mdn.mozilla.net`**

The base URL used in the Interactive Example iframes.

### `BUILD_MACROS_USED_LOGFILE`

**Default `not set`**

This needs to be be a file path.
E.g. `export BUILD_MACROS_USED_LOGFILE=/tmp/macros-used.log`
It will write one line for every (normalized) macro name used and its
arguments in rendering.

This is an advanced feature to help potentially figuring out which
kumascript macros, in the source, that aren't used.

### `BUILD_GOOGLE_ANALYTICS_ACCOUNT`

**Default: `''`**

If set, the rendered HTML will have a Google Analytics snippet.
For example, to test use: `export BUILD_GOOGLE_ANALYTICS_ACCOUNT=UA-00000000-0`.
By default it's disabled (empty string).

### `BUILD_GOOGLE_ANALYTICS_DEBUG`

**Default: `false`**

If true, and when `BUILD_GOOGLE_ANALYTICS_ACCOUNT` is truthy, when it injects
the Google Analytics script tag it will use
`<script src="https://www.google-analytics.com/anaytics_debug.js"></script>`
instead which triggers additional console logging which is useful for developers.

### `BUILD_SPEEDCURVE_LUX_ID`

**Default: `''`**

You can get it here on [this settings page](https://speedcurve.com/mozilla-add-ons/mdn/settings/lux/)
which will give you the ID in the snippet shown there. Also, try to match
this with the domains in those settings to match where we deploy it.

### `BUILD_ALWAYS_NO_ROBOTS`

**Default: `false`**

This exists so we can forcibly always include
`<meta name="robots" content="noindex, nofollow">` into the HTML no matter what.
For example, on our stage or dev builds, none of the documents should be indexed,
so we'll set `BUILD_ALWAYS_NO_ROBOTS` to `true`.

We use this to make absolutely sure that no dev or stage build ever gets into
the Google index. Thankfully we _always_ used a canonical URL
(`<link rel="canonical" href="https://developer.mozilla.org/$uri">`) as a "second
line of defense" for dev/stage URLs that are public.

## Server

### `SERVER_PORT`

**Default: `5000`**

Usually the `server` module is started with `foreman` (the `nf` command)
and this is the default port.

### `SERVER_WEBSOCKET_PORT`

**Default: `8080`**

This is the port for the WebSocket server, which is started when you run `yarn start`.

### `SERVER_STATIC_ROOT`

**Default `../client/build`**

If you want to serve static files some a completely different directory.

## Testing

### `TESTING_OPEN_BROWSER`

**Default: `false`**

When running the `jest-puppeteer` test suites, if you set this to `true`,
it will open a browser on every page navigation.

It might just flash by too quickly, so consider putting in
`await jestPuppeteer.debug()` inside the test function to slow it down.

### `TESTING_START_SERVER`

**Default: `false`**

When `jest-puppeteer` starts the `jest` tests, if this variable is set
to `true` it will execute `node ../server/index.js` to start the `server`
on `localhost:5000`.

In most cases, on your laptop it's better to start the server yourself
in a separate terminal and then run the headless tests in another.

For more information, see the `testing/README.md`.

## Client

### `HOST`

**Default: `localhost`**

Suggested value: `HOST=localhost.org`

For browser cookies to work as expected, you need to use the same host name
in Yari and in Kuma. The port numbers can be different. That means that any
cookies you picked up (e.g. `sessionid`) over on `http://localhost.org:8000`
will be automatically included in XHR calls on `http://localhost.org:3000`.

Note that even if you set this, you can still continue to
use `http://localhost:3000`.

### `REACT_APP_KUMA_HOST`

**Default: `not set`**

When doing local development in Yari, the "Sign in" URL depends on this.
If you're running the dev server (i.e. `localhost:3000` or `localhost.org:3000`)
the link to sign in with Kuma needs this to be set.

The suggested value is to set this to set
`REACT_APP_KUMA_HOST=localhost.org:8000` and now sign in links go to that
host name instead. That means you can log in _from_ Yari with a single click.

### `REACT_APP_DISABLE_AUTH`

**Default: `false`**

This removes sign-in and `whoami` XHR fetching.
Useful when using Yari purely for content editing as authentication is then not required.

### `REACT_APP_CRUD_MODE`

**Default: `NODE_ENV==='development'`**

Basically, these are the optional, lazy-loaded features of the app that only
make sense when you're working on authoring the content. For example the
Toolbar bar appears based on this.

It defaults to `NODE_ENV==='development'` if not set which means that
it's enable by default when doing development with the `localhost:3000`
dev server.

### `REACT_APP_NO_WATCHER`

**Default: `false`**

Disable file watching and hot reloading on content change.
Also disables search index updates.
