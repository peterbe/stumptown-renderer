# Functional tests

This is a workspace dedicated to simulating all of Yari but with a
fixed set of content files.

Unlike testing the real content, this content "never changes". I.e. we
control it and know exactly what to expect to find.

The general idea is that you run the same build commands as regular
Yari but you point it to a different "content root" and then we run
tests (using `jest`) on what got built.

This way we can trigger specific `kumascript` macros and other effects
and know exactly what it should have become.

## Local development

When hacking on the functional tests it's important that you can do
this _without_ continuous integration (CI). That way you don't need to
wait for CI to run your every commit in a pull request (PR).

Also, you can start the full development environment with:

```bash
yarn start:functional
```

That will start the `watcher` (long-running), the `server` (Express
serve on `localhost:5000`), and the client (`create-react-app` on
`localhost:3000`).
To test it go to:
[http://localhost:3000/en-US/docs/Web/Foo](http://localhost:3000/en-US/docs/Web/Foo)
for example.

Now you should be able to make edits and notice automatic reloading.
But remember to unstage edits, otherwise it might break the automated
test suite.

The first thing to do is to run the _whole_ test suite now:

```bash
./testing/scripts/functional-test.sh
```

That includes all the important pre-build, build, and starting the `jest`
tests. But once you've run that once, you can "break it apart" and just
run the `jest` test suite and this you can run repeatedly:

```bash
yarn workspace testing run test
```

This assumes you've set the appropriate environment variables and built the
content. Alternatively, you can run `./testing/scripts/functional-test.sh`
which takes care of all of these things.

## Conditional testing in CI

In GitHub Actions, instead of trying to optimize certain tests, just skip
them if none of the files that affect the tests have changed.

One such example is the tests for the `kumascript` source code plus macros.
Use this technique heartly to speed up the continous integration.

## Caveats

- At the moment it might not work on Windows. It should. At least the "Local
  development"
- We should put all known `kumascript` macros in some form. At least the ones
  intend to support.
- There is no guidelines for how to add tests but feel free to pile on
  sample pages. You should not be afraid to add more.

## Writing headless tests

Headless tests are when we use [`puppeteer`](https://pptr.dev/) to view pages
rendered from the functional tests. If it helps, the non-headless tests
use `fs.readFileSync()` and `cheerio` etc. to inspect the created files in
`client/build/**`.

We use [`jest-puppeteer`](https://github.com/smooth-code/jest-puppeteer) and
its README is very relevant to help you write tests. Here's the link to
the [document for `expect-puppeteer`](https://github.com/smooth-code/jest-puppeteer/blob/master/packages/expect-puppeteer/README.md#api)
which is your best friend when writing headless tests.

To get started, open `testing/tests/headless.test.js` and make changes there.
If you need a new page to open, you need to add that to
`testing/content/files/...` first. Then it becomes possible to open it
based on the slug you typed.

Before running the tests, start the function dev server instance:

```sh
yarn start:functional
```

Before you proceed, appreciate that you can now open `http://localhost:5000`
and from there open any page (for example using the search) and what you
see in your browser is what you can expect to see in `jest-puppeteer` in
the tests.

In a separate terminal, run all the tests:

```sh
./testing/scripts/functional-test.sh
```

As you notice, that shell script actually does a lot. It prebuilds the
assets, builds the actual documents, and it runs _all_ `jest` tests.

To just run all `jest` tests, just run the last command:

```sh
yarn workspace testing run test
```

Which is just an alias to start `jest` which means you can apply your own
parameters. For example, this starts the `jest` watcher:

```sh
yarn workspace testing run test --watch
```

Once the `jest` watcher has started press <kbd>p</kbd> and type `headless`
and now it only (re-runs) tests the headless tests.

**Note!** that only in local development do you need to start the functional
server first. In GitHub Actions (CI), `jest-puppeteer` is instructed to start
the server as a setup (and teardown) step.

## Debugging headless tests

It's very likely that you'll want to see and test
what the headless browser sees. To help with that there are a couple of
useful tricks.

The first trick is to set the `TESTING_OPEN_BROWSER=true` environment
variable.

```sh
TESTING_OPEN_BROWSER=true yarn workspace testing run test --watch
```

Now, you'll see a browser window open and shut as the tests run.
It's unlikely that you're fast enough to see what it's in that browser
but what you can do is to "pause" the tests a little by injecting
this line (temporarily) into your test code:

```javascript
await jestPuppeteer.debug();
```

Another useful trick is to dump the DOM HTML on the console. You can
put this in anywhere:

```javascript
console.log(await page.content());
```

## Headless tests should only test static server

To run the functional tests you need a server (on `localhost:5000`) and
it should just be a static file server. You *can* use `yarn start:functional`
but that server has many tricks such as building on-the-fly.

A better server to use is:

```sh
yarn start:staticserver
```

Now you can run just the functional `jest` tests over and over:

```sh
export TESTING_START_SERVER=false  # should be false by default anyway
./testing/scripts/functional-test.sh
```

If in doubt, look at the file `.github/workflows/testing.yml` and what it does.
