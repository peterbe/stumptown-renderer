const fs = require("fs");
const path = require("path");

const { execSync } = require("child_process");
const simpleGit = require("simple-git");

describe("fixing flaws", () => {
  const basePattern = path.join("testing", "content", "files");
  const pattern = path.join("web", "fixable_flaws");

  // If we don't specify this, doing things like `await git.diffSummary()`
  // will report files as they appear relative to the `.git/` root which
  // might be different from the current `process.cwd()` which makes it
  // impossible to look at file paths in a predictable way.
  const baseDir = path.resolve("..");

  async function getChangedFiles() {
    const git = simpleGit();
    const diff = await git.diffSummary();
    return diff.files
      .filter((f) => f.file.includes(basePattern) && f.file.includes(pattern))
      .map((f) => f.file);
  }

  beforeAll(async () => {
    // We assume we can test changes to the local git repo. If there were
    // already existing fixes to the fixtures we intend to actually change,
    // then tests are a no-go.
    // We basically want to check that none of the files we intend to mess
    // with, from jest, are already messed with.
    const files = await getChangedFiles();
    if (files.length) {
      // This is draconian but necessary.
      // See https://github.com/facebook/jest/issues/2713
      // which is closed but it't still not a feature for our version of
      // jest to have it so that it stops running the tests if you throw
      // an error in here.
      console.error(
        `Can't test these things when ${files} already has its own changes`
      );
      // Basically, if the files that we care to test changing, were
      // already changed (manually) then we can't run our tests and we
      // want to bail hard.
      process.exit(1);
    }
  });

  // Aka. clean up by checking out any unstaged file changes made by the tests
  afterAll(async () => {
    // Undo any changed files of the interesting pattern
    const git = simpleGit({
      baseDir,
    });
    const diff = await git.diffSummary();
    const files = diff.files
      .map((f) => f.file)
      .filter((f) => f.includes(basePattern) && f.includes(pattern));
    if (files.length) {
      try {
        await git.checkout(files);
      } catch (err) {
        // Otherwise any error here would be swallowed
        console.error(err);
        throw err;
      }
    }

    // XXX trigger a git reset here!!
  });

  // Got to test dry-run mode and non-dry-run mode in serial otherwise
  // tests might run those two things in parallel and it's not thread-safe
  // to change files on disk.
  // This is why this test does so much.
  test("build with options.fixFlaws", async () => {
    // The --no-cache option is important because otherwise, on consecutive
    // runs, the caching might claim that it's already been built, on disk,
    // so the flaw detection stuff never gets a chance to fix anything
    // afterwards.
    // Unlike 'yarn start' this one just does the building without first
    // preparing the build folder and ssr dist code.
    const command = "yarn workspace build start";

    const dryrunStdout = execSync(command, {
      cwd: baseDir,
      windowsHide: true,
      env: Object.assign(
        {
          BUILD_FIX_FLAWS: "true",
          BUILD_FIX_FLAWS_DRY_RUN: "true",
          BUILD_FOLDERSEARCH: pattern,
        },
        process.env
      ),
    }).toString();

    const regexPattern = /Would have modified "(.*)", if this was not a dry run/g;
    const dryRunNotices = dryrunStdout
      .split("\n")
      .filter((line) => regexPattern.test(line));
    expect(dryRunNotices.length).toBe(1);
    expect(dryRunNotices[0]).toContain(pattern);
    const dryrunFiles = await getChangedFiles();
    expect(dryrunFiles.length).toBe(0);

    // Now, let's do it without dry-run
    const stdout = execSync(command, {
      cwd: baseDir,
      windowsHide: true,
      env: Object.assign(
        {
          BUILD_FIX_FLAWS: "true",
          BUILD_FIX_FLAWS_DRY_RUN: "false",
          BUILD_FOLDERSEARCH: pattern,
        },
        process.env
      ),
    }).toString();
    expect(stdout).toContain(pattern);

    const files = await getChangedFiles();
    expect(files.length).toBe(1);
    const newRawHtml = fs.readFileSync(path.join(baseDir, files[0]), "utf-8");
    expect(newRawHtml).toContain("{{CSSxRef('number')}}");
    expect(newRawHtml).toContain('{{htmlattrxref("href", "a")}}');
    // Broken links that get fixed.
    expect(newRawHtml).toContain('href="/en-US/docs/Web/CSS/number"');
    expect(newRawHtml).toContain("href='/en-US/docs/Web/CSS/number'");
    expect(newRawHtml).toContain('href="/en-US/docs/Glossary/Bézier_curve"');
    expect(newRawHtml).toContain('href="/en-US/docs/Web/Foo"');
  });
});
