#!/usr/bin/env node
const path = require("path");

const program = require("@caporal/core").default;

const {
  DATABASE_URL,
  EXCLUDE_SLUG_PREFIXES,
  IMPORT_LOCALES,
} = require("./constants");
const { VALID_LOCALES } = require("../content");

const runImporter = require("./import");
// const runContributorsDump = require("./contributors");
const runContributorsDump = require("./contributors2");
const runMakePopularitiesFile = require("./popularities");

program
  .version(require("./package.json").version)
  .command("import")
  .help("Turn MySQL data into files on disk")
  .option("-l, --locales <locale>", "locales to limit on", {
    validator: program.ARRAY,
    default: IMPORT_LOCALES,
  })
  .option("--no-progressbar", "no progress bar but listing instead", {
    validator: program.BOOL,
  })
  .option("--start-clean", "delete anything created first", {
    validator: program.BOOL,
  })
  .option(
    "--skip-wiki-histories",
    "don't dump the $locale/_wikihistory.json files",
    {
      validator: program.BOOL,
    }
  )
  .option(
    "--exclude-prefixes <prefixes>",
    "slug prefixes to exclude (commas)",
    { validator: program.ARRAY, default: EXCLUDE_SLUG_PREFIXES.join(",") }
  )
  .argument("[URL]", "database url for connecting to MySQL", {
    validator: program.STRING,
    default: DATABASE_URL,
  })
  .action(({ args, options }) => {
    options.dbURL = args.url;
    return runImporter(options).catch((error) => {
      console.error("error while importing documents:", error);
      process.exit(1);
    });
  })

  .command("contributors")
  .help("Dump all the Wiki contributors to a CSV file")
  .argument("[URL]", "database url for connecting to MySQL", {
    validator: program.STRING,
    default: DATABASE_URL,
  })
  .action(({ args, options }) => {
    options.dbURL = args.url;
    return runContributorsDump(options).catch((error) => {
      console.error("error while dumping contributors:", error);
      process.exit(1);
    });
  })

  .command("makepopularities")
  .help("Transform a Google Analytics CSV report into a popularities.json file")
  .option(
    "--exclude-prefixes <prefixes>",
    "slug prefixes to exclude (commas)",
    { validator: program.ARRAY, default: EXCLUDE_SLUG_PREFIXES.join(",") }
  )
  .option("-l, --locales <locale>", "locales to limit on", {
    validator: (value) => {
      const checked = [];
      const locales = Array.isArray(value) ? value : Array(value);
      for (const locale of locales) {
        const localeLC = locale.toLowerCase();
        if (!VALID_LOCALES.has(localeLC)) {
          throw new Error(`'${locale}' is not a valid locale`);
        }
        checked.push(localeLC);
      }
      return checked;
    },
    default: [],
  })
  .option("-m, --max-uris <number>", "Number of URIs. Defaults to 20,000", {
    validator: program.INTEGER,
    default: 20000,
  })
  .option("-o, --outfile <path>", "Defaults to ./popularities.json", {
    validator: program.STRING,
    default: path.resolve("popularities.json"),
  })
  .argument("csvfile", "Google Analytics pages report CSV file")
  .action(({ args, options, logger }) => {
    return runMakePopularitiesFile(args.csvfile, options, logger).catch(
      (error) => {
        console.error("error generating JSON file:", error);
        process.exit(1);
      }
    );
  });

program.run();
