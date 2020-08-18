# Deployer

The Yari deployer does two things. First, it's used to upload document
redirects, pre-built document pages, static files (e.g. JS, CSS, and
image files), and sitemap files into an existing AWS S3 bucket.
Since we serve MDN from a S3 bucket via a CloudFront CDN, this is the
way we upload a new version of the site.

Second, it is used to update and publish changes to existing AWS Lambda
functions. For example, we use it to update and publish new versions of
a Lambda function that we use to transform incoming document URL's into
their corresponding S3 keys.

## Getting started

You can install it globally or in a virtualenv environment. Whichever you
prefer.

```sh
cd deployer
poetry install
poetry run deployer --help
```

Please refer to the [`boto3` documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration)
with regards to configuring AWS access credentials.

## Uploads

The `poetry run deployer upload` command uploads files as well as redirects
into an existing S3 bucket. As input, it takes a directory which contains the
files that should be uploaded, but it also needs to know where to find
any redirects that should be uploaded. By default it searches for redirects
within the content directories specified by `--content-root` (or
`CONTENT_ROOT`) and `--content-translated-root` (or `CONTENT_TRANSLATED_ROOT`).
It does this by searching for `_redirects.txt` files within those directories,
converting each line in a `_redirects.txt` file into an AWS S3 redirect key.
The files and redirects can be uploaded into the S3 bucket's root, or instead
into a sub-folder of the root (`--folder` option), which is what we do when
uploading experimental versions of the site.

Currently, we have three main S3 buckets that we upload into: `dev` (for
experimental or possible versions of the site), `stage`, and `prod`.

When uploading files (not redirects), the deployer is intelligent about
what it uploads. If only uploads files whose content has changed, skipping
the rest. However, since the `cache-control` attribute of a file is not
considered part of its content, if you'd like to change the `cache-control`
from what's in S3, it's important to use the `--force-refresh` option to
ensure that all files are uploaded with fresh `cache-control` attributes.

Redirects are always uploaded.

### Examples

```sh
cd deployer
poetry run deployer upload --bucket dev --folder pr1234 ../client/build
```

```sh
cd deployer
poetry run deployer upload --bucket prod ../client/build
```

## Updating Lambda Functions

The command:

```sh
cd deployer
poetry run deployer update-lambda-functions
```

will discover every folder that contains a Lambda function, create a
deployment package (Zip file) for each one by running:

```sh
yarn make-package
```

and if the deployment package is different from what is already in AWS,
it will upload and publish a new version.

## Environment variables

The following environment variables are supported.

- `DEPLOYER_BUCKET_NAME` is equivalent to using `--bucket`
- `DEPLOYER_NO_PROGRESSBAR` is equivalent to using `--no-progressbar`
- `DEPLOYER_CACHE_CONTROL` can be used to specify the `cache-control`
  header for all non-hashed files that are uploaded (the default is
  `3600` or one hour)
- `DEPLOYER_HASHED_CACHE_CONTROL` can be used to specify the `cache-control`
  header for all hashed files (e.g., `main.3c12da89.chunk.js`) that are
  uploaded (the default is `31536000` or one year)
- `DEPLOYER_MAX_WORKERS_PARALLEL_UPLOADS` controls the number of worker
  threads used when uploading (the default is `50`)
- `CONTENT_ROOT` is equivalent to using `--content-root`
- `CONTENT_TRANSLATED_ROOT` is equivalent to using `--content-translated-root`

## Contributing

You need to have [`poetry` installed on your system](https://python-poetry.org/docs/).
Now run:

```sh
cd deployer
poetry install
```

That should have installed the CLI:

```sh
poetry run deployer
```

If you wanna make a PR, make sure it's formatted with `black` and
passes `flake8`.

You can check that all files are `flake8` fine by running:

```sh
flake8 deployer
```

And to check that all files are formatted according to `black` run:

```sh
black --check deployer
```
