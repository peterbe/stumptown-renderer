import json
import os
import sys

from decouple import AutoConfig

config = AutoConfig(os.curdir)

CONTENT_ROOT = config("CONTENT_ROOT", default=None)
CONTENT_TRANSLATED_ROOT = config("CONTENT_TRANSLATED_ROOT", default=None)

DEFAULT_BUCKET_NAME = config("DEPLOYER_BUCKET_NAME", default="dev")

# When uploading a bunch of files, the work is done in a thread pool.
# If you use too many "workers" it might saturate your network meaning it's
# slower.
MAX_WORKERS_PARALLEL_UPLOADS = config(
    "DEPLOYER_MAX_WORKERS_PARALLEL_UPLOADS", default=50, cast=int
)

# E.g. /en-US/docs/Foo/Bar/index.html
DEFAULT_CACHE_CONTROL = config("DEPLOYER_CACHE_CONTROL", default=60 * 60, cast=int)
# E.g. '2.02b14290.chunk.css'
HASHED_CACHE_CONTROL = config(
    "DEPLOYER_HASHED_CACHE_CONTROL", default=60 * 60 * 24 * 365, cast=int
)

DEFAULT_NO_PROGRESSBAR = config(
    "DEPLOYER_NO_PROGRESSBAR",
    default=not sys.stdout.isatty() or bool(json.loads(os.environ.get("CI", "false"))),
    cast=bool,
)
