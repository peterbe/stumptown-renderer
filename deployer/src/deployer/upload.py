import concurrent.futures
import hashlib
import mimetypes
import re
from dataclasses import dataclass
from functools import cached_property
from itertools import chain
from pathlib import Path

import boto3
import click
from boto3.s3.transfer import S3TransferConfig

from .constants import (
    DEFAULT_CACHE_CONTROL,
    HASHED_CACHE_CONTROL,
    LOG_EACH_SUCCESSFUL_UPLOAD,
    MAX_WORKERS_PARALLEL_UPLOADS,
)
from .utils import StopWatch, fmt_size, iterdir, log

S3_MULTIPART_THRESHOLD = S3TransferConfig().multipart_threshold
S3_MULTIPART_CHUNKSIZE = S3TransferConfig().multipart_chunksize

NO_CACHE_VALUE = "no-store, must-revalidate"

hashed_filename_regex = re.compile(r"\.[a-f0-9]{8,32}\.")


def log_task(task):
    if task.error:
        if task.is_redirect:
            # With redirect upload tasks, we have to embellish the
            # error message.
            log.error(f"Failed to upload {task}: {task.error}")
        else:
            # With file upload tasks, the error message says it all.
            log.error(task.error)
    elif LOG_EACH_SUCCESSFUL_UPLOAD:
        log.success(task)


@dataclass
class Totals:
    """Class for keeping track of some useful totals."""

    failed: int = 0
    skipped: int = 0
    uploaded_files: int = 0
    uploaded_redirects: int = 0
    uploaded_files_size: int = 0

    def count(self, task):
        if task.skipped:
            self.skipped += 1
        elif task.error:
            self.failed += 1
        elif task.is_redirect:
            self.uploaded_redirects += 1
        else:
            self.uploaded_files += 1
            self.uploaded_files_size += task.size


class DisplayProgress:
    def __init__(self, total_count, show_progress_bar=True):
        if show_progress_bar:
            self.progress_bar = click.progressbar(
                fill_char="▋",
                show_pos=True,
                show_eta=False,
                show_percent=True,
                length=total_count,
            )
            self.failed_tasks = []
        else:
            self.progress_bar = None

    def __enter__(self):
        if self.progress_bar:
            self.progress_bar.__enter__()
            # Forces the progress-bar to show-up immediately.
            self.progress_bar.update(0)
        return self

    def __exit__(self, *exc_args):
        if self.progress_bar:
            self.progress_bar.__exit__(*exc_args)
            # Now that we've left the progress bar, let's report
            # any failed tasks.
            for task in self.failed_tasks:
                log_task(task)
        return False

    def update(self, task):
        if self.progress_bar:
            self.progress_bar.update(1)
            if task.error:
                self.failed_tasks.append(task)
        else:
            log_task(task)


class UploadTask:
    """
    Base class for indicating the common interface for all upload tasks.
    """

    error = None
    skipped = False
    is_redirect = False

    def upload(self):
        raise NotImplementedError()


class UploadFileTask(UploadTask):
    """
    Class for file upload tasks.
    """

    def __init__(self, file_path: Path, key: str):
        self.key = key
        self.file_path = file_path

    def __repr__(self):
        return f"UploadFileTask({self.file_path}, {self.key})"

    def __str__(self):
        return self.key

    @property
    def size(self) -> int:
        return self.file_path.stat().st_size

    @property
    def etag(self):
        """
        Calculates and returns a value equivalent to the file's AWS ETag value.
        """
        md5s = []

        with self.file_path.open("rb") as f:

            def read_chunks():
                yield f.read(S3_MULTIPART_THRESHOLD)
                while True:
                    yield f.read(S3_MULTIPART_CHUNKSIZE)

            for chunk in read_chunks():
                if not chunk:
                    break
                md5s.append(hashlib.md5(chunk))

        if not md5s:
            return f'"{hashlib.md5().hexdigest()}"'

        if len(md5s) == 1:
            return f'"{md5s[0].hexdigest()}"'

        digests_md5 = hashlib.md5(b"".join(m.digest() for m in md5s))
        return f'"{digests_md5.hexdigest()}-{len(md5s)}"'

    @property
    def content_type(self):
        mime_type = (
            mimetypes.guess_type(str(self.file_path))[0] or "binary/octet-stream"
        )
        if mime_type.startswith("text/") or (
            mime_type in ("application/json", "application/javascript")
        ):
            mime_type += "; charset=utf-8"

        if mime_type == "binary/octet-stream" and self.file_path.suffix == ".woff2":
            # See https://github.com/mdn/yari/issues/2017
            mime_type = "font/woff2"

        return mime_type

    @property
    def is_hashed(self):
        return hashed_filename_regex.search(self.file_path.name)

    @property
    def cache_control(self):

        if self.file_path.name == "service-worker.js":
            return NO_CACHE_VALUE

        if self.file_path.name == "404.html":
            return NO_CACHE_VALUE

        if self.file_path.parent.name == "_whatsdeployed":
            return NO_CACHE_VALUE

        if self.is_hashed:
            cache_control_seconds = HASHED_CACHE_CONTROL
        else:
            cache_control_seconds = DEFAULT_CACHE_CONTROL

        return f"max-age={cache_control_seconds}, public"

    def upload(self, bucket_manager):
        # print("WOULD UPLOAD FILE", self.file_path, "->", self.key)
        print("UPLOAD FILE", self.file_path, "->", self.key)
        bucket_manager.client.upload_file(
            str(self.file_path),
            bucket_manager.bucket_name,
            self.key,
            ExtraArgs={
                "ACL": "public-read",
                "ContentType": self.content_type,
                "CacheControl": self.cache_control,
            },
        )


class UploadRedirectTask(UploadTask):
    """
    Class for redirect upload tasks.
    """

    is_redirect = True

    def __init__(self, redirect_from_key, redirect_to_key):
        self.key = redirect_from_key
        self.to_key = redirect_to_key

    def __repr__(self):
        return f"UploadRedirectTask({self.key}, {self.to_key})"

    def __str__(self):
        return f"{self.key} -> {self.to_key}"

    @property
    def cache_control(self):
        return f"max-age={HASHED_CACHE_CONTROL}, public"

    def upload(self, bucket_manager):
        pass
        # print("PUT OBJECT (REDIRECT)", self.key)
        # bucket_manager.client.put_object(
        #     Body=b"",
        #     Key=self.key,
        #     ACL="public-read",
        #     CacheControl=self.cache_control,
        #     WebsiteRedirectLocation=self.to_key,
        #     Bucket=bucket_manager.bucket_name,
        # )


class BucketManager:
    def __init__(self, bucket_name, bucket_prefix):
        self.bucket_name = bucket_name
        self.bucket_prefix = bucket_prefix

    @property
    def key_prefix(self):
        if self.bucket_prefix:
            return f"{self.bucket_prefix.lower()}/"
        return ""

    @cached_property
    def client(self):
        # According to the boto3 docs, this low-level client is thread-safe.
        return boto3.client("s3")

    def get_key(self, build_directory, file_path):
        if file_path.name == "index.html":
            # NOTE: All incoming requests, unless immediately served from the CDN
            # cache, are first handled by our "origin-request" Lambda@Edge function,
            # which tansforms incoming URL's to S3 keys. This line of code allows
            # that function to remain as simple as possible, because we no longer
            # have to determine when to add an "/index.html" suffix when transforming
            # the incoming URL to it corresponding S3 key. We will simply store
            # "/en-us/docs/web/index.html" as "/en-us/docs/web", which mirrors
            # its URL. Also, note that the content type is not determined by the
            # suffix of the S3 key, but is explicitly set from the full filepath
            # when uploading the file.
            file_path = file_path.parent
        return f"{self.key_prefix}{str(file_path.relative_to(build_directory)).lower()}"

    def get_redirect_keys(self, from_url, to_url):
        return (
            f"{self.key_prefix}{from_url.strip('/').lower()}",
            f"/{to_url.strip('/')}" if to_url.startswith("/") else to_url,
        )

    def get_bucket_objects(self):
        result = {}
        continuation_token = None
        while True:
            kwargs = dict(Bucket=self.bucket_name)
            if self.key_prefix:
                kwargs["Prefix"] = self.key_prefix
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            response = self.client.list_objects_v2(**kwargs)
            for obj in response.get("Contents", ()):
                result[obj["Key"]] = obj
            if response["IsTruncated"]:
                continuation_token = response["NextContinuationToken"]
            else:
                break
        return result

    def iter_file_tasks(self, build_directory, for_counting_only=False):
        # Prepare a computation of what the root /index.html file would be
        # called as a S3 key. Do this once so it becomes a quicker operation
        # later when we compare *each* generated key to see if it matches this.
        root_index_html_as_key = self.get_key(
            build_directory, build_directory / "index.html"
        )

        # Walk the build_directory and yield file upload tasks.
        for fp in iterdir(build_directory):
            # Exclude any files that aren't artifacts of the build.
            if fp.name.startswith(".") or fp.name.endswith("~"):
                continue

            key = self.get_key(build_directory, fp)

            # The root index.html file is never useful. It's not the "home page"
            # because the home page is actually `/$locale/` since `/` is handled
            # specifically by the CDN.
            # The client/build/index.html is actually just a template from
            # create-react-app, used to server-side render all the other pages.
            # But we don't want to upload it S3. So, delete it before doing the
            # deployment step.
            if root_index_html_as_key == key:
                continue

            if for_counting_only:
                yield 1
            else:
                yield UploadFileTask(fp, key)

    def iter_redirect_tasks(self, content_roots, for_counting_only=False):
        # Walk the content roots and yield redirect upload tasks.
        for content_root in content_roots:
            # Look for "_redirects.txt" files up to two levels deep to
            # accommodate the content root specified as the root of the
            # repo or the "files" sub-directory of the root of the repo.
            for fp in chain(
                content_root.glob("*/_redirects.txt"),
                content_root.glob("*/*/_redirects.txt"),
            ):
                for line_num, line in enumerate(fp.read_text().split("\n"), start=1):
                    line = line.strip()
                    if line and not line.startswith("#"):
                        parts = line.split("\t")
                        if len(parts) != 2:
                            raise Exception(
                                f"Unable to parse {fp}:{line_num} into a from/to URL pair"
                            )
                        from_url, to_url = parts
                        if for_counting_only:
                            yield 1
                        else:
                            yield UploadRedirectTask(
                                *self.get_redirect_keys(from_url, to_url)
                            )

    def count_file_tasks(self, build_directory):
        return sum(self.iter_file_tasks(build_directory, for_counting_only=True))

    def count_redirect_tasks(self, content_roots):
        return sum(self.iter_redirect_tasks(content_roots, for_counting_only=True))

    def upload(
        self,
        build_directory,
        content_roots,
        existing_bucket_objects=None,
        on_task_complete=None,
    ):
        print("_BUILD_DIRECTORY:", repr(build_directory))

        def list_dir_files(d):
            for thing in d.iterdir():
                if thing.is_dir():
                    list_dir_files(thing)
                else:
                    print("\tthing:", thing)

        assert build_directory.exists(), build_directory
        list_dir_files(build_directory)
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_WORKERS_PARALLEL_UPLOADS
        ) as executor, StopWatch() as timer:
            # Upload the redirects first, then the built files. This
            # ensures that a built file overrides its stale redirect.
            for task_iter in (
                # lambda: self.iter_redirect_tasks(content_roots),
                lambda: self.iter_file_tasks(build_directory),
            ):
                futures = {}
                for task in task_iter():
                    print("CONSIDER TASK", repr(task))
                    # Note: redirect upload tasks are never skipped.
                    if existing_bucket_objects and not task.is_redirect:
                        s3_obj = existing_bucket_objects.get(task.key)
                        if s3_obj and s3_obj["ETag"] == task.etag:
                            task.skipped = True
                            if on_task_complete:
                                on_task_complete(task)
                            continue
                    future = executor.submit(task.upload, self)
                    futures[future] = task

                for future in concurrent.futures.as_completed(futures):
                    task = futures[future]
                    try:
                        task.error = future.exception()
                    except concurrent.futures.CancelledError as cancelled:
                        task.error = cancelled

                    if on_task_complete:
                        on_task_complete(task)

        return timer


def upload_content(build_directory, content_roots, config):
    full_timer = StopWatch().start()

    dry_run = config["dry_run"]
    bucket_name = config["bucket"]
    bucket_prefix = config["prefix"]
    force_refresh = config["force_refresh"]
    show_progress_bar = not config["no_progressbar"]

    log.info(f"Upload files from: {build_directory}")
    log.info(f"Upload redirects from: {', '.join(str(fp) for fp in content_roots)}")
    log.info("Upload into: ", nl=False)
    if bucket_prefix:
        log.info(f"{bucket_prefix}/ of ", nl=False)
    log.info(bucket_name)

    mgr = BucketManager(bucket_name, bucket_prefix)

    with StopWatch() as timer:
        total_redirects = mgr.count_redirect_tasks(content_roots)
        if not total_redirects:
            raise click.ClickException(
                "unable to find any redirects to upload (did you specify the right content-root?)"
            )
    log.info(f"Total pending redirect uploads: {total_redirects:,} ({timer})")

    with StopWatch() as timer:
        total_possible_files = mgr.count_file_tasks(build_directory)
    log.info(f"Total pending file uploads: {total_possible_files:,} ({timer})")

    if force_refresh:
        existing_bucket_objects = None
    else:
        log.info("Total existing S3 objects: ", nl=False)
        with StopWatch() as timer:
            existing_bucket_objects = mgr.get_bucket_objects()
        log.info(f"{len(existing_bucket_objects):,} ({timer})")

    totals = Totals()

    if dry_run:
        upload_timer = StopWatch()
    else:
        with DisplayProgress(
            total_redirects + total_possible_files, show_progress_bar
        ) as progress:

            def on_task_complete(task):
                progress.update(task)
                totals.count(task)

            upload_timer = mgr.upload(
                build_directory,
                content_roots,
                existing_bucket_objects,
                on_task_complete=on_task_complete,
            )

    log.info(
        f"Total uploaded files: {totals.uploaded_files:,} "
        f"({fmt_size(totals.uploaded_files_size)})"
    )
    log.info(f"Total uploaded redirects: {totals.uploaded_redirects:,}")
    log.info(f"Total skipped files: {totals.skipped:,} matched existing S3 objects")
    log.info(f"Total upload/skip time: {upload_timer}")
    log.info(f"Done in {full_timer.stop()}.")

    if totals.failed:
        raise click.ClickException(f"There were {totals.failed} failures.")
